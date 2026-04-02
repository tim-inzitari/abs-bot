import argparse
import asyncio
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Sequence

import aiohttp
import pytz

from app.config import load_settings
from app.db import Database
from app.integrations.baseball_savant import BaseballSavantClient
from app.integrations.mlb_stats import MlbStatsClient
from app.sync_service import SyncService

GAME_TYPE_CHOICES = ("spring", "regular", "postseason")


def _iso_date(value: str) -> str:
    date.fromisoformat(value)
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ABS bot updater and manual sync entrypoint.")
    subparsers = parser.add_subparsers(dest="command")

    full_refresh = subparsers.add_parser("full-refresh", help="Rebuild the season dataset and refresh today's games.")
    full_refresh.add_argument("--year", type=int, help="Season year. Defaults to DEFAULT_SEASON.")
    full_refresh.add_argument("--game-type", choices=GAME_TYPE_CHOICES, help="Game type. Defaults to DEFAULT_GAME_TYPE.")

    reconcile_date = subparsers.add_parser("reconcile-date", help="Reconcile a specific date authoritatively.")
    reconcile_date.add_argument("--date", required=True, type=_iso_date, help="Target date in YYYY-MM-DD format.")
    reconcile_date.add_argument("--year", type=int, help="Season year. Defaults to the year parsed from --date.")
    reconcile_date.add_argument("--game-type", choices=GAME_TYPE_CHOICES, help="Game type. Defaults to DEFAULT_GAME_TYPE.")

    integrity_sweep = subparsers.add_parser("integrity-sweep", help="Reconcile the recent rolling window again.")
    integrity_sweep.add_argument("--days", type=int, default=None, help="How many trailing days to sweep.")
    integrity_sweep.add_argument("--year", type=int, help="Season year. Defaults to DEFAULT_SEASON.")
    integrity_sweep.add_argument("--game-type", choices=GAME_TYPE_CHOICES, help="Game type. Defaults to DEFAULT_GAME_TYPE.")

    return parser


def _scheduled_run_time(now: datetime, hour_eastern: int) -> datetime:
    scheduled = now.replace(hour=hour_eastern, minute=0, second=0, microsecond=0)
    if scheduled <= now:
        scheduled += timedelta(days=1)
    return scheduled


def _parse_synced_at(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _should_run_daily_maintenance(last_synced_at: Optional[str], now: datetime, hour_eastern: int) -> bool:
    scheduled_today = now.replace(hour=hour_eastern, minute=0, second=0, microsecond=0)
    if now < scheduled_today:
        return False

    last_run = _parse_synced_at(last_synced_at)
    if last_run is None:
        return True
    return last_run.astimezone(now.tzinfo) < scheduled_today


def _print_progress(message: str) -> None:
    print(message, flush=True)


async def _execute_manual_command(
    sync_service: SyncService,
    command: str,
    year: int,
    game_type: str,
    *,
    target_date: Optional[str] = None,
    days: Optional[int] = None,
) -> str:
    if command == "full-refresh":
        await sync_service.manual_full_refresh(year, game_type)
        return f"Manual full refresh complete: {year} {game_type}"

    if command == "reconcile-date":
        if not target_date:
            raise ValueError("reconcile-date requires target_date")
        await sync_service.reconcile_date(year, game_type, target_date, sync_kind="admin")
        return f"Manual reconcile complete: {target_date} {game_type}"

    if command == "integrity-sweep":
        sweep_days = days or 7
        await sync_service.integrity_sweep(year, game_type, sweep_days)
        return f"Manual integrity sweep complete: {year} {game_type} last {sweep_days} days"

    raise ValueError(f"Unsupported manual command: {command}")


async def run_manual_command(
    command: str,
    year: int,
    game_type: str,
    *,
    target_date: Optional[str] = None,
    days: Optional[int] = None,
) -> str:
    settings = load_settings()
    _print_progress(f"Manual updater command starting: {command} ({year} {game_type})")
    db = Database(settings.database_path)
    db.ensure_schema()
    try:
        async with aiohttp.ClientSession(
            headers={"user-agent": "abs-bot-updater/1.0", "accept": "application/json,text/html;q=0.9,*/*;q=0.8"}
        ) as session:
            sync_service = SyncService(
                db=db,
                savant_client=BaseballSavantClient(session, settings.cache_ttl_seconds),
                mlb_stats_client=MlbStatsClient(session, settings.cache_ttl_seconds),
                progress=_print_progress,
            )
            result = await _execute_manual_command(
                sync_service,
                command,
                year,
                game_type,
                target_date=target_date,
                days=days,
            )
            _print_progress(result)
            return result
    finally:
        db.close()


async def run_updater_loop() -> None:
    settings = load_settings()
    _print_progress(
        f"Updater starting for {settings.default_season} {settings.default_game_type}; connecting to database"
    )
    db = Database(settings.database_path)
    db.ensure_schema()
    async with aiohttp.ClientSession(
        headers={"user-agent": "abs-bot-updater/1.0", "accept": "application/json,text/html;q=0.9,*/*;q=0.8"}
    ) as session:
        sync_service = SyncService(
            db=db,
            savant_client=BaseballSavantClient(session, settings.cache_ttl_seconds),
            mlb_stats_client=MlbStatsClient(session, settings.cache_ttl_seconds),
            progress=_print_progress,
        )
        _print_progress("Updater connected; ensuring default dataset exists")
        await sync_service.ensure_dataset(settings.default_season, settings.default_game_type)
        eastern = pytz.timezone("America/New_York")
        _print_progress("Updater ready; entering scheduled maintenance loop")
        while True:
            now = datetime.now(tz=eastern)
            if _should_run_daily_maintenance(
                db.get_sync_state(settings.default_season, settings.default_game_type, "reconcile"),
                now,
                settings.daily_update_hour_eastern,
            ):
                try:
                    _print_progress("Updater maintenance window opened; starting nightly jobs")
                    await sync_service.reconcile_previous_day(settings.default_season, settings.default_game_type)
                    await sync_service.integrity_sweep(
                        settings.default_season,
                        settings.default_game_type,
                        settings.integrity_sweep_days,
                    )
                    _print_progress("Updater nightly jobs complete")
                except Exception as error:
                    print(f"Updater error: {error}")
                    await asyncio.sleep(60)
                continue
            next_run = _scheduled_run_time(now, settings.daily_update_hour_eastern)
            sleep_seconds = max(60, int((next_run - datetime.now(tz=eastern)).total_seconds()))
            _print_progress(
                f"Updater idle; next maintenance window at {next_run.isoformat()} ({sleep_seconds}s)"
            )
            await asyncio.sleep(sleep_seconds)


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        asyncio.run(run_updater_loop())
        return

    settings = load_settings()
    if args.command == "reconcile-date":
        parsed_date = date.fromisoformat(args.date)
        year = args.year or parsed_date.year
    else:
        year = args.year or settings.default_season

    game_type = args.game_type or settings.default_game_type
    result = asyncio.run(
        run_manual_command(
            args.command,
            year,
            game_type,
            target_date=getattr(args, "date", None),
            days=getattr(args, "days", None),
        )
    )
    print(result)


if __name__ == "__main__":
    main()
