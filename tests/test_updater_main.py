from datetime import datetime, timezone

import pytest

from app.updater_main import (
    _build_parser,
    _execute_manual_command,
    _parse_synced_at,
    _scheduled_run_time,
    _should_run_daily_maintenance,
)


def test_scheduled_run_time_rolls_to_next_day_after_target_hour() -> None:
    now = datetime(2026, 4, 1, 15, 0, tzinfo=timezone.utc)
    scheduled = _scheduled_run_time(now, 3)
    assert scheduled.day == 2
    assert scheduled.hour == 3


def test_parse_synced_at_adds_utc_when_timezone_missing() -> None:
    parsed = _parse_synced_at("2026-04-01T03:00:00")
    assert parsed is not None
    assert parsed.tzinfo == timezone.utc


def test_should_run_daily_maintenance_false_before_hour() -> None:
    now = datetime(2026, 4, 1, 2, 30, tzinfo=timezone.utc)
    assert _should_run_daily_maintenance(None, now, 3) is False


def test_should_run_daily_maintenance_true_after_hour_without_prior_run() -> None:
    now = datetime(2026, 4, 1, 4, 0, tzinfo=timezone.utc)
    assert _should_run_daily_maintenance(None, now, 3) is True


def test_should_run_daily_maintenance_false_after_same_day_run() -> None:
    now = datetime(2026, 4, 1, 4, 0, tzinfo=timezone.utc)
    assert _should_run_daily_maintenance("2026-04-01T03:05:00+00:00", now, 3) is False


def test_build_parser_parses_full_refresh_command() -> None:
    parser = _build_parser()

    args = parser.parse_args(["full-refresh", "--year", "2026", "--game-type", "regular"])

    assert args.command == "full-refresh"
    assert args.year == 2026
    assert args.game_type == "regular"


def test_build_parser_parses_reconcile_date_command() -> None:
    parser = _build_parser()

    args = parser.parse_args(["reconcile-date", "--date", "2026-04-01"])

    assert args.command == "reconcile-date"
    assert args.date == "2026-04-01"


class FakeSyncService:
    def __init__(self) -> None:
        self.calls = []

    async def manual_full_refresh(self, year: int, game_type: str) -> None:
        self.calls.append(("full-refresh", year, game_type))

    async def reconcile_date(self, year: int, game_type: str, target_date: str, sync_kind: str = "reconcile") -> None:
        self.calls.append(("reconcile-date", year, game_type, target_date, sync_kind))

    async def integrity_sweep(self, year: int, game_type: str, days: int) -> None:
        self.calls.append(("integrity-sweep", year, game_type, days))


@pytest.mark.asyncio
async def test_execute_manual_command_runs_full_refresh() -> None:
    service = FakeSyncService()

    message = await _execute_manual_command(service, "full-refresh", 2026, "regular")

    assert service.calls == [("full-refresh", 2026, "regular")]
    assert message == "Manual full refresh complete: 2026 regular"


@pytest.mark.asyncio
async def test_execute_manual_command_runs_reconcile_date() -> None:
    service = FakeSyncService()

    message = await _execute_manual_command(service, "reconcile-date", 2026, "regular", target_date="2026-04-01")

    assert service.calls == [("reconcile-date", 2026, "regular", "2026-04-01", "admin")]
    assert message == "Manual reconcile complete: 2026-04-01 regular"
