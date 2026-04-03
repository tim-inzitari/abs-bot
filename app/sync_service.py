from dataclasses import dataclass
from datetime import date, datetime, timedelta
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

import pytz

from app.db import Database
from app.integrations.baseball_savant import BaseballSavantClient
from app.integrations.mlb_stats import MlbStatsClient, get_home_plate_umpire
from app.utils import normalize_search


def _parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def _is_same_season(target_date: str, year: int) -> bool:
    return _parse_iso_date(target_date).year == year


def _today_eastern_iso() -> str:
    eastern = pytz.timezone("America/New_York")
    return datetime.now(tz=eastern).date().isoformat()


def _should_log_progress(index: int, total: int) -> bool:
    if total <= 10:
        return True
    return index in {1, total} or index % 25 == 0


def _schedule_scan_skip_reason(game: Dict[str, Any], target_date: Optional[str] = None) -> Optional[str]:
    official_date = str(game.get("official_date") or "")
    if target_date and official_date != target_date:
        return f"official date moved to {official_date}"

    detailed_state = str(game.get("detailed_state") or "").lower()
    if any(token in detailed_state for token in ["postpon", "cancel", "suspend"]):
        return f"status {game.get('detailed_state') or 'unknown'}"

    return None


def _position_to_challenger_role(position: Optional[str]) -> str:
    normalized = (position or "").upper()
    if normalized == "P":
        return "pitcher"
    if normalized == "C":
        return "catcher"
    if normalized:
        return "batter"
    return "unknown"


def _role_priority(role: str) -> int:
    return {"unknown": 0, "batter": 1, "catcher": 2, "pitcher": 3}.get(role, 0)


def _record_role_alias(role_aliases: Dict[str, str], alias: Optional[str], role: str) -> None:
    if not alias:
        return
    key = normalize_search(alias)
    if not key:
        return
    existing = role_aliases.get(key, "unknown")
    if _role_priority(role) >= _role_priority(existing):
        role_aliases[key] = role


def _summary_role_aliases(feed: Dict[str, Any]) -> Dict[str, str]:
    role_aliases: Dict[str, str] = {}

    game_players = feed.get("gameData", {}).get("players", {})
    if isinstance(game_players, dict):
        for player in game_players.values():
            if not isinstance(player, dict):
                continue
            role = _position_to_challenger_role(player.get("primaryPosition", {}).get("abbreviation"))
            for alias in [
                player.get("fullName"),
                player.get("boxscoreName"),
                player.get("lastInitName"),
                player.get("nameFirstLast"),
                player.get("useLastName"),
                player.get("lastName"),
            ]:
                _record_role_alias(role_aliases, alias, role)

    boxscore_teams = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})
    for side in ["away", "home"]:
        players = boxscore_teams.get(side, {}).get("players", {})
        if not isinstance(players, dict):
            continue
        for player in players.values():
            if not isinstance(player, dict):
                continue
            person = player.get("person", {})
            role = _position_to_challenger_role(
                player.get("position", {}).get("abbreviation")
                or person.get("primaryPosition", {}).get("abbreviation")
            )
            for alias in [
                person.get("fullName"),
                person.get("boxscoreName"),
                person.get("lastInitName"),
                person.get("useLastName"),
                person.get("lastName"),
            ]:
                _record_role_alias(role_aliases, alias, role)

    return role_aliases


def _boxscore_abs_summary(feed: Dict[str, Any]) -> Optional[str]:
    info_rows = feed.get("liveData", {}).get("boxscore", {}).get("info", [])
    for row in info_rows:
        if not isinstance(row, dict):
            continue
        label = str(row.get("label") or "").strip().lower()
        if label == "abs challenge":
            value = row.get("value")
            return str(value).strip() if value else None
    return None


def _parse_boxscore_summary_outcome(result_text: str) -> str:
    normalized = result_text.lower()
    if "overturned" in normalized or "changed" in normalized:
        return "overturned"
    if "confirmed" in normalized or "upheld" in normalized:
        return "confirmed"
    return "unknown"


def _summary_challenge_candidates(
    feed: Dict[str, Any],
    game_pk: int,
    official_date: str,
    matchup: str,
    umpire_name: str,
) -> List[Tuple[int, str, str, str, str, Optional[int], Optional[str], Optional[str], Optional[str], str, str, str]]:
    summary = _boxscore_abs_summary(feed)
    if not summary:
        return []

    role_aliases = _summary_role_aliases(feed)
    candidates: List[Tuple[int, str, str, str, str, Optional[int], Optional[str], Optional[str], Optional[str], str, str, str]] = []
    for entry_index, entry in enumerate(summary.split(";")):
        cleaned_entry = entry.strip().rstrip(".")
        if not cleaned_entry:
            continue
        match = re.match(r"^(?P<challenger>.*?)\s*\((?P<results>.*?)\)$", cleaned_entry)
        if not match:
            continue

        challenger = re.sub(r"\s+\d+$", "", match.group("challenger").strip())
        role = role_aliases.get(normalize_search(challenger), "unknown")
        outcome_texts = [part.strip() for part in match.group("results").split(",") if part.strip()]
        for outcome_index, outcome_text in enumerate(outcome_texts):
            outcome = _parse_boxscore_summary_outcome(outcome_text)
            description = f"ABS Challenge summary: {challenger} ({outcome_text})"
            batter_name = challenger if role == "batter" else None
            pitcher_name = challenger if role == "pitcher" else None
            catcher_name = challenger if role == "catcher" else None
            candidates.append(
                (
                    game_pk,
                    f"summary-{entry_index}-{outcome_index}",
                    official_date,
                    matchup,
                    umpire_name,
                    None,
                    batter_name,
                    pitcher_name,
                    catcher_name,
                    role,
                    outcome,
                    description,
                )
            )
    return candidates


def _looks_like_abs_challenge(text: str) -> bool:
    abs_tokens = ["automatic ball", "automatic strike", "abs challenge", "abs review", "automated ball-strike", "automated ball strike"]
    role_tokens = [
        "pitcher challenge",
        "pitcher challenged",
        "challenged by pitcher",
        "challenged by the pitcher",
        "catcher challenge",
        "catcher challenged",
        "challenged by catcher",
        "challenged by the catcher",
        "batter challenge",
        "batter challenged",
        "challenged by batter",
        "challenged by the batter",
        "hitter challenge",
        "hitter challenged",
        "challenged by hitter",
        "challenged by the hitter",
    ]
    pitch_call_tokens = ["called strike", "called ball", "strike call", "ball call"]
    outcome_tokens = ["successful challenge", "unsuccessful challenge", "confirmed", "upheld", "overturned", "call was changed"]

    return (
        any(token in text for token in abs_tokens)
        or any(token in text for token in role_tokens)
        or ("challenge" in text and any(token in text for token in pitch_call_tokens + outcome_tokens))
    )


def parse_challenge_event(event_text: str) -> str:
    text = event_text.lower()
    if any(token in text for token in ["manager challenge", "replay challenge"]):
        return "not-challenge"
    if not _looks_like_abs_challenge(text):
        return "not-challenge"
    if any(token in text for token in ["confirmed", "upheld", "unsuccessful challenge"]):
        return "confirmed"
    if any(token in text for token in ["overturned", "successful challenge", "call was changed"]):
        return "overturned"
    return "unknown"


def parse_challenger_role(event_text: str) -> str:
    text = event_text.lower()
    if any(token in text for token in ["catcher challenge", "catcher challenged", "challenged by catcher", "challenged by the catcher"]):
        return "catcher"
    if any(token in text for token in ["pitcher challenge", "pitcher challenged", "challenged by pitcher", "challenged by the pitcher"]):
        return "pitcher"
    if any(
        token in text
        for token in [
            "batter challenge",
            "batter challenged",
            "hitter challenge",
            "hitter challenged",
            "challenged by batter",
            "challenged by the batter",
            "challenged by hitter",
            "challenged by the hitter",
        ]
    ):
        return "batter"
    return "unknown"


def _called_pitch_code(event: Dict[str, Any]) -> Optional[str]:
    details = event.get("details", {})
    call = details.get("call", {})
    if isinstance(call, dict):
        code = call.get("code")
        if isinstance(code, str):
            return code
    code = details.get("code")
    return code if isinstance(code, str) else None


def _called_pitch_is_correct(event: Dict[str, Any]) -> Optional[bool]:
    code = _called_pitch_code(event)
    if code not in {"B", "C"}:
        return None

    pitch_data = event.get("pitchData", {})
    coordinates = pitch_data.get("coordinates", {})
    plate_x = coordinates.get("pX")
    plate_z = coordinates.get("pZ")
    zone_top = pitch_data.get("strikeZoneTop")
    zone_bottom = pitch_data.get("strikeZoneBottom")
    zone_width = pitch_data.get("strikeZoneWidth")
    if not all(isinstance(value, (int, float)) for value in [plate_x, plate_z, zone_top, zone_bottom]):
        return None

    half_width = float(zone_width) / 2.0 if isinstance(zone_width, (int, float)) else 0.7083333333333334
    in_zone = abs(float(plate_x)) <= half_width and float(zone_bottom) <= float(plate_z) <= float(zone_top)
    if code == "C":
        return in_zone
    return not in_zone


@dataclass
class SyncService:
    db: Database
    savant_client: BaseballSavantClient
    mlb_stats_client: MlbStatsClient
    progress: Optional[Callable[[str], None]] = None

    def _emit_progress(self, message: str) -> None:
        if self.progress is not None:
            self.progress(message)

    async def _refresh_live_leaderboards(self, year: int, game_type: str) -> None:
        self._emit_progress(f"Today refresh: refreshing live Savant leaderboards for {year} {game_type}")
        for challenge_type in ["batter", "pitcher", "catcher", "batting-team"]:
            leaderboard = await self.savant_client.fetch_leaderboard(
                year,
                challenge_type,
                game_type,
                force_refresh=True,
            )
            self.db.replace_leaderboard_rows(year, game_type, challenge_type, leaderboard.rows)
            self._emit_progress(
                f"Today refresh: stored live {challenge_type} leaderboard ({len(leaderboard.rows)} rows)"
            )

    async def ensure_dataset(self, year: int, game_type: str) -> None:
        if self.db.get_sync_state(year, game_type, "full"):
            return
        self._emit_progress(f"Dataset seed missing for {year} {game_type}; starting initial full refresh")
        await self.full_refresh(year, game_type)

    async def manual_full_refresh(self, year: int, game_type: str) -> None:
        self._emit_progress(f"Manual full refresh starting for {year} {game_type}")
        await self.full_refresh(year, game_type)
        self._emit_progress(f"Manual full refresh historical pass complete for {year} {game_type}; refreshing today")
        await self.refresh_today(year, game_type)
        self._emit_progress(f"Manual full refresh complete for {year} {game_type}")

    async def full_refresh(self, year: int, game_type: str) -> None:
        self._emit_progress(f"Full refresh: fetching Savant leaderboards for {year} {game_type}")
        for challenge_type in ["batter", "pitcher", "catcher", "batting-team"]:
            leaderboard = await self.savant_client.fetch_leaderboard(year, challenge_type, game_type)
            self.db.replace_leaderboard_rows(year, game_type, challenge_type, leaderboard.rows)
            self._emit_progress(
                f"Full refresh: stored {challenge_type} leaderboard ({len(leaderboard.rows)} rows)"
            )

        teams = await self.mlb_stats_client.get_teams(year)
        self.db.replace_teams(year, [(team.team_id, team.name, team.abbreviation) for team in teams])
        self._emit_progress(f"Full refresh: stored team directory ({len(teams)} teams)")

        batter_rows = self.db.fetch_leaderboard_rows(year, game_type, "batter")
        person_ids = [int(row["id"]) for row in batter_rows if isinstance(row.get("id"), (int, float))]
        positions = await self.mlb_stats_client.get_people_positions(person_ids)
        self.db.replace_player_positions(
            year,
            [(entry.person_id, entry.full_name, entry.position) for entry in positions.values()],
        )
        self._emit_progress(f"Full refresh: stored player positions ({len(positions)} players)")

        games = await self.mlb_stats_client.get_schedule_games(year, game_type)
        historical_games = [
            game
            for game in games
            if str(game["official_date"]) < _today_eastern_iso() and _schedule_scan_skip_reason(game) is None
        ]
        self._emit_progress(f"Full refresh: scanning {len(historical_games)} historical games")
        scanned_at = datetime.utcnow().isoformat()
        game_catalog_rows: List[Tuple[int, str, Optional[str], Optional[str], str, Optional[str], str]] = []
        umpire_rows: List[Tuple[int, str, str, str, int, int, int]] = []
        audit_rows: List[Tuple[int, str, str, str, int, int, int, int]] = []
        challenge_rows: List[Tuple[int, str, str, str, str, Optional[int], Optional[str], Optional[str], Optional[str], str, str, str]] = []
        error_rows: List[Tuple[Optional[str], Optional[str], Optional[str], str, Optional[int]]] = []
        total_games = len(historical_games)
        for index, game in enumerate(historical_games, start=1):
            if _should_log_progress(index, total_games):
                self._emit_progress(
                    f"Full refresh: scanning game {index}/{total_games} on {game['official_date']} ({game['matchup']})"
                )
            game_row, audit_row, event_rows, game_errors = await self._scan_game(
                year=year,
                game_type=game_type,
                game_pk=int(game["game_pk"]),
                official_date=str(game["official_date"]),
                matchup=str(game["matchup"]),
                include_pitch_audit=True,
            )
            if game_row is not None:
                umpire_rows.append(game_row)
            game_catalog_rows.append(
                (
                    int(game["game_pk"]),
                    str(game["official_date"]),
                    game.get("away_team_name"),
                    game.get("home_team_name"),
                    str(game["matchup"]),
                    game_row[3] if game_row is not None else None,
                    scanned_at,
                )
            )
            if audit_row is not None:
                audit_rows.append(audit_row)
            challenge_rows.extend(event_rows)
            error_rows.extend(game_errors)

        self.db.upsert_games(year, game_type, game_catalog_rows)
        self.db.replace_umpire_games(year, game_type, umpire_rows)
        self.db.replace_umpire_pitch_audits(year, game_type, audit_rows)
        self.db.replace_challenge_events(year, game_type, challenge_rows)
        self.db.replace_untracked_errors(year, game_type, "umpire", error_rows)
        self.db.set_sync_state(year, game_type, "full", datetime.utcnow().isoformat())
        self.db.set_sync_state(year, game_type, "reconcile", datetime.utcnow().isoformat())
        self._emit_progress(
            f"Full refresh complete: {year} {game_type} ({len(umpire_rows)} umpire game rows, {len(challenge_rows)} challenge events)"
        )

    async def refresh_today(self, year: int, game_type: str) -> None:
        self._emit_progress(f"Today refresh: checking {_today_eastern_iso()} for {year} {game_type}")
        if _is_same_season(_today_eastern_iso(), year):
            await self._refresh_live_leaderboards(year, game_type)
        await self.reconcile_date(year, game_type, _today_eastern_iso(), sync_kind="today", include_pitch_audit=True)
        self._emit_progress(f"Today refresh complete for {year} {game_type}")

    async def reconcile_previous_day(self, year: int, game_type: str) -> None:
        eastern = pytz.timezone("America/New_York")
        previous_day = (datetime.now(tz=eastern).date() - timedelta(days=1)).isoformat()
        self._emit_progress(f"Nightly reconcile: starting previous day reconciliation for {previous_day}")
        await self.reconcile_date(year, game_type, previous_day, sync_kind="reconcile")
        self._emit_progress(f"Nightly reconcile complete for {previous_day}")

    async def integrity_sweep(self, year: int, game_type: str, days: int) -> None:
        eastern = pytz.timezone("America/New_York")
        today = datetime.now(tz=eastern).date()
        self._emit_progress(f"Integrity sweep: starting last {days} days for {year} {game_type}")
        for offset in range(2, days + 1):
            target = (today - timedelta(days=offset)).isoformat()
            self._emit_progress(f"Integrity sweep: reconciling {target}")
            await self.reconcile_date(year, game_type, target, sync_kind="sweep", include_pitch_audit=True)
        self._emit_progress(f"Integrity sweep complete for {year} {game_type}")

    async def reconcile_date(
        self,
        year: int,
        game_type: str,
        target_date: str,
        sync_kind: str = "reconcile",
        include_pitch_audit: bool = True,
    ) -> None:
        if not _is_same_season(target_date, year):
            self.db.set_sync_state(year, game_type, sync_kind, datetime.utcnow().isoformat())
            self._emit_progress(
                f"Reconcile {sync_kind}: skipped {target_date} because it is outside season {year}"
            )
            return
        schedule_games = await self.mlb_stats_client.get_schedule_games(year, game_type, start_date=target_date, end_date=target_date)
        if not schedule_games:
            self.db.set_sync_state(year, game_type, sync_kind, datetime.utcnow().isoformat())
            self._emit_progress(f"Reconcile {sync_kind}: no games found for {target_date}")
            return

        game_pks = [int(game["game_pk"]) for game in schedule_games]
        eligible_games: List[Dict[str, Any]] = []
        skipped_games: List[Tuple[Dict[str, Any], str]] = []
        for game in schedule_games:
            skip_reason = _schedule_scan_skip_reason(game, target_date=target_date)
            if skip_reason is None:
                eligible_games.append(game)
            else:
                skipped_games.append((game, skip_reason))

        self._emit_progress(
            f"Reconcile {sync_kind}: scanning {len(eligible_games)} eligible game(s) for {target_date}"
        )
        scanned_at = datetime.utcnow().isoformat()
        game_catalog_rows: List[Tuple[int, str, Optional[str], Optional[str], str, Optional[str], str]] = []
        rows: List[Tuple[int, str, str, str, int, int, int]] = []
        audit_rows: List[Tuple[int, str, str, str, int, int, int, int]] = []
        event_rows: List[Tuple[int, str, str, str, str, Optional[int], Optional[str], Optional[str], Optional[str], str, str, str]] = []
        errors: List[Tuple[Optional[str], Optional[str], Optional[str], str, Optional[int]]] = []
        for game, skip_reason in skipped_games:
            self._emit_progress(
                f"Reconcile {sync_kind}: skipping {game['matchup']} ({skip_reason})"
            )
            game_catalog_rows.append(
                (
                    int(game["game_pk"]),
                    str(game["official_date"]),
                    game.get("away_team_name"),
                    game.get("home_team_name"),
                    str(game["matchup"]),
                    None,
                    scanned_at,
                )
            )

        total_games = len(eligible_games)
        for index, game in enumerate(eligible_games, start=1):
            if _should_log_progress(index, total_games):
                self._emit_progress(
                    f"Reconcile {sync_kind}: scanning game {index}/{total_games} ({game['matchup']})"
                )
            game_row, audit_row, challenge_events, game_errors = await self._scan_game(
                year=year,
                game_type=game_type,
                game_pk=int(game["game_pk"]),
                official_date=str(game["official_date"]),
                matchup=str(game["matchup"]),
                include_pitch_audit=include_pitch_audit,
            )
            if game_row is not None:
                rows.append(game_row)
            game_catalog_rows.append(
                (
                    int(game["game_pk"]),
                    str(game["official_date"]),
                    game.get("away_team_name"),
                    game.get("home_team_name"),
                    str(game["matchup"]),
                    game_row[3] if game_row is not None else None,
                    scanned_at,
                )
            )
            if audit_row is not None:
                audit_rows.append(audit_row)
            event_rows.extend(challenge_events)
            errors.extend(game_errors)

        self.db.upsert_games(year, game_type, game_catalog_rows)
        self.db.delete_umpire_games_for_ids(year, game_type, game_pks)
        self.db.upsert_umpire_games(year, game_type, rows)
        if include_pitch_audit:
            self.db.delete_umpire_pitch_audits_for_ids(year, game_type, game_pks)
            self.db.upsert_umpire_pitch_audits(year, game_type, audit_rows)
        self.db.delete_challenge_events_for_ids(year, game_type, game_pks)
        self.db.upsert_challenge_events(year, game_type, event_rows)
        self.db.delete_untracked_errors_for_games(year, game_type, "umpire", game_pks)
        self.db.append_untracked_errors(year, game_type, "umpire", errors)
        self.db.set_sync_state(year, game_type, sync_kind, datetime.utcnow().isoformat())
        self._emit_progress(
            f"Reconcile {sync_kind} complete for {target_date} ({len(rows)} umpire rows, {len(event_rows)} challenge events)"
        )

    async def _scan_game(
        self,
        year: int,
        game_type: str,
        game_pk: int,
        official_date: str,
        matchup: str,
        include_pitch_audit: bool = True,
    ) -> Tuple[
        Optional[Tuple[int, str, str, str, int, int, int]],
        Optional[Tuple[int, str, str, str, int, int, int, int]],
        List[Tuple[int, str, str, str, str, Optional[int], Optional[str], Optional[str], Optional[str], str, str, str]],
        List[Tuple[Optional[str], Optional[str], Optional[str], str, Optional[int]]],
    ]:
        untracked_errors: List[Tuple[Optional[str], Optional[str], Optional[str], str, Optional[int]]] = []
        challenge_events: List[Tuple[int, str, str, str, str, Optional[int], Optional[str], Optional[str], Optional[str], str, str, str]] = []
        try:
            feed = await self.mlb_stats_client.get_live_feed(game_pk)
            umpire_name = get_home_plate_umpire(feed)
            if not umpire_name:
                untracked_errors.append((None, official_date, matchup, "missing home plate umpire assignment", game_pk))
                return None, None, challenge_events, untracked_errors

            tracked_challenges = 0
            overturned = 0
            confirmed = 0
            called_pitch_records: List[Dict[str, Any]] = []
            for play_index, play in enumerate(feed.get("liveData", {}).get("plays", {}).get("allPlays", [])):
                inning = play.get("about", {}).get("inning", "?")
                batter = play.get("matchup", {}).get("batter", {}).get("fullName") or "Batter"
                pitcher = play.get("matchup", {}).get("pitcher", {}).get("fullName") or "Pitcher"
                catcher = play.get("matchup", {}).get("catcher", {}).get("fullName")
                last_called_pitch_index: Optional[int] = None
                for event_index, event in enumerate(play.get("playEvents", [])):
                    pitch_is_correct = _called_pitch_is_correct(event) if include_pitch_audit else None
                    if pitch_is_correct is not None:
                        called_pitch_records.append({"correct": pitch_is_correct, "challenged": False})
                        last_called_pitch_index = len(called_pitch_records) - 1

                    description = " ".join(
                        part
                        for part in [event.get("details", {}).get("event", ""), event.get("details", {}).get("description", "")]
                        if part
                    )
                    parsed = parse_challenge_event(description)
                    if parsed == "not-challenge":
                        continue
                    challenger_role = parse_challenger_role(description)
                    event_key = f"{play_index}-{event_index}"
                    if include_pitch_audit:
                        if last_called_pitch_index is not None:
                            called_pitch_records[last_called_pitch_index]["challenged"] = True
                        else:
                            untracked_errors.append((umpire_name, official_date, matchup, "challenge missing called pitch context", game_pk))
                    if parsed == "unknown":
                        detail = f"[Inning {inning}] [{pitcher}] [{batter}] ambiguous challenge"
                        untracked_errors.append((umpire_name, official_date, matchup, detail, game_pk))
                        challenge_events.append(
                            (game_pk, event_key, official_date, matchup, umpire_name, int(inning) if str(inning).isdigit() else None, batter, pitcher, catcher, challenger_role, "unknown", description)
                        )
                        continue
                    challenge_events.append(
                        (game_pk, event_key, official_date, matchup, umpire_name, int(inning) if str(inning).isdigit() else None, batter, pitcher, catcher, challenger_role, parsed, description)
                    )
                    tracked_challenges += 1
                    if parsed == "overturned":
                        overturned += 1
                    else:
                        confirmed += 1

            summary_candidates = _summary_challenge_candidates(feed, game_pk, official_date, matchup, umpire_name)
            existing_summary_counts = {"overturned": overturned, "confirmed": confirmed}
            remaining_summary_counts = {
                "overturned": max(0, sum(1 for event in summary_candidates if event[10] == "overturned") - existing_summary_counts["overturned"]),
                "confirmed": max(0, sum(1 for event in summary_candidates if event[10] == "confirmed") - existing_summary_counts["confirmed"]),
            }
            summary_added_counts = {"overturned": 0, "confirmed": 0}
            for candidate in summary_candidates:
                outcome = candidate[10]
                if outcome not in remaining_summary_counts:
                    if outcome == "unknown":
                        untracked_errors.append((umpire_name, official_date, matchup, "ambiguous ABS summary challenge", game_pk))
                        challenge_events.append(candidate)
                    continue
                if remaining_summary_counts[outcome] <= 0:
                    continue
                challenge_events.append(candidate)
                tracked_challenges += 1
                if outcome == "overturned":
                    overturned += 1
                else:
                    confirmed += 1
                summary_added_counts[outcome] += 1
                remaining_summary_counts[outcome] -= 1

            pitch_audit_row: Optional[Tuple[int, str, str, str, int, int, int, int]] = None
            if include_pitch_audit:
                challenged_pitches = sum(1 for record in called_pitch_records if record["challenged"])
                unchallenged_correct = sum(1 for record in called_pitch_records if (not record["challenged"]) and record["correct"])
                unchallenged_incorrect = sum(1 for record in called_pitch_records if (not record["challenged"]) and (not record["correct"]))
                summary_confirmed = min(summary_added_counts["confirmed"], unchallenged_correct)
                summary_overturned = min(summary_added_counts["overturned"], unchallenged_incorrect)
                if summary_confirmed != summary_added_counts["confirmed"] or summary_overturned != summary_added_counts["overturned"]:
                    untracked_errors.append((umpire_name, official_date, matchup, "summary challenge totals exceeded called pitch audit", game_pk))
                challenged_pitches += summary_confirmed + summary_overturned
                unchallenged_correct -= summary_confirmed
                unchallenged_incorrect -= summary_overturned
                pitch_audit_row = (
                    game_pk,
                    official_date,
                    matchup,
                    umpire_name,
                    len(called_pitch_records),
                    challenged_pitches,
                    unchallenged_correct,
                    unchallenged_incorrect,
                )

            return (game_pk, official_date, matchup, umpire_name, tracked_challenges, overturned, confirmed), pitch_audit_row, challenge_events, untracked_errors
        except Exception as error:
            untracked_errors.append((None, official_date, matchup, str(error), game_pk))
            return None, None, challenge_events, untracked_errors
