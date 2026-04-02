from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

from app.db import Database
from app.utils import normalize_search, percent, score_match


@dataclass
class AnalyticsReport:
    summary: str
    untracked_errors: List[str]


def _number(row: Dict[str, Any], key: str) -> float:
    value = row.get(key)
    return float(value) if isinstance(value, (int, float)) else 0.0


def _text(row: Dict[str, Any], key: str) -> str:
    value = row.get(key)
    return value if isinstance(value, str) else ""


def _entity_fields(row: Dict[str, Any]) -> List[str]:
    return [_text(row, "player_name"), _text(row, "team_abbr"), _text(row, "parent_org")]


def _best_match_score(query: str, fields: List[str]) -> int:
    return max((score_match(query, field) for field in fields), default=0)


def _same_entity(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
    left_id = left.get("id")
    right_id = right.get("id")
    if isinstance(left_id, (int, float)) and isinstance(right_id, (int, float)):
        return int(left_id) == int(right_id)
    return _text(left, "player_name") == _text(right, "player_name") and _text(left, "team_abbr") == _text(right, "team_abbr")


def _percent_with_counts(numerator: int, denominator: int) -> str:
    accuracy = (numerator / denominator) if denominator else 0.0
    return f"{percent(accuracy)} ({numerator}/{denominator})"


def _chunk_name_lines(names: List[str], max_lines: int = 2, max_chars: int = 110) -> List[str]:
    if not names:
        return []

    lines: List[str] = []
    current = ""
    omitted = 0

    for index, name in enumerate(names):
        candidate = f"{current} | {name}" if current else name
        if len(candidate) <= max_chars:
            current = candidate
            continue

        if current:
            lines.append(current)
        current = name

        if len(lines) == max_lines:
            omitted = len(names) - index
            current = ""
            break

    if current and len(lines) < max_lines:
        lines.append(current)

    if omitted:
        suffix = f"... +{omitted} more"
        if lines:
            base = lines[-1]
            if len(base) + 1 + len(suffix) <= max_chars:
                lines[-1] = f"{base} {suffix}"
            else:
                lines[-1] = f"{base[: max(0, max_chars - len(suffix) - 1)]} {suffix}".rstrip()
        else:
            lines.append(suffix)

    return lines[:max_lines]


def _umpire_display_name(names: List[str], tracked_totals: Dict[str, int]) -> str:
    ordered = sorted(
        names,
        key=lambda candidate: (
            tracked_totals.get(candidate, 0),
            len(candidate),
            candidate.count("."),
            candidate,
        ),
        reverse=True,
    )
    return ordered[0]


class AnalyticsService:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def build_player_report(self, name: str, year: int, game_type: str, role: Optional[str] = None) -> AnalyticsReport:
        challenge_types = [role] if role else ["batter", "pitcher", "catcher"]
        leaderboard_rows: List[Tuple[str, Dict[str, Any]]] = []
        for challenge_type in challenge_types:
            leaderboard_rows.extend((challenge_type, row) for row in self.db.fetch_leaderboard_rows(year, game_type, challenge_type))

        ranked_matches = sorted(
            (
                item
                for item in leaderboard_rows
                if _best_match_score(name, _entity_fields(item[1])) > 0
            ),
            key=lambda item: _best_match_score(name, _entity_fields(item[1])),
            reverse=True,
        )
        if not ranked_matches:
            return AnalyticsReport(summary=f"Player {name}: no cached ABS data", untracked_errors=[])

        challenge_type, row = ranked_matches[0]
        rows_for_role = self.db.fetch_leaderboard_rows(year, game_type, challenge_type)
        ordered = sorted(rows_for_role, key=lambda entry: _number(entry, "rate_overturns"), reverse=True)
        rank = next((index + 1 for index, entry in enumerate(ordered) if _same_entity(entry, row)), len(ordered))
        summary = (
            f"{challenge_type.title()} {_text(row, 'player_name')}: "
            f"{_percent_with_counts(int(_number(row, 'n_overturns')), int(_number(row, 'n_challenges')))} accuracy "
            f"[{_text(row, 'team_abbr') or 'n/a'}] [rank {rank}/{len(ordered)}]"
        )
        return AnalyticsReport(summary=summary, untracked_errors=[])

    async def build_team_report(self, team_query: str, year: int, game_type: str) -> AnalyticsReport:
        rows = self.db.fetch_leaderboard_rows(year, game_type, "batting-team")
        ranked = sorted(
            (
                row
                for row in rows
                if _best_match_score(team_query, [_text(row, "player_name"), _text(row, "team_abbr")]) > 0
            ),
            key=lambda row: _best_match_score(team_query, [_text(row, "player_name"), _text(row, "team_abbr")]),
            reverse=True,
        )
        if not ranked:
            return AnalyticsReport(summary=f"Team {team_query}: no cached ABS data", untracked_errors=[])

        row = ranked[0]
        ordered = sorted(rows, key=lambda entry: _number(entry, "rate_overturns"), reverse=True)
        rank = next((index + 1 for index, entry in enumerate(ordered) if _same_entity(entry, row)), len(ordered))
        opponent_challenges = int(_number(row, "n_challenges_against"))
        opponent_overturns = int(_number(row, "n_overturns_against"))
        if opponent_challenges:
            opponent_bit = _percent_with_counts(opponent_overturns, opponent_challenges)
        else:
            opponent_bit = percent(row.get("rate_overturns_against"))
        summary = (
            f"Team {_text(row, 'player_name')}: "
            f"{_percent_with_counts(int(_number(row, 'n_overturns')), int(_number(row, 'n_challenges')))} accuracy "
            f"[opp {opponent_bit}] [rank {rank}/{len(ordered)}]"
        )
        return AnalyticsReport(summary=summary, untracked_errors=[])

    async def build_league_report(self, year: int, game_type: str) -> AnalyticsReport:
        role_rows = {
            role: self.db.fetch_leaderboard_rows(year, game_type, role)
            for role in ["batter", "pitcher", "catcher", "batting-team"]
        }
        position_map = self.db.fetch_player_positions(year)
        batter_rows = role_rows["batter"]
        position_counts: Dict[str, int] = {}
        for row in batter_rows:
            person_id = int(_number(row, "id"))
            position = position_map.get(person_id, {}).get("position", "UNK")
            position_counts[position] = position_counts.get(position, 0) + int(_number(row, "n_challenges"))

        ordered_positions = sorted(position_counts.items(), key=lambda item: item[1], reverse=True)
        lead_position = ordered_positions[0][0] if ordered_positions else "UNK"
        lead_position_count = ordered_positions[0][1] if ordered_positions else 0
        role_bits = []
        for role, rows in role_rows.items():
            challenges = sum(int(_number(row, "n_challenges")) for row in rows)
            overturns = sum(int(_number(row, "n_overturns")) for row in rows)
            role_bits.append(f"{role}:{_percent_with_counts(overturns, challenges)}")
        summary = (
            f"League {year}: {' '.join(role_bits)} "
            f"[top position {lead_position}:{lead_position_count}]"
        )
        errors = [
            f"missing position for batter {row.get('player_name')}"
            for row in batter_rows
            if int(_number(row, "id")) not in position_map
        ][:1]
        return AnalyticsReport(summary=summary, untracked_errors=errors)

    async def build_umpire_report(self, name: str, year: int, game_type: str) -> AnalyticsReport:
        games = self.db.fetch_umpire_games(year, game_type)
        tracked_totals: Dict[str, int] = {}
        grouped: Dict[str, Dict[str, Any]] = {}
        for game in games:
            raw_name = str(game["umpire_name"])
            tracked = int(game["tracked_challenges"])
            key = normalize_search(raw_name)
            stats = grouped.setdefault(
                key,
                {"tracked": 0, "overturned": 0, "confirmed": 0, "names": set()},
            )
            stats["tracked"] += tracked
            stats["overturned"] += int(game["overturned"])
            stats["confirmed"] += int(game["confirmed"])
            stats["names"].add(raw_name)
            tracked_totals[raw_name] = tracked_totals.get(raw_name, 0) + tracked

        ordered = sorted(
            grouped.items(),
            key=lambda item: (
                (item[1]["confirmed"] / item[1]["tracked"]) if item[1]["tracked"] else 0.0,
                item[1]["tracked"],
            ),
            reverse=True,
        )
        matches = sorted(
            (
                item
                for item in ordered
                if max(score_match(name, candidate) for candidate in item[1]["names"]) > 0
            ),
            key=lambda item: (
                max(score_match(name, candidate) for candidate in item[1]["names"]),
                item[1]["tracked"],
            ),
            reverse=True,
        )
        if not matches:
            return AnalyticsReport(summary=f"Umpire {name}: no cached ABS data", untracked_errors=[])

        umpire_key, stats = matches[0]
        umpire_name = _umpire_display_name(sorted(stats["names"]), tracked_totals)
        rank = next((index + 1 for index, entry in enumerate(ordered) if entry[0] == umpire_key), len(ordered))
        accuracy = stats["confirmed"] / stats["tracked"] if stats["tracked"] else 0.0

        role_breakdown = self._umpire_role_breakdown(umpire_key, year, game_type)
        events = [event for event in self.db.fetch_challenge_events(year, game_type) if normalize_search(str(event.get("umpire_name") or "")) == umpire_key]
        unknown_challenges = sum(1 for event in events if event.get("outcome") == "unknown")
        audits = [audit for audit in self.db.fetch_umpire_pitch_audits(year, game_type) if normalize_search(str(audit.get("umpire_name") or "")) == umpire_key]
        called_pitches = sum(int(audit["called_pitches"]) for audit in audits)
        unchallenged_correct = sum(int(audit["unchallenged_correct"]) for audit in audits)
        unchallenged_incorrect = sum(int(audit["unchallenged_incorrect"]) for audit in audits)
        unchallenged_total = unchallenged_correct + unchallenged_incorrect
        total_correct = unchallenged_correct + stats["confirmed"]
        summary = (
            f"Umpire {umpire_name}: {percent(accuracy)} accuracy "
            f"({stats['confirmed']}/{stats['tracked']}) "
            f"[{role_breakdown}] [rank {rank}/{len(ordered)}]"
        )
        detail_lines: List[str] = []
        if called_pitches and unchallenged_total:
            unchallenged_accuracy = unchallenged_correct / unchallenged_total
            detail_lines.append(
                f"Unchallenged: {percent(unchallenged_accuracy)} [{unchallenged_correct}/{unchallenged_total}] "
                f"| total [{total_correct}/{called_pitches}]"
            )
        elif called_pitches:
            detail_lines.append(f"Unchallenged: n/a [0/0] | total [{total_correct}/{called_pitches}]")
        else:
            detail_lines.append("Unchallenged: pending nightly audit")
        if unknown_challenges:
            detail_lines.append(f"Unresolved: {unknown_challenges}")
        return AnalyticsReport(summary=summary, untracked_errors=detail_lines)

    async def build_umpire_list_report(self, year: int, game_type: str, query: Optional[str] = None) -> AnalyticsReport:
        games = self.db.fetch_umpire_games(year, game_type)
        if not games:
            return AnalyticsReport(summary=f"Umpires {year}: no cached ABS data", untracked_errors=[])

        tracked_totals: Dict[str, int] = {}
        grouped_names: Dict[str, Set[str]] = {}
        for row in games:
            raw_name = str(row["umpire_name"])
            key = normalize_search(raw_name)
            grouped_names.setdefault(key, set()).add(raw_name)
            tracked_totals[raw_name] = tracked_totals.get(raw_name, 0) + int(row["tracked_challenges"])

        names = sorted(_umpire_display_name(sorted(list(name_group)), tracked_totals) for name_group in grouped_names.values())

        selected_names = names
        if query:
            selected_names = sorted(
                (name for name in names if score_match(query, name) > 0),
                key=lambda name: score_match(query, name),
                reverse=True,
            )
            if not selected_names:
                return AnalyticsReport(summary=f"Umpires {year}: no matches for {query}", untracked_errors=[])
            summary = f"Umpires {year}: {len(selected_names)} match{'es' if len(selected_names) != 1 else ''} for {query}"
        else:
            summary = f"Umpires {year}: {len(selected_names)} cached"

        return AnalyticsReport(summary=summary, untracked_errors=_chunk_name_lines(selected_names))

    def _umpire_role_breakdown(self, umpire_key: str, year: int, game_type: str) -> str:
        events = [
            event
            for event in self.db.fetch_challenge_events(year, game_type)
            if normalize_search(str(event.get("umpire_name") or "")) == umpire_key
        ]
        role_stats: Dict[str, Dict[str, int]] = {
            "pitcher": {"count": 0, "confirmed": 0},
            "batter": {"count": 0, "confirmed": 0},
            "catcher": {"count": 0, "confirmed": 0},
        }
        for event in events:
            role = str(event.get("challenger_role") or "unknown")
            if role not in role_stats:
                continue
            role_stats[role]["count"] += 1
            if event.get("outcome") == "confirmed":
                role_stats[role]["confirmed"] += 1

        parts = []
        for role in ["pitcher", "batter", "catcher"]:
            count = role_stats[role]["count"]
            confirmed = role_stats[role]["confirmed"]
            parts.append(f"{role} {confirmed}/{count}")
        return " ".join(parts)
