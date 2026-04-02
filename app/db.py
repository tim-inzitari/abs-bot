import json
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - exercised indirectly when postgres is configured
    psycopg = None
    dict_row = None


def _dict_factory(cursor: sqlite3.Cursor, row: Tuple[Any, ...]) -> Dict[str, Any]:
    return {column[0]: row[index] for index, column in enumerate(cursor.description)}


def _is_postgres_dsn(value: str) -> bool:
    normalized = value.strip().lower()
    return normalized.startswith("postgres://") or normalized.startswith("postgresql://")


def _postgres_sql(query: str) -> str:
    return re.sub(r"\?", "%s", query)


def _leaderboard_entity_key(row: Dict[str, Any]) -> str:
    entity_id = row.get("id")
    if isinstance(entity_id, (int, float)):
        return str(int(entity_id))
    unique_id = row.get("uniqueId")
    if unique_id not in {None, ""}:
        return str(unique_id)
    return str(row.get("player_name") or "")


class _PostgresCursorAdapter:
    def __init__(self, cursor: Any, closed: bool = False) -> None:
        self._cursor = cursor
        self._closed = closed

    def fetchone(self) -> Optional[Dict[str, Any]]:
        if self._closed or self._cursor is None:
            return None
        try:
            return self._cursor.fetchone()
        finally:
            self._cursor.close()
            self._closed = True

    def fetchall(self) -> List[Dict[str, Any]]:
        if self._closed or self._cursor is None:
            return []
        try:
            return self._cursor.fetchall()
        finally:
            self._cursor.close()
            self._closed = True


class _PostgresConnectionAdapter:
    def __init__(self, dsn: str) -> None:
        if psycopg is None or dict_row is None:
            raise RuntimeError("PostgreSQL support requires psycopg to be installed")
        attempts = max(1, int(os.getenv("DATABASE_CONNECT_RETRIES", "30")))
        retry_seconds = max(0.0, float(os.getenv("DATABASE_CONNECT_RETRY_SECONDS", "2")))
        last_error: Optional[Exception] = None

        for attempt in range(1, attempts + 1):
            try:
                self._connection = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)
                break
            except Exception as error:
                if not isinstance(error, psycopg.OperationalError):
                    raise
                last_error = error
                if attempt == attempts:
                    raise
                time.sleep(retry_seconds)
        else:  # pragma: no cover - guarded by the raise above
            raise last_error or RuntimeError("PostgreSQL connection failed")

    def execute(self, query: str, params: Tuple[Any, ...] = ()) -> _PostgresCursorAdapter:
        cursor = self._connection.cursor()
        cursor.execute(_postgres_sql(query), params)
        if query.lstrip().upper().startswith("SELECT"):
            return _PostgresCursorAdapter(cursor)
        cursor.close()
        return _PostgresCursorAdapter(None, closed=True)

    def executemany(self, query: str, params_seq: Iterable[Tuple[Any, ...]]) -> None:
        with self._connection.cursor() as cursor:
            cursor.executemany(_postgres_sql(query), list(params_seq))

    def executescript(self, script: str) -> None:
        statements = [statement.strip() for statement in script.split(";") if statement.strip()]
        with self._connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)

    def commit(self) -> None:
        return None

    def close(self) -> None:
        self._connection.close()


class Database:
    def __init__(self, path: str) -> None:
        self.path = Path(path) if not _is_postgres_dsn(path) else None
        self.backend = "postgres" if _is_postgres_dsn(path) else "sqlite"
        if self.backend == "postgres":
            self.connection = _PostgresConnectionAdapter(path)
        else:
            assert self.path is not None
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.connection = sqlite3.connect(self.path, timeout=30.0)
            self.connection.row_factory = _dict_factory
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.connection.execute("PRAGMA foreign_keys=ON")
            self.connection.execute("PRAGMA busy_timeout=30000")

    def close(self) -> None:
        self.connection.close()

    def ensure_schema(self) -> None:
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )
        migrations_dir = Path(__file__).resolve().parent / "migrations"
        for migration_path in sorted(migrations_dir.glob("*.sql")):
            version = migration_path.name
            existing = self.connection.execute(
                "SELECT version FROM schema_migrations WHERE version = ?",
                (version,),
            ).fetchone()
            if existing:
                continue
            self.connection.executescript(migration_path.read_text())
            self.connection.execute(
                "INSERT INTO schema_migrations (version) VALUES (?)",
                (version,),
            )
        self.connection.commit()

    def replace_leaderboard_rows(
        self,
        year: int,
        game_type: str,
        challenge_type: str,
        rows: Iterable[Dict[str, Any]],
    ) -> None:
        self.connection.execute(
            "DELETE FROM leaderboard_rows WHERE year = ? AND game_type = ? AND challenge_type = ?",
            (year, game_type, challenge_type),
        )
        self.connection.executemany(
            """
            INSERT INTO leaderboard_rows (year, game_type, challenge_type, entity_key, entity_id, player_name, team_abbr, parent_org, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    year,
                    game_type,
                    challenge_type,
                    _leaderboard_entity_key(row),
                    int(row["id"]) if isinstance(row.get("id"), (int, float)) else None,
                    str(row.get("player_name") or ""),
                    row.get("team_abbr"),
                    row.get("parent_org"),
                    json.dumps(row),
                )
                for row in rows
            ],
        )
        self.connection.commit()

    def fetch_leaderboard_rows(self, year: int, game_type: str, challenge_type: str) -> List[Dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT raw_json
            FROM leaderboard_rows
            WHERE year = ? AND game_type = ? AND challenge_type = ?
            """,
            (year, game_type, challenge_type),
        ).fetchall()
        return [json.loads(row["raw_json"]) for row in rows]

    def replace_teams(self, year: int, teams: Iterable[Tuple[int, str, str]]) -> None:
        self.connection.execute("DELETE FROM teams WHERE year = ?", (year,))
        self.connection.executemany(
            "INSERT INTO teams (year, team_id, name, abbreviation) VALUES (?, ?, ?, ?)",
            [(year, team_id, name, abbreviation) for team_id, name, abbreviation in teams],
        )
        self.connection.commit()

    def fetch_teams(self, year: int) -> List[Dict[str, Any]]:
        return self.connection.execute(
            "SELECT team_id, name, abbreviation FROM teams WHERE year = ?",
            (year,),
        ).fetchall()

    def replace_player_positions(self, year: int, positions: Iterable[Tuple[int, str, str]]) -> None:
        self.connection.execute("DELETE FROM player_positions WHERE year = ?", (year,))
        self.connection.executemany(
            "INSERT INTO player_positions (year, person_id, full_name, position) VALUES (?, ?, ?, ?)",
            [(year, person_id, full_name, position) for person_id, full_name, position in positions],
        )
        self.connection.commit()

    def fetch_player_positions(self, year: int) -> Dict[int, Dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT person_id, full_name, position FROM player_positions WHERE year = ?",
            (year,),
        ).fetchall()
        return {int(row["person_id"]): row for row in rows}

    def upsert_games(
        self,
        year: int,
        game_type: str,
        rows: Iterable[Tuple[int, str, Optional[str], Optional[str], str, Optional[str], str]],
    ) -> None:
        self.connection.executemany(
            """
            INSERT INTO games
            (year, game_type, game_pk, official_date, away_team_name, home_team_name, matchup, home_plate_umpire, last_scanned_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(year, game_type, game_pk)
            DO UPDATE SET
                official_date=excluded.official_date,
                away_team_name=excluded.away_team_name,
                home_team_name=excluded.home_team_name,
                matchup=excluded.matchup,
                home_plate_umpire=excluded.home_plate_umpire,
                last_scanned_at=excluded.last_scanned_at
            """,
            [
                (
                    year,
                    game_type,
                    game_pk,
                    official_date,
                    away_team_name,
                    home_team_name,
                    matchup,
                    home_plate_umpire,
                    last_scanned_at,
                )
                for game_pk, official_date, away_team_name, home_team_name, matchup, home_plate_umpire, last_scanned_at in rows
            ],
        )
        self.connection.commit()

    def fetch_games(self, year: int, game_type: str) -> List[Dict[str, Any]]:
        return self.connection.execute(
            """
            SELECT *
            FROM games
            WHERE year = ? AND game_type = ?
            ORDER BY official_date, game_pk
            """,
            (year, game_type),
        ).fetchall()

    def replace_umpire_games(
        self,
        year: int,
        game_type: str,
        rows: Iterable[Tuple[int, str, str, str, int, int, int]],
    ) -> None:
        self.connection.execute(
            "DELETE FROM umpire_game_stats WHERE year = ? AND game_type = ?",
            (year, game_type),
        )
        self.connection.executemany(
            """
            INSERT INTO umpire_game_stats
            (year, game_type, game_pk, official_date, matchup, umpire_name, tracked_challenges, overturned, confirmed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (year, game_type, game_pk, official_date, matchup, umpire_name, tracked, overturned, confirmed)
                for game_pk, official_date, matchup, umpire_name, tracked, overturned, confirmed in rows
            ],
        )
        self.connection.commit()

    def upsert_umpire_games(
        self,
        year: int,
        game_type: str,
        rows: Iterable[Tuple[int, str, str, str, int, int, int]],
    ) -> None:
        self.connection.executemany(
            """
            INSERT INTO umpire_game_stats
            (year, game_type, game_pk, official_date, matchup, umpire_name, tracked_challenges, overturned, confirmed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(year, game_type, game_pk)
            DO UPDATE SET
                official_date=excluded.official_date,
                matchup=excluded.matchup,
                umpire_name=excluded.umpire_name,
                tracked_challenges=excluded.tracked_challenges,
                overturned=excluded.overturned,
                confirmed=excluded.confirmed
            """,
            [
                (year, game_type, game_pk, official_date, matchup, umpire_name, tracked, overturned, confirmed)
                for game_pk, official_date, matchup, umpire_name, tracked, overturned, confirmed in rows
            ],
        )
        self.connection.commit()

    def delete_umpire_games_for_ids(self, year: int, game_type: str, game_pks: Iterable[int]) -> None:
        game_pks = list(game_pks)
        if not game_pks:
            return
        placeholders = ",".join("?" for _ in game_pks)
        self.connection.execute(
            f"DELETE FROM umpire_game_stats WHERE year = ? AND game_type = ? AND game_pk IN ({placeholders})",
            (year, game_type, *game_pks),
        )
        self.connection.commit()

    def fetch_umpire_games(self, year: int, game_type: str) -> List[Dict[str, Any]]:
        return self.connection.execute(
            """
            SELECT game_pk, official_date, matchup, umpire_name, tracked_challenges, overturned, confirmed
            FROM umpire_game_stats
            WHERE year = ? AND game_type = ?
            """,
            (year, game_type),
        ).fetchall()

    def replace_umpire_pitch_audits(
        self,
        year: int,
        game_type: str,
        rows: Iterable[Tuple[int, str, str, str, int, int, int, int]],
    ) -> None:
        self.connection.execute(
            "DELETE FROM umpire_pitch_audit WHERE year = ? AND game_type = ?",
            (year, game_type),
        )
        self.upsert_umpire_pitch_audits(year, game_type, rows)

    def upsert_umpire_pitch_audits(
        self,
        year: int,
        game_type: str,
        rows: Iterable[Tuple[int, str, str, str, int, int, int, int]],
    ) -> None:
        self.connection.executemany(
            """
            INSERT INTO umpire_pitch_audit
            (year, game_type, game_pk, official_date, matchup, umpire_name, called_pitches, challenged_pitches, unchallenged_correct, unchallenged_incorrect)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(year, game_type, game_pk)
            DO UPDATE SET
                official_date=excluded.official_date,
                matchup=excluded.matchup,
                umpire_name=excluded.umpire_name,
                called_pitches=excluded.called_pitches,
                challenged_pitches=excluded.challenged_pitches,
                unchallenged_correct=excluded.unchallenged_correct,
                unchallenged_incorrect=excluded.unchallenged_incorrect
            """,
            [
                (
                    year,
                    game_type,
                    game_pk,
                    official_date,
                    matchup,
                    umpire_name,
                    called_pitches,
                    challenged_pitches,
                    unchallenged_correct,
                    unchallenged_incorrect,
                )
                for game_pk, official_date, matchup, umpire_name, called_pitches, challenged_pitches, unchallenged_correct, unchallenged_incorrect in rows
            ],
        )
        self.connection.commit()

    def delete_umpire_pitch_audits_for_ids(self, year: int, game_type: str, game_pks: Iterable[int]) -> None:
        game_pks = list(game_pks)
        if not game_pks:
            return
        placeholders = ",".join("?" for _ in game_pks)
        self.connection.execute(
            f"DELETE FROM umpire_pitch_audit WHERE year = ? AND game_type = ? AND game_pk IN ({placeholders})",
            (year, game_type, *game_pks),
        )
        self.connection.commit()

    def fetch_umpire_pitch_audits(self, year: int, game_type: str, umpire_name: Optional[str] = None) -> List[Dict[str, Any]]:
        if umpire_name:
            return self.connection.execute(
                """
                SELECT *
                FROM umpire_pitch_audit
                WHERE year = ? AND game_type = ? AND umpire_name = ?
                ORDER BY official_date, game_pk
                """,
                (year, game_type, umpire_name),
            ).fetchall()
        return self.connection.execute(
            """
            SELECT *
            FROM umpire_pitch_audit
            WHERE year = ? AND game_type = ?
            ORDER BY official_date, game_pk
            """,
            (year, game_type),
        ).fetchall()

    def replace_challenge_events(
        self,
        year: int,
        game_type: str,
        rows: Iterable[Tuple[int, str, str, str, Optional[int], Optional[str], Optional[str], Optional[str], str, str, str]],
    ) -> None:
        self.connection.execute(
            "DELETE FROM challenge_events WHERE year = ? AND game_type = ?",
            (year, game_type),
        )
        self.upsert_challenge_events(year, game_type, rows)

    def upsert_challenge_events(
        self,
        year: int,
        game_type: str,
        rows: Iterable[Tuple[int, str, str, str, Optional[int], Optional[str], Optional[str], Optional[str], str, str, str]],
    ) -> None:
        self.connection.executemany(
            """
            INSERT INTO challenge_events
            (year, game_type, game_pk, event_key, official_date, matchup, umpire_name, inning, batter_name, pitcher_name, catcher_name, challenger_role, outcome, description)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(year, game_type, game_pk, event_key)
            DO UPDATE SET
                official_date=excluded.official_date,
                matchup=excluded.matchup,
                umpire_name=excluded.umpire_name,
                inning=excluded.inning,
                batter_name=excluded.batter_name,
                pitcher_name=excluded.pitcher_name,
                catcher_name=excluded.catcher_name,
                challenger_role=excluded.challenger_role,
                outcome=excluded.outcome,
                description=excluded.description
            """,
            [
                (
                    year,
                    game_type,
                    game_pk,
                    event_key,
                    official_date,
                    matchup,
                    umpire_name,
                    inning,
                    batter_name,
                    pitcher_name,
                    catcher_name,
                    challenger_role,
                    outcome,
                    description,
                )
                for game_pk, event_key, official_date, matchup, umpire_name, inning, batter_name, pitcher_name, catcher_name, challenger_role, outcome, description in rows
            ],
        )
        self.connection.commit()

    def delete_challenge_events_for_ids(self, year: int, game_type: str, game_pks: Iterable[int]) -> None:
        game_pks = list(game_pks)
        if not game_pks:
            return
        placeholders = ",".join("?" for _ in game_pks)
        self.connection.execute(
            f"DELETE FROM challenge_events WHERE year = ? AND game_type = ? AND game_pk IN ({placeholders})",
            (year, game_type, *game_pks),
        )
        self.connection.commit()

    def fetch_challenge_events(self, year: int, game_type: str, umpire_name: Optional[str] = None) -> List[Dict[str, Any]]:
        if umpire_name:
            return self.connection.execute(
                """
                SELECT *
                FROM challenge_events
                WHERE year = ? AND game_type = ? AND umpire_name = ?
                ORDER BY official_date, game_pk, event_key
                """,
                (year, game_type, umpire_name),
            ).fetchall()
        return self.connection.execute(
            """
            SELECT *
            FROM challenge_events
            WHERE year = ? AND game_type = ?
            ORDER BY official_date, game_pk, event_key
            """,
            (year, game_type),
        ).fetchall()

    def replace_untracked_errors(
        self,
        year: int,
        game_type: str,
        scope: str,
        rows: Iterable[Tuple[Optional[str], Optional[str], Optional[str], str, Optional[int]]],
    ) -> None:
        self.connection.execute(
            "DELETE FROM untracked_errors WHERE year = ? AND game_type = ? AND scope = ?",
            (year, game_type, scope),
        )
        self.connection.executemany(
            """
            INSERT INTO untracked_errors
            (year, game_type, scope, entity_name, official_date, matchup, detail, game_pk)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (year, game_type, scope, entity_name, official_date, matchup, detail, game_pk)
                for entity_name, official_date, matchup, detail, game_pk in rows
            ],
        )
        self.connection.commit()

    def append_untracked_errors(
        self,
        year: int,
        game_type: str,
        scope: str,
        rows: Iterable[Tuple[Optional[str], Optional[str], Optional[str], str, Optional[int]]],
    ) -> None:
        self.connection.executemany(
            """
            INSERT INTO untracked_errors
            (year, game_type, scope, entity_name, official_date, matchup, detail, game_pk)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (year, game_type, scope, entity_name, official_date, matchup, detail, game_pk)
                for entity_name, official_date, matchup, detail, game_pk in rows
            ],
        )
        self.connection.commit()

    def delete_untracked_errors_for_games(self, year: int, game_type: str, scope: str, game_pks: Iterable[int]) -> None:
        game_pks = list(game_pks)
        if not game_pks:
            return
        placeholders = ",".join("?" for _ in game_pks)
        self.connection.execute(
            f"DELETE FROM untracked_errors WHERE year = ? AND game_type = ? AND scope = ? AND game_pk IN ({placeholders})",
            (year, game_type, scope, *game_pks),
        )
        self.connection.commit()

    def fetch_untracked_errors(self, year: int, game_type: str, scope: str, entity_name: Optional[str] = None) -> List[Dict[str, Any]]:
        if entity_name:
            return self.connection.execute(
                """
                SELECT entity_name, official_date, matchup, detail, game_pk
                FROM untracked_errors
                WHERE year = ? AND game_type = ? AND scope = ? AND entity_name = ?
                ORDER BY created_at DESC
                """,
                (year, game_type, scope, entity_name),
            ).fetchall()
        return self.connection.execute(
            """
            SELECT entity_name, official_date, matchup, detail, game_pk
            FROM untracked_errors
            WHERE year = ? AND game_type = ? AND scope = ?
            ORDER BY created_at DESC
            """,
            (year, game_type, scope),
        ).fetchall()

    def set_sync_state(self, year: int, game_type: str, sync_kind: str, synced_at: str) -> None:
        self.connection.execute(
            """
            INSERT INTO sync_state (year, game_type, sync_kind, synced_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(year, game_type, sync_kind)
            DO UPDATE SET synced_at=excluded.synced_at
            """,
            (year, game_type, sync_kind, synced_at),
        )
        self.connection.commit()

    def get_sync_state(self, year: int, game_type: str, sync_kind: str) -> Optional[str]:
        row = self.connection.execute(
            """
            SELECT synced_at
            FROM sync_state
            WHERE year = ? AND game_type = ? AND sync_kind = ?
            """,
            (year, game_type, sync_kind),
        ).fetchone()
        return None if row is None else str(row["synced_at"])
