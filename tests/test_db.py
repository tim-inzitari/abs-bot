import sqlite3
from pathlib import Path

import pytest

import app.db as db_module
from app.db import Database


def test_database_schema_and_round_trip(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "absbot.sqlite3"))
    db.ensure_schema()
    db.replace_leaderboard_rows(2026, "regular", "batter", [{"id": 1, "player_name": "A", "n_challenges": 2, "n_overturns": 1}])
    rows = db.fetch_leaderboard_rows(2026, "regular", "batter")
    assert len(rows) == 1
    assert rows[0]["player_name"] == "A"
    migrations = db.connection.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
    assert [row["version"] for row in migrations] == [
        "001_initial.sql",
        "002_challenge_events.sql",
        "003_umpire_pitch_audit.sql",
        "004_leaderboard_entity_key.sql",
        "005_games.sql",
    ]
    db.close()


def test_database_sets_busy_timeout(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "absbot.sqlite3"))
    timeout = db.connection.execute("PRAGMA busy_timeout").fetchone()["timeout"]
    assert timeout == 30000
    db.close()


def test_umpire_pitch_audit_round_trip_and_delete(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "absbot.sqlite3"))
    db.ensure_schema()
    db.upsert_umpire_pitch_audits(2026, "regular", [(10, "2026-08-23", "Rockies @ Tigers", "CB Buckner", 200, 10, 180, 10)])

    rows = db.fetch_umpire_pitch_audits(2026, "regular", umpire_name="CB Buckner")

    assert rows[0]["called_pitches"] == 200
    assert rows[0]["unchallenged_incorrect"] == 10

    db.delete_umpire_pitch_audits_for_ids(2026, "regular", [10])
    assert db.fetch_umpire_pitch_audits(2026, "regular") == []
    db.close()


def test_untracked_errors_filter_by_entity(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "absbot.sqlite3"))
    db.ensure_schema()
    db.append_untracked_errors(
        2026,
        "regular",
        "umpire",
        [
            ("CB Buckner", "2026-08-23", "Rockies @ Tigers", "ambiguous challenge", 10),
            ("Angel Hernandez", "2026-08-24", "Yankees @ Red Sox", "missing assignment", 11),
        ],
    )

    rows = db.fetch_untracked_errors(2026, "regular", "umpire", entity_name="CB Buckner")

    assert len(rows) == 1
    assert rows[0]["detail"] == "ambiguous challenge"
    db.close()


def test_sync_state_round_trip(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "absbot.sqlite3"))
    db.ensure_schema()
    db.set_sync_state(2026, "regular", "today", "2026-04-01T03:00:00")
    assert db.get_sync_state(2026, "regular", "today") == "2026-04-01T03:00:00"
    db.close()


def test_games_round_trip(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "absbot.sqlite3"))
    db.ensure_schema()
    db.upsert_games(
        2026,
        "regular",
        [(10, "2026-03-31", "Rockies", "Tigers", "Rockies @ Tigers", "CB Buckner", "2026-04-01T03:00:00")],
    )

    rows = db.fetch_games(2026, "regular")

    assert rows[0]["away_team_name"] == "Rockies"
    assert rows[0]["home_team_name"] == "Tigers"
    assert rows[0]["home_plate_umpire"] == "CB Buckner"
    db.close()


def test_leaderboard_rows_allow_duplicate_names_with_distinct_ids(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "absbot.sqlite3"))
    db.ensure_schema()
    db.replace_leaderboard_rows(
        2026,
        "regular",
        "batter",
        [
            {"id": 1, "player_name": "Same Name", "team_abbr": "DET", "n_challenges": 2, "n_overturns": 1},
            {"id": 2, "player_name": "Same Name", "team_abbr": "COL", "n_challenges": 4, "n_overturns": 3},
        ],
    )

    rows = db.fetch_leaderboard_rows(2026, "regular", "batter")

    assert len(rows) == 2
    assert sorted(row["team_abbr"] for row in rows) == ["COL", "DET"]
    db.close()


def test_legacy_leaderboard_table_migrates_to_entity_key_schema(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    connection.execute(
        """
        CREATE TABLE leaderboard_rows (
            year INTEGER NOT NULL,
            game_type TEXT NOT NULL,
            challenge_type TEXT NOT NULL,
            entity_id INTEGER,
            player_name TEXT NOT NULL,
            team_abbr TEXT,
            parent_org TEXT,
            raw_json TEXT NOT NULL,
            PRIMARY KEY (year, game_type, challenge_type, player_name)
        )
        """
    )
    connection.execute(
        """
        INSERT INTO leaderboard_rows (year, game_type, challenge_type, entity_id, player_name, team_abbr, parent_org, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (2026, "regular", "batter", 1, "Legacy Name", "DET", None, '{"id": 1, "player_name": "Legacy Name"}'),
    )
    for version in ["001_initial.sql", "002_challenge_events.sql", "003_umpire_pitch_audit.sql"]:
        connection.execute("INSERT INTO schema_migrations (version) VALUES (?)", (version,))
    connection.commit()
    connection.close()

    db = Database(str(path))
    db.ensure_schema()

    rows = db.fetch_leaderboard_rows(2026, "regular", "batter")
    columns = {row["name"] for row in db.connection.execute("PRAGMA table_info(leaderboard_rows)").fetchall()}

    assert rows[0]["player_name"] == "Legacy Name"
    assert "entity_key" in columns
    db.close()


def test_is_postgres_dsn_detects_postgres_urls() -> None:
    assert db_module._is_postgres_dsn("postgresql://absbot:secret@db:5432/absbot") is True
    assert db_module._is_postgres_dsn("postgres://absbot:secret@db:5432/absbot") is True
    assert db_module._is_postgres_dsn("/tmp/absbot.sqlite3") is False


def test_database_rejects_postgres_without_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(db_module, "psycopg", None)
    monkeypatch.setattr(db_module, "dict_row", None)

    with pytest.raises(RuntimeError, match="psycopg"):
        Database("postgresql://absbot:secret@db:5432/absbot")


def test_database_retries_postgres_connection_until_available(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = {"count": 0}
    sleeps = []

    class FakeOperationalError(Exception):
        pass

    class FakeConnection:
        def close(self) -> None:
            return None

    class FakePsycopg:
        OperationalError = FakeOperationalError

        @staticmethod
        def connect(dsn: str, autocommit: bool, row_factory) -> FakeConnection:
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise FakeOperationalError("connection refused")
            return FakeConnection()

    monkeypatch.setattr(db_module, "psycopg", FakePsycopg)
    monkeypatch.setattr(db_module, "dict_row", object())
    monkeypatch.setattr(db_module.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setenv("DATABASE_CONNECT_RETRIES", "3")
    monkeypatch.setenv("DATABASE_CONNECT_RETRY_SECONDS", "0.25")

    db = Database("postgresql://absbot:secret@db:5432/absbot")

    assert attempts["count"] == 3
    assert sleeps == [0.25, 0.25]
    db.close()
