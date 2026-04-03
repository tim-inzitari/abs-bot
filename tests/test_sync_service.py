from pathlib import Path

import pytest

from app.db import Database
from app.sync_service import SyncService


class FakeSavantClient:
    async def fetch_leaderboard(self, year: int, challenge_type: str, game_type: str):
        class Leaderboard:
            def __init__(self, rows):
                self.rows = rows

        rows = [{"id": 1, "player_name": f"{challenge_type}-name", "team_abbr": "DET", "n_challenges": 2, "n_overturns": 1}]
        return Leaderboard(rows)


class FakeTeam:
    def __init__(self, team_id: int, name: str, abbreviation: str):
        self.team_id = team_id
        self.name = name
        self.abbreviation = abbreviation


class FakePosition:
    def __init__(self, person_id: int, full_name: str, position: str):
        self.person_id = person_id
        self.full_name = full_name
        self.position = position


class FakeMlbClient:
    def __init__(self):
        self.feed_version = 0

    async def get_teams(self, season: int):
        return [FakeTeam(1, "Detroit Tigers", "DET")]

    async def get_people_positions(self, person_ids):
        return {1: FakePosition(1, "batter-name", "RF")}

    async def get_schedule_games(self, year: int, game_type: str, start_date=None, end_date=None):
        return [{"game_pk": 10, "official_date": "2026-03-31", "matchup": "Rockies @ Tigers"}]

    async def get_live_feed(self, game_pk: int):
        description = "successful challenge overturned" if self.feed_version == 0 else "unsuccessful challenge confirmed"
        return {
            "liveData": {
                "boxscore": {"officials": [{"officialType": "Home Plate", "official": {"fullName": "CB Buckner"}}]},
                "plays": {
                    "allPlays": [
                        {
                            "about": {"inning": 8},
                            "matchup": {
                                "batter": {"fullName": "Batter"},
                                "pitcher": {"fullName": "Pitcher"},
                            },
                            "playEvents": [
                                {
                                    "details": {
                                        "call": {"code": "B", "description": "Ball"},
                                        "description": "Ball",
                                        "code": "B",
                                    },
                                    "pitchData": {
                                        "coordinates": {"pX": 1.1, "pZ": 2.2},
                                        "strikeZoneTop": 3.4,
                                        "strikeZoneBottom": 1.6,
                                        "strikeZoneWidth": 1.4166666667,
                                    },
                                },
                                {
                                    "details": {
                                        "call": {"code": "C", "description": "Called Strike"},
                                        "description": "Called Strike",
                                        "code": "C",
                                    },
                                    "pitchData": {
                                        "coordinates": {"pX": 0.0, "pZ": 2.3},
                                        "strikeZoneTop": 3.4,
                                        "strikeZoneBottom": 1.6,
                                        "strikeZoneWidth": 1.4166666667,
                                    },
                                },
                                {"details": {"event": "challenge", "description": description}},
                            ],
                        }
                    ]
                },
            }
        }


@pytest.mark.asyncio
async def test_full_refresh_populates_database(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "absbot.sqlite3"))
    db.ensure_schema()
    service = SyncService(db=db, savant_client=FakeSavantClient(), mlb_stats_client=FakeMlbClient())
    await service.full_refresh(2026, "regular")
    assert db.fetch_leaderboard_rows(2026, "regular", "batter")
    assert db.fetch_umpire_games(2026, "regular")
    assert db.fetch_challenge_events(2026, "regular")
    games = db.fetch_games(2026, "regular")
    assert games[0]["matchup"] == "Rockies @ Tigers"
    assert games[0]["home_plate_umpire"] == "CB Buckner"
    audits = db.fetch_umpire_pitch_audits(2026, "regular", umpire_name="CB Buckner")
    assert audits[0]["called_pitches"] == 2
    assert audits[0]["challenged_pitches"] == 1
    assert audits[0]["unchallenged_correct"] == 1


@pytest.mark.asyncio
async def test_reconcile_date_replaces_previous_ad_hoc_result(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "absbot.sqlite3"))
    db.ensure_schema()
    mlb = FakeMlbClient()
    service = SyncService(db=db, savant_client=FakeSavantClient(), mlb_stats_client=mlb)

    await service.reconcile_date(2026, "regular", "2026-03-31", sync_kind="today", include_pitch_audit=False)
    first = db.fetch_umpire_games(2026, "regular")[0]
    assert first["overturned"] == 1
    assert first["confirmed"] == 0
    assert db.fetch_umpire_pitch_audits(2026, "regular", umpire_name="CB Buckner") == []

    mlb.feed_version = 1
    await service.reconcile_date(2026, "regular", "2026-03-31", sync_kind="reconcile")
    second = db.fetch_umpire_games(2026, "regular")[0]
    assert second["overturned"] == 0
    assert second["confirmed"] == 1
    games = db.fetch_games(2026, "regular")
    assert games[0]["home_plate_umpire"] == "CB Buckner"
    events = db.fetch_challenge_events(2026, "regular", umpire_name="CB Buckner")
    assert events[0]["outcome"] == "confirmed"
    audits = db.fetch_umpire_pitch_audits(2026, "regular", umpire_name="CB Buckner")
    assert audits[0]["called_pitches"] == 2


class MissingUmpireMlbClient(FakeMlbClient):
    async def get_live_feed(self, game_pk: int):
        return {"liveData": {"boxscore": {"officials": []}, "plays": {"allPlays": []}}}


class UnknownChallengeMlbClient(FakeMlbClient):
    async def get_live_feed(self, game_pk: int):
        return {
            "liveData": {
                "boxscore": {"officials": [{"officialType": "Home Plate", "official": {"fullName": "CB Buckner"}}]},
                "plays": {
                    "allPlays": [
                        {
                            "about": {"inning": 3},
                            "matchup": {
                                "batter": {"fullName": "Batter"},
                                "pitcher": {"fullName": "Pitcher"},
                            },
                            "playEvents": [
                                {
                                    "details": {
                                        "call": {"code": "B", "description": "Ball"},
                                        "description": "Ball",
                                        "code": "B",
                                    },
                                    "pitchData": {
                                        "coordinates": {"pX": 1.1, "pZ": 2.2},
                                        "strikeZoneTop": 3.4,
                                        "strikeZoneBottom": 1.6,
                                    },
                                },
                                {"details": {"event": "challenge", "description": "ABS challenge"}},
                            ],
                        }
                    ]
                },
            }
        }


class EmptyScheduleMlbClient(FakeMlbClient):
    async def get_schedule_games(self, year: int, game_type: str, start_date=None, end_date=None):
        return []


class RecordingScheduleMlbClient(FakeMlbClient):
    def __init__(self):
        super().__init__()
        self.calls = []

    async def get_schedule_games(self, year: int, game_type: str, start_date=None, end_date=None):
        self.calls.append((year, game_type, start_date, end_date))
        return await super().get_schedule_games(year, game_type, start_date, end_date)


class PostponedScheduleMlbClient(FakeMlbClient):
    def __init__(self):
        super().__init__()
        self.scanned_game_pks = []

    async def get_schedule_games(self, year: int, game_type: str, start_date=None, end_date=None):
        return [
            {
                "game_pk": 10,
                "official_date": "2026-03-31",
                "reschedule_date": None,
                "away_team_name": "Rockies",
                "home_team_name": "Tigers",
                "matchup": "Rockies @ Tigers",
                "detailed_state": "Final",
                "abstract_game_state": "Final",
                "coded_game_state": "F",
            },
            {
                "game_pk": 11,
                "official_date": "2026-04-01",
                "reschedule_date": "2026-04-01T18:10:00Z",
                "away_team_name": "Blue Jays",
                "home_team_name": "White Sox",
                "matchup": "Blue Jays @ White Sox",
                "detailed_state": "Postponed",
                "abstract_game_state": "Final",
                "coded_game_state": "D",
            },
        ]

    async def get_live_feed(self, game_pk: int):
        self.scanned_game_pks.append(game_pk)
        return await super().get_live_feed(game_pk)


class BoxscoreSummaryMlbClient(FakeMlbClient):
    async def get_live_feed(self, game_pk: int):
        return {
            "gameData": {
                "players": {
                    "ID1": {
                        "fullName": "Will Benson",
                        "boxscoreName": "Benson",
                        "lastInitName": "Benson, W",
                        "lastName": "Benson",
                        "primaryPosition": {"abbreviation": "CF"},
                    },
                    "ID2": {
                        "fullName": "Tyler Stephenson",
                        "boxscoreName": "Stephenson",
                        "lastInitName": "Stephenson, T",
                        "lastName": "Stephenson",
                        "primaryPosition": {"abbreviation": "C"},
                    },
                }
            },
            "liveData": {
                "boxscore": {
                    "officials": [{"officialType": "Home Plate", "official": {"fullName": "CB Bucknor"}}],
                    "info": [
                        {
                            "label": "ABS Challenge",
                            "value": "Benson 2 (Strike-Overturned to Ball, Ball-Confirmed); Stephenson (Strike-Confirmed).",
                        }
                    ],
                },
                "plays": {
                    "allPlays": [
                        {
                            "about": {"inning": 1},
                            "matchup": {
                                "batter": {"fullName": "Will Benson"},
                                "pitcher": {"fullName": "Pitcher"},
                                "catcher": {"fullName": "Tyler Stephenson"},
                            },
                            "playEvents": [
                                {
                                    "details": {
                                        "call": {"code": "B", "description": "Ball"},
                                        "description": "Ball",
                                        "code": "B",
                                    },
                                    "pitchData": {
                                        "coordinates": {"pX": 0.0, "pZ": 2.5},
                                        "strikeZoneTop": 3.4,
                                        "strikeZoneBottom": 1.6,
                                        "strikeZoneWidth": 1.4166666667,
                                    },
                                },
                                {
                                    "details": {
                                        "call": {"code": "C", "description": "Called Strike"},
                                        "description": "Called Strike",
                                        "code": "C",
                                    },
                                    "pitchData": {
                                        "coordinates": {"pX": 0.0, "pZ": 2.3},
                                        "strikeZoneTop": 3.4,
                                        "strikeZoneBottom": 1.6,
                                        "strikeZoneWidth": 1.4166666667,
                                    },
                                },
                                {
                                    "details": {
                                        "call": {"code": "C", "description": "Called Strike"},
                                        "description": "Called Strike",
                                        "code": "C",
                                    },
                                    "pitchData": {
                                        "coordinates": {"pX": 0.0, "pZ": 2.4},
                                        "strikeZoneTop": 3.4,
                                        "strikeZoneBottom": 1.6,
                                        "strikeZoneWidth": 1.4166666667,
                                    },
                                },
                            ],
                        }
                    ]
                },
            },
        }


@pytest.mark.asyncio
async def test_full_refresh_records_missing_umpire_error(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "absbot.sqlite3"))
    db.ensure_schema()
    service = SyncService(db=db, savant_client=FakeSavantClient(), mlb_stats_client=MissingUmpireMlbClient())

    await service.full_refresh(2026, "regular")

    errors = db.fetch_untracked_errors(2026, "regular", "umpire")
    assert errors[0]["detail"] == "missing home plate umpire assignment"
    assert db.fetch_umpire_games(2026, "regular") == []


@pytest.mark.asyncio
async def test_unknown_challenge_is_preserved_and_logged(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "absbot.sqlite3"))
    db.ensure_schema()
    service = SyncService(db=db, savant_client=FakeSavantClient(), mlb_stats_client=UnknownChallengeMlbClient())

    await service.reconcile_date(2026, "regular", "2026-03-31", sync_kind="reconcile")

    events = db.fetch_challenge_events(2026, "regular", umpire_name="CB Buckner")
    errors = db.fetch_untracked_errors(2026, "regular", "umpire", entity_name="CB Buckner")
    assert events[0]["outcome"] == "unknown"
    assert "ambiguous challenge" in errors[0]["detail"]


@pytest.mark.asyncio
async def test_reconcile_date_with_no_games_still_updates_sync_state(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "absbot.sqlite3"))
    db.ensure_schema()
    service = SyncService(db=db, savant_client=FakeSavantClient(), mlb_stats_client=EmptyScheduleMlbClient())

    await service.reconcile_date(2026, "regular", "2026-09-01", sync_kind="sweep")

    assert db.get_sync_state(2026, "regular", "sweep") is not None


@pytest.mark.asyncio
async def test_reconcile_date_skips_cross_season_target_date(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "absbot.sqlite3"))
    db.ensure_schema()
    mlb = RecordingScheduleMlbClient()
    service = SyncService(db=db, savant_client=FakeSavantClient(), mlb_stats_client=mlb)

    await service.reconcile_date(2025, "regular", "2026-08-23", sync_kind="today")

    assert mlb.calls == []
    assert db.fetch_umpire_games(2025, "regular") == []
    assert db.get_sync_state(2025, "regular", "today") is not None


@pytest.mark.asyncio
async def test_reconcile_date_skips_postponed_or_rescheduled_games(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "absbot.sqlite3"))
    db.ensure_schema()
    db.upsert_umpire_games(2026, "regular", [(11, "2026-03-31", "Blue Jays @ White Sox", "CB Buckner", 2, 1, 1)])
    db.upsert_challenge_events(
        2026,
        "regular",
        [(11, "stale", "2026-03-31", "Blue Jays @ White Sox", "CB Buckner", 1, "Batter", "Pitcher", None, "batter", "confirmed", "stale")],
    )
    db.upsert_umpire_pitch_audits(
        2026,
        "regular",
        [(11, "2026-03-31", "Blue Jays @ White Sox", "CB Buckner", 20, 2, 15, 3)],
    )
    mlb = PostponedScheduleMlbClient()
    service = SyncService(db=db, savant_client=FakeSavantClient(), mlb_stats_client=mlb)

    await service.reconcile_date(2026, "regular", "2026-03-31", sync_kind="reconcile")

    assert mlb.scanned_game_pks == [10]
    games = db.fetch_games(2026, "regular")
    assert any(row["game_pk"] == 11 and row["official_date"] == "2026-04-01" for row in games)
    assert all(row["game_pk"] != 11 for row in db.fetch_umpire_games(2026, "regular"))
    assert all(row["game_pk"] != 11 for row in db.fetch_challenge_events(2026, "regular"))
    assert all(row["game_pk"] != 11 for row in db.fetch_umpire_pitch_audits(2026, "regular"))


@pytest.mark.asyncio
async def test_boxscore_summary_challenges_are_counted_and_adjust_audit(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "absbot.sqlite3"))
    db.ensure_schema()
    service = SyncService(db=db, savant_client=FakeSavantClient(), mlb_stats_client=BoxscoreSummaryMlbClient())

    await service.reconcile_date(2026, "regular", "2026-03-31", sync_kind="reconcile")

    games = db.fetch_umpire_games(2026, "regular")
    assert games[0]["umpire_name"] == "CB Bucknor"
    assert games[0]["tracked_challenges"] == 3
    assert games[0]["overturned"] == 1
    assert games[0]["confirmed"] == 2

    events = db.fetch_challenge_events(2026, "regular", umpire_name="CB Bucknor")
    assert len(events) == 3
    assert [event["challenger_role"] for event in events] == ["batter", "batter", "catcher"]
    assert [event["outcome"] for event in events] == ["overturned", "confirmed", "confirmed"]

    audits = db.fetch_umpire_pitch_audits(2026, "regular", umpire_name="CB Bucknor")
    assert audits[0]["called_pitches"] == 3
    assert audits[0]["challenged_pitches"] == 3
    assert audits[0]["unchallenged_correct"] == 0
    assert audits[0]["unchallenged_incorrect"] == 0


@pytest.mark.asyncio
async def test_integrity_sweep_skips_yesterday_duplicate(monkeypatch, tmp_path: Path) -> None:
    db = Database(str(tmp_path / "absbot.sqlite3"))
    db.ensure_schema()
    service = SyncService(db=db, savant_client=FakeSavantClient(), mlb_stats_client=FakeMlbClient())
    requested_dates = []

    async def fake_reconcile_date(year: int, game_type: str, target_date: str, sync_kind: str = "reconcile", include_pitch_audit: bool = True) -> None:
        requested_dates.append((target_date, sync_kind, include_pitch_audit))

    class FrozenDateTime:
        @staticmethod
        def now(tz=None):
            import datetime as dt
            return dt.datetime(2026, 8, 25, 12, 0, tzinfo=tz)

        @staticmethod
        def utcnow():
            import datetime as dt
            return dt.datetime(2026, 8, 25, 16, 0)

    monkeypatch.setattr("app.sync_service.datetime", FrozenDateTime)
    monkeypatch.setattr(service, "reconcile_date", fake_reconcile_date)

    await service.integrity_sweep(2026, "regular", 3)

    assert requested_dates == [
        ("2026-08-23", "sweep", True),
        ("2026-08-22", "sweep", True),
    ]


@pytest.mark.asyncio
async def test_manual_full_refresh_runs_historical_then_today(monkeypatch, tmp_path: Path) -> None:
    db = Database(str(tmp_path / "absbot.sqlite3"))
    db.ensure_schema()
    service = SyncService(db=db, savant_client=FakeSavantClient(), mlb_stats_client=FakeMlbClient())
    calls = []

    async def fake_full_refresh(year: int, game_type: str) -> None:
        calls.append(("full", year, game_type))

    async def fake_refresh_today(year: int, game_type: str) -> None:
        calls.append(("today", year, game_type))

    monkeypatch.setattr(service, "full_refresh", fake_full_refresh)
    monkeypatch.setattr(service, "refresh_today", fake_refresh_today)

    await service.manual_full_refresh(2026, "regular")

    assert calls == [
        ("full", 2026, "regular"),
        ("today", 2026, "regular"),
    ]


@pytest.mark.asyncio
async def test_full_refresh_emits_progress_messages(tmp_path: Path) -> None:
    db = Database(str(tmp_path / "absbot.sqlite3"))
    db.ensure_schema()
    messages = []
    service = SyncService(
        db=db,
        savant_client=FakeSavantClient(),
        mlb_stats_client=FakeMlbClient(),
        progress=messages.append,
    )

    await service.full_refresh(2026, "regular")

    assert any("Full refresh: fetching Savant leaderboards" in message for message in messages)
    assert any("Full refresh: scanning 1 historical games" in message for message in messages)
    assert any("Full refresh complete: 2026 regular" in message for message in messages)
