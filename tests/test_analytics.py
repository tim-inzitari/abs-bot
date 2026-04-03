from pathlib import Path

import pytest

from app.analytics import AnalyticsService
from app.db import Database


@pytest.fixture
def db(tmp_path: Path) -> Database:
    database = Database(str(tmp_path / "absbot.sqlite3"))
    database.ensure_schema()
    database.replace_leaderboard_rows(
        2026,
        "regular",
        "batter",
        [
            {"id": 1, "player_name": "Player One", "team_abbr": "DET", "n_challenges": 10, "n_overturns": 7, "rate_overturns": 0.7},
            {"id": 2, "player_name": "Player Two", "team_abbr": "COL", "n_challenges": 10, "n_overturns": 5, "rate_overturns": 0.5},
        ],
    )
    database.replace_leaderboard_rows(
        2026,
        "regular",
        "pitcher",
        [{"id": 3, "player_name": "Pitcher One", "team_abbr": "DET", "n_challenges": 8, "n_overturns": 4, "rate_overturns": 0.5}],
    )
    database.replace_leaderboard_rows(
        2026,
        "regular",
        "catcher",
        [{"id": 4, "player_name": "Catcher One", "team_abbr": "DET", "n_challenges": 12, "n_overturns": 6, "rate_overturns": 0.5}],
    )
    database.replace_leaderboard_rows(
        2026,
        "regular",
        "batting-team",
        [{
            "id": 5,
            "player_name": "Detroit Tigers",
            "team_abbr": "DET",
            "n_challenges": 20,
            "n_overturns": 12,
            "rate_overturns": 0.6,
            "n_challenges_against": 10,
            "n_overturns_against": 4,
            "rate_overturns_against": 0.4,
        }],
    )
    database.replace_player_positions(2026, [(1, "Player One", "RF"), (2, "Player Two", "C")])
    database.replace_umpire_games(2026, "regular", [(10, "Aug 23", "Rockies @ Tigers", "C.B. Bucknor", 200, 70, 130)])
    database.replace_umpire_pitch_audits(
        2026,
        "regular",
        [(10, "Aug 23", "Rockies @ Tigers", "C.B. Bucknor", 2000, 200, 1760, 40)],
    )
    database.upsert_challenge_events(
        2026,
        "regular",
        [
            (10, "a", "Aug 23", "Rockies @ Tigers", "C.B. Bucknor", 8, "Batter A", "Pitcher A", "Catcher A", "pitcher", "overturned", "pitcher challenge"),
            (10, "b", "Aug 23", "Rockies @ Tigers", "C.B. Bucknor", 8, "Batter B", "Pitcher B", "Catcher B", "pitcher", "confirmed", "pitcher challenge"),
            (10, "c", "Aug 23", "Rockies @ Tigers", "C.B. Bucknor", 8, "Batter C", "Pitcher C", "Catcher C", "batter", "overturned", "batter challenge"),
            (10, "d", "Aug 23", "Rockies @ Tigers", "C.B. Bucknor", 8, "Batter D", "Pitcher D", "Catcher D", "catcher", "confirmed", "catcher challenge"),
            (10, "e", "Aug 23", "Rockies @ Tigers", "C.B. Bucknor", 8, "Batter E", "Pitcher E", "Catcher E", "catcher", "unknown", "ambiguous challenge"),
        ],
    )
    return database


@pytest.mark.asyncio
async def test_player_report_is_short(db: Database) -> None:
    service = AnalyticsService(db)
    report = await service.build_player_report("Player One", 2026, "regular")
    assert "Player One" in report.summary
    assert "\n" not in report.summary


@pytest.mark.asyncio
async def test_player_report_handles_missing_cache(db: Database) -> None:
    service = AnalyticsService(db)
    report = await service.build_player_report("No Match", 2026, "postseason")
    assert report.summary == "Player No Match: no cached ABS data"


@pytest.mark.asyncio
async def test_player_report_requires_positive_match_score(db: Database) -> None:
    service = AnalyticsService(db)
    report = await service.build_player_report("Definitely Not Here", 2026, "regular")
    assert report.summary == "Player Definitely Not Here: no cached ABS data"


@pytest.mark.asyncio
async def test_team_report_requires_positive_match_score(db: Database) -> None:
    service = AnalyticsService(db)
    report = await service.build_team_report("Definitely Not Here", 2026, "regular")
    assert report.summary == "Team Definitely Not Here: no cached ABS data"


@pytest.mark.asyncio
async def test_team_report_includes_opponent_counts_when_available(db: Database) -> None:
    service = AnalyticsService(db)

    report = await service.build_team_report("Detroit", 2026, "regular")

    assert report.summary == "Team Detroit Tigers: batting 60.0% (12/20) fielding 50.0% (10/20) [pitcher 4/8 catcher 6/12]"


@pytest.mark.asyncio
async def test_league_report_includes_role_counts(db: Database) -> None:
    service = AnalyticsService(db)

    report = await service.build_league_report(2026, "regular")

    assert report.summary == (
        "League 2026: batter:60.0% (12/20) pitcher:50.0% (4/8) catcher:50.0% (6/12) "
        "batting-team:60.0% (12/20) [top position RF:10]"
    )


@pytest.mark.asyncio
async def test_umpire_report_has_rank_and_error_line(db: Database) -> None:
    service = AnalyticsService(db)
    report = await service.build_umpire_report("CB Buckner", 2026, "regular")
    assert "Umpire C.B. Bucknor: 65.0% accuracy (130/200)" in report.summary
    assert "rank 1/1" in report.summary
    assert "[pitcher 1/2 batter 0/1 catcher 1/2]" in report.summary
    assert report.untracked_errors
    assert report.untracked_errors[0] == "Unchallenged: 97.8% [1760/1800] | total [1890/2000]"
    assert report.untracked_errors[1] == "Unresolved: 1"


@pytest.mark.asyncio
async def test_umpire_report_falls_back_when_pitch_audit_missing(db: Database) -> None:
    service = AnalyticsService(db)
    db.delete_umpire_pitch_audits_for_ids(2026, "regular", [10])

    report = await service.build_umpire_report("CB Buckner", 2026, "regular")

    assert report.untracked_errors[0] == "Unchallenged: pending nightly audit"


@pytest.mark.asyncio
async def test_umpire_report_handles_zero_unchallenged_sample(db: Database) -> None:
    service = AnalyticsService(db)
    db.replace_umpire_pitch_audits(
        2026,
        "regular",
        [(10, "Aug 23", "Rockies @ Tigers", "C.B. Bucknor", 200, 200, 0, 0)],
    )

    report = await service.build_umpire_report("CB Buckner", 2026, "regular")

    assert report.untracked_errors[0] == "Unchallenged: n/a [0/0] | total [130/200]"


@pytest.mark.asyncio
async def test_umpire_list_report_lists_cached_umpires(db: Database) -> None:
    service = AnalyticsService(db)

    report = await service.build_umpire_list_report(2026, "regular")

    assert report.summary == "Umpires 2026: 1 cached"
    assert report.untracked_errors == ["C.B. Bucknor"]


@pytest.mark.asyncio
async def test_umpire_list_report_uses_fuzzy_matching(db: Database) -> None:
    service = AnalyticsService(db)

    report = await service.build_umpire_list_report(2026, "regular", query="CB Buckner")

    assert report.summary == "Umpires 2026: 1 match for CB Buckner"
    assert report.untracked_errors == ["C.B. Bucknor"]


@pytest.mark.asyncio
async def test_umpire_report_merges_name_variants_into_one_identity(db: Database) -> None:
    service = AnalyticsService(db)
    db.upsert_umpire_games(
        2026,
        "regular",
        [
            (11, "Aug 24", "Dodgers @ Tigers", "CB Bucknor", 0, 0, 0),
            (12, "Aug 25", "Mets @ Tigers", "C.B. Bucknor", 5, 1, 4),
        ],
    )
    db.upsert_umpire_pitch_audits(
        2026,
        "regular",
        [
            (11, "Aug 24", "Dodgers @ Tigers", "CB Bucknor", 100, 0, 80, 20),
            (12, "Aug 25", "Mets @ Tigers", "C.B. Bucknor", 120, 5, 100, 15),
        ],
    )
    db.upsert_challenge_events(
        2026,
        "regular",
        [
            (12, "f", "Aug 25", "Mets @ Tigers", "C.B. Bucknor", 3, "Batter F", "Pitcher F", "Catcher F", "pitcher", "overturned", "pitcher challenge"),
            (12, "g", "Aug 25", "Mets @ Tigers", "C.B. Bucknor", 4, "Batter G", "Pitcher G", "Catcher G", "pitcher", "confirmed", "pitcher challenge"),
        ],
    )

    report = await service.build_umpire_report("CB Bucknor", 2026, "regular")

    assert "Umpire C.B. Bucknor: 65.4% accuracy (134/205)" in report.summary
    assert "[pitcher 2/4 batter 0/1 catcher 1/2]" in report.summary
    assert report.untracked_errors[0] == "Unchallenged: 96.3% [1940/2015] | total [2074/2220]"


@pytest.mark.asyncio
async def test_league_report_emits_missing_position_error_when_needed(db: Database) -> None:
    service = AnalyticsService(db)
    db.replace_player_positions(2026, [(1, "Player One", "RF")])

    report = await service.build_league_report(2026, "regular")

    assert report.untracked_errors == ["missing position for batter Player Two"]
