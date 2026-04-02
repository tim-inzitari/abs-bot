import pytest

from app.integrations.baseball_savant import parse_leaderboard_page
from app.integrations.mlb_stats import get_home_plate_umpire
from app.sync_service import (
    _called_pitch_code,
    _called_pitch_is_correct,
    _is_same_season,
    parse_challenge_event,
    parse_challenger_role,
)


def test_parse_leaderboard_page_raises_without_marker() -> None:
    with pytest.raises(RuntimeError, match="absData"):
        parse_leaderboard_page("<html></html>")


def test_get_home_plate_umpire_returns_none_when_missing() -> None:
    assert get_home_plate_umpire({"liveData": {"boxscore": {"officials": []}}}) is None


def test_parse_challenge_event_ignores_generic_replay_challenge() -> None:
    assert parse_challenge_event("manager challenge confirmed at first base") == "not-challenge"


def test_parse_challenge_event_detects_abs_phrases() -> None:
    assert parse_challenge_event("pitcher challenge successful challenge overturned") == "overturned"
    assert parse_challenge_event("automatic strike unsuccessful challenge confirmed") == "confirmed"


def test_parse_challenger_role_accepts_the_variants() -> None:
    assert parse_challenger_role("challenged by the pitcher") == "pitcher"
    assert parse_challenger_role("challenged by the catcher") == "catcher"
    assert parse_challenger_role("challenged by the batter") == "batter"


def test_called_pitch_code_prefers_nested_call_code() -> None:
    event = {"details": {"call": {"code": "B"}, "code": "C"}}
    assert _called_pitch_code(event) == "B"


def test_called_pitch_is_correct_for_ball_and_called_strike() -> None:
    called_ball = {
        "details": {"call": {"code": "B"}},
        "pitchData": {
            "coordinates": {"pX": 1.2, "pZ": 2.4},
            "strikeZoneTop": 3.5,
            "strikeZoneBottom": 1.5,
            "strikeZoneWidth": 1.4166666667,
        },
    }
    called_strike = {
        "details": {"call": {"code": "C"}},
        "pitchData": {
            "coordinates": {"pX": 0.0, "pZ": 2.4},
            "strikeZoneTop": 3.5,
            "strikeZoneBottom": 1.5,
            "strikeZoneWidth": 1.4166666667,
        },
    }
    assert _called_pitch_is_correct(called_ball) is True
    assert _called_pitch_is_correct(called_strike) is True


def test_called_pitch_is_correct_returns_none_without_geometry() -> None:
    assert _called_pitch_is_correct({"details": {"call": {"code": "B"}}, "pitchData": {}}) is None


def test_is_same_season_validates_target_date_year() -> None:
    assert _is_same_season("2026-08-23", 2026) is True
    assert _is_same_season("2025-08-23", 2026) is False
