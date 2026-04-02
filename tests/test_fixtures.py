import json
from pathlib import Path

from app.integrations.baseball_savant import parse_leaderboard_page
from app.integrations.mlb_stats import get_home_plate_umpire


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_real_savant_fixture_parses_rows() -> None:
    page = (FIXTURES / "savant_abs_page.html").read_text()
    rows = parse_leaderboard_page(page)
    assert rows
    assert "player_name" in rows[0]


def test_real_mlb_fixture_parses_home_plate_umpire() -> None:
    payload = json.loads((FIXTURES / "mlb_live_feed_trimmed.json").read_text())
    umpire = get_home_plate_umpire(payload)
    assert umpire is not None
