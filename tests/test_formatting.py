from app.formatting import game_context_line, one_line_error, one_line_report


def test_one_line_report_returns_summary() -> None:
    assert one_line_report("hello") == "hello"


def test_one_line_error_returns_first_item_or_none() -> None:
    assert one_line_error([]) is None
    assert one_line_error(["first", "second"]) == "first"


def test_game_context_line_skips_missing_parts() -> None:
    assert game_context_line("Aug 23", "Rockies @ Tigers", "ambiguous challenge") == "Aug 23 Rockies @ Tigers ambiguous challenge"
    assert game_context_line(None, "Rockies @ Tigers", "ambiguous challenge") == "Rockies @ Tigers ambiguous challenge"
