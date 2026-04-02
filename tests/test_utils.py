from app.utils import normalize_search, percent, score_match, signed


def test_percent_formats_values_and_none() -> None:
    assert percent(0.125) == "12.5%"
    assert percent(None) == "n/a"


def test_signed_formats_values_and_none() -> None:
    assert signed(1.234) == "+1.23"
    assert signed(-0.5) == "-0.50"
    assert signed(None) == "n/a"


def test_normalize_search_removes_non_alnum() -> None:
    assert normalize_search("C.B. Buckner!") == "cbbuckner"
    assert normalize_search("Suárez") == "suarez"


def test_score_match_prefers_exact_then_prefix_then_contains() -> None:
    assert score_match("cb buckner", "CB Buckner") == 100
    assert score_match("buck", "Buckner") == 75
    assert score_match("buck", "CB Buckner") == 50
    assert score_match("zzz", "CB Buckner") == 0


def test_score_match_allows_close_fuzzy_names() -> None:
    assert score_match("CB Buckner", "C.B. Bucknor") >= 84
