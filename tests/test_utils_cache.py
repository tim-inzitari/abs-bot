from app.cache import TTLCache
from app.utils import normalize_search, percent, score_match, signed


def test_percent_formats_none_and_value() -> None:
    assert percent(None) == "n/a"
    assert percent(0.125) == "12.5%"
    assert percent(0.125, digits=2) == "12.50%"


def test_signed_formats_none_and_value() -> None:
    assert signed(None) == "n/a"
    assert signed(1.234) == "+1.23"
    assert signed(-1.234, digits=1) == "-1.2"


def test_normalize_search_strips_non_alphanumeric() -> None:
    assert normalize_search("C.B. Buckner!") == "cbbuckner"


def test_score_match_orders_exact_prefix_and_substring() -> None:
    assert score_match("cb buckner", "CB Buckner") == 100
    assert score_match("buck", "Buckner") == 75
    assert score_match("uckn", "Buckner") == 50
    assert score_match("x", "Buckner") == 0


def test_ttl_cache_returns_value_before_expiry(monkeypatch) -> None:
    current_time = [100.0]
    monkeypatch.setattr("app.cache.time.time", lambda: current_time[0])
    cache = TTLCache[str](ttl_seconds=5)

    cache.set("key", "value")
    current_time[0] = 104.0

    assert cache.get("key") == "value"


def test_ttl_cache_evicts_expired_values(monkeypatch) -> None:
    current_time = [100.0]
    monkeypatch.setattr("app.cache.time.time", lambda: current_time[0])
    cache = TTLCache[str](ttl_seconds=5)

    cache.set("key", "value")
    current_time[0] = 106.0

    assert cache.get("key") is None
    assert cache.get("missing") is None
