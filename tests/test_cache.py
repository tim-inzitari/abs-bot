from app.cache import TTLCache


def test_ttl_cache_returns_none_for_missing_key() -> None:
    cache: TTLCache[str] = TTLCache(10)
    assert cache.get("missing") is None


def test_ttl_cache_expires_entries(monkeypatch) -> None:
    now = {"value": 100.0}

    def fake_time() -> float:
        return now["value"]

    monkeypatch.setattr("app.cache.time.time", fake_time)
    cache: TTLCache[str] = TTLCache(5)
    cache.set("key", "value")
    assert cache.get("key") == "value"

    now["value"] = 106.0
    assert cache.get("key") is None
