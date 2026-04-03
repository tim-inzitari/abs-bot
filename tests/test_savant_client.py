import pytest

from app.integrations.baseball_savant import BaseballSavantClient, parse_leaderboard_page


class FakeTextResponse:
    def __init__(self, text: str) -> None:
        self._text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    def raise_for_status(self) -> None:
        return None

    async def text(self) -> str:
        return self._text


class FakeSession:
    def __init__(self, text: str) -> None:
        self.text = text
        self.calls = []

    def get(self, url: str) -> FakeTextResponse:
        self.calls.append(url)
        return FakeTextResponse(self.text)


def test_parse_leaderboard_page_handles_embedded_payload_with_trailing_js() -> None:
    page = 'before const absData = [{"player_name":"A","id":1}]; window.moreStuff = true;'
    rows = parse_leaderboard_page(page)
    assert rows == [{"player_name": "A", "id": 1}]


def test_parse_leaderboard_page_rejects_non_list_payload() -> None:
    with pytest.raises(RuntimeError, match="absData payload was not a list"):
        parse_leaderboard_page('const absData = {"player_name":"A"};')


@pytest.mark.asyncio
async def test_fetch_leaderboard_uses_cache() -> None:
    session = FakeSession('const absData = [{"player_name":"A","id":1}];')
    client = BaseballSavantClient(session, ttl_seconds=60)

    first = await client.fetch_leaderboard(2026, "batter", "regular")
    second = await client.fetch_leaderboard(2026, "batter", "regular")

    assert first.rows == [{"player_name": "A", "id": 1}]
    assert second.rows == first.rows
    assert len(session.calls) == 1


@pytest.mark.asyncio
async def test_fetch_leaderboard_force_refresh_bypasses_cache() -> None:
    class RollingSession:
        def __init__(self) -> None:
            self.calls = []
            self.payloads = [
                'const absData = [{"player_name":"A","id":1,"n_challenges":1}];',
                'const absData = [{"player_name":"A","id":1,"n_challenges":2}];',
            ]

        def get(self, url: str) -> FakeTextResponse:
            self.calls.append(url)
            return FakeTextResponse(self.payloads.pop(0))

    session = RollingSession()
    client = BaseballSavantClient(session, ttl_seconds=60)

    first = await client.fetch_leaderboard(2026, "batter", "regular")
    second = await client.fetch_leaderboard(2026, "batter", "regular", force_refresh=True)

    assert first.rows[0]["n_challenges"] == 1
    assert second.rows[0]["n_challenges"] == 2
    assert len(session.calls) == 2
