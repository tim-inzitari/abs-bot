import pytest

from app.integrations.mlb_stats import MlbStatsClient, get_home_plate_umpire


class FakeJsonResponse:
    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    def raise_for_status(self) -> None:
        return None

    async def json(self):
        return self.payload


class FakeSession:
    def __init__(self, payloads):
        self.payloads = payloads
        self.calls = []

    def get(self, url: str):
        self.calls.append(url)
        return FakeJsonResponse(self.payloads[url])


def test_get_home_plate_umpire_returns_none_without_assignment() -> None:
    assert get_home_plate_umpire({"liveData": {"boxscore": {"officials": []}}}) is None


@pytest.mark.asyncio
async def test_get_schedule_games_formats_matchup() -> None:
    url = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&gameType=R&season=2026"
    session = FakeSession(
        {
            url: {
                "dates": [
                    {
                        "date": "2026-08-23",
                        "games": [
                            {
                                "gamePk": 10,
                                "teams": {
                                    "away": {"team": {"name": "Rockies"}},
                                    "home": {"team": {"name": "Tigers"}},
                                },
                            }
                        ],
                    }
                ]
            }
        }
    )
    client = MlbStatsClient(session, ttl_seconds=60)

    games = await client.get_schedule_games(2026, "regular")

    assert games == [
        {
            "game_pk": 10,
            "official_date": "2026-08-23",
            "away_team_name": "Rockies",
            "home_team_name": "Tigers",
            "matchup": "Rockies @ Tigers",
        }
    ]


@pytest.mark.asyncio
async def test_get_live_feed_uses_cache() -> None:
    url = "https://statsapi.mlb.com/api/v1.1/game/10/feed/live"
    session = FakeSession({url: {"gamePk": 10}})
    client = MlbStatsClient(session, ttl_seconds=60)

    first = await client.get_live_feed(10)
    second = await client.get_live_feed(10)

    assert first == {"gamePk": 10}
    assert second == first
    assert session.calls == [url]


@pytest.mark.asyncio
async def test_get_people_positions_uses_cache() -> None:
    url = "https://statsapi.mlb.com/api/v1/people?personIds=1,2"
    session = FakeSession(
        {
            url: {
                "people": [
                    {"id": 1, "fullName": "Player One", "primaryPosition": {"abbreviation": "RF"}},
                    {"id": 2, "fullName": "Player Two", "primaryPosition": {"abbreviation": "C"}},
                ]
            }
        }
    )
    client = MlbStatsClient(session, ttl_seconds=60)

    first = await client.get_people_positions([1, 2])
    second = await client.get_people_positions([2, 1, 2])

    assert set(first) == {1, 2}
    assert second[1].position == "RF"
    assert session.calls == [url]
