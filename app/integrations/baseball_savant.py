import json
from dataclasses import dataclass
from typing import Any, Dict, List
from urllib.parse import urlencode

import aiohttp

from app.cache import TTLCache


@dataclass
class SavantLeaderboard:
    challenge_type: str
    year: int
    game_type: str
    rows: List[Dict[str, Any]]


class BaseballSavantClient:
    def __init__(self, session: aiohttp.ClientSession, ttl_seconds: int) -> None:
        self.session = session
        self.cache: TTLCache[SavantLeaderboard] = TTLCache(ttl_seconds)

    async def fetch_leaderboard(
        self,
        year: int,
        challenge_type: str,
        game_type: str,
        page_size: int = 500,
    ) -> SavantLeaderboard:
        cache_key = f"{year}:{challenge_type}:{game_type}:{page_size}"
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached

        params = {
            "year": str(year),
            "challengeType": challenge_type,
            "gameType": game_type,
            "level": "mlb",
            "page": "0",
            "pageSize": str(page_size),
            "minChal": "1",
            "minOppChal": "0",
        }
        url = f"https://baseballsavant.mlb.com/leaderboard/abs-challenges?{urlencode(params)}"
        async with self.session.get(url) as response:
            response.raise_for_status()
            page = await response.text()

        rows = parse_leaderboard_page(page)
        leaderboard = SavantLeaderboard(
            challenge_type=challenge_type,
            year=year,
            game_type=game_type,
            rows=rows,
        )
        self.cache.set(cache_key, leaderboard)
        return leaderboard


def parse_leaderboard_page(page: str) -> List[Dict[str, Any]]:
    marker = "const absData = "
    start = page.find(marker)
    if start == -1:
        raise RuntimeError("Could not find absData on the Savant page")

    after = start + len(marker)
    decoder = json.JSONDecoder()
    payload = page[after:].lstrip()
    rows, _ = decoder.raw_decode(payload)
    if not isinstance(rows, list):
        raise RuntimeError("absData payload was not a list")
    return rows
