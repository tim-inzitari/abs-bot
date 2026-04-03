from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import aiohttp

from app.cache import TTLCache


@dataclass
class TeamDirectoryEntry:
    team_id: int
    name: str
    abbreviation: str


@dataclass
class PlayerPositionEntry:
    person_id: int
    full_name: str
    position: str


class MlbStatsClient:
    def __init__(self, session: aiohttp.ClientSession, ttl_seconds: int) -> None:
        self.session = session
        self.team_cache: TTLCache[List[TeamDirectoryEntry]] = TTLCache(ttl_seconds)
        self.people_cache: TTLCache[Dict[int, PlayerPositionEntry]] = TTLCache(ttl_seconds)
        self.live_feed_cache: TTLCache[Dict[str, Any]] = TTLCache(ttl_seconds)

    async def get_teams(self, season: int) -> List[TeamDirectoryEntry]:
        cache_key = str(season)
        cached = self.team_cache.get(cache_key)
        if cached is not None:
            return cached

        url = f"https://statsapi.mlb.com/api/v1/teams?sportId=1&season={season}"
        async with self.session.get(url) as response:
            response.raise_for_status()
            payload = await response.json()

        teams = [
            TeamDirectoryEntry(
                team_id=team["id"],
                name=team["name"],
                abbreviation=team["abbreviation"],
            )
            for team in payload.get("teams", [])
        ]
        self.team_cache.set(cache_key, teams)
        return teams

    async def get_people_positions(self, person_ids: List[int]) -> Dict[int, PlayerPositionEntry]:
        unique_ids = sorted({person_id for person_id in person_ids if person_id})
        cache_key = ",".join(str(person_id) for person_id in unique_ids)
        cached = self.people_cache.get(cache_key)
        if cached is not None:
            return cached

        result: Dict[int, PlayerPositionEntry] = {}
        batch_size = 50
        for start in range(0, len(unique_ids), batch_size):
            batch = unique_ids[start : start + batch_size]
            url = f"https://statsapi.mlb.com/api/v1/people?personIds={','.join(str(person_id) for person_id in batch)}"
            async with self.session.get(url) as response:
                response.raise_for_status()
                payload = await response.json()

            for person in payload.get("people", []):
                result[person["id"]] = PlayerPositionEntry(
                    person_id=person["id"],
                    full_name=person["fullName"],
                    position=person.get("primaryPosition", {}).get("abbreviation", "UNK"),
                )

        self.people_cache.set(cache_key, result)
        return result

    async def get_schedule_game_pks(self, year: int, game_type: str) -> List[int]:
        code = {"spring": "S", "regular": "R", "postseason": "F"}[game_type]
        url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&season={year}&gameType={code}"
        async with self.session.get(url) as response:
            response.raise_for_status()
            payload = await response.json()

        return [
            game["gamePk"]
            for date in payload.get("dates", [])
            for game in date.get("games", [])
        ]

    async def get_schedule_games(
        self,
        year: int,
        game_type: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        code = {"spring": "S", "regular": "R", "postseason": "F"}[game_type]
        query_parts = [f"sportId=1", f"gameType={code}"]
        if start_date and end_date:
            query_parts.extend([f"startDate={start_date}", f"endDate={end_date}"])
        else:
            query_parts.append(f"season={year}")
        url = f"https://statsapi.mlb.com/api/v1/schedule?{'&'.join(query_parts)}"
        async with self.session.get(url) as response:
            response.raise_for_status()
            payload = await response.json()

        games: List[Dict[str, Any]] = []
        for date in payload.get("dates", []):
            for game in date.get("games", []):
                away = game.get("teams", {}).get("away", {}).get("team", {}).get("name", "Away")
                home = game.get("teams", {}).get("home", {}).get("team", {}).get("name", "Home")
                games.append(
                    {
                        "game_pk": game["gamePk"],
                        "official_date": game.get("officialDate") or date.get("date"),
                        "reschedule_date": game.get("rescheduleDate"),
                        "away_team_name": away,
                        "home_team_name": home,
                        "matchup": f"{away} @ {home}",
                        "detailed_state": game.get("status", {}).get("detailedState", ""),
                        "abstract_game_state": game.get("status", {}).get("abstractGameState", ""),
                        "coded_game_state": game.get("status", {}).get("codedGameState", ""),
                    }
                )
        return games

    async def get_live_feed(self, game_pk: int) -> Dict[str, Any]:
        cache_key = str(game_pk)
        cached = self.live_feed_cache.get(cache_key)
        if cached is not None:
            return cached

        url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
        async with self.session.get(url) as response:
            response.raise_for_status()
            payload = await response.json()

        self.live_feed_cache.set(cache_key, payload)
        return payload


def get_home_plate_umpire(feed: Dict[str, Any]) -> Optional[str]:
    officials = feed.get("liveData", {}).get("boxscore", {}).get("officials", [])
    for official in officials:
        official_type = (official.get("officialType") or "").lower()
        if "home plate" in official_type:
            return official.get("official", {}).get("fullName")
    return None
