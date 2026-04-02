import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    discord_token: str
    discord_client_id: str
    discord_guild_id: str
    default_season: int
    default_game_type: str
    cache_ttl_seconds: int
    database_path: str
    daily_update_hour_eastern: int
    integrity_sweep_days: int


def load_settings() -> Settings:
    load_dotenv()

    app_role = os.getenv("APP_ROLE", "bot").strip().lower()
    discord_token = os.getenv("DISCORD_TOKEN", "").strip()
    discord_client_id = os.getenv("DISCORD_CLIENT_ID", "").strip()
    discord_guild_id = os.getenv("DISCORD_GUILD_ID", "").strip()
    default_season = int(os.getenv("DEFAULT_SEASON", "2026"))
    default_game_type = os.getenv("DEFAULT_GAME_TYPE", "regular").strip().lower()
    cache_ttl_seconds = int(os.getenv("CACHE_TTL_SECONDS", "900"))
    database_path = os.getenv("DATABASE_URL", "").strip() or os.getenv("DATABASE_PATH", "bot-data/absbot.sqlite3").strip()
    daily_update_hour_eastern = int(os.getenv("DAILY_UPDATE_HOUR_EASTERN", "3"))
    integrity_sweep_days = int(os.getenv("INTEGRITY_SWEEP_DAYS", "7"))

    if default_game_type not in {"spring", "regular", "postseason"}:
        raise ValueError("DEFAULT_GAME_TYPE must be spring, regular, or postseason")
    if default_season < 2000:
        raise ValueError("DEFAULT_SEASON must be a modern season year")
    if cache_ttl_seconds < 0:
        raise ValueError("CACHE_TTL_SECONDS must be >= 0")
    if not 0 <= daily_update_hour_eastern <= 23:
        raise ValueError("DAILY_UPDATE_HOUR_EASTERN must be between 0 and 23")
    if integrity_sweep_days < 1:
        raise ValueError("INTEGRITY_SWEEP_DAYS must be >= 1")
    if app_role not in {"bot", "updater"}:
        raise ValueError("APP_ROLE must be bot or updater")
    if discord_guild_id and not discord_guild_id.isdigit():
        raise ValueError("DISCORD_GUILD_ID must be a numeric Discord guild id")
    if not database_path:
        raise ValueError("DATABASE_URL or DATABASE_PATH is required")

    if app_role == "bot" and not discord_token:
        raise ValueError("DISCORD_TOKEN is required")

    if app_role == "bot" and not discord_client_id:
        raise ValueError("DISCORD_CLIENT_ID is required")

    return Settings(
        discord_token=discord_token,
        discord_client_id=discord_client_id,
        discord_guild_id=discord_guild_id,
        default_season=default_season,
        default_game_type=default_game_type,
        cache_ttl_seconds=cache_ttl_seconds,
        database_path=database_path,
        daily_update_hour_eastern=daily_update_hour_eastern,
        integrity_sweep_days=integrity_sweep_days,
    )
