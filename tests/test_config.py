import pytest

from app.config import load_settings


@pytest.fixture(autouse=True)
def clear_env(monkeypatch) -> None:
    monkeypatch.setattr("app.config.load_dotenv", lambda: None)
    for key in [
        "APP_ROLE",
        "DATABASE_URL",
        "DISCORD_TOKEN",
        "DISCORD_CLIENT_ID",
        "DISCORD_GUILD_ID",
        "DEFAULT_SEASON",
        "DEFAULT_GAME_TYPE",
        "CACHE_TTL_SECONDS",
        "DATABASE_URL",
        "DATABASE_PATH",
        "DAILY_UPDATE_HOUR_EASTERN",
        "INTEGRITY_SWEEP_DAYS",
    ]:
        monkeypatch.delenv(key, raising=False)


def test_load_settings_reads_valid_environment(monkeypatch) -> None:
    monkeypatch.setenv("APP_ROLE", "bot")
    monkeypatch.setenv("DISCORD_TOKEN", "token")
    monkeypatch.setenv("DISCORD_CLIENT_ID", "client")
    monkeypatch.setenv("DISCORD_GUILD_ID", "123456789")
    monkeypatch.setenv("DEFAULT_SEASON", "2026")
    monkeypatch.setenv("DEFAULT_GAME_TYPE", "regular")
    monkeypatch.setenv("CACHE_TTL_SECONDS", "60")
    monkeypatch.setenv("DATABASE_PATH", "/tmp/test.sqlite3")
    monkeypatch.setenv("DAILY_UPDATE_HOUR_EASTERN", "3")
    monkeypatch.setenv("INTEGRITY_SWEEP_DAYS", "5")

    settings = load_settings()

    assert settings.discord_token == "token"
    assert settings.discord_client_id == "client"
    assert settings.default_season == 2026
    assert settings.daily_update_hour_eastern == 3
    assert settings.integrity_sweep_days == 5


def test_load_settings_prefers_database_url_over_path(monkeypatch) -> None:
    monkeypatch.setenv("APP_ROLE", "bot")
    monkeypatch.setenv("DISCORD_TOKEN", "token")
    monkeypatch.setenv("DISCORD_CLIENT_ID", "client")
    monkeypatch.setenv("DATABASE_URL", "postgresql://absbot:secret@db:5432/absbot")
    monkeypatch.setenv("DATABASE_PATH", "/tmp/test.sqlite3")

    settings = load_settings()

    assert settings.database_path == "postgresql://absbot:secret@db:5432/absbot"


def test_load_settings_requires_discord_token(monkeypatch) -> None:
    monkeypatch.setenv("APP_ROLE", "bot")
    monkeypatch.setenv("DISCORD_CLIENT_ID", "client")
    with pytest.raises(ValueError, match="DISCORD_TOKEN"):
        load_settings()


def test_load_settings_requires_client_id(monkeypatch) -> None:
    monkeypatch.setenv("APP_ROLE", "bot")
    monkeypatch.setenv("DISCORD_TOKEN", "token")
    with pytest.raises(ValueError, match="DISCORD_CLIENT_ID"):
        load_settings()


def test_load_settings_rejects_invalid_game_type(monkeypatch) -> None:
    monkeypatch.setenv("APP_ROLE", "bot")
    monkeypatch.setenv("DISCORD_TOKEN", "token")
    monkeypatch.setenv("DISCORD_CLIENT_ID", "client")
    monkeypatch.setenv("DEFAULT_GAME_TYPE", "weird")
    with pytest.raises(ValueError, match="DEFAULT_GAME_TYPE"):
        load_settings()


def test_load_settings_rejects_invalid_update_hour(monkeypatch) -> None:
    monkeypatch.setenv("APP_ROLE", "bot")
    monkeypatch.setenv("DISCORD_TOKEN", "token")
    monkeypatch.setenv("DISCORD_CLIENT_ID", "client")
    monkeypatch.setenv("DAILY_UPDATE_HOUR_EASTERN", "24")
    with pytest.raises(ValueError, match="DAILY_UPDATE_HOUR_EASTERN"):
        load_settings()


def test_load_settings_rejects_invalid_integrity_window(monkeypatch) -> None:
    monkeypatch.setenv("APP_ROLE", "bot")
    monkeypatch.setenv("DISCORD_TOKEN", "token")
    monkeypatch.setenv("DISCORD_CLIENT_ID", "client")
    monkeypatch.setenv("INTEGRITY_SWEEP_DAYS", "0")
    with pytest.raises(ValueError, match="INTEGRITY_SWEEP_DAYS"):
        load_settings()


def test_load_settings_allows_updater_without_discord_credentials(monkeypatch) -> None:
    monkeypatch.setenv("APP_ROLE", "updater")
    settings = load_settings()
    assert settings.discord_token == ""
    assert settings.discord_client_id == ""


def test_load_settings_defaults_to_sqlite_when_no_database_env_is_set(monkeypatch) -> None:
    monkeypatch.setenv("APP_ROLE", "bot")
    monkeypatch.setenv("DISCORD_TOKEN", "token")
    monkeypatch.setenv("DISCORD_CLIENT_ID", "client")

    settings = load_settings()

    assert settings.database_path == "bot-data/absbot.sqlite3"


def test_load_settings_rejects_invalid_guild_id(monkeypatch) -> None:
    monkeypatch.setenv("APP_ROLE", "bot")
    monkeypatch.setenv("DISCORD_TOKEN", "token")
    monkeypatch.setenv("DISCORD_CLIENT_ID", "client")
    monkeypatch.setenv("DISCORD_GUILD_ID", "abc")
    with pytest.raises(ValueError, match="DISCORD_GUILD_ID"):
        load_settings()
