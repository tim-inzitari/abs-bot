import pytest

from app.config import Settings, load_settings
from app.entrypoint import main as entrypoint_main


def _set_required_env(monkeypatch) -> None:
    monkeypatch.setattr("app.config.load_dotenv", lambda: None)
    monkeypatch.setenv("DISCORD_TOKEN", "token")
    monkeypatch.setenv("DISCORD_CLIENT_ID", "client-id")


def test_load_settings_reads_valid_environment(monkeypatch) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("DISCORD_GUILD_ID", "123456789")
    monkeypatch.setenv("DEFAULT_SEASON", "2027")
    monkeypatch.setenv("DEFAULT_GAME_TYPE", "spring")
    monkeypatch.setenv("CACHE_TTL_SECONDS", "60")
    monkeypatch.setenv("DATABASE_URL", "postgresql://absbot:secret@db:5432/absbot")
    monkeypatch.setenv("DATABASE_PATH", "/tmp/test.sqlite3")
    monkeypatch.setenv("DAILY_UPDATE_HOUR_EASTERN", "5")
    monkeypatch.setenv("INTEGRITY_SWEEP_DAYS", "3")

    settings = load_settings()

    assert settings == Settings(
        discord_token="token",
        discord_client_id="client-id",
        discord_guild_id="123456789",
        default_season=2027,
        default_game_type="spring",
        cache_ttl_seconds=60,
        database_path="postgresql://absbot:secret@db:5432/absbot",
        daily_update_hour_eastern=5,
        integrity_sweep_days=3,
    )


@pytest.mark.parametrize(
    ("env_name", "env_value", "message"),
    [
        ("DEFAULT_GAME_TYPE", "invalid", "DEFAULT_GAME_TYPE"),
        ("DEFAULT_SEASON", "1999", "DEFAULT_SEASON"),
        ("CACHE_TTL_SECONDS", "-1", "CACHE_TTL_SECONDS"),
        ("DAILY_UPDATE_HOUR_EASTERN", "24", "DAILY_UPDATE_HOUR_EASTERN"),
        ("INTEGRITY_SWEEP_DAYS", "0", "INTEGRITY_SWEEP_DAYS"),
    ],
)
def test_load_settings_rejects_invalid_values(monkeypatch, env_name: str, env_value: str, message: str) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv(env_name, env_value)

    with pytest.raises(ValueError, match=message):
        load_settings()


@pytest.mark.parametrize(("env_name", "message"), [("DISCORD_TOKEN", "DISCORD_TOKEN"), ("DISCORD_CLIENT_ID", "DISCORD_CLIENT_ID")])
def test_load_settings_requires_discord_credentials(monkeypatch, env_name: str, message: str) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.delenv(env_name, raising=False)

    with pytest.raises(ValueError, match=message):
        load_settings()


def test_entrypoint_routes_to_bot(monkeypatch) -> None:
    calls = []
    monkeypatch.setenv("APP_ROLE", "bot")
    monkeypatch.setattr("app.entrypoint.run_bot", lambda: calls.append("bot"))
    monkeypatch.setattr("app.entrypoint.run_updater", lambda: calls.append("updater"))

    entrypoint_main()

    assert calls == ["bot"]


def test_entrypoint_routes_to_updater(monkeypatch) -> None:
    calls = []
    monkeypatch.setenv("APP_ROLE", "updater")
    monkeypatch.setattr("app.entrypoint.run_bot", lambda: calls.append("bot"))
    monkeypatch.setattr("app.entrypoint.run_updater", lambda: calls.append("updater"))

    entrypoint_main()

    assert calls == ["updater"]


def test_entrypoint_rejects_unknown_role(monkeypatch) -> None:
    monkeypatch.setenv("APP_ROLE", "unknown")

    with pytest.raises(SystemExit, match="Unsupported APP_ROLE"):
        entrypoint_main()
