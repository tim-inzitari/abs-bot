from types import SimpleNamespace

import pytest

from app.analytics import AnalyticsReport
from app.config import Settings
from app.discord_bot import AbsBot


class FakeResponse:
    def __init__(self) -> None:
        self.deferred = []

    async def defer(self, **kwargs) -> None:
        self.deferred.append(kwargs)


class FakeFollowup:
    def __init__(self) -> None:
        self.messages = []

    async def send(self, message: str) -> None:
        self.messages.append(message)


@pytest.mark.asyncio
async def test_send_report_caps_output_at_three_lines() -> None:
    bot = AbsBot(
        Settings(
            discord_token="token",
            discord_client_id="client",
            discord_guild_id="",
            default_season=2026,
            default_game_type="regular",
            cache_ttl_seconds=60,
            database_path="/tmp/unused.sqlite3",
            daily_update_hour_eastern=3,
            integrity_sweep_days=7,
        )
    )
    interaction = SimpleNamespace(response=FakeResponse(), followup=FakeFollowup())
    report = AnalyticsReport(summary="line1", untracked_errors=["line2", "line3", "line4"])

    await bot._send_report(interaction, report)

    assert interaction.response.deferred == [{"thinking": True}]
    assert interaction.followup.messages == ["line1\nline2\nline3"]


def test_register_commands_include_umpires_and_exclude_admin_full_refresh() -> None:
    bot = AbsBot(
        Settings(
            discord_token="token",
            discord_client_id="client",
            discord_guild_id="",
            default_season=2026,
            default_game_type="regular",
            cache_ttl_seconds=60,
            database_path="/tmp/unused.sqlite3",
            daily_update_hour_eastern=3,
            integrity_sweep_days=7,
        )
    )

    bot._register_commands()

    assert bot.tree.get_command("umpires") is not None
    assert bot.tree.get_command("admin_full_refresh") is None
