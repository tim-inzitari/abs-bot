from datetime import date, datetime, timedelta
from typing import Optional

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands
import pytz

from app.analytics import AnalyticsReport, AnalyticsService
from app.config import Settings
from app.db import Database
from app.formatting import one_line_report
from app.integrations.baseball_savant import BaseballSavantClient
from app.integrations.mlb_stats import MlbStatsClient
from app.sync_service import SyncService


class AbsBot(commands.Bot):
    def __init__(self, settings: Settings) -> None:
        super().__init__(command_prefix="!", intents=discord.Intents.default())
        self.settings = settings
        self.http_session: Optional[aiohttp.ClientSession] = None
        self.analytics_service: Optional[AnalyticsService] = None
        self.sync_service: Optional[SyncService] = None
        self.db: Optional[Database] = None

    async def setup_hook(self) -> None:
        self.db = Database(self.settings.database_path)
        self.db.ensure_schema()
        self.http_session = aiohttp.ClientSession(
            headers={
                "user-agent": "abs-bot/1.0",
                "accept": "application/json,text/html;q=0.9,*/*;q=0.8",
            }
        )
        savant_client = BaseballSavantClient(self.http_session, self.settings.cache_ttl_seconds)
        mlb_stats_client = MlbStatsClient(self.http_session, self.settings.cache_ttl_seconds)
        self.sync_service = SyncService(self.db, savant_client, mlb_stats_client)
        await self.sync_service.ensure_dataset(self.settings.default_season, self.settings.default_game_type)
        self.analytics_service = AnalyticsService(self.db)

        self._register_commands()
        if self.settings.discord_guild_id:
            guild = discord.Object(id=int(self.settings.discord_guild_id))
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
        else:
            await self.tree.sync()

    async def close(self) -> None:
        if self.http_session is not None:
            await self.http_session.close()
        if self.db is not None:
            self.db.close()
        await super().close()

    async def on_ready(self) -> None:
        if self.user:
            print(f"Logged in as {self.user} (ID: {self.user.id})")

    def _register_commands(self) -> None:
        default_year = self.settings.default_season
        default_game_type = self.settings.default_game_type

        @self.tree.command(name="health", description="Check that the bot is up and responding.")
        async def health(interaction: discord.Interaction) -> None:
            await interaction.response.send_message("The bot is live and ready to pull Savant ABS data.")

        @self.tree.command(name="admin_reconcile", description="Admin-only manual reconciliation for yesterday or a specific date.")
        @app_commands.default_permissions(administrator=True)
        @app_commands.describe(
            mode="Use yesterday or a specific date",
            target_date="Optional date in YYYY-MM-DD format when mode is date",
        )
        @app_commands.choices(
            mode=[
                app_commands.Choice(name="Yesterday", value="yesterday"),
                app_commands.Choice(name="Specific date", value="date"),
            ]
        )
        async def admin_reconcile(
            interaction: discord.Interaction,
            mode: app_commands.Choice[str],
            target_date: Optional[str] = None,
        ) -> None:
            await interaction.response.defer(thinking=True, ephemeral=True)
            if mode.value == "yesterday":
                eastern = pytz.timezone("America/New_York")
                previous_day = (datetime.now(tz=eastern).date() - timedelta(days=1))
                await self.sync_service.reconcile_date(previous_day.year, default_game_type, previous_day.isoformat(), sync_kind="admin")
                await interaction.followup.send("Admin reconcile: yesterday refreshed", ephemeral=True)
                return
            if not target_date:
                await interaction.followup.send("Admin reconcile: provide YYYY-MM-DD", ephemeral=True)
                return
            try:
                parsed_date = date.fromisoformat(target_date)
            except ValueError:
                await interaction.followup.send("Admin reconcile: invalid date, use YYYY-MM-DD", ephemeral=True)
                return
            await self.sync_service.reconcile_date(parsed_date.year, default_game_type, parsed_date.isoformat(), sync_kind="admin")
            await interaction.followup.send(f"Admin reconcile: {target_date} refreshed", ephemeral=True)

        @self.tree.command(name="player", description="Show a player challenge profile from Baseball Savant ABS data.")
        @app_commands.describe(name="Player name", role="Optional challenge role", year="Season year", game_type="Game segment")
        @app_commands.choices(
            role=[
                app_commands.Choice(name="Batter", value="batter"),
                app_commands.Choice(name="Pitcher", value="pitcher"),
                app_commands.Choice(name="Catcher", value="catcher"),
            ],
            game_type=[
                app_commands.Choice(name="Spring", value="spring"),
                app_commands.Choice(name="Regular season", value="regular"),
                app_commands.Choice(name="Postseason", value="postseason"),
            ],
        )
        async def player(
            interaction: discord.Interaction,
            name: str,
            role: Optional[app_commands.Choice[str]] = None,
            year: Optional[int] = None,
            game_type: Optional[app_commands.Choice[str]] = None,
        ) -> None:
            selected_year = year or default_year
            selected_game_type = game_type.value if game_type else default_game_type
            await self.sync_service.ensure_dataset(selected_year, selected_game_type)
            await self.sync_service.refresh_today(selected_year, selected_game_type)
            await self._send_report(
                interaction,
                await self.analytics_service.build_player_report(
                    name=name,
                    year=selected_year,
                    game_type=selected_game_type,
                    role=role.value if role else None,
                ),
            )

        @self.tree.command(name="team", description="Show a team challenge profile from Baseball Savant ABS data.")
        @app_commands.describe(name="Team name or abbreviation", year="Season year", game_type="Game segment")
        @app_commands.choices(
            game_type=[
                app_commands.Choice(name="Spring", value="spring"),
                app_commands.Choice(name="Regular season", value="regular"),
                app_commands.Choice(name="Postseason", value="postseason"),
            ]
        )
        async def team(
            interaction: discord.Interaction,
            name: str,
            year: Optional[int] = None,
            game_type: Optional[app_commands.Choice[str]] = None,
        ) -> None:
            selected_year = year or default_year
            selected_game_type = game_type.value if game_type else default_game_type
            await self.sync_service.ensure_dataset(selected_year, selected_game_type)
            await self.sync_service.refresh_today(selected_year, selected_game_type)
            await self._send_report(
                interaction,
                await self.analytics_service.build_team_report(
                    team_query=name,
                    year=selected_year,
                    game_type=selected_game_type,
                ),
            )

        @self.tree.command(name="league", description="Show league-wide challenge breakdowns by role and batter position.")
        @app_commands.describe(year="Season year", game_type="Game segment")
        @app_commands.choices(
            game_type=[
                app_commands.Choice(name="Spring", value="spring"),
                app_commands.Choice(name="Regular season", value="regular"),
                app_commands.Choice(name="Postseason", value="postseason"),
            ]
        )
        async def league(
            interaction: discord.Interaction,
            year: Optional[int] = None,
            game_type: Optional[app_commands.Choice[str]] = None,
        ) -> None:
            selected_year = year or default_year
            selected_game_type = game_type.value if game_type else default_game_type
            await self.sync_service.ensure_dataset(selected_year, selected_game_type)
            await self.sync_service.refresh_today(selected_year, selected_game_type)
            await self._send_report(
                interaction,
                await self.analytics_service.build_league_report(
                    year=selected_year,
                    game_type=selected_game_type,
                ),
            )

        @self.tree.command(name="umpire", description="Show an umpire challenge overturn profile.")
        @app_commands.describe(name="Umpire name", year="Season year", game_type="Game segment")
        @app_commands.choices(
            game_type=[
                app_commands.Choice(name="Spring", value="spring"),
                app_commands.Choice(name="Regular season", value="regular"),
                app_commands.Choice(name="Postseason", value="postseason"),
            ]
        )
        async def umpire(
            interaction: discord.Interaction,
            name: str,
            year: Optional[int] = None,
            game_type: Optional[app_commands.Choice[str]] = None,
        ) -> None:
            selected_year = year or default_year
            selected_game_type = game_type.value if game_type else default_game_type
            await self.sync_service.ensure_dataset(selected_year, selected_game_type)
            await self.sync_service.refresh_today(selected_year, selected_game_type)
            await self._send_report(
                interaction,
                await self.analytics_service.build_umpire_report(
                    name=name,
                    year=selected_year,
                    game_type=selected_game_type,
                ),
            )

        @self.tree.command(name="umpires", description="List cached umpire names or fuzzy matches.")
        @app_commands.describe(name="Optional fuzzy search for umpire names", year="Season year", game_type="Game segment")
        @app_commands.choices(
            game_type=[
                app_commands.Choice(name="Spring", value="spring"),
                app_commands.Choice(name="Regular season", value="regular"),
                app_commands.Choice(name="Postseason", value="postseason"),
            ]
        )
        async def umpires(
            interaction: discord.Interaction,
            name: Optional[str] = None,
            year: Optional[int] = None,
            game_type: Optional[app_commands.Choice[str]] = None,
        ) -> None:
            selected_year = year or default_year
            selected_game_type = game_type.value if game_type else default_game_type
            await self.sync_service.ensure_dataset(selected_year, selected_game_type)
            await self.sync_service.refresh_today(selected_year, selected_game_type)
            await self._send_report(
                interaction,
                await self.analytics_service.build_umpire_list_report(
                    year=selected_year,
                    game_type=selected_game_type,
                    query=name,
                ),
            )

    async def _send_report(self, interaction: discord.Interaction, report: AnalyticsReport) -> None:
        await interaction.response.defer(thinking=True)
        lines = [one_line_report(report.summary)]
        lines.extend(report.untracked_errors[:2])
        await interaction.followup.send("\n".join(lines[:3]))
