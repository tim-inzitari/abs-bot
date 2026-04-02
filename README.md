# ABS Bot

Python Discord ABS bot with a PostgreSQL-first database setup for NAS deployment.

## Simplest TrueNAS Setup

If you want this to be as easy as possible on TrueNAS Scale, pull the same Docker Hub image twice:

- one app with `APP_ROLE=bot`
- one app with `APP_ROLE=updater`

Run three app instances:

- one Postgres app/container
- one app with `APP_ROLE=bot`
- one app with `APP_ROLE=updater`

The Postgres app should mount your dedicated NAS dataset to the Postgres data directory, for example:

```text
/var/lib/postgresql/data
```

The bot and updater containers do not need to store database files locally anymore. They only need network access to that Postgres instance through `DATABASE_URL`.

## Required env vars

Use these on the bot container:

```text
DISCORD_TOKEN=...
DISCORD_CLIENT_ID=...
APP_ROLE=bot
DEFAULT_SEASON=2026
DEFAULT_GAME_TYPE=regular
DATABASE_URL=postgresql://absbot:your-password@your-postgres-host:5432/absbot
DATABASE_CONNECT_RETRIES=30
DATABASE_CONNECT_RETRY_SECONDS=2
DAILY_UPDATE_HOUR_EASTERN=3
INTEGRITY_SWEEP_DAYS=7
```

Use these on the updater container:

```text
APP_ROLE=updater
DEFAULT_SEASON=2026
DEFAULT_GAME_TYPE=regular
DATABASE_URL=postgresql://absbot:your-password@your-postgres-host:5432/absbot
DAILY_UPDATE_HOUR_EASTERN=3
INTEGRITY_SWEEP_DAYS=7
```

`DATABASE_URL` is the preferred production setting. In the bundled Compose setup, the bot and updater now default to the local `ump-db` Postgres service even if `.env` does not explicitly set `DATABASE_URL`.
The retry settings are optional, but useful on NAS startups where Postgres may come up a few seconds after the app containers.

Use these on the Postgres container:

```text
POSTGRES_DB=absbot
POSTGRES_USER=absbot
POSTGRES_PASSWORD=your-password
```

`DATABASE_PATH` still works as a SQLite fallback for local-only use, but the intended NAS deployment is PostgreSQL.

## What each role does

- `APP_ROLE=bot`
  Starts the Discord bot.
- `APP_ROLE=updater`
  Starts the nightly reconciliation loop.
- PostgreSQL
  Stores the application data on your dedicated NAS dataset.

## Data behavior

- First startup seeds the DB once if empty.
- Bot queries refresh only current-day data on demand for the selected season.
- Updater reconciles the previous day at 3 AM Eastern.
- Updater also sweeps the last `INTEGRITY_SWEEP_DAYS` days to catch delayed corrections without broad rebuilds.
- Date-based refreshes are season-guarded, so a past-season query will not write today’s games into the wrong season table.
- The DB is not part of the image, so Docker Hub pushes stay small.

## Extra capabilities

- Challenge-level umpire events are stored in the database, not just per-game totals.
- Game metadata is stored in a dedicated `games` table for richer relational structure.
- Umpire role breakdowns are built from those stored challenge events.
- Structured SQL migrations live in `app/migrations/`.
- `/admin_reconcile` lets you refresh yesterday or a specific date manually.
- `/umpires` lists cached umpire names and supports fuzzy filtering.
- Real upstream fixture files live in `tests/fixtures/` and back parser tests.

## Output style

Responses are intentionally short:

- main result in one line
- optional one-line `Unchallenged:` follow-up
- optional one-line `Unresolved:` follow-up
- hard cap of 3 lines total

Example:

```text
Umpire CB Buckner: 35.0% accuracy (70/200) [pitcher 25/51 batter 14/33 catcher 31/74] [rank 12/34]
Unchallenged: 97.8% [1760/1800] | total [1890/2000]
Unresolved: 1
```

## Local run

Bot:

```bash
chmod +x scripts/run-local.sh
./scripts/run-local.sh
```

Updater:

```bash
chmod +x scripts/run-updater-local.sh
./scripts/run-updater-local.sh
```

Manual season rebuild from the updater container or a shell:

```bash
python -m app.updater_main full-refresh --year 2026 --game-type regular
```

Other manual maintenance commands:

```bash
python -m app.updater_main reconcile-date --date 2026-04-01 --game-type regular
python -m app.updater_main integrity-sweep --year 2026 --game-type regular --days 7
```

## Docker Compose

Start both:

```bash
docker compose up --build -d
```

Stop both:

```bash
docker compose down
```

The compose setup now includes:

- `ump-db` for PostgreSQL
- `ump-bot`
- `ump-updater`

The Postgres container stores its data in:

```text
./postgres-data
```

On TrueNAS, replace that with your dedicated dataset mount.

## Tests

```bash
source .venv/bin/activate
pytest
```
# abs-bot
# abs-bot
# abs-bot
