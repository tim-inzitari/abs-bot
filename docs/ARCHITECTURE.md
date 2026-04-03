# ABS Bot Architecture

## Purpose

This document describes the current architecture of the ABS Discord bot as it exists in the codebase today.

It covers:

- runtime roles
- data sources
- database design
- sync behavior
- Discord command surface
- analytics and output semantics
- deployment model
- testing strategy
- known limitations

This version reflects the current Python/PostgreSQL-first system, including the latest umpire parsing fixes and output semantics.

## System Summary

The project is a modular Python Discord bot that tracks MLB ABS challenge behavior using:

- Baseball Savant ABS leaderboard data for player, team, and league views
- MLB Stats API schedule and live-feed data for game discovery, home-plate umpire attribution, challenge extraction, and called-pitch auditing

The system is designed around three long-lived components:

1. PostgreSQL for persistent storage
2. a `bot` runtime for Discord slash commands
3. an `updater` runtime for scheduled reconciliation and manual maintenance

The bot and updater are started from the same application image and selected by `APP_ROLE`.

## Design Goals

- Keep Docker Hub images code-only, not data-heavy
- Keep database state outside the app image
- Make NAS deployment easy by reusing one app image for both long-running roles
- Avoid full historical recalculation during normal daily operation
- Reconcile recent dates authoritatively so ad hoc refreshes can stay lightweight
- Keep Discord responses compact and readable
- Preserve room for future integrations without rewriting core storage or analytics layers

## High-Level Architecture

```text
Discord Slash Command
        |
        v
   bot runtime
        |
        +--> ensure dataset exists
        +--> refresh today only
        +--> query analytics
        +--> return <= 3 lines
        |
        v
   PostgreSQL
        ^
        |
  updater runtime
        |
        +--> initial seed if empty
        +--> 3 AM Eastern previous-day reconcile
        +--> rolling integrity sweep
        +--> manual CLI maintenance commands
```

## Project Layout

```text
ump-bot/
├─ app/
│  ├─ analytics.py
│  ├─ cache.py
│  ├─ config.py
│  ├─ db.py
│  ├─ discord_bot.py
│  ├─ entrypoint.py
│  ├─ formatting.py
│  ├─ main.py
│  ├─ sync_service.py
│  ├─ updater_main.py
│  ├─ integrations/
│  │  ├─ baseball_savant.py
│  │  └─ mlb_stats.py
│  └─ migrations/
│     ├─ 001_initial.sql
│     ├─ 002_challenge_events.sql
│     ├─ 003_umpire_pitch_audit.sql
│     ├─ 004_leaderboard_entity_key.sql
│     └─ 005_games.sql
├─ docs/
│  └─ ARCHITECTURE.md
├─ scripts/
│  ├─ publish-dockerhub.sh
│  ├─ run-local.sh
│  └─ run-updater-local.sh
├─ tests/
│  ├─ fixtures/
│  ├─ conftest.py
│  └─ test_*.py
├─ docker-compose.yml
├─ Dockerfile
├─ README.md
├─ requirements.txt
└─ pytest.ini
```

## Runtime Model

The application uses one image and two runtime roles.

Entry selection happens in [entrypoint.py](/Users/timinzitari/Documents/Playground/ump-bot/app/entrypoint.py):

- `APP_ROLE=bot` -> launches the Discord bot
- `APP_ROLE=updater` -> launches the scheduled updater / CLI maintenance entrypoint

This lets the deployment model stay simple:

- one app image on Docker Hub
- one Postgres container
- two app containers using the same code image

## Main Components

### Bot Runtime

Implemented in [discord_bot.py](/Users/timinzitari/Documents/Playground/ump-bot/app/discord_bot.py).

Responsibilities:

- connect to Discord
- register and sync slash commands
- open the configured database
- initialize HTTP clients
- ensure the default dataset exists on startup
- refresh current-day data before answering queries
- run analytics queries against cached data
- send short text responses capped to 3 lines

Important behavior:

- every analytics command calls `ensure_dataset(...)`
- every analytics command calls `refresh_today(...)`
- current-day refresh is intentionally lighter than nightly maintenance

### Updater Runtime

Implemented in [updater_main.py](/Users/timinzitari/Documents/Playground/ump-bot/app/updater_main.py).

Responsibilities:

- ensure the default dataset exists
- wait for the nightly maintenance window
- reconcile the previous day at 3 AM Eastern
- run a rolling integrity sweep over recent prior days
- support explicit manual maintenance commands from the command line

The updater prints progress messages directly to stdout so NAS logs and shell sessions show useful state.

### Sync Engine

Implemented in [sync_service.py](/Users/timinzitari/Documents/Playground/ump-bot/app/sync_service.py).

Responsibilities:

- fetch Savant leaderboards
- fetch MLB schedules and live feeds
- interpret challenges
- perform per-date reconciliation
- compute umpire pitch audits
- write normalized database rows
- replace or upsert data safely without creating duplicates

### Analytics Layer

Implemented in [analytics.py](/Users/timinzitari/Documents/Playground/ump-bot/app/analytics.py).

Responsibilities:

- match user queries to cached entities
- aggregate database rows into concise summaries
- compute league, team, player, and umpire output
- normalize umpire aliases into a single identity
- format season-wide unchallenged and total umpire lines

### Database Layer

Implemented in [db.py](/Users/timinzitari/Documents/Playground/ump-bot/app/db.py).

Responsibilities:

- connect to PostgreSQL or SQLite
- apply SQL migrations
- expose storage/retrieval methods
- perform upserts/deletes around game-level reconciliation

PostgreSQL is the intended persistent production backend. SQLite remains as a lightweight local/dev fallback.

## Configuration

Configuration is loaded in [config.py](/Users/timinzitari/Documents/Playground/ump-bot/app/config.py).

Important environment variables:

- `APP_ROLE`
- `DISCORD_TOKEN`
- `DISCORD_CLIENT_ID`
- `DISCORD_GUILD_ID`
- `DEFAULT_SEASON`
- `DEFAULT_GAME_TYPE`
- `CACHE_TTL_SECONDS`
- `DATABASE_URL`
- `DATABASE_PATH`
- `DAILY_UPDATE_HOUR_EASTERN`
- `INTEGRITY_SWEEP_DAYS`
- `DATABASE_CONNECT_RETRIES`
- `DATABASE_CONNECT_RETRY_SECONDS`

Validation rules:

- `APP_ROLE` must be `bot` or `updater`
- `DEFAULT_GAME_TYPE` must be `spring`, `regular`, or `postseason`
- `DISCORD_TOKEN` and `DISCORD_CLIENT_ID` are required for `bot`
- `DATABASE_URL` or `DATABASE_PATH` must exist

## Data Sources

### Baseball Savant

Client: [baseball_savant.py](/Users/timinzitari/Documents/Playground/ump-bot/app/integrations/baseball_savant.py)

Used for:

- batter leaderboard
- pitcher leaderboard
- catcher leaderboard
- batting-team leaderboard

Parsing strategy:

- fetch the public ABS page HTML
- extract embedded `absData`
- store each leaderboard row as raw JSON plus normalized metadata

This is the primary source for player, team, and league challenge statistics.

### MLB Stats API

Client: [mlb_stats.py](/Users/timinzitari/Documents/Playground/ump-bot/app/integrations/mlb_stats.py)

Used for:

- team directory
- player primary positions
- schedules
- live feeds
- home-plate umpire assignment
- challenge-event parsing
- called-pitch auditing

This fills the gap that Savant does not expose publicly for umpire-specific ABS reporting.

## Database Architecture

The system is PostgreSQL-first for persistent deployment.

Production model:

- Postgres owns the persistent NAS-backed volume
- bot and updater connect via `DATABASE_URL`
- image updates do not change or overwrite stored data

Local/dev fallback:

- if `DATABASE_URL` is absent, the app can use `DATABASE_PATH`
- SQLite remains useful for tests and lightweight local execution

### Migrations

Schema evolution is handled with versioned SQL files in:

- [001_initial.sql](/Users/timinzitari/Documents/Playground/ump-bot/app/migrations/001_initial.sql)
- [002_challenge_events.sql](/Users/timinzitari/Documents/Playground/ump-bot/app/migrations/002_challenge_events.sql)
- [003_umpire_pitch_audit.sql](/Users/timinzitari/Documents/Playground/ump-bot/app/migrations/003_umpire_pitch_audit.sql)
- [004_leaderboard_entity_key.sql](/Users/timinzitari/Documents/Playground/ump-bot/app/migrations/004_leaderboard_entity_key.sql)
- [005_games.sql](/Users/timinzitari/Documents/Playground/ump-bot/app/migrations/005_games.sql)

Applied migration versions are tracked in `schema_migrations`.

### Tables

Current major tables:

1. `leaderboard_rows`
2. `teams`
3. `player_positions`
4. `games`
5. `umpire_game_stats`
6. `challenge_events`
7. `umpire_pitch_audit`
8. `untracked_errors`
9. `sync_state`
10. `schema_migrations`

### `leaderboard_rows`

Stores raw Savant ABS leaderboard rows by:

- season
- game type
- challenge type
- stable entity key

This supports:

- `/player`
- `/team`
- `/league`

`004_leaderboard_entity_key.sql` exists because name-only identity was unsafe for duplicate player names.

### `teams`

Stores the MLB team directory for a season.

Used for:

- team matching
- downstream enrichment

### `player_positions`

Stores primary positions by person ID and season.

Used for:

- league reporting by position

### `games`

Stores one normalized row per scanned game:

- `game_pk`
- `official_date`
- `away_team_name`
- `home_team_name`
- `matchup`
- `home_plate_umpire`
- `last_scanned_at`

This acts as the stable per-game backbone for reconciliation and future game-specific reporting.

### `umpire_game_stats`

Stores one aggregated umpire row per game:

- `tracked_challenges`
- `overturned`
- `confirmed`

This is the coarse-grained season summary layer for umpire reporting.

### `challenge_events`

Stores challenge-level event rows:

- game identity
- inning
- batter / pitcher / catcher names
- umpire name
- challenger role
- outcome
- raw description

This is the important event-level store that allows:

- umpire role breakdowns
- later reconciliation without guessing from pure game totals
- future richer integrations

### `umpire_pitch_audit`

Stores game-level called-pitch audit totals for each umpire:

- `called_pitches`
- `challenged_pitches`
- `unchallenged_correct`
- `unchallenged_incorrect`

This table powers the season-wide `Unchallenged` and `total` lines. It is updated during live current-day refreshes and then reconciled cleanly again during the 3 AM Eastern maintenance pass.

### `untracked_errors`

Stores ingestion issues such as:

- missing umpire assignment
- ambiguous challenge interpretation
- missing called-pitch context
- parsing failures

These are human-readable and intended to flow through to output when useful.

### `sync_state`

Stores the last execution time for sync kinds such as:

- `full`
- `today`
- `reconcile`
- `sweep`
- `admin`

## Sync Lifecycle

### 1. Initial Dataset Seed

`ensure_dataset(...)` checks whether the requested dataset has ever been fully seeded.

If not:

- Savant leaderboards are fetched
- teams are fetched
- player positions are fetched
- historical games are scanned
- game, umpire, challenge, audit, and error rows are written

This happens once per season/game-type dataset unless a manual rebuild is requested.

### 2. Ad Hoc Query Refresh

Before most Discord analytics commands, the bot calls `refresh_today(...)`.

Behavior:

- refresh only today’s games
- update `games`
- update `umpire_game_stats`
- update `challenge_events`
- update `umpire_pitch_audit`
- update `untracked_errors`
- recompute live current-day umpire pitch-audit totals

This keeps in-game umpire totals live while still letting the nightly reconcile rewrite the same rows authoritatively.

If the selected season does not match today’s calendar year, the refresh safely skips.

### 3. Nightly Reconciliation

At 3 AM Eastern, the updater runs:

1. `reconcile_previous_day(...)`
2. `integrity_sweep(...)`

Previous-day reconciliation is authoritative:

- target games are re-fetched
- prior rows for those games are deleted/replaced
- stale errors are removed
- pitch audits are recomputed

### 4. Rolling Integrity Sweep

The updater then reconciles a recent rolling window controlled by `INTEGRITY_SWEEP_DAYS`.

This is meant to catch:

- delayed upstream corrections
- incomplete current-day ingestion from ad hoc refreshes
- prior-day feed changes that appear after the first nightly run

The sweep intentionally skips yesterday duplication because yesterday is already handled explicitly.

### 5. Manual Maintenance

The updater also supports manual command-line maintenance:

- `full-refresh`
- `reconcile-date`
- `integrity-sweep`

Examples:

```bash
python -m app.updater_main full-refresh --year 2026 --game-type regular
python -m app.updater_main reconcile-date --date 2026-04-01 --game-type regular
python -m app.updater_main integrity-sweep --days 7 --year 2026 --game-type regular
```

There is no Discord-side full-season rebuild command. Full historical rebuilds are intentionally CLI-only.

## Challenge Parsing Model

The hardest part of the system is umpire challenge interpretation.

### Primary Source: Play Events

The first parsing pass walks:

- `liveData.plays.allPlays[].playEvents[]`

It looks for challenge-shaped descriptions and attempts to classify:

- `confirmed`
- `overturned`
- `unknown`
- `not-challenge`

It also tries to infer the challenger role from challenge text:

- `pitcher`
- `batter`
- `catcher`
- `unknown`

### Fallback Source: Boxscore ABS Summary

Some MLB feeds include meaningful ABS challenge outcomes under:

- `liveData.boxscore.info`
- label: `ABS Challenge`

The sync engine now parses that summary as a fallback source when play-event text is incomplete.

Example pattern:

```text
Anthony 2 (Strike-Overturned to Ball, Strike-Confirmed)
```

The fallback parser:

- splits the summary into challenge entries
- extracts individual outcomes
- maps `Overturned` / `Confirmed`
- attempts to infer role from roster position aliases
- inserts synthetic challenge-event rows with stable summary event keys

### Audit Adjustment for Summary Events

When summary-derived events are added, the pitch audit is adjusted so those challenges are moved out of the unchallenged bucket when possible.

This keeps the output closer to reality for games where the feed summary is richer than the play-event challenge text.

### Remaining Parser Limits

- role inference from summary entries is still alias/position based, not official structured ABS metadata
- some MLB feeds remain vague and produce `unknown` outcomes or `unknown` roles
- summary-derived challenge rows do not always carry inning or full participant context

## Umpire Identity Model

Umpire names are messy in practice.

Examples:

- `CB Bucknor`
- `C.B. Bucknor`
- user query `CB Buckner`

To handle this, the analytics layer:

- normalizes names with punctuation/spacing removed
- strips diacritics
- fuzzy-matches query strings
- groups game rows, event rows, and audit rows by normalized umpire identity

This allows the bot to merge alias variants into one report instead of splitting totals across multiple near-identical names.

## Analytics Semantics

### Player / Team / League

Player, team, and league reporting is challenger-oriented because it comes from Savant ABS leaderboard data.

That means:

- `accuracy` there effectively reflects overturn success from the challenger’s perspective
- counts are shown as `n_overturns / n_challenges`

### Umpire

Umpire reporting is umpire-oriented.

For umpires:

- `confirmed` means the umpire was correct on the challenged call
- `overturned` means the challenge beat the umpire’s original call

So umpire `accuracy` is:

```text
confirmed / tracked challenges
```

Role breakdowns also use umpire-correct counts:

```text
[pitcher confirmed/total batter confirmed/total catcher confirmed/total]
```

The second line uses nightly pitch auditing:

- `Unchallenged` = unchallenged correct / unchallenged total
- `total` = (unchallenged correct + confirmed challenged calls) / called pitches

## Discord Command Surface

Current slash commands in [discord_bot.py](/Users/timinzitari/Documents/Playground/ump-bot/app/discord_bot.py):

- `/health`
- `/admin_reconcile`
- `/player`
- `/team`
- `/league`
- `/umpire`
- `/umpires`

### `/health`

Simple readiness check.

### `/admin_reconcile`

Admin-only date reconciliation.

Supports:

- yesterday
- specific `YYYY-MM-DD` date

This is a targeted date repair tool, not a season rebuild tool.

### `/player`

Shows one player’s challenge profile from Savant leaderboards.

Optional:

- role
- year
- game type

### `/team`

Shows one team’s batting-team challenge profile.

### `/league`

Shows role-wide league aggregates plus top batter position by challenge count.

### `/umpire`

Shows:

- umpire challenge accuracy
- role breakdown
- rank
- unchallenged line
- unresolved count when present

### `/umpires`

Lists cached umpire names or fuzzy matches.

This exists mainly to help discover the spelling/alias currently present in cached data.

## Output Contract

Output is intentionally compact.

The bot sends at most 3 lines:

1. summary line
2. first detail line
3. optional second detail line

For umpires, the intended pattern is:

```text
Umpire CB Bucknor: 65.0% accuracy (130/200) [pitcher 1/2 batter 0/1 catcher 1/2] [rank 12/68]
Unchallenged: 97.8% [1760/1800] | total [1890/2000]
Unresolved: 1
```

League output includes counts inline, for example:

```text
League 2026: batter:52.1% (34/78) pitcher:41.7% (20/48) catcher:58.9% (43/73) batting-team:52.1% (34/78) [top position RF:33]
```

## Formatting Layer

[formatting.py](/Users/timinzitari/Documents/Playground/ump-bot/app/formatting.py) is intentionally thin right now.

The current formatting strategy is:

- keep report generation mostly inside analytics
- keep Discord output line-oriented and minimal

This leaves room for future integrations to reuse analytics results without depending on Discord-specific embed code.

## Caching

The HTTP clients use in-process TTL caches via [cache.py](/Users/timinzitari/Documents/Playground/ump-bot/app/cache.py).

Current caching scope includes:

- team directory
- people/position lookups
- live feeds

These caches are runtime-local only. Persistent truth lives in the database.

## Deployment Model

### TrueNAS / NAS Pattern

Recommended deployment:

1. Postgres container with persistent dataset mounted at `/var/lib/postgresql/data`
2. bot container using `APP_ROLE=bot`
3. updater container using `APP_ROLE=updater`

The app containers do not store their own persistent state.

### Local Compose Pattern

[docker-compose.yml](/Users/timinzitari/Documents/Playground/ump-bot/docker-compose.yml) provides a simple local flow:

- `ump-db`
- `ump-bot`
- `ump-updater`

Compose currently builds the local image directly, while Docker Hub deployment uses pushed tags.

## Testing Strategy

The project uses `pytest` with a broad suite across:

- analytics
- config validation
- DB behavior
- Discord command registration
- Savant parsing
- MLB client behavior
- sync engine flows
- updater scheduling
- utilities and caches

Fixtures include captured upstream payloads:

- [savant_abs_page.html](/Users/timinzitari/Documents/Playground/ump-bot/tests/fixtures/savant_abs_page.html)
- [mlb_live_feed_trimmed.json](/Users/timinzitari/Documents/Playground/ump-bot/tests/fixtures/mlb_live_feed_trimmed.json)

This is important because the system depends on brittle public payload shapes.

At the time of this document update, the suite passes with 103 tests.

## Known Risks And Limitations

### Upstream Fragility

- Savant HTML structure may change
- MLB feed wording may change
- ABS summary text is still not a fully structured official challenge API

### Role Inference

Challenge role attribution remains heuristic in some cases.

Best case:

- explicit challenge text identifies pitcher, batter, or catcher

Fallback case:

- summary parsing infers role from roster position aliases

Worst case:

- role remains `unknown`

### Current-Day Accuracy vs Nightly Accuracy

Ad hoc current-day refresh now recomputes live umpire pitch-audit totals, so same-day `Unchallenged` and `total` lines should move during the game.

The 3 AM Eastern maintenance pass is still authoritative because it:

- rewrites prior-day rows cleanly
- deduplicates per-game data
- rescans the rolling integrity window

### Single Region / Timezone Assumption

The scheduled maintenance model is specifically built around:

- `America/New_York`
- one daily maintenance window

## Extension Points

The project is already organized so new integrations can be added without rewriting the core:

- new upstream clients can live under `app/integrations/`
- analytics can emit integration-agnostic summaries
- Discord is only one presentation layer
- the normalized `games` + `challenge_events` + `umpire_pitch_audit` model is reusable for future outputs

Examples of realistic future additions:

- social posting integrations
- richer game-specific reporting
- admin inspection/debug commands
- external dashboards

## Practical Operating Notes

- If cached data is wrong because historical parsing changed, use the CLI `full-refresh`.
- If one specific date is wrong, use `reconcile-date` or Discord `/admin_reconcile`.
- If the bot appears live but umpire `Unchallenged` data looks stale during a live game, rerun the query once to trigger another current-day refresh. The nightly updater still performs the clean authoritative rewrite afterward.
- If an umpire query returns a strange spelling, use `/umpires` to inspect the cached alias set.

## Key Files

- [app/entrypoint.py](/Users/timinzitari/Documents/Playground/ump-bot/app/entrypoint.py)
- [app/config.py](/Users/timinzitari/Documents/Playground/ump-bot/app/config.py)
- [app/discord_bot.py](/Users/timinzitari/Documents/Playground/ump-bot/app/discord_bot.py)
- [app/updater_main.py](/Users/timinzitari/Documents/Playground/ump-bot/app/updater_main.py)
- [app/sync_service.py](/Users/timinzitari/Documents/Playground/ump-bot/app/sync_service.py)
- [app/analytics.py](/Users/timinzitari/Documents/Playground/ump-bot/app/analytics.py)
- [app/db.py](/Users/timinzitari/Documents/Playground/ump-bot/app/db.py)
- [app/integrations/baseball_savant.py](/Users/timinzitari/Documents/Playground/ump-bot/app/integrations/baseball_savant.py)
- [app/integrations/mlb_stats.py](/Users/timinzitari/Documents/Playground/ump-bot/app/integrations/mlb_stats.py)
