CREATE TABLE IF NOT EXISTS leaderboard_rows (
    year INTEGER NOT NULL,
    game_type TEXT NOT NULL,
    challenge_type TEXT NOT NULL,
    entity_id INTEGER,
    player_name TEXT NOT NULL,
    team_abbr TEXT,
    parent_org TEXT,
    raw_json TEXT NOT NULL,
    PRIMARY KEY (year, game_type, challenge_type, player_name)
);

CREATE TABLE IF NOT EXISTS teams (
    year INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    abbreviation TEXT NOT NULL,
    PRIMARY KEY (year, team_id)
);

CREATE TABLE IF NOT EXISTS player_positions (
    year INTEGER NOT NULL,
    person_id INTEGER NOT NULL,
    full_name TEXT NOT NULL,
    position TEXT NOT NULL,
    PRIMARY KEY (year, person_id)
);

CREATE TABLE IF NOT EXISTS umpire_game_stats (
    year INTEGER NOT NULL,
    game_type TEXT NOT NULL,
    game_pk INTEGER NOT NULL,
    official_date TEXT NOT NULL,
    matchup TEXT NOT NULL,
    umpire_name TEXT NOT NULL,
    tracked_challenges INTEGER NOT NULL,
    overturned INTEGER NOT NULL,
    confirmed INTEGER NOT NULL,
    PRIMARY KEY (year, game_type, game_pk)
);

CREATE TABLE IF NOT EXISTS untracked_errors (
    year INTEGER NOT NULL,
    game_type TEXT NOT NULL,
    scope TEXT NOT NULL,
    entity_name TEXT,
    official_date TEXT,
    matchup TEXT,
    detail TEXT NOT NULL,
    game_pk INTEGER,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sync_state (
    year INTEGER NOT NULL,
    game_type TEXT NOT NULL,
    sync_kind TEXT NOT NULL,
    synced_at TEXT NOT NULL,
    PRIMARY KEY (year, game_type, sync_kind)
);
