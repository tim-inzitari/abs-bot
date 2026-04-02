CREATE TABLE IF NOT EXISTS challenge_events (
    year INTEGER NOT NULL,
    game_type TEXT NOT NULL,
    game_pk INTEGER NOT NULL,
    event_key TEXT NOT NULL,
    official_date TEXT NOT NULL,
    matchup TEXT NOT NULL,
    umpire_name TEXT NOT NULL,
    inning INTEGER,
    batter_name TEXT,
    pitcher_name TEXT,
    catcher_name TEXT,
    challenger_role TEXT NOT NULL,
    outcome TEXT NOT NULL,
    description TEXT NOT NULL,
    PRIMARY KEY (year, game_type, game_pk, event_key)
);

CREATE INDEX IF NOT EXISTS idx_challenge_events_umpire
ON challenge_events (year, game_type, umpire_name);

CREATE INDEX IF NOT EXISTS idx_challenge_events_role
ON challenge_events (year, game_type, umpire_name, challenger_role);
