CREATE TABLE IF NOT EXISTS games (
    year INTEGER NOT NULL,
    game_type TEXT NOT NULL,
    game_pk INTEGER NOT NULL,
    official_date TEXT NOT NULL,
    away_team_name TEXT,
    home_team_name TEXT,
    matchup TEXT NOT NULL,
    home_plate_umpire TEXT,
    last_scanned_at TEXT NOT NULL,
    PRIMARY KEY (year, game_type, game_pk)
);

CREATE INDEX IF NOT EXISTS idx_games_date
ON games (year, game_type, official_date);

CREATE INDEX IF NOT EXISTS idx_games_umpire
ON games (year, game_type, home_plate_umpire);
