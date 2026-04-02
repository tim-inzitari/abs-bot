CREATE TABLE IF NOT EXISTS umpire_pitch_audit (
    year INTEGER NOT NULL,
    game_type TEXT NOT NULL,
    game_pk INTEGER NOT NULL,
    official_date TEXT NOT NULL,
    matchup TEXT NOT NULL,
    umpire_name TEXT NOT NULL,
    called_pitches INTEGER NOT NULL,
    challenged_pitches INTEGER NOT NULL,
    unchallenged_correct INTEGER NOT NULL,
    unchallenged_incorrect INTEGER NOT NULL,
    PRIMARY KEY (year, game_type, game_pk)
);

CREATE INDEX IF NOT EXISTS idx_umpire_pitch_audit_umpire
ON umpire_pitch_audit (year, game_type, umpire_name);
