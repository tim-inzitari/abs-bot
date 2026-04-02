ALTER TABLE leaderboard_rows RENAME TO leaderboard_rows_old;

CREATE TABLE leaderboard_rows (
    year INTEGER NOT NULL,
    game_type TEXT NOT NULL,
    challenge_type TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    entity_id INTEGER,
    player_name TEXT NOT NULL,
    team_abbr TEXT,
    parent_org TEXT,
    raw_json TEXT NOT NULL,
    PRIMARY KEY (year, game_type, challenge_type, entity_key)
);

INSERT INTO leaderboard_rows (
    year,
    game_type,
    challenge_type,
    entity_key,
    entity_id,
    player_name,
    team_abbr,
    parent_org,
    raw_json
)
SELECT
    year,
    game_type,
    challenge_type,
    COALESCE(CAST(entity_id AS TEXT), player_name),
    entity_id,
    player_name,
    team_abbr,
    parent_org,
    raw_json
FROM leaderboard_rows_old;

DROP TABLE leaderboard_rows_old;
