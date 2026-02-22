USE ROLE ACCOUNTADMIN;
USE DATABASE nba_fantasy_db;
USE SCHEMA raw_data;

-- Add new column
ALTER TABLE dim_players ADD COLUMN sport VARCHAR(50);

-- Backfill existing NBA players so they aren't blank
UPDATE dim_players SET sport = 'NBA' WHERE sport IS NULL;

CREATE TABLE IF NOT EXISTS fact_nhl_performance (
    game_id VARCHAR(100),
    player_id INT,
    team_id VARCHAR(50),
    game_date DATE,
    goals INT,
    assists INT,
    shots INT,
    penalty_minutes INT,
    plus_minus INT,
    fantasy_points FLOAT
);