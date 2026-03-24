-- Run once in your Postgres SQL editor (Supabase: SQL → New query; Neon: SQL Editor).
-- Creates the same logical layout as the old Snowflake RAW_DATA schema.

CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE IF NOT EXISTS raw_data.dim_players (
    player_id BIGINT NOT NULL,
    full_name TEXT NOT NULL,
    sport TEXT NOT NULL,
    PRIMARY KEY (player_id, sport)
);

CREATE TABLE IF NOT EXISTS raw_data.fact_game_performance (
    game_id TEXT,
    player_id BIGINT NOT NULL,
    team_id TEXT,
    game_date DATE,
    points DOUBLE PRECISION,
    rebounds DOUBLE PRECISION,
    assists DOUBLE PRECISION,
    steals DOUBLE PRECISION,
    blocks DOUBLE PRECISION,
    fantasy_points DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS raw_data.fact_nhl_performance (
    game_id TEXT,
    player_id BIGINT NOT NULL,
    team_id TEXT,
    game_date DATE,
    goals DOUBLE PRECISION,
    assists DOUBLE PRECISION,
    shots DOUBLE PRECISION,
    penalty_minutes DOUBLE PRECISION,
    plus_minus DOUBLE PRECISION,
    fantasy_points DOUBLE PRECISION
);
