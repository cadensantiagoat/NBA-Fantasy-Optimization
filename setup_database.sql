-- 1. Creating Database and Schema
CREATE DATABASE IF NOT EXISTS nba_fantasy_db;
USE DATABASE nba_fantasy_db;

CREATE SCHEMA IF NOT EXISTS raw_data;
USE SCHEMA raw_data;

--2. Creating final dimension and fact tables (Star schema)
CREATE TABLE IF NOT EXISTS dim_players (
    player_id INT PRIMARY KEY,
    full_name VARCHAR,
    is_active BOOLEAN DEFAULT TRUE,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS fact_game_performance(
    game_id VARCHAR,
    player_id INT,
    team_id INT,
    game_date DATE,
    points INT,
    assists INT,
    rebounds INT,
    blocks INT,
    turnovers INT,
    minutes_played FLOAT,
    fantasy_points FLOAT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 3. Creating STAGING table
-- temporary landing zone for Python script's raw CSV data
CREATE TABLE IF NOT EXISTS stg_dim_players (
    player_id INT,
    full_name VARCHAR
);

-- 1. Recreate the fact table to include the missing 'steals' column
CREATE OR REPLACE TABLE fact_game_performance(
    game_id VARCHAR,
    player_id INT,
    team_id INT,
    game_date DATE,
    points INT,
    assists INT,
    rebounds INT,
    steals INT,       -- <--- Added steals back in!
    blocks INT,
    turnovers INT,
    minutes_played FLOAT,
    fantasy_points FLOAT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 2. Creating STAGING table for CSV data of game performance
CREATE TABLE IF NOT EXISTS stg_fact_game_performance (
    game_id VARCHAR,
    player_id INT,
    team_id INT,
    game_date DATE,
    points INT,
    assists INT,
    rebounds INT,
    steals INT,
    blocks INT,
    turnovers INT,
    minutes_played FLOAT
);

