CREATE OR REPLACE TABLE fact_game_performance (
    game_id VARCHAR,
    player_id NUMBER,
    team_id VARCHAR,
    game_date DATE,
    points NUMBER,
    rebounds NUMBER,
    assists NUMBER,
    steals NUMBER,
    blocks NUMBER,
    fantasy_points FLOAT
);