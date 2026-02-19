import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# 1. Snowflake Credentials
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

# 2. Loading local CSV data
print("Reading local mock data...")
# Read the CSV generated earlier
df_players = pd.read_csv("nba_data/dim_players_20260211.csv")

# changing columns to uppercase to match Snowflake default
df_players.columns = df_players.columns.str.upper()

df_stats = pd.read_csv("nba_data/fact_perf_20260211.csv")
df_stats.columns = df_stats.columns.str.upper()

# 3. Connecting to Snowflake
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse="COMPUTE_WH",
    database='NBA_FANTASY_DB',
    schema='RAW_DATA'
)

cursor = conn.cursor()

# Forcing Snowflake to use correct database and schema
cursor.execute("USE DATABASE NBA_FANTASY_DB")
cursor.execute("USE SCHEMA RAW_DATA")

# LOADING DIMENSION TABLE (PLAYERS)
print("Uploading players to staging table...")
# Clear out yesterday's old staging data
cursor.execute("TRUNCATE TABLE stg_dim_players")

# pushing new pandas dataframe into the staging table
write_pandas(conn=conn,df=df_players,table_name='STG_DIM_PLAYERS')
print(f"Merging Players...")
merge_players_sql= """
MERGE INTO dim_players AS target
USING stg_dim_players AS source
ON target.player_id = source.player_id
WHEN MATCHED THEN
    UPDATE SET target.full_name = source.full_name
WHEN NOT MATCHED THEN
    INSERT (player_id, full_name)
    VALUES (source.player_id, source.full_name)
"""
cursor.execute(merge_players_sql)

# LOAD FACT TABLE (STATS)
print("Uploading stats to staging table...")
cursor.execute("TRUNCATE TABLE stg_fact_game_performance")

write_pandas(conn=conn,df=df_stats,table_name='STG_FACT_GAME_PERFORMANCE')

print(f"Merging Stats...")
# 'unique ID" is the combination of Game ID and Player ID
merge_stats_sql= """
MERGE INTO fact_game_performance AS target
USING stg_fact_game_performance AS source
ON target.game_id = source.game_id AND target.player_id = source.player_id
WHEN MATCHED THEN
    UPDATE SET 
    target.points = source.points,
    target.assists = source.assists,
    target.rebounds = source.rebounds,
    target.steals = source.steals,
    target.blocks = source.blocks,
    target.turnovers = source.turnovers,
    target.minutes_played = source.minutes_played
WHEN NOT MATCHED THEN
    INSERT (game_id, player_id, team_id, game_date, points, assists, rebounds, steals, blocks, turnovers, minutes_played)
    VALUES (source.game_id, source.player_id, source.team_id, source.game_date, source.points, source.assists, source.rebounds, source.steals, source.blocks, source.turnovers, source.minutes_played)
"""
cursor.execute(merge_stats_sql)

print("Both Players and Stats are fully updated in Snowflake!")

cursor.close()
conn.close()
