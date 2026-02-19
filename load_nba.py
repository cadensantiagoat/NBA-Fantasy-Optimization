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

# 4. Truncate staging and uploading new data
print("Uploading data to staging table...")
# Clear out yesterday's old staging data
cursor.execute("TRUNCATE TABLE stg_dim_players")

# pushing new pandas dataframe into the staging table
success, num_chunks, num_rows, output = write_pandas(
    conn=conn,
    df=df_players,
    table_name='STG_DIM_PLAYERS'
)
print(f"Successfully uploaded {num_rows} rows to staging!")

# Executing UPSERT (merge)
print("Running MERGE statement to update final dimension table...")
merge_sql = """
MERGE INTO dim_players AS target
USING stg_dim_players AS source
ON target.player_id = source.player_id
WHEN MATCHED THEN
    UPDATE SET target.full_name = source.full_name
WHEN NOT MATCHED THEN
    INSERT (player_id, full_name)
    VALUES (source.player_id, source.full_name)
"""

cursor.execute(merge_sql)
print("MERGE complete! Snowflake database is now up to date.")

# Closing connection
cursor.close()
conn.close()