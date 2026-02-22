import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv

load_dotenv()

# Point to the NBA data folder
DATA_FOLDER = "nba_data"
date_str = pd.Timestamp.now().strftime('%Y%m%d')

# Read the NEW real data files
df_dim = pd.read_csv(f"{DATA_FOLDER}/dim_players_nba_{date_str}.csv")
df_fact = pd.read_csv(f"{DATA_FOLDER}/fact_nba_perf_{date_str}.csv")

# Snowflake prefers uppercase column names
df_dim.columns = [col.upper() for col in df_dim.columns]
df_fact.columns = [col.upper() for col in df_fact.columns]

print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse="COMPUTE_WH",
    database='NBA_FANTASY_DB',
    schema='RAW_DATA'
)

# Upload the Dimension Table
print("Uploading NBA Players to dim_players...")
success, nchunks, nrows, _ = write_pandas(conn, df_dim, 'DIM_PLAYERS', quote_identifiers=False)
print(f"Inserted {nrows} players into dimension table.")

# Upload the Fact Table
print("Uploading NBA Stats to fact_game_performance...")
success, nchunks, nrows, _ = write_pandas(conn, df_fact, 'FACT_GAME_PERFORMANCE', quote_identifiers=False)
print(f"Inserted {nrows} stat rows into fact table.")

conn.close()
print("Successfully loaded all NBA data into Snowflake!")