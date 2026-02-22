import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv

load_dotenv()

# Loading newest CSV files we just generated
DATA_FOLDER = "nhl_data"
date_str = pd.Timestamp.now().strftime('%Y%m%d')

df_dim = pd.read_csv(f"{DATA_FOLDER}/dim_players_nhl_{date_str}.csv")
df_fact = pd.read_csv(f"{DATA_FOLDER}/fact_nhl_perf_{date_str}.csv")

# Changing to uppercase for snowflake
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

# Making pipeline idempotent
print("Truncating old NHL tables...")
conn.cursor().execute("TRUNCATE TABLE fact_nhl_performance")

# Uploading Dimension Table
print("Uploading NHL Players to dim_players...")
# Using chunk_size to avoid memory issues 
success, nchunks, nrows, _ = write_pandas(conn, df_dim, 'DIM_PLAYERS', quote_identifiers=False)
print(f"Inserted {nrows} players into fact table.")

# Uploading Fact Table
print("Uploading NHL Stats to fact_nhl_performance...")
success, nchunks, nrows, _ = write_pandas(conn, df_fact, 'FACT_NHL_PERFORMANCE', quote_identifiers=False)
print(f"Inserted {nrows} stat rows into fact table.")

conn.close()
print("Successfully loaded all NHL data into Snowflake!")