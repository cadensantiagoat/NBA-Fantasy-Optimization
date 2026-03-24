import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from db import get_engine

load_dotenv()

DATA_FOLDER = "nhl_data"
date_str = pd.Timestamp.now().strftime("%Y%m%d")

df_dim = pd.read_csv(f"{DATA_FOLDER}/dim_players_nhl_{date_str}.csv")
df_fact = pd.read_csv(f"{DATA_FOLDER}/fact_nhl_perf_{date_str}.csv")

print("Connecting to PostgreSQL...")
engine = get_engine()

print("Truncating old NHL fact table...")
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE raw_data.fact_nhl_performance"))

print("Uploading NHL Players to dim_players...")
df_dim.to_sql(
    "dim_players",
    engine,
    schema="raw_data",
    if_exists="append",
    index=False,
    method="multi",
    chunksize=5000,
)
print(f"Inserted {len(df_dim)} players into dimension table.")

print("Uploading NHL Stats to fact_nhl_performance...")
df_fact.to_sql(
    "fact_nhl_performance",
    engine,
    schema="raw_data",
    if_exists="append",
    index=False,
    method="multi",
    chunksize=5000,
)
print(f"Inserted {len(df_fact)} stat rows into fact table.")

engine.dispose()
print("Successfully loaded all NHL data into PostgreSQL!")
