import pandas as pd
from nba_api.stats.endpoints import playergamelog
from datetime import datetime, timedelta
import time
import os

# configuration
CURRENT_SEASON = "2025-2026"
DATA_FOLDER = "nba_data"

# checks if data folder exists
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

def fetch_daily_stats(date_str=None):
    """
    Fetches stats for a specific date. 
    If no date provided, defaults to YETERDAY.
    """
    if date_str is None:
        # get yesterday's date
        yesterday = datetime.now() = timedelta(days=1)
        date_str = yesterday.strftime('%m/%d/%Y')

    print(f"Fetching NBA stats for {date_str}...")

    try:
        # 1. Fetch Game logs for the specific date

        # DateFrom and DateTo are the same to get just one day
        logs = playergamelogs.PlayerGameLogs(
            season_nullable=CURRENT_SEASON,
            date_from_nullable=date_str,
            date_to_nullable=date_str,
        )

        # 2. Convert to DataFrame
        df_stats = logs.get_data_frames()[0]

        if df_stats.empty:
            print(f"No games found for {date_str}.")
            return

        # 3. Clean/Rename Clumns to match our Database Schema
        # Mapping NBA API columns to our 'fact_game_performance' schema
        df_fact = df_stats.rename(columns={
            'GAME_ID': 'game_id',
            'PLAYER_ID': 'player_id',
            'TEAM_ID': 'team_id',
            'GAME_DATE': 'game_date',
            'PTS': 'points',
            'AST': 'assists',
            'REB': 'rebounds',
            'STL': 'steals',
            'BLK': 'blocks',
            'TOV': 'turnovers',
            'MIN': 'minutes_played'
        })

        # 4. Extract Dimension Data (Player Info)
        # Saving the unique players from this batch.
        df_dim_players = df_stats[['PLAYER_ID', 'PLAYER_NAME']].drop_duplicates()
        df_dim_players = df_dim_players.rename(columns={
            'PLAYER_ID': 'player_id',
            'PLAYER_NAME': 'full_name'
        })

        # 5. Save to CSV (Staging Area)
        # Use the data in the filename so we don't overwrite
        file_date = datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y%m%d')

        fact_filename = f"{DATA_FOLDER}/fact_perf_{file_date}.csv"
        dim_filename = f"{DATA_FOLDER}/dim_players_{file_date}.csv"

        df_fact.to_csv(fact_filename, index=False)
        df_dim_players.to_csv(dim_filename, index=False)

        print(f"Saved {len(df_fact)} rows to {fact_filename}")
        print(f"Saved player reference data to {dim_filename}")

    except Exception as e:
        print(f"Error fetching data: {e}")

# main execution
if __name__ == "__main__":
    # can pass a specific date or leave empty for yesterdays stats
    fetch_daily_stats()