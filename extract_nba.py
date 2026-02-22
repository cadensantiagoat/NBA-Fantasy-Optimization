import pandas as pd
import os
from datetime import datetime
from nba_api.stats.endpoints import leaguedashplayerstats

# Grabbing proxy from Github Secrets
proxy_url = os.getenv("PROXY_URL")

# Setting up folder
DATA_FOLDER = "nba_data"
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

print("Fetching NBA data from NBA.com...")

# Fetching current season stats (Per Game avgs)
nba_stats = leaguedashplayerstats.LeagueDashPlayerStats(
    per_mode_detailed='PerGame',
    proxy=proxy_url
)
df_raw = nba_stats.get_data_frames()[0]

# Preparing Dimension Data (player profile)
df_dim = pd.DataFrame({
    'player_id': df_raw['PLAYER_ID'],
    'full_name': df_raw['PLAYER_NAME'],
    'sport': 'NBA'
})

# Preparing Fact Data (stats)
df_fact = pd.DataFrame({
    'game_id': 'NBA_2024_SEASON',
    'player_id': df_raw['PLAYER_ID'],
    'team_id': df_raw['TEAM_ABBREVIATION'],
    'game_date': datetime.today().strftime('%Y-%m-%d'),
    'points': df_raw['PTS'],
    'rebounds': df_raw['REB'],
    'assists': df_raw['AST'],
    'steals': df_raw['STL'],
    'blocks': df_raw['BLK'],
    # DraftKings NBA Scoring Model: PTS(1), REB(1.2), AST(1.5), STL(3), BLK(3)
    'fantasy_points': (df_raw['PTS'] * 1) + (df_raw['REB'] * 1.2) + (df_raw['AST'] * 1.5) + (df_raw['STL'] * 3) + (df_raw['BLK'] * 3)
})

# Saving to CSVs
date_str = datetime.today().strftime('%Y%m%d')
df_dim.to_csv(f"{DATA_FOLDER}/dim_players_nba_{date_str}.csv", index=False)
df_fact.to_csv(f"{DATA_FOLDER}/fact_nba_perf_{date_str}.csv", index=False)

print(f"Successfully saved stats for {len(df_dim)} NBA players!")