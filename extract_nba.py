import pandas as pd
import os
import time
from datetime import datetime
from nba_api.stats.endpoints import leaguedashplayerstats
from dotenv import load_dotenv

load_dotenv()

# Grabbing proxy from Github Secrets
proxy_url = os.getenv("PROXY_URL")

# Explicitly set proxy environment variables. 
# 'requests' (used by nba_api) automatically detects these and handles 
# proxy authentication handshakes more reliably in headless environments.
if proxy_url:
    os.environ["HTTP_PROXY"] = proxy_url
    os.environ["HTTPS_PROXY"] = proxy_url

# Setting up folder
DATA_FOLDER = "nba_data"
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

print("Fetching NBA data from NBA.com...")

# Dynamically calculate the current NBA season string (e.g., "2024-25")
now = datetime.now()
if now.month < 10:  # Before October, we are in the season that started last year
    current_season = f"{now.year-1}-{str(now.year)[-2:]}"
else:  # From October onwards, we are in the season starting this year
    current_season = f"{now.year}-{str(now.year+1)[-2:]}"

print(f"Targeting Season: {current_season}")

MAX_RETRIES = 3
RETRY_DELAY = 5 # seconds

custom_headers = {
    'Host': 'stats.nba.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.nba.com/',
    'Origin': 'https://www.nba.com',
    'Connection': 'keep-alive',
}

df_raw = None
for attempt in range(MAX_RETRIES):
    try:
        nba_stats = leaguedashplayerstats.LeagueDashPlayerStats(
            per_mode_detailed='PerGame',
            season=current_season,
            proxy=proxy_url,
            headers=custom_headers,
            timeout=30
        )
        df_raw = nba_stats.get_data_frames()[0]
        break # Success!
    except Exception as e:
        print(f"Attempt {attempt + 1} failed: {e}")
        if attempt < MAX_RETRIES - 1:
            print(f"Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
        else:
            print("All attempts failed. Exiting.")
            raise


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
})

print(df_fact.columns.tolist())

# Saving to CSVs
date_str = datetime.today().strftime('%Y%m%d')
df_dim.to_csv(f"{DATA_FOLDER}/dim_players_nba_{date_str}.csv", index=False)
df_fact.to_csv(f"{DATA_FOLDER}/fact_nba_perf_{date_str}.csv", index=False)

print(f"Successfully saved stats for {len(df_dim)} NBA players!")