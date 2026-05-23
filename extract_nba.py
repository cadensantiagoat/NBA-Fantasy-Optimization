import pandas as pd
import os
import time
from datetime import datetime
from nba_api.stats.endpoints import leaguedashplayerstats
from dotenv import load_dotenv

load_dotenv()

# Grabbing proxy from Github Secrets
proxy_url = os.getenv("PROXY_URL").strip() if os.getenv("PROXY_URL") else None

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
    'Connection': 'keep-alive',
    'Accept': 'application/json, text/plain, */*',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Origin': 'https://www.nba.com',
    'Sec-Fetch-Site': 'same-site',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'Referer': 'https://www.nba.com/',
    'Accept-Language': 'en-US,en;q=0.9',
}

season_types = ['Regular Season', 'Playoffs']
dfs_raw = []

for s_type in season_types:
    print(f"Fetching {s_type} stats...")
    for attempt in range(MAX_RETRIES):
        try:
            nba_stats = leaguedashplayerstats.LeagueDashPlayerStats(
                per_mode_detailed='PerGame',
                season=current_season,
                season_type_all_star=s_type,
                proxy=proxy_url,
                headers=custom_headers,
                timeout=30
            )
            df = nba_stats.get_data_frames()[0]
            if not df.empty:
                df['SEASON_TYPE'] = s_type
                dfs_raw.append(df)
            break # Success for this season type
        except Exception as e:
            print(f"Attempt {attempt + 1} for {s_type} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"All attempts for {s_type} failed.")

if not dfs_raw:
    print("No data fetched for any season type. Exiting.")
    exit(1)

df_raw = pd.concat(dfs_raw, ignore_index=True)

# Preparing Dimension Data (player profile)
df_dim = pd.DataFrame({
    'player_id': df_raw['PLAYER_ID'],
    'full_name': df_raw['PLAYER_NAME'],
    'sport': 'NBA'
}).drop_duplicates(subset=['player_id'])

# Preparing Fact Data (stats)
df_fact = pd.DataFrame({
    'game_id': df_raw.apply(lambda row: f"NBA_{current_season.replace('-', '_')}_{row['SEASON_TYPE'].replace(' ', '_').upper()}", axis=1),
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

print(f"Successfully saved {len(df_fact)} stats entries for {len(df_dim)} NBA players (Regular Season & Playoffs)!")