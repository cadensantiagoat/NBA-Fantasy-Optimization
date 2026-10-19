import requests
import pandas as pd
import os
import sys
from datetime import datetime

# ESPN's public stats API (stats.nba.com blocks cloud IPs like GitHub Actions)
URL = "https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/statistics/byathlete"

# Setting up folder
DATA_FOLDER = "nba_data"
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

print("Fetching NBA data from ESPN...")

# Without a season param ESPN returns the latest regular season with stats (Per Game avgs)
response = requests.get(URL, params={"limit": 1000}, timeout=30)
response.raise_for_status()
data = response.json()
athletes = data.get("athletes", [])

# load_nba.py truncates before inserting, so never hand it an empty file
if not athletes:
    sys.exit("No NBA stats returned; aborting so existing data isn't wiped.")

season = data["requestedSeason"]["year"]
all_players = []
all_stats = []

for entry in athletes:
    athlete = entry["athlete"]
    # Each category's values line up with the top-level category's stat names
    stats = {}
    for meta, cat in zip(data["categories"], entry["categories"]):
        stats.update(zip(meta["names"], cat["totals"]))

    # DIMENSION DATA (Player Profile)
    all_players.append({
        "player_id": int(athlete["id"]),
        "full_name": athlete["displayName"],
        "sport": "NBA",
    })

    # FACT DATA (Stats)
    all_stats.append({
        "game_id": f"NBA_{season}_SEASON",
        "player_id": int(athlete["id"]),
        "team_id": athlete.get("teamShortName"),
        "game_date": datetime.today().strftime('%Y-%m-%d'),
        "points": float(stats["avgPoints"]),
        "rebounds": float(stats["avgRebounds"]),
        "assists": float(stats["avgAssists"]),
        "steals": float(stats["avgSteals"]),
        "blocks": float(stats["avgBlocks"]),
    })

df_dim = pd.DataFrame(all_players).drop_duplicates(subset=["player_id"])
df_fact = pd.DataFrame(all_stats)

# Saving to CSVs
date_str = datetime.today().strftime('%Y%m%d')
df_dim.to_csv(f"{DATA_FOLDER}/dim_players_nba_{date_str}.csv", index=False)
df_fact.to_csv(f"{DATA_FOLDER}/fact_nba_perf_{date_str}.csv", index=False)

print(f"Successfully saved stats for {len(df_dim)} NBA players!")
