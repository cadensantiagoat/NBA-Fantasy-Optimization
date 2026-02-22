import requests
import pandas as pd
import os
from datetime import datetime

# Setting up folder
DATA_FOLDER = "nhl_data"
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

print("Fetching Data from public NHL API...")

# Pulling stats for a few teams to populate database
teams = ["BOS", "TOR", "NYR", "COL"]
all_players = []
all_stats = []

for team in teams:
    # official NHL API endpoint for a team's current stats
    url = f"https://api-web.nhle.com/v1/club-stats/{team}/now"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        skaters = data.get("skaters", [])

        for skater in skaters:
            # DIMENSION DATA (Player Profile)
            player_id = skater.get("playerId")
            first_name = skater.get(f"firstName", {}).get("default", "")
            last_name = skater.get("lastName", {}).get("default", "")
            full_name = f"{first_name} {last_name}".strip()

            all_players.append({
                "player_id": player_id,
                "full_name": full_name,
                "sport": "NHL"      # added sport column to clarify which table we want
            })

            # FACT DATA (Stats)
            all_stats.append({
                "game_id": f"NHL_2024_{team}", 
                "player_id": player_id,
                "team_id": team,
                "game_date": datetime.today().strftime('%Y-%m-%d'),
                "goals": skater.get("goals", 0),
                "assists": skater.get("assists", 0),
                "shots": skater.get("shots", 0),
                "penalty_minutes": skater.get("penaltyMinutes", 0),
                "plus_minus": skater.get("plusMinus", 0)
            })

# Converting to Pandas DataFrames
df_dim = pd.DataFrame(all_players).drop_duplicates(subset=["player_id"])
df_fact = pd.DataFrame(all_stats)

# Saving to CSVs
date_str = datetime.today().strftime('%Y%m%d')
dim_path = f"{DATA_FOLDER}/dim_players_nhl_{date_str}.csv"
fact_path = f"{DATA_FOLDER}/fact_nhl_perf_{date_str}.csv"

df_dim.to_csv(dim_path, index=False)
df_fact.to_csv(fact_path, index=False)

print(f"Succesfully extracted {len(df_dim)} NHL players and saved their stats to CSV!")