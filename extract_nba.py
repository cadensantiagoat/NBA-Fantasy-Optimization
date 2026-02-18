import pandas as pd
import random
import os

DATA_FOLDER = "nba_data"
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

print("NBA API is blocking our connection. Switching to Mock Data Generator...")

# A list of real player IDs and names
players = [
    (2544, "LeBron James"), (201939, "Stephen Curry"), (203999, "Nikola Jokic"), 
    (1629029, "Luka Doncic"), (203507, "Giannis Antetokounmpo"), (1628369, "Jayson Tatum")
]

# 1. Create Dimension Data (The Nouns)
df_dim = pd.DataFrame(players, columns=['player_id', 'full_name'])

# 2. Create Fact Data (The Verbs/Stats)
fact_data = []
for pid, name in players:
    fact_data.append({
        'game_id': '0022300001',
        'player_id': pid,
        'team_id': random.randint(1610612737, 1610612766),
        'game_date': '2026-02-11',
        'points': random.randint(15, 40),
        'assists': random.randint(2, 12),
        'rebounds': random.randint(3, 15),
        'steals': random.randint(0, 3),
        'blocks': random.randint(0, 3),
        'turnovers': random.randint(0, 5),
        'minutes_played': round(random.uniform(25.0, 40.0), 1)
    })

df_fact = pd.DataFrame(fact_data)

# 3. Save to CSVs
df_fact.to_csv(f"{DATA_FOLDER}/fact_perf_20260211.csv", index=False)
df_dim.to_csv(f"{DATA_FOLDER}/dim_players_20260211.csv", index=False)

print("Success! Generated local CSVs safely.")