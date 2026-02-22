import streamlit as st
import snowflake.connector
import os
from dotenv import load_dotenv

# Set page layout
st.set_page_config(page_title="Fantasy Sports Optimizer", layout="wide")

# Load credentials
load_dotenv()

st.title("Multi-Sport Fantasy Optimizer Dashboard")
st.markdown("Toggle between NBA and NHL data pulled directly from our Snowflake Data Warehouse.")

# 1. Add an interactive dropdown filter!
sport_choice = st.selectbox("Select Sport:", ["NBA", "NHL"])

@st.cache_data
def load_data(sport):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        database='NBA_FANTASY_DB',
        schema='RAW_DATA'
    )
    
    cursor = conn.cursor()
    
    # 2. Dynamically change the SQL query based on the user's dropdown selection
    if sport == "NBA":
        query = """
        SELECT p.full_name, f.fantasy_points, f.points, f.rebounds, f.assists, f.steals, f.blocks
        FROM fact_game_performance f
        JOIN dim_players p ON f.player_id = p.player_id
        WHERE p.sport = 'NBA'
        ORDER BY f.fantasy_points DESC
        LIMIT 50;
        """
    else:
        query = """
        SELECT p.full_name, f.fantasy_points, f.goals, f.assists, f.shots, f.penalty_minutes, f.plus_minus
        FROM fact_nhl_performance f
        JOIN dim_players p ON f.player_id = p.player_id
        WHERE p.sport = 'NHL'
        ORDER BY f.fantasy_points DESC
        LIMIT 50;
        """
        
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    conn.close()
    
    return df

# 3. Load the data using the selected sport
df_fantasy = load_data(sport_choice)

# 4. Build the UI elements
col1, col2 = st.columns(2)

with col1:
    st.subheader(f"Top {sport_choice} Performers (Raw Data)")
    st.dataframe(df_fantasy, use_container_width=True)

with col2:
    st.subheader(f"{sport_choice} Fantasy Points Leaderboard")
    st.bar_chart(data=df_fantasy, x='FULL_NAME', y='FANTASY_POINTS')