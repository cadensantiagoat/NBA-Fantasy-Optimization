import streamlit as st
import snowflake.connector
import os
from dotenv import load_dotenv
import altair as alt

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
top_n = st.slider(f"Select number of players to show in {sport_choice} chart:", min_value=10, max_value=50, value=20)
col1, col2 = st.columns(2)

with col1:
    st.subheader(f"Top {sport_choice} Performers (Raw Data)")
    st.dataframe(df_fantasy, use_container_width=True)

with col2:
    st.subheader(f"{sport_choice} Fantasy Points Leaderboard")

    # Filtering the dataframe based on slider value
    df_chart_data = df_fantasy.head(top_n)

    # Creating advanced horizontal, sorted barchart
    chart = alt.Chart(df_chart_data).mark_bar(color='#1f77b4').encode(
        x=alt.X('FANTASY_POINTS:Q', title='Fantasy Points'),
        y=alt.Y('FULL_NAME:N', sort='-x', title='Player'), # sort='-x' forces highest to lowest!
        tooltip=['FULL_NAME', 'FANTASY_POINTS'] # Adds hover details
    ).interactive()
    
    # Display the chart
    st.altair_chart(chart, use_container_width=True)