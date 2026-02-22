import streamlit as st
import snowflake.connector
import os
from dotenv import load_dotenv

# Setting page layout
st.set_page_config(page_title="Fantasy Sports Optimizer", layout="wide")

# Load credentials
load_dotenv()

st.title("NBA Fantasy Optimizer Dashboard")
st.markdown("This dashboard pulls transformed data directly from our Snowflake Data Warehouse.")

# Functino that pulls data from Snowflake
@st.cache_data
def load_data():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        database='NBA_FANTASY_DB',
        schema='RAW_DATA'
    )

    # Query to join our tables and get the top fantasy performers
    query = """
    SELECT p.full_name, f.fantasy_points, f.points, f.assists, f.steals, f.blocks
    FROM fact_game_performance f
    JOIN dim_players p ON f.player_id = p.player_id
    ORDER BY f.fantasy_points DESC;
    """

    cursor = conn.cursor()
    cursor.execute(query)

    # Fetch results directly into a Pandas DataFrame
    df = cursor.fetch_pandas_all()
    conn.close()
    
    return df

# Loading data
df_fantasy = load_data()

# Building UI elements
col1, col2 = st.columns(2)

with col1:
    st.subheader("Top Performers (Raw Data)")

    # Display interactive dataframe
    st.dataframe(df_fantasy, use_container_width=True)

with col2:
    st.subheader("Fantasy Points Leaderboard")
    st.bar_chart(data=df_fantasy, x='FULL_NAME', y='FANTASY_POINTS')