import os

import altair as alt
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import text

from db import get_engine

st.set_page_config(page_title="Fantasy Sports Optimizer", layout="wide")

load_dotenv()

st.title("Multi-Sport Fantasy Optimizer Dashboard")
st.markdown(
    "Toggle between NBA and NHL data pulled directly from our PostgreSQL database."
)

sport_choice = st.selectbox("Select Sport:", ["NBA", "NHL"])


@st.cache_data
def load_data(sport):
    engine = get_engine()
    if sport == "NBA":
        query = """
        SELECT p.full_name, f.fantasy_points, f.points, f.rebounds, f.assists, f.steals, f.blocks
        FROM raw_data.fact_game_performance f
        JOIN raw_data.dim_players p ON f.player_id = p.player_id AND p.sport = 'NBA'
        WHERE p.sport = 'NBA'
        ORDER BY f.fantasy_points DESC NULLS LAST
        LIMIT 50;
        """
    else:
        query = """
        SELECT p.full_name, f.fantasy_points, f.goals, f.assists, f.shots, f.penalty_minutes, f.plus_minus
        FROM raw_data.fact_nhl_performance f
        JOIN raw_data.dim_players p ON f.player_id = p.player_id AND p.sport = 'NHL'
        WHERE p.sport = 'NHL'
        ORDER BY f.fantasy_points DESC NULLS LAST
        LIMIT 50;
        """
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    engine.dispose()
    df.columns = [c.upper() for c in df.columns]
    return df


df_fantasy = load_data(sport_choice)

top_n = st.slider(
    f"Select number of players to show in {sport_choice} chart:",
    min_value=10,
    max_value=50,
    value=20,
)
col1, col2 = st.columns(2)

with col1:
    st.subheader(f"Top {sport_choice} Performers (Raw Data)")
    st.dataframe(df_fantasy, use_container_width=True)

with col2:
    st.subheader(f"{sport_choice} Fantasy Points Leaderboard")

    df_chart_data = df_fantasy.head(top_n)

    chart = alt.Chart(df_chart_data).mark_bar(color="#1f77b4").encode(
        x=alt.X("FANTASY_POINTS:Q", title="Fantasy Points"),
        y=alt.Y("FULL_NAME:N", sort="-x", title="Player"),
        tooltip=["FULL_NAME", "FANTASY_POINTS"],
    ).interactive()

    st.altair_chart(chart, use_container_width=True)
