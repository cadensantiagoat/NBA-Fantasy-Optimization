from dotenv import load_dotenv
from sqlalchemy import text

from db import get_engine

load_dotenv()

print("Connecting to PostgreSQL...")
engine = get_engine()

sql = """
UPDATE raw_data.fact_nhl_performance
SET fantasy_points =
    (COALESCE(goals, 0) * 8.5) +
    (COALESCE(assists, 0) * 5.0) +
    (COALESCE(shots, 0) * 1.5);
"""

print("Transforming NHL data and calculating fantasy points...")
with engine.begin() as conn:
    conn.execute(text(sql))
print("Transformation complete! NHL Fantasy points have been calculated and saved.")

engine.dispose()
