from dotenv import load_dotenv
from sqlalchemy import text

from db import get_engine

load_dotenv()

print("Connecting to PostgreSQL for transformation...")
engine = get_engine()

transform_sql = """
UPDATE raw_data.fact_game_performance
SET fantasy_points =
    (COALESCE(points, 0) * 1.0) +
    (COALESCE(rebounds, 0) * 1.2) +
    (COALESCE(assists, 0) * 1.5) +
    (COALESCE(steals, 0) * 3.0) +
    (COALESCE(blocks, 0) * 3.0);
"""

print("Calculating Fantasy Points based on raw stats...")

try:
    with engine.begin() as conn:
        conn.execute(text(transform_sql))
    print("Transformation complete! Fantasy points updated in PostgreSQL.")
except Exception as e:
    print(f"Error during transformation: {e}")
finally:
    engine.dispose()
    print("Connection closed.")
