import os
import snowflake.connector
from dotenv import load_dotenv

# Loading .env file for credentials
load_dotenv()

print("Connecting to Snowflake for Tranformation...")
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse="COMPUTE_WH",
    database='NBA_FANTASY_DB',
    schema='RAW_DATA'
)

cursor = conn.cursor()

# Transformation SQL

# Calculates fantasy score based on standard DFS rules
transform_sql = """
UPDATE fact_game_performance
SET fantasy_points =
    (COALESCE(points, 0) * 1.0) +
    (COALESCE(rebounds, 0) * 1.2) +
    (COALESCE(assists, 0) * 1.5) +
    (COALESCE(steals, 0) * 3.0) +
    (COALESCE(blocks, 0) * 3.0);
"""

print("Calculating Fantasy Points based on raw stats...")

try:
    # executing SQL
    cursor.execute(transform_sql)
    print("Transformation complete! Fantasy points updated in Snowflake.")
except Exception as e:
    print(f"Error during transformation: {e}")
finally:
    # Clean up the connection
    cursor.close()
    conn.close()
    print("Connection closed.")