import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse="COMPUTE_WH",
    database='NBA_FANTASY_DB',
    schema='RAW_DATA'
)

cursor = conn.cursor()

# Using DraftKings Daily Fantasy scoring model for NHL stats
sql = """
UPDATE fact_nhl_performance
SET fantasy_points = (goals * 8.5) + (assists * 5.0) + (shots * 1.5);
"""

print("Transforming NHL data and calculating fantasy points...")
cursor.execute(sql)
print("Transformation complete! NHL Fantasy points have been calculated and saved.")

cursor.close()
conn.close()