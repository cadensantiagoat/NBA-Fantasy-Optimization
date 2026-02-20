NBA Fantasy Optimization Pipeline:
An end-to-end data engineering pipeline that extracts NBA player performance data, transforms it using Python and Pandas, and loads it into a Snowflake Cloud Data Warehouse using a Star Schema architecture.

Note:
During development, external API restrictions blocked live data fetching. To maintain development velocity, I implemented a Mock Data Engine (extract_nba.py) that generates schema-accurate player and game stats, allowing for continuous testing of the Snowflake integration.

Tech Stack
Language: Python 3.x

Data Warehouse: Snowflake

Libraries: Pandas, Snowflake-Connector-Python, Dotenv

Database Logic: SQL (DDL, DML, MERGE)

Architecture
The project follows a Star Schema design to optimize for analytical queries:

Fact Table: fact_game_performance (Points, Assists, Rebounds, Steals, etc.)

Dimension Table: dim_players (Player metadata)

Staging Area: Temporary tables (stg_) used for data validation before the final MERGE

Key Features
Idempotent Loads: Uses SQL MERGE logic to prevent duplicate records during daily stat updates.

Automated Schema: Includes setup_database.sql for environment initialization.

Secure Configuration: Utilizes environment variables for database authentication.

Getting Started
Clone the repo.

Create a .env file with your Snowflake credentials.

Run setup_database.sql in your Snowflake worksheet.

Execute extract_nba.py to generate data.

Execute load_nba.py to push to the cloud.
