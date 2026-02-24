# Multi-Sport Fantasy Optimization Pipeline & Dashboard

An end-to-end, fully automated data engineering pipeline that extracts live NBA and NHL player performance data, transforms it to calculate Daily Fantasy Sports (DFS) points, loads it into a Snowflake Cloud Data Warehouse, and serves it to an interactive web dashboard.

## Live Dashboard
**[(https://fantasy-optimization.streamlit.app/)]**

<img width="1197" height="551" alt="image" src="https://github.com/user-attachments/assets/ae98d6f7-81e1-4761-8c8d-9efbd5f08261" />
<img width="1172" height="555" alt="image" src="https://github.com/user-attachments/assets/e64eb253-5688-456b-9a94-597c2eaf7749" />


## Tech Stack
* **Language:** Python 3.10
* **Data Warehouse:** Snowflake
* **Frontend:** Streamlit, Altair (for interactive horizontal bar charts)
* **Automation & CI/CD:** GitHub Actions
* **Libraries:** Pandas, `snowflake-connector-python[pandas]`, `nba_api`, `requests`, `python-dotenv`

## Architecture
The project follows a Star Schema design optimized for analytical queries and dashboarding:
* **Fact Tables:** `fact_game_performance` (NBA stats) and `fact_nhl_performance` (NHL stats)
* **Dimension Table:** `dim_players` (Player metadata and sport classification)

## Key Features
* **Fully Automated Daily ETL/ELT:** A GitHub Actions Cron job spins up a cloud server daily to run the extraction, loading, and transformation scripts without manual intervention.
* **Firewall Evasion & Web Scraping:** Uses a custom residential proxy network to successfully pull live JSON data from strict sports endpoints (stats.nba.com).
* **Idempotent Cloud Loads:** Utilizes `TRUNCATE TABLE` before `write_pandas` inserts to ensure perfectly clean, duplicate-free daily data refreshes in Snowflake.
* **Custom DFS Scoring:** Uses SQL to calculate real-world DraftKings Daily Fantasy scoring models for both basketball and hockey directly in the data warehouse.
* **Interactive Data Visualization:** A Streamlit frontend that queries Snowflake in real-time, featuring dynamic Altair horizontal bar charts with interactive sliders and tooltips.

## Getting Started (Local Development)

1. **Clone the repository:**
   git clone [https://github.com/cadensantiagoat/NBA-Fantasy-Optimization.git](https://github.com/cadensantiagoat/NBA-Fantasy-Optimization.git)

2. **Install Requrements:**
   pip install -r requirements.txt

3. **Configure Environment Variables**
   Create a .env file in the root directory and add your Snowflake and Proxy credentials:
   SNOWFLAKE_USER="your_username"
   SNOWFLAKE_PASSWORD="your_password"
   SNOWFLAKE_ACCOUNT="your_account_identifier"
   PROXY_URL="your_webshare_proxy_url"

4. **Run the Extraction Scripts (API to CSV)**
   python extract_nba.py
   python extract_nhl.py

5. **Run the Load Scripts (CSV to Snowflake)**
   python load_nba.py
   python load_nhl.py

6. **Run the Transformation Scripts (Calculate Fantasy Points)**
   python transform_nba.py
   python transform_nhl.py

7. **Launch the Dashboard:**
   streamlit run dashboard.py
