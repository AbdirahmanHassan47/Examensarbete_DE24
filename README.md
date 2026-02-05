# SL Analytics Data Warehouse

## Project Overview
This project implements a modern data stack to analyze SL public transport departures. The system automates extraction, loading, transformation, and visualization to support data-driven insights.

## Architecture
SL API -> dlt -> Snowflake -> dbt -> Streamlit

Components:
- Data source: SL Transport API
- Ingestion: dlt
- Warehouse: Snowflake
- Transformation: dbt
- Analytics: Streamlit dashboard

## Data Model
Staging:
- `stg_sl_departures` (view)

Warehouse:
- `fct_sl_departures` (table)

Mart:
- `sl_departures_mart` (table, daily counts by site)

## Repository Structure
- `dlt_code/` ingestion scripts
- `sl_analytics_dbt/` dbt project
- `projekt_sl_analytics/` Streamlit app
- `worksheets_sql/` SQL helpers and exploration queries
- `requirements.txt`

## Getting Started
Prerequisites:
- Python 3.9+
- Snowflake account
- dbt

Setup:
1. Create and activate a virtual environment
2. Install dependencies:
   `pip install -r requirements.txt`
3. Configure dlt secrets in `dlt_code/.dlt/secrets.toml`
4. Run ingestion:
   `python dlt_code/dlt_laod_sl.py`
5. Configure dbt profile in `C:\Users\Abdirahman\.dbt\profiles.yml`
6. Run dbt:
   `dbt run`
   `dbt test`
7. Run Streamlit:
   `streamlit run projekt_sl_analytics/app.py`

