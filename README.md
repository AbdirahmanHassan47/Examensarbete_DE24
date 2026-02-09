# SL Analytics Data Warehouse

## Project Overview
This project implements a modern data stack to analyze **SL Metro (Tunnelbana)** using **GTFS static data**. The system is batch-based (historical), focused on metro only, and provides a clean dashboard for planning and analysis.

## Architecture
SL GTFS API -> dlt -> Snowflake -> dbt -> Streamlit

Components:
- Data source: Trafiklab SL GTFS Static
- Ingestion: dlt
- Warehouse: Snowflake
- Transformation: dbt
- Analytics: Streamlit dashboard

## Scope / Limitations
- Only metro (tunnelbana) data is analyzed
- Historical/batch data only (no realtime)
- No predictive modeling (focus is a stable platform)

## Data Model
**Staging (views)**
- `stg_metro_routes`
- `stg_metro_trips`
- `stg_metro_stop_times`
- `stg_metro_stops`

**Warehouse (tables)**
- `dim_metro_stops`

**Mart (tables)**
- `mart_metro_overview`
- `mart_station_departures`
- `mart_line_stats`
- `mart_timetable` (optional, can be removed from dashboard)

## Repository Structure
- `dlt_code/` ingestion scripts (GTFS static)
- `sl_analytics_dbt/` dbt project + Streamlit app
- `worsheet_sql/` SQL helpers and exploration queries
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
4. Add API key in `.env`:
   `API_KEY=your_tarfiklab_key`
5. Run ingestion:
   `python dlt_code/gtfs_static_sl.py`
6. Configure dbt profile in `C:\Users\.dbt\profiles.yml`
7. Run dbt:
   `dbt run`
   `dbt test`
8. Run Streamlit (from dbt folder):
   `cd sl_analytics_dbt`
   `python -m streamlit run app.py`

## Notes
- If calendar tables are missing in GTFS, the dashboard will show totals for the full dataset.
- For consistent filters and line colors, ensure marts are up to date by running `dbt run`.

