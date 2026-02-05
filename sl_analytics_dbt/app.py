import pandas as pd
import streamlit as st
import snowflake.connector
from cryptography.hazmat.primitives import serialization


st.set_page_config(page_title="SL Departures Analytics", layout="wide")


def _load_private_key(path: str, passphrase: str | None) -> bytes:
    with open(path, "rb") as f:
        key_data = f.read()
    password = passphrase.encode() if passphrase else None
    pkey = serialization.load_pem_private_key(key_data, password=password)
    return pkey.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


@st.cache_resource
def _get_conn():
    cfg = st.secrets["snowflake"]
    private_key = _load_private_key(
        cfg["private_key_path"], cfg.get("private_key_passphrase", "")
    )
    return snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        role=cfg["role"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg.get("schema", "MART"),
        private_key=private_key,
    )


@st.cache_data
def _query_df(sql: str) -> pd.DataFrame:
    conn = _get_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()


st.title("SL Departures Analytics")

cfg = st.secrets["snowflake"]
db = cfg["database"]
mart_schema = cfg.get("schema", "MART")

mart_table = f"{db}.{mart_schema}.SL_DEPARTURES_MART"
staging_table = f"{db}.STAGING.STG_SL_DEPARTURES"

with st.sidebar:
    st.header("Filters")
    sites_df = _query_df(f"select distinct _site_id from {mart_table} order by _site_id")
    site_ids = sites_df["_SITE_ID"].tolist() if not sites_df.empty else []
    site_choice = st.selectbox("Site", ["All"] + site_ids)

    date_df = _query_df(
        f"select min(ingested_date) as min_d, max(ingested_date) as max_d from {mart_table}"
    )
    if not date_df.empty:
        min_d = date_df.loc[0, "MIN_D"]
        max_d = date_df.loc[0, "MAX_D"]
        date_range = st.date_input("Date range", (min_d, max_d))
    else:
        date_range = None

st.subheader("Overview")
metrics_df = _query_df(
    f"""
    select
        count(*) as total_rows,
        count(distinct _site_id) as total_sites,
        max(ingested_date) as latest_date
    from {mart_table}
    """
)

if not metrics_df.empty:
    c1, c2, c3 = st.columns(3)
    c1.metric("Rows", int(metrics_df.loc[0, "TOTAL_ROWS"]))
    c2.metric("Sites", int(metrics_df.loc[0, "TOTAL_SITES"]))
    c3.metric("Latest Date", str(metrics_df.loc[0, "LATEST_DATE"]))

st.subheader("Daily Departures")
where_clauses = []
if site_choice != "All":
    where_clauses.append(f"_site_id = {int(site_choice)}")
if date_range:
    start_d, end_d = date_range
    where_clauses.append(f"ingested_date between '{start_d}' and '{end_d}'")
where_sql = "where " + " and ".join(where_clauses) if where_clauses else ""

series_df = _query_df(
    f"""
    select
        ingested_date,
        sum(departures_count) as departures_count
    from {mart_table}
    {where_sql}
    group by ingested_date
    order by ingested_date
    """
)
st.line_chart(series_df, x="INGESTED_DATE", y="DEPARTURES_COUNT")

st.subheader("Top Sites")
top_df = _query_df(
    f"""
    select _site_id, sum(departures_count) as departures_count
    from {mart_table}
    {where_sql}
    group by _site_id
    order by departures_count desc
    limit 10
    """
)
st.dataframe(top_df, width="stretch")

st.subheader("Latest Raw Rows")
raw_df = _query_df(f"select * from {staging_table} order by ingested_at_ts desc limit 20")
st.dataframe(raw_df, width="stretch")
