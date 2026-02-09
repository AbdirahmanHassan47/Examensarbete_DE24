import snowflake.connector
import streamlit as st
from cryptography.hazmat.primitives import serialization


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
def get_connection():
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
        schema=cfg.get("schema", "STATIC"),
        private_key=private_key,
    )


@st.cache_data
def query_df(sql: str):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        return rows, cols
    finally:
        cur.close()


def has_calendar(db: str, schema: str) -> bool:
    rows, cols = query_df(
        f"""
        select count(*) as cnt
        from {db}.information_schema.tables
        where table_schema = '{schema}'
          and table_name in ('METRO_CALENDAR','METRO_CALENDAR_DATES')
        """
    )
    if not rows:
        return False
    return int(rows[0][0]) > 0


def table_has_column(db: str, schema: str, table: str, column: str) -> bool:
    rows, cols = query_df(
        f"""
        select count(*) as cnt
        from {db}.information_schema.columns
        where table_schema = '{schema}'
          and table_name = '{table}'
          and column_name = '{column}'
        """
    )
    if not rows:
        return False
    return int(rows[0][0]) > 0


def service_cte(db: str, schema: str, date_str: str, day_col: str) -> str:
    return f"""
    with base_services as (
        select service_id
        from {db}.{schema}.METRO_CALENDAR
        where start_date <= '{date_str}'
          and end_date >= '{date_str}'
          and {day_col} = 1
    ),
    exceptions as (
        select service_id, exception_type
        from {db}.{schema}.METRO_CALENDAR_DATES
        where date = '{date_str}'
    ),
    valid_services as (
        select service_id from base_services
        union
        select service_id from exceptions where exception_type = 1
        minus
        select service_id from exceptions where exception_type = 2
    )
    """


def service_join(use_calendar: bool) -> str:
    return "join valid_services vs on t.service_id = vs.service_id" if use_calendar else ""
