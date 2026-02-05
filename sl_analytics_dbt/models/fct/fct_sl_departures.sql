select
    *,
    date_trunc('day', ingested_at_ts) as ingested_date
from {{ ref('stg_sl_departures') }}
