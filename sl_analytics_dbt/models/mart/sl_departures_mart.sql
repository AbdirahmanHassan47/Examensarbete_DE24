select
    _site_id,
    ingested_date,
    count(*) as departures_count
from {{ ref('fct_sl_departures') }}
group by
    _site_id,
    ingested_date
