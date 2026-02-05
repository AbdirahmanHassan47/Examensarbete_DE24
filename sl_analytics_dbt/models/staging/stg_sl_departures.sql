with source as (
    select *
    from {{ source('sl', 'sl_departures_raw') }}
)

select
    *,
    try_to_timestamp_ntz(_ingested_at) as ingested_at_ts
from source
