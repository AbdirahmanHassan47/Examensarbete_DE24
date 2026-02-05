with source as (
    select *
    from {{ source('sl', 'sl_departures_raw') }}
)

select
    *,
    convert_timezone('UTC', _ingested_at)::timestamp_ntz as ingested_at_ts
from source
