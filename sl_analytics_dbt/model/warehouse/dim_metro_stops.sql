select
    stop_id,
    stop_name,
    stop_lat,
    stop_lon
from {{ ref('stg_metro_stops') }}
