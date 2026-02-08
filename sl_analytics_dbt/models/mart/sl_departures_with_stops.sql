with departures as (
    select *
    from {{ ref('stg_sl_departures') }}
),
stops as (
    select *
    from {{ ref('stg_metro_stops') }}
)

select
    d.*,
    s.stop_name as metro_stop_name,
    s.stop_lat as metro_stop_lat,
    s.stop_lon as metro_stop_lon
from departures d
left join stops s
    on {{ departures_stop_join_condition(ref('stg_sl_departures'), 'd', 's') }}
