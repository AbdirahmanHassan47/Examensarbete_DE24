with stop_times as (
    select * from {{ ref("stg_metro_stop_times") }}
),
trips as (
    select * from {{ ref("stg_metro_trips") }}
),
routes as (
    select * from {{ ref("stg_metro_routes") }}
)

select
    routes.route_id,
    routes.route_short_name,
    routes.route_long_name,
    count(distinct trips.trip_id) as trips_count,
    count(distinct stop_times.stop_id) as stops_count,
    count(*) as stop_times_count
from stop_times
join trips on stop_times.trip_id = trips.trip_id
join routes on trips.route_id = routes.route_id
group by
    routes.route_id,
    routes.route_short_name,
    routes.route_long_name
