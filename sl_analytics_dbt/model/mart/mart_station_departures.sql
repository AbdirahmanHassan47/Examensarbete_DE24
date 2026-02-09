with stop_times as (
    select * from {{ ref("stg_metro_stop_times") }}
),
stops as (
    select * from {{ ref("stg_metro_stops") }}
),
trips as (
    select * from {{ ref("stg_metro_trips") }}
),
routes as (
    select * from {{ ref("stg_metro_routes") }}
)

select
    stops.stop_id,
    stops.stop_name,
    routes.route_id,
    routes.route_short_name,
    routes.route_long_name,
    count(*) as planned_departures
from stop_times
join stops on stop_times.stop_id = stops.stop_id
left join trips on stop_times.trip_id = trips.trip_id
left join routes on trips.route_id = routes.route_id
group by
    stops.stop_id,
    stops.stop_name,
    routes.route_id,
    routes.route_short_name,
    routes.route_long_name
