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
    routes.route_short_name as line,
    routes.route_long_name as line_name,
    routes.route_id,
    trips.service_id,
    stops.stop_name as station,
    stops.stop_id,
    stop_times.arrival_time,
    stop_times.departure_time,
    stop_times.stop_sequence,
    trips.trip_id
from stop_times
join trips on stop_times.trip_id = trips.trip_id
join routes on trips.route_id = routes.route_id
join stops on stop_times.stop_id = stops.stop_id
