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
    try_to_number(routes.route_short_name) as line_number,
    case
        when lower(routes.route_long_name) like '%röda%' then 'Red'
        when lower(routes.route_long_name) like '%gröna%' then 'Green'
        when lower(routes.route_long_name) like '%blå%' then 'Blue'
        else null
    end as line_color,
    count(distinct trips.trip_id) as trips_count,
    count(distinct stop_times.stop_id) as stops_count,
    count(*) as stop_times_count
from stop_times
join trips on stop_times.trip_id = trips.trip_id
join routes on trips.route_id = routes.route_id
group by
    routes.route_id,
    routes.route_short_name,
    routes.route_long_name,
    try_to_number(routes.route_short_name),
    case
        when lower(routes.route_long_name) like '%röda%' then 'Red'
        when lower(routes.route_long_name) like '%gröna%' then 'Green'
        when lower(routes.route_long_name) like '%blå%' then 'Blue'
        else null
    end
