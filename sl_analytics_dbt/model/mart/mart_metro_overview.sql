with routes as (
    select * from {{ ref("stg_metro_routes") }}
),
trips as (
    select * from {{ ref("stg_metro_trips") }}
),
stop_times as (
    select * from {{ ref("stg_metro_stop_times") }}
),
stops as (
    select * from {{ ref("stg_metro_stops") }}
)

select
    (select count(*) from routes) as routes_count,
    (select count(*) from trips) as trips_count,
    (select count(*) from stop_times) as stop_times_count,
    (select count(*) from stops) as stops_count
