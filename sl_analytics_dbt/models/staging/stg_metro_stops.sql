with source as (
    select *
    from {{ source('gtfs_static', 'metro_stops') }}
)

select *
from source
