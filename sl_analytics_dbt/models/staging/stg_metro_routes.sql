with source as (
    select *
    from {{ source('gtfs_static', 'metro_routes') }}
)

select *
from source
