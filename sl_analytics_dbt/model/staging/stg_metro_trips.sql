with source as (
    select *
    from {{ source('gtfs_static', 'metro_trips') }}
)

select *
from source
