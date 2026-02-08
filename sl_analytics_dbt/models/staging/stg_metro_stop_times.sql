with source as (
    select *
    from {{ source('gtfs_static', 'metro_stop_times') }}
)

select *
from source
