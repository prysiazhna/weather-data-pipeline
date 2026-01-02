{{ config(materialized='table') }}

with src as (
    select
        location_id,
        name,
        region,
        country,
        lat,
        lon,
        tz_id,
        local_time,
        ingested_at
    from {{ ref('stg_locations') }}
),

dedup as (
    select
        *,
        row_number() over (
            partition by location_id
            order by ingested_at desc
        ) as rn
    from src
)

select
    location_id,
    name,
    region,
    country,
    lat,
    lon,
    tz_id,
    local_time
from dedup
where rn = 1