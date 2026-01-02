
  
    

  create  table "weather"."analytics_core"."dim_location__dbt_tmp"
  
  
    as
  
  (
    

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
    from "weather"."analytics_staging_models"."stg_locations"
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
  );
  