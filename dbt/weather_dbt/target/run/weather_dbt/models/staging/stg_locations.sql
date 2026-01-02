
  create view "weather"."analytics_staging_models"."stg_locations__dbt_tmp"
    
    
  as (
    select
  location_id::text as location_id,
  name::text as name,
  region::text as region,
  country::text as country,
  lat::numeric as lat,
  lon::numeric as lon,
  tz_id::text as tz_id,
  local_time as local_time,
  ingested_at as ingested_at
from "weather"."staging"."stg_locations"
  );