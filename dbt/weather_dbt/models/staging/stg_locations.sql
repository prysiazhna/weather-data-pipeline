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
from {{ source('weather_staging', 'stg_locations') }}