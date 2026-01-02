
  create view "weather"."analytics_staging_models"."stg_weather_daily__dbt_tmp"
    
    
  as (
    with src as (
  select *
  from "weather"."staging"."stg_weather_daily"
)

select
  dt::date as dt,
  location_id::text as location_id,
  date::date as date,

  temp_min_c::numeric as temp_min_c,
  temp_max_c::numeric as temp_max_c,
  temp_avg_c::numeric as temp_avg_c,

  precip_mm::numeric as precip_mm,
  humidity_avg::numeric as humidity_avg,
  wind_max_kph::numeric as wind_max_kph,

  condition_code::int as condition_code,
  condition_text::text as condition_text,

  ingested_at::timestamptz as ingested_at
from src
  );