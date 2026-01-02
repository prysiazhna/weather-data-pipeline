select
  w.location_id,
  w.date,
  w.dt,

  w.temp_min_c,
  w.temp_max_c,
  w.temp_avg_c,
  w.precip_mm,
  w.humidity_avg,
  w.wind_max_kph,
  w.condition_code,
  w.condition_text,

  w.ingested_at
from "weather"."analytics_staging_models"."stg_weather_daily" w