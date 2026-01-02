
  create view "weather"."analytics_bi"."weather_daily_bi__dbt_tmp"
    
    
  as (
    select
    d.date,
    d.year,
    d.month,
    l.location_id,
    l.name as city,
    l.country,
    f.temp_avg_c,
    f.temp_min_c,
    f.temp_max_c,
    f.precip_mm
from "weather"."analytics_core"."fact_weather_daily" f
join "weather"."analytics_core"."dim_date" d using (date)
join "weather"."analytics_core"."dim_location" l using (location_id)
  );