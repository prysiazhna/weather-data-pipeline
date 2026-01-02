with base as (
  select
    location_id,
    date,
    temp_avg_c,
    avg(temp_avg_c) over (partition by location_id) as mean_temp,
    stddev_samp(temp_avg_c) over (partition by location_id) as sd_temp
  from "weather"."analytics_core"."fact_weather_daily"
)
select
  location_id,
  date,
  temp_avg_c,
  case
    when sd_temp is null or sd_temp = 0 then null
    else (temp_avg_c - mean_temp) / sd_temp
  end as z_score
from base