select
  location_id,
  date,
  temp_avg_c,
  temp_avg_c - lag(temp_avg_c, 7) over (partition by location_id order by date) as temp_wow_change
from "weather"."analytics_core"."fact_weather_daily"