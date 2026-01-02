select
  location_id,
  date,

  temp_avg_c,
  avg(temp_avg_c) over (
    partition by location_id
    order by date
    rows between 6 preceding and current row
  ) as temp_avg_7d,

  avg(temp_avg_c) over (
    partition by location_id
    order by date
    rows between 29 preceding and current row
  ) as temp_avg_30d
from {{ ref('fact_weather_daily') }}