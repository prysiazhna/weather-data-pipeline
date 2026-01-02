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
from {{ ref('fact_weather_daily') }} f
join {{ ref('dim_date') }} d using (date)
join {{ ref('dim_location') }} l using (location_id)