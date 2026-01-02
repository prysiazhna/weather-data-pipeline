with base as (
  select
    f.location_id,
    f.date,
    f.temp_avg_c,
    f.precip_mm,
    f.wind_max_kph
  from {{ ref('fact_weather_daily') }} f
),

calc as (
  select
    location_id,
    date,
    temp_avg_c,
    precip_mm,
    wind_max_kph,

    avg(temp_avg_c) over (
      partition by location_id
      order by date
      rows between 7 preceding and 1 preceding
    )::numeric(10,2) as temp_baseline_7d,

    avg(coalesce(precip_mm, 0)) over (
      partition by location_id
      order by date
      rows between 7 preceding and 1 preceding
    )::numeric(10,2) as precip_baseline_7d
  from base
),

final as (
  select
    location_id,
    date,
    temp_avg_c,
    temp_baseline_7d,
    (temp_avg_c - temp_baseline_7d)::numeric(10,2) as temp_anomaly_vs_7d,

    precip_mm,
    precip_baseline_7d,
    (coalesce(precip_mm, 0) - coalesce(precip_baseline_7d, 0))::numeric(10,2) as precip_anomaly_vs_7d,

    wind_max_kph
  from calc
  where temp_baseline_7d is not null
)

select
  f.date,
  d.year,
  d.month,
  d.day,
  d.iso_dow,
  d.year_month,

  f.location_id,
  l.name as city,
  l.country,
  l.region,
  l.tz_id,

  f.temp_avg_c,
  f.temp_baseline_7d,
  f.temp_anomaly_vs_7d,

  f.precip_mm,
  f.precip_baseline_7d,
  f.precip_anomaly_vs_7d,

  f.wind_max_kph,

  (abs(f.temp_anomaly_vs_7d) >= 5)::boolean as is_temp_anomaly_5c
from final f
join {{ ref('dim_date') }} d using (date)
join {{ ref('dim_location') }} l using (location_id)
order by f.date, f.location_id