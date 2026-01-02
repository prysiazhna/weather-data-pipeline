with base as (
  select
    f.location_id,
    f.date,
    f.temp_min_c,
    f.temp_max_c,
    f.temp_avg_c,
    f.precip_mm,
    f.humidity_avg,
    f.wind_max_kph
  from {{ ref('fact_weather_daily') }} f
),

weekly as (
  select
    location_id,
    date_trunc('week', date)::date as week_start,
    (date_trunc('week', date)::date + interval '6 day')::date as week_end,

    count(*) as days_in_week,

    avg(temp_avg_c)::numeric(10,2) as temp_avg_c_wk,
    min(temp_min_c)::numeric(10,2) as temp_min_c_wk,
    max(temp_max_c)::numeric(10,2) as temp_max_c_wk,

    sum(coalesce(precip_mm, 0))::numeric(10,2) as precip_mm_wk,
    avg(humidity_avg)::numeric(10,2) as humidity_avg_wk,
    max(wind_max_kph)::numeric(10,2) as wind_max_kph_wk
  from base
  group by 1, 2, 3
)

select
  w.week_start,
  w.week_end,

  extract(isoyear from w.week_start)::int as iso_year,
  extract(week from w.week_start)::int as iso_week,

  w.location_id,
  l.name as city,
  l.country,
  l.region,
  l.tz_id,

  w.days_in_week,
  w.temp_avg_c_wk,
  w.temp_min_c_wk,
  w.temp_max_c_wk,
  w.precip_mm_wk,
  w.humidity_avg_wk,
  w.wind_max_kph_wk
from weekly w
join {{ ref('dim_location') }} l using (location_id)
order by w.week_start, w.location_id