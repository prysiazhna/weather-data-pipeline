with bounds as (
  select
    min(date) as min_date,
    max(date) as max_date
  from "weather"."analytics_staging_models"."stg_weather_daily"
),
dates as (
  select generate_series(min_date, max_date, interval '1 day')::date as date
  from bounds
)
select
  date,
  extract(year from date)::int as year,
  extract(month from date)::int as month,
  extract(day from date)::int as day,
  extract(isodow from date)::int as iso_dow,
  to_char(date, 'YYYY-MM') as year_month
from dates