-- sql/performance_queries.sql
-- Purpose: Performance demo for Postgres using EXPLAIN (ANALYZE, BUFFERS)
-- Dataset: Weather Lakehouse (analytics_core)
-- Tables:
--   - analytics_core.fact_weather_daily
--   - analytics_core.dim_location
--
-- Run order:
--   1) Run section A (EXPLAIN BEFORE indexes)
--   2) Create indexes (section B)
--   3) Run section C (EXPLAIN AFTER indexes)

-- =========================================================
-- A) EXPLAIN BEFORE INDEXES
-- =========================================================

-- A0) Quick sanity: what is the data range and one sample location_id?
SELECT
  MIN(date) AS min_date,
  MAX(date) AS max_date,
  COUNT(*)  AS rows_total
FROM analytics_core.fact_weather_daily;

SELECT location_id, country, region, name
FROM analytics_core.dim_location
ORDER BY location_id
LIMIT 10;

-- A1) BI-like query: filter by location_id + date range
--     Typical dashboard query: show daily metrics for one city in a period.
WITH params AS (
  SELECT
    (SELECT location_id FROM analytics_core.dim_location ORDER BY location_id LIMIT 1) AS location_id,
    (SELECT MAX(date) - INTERVAL '30 day' FROM analytics_core.fact_weather_daily)      AS date_from,
    (SELECT MAX(date) FROM analytics_core.fact_weather_daily)                          AS date_to
)
EXPLAIN (ANALYZE, BUFFERS)
SELECT
  f.location_id,
  f.date,
  f.temp_avg_c,
  f.precip_mm,
  f.wind_max_kph
FROM analytics_core.fact_weather_daily f
JOIN params p ON p.location_id = f.location_id
WHERE f.date BETWEEN p.date_from::date AND p.date_to::date
ORDER BY f.date;

-- A2) Rolling window query (7-day rolling average)
--     Typical analytics query for trends.
WITH params AS (
  SELECT
    (SELECT location_id FROM analytics_core.dim_location ORDER BY location_id LIMIT 1) AS location_id,
    (SELECT MAX(date) - INTERVAL '60 day' FROM analytics_core.fact_weather_daily)      AS date_from,
    (SELECT MAX(date) FROM analytics_core.fact_weather_daily)                          AS date_to
)
EXPLAIN (ANALYZE, BUFFERS)
SELECT
  f.location_id,
  f.date,
  f.temp_avg_c,
  AVG(f.temp_avg_c) OVER (
    PARTITION BY f.location_id
    ORDER BY f.date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS temp_avg_7d
FROM analytics_core.fact_weather_daily f
JOIN params p ON p.location_id = f.location_id
WHERE f.date BETWEEN p.date_from::date AND p.date_to::date
ORDER BY f.date;

-- A3) Join + filter by country (aggregation per day)
--     Typical BI query: show country-level daily avg temperature.
WITH params AS (
  SELECT
    (SELECT country FROM analytics_core.dim_location WHERE country IS NOT NULL ORDER BY country LIMIT 1) AS country,
    (SELECT MAX(date) - INTERVAL '30 day' FROM analytics_core.fact_weather_daily)                         AS date_from,
    (SELECT MAX(date) FROM analytics_core.fact_weather_daily)                                             AS date_to
)
EXPLAIN (ANALYZE, BUFFERS)
SELECT
  d.country,
  f.date,
  AVG(f.temp_avg_c)::numeric(10,2) AS avg_temp_avg_c
FROM analytics_core.fact_weather_daily f
JOIN analytics_core.dim_location d
  ON d.location_id = f.location_id
JOIN params p
  ON d.country = p.country
WHERE f.date BETWEEN p.date_from::date AND p.date_to::date
GROUP BY d.country, f.date
ORDER BY f.date;

-- =========================================================
-- B) INDEXES TO ADD (run once, then re-run EXPLAIN)
-- =========================================================
-- These indexes target the access patterns above:
-- - filter by (location_id, date) + ordering by date
-- - filter by date range
-- - join on location_id
-- - filter dim by country

CREATE INDEX IF NOT EXISTS idx_fact_weather_daily_location_date
  ON analytics_core.fact_weather_daily (location_id, date);

CREATE INDEX IF NOT EXISTS idx_fact_weather_daily_date
  ON analytics_core.fact_weather_daily (date);

CREATE INDEX IF NOT EXISTS idx_dim_location_country
  ON analytics_core.dim_location (country);

-- Optional: analyze to refresh planner stats (useful if data volume is small/changes often)
ANALYZE analytics_core.fact_weather_daily;
ANALYZE analytics_core.dim_location;

-- =========================================================
-- C) EXPLAIN AFTER INDEXES (run the same queries again)
-- =========================================================

-- C1) Re-run A1
WITH params AS (
  SELECT
    (SELECT location_id FROM analytics_core.dim_location ORDER BY location_id LIMIT 1) AS location_id,
    (SELECT MAX(date) - INTERVAL '30 day' FROM analytics_core.fact_weather_daily)      AS date_from,
    (SELECT MAX(date) FROM analytics_core.fact_weather_daily)                          AS date_to
)
EXPLAIN (ANALYZE, BUFFERS)
SELECT
  f.location_id,
  f.date,
  f.temp_avg_c,
  f.precip_mm,
  f.wind_max_kph
FROM analytics_core.fact_weather_daily f
JOIN params p ON p.location_id = f.location_id
WHERE f.date BETWEEN p.date_from::date AND p.date_to::date
ORDER BY f.date;

-- C2) Re-run A2
WITH params AS (
  SELECT
    (SELECT location_id FROM analytics_core.dim_location ORDER BY location_id LIMIT 1) AS location_id,
    (SELECT MAX(date) - INTERVAL '60 day' FROM analytics_core.fact_weather_daily)      AS date_from,
    (SELECT MAX(date) FROM analytics_core.fact_weather_daily)                          AS date_to
)
EXPLAIN (ANALYZE, BUFFERS)
SELECT
  f.location_id,
  f.date,
  f.temp_avg_c,
  AVG(f.temp_avg_c) OVER (
    PARTITION BY f.location_id
    ORDER BY f.date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS temp_avg_7d
FROM analytics_core.fact_weather_daily f
JOIN params p ON p.location_id = f.location_id
WHERE f.date BETWEEN p.date_from::date AND p.date_to::date
ORDER BY f.date;

-- C3) Re-run A3
WITH params AS (
  SELECT
    (SELECT country FROM analytics_core.dim_location WHERE country IS NOT NULL ORDER BY country LIMIT 1) AS country,
    (SELECT MAX(date) - INTERVAL '30 day' FROM analytics_core.fact_weather_daily)                         AS date_from,
    (SELECT MAX(date) FROM analytics_core.fact_weather_daily)                                             AS date_to
)
EXPLAIN (ANALYZE, BUFFERS)
SELECT
  d.country,
  f.date,
  AVG(f.temp_avg_c)::numeric(10,2) AS avg_temp_avg_c
FROM analytics_core.fact_weather_daily f
JOIN analytics_core.dim_location d
  ON d.location_id = f.location_id
JOIN params p
  ON d.country = p.country
WHERE f.date BETWEEN p.date_from::date AND p.date_to::date
GROUP BY d.country, f.date
ORDER BY f.date;