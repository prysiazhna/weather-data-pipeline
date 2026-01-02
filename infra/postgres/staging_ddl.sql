CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.stg_weather_daily (
  dt date NOT NULL,
  location_id text NOT NULL,
  date date NOT NULL,

  temp_min_c numeric,
  temp_max_c numeric,
  temp_avg_c numeric,
  precip_mm numeric,
  humidity_avg numeric,
  wind_max_kph numeric,
  condition_code integer,
  condition_text text,

  ingested_at timestamptz,
  loaded_at timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (location_id, date)
);

CREATE INDEX IF NOT EXISTS idx_stg_weather_daily_dt
  ON staging.stg_weather_daily (dt);

CREATE INDEX IF NOT EXISTS idx_stg_weather_daily_loc_date
  ON staging.stg_weather_daily (location_id, date);
  

CREATE TABLE IF NOT EXISTS staging.stg_locations (
  dt date,
  location_id text PRIMARY KEY,

  name text,
  region text,
  country text,
  lat numeric,
  lon numeric,
  tz_id text,
  local_time timestamp,

  ingested_at timestamptz,
  loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_stg_locations_country
  ON staging.stg_locations (country);

CREATE INDEX IF NOT EXISTS idx_stg_locations_region
  ON staging.stg_locations (region);