# Data Contract: Weather Lakehouse Pipeline (Bronze/Silver/Gold)

## 0) Source of truth
- Locations list & IDs (authoritative): `config/locations.yml`
  - `location_id` is owned by this project and must be stable over time.
- Contract (authoritative): this document `docs/data_contract.md`
- Runtime parameter (authoritative): Airflow `ds` passed to tasks as `dt` (see section 2)

---

## 1) Purpose
Daily ingestion of historical weather data for a fixed list of locations.

- Primary grain: **DAILY**
- Optional grain: **HOURLY** (future)

---

## 2) Business date (`dt`)
- `dt` is the business date being processed.
- Definition (current implementation): `dt = Airflow ds (UTC)` in format `YYYY-MM-DD`.
- Note: If later you want “yesterday in each location timezone”, this contract must change because `dt` could differ per location.

---

## 3) Source system
- Provider: **WeatherAPI**
- Endpoint/entity: history for a given `dt` and location (query)

---

## 4) Common identifiers & metadata
- `location_id` (text): stable identifier from `config/locations.yml`
- `ingested_at` (timestamp, UTC): time the pipeline saved the record
- `source` (text): fixed string `"weatherapi"`
- `api_version` (text, optional): version tag if available

---

## 5) Idempotency
For the same (`dt`, `location_id`) reruns must not create duplicates.
- Bronze write mode: overwrite same object key for the same (`dt`, `location_id`)
- Silver write mode: overwrite dataset partition for the same `dt`
- Postgres staging load: upsert on primary keys

---

## 6) Storage layout (MinIO / S3-compatible)

### 6.1 Bronze (raw)
- Format: JSON (single payload file per (`dt`, `location_id`))
- Partition keys: `dt`, `location_id`
- Write mode: overwrite

**Path pattern**
`s3://<bucket>/bronze/weather_history/dt=YYYY-MM-DD/location_id=<location_id>/raw.json`

**Bronze file requirements**
Bronze JSON must include:
- Raw WeatherAPI payload (as received)
- Added metadata fields:
  - `ingested_at` (UTC timestamp)
  - `source` = `"weatherapi"`
  - `api_version` (optional)

---

### 6.2 Silver (cleaned, typed)
- Format: Parquet
- Partition: by `dt` (one partition per day)

#### silver_locations
Primary key: `location_id`

**Path**
`s3://<bucket>/silver/locations/dt=YYYY-MM-DD/locations.parquet`

**Columns**
- location_id (text, not null)
- name (text, not null)
- country (text, not null)
- region (text, null)
- lat (numeric, not null)
- lon (numeric, not null)
- tz_id (text, not null)
- ingested_at (timestamp, not null, UTC)

**Quality rules**
- unique(location_id)
- not_null(location_id, name, country, lat, lon, tz_id, ingested_at)

#### silver_weather_daily
Primary key: (location_id, date)

**Path**
`s3://<bucket>/silver/weather_daily/dt=YYYY-MM-DD/weather_daily.parquet`

**Columns**
- location_id (text, not null)
- date (date, not null)                     -- equals dt
- temp_min_c (numeric, null)                -- unit: Celsius
- temp_max_c (numeric, null)                -- unit: Celsius
- temp_avg_c (numeric, null)                -- unit: Celsius
- precip_mm (numeric, null)                 -- unit: millimeters
- humidity_avg (numeric, null)              -- unit: percent (0..100)
- wind_max_kph (numeric, null)              -- unit: kph
- condition_text (text, null)
- condition_code (int, null)
- ingested_at (timestamp, not null, UTC)

**Quality rules**
- unique(location_id, date)
- not_null(location_id, date, ingested_at)
- range(temp_*_c, -80..60) for non-null values
- range(humidity_avg, 0..100) for non-null values
- freshness: must contain records for `date == dt`

---

## 7) Load to DWH (Postgres staging)
- Target schema: `staging`
- Tables:
  - `staging.stg_locations`
  - `staging.stg_weather_daily`
- Incremental strategy: load by `dt`
- Upsert strategy:
  - locations: ON CONFLICT (location_id) DO UPDATE
  - daily: ON CONFLICT (location_id, date) DO UPDATE

Indexes/constraints:
- `stg_locations`: PK(location_id)
- `stg_weather_daily`: PK(location_id, date)
- Additional indexes allowed for BI patterns (location_id, date)

---

## 8) Gold (dbt in Postgres)
dbt models produce:
- `analytics_staging_models` (views): cleaned staging layer in Postgres
- `analytics_core` (tables): dims/facts/marts
- `analytics_bi` (views): BI-friendly views (thin semantic layer)

---

## 9) Quality Gate: acceptance criteria (DoD)
For a given `dt`:
- Bronze exists for >= 95% of configured locations
- Silver Parquet exists for `dt`
- Silver daily has:
  - no nulls in (location_id, date, ingested_at)
  - unique (location_id, date)
  - freshness: all rows `date == dt`
  - temperature ranges respected for non-null values
- If any check fails: pipeline must **fail** and not proceed to dbt/gold