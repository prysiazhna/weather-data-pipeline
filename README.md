# Weather Data Pipeline 

This project implements a production-style lakehouse pipeline for daily weather analytics.
It demonstrates best practices in data ingestion, transformation, quality validation,
orchestration, and SQL performance optimization.


## Tech Stack

- **Apache Airflow** — pipeline orchestration and scheduling
- **PostgreSQL** — data warehouse (staging + analytics marts)
- **Python** — ingestion, transformation, validation, loading
- **Docker Compose** — local, production-like environment
- **MinIO (S3-compatible)** — data lake storage (Bronze / Silver layers)
- **dbt Core** — SQL transformations, tests, and documentation
- **pytest** — unit and integration testing
- **WeatherAPI** — external data source (daily historical weather data)

## Architecture Overview

The pipeline follows a standard lakehouse architecture:

**Bronze → Silver → Gold**

- **Bronze**: raw WeatherAPI JSON data stored in MinIO, partitioned by `dt` and `location_id`
- **Silver**: cleaned and normalized Parquet datasets with enforced schemas and keys
- **Gold**: analytics-ready tables and marts in PostgreSQL, built with dbt

Airflow orchestrates the full end-to-end workflow with retries, backfill support,
and explicit quality gates.


## Data Flow

1. **Ingestion (WeatherAPI → Bronze)**
   - Daily weather history fetched per location
   - Raw JSON stored with metadata
   - Idempotent writes and retry logic for API failures

2. **Transformation (Bronze → Silver)**
   - JSON normalization into tabular format
   - Type casting, deduplication, schema enforcement
   - Parquet output stored in MinIO

3. **Data Quality Gate**
   - Completeness, uniqueness, not-null, range and freshness checks
   - Pipeline fails fast if data quality rules are violated

4. **Load (Silver → Postgres staging)**
   - Incremental, date-based loads
   - Schema validation and safe type conversion
   - Idempotent delete+insert strategy per `dt`

5. **Analytics (dbt Core)**
   - Staging models, dimensions and facts
   - Analytical marts (rolling averages, anomalies, WoW metrics)
   - dbt tests and documentation generation


## Orchestration
<img width="1635" height="818" alt="Screenshot 2026-01-02 at 20 55 31" src="https://github.com/user-attachments/assets/42041a27-79d7-4240-9f9f-777e297d5ddc" />

A single Airflow DAG controls the entire pipeline:

- Parameterized by business date (`dt`)
- Supports retries, exponential backoff, and backfill
- Uses TaskGroups for clear stage separation
- Ensures correct execution order (dimensions before facts)
- Blocks downstream steps on data quality failures

<img width="1484" height="798" alt="Screenshot 2026-01-02 at 21 01 53" src="https://github.com/user-attachments/assets/e8cd733e-f948-424a-bc03-32cb78de7112" />


## Performance Optimization

The project includes a dedicated performance analysis section
demonstrating SQL optimization for BI-style queries.

- Index design based on real access patterns
- `EXPLAIN (ANALYZE, BUFFERS)` before/after comparison
- Discussion of planner behavior on small vs production-scale datasets

Details are available in `docs/performance.md`.



## Testing

Testing is **currently in progress**.

Planned coverage includes:
- Unit tests for transformation and validation logic
- Integration tests for a full pipeline run on a single business date


## Summary

This project demonstrates a **production-oriented data engineering pipeline** with:

- Clear separation of data layers
- Explicit data quality enforcement
- Orchestrated, idempotent processing
- SQL-first analytics modeling
- Attention to scalability and performance

The repository is intended as a portfolio project for data engineering roles
and is actively being iterated and extended.
