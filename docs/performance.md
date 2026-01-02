# Performance: SQL Optimization (PostgreSQL)

## Context

This document demonstrates SQL performance analysis for BI-style queries
in PostgreSQL using `EXPLAIN (ANALYZE, BUFFERS)`.

The goal is to validate indexing strategy and planner behavior
rather than micro-optimizing execution time on a small dataset.

---

### Query 1: Filter by location and date range

**Pattern:**  
BI query filtering fact data by `location_id` and `date range`,
used by dashboards and time-series charts.

**SQL:**  
See `sql/performance_queries.sql`.

## BEFORE

- Scan: `Seq Scan on fact_weather_daily`
- Filter: `(location_id = ...) AND (date BETWEEN ...)`
- Rows removed by filter: `~150`
- Buffers: `shared hit=10`
- Execution time: `~0.39 ms`

**Reason:**  
Table size is small (~156 rows), so PostgreSQL correctly prefers
a sequential scan.

---

## Change

```sql
CREATE INDEX idx_fact_weather_daily_location_date
ON analytics_core.fact_weather_daily (location_id, date);
```

## AFTER (index usage demonstrated)
To emulate production-scale behavior:

``` sql
SET enable_seqscan = off;
```

- Scan: Index Scan using idx_fact_weather_daily_location_date
- Index condition: (location_id = ?) AND (date >= ?) AND (date <= ?)
- Buffers: shared hit=12 read=1

Note: execution time is dominated by JIT overhead on small datasets.
Disabling JIT (SET jit = off) removes this overhead.


### Result
- Index matches BI access pattern (location_id, date)
- Planner switches from Seq Scan to Index Scan when appropriate
- Sequential scan on small data is expected and correct
- Index enables scalable performance for production workloads