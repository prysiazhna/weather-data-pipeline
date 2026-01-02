from __future__ import annotations

import io
import os
from typing import Any

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from src.common.s3_client import get_s3_client, get_bucket_name


def _read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    return pd.read_parquet(io.BytesIO(body))


def _get_pg_conn():
    dsn = os.getenv("WEATHER_DWH_PG_DSN")
    if not dsn:
        raise ValueError("WEATHER_DWH_PG_DSN is not set")
    return psycopg2.connect(dsn)


def _to_py_rows(df: pd.DataFrame) -> list[tuple[Any, ...]]:
    out: list[tuple[Any, ...]] = []
    for row in df.itertuples(index=False, name=None):
        py_row = []
        for v in row:
            if v is None or pd.isna(v):
                py_row.append(None)
                continue
            if isinstance(v, (np.integer,)):
                py_row.append(int(v))
            elif isinstance(v, (np.floating,)):
                py_row.append(float(v))
            elif isinstance(v, (np.bool_,)):
                py_row.append(bool(v))
            else:
                py_row.append(v)
        out.append(tuple(py_row))
    return out


def run(dt: str) -> None:

    # Load Silver locations parquet (for dt) into Postgres staging.stg_locations.
   
    bucket = get_bucket_name()
    s3_key = f"silver/locations/dt={dt}/locations.parquet"
    df = _read_parquet_from_s3(bucket, s3_key)

    if "dt" in df.columns:
        df["dt"] = pd.to_datetime(df["dt"], errors="coerce").dt.date
    df["ingested_at"] = pd.to_datetime(df.get("ingested_at"), errors="coerce", utc=True)
    df["lat"] = pd.to_numeric(df.get("lat"), errors="coerce")
    df["lon"] = pd.to_numeric(df.get("lon"), errors="coerce")
    df["local_time"] = pd.to_datetime(df.get("local_time"), errors="coerce")

    cols = [
        "dt",
        "location_id",
        "name",
        "region",
        "country",
        "lat",
        "lon",
        "tz_id",
        "local_time",
        "ingested_at",
    ]

    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Silver locations schema mismatch. Missing columns: {missing}")

    df = df[cols].drop_duplicates(subset=["location_id"], keep="last")
    rows = _to_py_rows(df)

    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM staging.stg_locations;")

            insert_sql = """
                INSERT INTO staging.stg_locations (
                    dt, location_id, name, region, country, lat, lon, tz_id, local_time, ingested_at
                ) VALUES %s
            """
            execute_values(cur, insert_sql, rows, page_size=1000)

    print(f"[LOAD_POSTGRES_LOCATIONS] OK dt={dt} rows={len(rows)} from s3://{bucket}/{s3_key}")


if __name__ == "__main__":
    import argparse
    from datetime import date

    parser = argparse.ArgumentParser(description="Load Silver locations parquet -> Postgres staging")
    parser.add_argument("--dt", type=str, default=date.today().isoformat())
    args = parser.parse_args()

    run(args.dt)