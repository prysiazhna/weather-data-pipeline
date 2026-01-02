from __future__ import annotations

import io
import os
from typing import Any, Iterable

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
    """
    Convert pandas/numpy scalar types to native Python types for psycopg2.
    Also converts NaN/NaT to None.
    """
    out: list[tuple[Any, ...]] = []

    for row in df.itertuples(index=False, name=None):
        py_row = []
        for v in row:
            if v is None:
                py_row.append(None)
                continue

            # pandas NaT / NaN
            if isinstance(v, float) and np.isnan(v):
                py_row.append(None)
                continue
            if pd.isna(v):
                py_row.append(None)
                continue

            # numpy scalar -> python scalar
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
    
    # Load Silver daily parquet (for dt) into Postgres staging.stg_weather_daily.
  
    bucket = get_bucket_name()
    s3_key = f"silver/weather_daily/dt={dt}/weather_daily.parquet"
    df = _read_parquet_from_s3(bucket, s3_key)

    df["dt"] = pd.to_datetime(df["dt"]).dt.date
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], errors="coerce", utc=True)

    cols = [
        "dt",
        "location_id",
        "date",
        "temp_min_c",
        "temp_max_c",
        "temp_avg_c",
        "precip_mm",
        "humidity_avg",
        "wind_max_kph",
        "condition_code",
        "condition_text",
        "ingested_at",
    ]

    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Silver schema mismatch. Missing columns: {missing}")

    df = df[cols]

    rows = _to_py_rows(df)

    with _get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM staging.stg_weather_daily WHERE dt = %s;", (dt,))

            insert_sql = """
                INSERT INTO staging.stg_weather_daily (
                    dt, location_id, date,
                    temp_min_c, temp_max_c, temp_avg_c,
                    precip_mm, humidity_avg, wind_max_kph,
                    condition_code, condition_text, ingested_at
                ) VALUES %s
            """
            execute_values(cur, insert_sql, rows, page_size=1000)

    print(f"[LOAD_POSTGRES] OK dt={dt} rows={len(rows)} from s3://{bucket}/{s3_key}")


if __name__ == "__main__":
    import argparse
    from datetime import date

    parser = argparse.ArgumentParser(description="Load Silver daily parquet -> Postgres staging")
    parser.add_argument("--dt", type=str, default=date.today().isoformat())
    args = parser.parse_args()

    run(args.dt)