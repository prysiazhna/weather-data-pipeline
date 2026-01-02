
from __future__ import annotations

import io
from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd

from src.common.s3_client import get_s3_client, get_bucket_name


@dataclass
class QualityConfig:
    min_completeness_ratio: float = 0.95  # >= 95% locations must be present
    temp_min_c: float = -80.0
    temp_max_c: float = 60.0
    humidity_min: float = 0.0
    humidity_max: float = 100.0


def _read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    body: bytes = obj["Body"].read()
    return pd.read_parquet(io.BytesIO(body))


def _exists_s3_key(bucket: str, key: str) -> bool:
    s3 = get_s3_client()
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def _list_keys(bucket: str, prefix: str) -> list[str]:
    s3 = get_s3_client()
    keys: list[str] = []
    token: str | None = None

    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token

        resp = s3.list_objects_v2(**kwargs)
        for item in resp.get("Contents", []) or []:
            k = item.get("Key")
            if k:
                keys.append(k)

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    return keys


def _fail(msg: str) -> None:
    raise ValueError(f"[QUALITY_GATE] {msg}")


def _require_columns(df: pd.DataFrame, cols: Iterable[str], df_name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        _fail(f"{df_name} missing required columns: {missing}")


def _not_null(df: pd.DataFrame, col: str, df_name: str) -> None:
    n = int(df[col].isna().sum())
    if n:
        _fail(f"{df_name} has NULL {col}: {n} rows")


def _parse_dt(dt: str) -> pd.Timestamp:
    try:
        return pd.to_datetime(dt, format="%Y-%m-%d", errors="raise")
    except Exception as e:
        _fail(f"Invalid dt '{dt}'. Expected YYYY-MM-DD. Error: {e}")
        raise  


def _check_range_numeric(
    df: pd.DataFrame,
    col: str,
    min_v: float,
    max_v: float,
    df_name: str,
) -> None:
    if col not in df.columns:
        return

    s = pd.to_numeric(df[col], errors="coerce")
    mask = s.notna()
    if not mask.any():
        _fail(f"{df_name} range check: column '{col}' exists but all values are NULL")

    low = int((s[mask] < min_v).sum())
    high = int((s[mask] > max_v).sum())
    if low or high:
        sample = (
            df.loc[mask & ((s < min_v) | (s > max_v)), ["location_id", "date", col]]
            .head(5)
            .to_dict("records")
        )
        _fail(
            f"{df_name} range check failed for '{col}': "
            f"{low} below {min_v}, {high} above {max_v}. Sample: {sample}"
        )


def run(dt: str, cfg: QualityConfig | None = None) -> None:
    cfg = cfg or QualityConfig()

    bucket = get_bucket_name()
    daily_key = f"silver/weather_daily/dt={dt}/weather_daily.parquet"
    locations_key = f"silver/locations/dt={dt}/locations.parquet"

    if not _exists_s3_key(bucket, daily_key):
        prefix = f"silver/weather_daily/dt={dt}/"
        found = [k for k in _list_keys(bucket, prefix) if k.endswith(".parquet")]
        hint = f" Found parquet files under {prefix}: {found}" if found else ""
        _fail(f"Missing daily parquet: s3://{bucket}/{daily_key}.{hint}")

    if not _exists_s3_key(bucket, locations_key):
        prefix = f"silver/locations/dt={dt}/"
        found = [k for k in _list_keys(bucket, prefix) if k.endswith(".parquet")]
        hint = f" Found parquet files under {prefix}: {found}" if found else ""
        _fail(f"Missing locations parquet: s3://{bucket}/{locations_key}.{hint}")

    df_daily = _read_parquet_from_s3(bucket, daily_key)
    df_loc = _read_parquet_from_s3(bucket, locations_key)

    if df_daily.empty:
        _fail("Daily parquet is empty.")
    if df_loc.empty:
        _fail("Locations parquet is empty.")

    # ---- Required columns ----
    _require_columns(df_daily, ["location_id", "date", "ingested_at"], "Daily")
    _require_columns(
        df_loc,
        ["location_id", "name", "country", "lat", "lon", "tz_id", "ingested_at"],
        "Locations",
    )

    # ---- Not null checks ----
    for c in ["location_id", "date", "ingested_at"]:
        _not_null(df_daily, c, "Daily")

    for c in ["location_id", "name", "country", "lat", "lon", "tz_id", "ingested_at"]:
        _not_null(df_loc, c, "Locations")

    # ---- Freshness: date == dt ----
    dt_ts = _parse_dt(dt).date()

    daily_date_series = pd.to_datetime(df_daily["date"], errors="coerce").dt.date
    bad_parse = int(daily_date_series.isna().sum())
    if bad_parse:
        _fail(f"Daily has unparseable 'date' values: {bad_parse} rows")

    mismatch = int((daily_date_series != dt_ts).sum())
    if mismatch:
        offenders = (
            df_daily.loc[daily_date_series != dt_ts, ["location_id", "date"]]
            .head(5)
            .to_dict("records")
        )
        _fail(f"Freshness failed: 'date' != dt for {mismatch} rows. Sample: {offenders}")

    # ---- Uniqueness: (location_id, date) ----
    dup_mask = df_daily.duplicated(subset=["location_id", "date"], keep=False)
    dup_count = int(dup_mask.sum())
    if dup_count:
        sample = (
            df_daily.loc[dup_mask, ["location_id", "date"]]
            .value_counts()
            .head(5)
            .reset_index()
            .rename(columns={0: "count"})
            .to_dict("records")
        )
        _fail(f"Uniqueness failed: duplicated (location_id, date) rows={dup_count}. Sample: {sample}")

    # ---- Range checks (allow NULLs; validate only non-null values) ----
    for c in ["temp_min_c", "temp_max_c", "temp_avg_c"]:
        _check_range_numeric(df_daily, c, cfg.temp_min_c, cfg.temp_max_c, "Daily")

    _check_range_numeric(df_daily, "humidity_avg", cfg.humidity_min, cfg.humidity_max, "Daily")

    # ---- Completeness ----
    daily_locations = set(df_daily["location_id"].astype(str).tolist())
    loc_locations = set(df_loc["location_id"].astype(str).tolist())

    if not loc_locations:
        _fail("Locations table has no location_id values.")

    present = len(daily_locations.intersection(loc_locations))
    expected = len(loc_locations)
    ratio = present / expected if expected else 0.0

    if ratio < cfg.min_completeness_ratio:
        missing = sorted(list(loc_locations - daily_locations))[:10]
        _fail(
            f"Completeness failed: present={present}, expected={expected}, ratio={ratio:.3f} "
            f"(min={cfg.min_completeness_ratio}). Missing sample: {missing}"
        )

    print(
        "[QUALITY_GATE] PASSED "
        f"dt={dt} | daily_rows={len(df_daily)} | locations={expected} | coverage={ratio:.3f}"
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Silver quality gate (daily)")
    parser.add_argument("--dt", type=str, required=True, help="Business date YYYY-MM-DD")
    parser.add_argument("--min_completeness_ratio", type=float, default=0.95)
    parser.add_argument("--temp_min_c", type=float, default=-80.0)
    parser.add_argument("--temp_max_c", type=float, default=60.0)
    parser.add_argument("--humidity_min", type=float, default=0.0)
    parser.add_argument("--humidity_max", type=float, default=100.0)
    args = parser.parse_args()

    config = QualityConfig(
        min_completeness_ratio=args.min_completeness_ratio,
        temp_min_c=args.temp_min_c,
        temp_max_c=args.temp_max_c,
        humidity_min=args.humidity_min,
        humidity_max=args.humidity_max,
    )
    run(args.dt, cfg=config)