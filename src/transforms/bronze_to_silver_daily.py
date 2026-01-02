import io
import json
from typing import Any

import pandas as pd

from src.common.s3_client import get_s3_client, get_bucket_name


def _read_json_from_s3(bucket: str, key: str) -> dict[str, Any]:
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")
    return json.loads(body)


def run(dt: str) -> None:
    bucket = get_bucket_name()
    s3 = get_s3_client()

    prefix = f"bronze/weather_history/dt={dt}/"
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    contents = resp.get("Contents", [])
    if not contents:
        raise ValueError(f"No bronze objects found under s3://{bucket}/{prefix}")

    daily_rows: list[dict[str, Any]] = []
    location_rows: list[dict[str, Any]] = []

    for item in contents:
        key = item["Key"]
        if not key.endswith("raw.json"):
            continue

        record = _read_json_from_s3(bucket, key)

        metadata = record.get("metadata", {}) or {}
        payload = record.get("payload", {}) or {}

        location_id = metadata.get("location_id")
        ingested_at = metadata.get("ingested_at")

        # -------- locations (dimension-like) --------
        loc = payload.get("location", {}) or {}
        location_rows.append(
            {
                "dt": dt,  
                "location_id": location_id,
                "name": loc.get("name"),
                "region": loc.get("region"),
                "country": loc.get("country"),
                "lat": loc.get("lat"),
                "lon": loc.get("lon"),
                "tz_id": loc.get("tz_id"),
                "local_time": loc.get("local_time"),
                "ingested_at": ingested_at,
            }
        )

        # -------- weather_daily (fact-like) --------
        forecastday = (payload.get("forecast", {}) or {}).get("forecastday", []) or []
        if not forecastday:
            continue

        fd0 = forecastday[0] or {}
        day = fd0.get("day", {}) or {}
        cond = day.get("condition", {}) or {}

        daily_rows.append(
            {
                "dt": dt,
                "location_id": location_id,
                "ingested_at": ingested_at,
                "date": fd0.get("date"),
                "temp_min_c": day.get("mintemp_c"),
                "temp_max_c": day.get("maxtemp_c"),
                "temp_avg_c": day.get("avgtemp_c"),
                "precip_mm": day.get("totalprecip_mm"),
                "snow_cm": day.get("totalsnow_cm"),
                "humidity_avg": day.get("avghumidity"),
                "wind_max_kph": day.get("maxwind_kph"),
                "condition_code": cond.get("code"),
                "condition_text": cond.get("text"),
            }
        )

    df_daily = pd.DataFrame(daily_rows)
    df_locations = pd.DataFrame(location_rows)

    if df_daily.empty:
        raise ValueError(f"No daily rows produced for dt={dt}. Check bronze payload structure.")
    if df_locations.empty:
        raise ValueError(f"No location rows produced for dt={dt}. Check bronze payload structure.")

   
    df_daily["dt"] = pd.to_datetime(df_daily["dt"]).dt.date
    df_daily["date"] = pd.to_datetime(df_daily["date"]).dt.date
    df_daily["ingested_at"] = pd.to_datetime(df_daily["ingested_at"], errors="coerce", utc=True)

    for col in ["temp_min_c", "temp_max_c", "temp_avg_c", "precip_mm", "snow_cm", "wind_max_kph", "humidity_avg"]:
        df_daily[col] = pd.to_numeric(df_daily[col], errors="coerce")

    df_daily["condition_code"] = pd.to_numeric(df_daily["condition_code"], errors="coerce").astype("Int64")

    df_locations["dt"] = pd.to_datetime(df_locations["dt"]).dt.date
    df_locations["ingested_at"] = pd.to_datetime(df_locations["ingested_at"], errors="coerce", utc=True)
    df_locations["lat"] = pd.to_numeric(df_locations["lat"], errors="coerce")
    df_locations["lon"] = pd.to_numeric(df_locations["lon"], errors="coerce")
    df_locations["local_time"] = pd.to_datetime(df_locations["local_time"], errors="coerce")

    df_daily = df_daily.drop_duplicates(subset=["location_id", "date"], keep="last")
    df_locations = df_locations.drop_duplicates(subset=["location_id"], keep="last")

    # ---------- write parquet to S3/MinIO ----------
    out_daily_key = f"silver/weather_daily/dt={dt}/weather_daily.parquet"
    out_locations_key = f"silver/locations/dt={dt}/locations.parquet"

    buf_daily = io.BytesIO()
    df_daily.to_parquet(buf_daily, index=False)
    buf_daily.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=out_daily_key,
        Body=buf_daily.getvalue(),
        ContentType="application/octet-stream",
    )

    buf_loc = io.BytesIO()
    df_locations.to_parquet(buf_loc, index=False)
    buf_loc.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=out_locations_key,
        Body=buf_loc.getvalue(),
        ContentType="application/octet-stream",
    )

    print(f"Written silver daily parquet: s3://{bucket}/{out_daily_key} (rows={len(df_daily)})")
    print(f"Written silver locations parquet: s3://{bucket}/{out_locations_key} (rows={len(df_locations)})")


if __name__ == "__main__":
    import argparse
    from datetime import date

    parser = argparse.ArgumentParser(description="Bronze -> Silver (daily + locations)")
    parser.add_argument("--dt", type=str, default=date.today().isoformat())
    args = parser.parse_args()

    run(args.dt)