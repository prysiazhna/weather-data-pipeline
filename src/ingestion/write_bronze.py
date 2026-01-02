import os
import json
from pathlib import Path
from datetime import datetime, timezone
import yaml

from src.common.s3_client import get_s3_client, get_bucket_name
from src.ingestion.weatherapi_client import fetch_history


def load_locations(path: str = "docs/locations.yml") -> list[dict]:
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    locations = cfg.get("locations", [])
    if not locations:
        raise ValueError(f"No locations found in {path}")

    return locations


def run(dt: str, locations_path: str = "docs/locations.yml") -> None:
    locations = load_locations(locations_path)
    s3 = get_s3_client()
    bucket = get_bucket_name()

    # Technical ingestion timestamp (UTC)
    ingested_at = datetime.now(timezone.utc).isoformat()

    for loc in locations:
        location_id = loc["location_id"]
        location_name = loc.get("name", location_id)

        # Prefer coordinates to avoid ambiguity
        lat = loc.get("lat")
        lon = loc.get("lon")
        if lat is None or lon is None:
            raise ValueError(
                f"Location {location_id} is missing lat/lon in {locations_path}"
            )

        # Fetch REAL raw JSON payload from WeatherAPI (History endpoint)
        api_payload = fetch_history(lat=float(lat), lon=float(lon), dt=dt)

        # Bronze record structure: metadata + raw payload
        data = {
            "metadata": {
                "dt": dt,
                "location_id": location_id,
                "location_name": location_name,
                "ingested_at": ingested_at,
                "source": "weatherapi",
                "api_version": "v1",
                "request": {
                    "q": f"{lat},{lon}",
                    "endpoint": "history.json",
                },
            },
            "payload": api_payload,
        }

        key = (
            "bronze/weather_history/"
            f"dt={dt}/"
            f"location_id={location_id}/"
            "raw.json"
        )

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False, indent=2),
            ContentType="application/json",
        )

        print(f"Written to s3://{bucket}/{key}")


if __name__ == "__main__":
    import argparse
    from datetime import date

    parser = argparse.ArgumentParser(description="Bronze weather ingestion")
    parser.add_argument(
        "--dt",
        type=str,
        default=date.today().isoformat(),
        help="Business date in YYYY-MM-DD",
    )
    parser.add_argument(
        "--locations_path",
        type=str,
        default="docs/locations.yml",
        help="Path to locations.yml",
    )
    args = parser.parse_args()

    run(args.dt, args.locations_path)