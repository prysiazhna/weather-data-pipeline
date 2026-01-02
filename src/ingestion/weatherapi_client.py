import os
import time
import requests
from typing import Any

BASE_URL = os.getenv("WEATHERAPI_BASE_URL")
API_KEY = os.getenv("WEATHERAPI_KEY")

class WeatherApiError(RuntimeError):
    pass

def fetch_history(lat: float, lon: float, dt: str, timeout_s: int = 30) -> dict[str, Any]:
    if not API_KEY:
        raise ValueError("WEATHERAPI_KEY is not set in .env")

    url = f"{BASE_URL}/history.json"
    params = {
        "key": API_KEY,
        "q": f"{lat},{lon}",
        "dt": dt,          # YYYY-MM-DD
        "aqi": "no",
        "alerts": "no",
    }

    # retry: 429/5xx
    max_attempts = 5
    base_sleep = 2

    last_err: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout_s)

            if r.status_code == 200:
                return r.json()

            if r.status_code == 429 or 500 <= r.status_code < 600:
                sleep_s = base_sleep * (2 ** (attempt - 1))
                time.sleep(sleep_s)
                continue

            # other 4xx — fail
            raise WeatherApiError(f"WeatherAPI error {r.status_code}: {r.text[:200]}")

        except Exception as e:
            last_err = e
            sleep_s = base_sleep * (2 ** (attempt - 1))
            time.sleep(sleep_s)

    raise WeatherApiError(f"Failed after retries. Last error: {last_err}")