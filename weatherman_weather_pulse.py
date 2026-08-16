"""
Weatherman Weather Pulse — multi-region async engine.

Behavior matches `weatherman_weather_pulse_spec.md`:
  - Outputs: marketing_weather_report_us.csv, _uk.csv, _can.csv
  - Locations: `data/city_coordinates.json` (sales-weighted hubs; no >10k city expansion)
  - US: NWS narrative QPF when parseable; Open-Meteo 24h model fallback + archive/UV
  - UK / CAN: Open-Meteo forecast + archive + UV only
  - Exactly 9 CSV columns (no orders/revenue fields)
"""

from __future__ import annotations

import asyncio
import aiohttp
import csv
import json
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

RAIN_THRESHOLD_INCHES = 0.20
RESIDUAL_RAIN_THRESHOLD_INCHES = 0.50
UV_THRESHOLD = 8
# Open-Meteo hourly precipitation is millimeters; thresholds are inches.
MM_TO_IN = 1.0 / 25.4
TIMEOUT = 7
MAX_RETRIES = 3
RETRY_DELAY = 2
# Limit parallel HTTP calls (NWS / Open-Meteo) to reduce 429s and load spikes.
MAX_CONCURRENT_REQUESTS = 25

_OUTPUT_SEM: Optional[asyncio.Semaphore] = None


def _sem() -> asyncio.Semaphore:
    global _OUTPUT_SEM
    if _OUTPUT_SEM is None:
        _OUTPUT_SEM = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    return _OUTPUT_SEM


# Spec §2 — three regional files for Looker Studio
OUTPUT_CSV_US = "marketing_weather_report_us.csv"
OUTPUT_CSV_UK = "marketing_weather_report_uk.csv"
OUTPUT_CSV_CAN = "marketing_weather_report_can.csv"

# Spec §2 — exact column order (9 fields)
CSV_FIELDNAMES = [
    "City",
    "State",
    "Lat",
    "Lon",
    "Marketing_Action",
    "Rain_Amount",
    "Yesterday_Rain",
    "Max_UV_Index",
    "Logic_Summary",
]

_DATA_DIR = Path(__file__).resolve().parent / "data"
_CITY_COORDINATES_PATH = _DATA_DIR / "city_coordinates.json"

UK_REGIONS_LOWER = frozenset(
    {"england", "scotland", "wales", "northern ireland", "london", "uk"}
)
CAN_REGIONS_LOWER = frozenset(
    {
        "ontario",
        "british columbia",
        "quebec",
        "alberta",
        "manitoba",
        "saskatchewan",
        "nova scotia",
        "new brunswick",
        "newfoundland and labrador",
        "prince edward island",
        "yukon",
        "northwest territories",
        "nunavut",
        "canada",
    }
)


class CityRow:
    __slots__ = ("state", "city", "lat", "lon", "region")

    def __init__(self, state: str, city: str, lat: float, lon: float, region: str = "US") -> None:
        self.state = state
        self.city = city
        self.lat = float(lat)
        self.lon = float(lon)
        self.region = region.upper()


def _load_city_rows() -> List[CityRow]:
    """Sales-hub / outlier list from `city_coordinates.json` (not a full >10k population grid)."""
    if not _CITY_COORDINATES_PATH.is_file():
        return []
    payload = json.loads(_CITY_COORDINATES_PATH.read_text(encoding="utf-8"))
    out: List[CityRow] = []
    for r in payload.get("rows", []):
        reg = str(r.get("region", "US")).strip().upper() or "US"
        out.append(CityRow(str(r["state"]), str(r["city"]), float(r["lat"]), float(r["lon"]), reg))
    return out


CITY_ROWS = _load_city_rows()


def _decide_action(
    forecast_precip_in: float, yesterday_precip_in: float, max_uv: Optional[float]
) -> Tuple[str, str]:
    """Spec §4 — residual from archive; umbrellas from forward QPF; UV sun line."""
    if yesterday_precip_in >= RESIDUAL_RAIN_THRESHOLD_INCHES:
        return "Scale Umbrellas (Residual Demand)", (
            f"Triggered by: previous-day rain ≥ {RESIDUAL_RAIN_THRESHOLD_INCHES:.2f} in"
        )
    if forecast_precip_in >= RESIDUAL_RAIN_THRESHOLD_INCHES:
        return "Scale Umbrellas", (
            f"Triggered by: forecast precip ≥ {RESIDUAL_RAIN_THRESHOLD_INCHES:.2f} in"
        )
    if forecast_precip_in >= RAIN_THRESHOLD_INCHES:
        return "Scale Umbrellas", f"Triggered by: forecast precip ≥ {RAIN_THRESHOLD_INCHES:.2f} in"
    if max_uv is not None and max_uv >= UV_THRESHOLD:
        return "Sun Protection (Hats/Shirts)", f"Triggered by: UV {max_uv:.1f}"
    return "Baseline", "No marketing threshold met."


async def get_with_retry(
    session: aiohttp.ClientSession, url: str, headers: Optional[Dict[str, str]] = None
) -> Optional[Any]:
    """Spec §5 — up to MAX_RETRIES with exponential backoff (capped)."""
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, headers=headers, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 429:
                    delay = min(32.0, float(RETRY_DELAY) * (2**attempt))
                    await asyncio.sleep(delay)
                    continue
        except Exception:
            delay = min(32.0, float(RETRY_DELAY) * (2**attempt))
            await asyncio.sleep(delay)
    return None


async def get_nws_qpf_inches(session: aiohttp.ClientSession, lat: float, lon: float) -> Optional[float]:
    """Parse liquid QPF from NWS narrative when present; otherwise None (Open-Meteo fallback)."""
    url = f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}/forecast"
    data = await get_with_retry(session, url, headers={"User-Agent": "WeathermanMarketingTool/2.4"})
    if not data:
        return None
    periods = data.get("properties", {}).get("periods", [])
    for p in periods[:4]:
        detailed = p.get("detailedForecast", "").lower()
        matches = re.findall(r"(\d+(?:\.\d+)?)\s*(?:inch|inches|in)\b", detailed)
        if matches:
            return float(matches[0])
        if "less than a tenth" in detailed or "less than one tenth" in detailed:
            return 0.05
        if "around a tenth" in detailed or "near a tenth" in detailed:
            return 0.10
        if "quarter of an inch" in detailed or "quarter inch" in detailed:
            return 0.25
        if "half an inch" in detailed or "half inch" in detailed:
            return 0.50
        if "tenth of an inch" in detailed and "less" not in detailed:
            return 0.10
    return None


async def get_open_meteo_forecast_rain_24h_inches(
    session: aiohttp.ClientSession, lat: float, lon: float
) -> float:
    """Next ~24h liquid precip from Open-Meteo (mm → inches). Fallback QPF for US; primary for UK/CAN."""
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}&hourly=precipitation&forecast_hours=48"
    )
    data = await get_with_retry(session, url)
    if not data:
        return 0.0
    hourly = data.get("hourly", {}).get("precipitation") or []
    total_mm = sum(float(x or 0) for x in hourly[:24])
    return total_mm * MM_TO_IN


async def get_open_meteo_archive_rain(
    session: aiohttp.ClientSession, lat: float, lon: float, date_str: str
) -> Optional[float]:
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}&start_date={date_str}&end_date={date_str}&hourly=precipitation"
    )
    data = await get_with_retry(session, url)
    if data:
        total_mm = sum(float(x or 0) for x in data.get("hourly", {}).get("precipitation", []))
        return total_mm * MM_TO_IN
    return None


async def get_open_meteo_uv(session: aiohttp.ClientSession, lat: float, lon: float) -> Optional[float]:
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=uv_index_max&timezone=auto"
    data = await get_with_retry(session, url)
    if data:
        uvi = data.get("daily", {}).get("uv_index_max", [None])
        if uvi and uvi[0] is not None:
            return float(uvi[0])
    return None


async def process_city(session: aiohttp.ClientSession, cr: CityRow, date_str: str) -> Dict[str, Any]:
    async with _sem():
        use_nws = cr.region == "US"

        if use_nws:
            nws_text_qpf = await get_nws_qpf_inches(session, cr.lat, cr.lon)
            if nws_text_qpf is not None:
                forecast_in = float(nws_text_qpf)
                qpf_label = "NWS narrative"
            else:
                forecast_in = await get_open_meteo_forecast_rain_24h_inches(session, cr.lat, cr.lon)
                qpf_label = "Open-Meteo 24h model (NWS fallback)"
        else:
            forecast_in = await get_open_meteo_forecast_rain_24h_inches(session, cr.lat, cr.lon)
            qpf_label = "Open-Meteo 24h model"

        om_rain = await get_open_meteo_archive_rain(session, cr.lat, cr.lon, date_str)
        om_uv = await get_open_meteo_uv(session, cr.lat, cr.lon)

        if om_uv is None:
            om_uv = 0.0

        yesterday_in = om_rain if om_rain is not None else 0.0
        action, logic = _decide_action(forecast_in, yesterday_in, om_uv)

        region_preamble = ""
        if not use_nws:
            region_preamble = (
                f"Region {cr.region}: Open-Meteo forecast + archive (NWS not used) | "
            )

        logic = (
            region_preamble
            + f"Next-24h precip ({qpf_label}) {forecast_in:.3f} in "
            + f"(umbrella threshold {RAIN_THRESHOLD_INCHES:.2f} in; heavy {RESIDUAL_RAIN_THRESHOLD_INCHES:.2f} in) | "
            + f"Previous-24h rain (Open-Meteo archive) {yesterday_in:.3f} in "
            + f"(residual threshold {RESIDUAL_RAIN_THRESHOLD_INCHES:.2f} in) | "
            + f"Daily max UV (Open-Meteo) {om_uv:.1f} (threshold {UV_THRESHOLD}) | "
            + logic
        )

        row: Dict[str, Any] = {
            "City": cr.city,
            "State": cr.state,
            "Lat": f"{cr.lat:.4f}",
            "Lon": f"{cr.lon:.4f}",
            "Marketing_Action": action,
            "Rain_Amount": f"{forecast_in:.3f}",
            "Yesterday_Rain": f"{yesterday_in:.3f}",
            "Max_UV_Index": f"{om_uv:.2f}",
            "Logic_Summary": logic,
            "_region": cr.region,
        }
        return row


def _write_report(filename: str, rows: List[Dict[str, Any]]) -> None:
    """Write exactly the 9 spec columns; ignore any internal keys such as _region."""
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r[k] for k in CSV_FIELDNAMES})


def _route_rows(results: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    us_out: List[Dict[str, Any]] = []
    uk_out: List[Dict[str, Any]] = []
    can_out: List[Dict[str, Any]] = []
    for row in results:
        reg = row.pop("_region", "US").upper()
        st = str(row.get("State", "")).lower()
        if reg == "UK" or st in UK_REGIONS_LOWER:
            uk_out.append(row)
        elif reg == "CA" or st in CAN_REGIONS_LOWER:
            can_out.append(row)
        else:
            us_out.append(row)
    return us_out, uk_out, can_out


async def main() -> None:
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")
    print(f"[INFO] Processing {len(CITY_ROWS)} hub locations (date_str={date_str})...")
    start = time.time()

    async with aiohttp.ClientSession() as session:
        tasks = [process_city(session, cr, date_str) for cr in CITY_ROWS]
        results = await asyncio.gather(*tasks)

    us_rows, uk_rows, can_rows = _route_rows(results)
    _write_report(OUTPUT_CSV_US, us_rows)
    _write_report(OUTPUT_CSV_UK, uk_rows)
    _write_report(OUTPUT_CSV_CAN, can_rows)

    elapsed = time.time() - start
    print(f"[INFO] Wrote {len(us_rows)} rows to {OUTPUT_CSV_US}")
    print(f"[INFO] Wrote {len(uk_rows)} rows to {OUTPUT_CSV_UK}")
    print(f"[INFO] Wrote {len(can_rows)} rows to {OUTPUT_CSV_CAN}")
    print(f"[INFO] Processing complete in {elapsed:.2f} seconds.")


if __name__ == "__main__":
    if not _CITY_COORDINATES_PATH.is_file():
        print(f"[ERROR] Missing {_CITY_COORDINATES_PATH}", file=sys.stderr)
        raise SystemExit(1)
    if not CITY_ROWS:
        print("[ERROR] No cities in city_coordinates.json (rows empty).", file=sys.stderr)
        raise SystemExit(1)
    asyncio.run(main())
