#!/usr/bin/env python3
"""
ETL engine — drop raw CSVs, run once, refresh:
  historic_sales_bridge.csv         (Phase 2 strategy/sales bridge)
  historic_verification_bridge.csv  (Phase 1 observed rain ↔ umbrella sales)
  forecast_ad_spend_bridge.csv      (Phase 2 forecast triggers + ad allocation)

Usage:  python pipeline.py
"""

from __future__ import annotations

import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

ROOT = Path(__file__).resolve().parent
OUTPUT_PATH = ROOT / "historic_sales_bridge.csv"
VERIFICATION_OUTPUT_PATH = ROOT / "historic_verification_bridge.csv"
HISTORY_WEATHER_DIR = ROOT / "data" / "history" / "weather"
OBSERVED_PRECIP_CACHE_PATH = ROOT / "data" / "history" / "observed_precip_cache.csv"
CITY_COORDINATES_PATH = ROOT / "data" / "city_coordinates.json"
UMBRELLA_SKU_SIGNALS_PATH = ROOT / "data" / "umbrella_sku_signals.json"
MASTER_SALES_PREVIEW = ROOT / "data" / "weather_pulse_sales_bridge_master_preview.csv"
RAW_SALES_BRIDGE = ROOT / "raw_data" / "sales_data_bridge.csv"

# Observed-rain threshold (inches) — same primary trigger as Weather Pulse.
RAIN_THRESHOLD_INCHES = 0.20
UV_THRESHOLD = 8.0
MM_TO_IN = 1.0 / 25.4
OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"
PRECIP_FETCH_WORKERS = 4
PRECIP_REQUEST_PAUSE_S = 0.25
PRECIP_MAX_RETRIES = 4

FORECAST_OUTPUT_PATH = ROOT / "forecast_ad_spend_bridge.csv"
FORECAST_DAYS = 6
FORECAST_BATCH_SIZE = 100

SCAN_DIRS: tuple[Path, ...] = (ROOT, ROOT / "raw_data")

FILE_PATTERNS: tuple[str, ...] = (
    "marketing_weather_report*.csv",
    "sales_data*.csv",
    "triple_whale*.csv",
    "marketing_execution*.csv",
)

EXCLUDE_FILENAMES: frozenset[str] = frozenset({"historic_sales_bridge.csv"})
PREVIEW_SUFFIX = re.compile(r"_preview\.csv$", re.I)

BRIDGE_COLUMNS: tuple[str, ...] = ("Date", "Region", "Type", "Value", "Lat", "Lon")

VERIFICATION_COLUMNS: tuple[str, ...] = (
    "Date",
    "Region",
    "City",
    "State",
    "Lat",
    "Lon",
    "Umbrella_Revenue",
    "Umbrella_Units",
    "Non_Umbrella_Revenue",
    "Non_Umbrella_Units",
    "Observed_Precip_In",
    "Is_Rainy",
    "Verified_Match",
)

FORECAST_COLUMNS: tuple[str, ...] = (
    "Date",
    "Region",
    "City",
    "State",
    "Lat",
    "Lon",
    "Forecast_Precip_In",
    "Forecast_UV",
    "Action",
    "Trigger_Reason",
    "Hist_Umbrella_Revenue",
    "Priority_Score",
    "Spend_Share",
)

INVALID_REGIONS: frozenset[str] = frozenset({"", "NAN", "NONE", "NULL"})
BLOCKED_CITIES: frozenset[str] = frozenset({"", "BLOCKED", "NAN", "NONE", "NULL", "UNKNOWN"})

REGION_ALIASES: dict[str, str] = {
    "US": "US",
    "USA": "US",
    "CA": "CAN",
    "CAN": "CAN",
    "CANADA": "CAN",
    "UK": "UK",
    "GB": "UK",
}

REGION_FROM_FILENAME: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"(?:^|[_\-])us(?:[_\-]|$)", re.I), "US"),
    (re.compile(r"(?:^|[_\-])uk(?:[_\-]|$)", re.I), "UK"),
    (re.compile(r"(?:^|[_\-])can(?:[_\-]|$)", re.I), "CAN"),
    (re.compile(r"(?:^|[_\-])ca(?:[_\-]|$)", re.I), "CAN"),
)


def log(msg: str) -> None:
    print(f"[pipeline] {msg}")


def normalize_region(value: object, fallback: str = "") -> str:
    code = str(value or fallback).strip().upper()
    if code in INVALID_REGIONS:
        return ""
    return REGION_ALIASES.get(code, code)


def region_from_filename(path: Path) -> str:
    for pattern, region in REGION_FROM_FILENAME:
        if pattern.search(path.stem.lower()):
            return region
    return ""


def snapshot_date(path: Path) -> str:
    parts = path.parts
    for idx, part in enumerate(parts):
        if part == "weather" and idx + 1 < len(parts):
            candidate = parts[idx + 1]
            if len(candidate) == 10 and candidate[4] == "-" and candidate[7] == "-":
                return candidate
    return datetime.fromtimestamp(path.stat().st_mtime).date().isoformat()


def discover_history_weather_files() -> list[Path]:
    if not HISTORY_WEATHER_DIR.is_dir():
        return []
    found: list[Path] = []
    for path in sorted(HISTORY_WEATHER_DIR.rglob("marketing_weather_report*.csv")):
        if path.is_file() and path.name not in EXCLUDE_FILENAMES:
            found.append(path)
    return found


def empty_bridge() -> pd.DataFrame:
    return pd.DataFrame(columns=list(BRIDGE_COLUMNS))


def discover_raw_files() -> list[Path]:
    found: list[Path] = []
    seen: set[Path] = set()
    for directory in SCAN_DIRS:
        if not directory.is_dir():
            continue
        for pattern in FILE_PATTERNS:
            for path in sorted(directory.glob(pattern)):
                if path.name in EXCLUDE_FILENAMES or PREVIEW_SUFFIX.search(path.name):
                    continue
                resolved = path.resolve()
                if resolved not in seen:
                    seen.add(resolved)
                    found.append(path)
    for path in discover_history_weather_files():
        resolved = path.resolve()
        if resolved in seen:
            continue
        snap = snapshot_date(path)
        root_match = ROOT / path.name
        if root_match.is_file() and snapshot_date(root_match) == snap:
            continue
        seen.add(resolved)
        found.append(path)
    return found


def live_sales_dates() -> frozenset[str]:
    """Dates covered by the staged live sales bridge (recent Triple Whale window)."""
    live_path = ROOT / "raw_data" / "sales_data_bridge.csv"
    if not live_path.is_file():
        return frozenset()
    try:
        dates = pd.read_csv(live_path, usecols=["Date"], dtype=str)["Date"].dropna().astype(str)
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError, ValueError):
        return frozenset()
    return frozenset(dates.unique())


def build_geo_lookup(paths: list[Path]) -> dict[tuple[str, str, str], tuple[float, float]]:
    lookup: dict[tuple[str, str, str], tuple[float, float]] = {}
    for path in paths:
        if "marketing_weather_report" not in path.name.lower():
            continue
        region = region_from_filename(path) or "US"
        try:
            df = pd.read_csv(path, dtype=str, usecols=lambda c: c in {"City", "State", "Lat", "Lon"})
        except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError, ValueError) as exc:
            log(f"SKIP geo {path.name}: {exc}")
            continue
        for _, row in df.iterrows():
            city = str(row.get("City", "")).strip().casefold()
            state = str(row.get("State", "")).strip().casefold()
            lat = pd.to_numeric(row.get("Lat"), errors="coerce")
            lon = pd.to_numeric(row.get("Lon"), errors="coerce")
            if city and state and pd.notna(lat) and pd.notna(lon):
                lookup[(region, city, state)] = (float(lat), float(lon))
    return lookup


def resolve_coords(row: pd.Series, region: str, geo: dict) -> tuple[float | None, float | None]:
    lat = pd.to_numeric(row.get("Lat", row.get("lat")), errors="coerce")
    lon = pd.to_numeric(row.get("Lon", row.get("lon")), errors="coerce")
    if pd.notna(lat) and pd.notna(lon):
        return float(lat), float(lon)
    city = str(row.get("Shipping_City", row.get("City", ""))).strip().casefold()
    state = str(row.get("Shipping_State", row.get("State", ""))).strip().casefold()
    if city and state and (region, city, state) in geo:
        return geo[(region, city, state)]
    return None, None


def emit_rows(
    *,
    date: str,
    region: str,
    lat: float | None,
    lon: float | None,
    pairs: list[tuple[str, object]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for type_name, value in pairs:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            continue
        if isinstance(value, str) and not value.strip():
            continue
        rows.append(
            {"Date": date, "Region": region, "Type": type_name, "Value": value, "Lat": lat, "Lon": lon}
        )
    return rows


def transform_marketing_weather(path: Path, geo: dict) -> pd.DataFrame:
    region = region_from_filename(path)
    if not region:
        log(f"SKIP {path.name}: cannot infer Region from filename.")
        return empty_bridge()
    try:
        df = pd.read_csv(path, dtype=str, low_memory=False)
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError) as exc:
        log(f"SKIP {path.name}: {exc}")
        return empty_bridge()
    if df.empty:
        log(f"SKIP {path.name}: empty.")
        return empty_bridge()

    snap = snapshot_date(path)
    records: list[dict[str, object]] = []
    for _, row in df.iterrows():
        lat, lon = resolve_coords(row, region, geo)
        records.extend(
            emit_rows(
                date=snap,
                region=region,
                lat=lat,
                lon=lon,
                pairs=[
                    ("suggested_strategy", str(row.get("Marketing_Action", "")).strip()),
                    ("weather_rain", pd.to_numeric(row.get("Rain_Amount"), errors="coerce")),
                    ("weather_yesterday_rain", pd.to_numeric(row.get("Yesterday_Rain"), errors="coerce")),
                    ("weather_uv", pd.to_numeric(row.get("Max_UV_Index"), errors="coerce")),
                ],
            )
        )
    out = pd.DataFrame(records, columns=list(BRIDGE_COLUMNS))
    log(f"OK {path.name}: {len(out):,} rows → Region={region}, Date={snap}")
    return out


def transform_sales(path: Path, geo: dict, *, exclude_dates: frozenset[str] | None = None) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, dtype=str, low_memory=False)
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError) as exc:
        log(f"SKIP {path.name}: {exc}")
        return empty_bridge()
    return transform_sales_dataframe(df, geo, label=path.name, exclude_dates=exclude_dates)


def transform_sales_dataframe(
    df: pd.DataFrame,
    geo: dict,
    *,
    label: str = "sales",
    exclude_dates: frozenset[str] | None = None,
) -> pd.DataFrame:
    if df.empty:
        return empty_bridge()

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    file_region = region_from_filename(Path(label)) if label.endswith(".csv") else ""
    records: list[dict[str, object]] = []

    for _, row in df.iterrows():
        dt = pd.to_datetime(row.get("Date", row.get("date")), errors="coerce")
        if pd.isna(dt):
            continue
        date_str = dt.date().isoformat()
        if exclude_dates and date_str in exclude_dates:
            continue
        region = normalize_region(
            row.get("Region", row.get("Shipping_Country", row.get("region"))),
            fallback=file_region,
        )
        if not region:
            continue
        lat, lon = resolve_coords(row, region, geo)
        revenue = pd.to_numeric(row.get("Order_Revenue", row.get("Revenue")), errors="coerce")
        units = pd.to_numeric(row.get("Quantity_Sold", row.get("Units_Sold")), errors="coerce")
        action = str(row.get("Marketing_Action", row.get("Actual_Action", ""))).strip()

        pairs: list[tuple[str, object]] = []
        if pd.notna(revenue) and revenue != 0:
            pairs.append(("sales_revenue", float(revenue)))
        if pd.notna(units) and units != 0:
            pairs.append(("sales_units", float(units)))
        if action and action.lower() not in {"nan", "none"}:
            pairs.append(("actual_action", action))
        records.extend(
            emit_rows(date=date_str, region=region, lat=lat, lon=lon, pairs=pairs)
        )

    out = pd.DataFrame(records, columns=list(BRIDGE_COLUMNS))
    log(f"OK {label}: {len(out):,} rows")
    return out


def transform_historical_sales(geo: dict, *, exclude_dates: frozenset[str]) -> pd.DataFrame:
    """Load committed/local historical sales (omnichannel + archives) into bridge rows."""
    try:
        import sync_sales_bridge as sb
    except ImportError:
        return empty_bridge()

    historical = sb.load_historical_sales()
    if historical.empty:
        return empty_bridge()
    if exclude_dates:
        historical = historical[~historical["Date"].astype(str).isin(exclude_dates)].copy()
    if historical.empty:
        return empty_bridge()
    return transform_sales_dataframe(historical, geo, label="historical_sales", exclude_dates=None)


def transform_execution(path: Path, geo: dict) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, dtype=str, low_memory=False)
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError) as exc:
        log(f"SKIP {path.name}: {exc}")
        return empty_bridge()
    if df.empty:
        return empty_bridge()

    df.columns = [str(c).strip() for c in df.columns]
    action_col = next(
        (c for c in ("Actual_Action", "Marketing_Action", "Action", "Strategy") if c in df.columns),
        None,
    )
    if not action_col:
        log(f"SKIP {path.name}: no action column.")
        return empty_bridge()

    file_region = region_from_filename(path)
    records: list[dict[str, object]] = []
    for _, row in df.iterrows():
        dt = pd.to_datetime(row.get("Date", row.get("date")), errors="coerce")
        if pd.isna(dt):
            continue
        region = normalize_region(row.get("Region", row.get("Shipping_Country")), file_region)
        if not region:
            continue
        lat, lon = resolve_coords(row, region, geo)
        action = str(row.get(action_col, "")).strip()
        if not action:
            continue
        records.extend(
            emit_rows(
                date=dt.date().isoformat(),
                region=region,
                lat=lat,
                lon=lon,
                pairs=[("actual_action", action)],
            )
        )
    out = pd.DataFrame(records, columns=list(BRIDGE_COLUMNS))
    log(f"OK {path.name}: {len(out):,} rows")
    return out


def transform_file(path: Path, geo: dict) -> pd.DataFrame:
    name = path.name.lower()
    if "marketing_weather_report" in name:
        return transform_marketing_weather(path, geo)
    if "marketing_execution" in name:
        return transform_execution(path, geo)
    return transform_sales(path, geo)


def clean_master(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return empty_bridge()

    out = df.copy()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
    out = out[out["Date"].notna()].copy()
    out["Date"] = out["Date"].dt.date.astype(str)
    out["Region"] = out["Region"].astype(str).str.strip().str.upper().map(normalize_region)
    out = out[~out["Region"].isin(INVALID_REGIONS)].copy()
    out["Type"] = out["Type"].astype(str).str.strip()
    out = out[out["Type"].ne("")].copy()
    out["Lat"] = pd.to_numeric(out["Lat"], errors="coerce")
    out["Lon"] = pd.to_numeric(out["Lon"], errors="coerce")

    numeric_mask = out["Type"].str.startswith(("sales_", "weather_"))
    out.loc[numeric_mask, "Value"] = pd.to_numeric(out.loc[numeric_mask, "Value"], errors="coerce")
    out.loc[~numeric_mask, "Value"] = out.loc[~numeric_mask, "Value"].astype(str).str.strip()

    return out[list(BRIDGE_COLUMNS)].sort_values(["Date", "Region", "Type"]).reset_index(drop=True)


def compact_for_git(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep strategy/weather rows as-is; collapse sales_* to Date×Region totals.

    SKU-level sales belong in Google Sheets / local previews — not git (GH 100MB limit).
    """
    if df.empty:
        return df
    sales_mask = df["Type"].astype(str).str.startswith("sales_")
    strategy = df.loc[~sales_mask].copy()
    sales = df.loc[sales_mask].copy()
    if sales.empty:
        return strategy.reset_index(drop=True)

    sales["Value"] = pd.to_numeric(sales["Value"], errors="coerce")
    sales = sales[sales["Value"].notna()]
    sales_agg = (
        sales.groupby(["Date", "Region", "Type"], as_index=False, dropna=False)
        .agg(Value=("Value", "sum"))
        .assign(Lat=pd.NA, Lon=pd.NA)
    )
    sales_agg = sales_agg[list(BRIDGE_COLUMNS)]
    out = pd.concat([strategy, sales_agg], ignore_index=True)
    return out.sort_values(["Date", "Region", "Type"]).reset_index(drop=True)


def run_pipeline() -> pd.DataFrame:
    log("Scanning project root and raw_data/ …")
    paths = discover_raw_files()
    if not paths:
        log("No matching CSV files found. Drop exports into project root or raw_data/.")
        master = empty_bridge()
    else:
        log(f"Processing {len(paths)} file(s):")
        for p in paths:
            log(f"  · {p.relative_to(ROOT) if p.is_relative_to(ROOT) else p.name}")
        geo = build_geo_lookup(paths)
        exclude_dates = live_sales_dates()
        frames = [transform_file(p, geo) for p in paths]
        hist_sales = transform_historical_sales(geo, exclude_dates=exclude_dates)
        if not hist_sales.empty:
            frames.append(hist_sales)
        master = clean_master(pd.concat([f for f in frames if not f.empty], ignore_index=True))

    master = compact_for_git(master)
    master.to_csv(OUTPUT_PATH, index=False)
    log(f"Saved {len(master):,} rows → {OUTPUT_PATH.name}")

    if master.empty:
        log("Type breakdown: (empty)")
    else:
        counts = master["Type"].value_counts().sort_index()
        log("Type breakdown:")
        for type_name, count in counts.items():
            log(f"  {type_name}: {count:,}")

    return master


# ---------------------------------------------------------------------------
# Phase 1 — Historical verification (observed rain ↔ umbrella sales)
# ---------------------------------------------------------------------------


def empty_verification() -> pd.DataFrame:
    return pd.DataFrame(columns=list(VERIFICATION_COLUMNS))


def load_umbrella_sku_signals() -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "mode": "exclude_sun",
        "include_prefixes": [],
        "include_contains": ["umb", "brella"],
        "exclude_prefixes": ["HS01", "HS0", "SLS01"],
        "exclude_contains": ["hat", "shirt", "hoodie", "sunprot"],
    }
    if not UMBRELLA_SKU_SIGNALS_PATH.is_file():
        log(f"WARN missing {UMBRELLA_SKU_SIGNALS_PATH.name}; using built-in umbrella signals.")
        return defaults
    try:
        payload = json.loads(UMBRELLA_SKU_SIGNALS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        log(f"WARN could not read umbrella signals ({exc}); using defaults.")
        return defaults
    if not isinstance(payload, dict):
        return defaults
    merged = {**defaults, **payload}
    for key in ("include_prefixes", "include_contains", "exclude_prefixes", "exclude_contains"):
        value = merged.get(key, [])
        merged[key] = [str(v) for v in value] if isinstance(value, list) else []
    merged["mode"] = str(merged.get("mode") or "exclude_sun").strip().lower()
    return merged


_FBA_PREFIX_RE = re.compile(r"^(?:FBA\d*|FBM\d*)[-_\s]+", re.I)


def normalize_sku(sku: object) -> str:
    text = str(sku or "").strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return ""
    return _FBA_PREFIX_RE.sub("", text).strip()


def is_umbrella_sku(sku: object, signals: dict[str, Any]) -> bool:
    """Classify SKU via data/umbrella_sku_signals.json (tags / prefixes / keywords)."""
    normalized = normalize_sku(sku)
    if not normalized:
        return False
    folded = normalized.casefold()
    upper = normalized.upper()

    for needle in signals.get("include_contains", []):
        if needle and needle.casefold() in folded:
            return True
    for prefix in signals.get("include_prefixes", []):
        if prefix and upper.startswith(str(prefix).upper()):
            return True
    for needle in signals.get("exclude_contains", []):
        if needle and needle.casefold() in folded:
            return False
    for prefix in signals.get("exclude_prefixes", []):
        if prefix and upper.startswith(str(prefix).upper()):
            return False

    mode = str(signals.get("mode", "exclude_sun")).lower()
    if mode == "include_only":
        return False
    # exclude_sun (default): remaining catalog treated as umbrella product.
    return True


def load_city_coordinate_lookup() -> dict[tuple[str, str, str], tuple[float, float]]:
    lookup: dict[tuple[str, str, str], tuple[float, float]] = {}
    if not CITY_COORDINATES_PATH.is_file():
        log(f"WARN missing {CITY_COORDINATES_PATH.name}; verification geo join limited.")
        return lookup
    try:
        payload = json.loads(CITY_COORDINATES_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        log(f"WARN city coordinates unreadable: {exc}")
        return lookup
    for row in payload.get("rows", []):
        region = normalize_region(row.get("region", "US")) or "US"
        city = str(row.get("city", "")).strip().casefold()
        state = str(row.get("state", "")).strip().casefold()
        lat = pd.to_numeric(row.get("lat"), errors="coerce")
        lon = pd.to_numeric(row.get("lon"), errors="coerce")
        if region and city and state and pd.notna(lat) and pd.notna(lon):
            lookup[(region, city, state)] = (float(lat), float(lon))
    return lookup


def discover_verification_sales_paths() -> list[Path]:
    """Prefer the master preview; fall back to staged raw bridge / ledger."""
    if MASTER_SALES_PREVIEW.is_file():
        return [MASTER_SALES_PREVIEW]
    if RAW_SALES_BRIDGE.is_file():
        return [RAW_SALES_BRIDGE]
    ledger = ROOT / "data" / "history" / "sales_ledger.csv"
    if ledger.is_file():
        return [ledger]
    return []


def load_transactional_sales_for_verification() -> pd.DataFrame:
    """Load city-level transactional sales (SKU grain) for Phase 1 matching."""
    frames: list[pd.DataFrame] = []
    for path in discover_verification_sales_paths():
        try:
            df = pd.read_csv(path, dtype=str, low_memory=False)
        except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError) as exc:
            log(f"SKIP verification sales {path.name}: {exc}")
            continue
        if df.empty:
            continue
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        frames.append(df)
        log(f"Verification sales source: {path.relative_to(ROOT) if path.is_relative_to(ROOT) else path.name} ({len(df):,} rows)")

    try:
        import sync_sales_bridge as sb

        historical = sb.load_historical_sales()
        if not historical.empty:
            frames.append(historical)
            log(f"Verification sales source: historical_sales ({len(historical):,} rows)")
    except Exception as exc:  # noqa: BLE001 — keep verification resilient
        log(f"WARN historical sales unavailable for verification: {exc}")

    if not frames:
        return pd.DataFrame(
            columns=[
                "Date",
                "Region",
                "City",
                "State",
                "SKU",
                "Quantity_Sold",
                "Order_Revenue",
            ]
        )

    raw = pd.concat(frames, ignore_index=True)
    raw.columns = [str(c).strip() for c in raw.columns]

    out = pd.DataFrame(
        {
            "Date": pd.to_datetime(raw.get("Date", raw.get("date")), errors="coerce"),
            "City": raw.get("Shipping_City", raw.get("City", "")).astype(str).str.strip(),
            "State": raw.get("Shipping_State", raw.get("State", "")).astype(str).str.strip(),
            "Region": raw.get("Region", raw.get("Shipping_Country", "")),
            "SKU": raw.get("SKU", "").astype(str).str.strip(),
            "Quantity_Sold": pd.to_numeric(
                raw.get("Quantity_Sold", raw.get("Units_Sold")), errors="coerce"
            ).fillna(0.0),
            "Order_Revenue": pd.to_numeric(
                raw.get("Order_Revenue", raw.get("Revenue")), errors="coerce"
            ).fillna(0.0),
        }
    )
    out["Region"] = out["Region"].map(lambda v: normalize_region(v) or "")
    out = out[out["Date"].notna()].copy()
    out["Date"] = out["Date"].dt.date.astype(str)
    out = out[~out["City"].str.upper().isin(BLOCKED_CITIES)].copy()
    out = out[out["Region"].isin({"US", "UK", "CAN"})].copy()
    out = out[(out["Order_Revenue"] != 0) | (out["Quantity_Sold"] != 0)].copy()
    return out.reset_index(drop=True)


def attach_coordinates(sales: pd.DataFrame, geo: dict[tuple[str, str, str], tuple[float, float]]) -> pd.DataFrame:
    if sales.empty:
        return sales.assign(Lat=pd.NA, Lon=pd.NA)

    def _lookup(row: pd.Series) -> tuple[float | None, float | None]:
        region = str(row["Region"])
        city = str(row["City"]).casefold()
        state = str(row["State"]).casefold()
        hit = geo.get((region, city, state))
        if hit:
            return hit
        # Soft fallback: city-only match within region when state is blank/mismatched.
        if city:
            for (reg, cty, _st), coords in geo.items():
                if reg == region and cty == city:
                    return coords
        return None, None

    coords = sales.apply(_lookup, axis=1, result_type="expand")
    sales = sales.copy()
    sales["Lat"] = coords[0]
    sales["Lon"] = coords[1]
    return sales.dropna(subset=["Lat", "Lon"]).copy()


def load_observed_precip_cache() -> pd.DataFrame:
    if not OBSERVED_PRECIP_CACHE_PATH.is_file():
        return pd.DataFrame(columns=["Lat", "Lon", "Date", "Observed_Precip_In"])
    try:
        cache = pd.read_csv(OBSERVED_PRECIP_CACHE_PATH, dtype=str)
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame(columns=["Lat", "Lon", "Date", "Observed_Precip_In"])
    if cache.empty:
        return pd.DataFrame(columns=["Lat", "Lon", "Date", "Observed_Precip_In"])
    cache["Lat"] = pd.to_numeric(cache["Lat"], errors="coerce").round(4)
    cache["Lon"] = pd.to_numeric(cache["Lon"], errors="coerce").round(4)
    cache["Observed_Precip_In"] = pd.to_numeric(cache["Observed_Precip_In"], errors="coerce")
    cache["Date"] = cache["Date"].astype(str)
    return cache.dropna(subset=["Lat", "Lon", "Date"]).drop_duplicates(
        subset=["Lat", "Lon", "Date"], keep="last"
    )


def save_observed_precip_cache(cache: pd.DataFrame) -> None:
    OBSERVED_PRECIP_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    out = cache.copy()
    out["Lat"] = pd.to_numeric(out["Lat"], errors="coerce").round(4)
    out["Lon"] = pd.to_numeric(out["Lon"], errors="coerce").round(4)
    out = out.dropna(subset=["Lat", "Lon", "Date"]).drop_duplicates(
        subset=["Lat", "Lon", "Date"], keep="last"
    )
    out.to_csv(OBSERVED_PRECIP_CACHE_PATH, index=False)


def _fetch_open_meteo_daily_precip(
    lat: float, lon: float, start_date: str, end_date: str
) -> list[tuple[str, float]]:
    """Return [(date, precip_inches), ...] from Open-Meteo historical archive."""
    params = {
        "latitude": f"{lat:.4f}",
        "longitude": f"{lon:.4f}",
        "start_date": start_date,
        "end_date": end_date,
        "daily": "precipitation_sum",
        "timezone": "UTC",
    }
    url = f"{OPEN_METEO_ARCHIVE}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": "WeathermanMarketingTool/3.0-verification"})
    last_error: Exception | None = None
    for attempt in range(PRECIP_MAX_RETRIES):
        try:
            with urlopen(req, timeout=30) as resp:  # noqa: S310 — fixed Open-Meteo host
                payload = json.loads(resp.read().decode("utf-8"))
            daily = payload.get("daily") or {}
            dates = daily.get("time") or []
            precip_mm = daily.get("precipitation_sum") or []
            rows: list[tuple[str, float]] = []
            for date_str, mm in zip(dates, precip_mm, strict=False):
                if mm is None:
                    continue
                rows.append((str(date_str), float(mm) * MM_TO_IN))
            return rows
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
            last_error = exc
            time.sleep(min(8.0, 1.5 * (2**attempt)))
    raise RuntimeError(f"Open-Meteo archive failed for {lat},{lon}: {last_error}")


def fetch_observed_precip_for_locations(
    locations: pd.DataFrame,
    *,
    start_date: str,
    end_date: str,
    cache: pd.DataFrame,
) -> pd.DataFrame:
    """
    Fetch observed daily precip for unique lat/lon hubs over [start_date, end_date].
    Uses on-disk cache; only gaps hit the Open-Meteo archive API.
    """
    if locations.empty:
        return cache

    hubs = (
        locations[["Lat", "Lon"]]
        .assign(
            Lat=lambda d: pd.to_numeric(d["Lat"], errors="coerce").round(4),
            Lon=lambda d: pd.to_numeric(d["Lon"], errors="coerce").round(4),
        )
        .dropna()
        .drop_duplicates()
    )
    if hubs.empty:
        return cache

    needed_dates = pd.date_range(start_date, end_date, freq="D").strftime("%Y-%m-%d").tolist()
    cache_keys = set()
    if not cache.empty:
        cache_keys = {
            (round(float(r.Lat), 4), round(float(r.Lon), 4), str(r.Date))
            for r in cache.itertuples(index=False)
        }

    to_fetch: list[tuple[float, float]] = []
    for row in hubs.itertuples(index=False):
        lat, lon = float(row.Lat), float(row.Lon)
        missing = any((lat, lon, d) not in cache_keys for d in needed_dates)
        if missing:
            to_fetch.append((lat, lon))

    if not to_fetch:
        log(f"Observed precip cache hit for all {len(hubs):,} hubs.")
        return cache

    log(
        f"Fetching Open-Meteo archive precip for {len(to_fetch):,} hub(s) "
        f"({start_date} → {end_date})…"
    )
    new_rows: list[dict[str, object]] = []

    def _one(lat: float, lon: float) -> list[dict[str, object]]:
        time.sleep(PRECIP_REQUEST_PAUSE_S)
        series = _fetch_open_meteo_daily_precip(lat, lon, start_date, end_date)
        return [
            {"Lat": lat, "Lon": lon, "Date": date_str, "Observed_Precip_In": precip}
            for date_str, precip in series
        ]

    errors = 0
    with ThreadPoolExecutor(max_workers=PRECIP_FETCH_WORKERS) as pool:
        futures = {pool.submit(_one, lat, lon): (lat, lon) for lat, lon in to_fetch}
        for fut in as_completed(futures):
            lat, lon = futures[fut]
            try:
                new_rows.extend(fut.result())
            except Exception as exc:  # noqa: BLE001
                errors += 1
                log(f"WARN precip fetch failed ({lat},{lon}): {exc}")

    if new_rows:
        fetched = pd.DataFrame(new_rows)
        cache = pd.concat([cache, fetched], ignore_index=True)
        save_observed_precip_cache(cache)
        log(f"Cached {len(fetched):,} observed precip rows ({errors} hub errors).")
    elif errors:
        log(f"WARN no precip rows fetched ({errors} hub errors).")

    return load_observed_precip_cache()


def aggregate_umbrella_city_days(sales: pd.DataFrame, signals: dict[str, Any]) -> pd.DataFrame:
    if sales.empty:
        return empty_verification().iloc[0:0]

    tagged = sales.copy()
    tagged["Is_Umbrella"] = tagged["SKU"].map(lambda s: is_umbrella_sku(s, signals))
    tagged["City"] = tagged["City"].astype(str).str.strip()
    tagged["State"] = tagged["State"].astype(str).str.strip()

    group_cols = ["Date", "Region", "City", "State", "Lat", "Lon"]
    umbrella = (
        tagged[tagged["Is_Umbrella"]]
        .groupby(group_cols, as_index=False)
        .agg(Umbrella_Revenue=("Order_Revenue", "sum"), Umbrella_Units=("Quantity_Sold", "sum"))
    )
    other = (
        tagged[~tagged["Is_Umbrella"]]
        .groupby(group_cols, as_index=False)
        .agg(
            Non_Umbrella_Revenue=("Order_Revenue", "sum"),
            Non_Umbrella_Units=("Quantity_Sold", "sum"),
        )
    )
    base = (
        tagged.groupby(group_cols, as_index=False)
        .size()
        .drop(columns="size")
    )
    out = base.merge(umbrella, on=group_cols, how="left").merge(other, on=group_cols, how="left")
    for col in (
        "Umbrella_Revenue",
        "Umbrella_Units",
        "Non_Umbrella_Revenue",
        "Non_Umbrella_Units",
    ):
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    return out


def build_verification_bridge() -> pd.DataFrame:
    """
    Phase 1 proof dataset: umbrella sales matched to *observed* Open-Meteo archive
    precip on the same date + city (US / UK / CAN).
    """
    log("── Phase 1 verification bridge ──")
    signals = load_umbrella_sku_signals()
    log(f"Umbrella SKU mode={signals.get('mode')}")

    sales = load_transactional_sales_for_verification()
    if sales.empty:
        log("No transactional sales with real cities for verification.")
        out = empty_verification()
        out.to_csv(VERIFICATION_OUTPUT_PATH, index=False)
        return out

    geo = load_city_coordinate_lookup()
    sales = attach_coordinates(sales, geo)
    if sales.empty:
        log("No sales rows joined to city coordinates.")
        out = empty_verification()
        out.to_csv(VERIFICATION_OUTPUT_PATH, index=False)
        return out

    city_days = aggregate_umbrella_city_days(sales, signals)
    umbrella_days = city_days[city_days["Umbrella_Revenue"] > 0].copy()
    if umbrella_days.empty:
        # Still emit city-days so stakeholders can inspect non-umbrella mix.
        umbrella_days = city_days.copy()
        log("WARN no umbrella-tagged revenue; check data/umbrella_sku_signals.json.")

    start_date = str(umbrella_days["Date"].min())
    end_date = str(umbrella_days["Date"].max())
    cache = load_observed_precip_cache()
    cache = fetch_observed_precip_for_locations(
        umbrella_days,
        start_date=start_date,
        end_date=end_date,
        cache=cache,
    )

    precip = cache.copy()
    precip["Lat"] = pd.to_numeric(precip["Lat"], errors="coerce").round(4)
    precip["Lon"] = pd.to_numeric(precip["Lon"], errors="coerce").round(4)
    precip["Date"] = precip["Date"].astype(str)

    joined = umbrella_days.copy()
    joined["Lat"] = pd.to_numeric(joined["Lat"], errors="coerce").round(4)
    joined["Lon"] = pd.to_numeric(joined["Lon"], errors="coerce").round(4)
    joined = joined.merge(precip, on=["Lat", "Lon", "Date"], how="left")
    joined["Observed_Precip_In"] = pd.to_numeric(joined["Observed_Precip_In"], errors="coerce")
    # Drop city-days where archive weather could not be resolved — proof requires observed rain.
    before = len(joined)
    joined = joined[joined["Observed_Precip_In"].notna()].copy()
    log(f"Weather-matched city-days: {len(joined):,} / {before:,}")

    joined["Is_Rainy"] = joined["Observed_Precip_In"] >= RAIN_THRESHOLD_INCHES
    joined["Verified_Match"] = (joined["Umbrella_Revenue"] > 0) & joined["Is_Rainy"]

    out = joined[list(VERIFICATION_COLUMNS)].sort_values(
        ["Date", "Region", "City"]
    ).reset_index(drop=True)
    out.to_csv(VERIFICATION_OUTPUT_PATH, index=False)

    umbrella_rev = float(out["Umbrella_Revenue"].sum())
    rainy_rev = float(out.loc[out["Is_Rainy"], "Umbrella_Revenue"].sum())
    pct = (100.0 * rainy_rev / umbrella_rev) if umbrella_rev > 0 else 0.0
    log(
        f"Saved {len(out):,} rows → {VERIFICATION_OUTPUT_PATH.name} · "
        f"umbrella $ on rainy days = {pct:.1f}% "
        f"(threshold {RAIN_THRESHOLD_INCHES:.2f} in)"
    )
    return out


# ---------------------------------------------------------------------------
# Phase 2 — Forecast triggers + ad-spend allocation
# ---------------------------------------------------------------------------


def _fetch_open_meteo_forecast(hubs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Bulk daily forecast (precip inches + max UV) for a batch of hubs."""
    params = {
        "latitude": ",".join(f"{h['lat']:.4f}" for h in hubs),
        "longitude": ",".join(f"{h['lon']:.4f}" for h in hubs),
        "daily": "precipitation_sum,uv_index_max",
        "forecast_days": str(FORECAST_DAYS),
        "timezone": "auto",
        "precipitation_unit": "inch",
    }
    url = f"{OPEN_METEO_FORECAST}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": "WeathermanMarketingTool/3.1-forecast"})
    last_error: Exception | None = None
    for attempt in range(PRECIP_MAX_RETRIES):
        try:
            with urlopen(req, timeout=45) as resp:  # noqa: S310 — fixed Open-Meteo host
                payload = json.loads(resp.read().decode("utf-8"))
            break
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
            last_error = exc
            time.sleep(min(8.0, 1.5 * (2**attempt)))
    else:
        raise RuntimeError(f"Open-Meteo forecast failed: {last_error}")

    results = payload if isinstance(payload, list) else [payload]
    rows: list[dict[str, Any]] = []
    for hub, result in zip(hubs, results, strict=False):
        daily = result.get("daily") or {}
        for date_str, precip, uv in zip(
            daily.get("time") or [],
            daily.get("precipitation_sum") or [],
            daily.get("uv_index_max") or [],
            strict=False,
        ):
            rows.append(
                {
                    "Date": str(date_str),
                    "Region": hub["region"],
                    "City": hub["city"],
                    "State": hub["state"],
                    "Lat": hub["lat"],
                    "Lon": hub["lon"],
                    "Forecast_Precip_In": float(precip or 0.0),
                    "Forecast_UV": float(uv) if uv is not None else 0.0,
                }
            )
    return rows


def decide_forecast_action(precip_in: float, uv: float) -> tuple[str, str]:
    if precip_in >= RAIN_THRESHOLD_INCHES:
        return "Scale Umbrellas", f"Forecast rain {precip_in:.2f} in ≥ {RAIN_THRESHOLD_INCHES:.2f} in"
    if uv >= UV_THRESHOLD:
        return "Sun Protection (Hats/Shirts)", f"Forecast UV {uv:.1f} ≥ {UV_THRESHOLD:.0f}"
    return "Baseline", "No forecast trigger"


def historical_umbrella_weights() -> pd.DataFrame:
    """Per-city historical umbrella revenue — the ad-allocation weight from Phase 1."""
    empty = pd.DataFrame(columns=["Region", "City", "Hist_Umbrella_Revenue"])
    if not VERIFICATION_OUTPUT_PATH.is_file():
        return empty
    try:
        ver = pd.read_csv(VERIFICATION_OUTPUT_PATH, low_memory=False)
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError):
        return empty
    if ver.empty or "Umbrella_Revenue" not in ver.columns:
        return empty
    ver["Umbrella_Revenue"] = pd.to_numeric(ver["Umbrella_Revenue"], errors="coerce").fillna(0.0)
    return (
        ver.groupby(["Region", "City"], as_index=False)["Umbrella_Revenue"]
        .sum()
        .rename(columns={"Umbrella_Revenue": "Hist_Umbrella_Revenue"})
    )


def allocate_ad_spend(df: pd.DataFrame) -> pd.DataFrame:
    """
    Priority = historical umbrella revenue × forecast intensity.
    Spend share is normalized per Date across triggered (non-Baseline) cities.
    """
    out = df.copy()
    intensity = (out["Forecast_Precip_In"] / RAIN_THRESHOLD_INCHES).clip(lower=0.0, upper=3.0)
    sun = out["Action"].eq("Sun Protection (Hats/Shirts)")
    intensity = intensity.where(~sun, (out["Forecast_UV"] / UV_THRESHOLD).clip(lower=0.0, upper=2.0))

    # Cities with no Phase 1 history still get a floor weight so new markets are not invisible.
    weight = out["Hist_Umbrella_Revenue"].fillna(0.0)
    floor = float(weight[weight > 0].median()) if (weight > 0).any() else 1.0
    weight = weight.where(weight > 0, floor * 0.1)

    out["Priority_Score"] = (weight * intensity).where(out["Action"].ne("Baseline"), 0.0).round(2)
    totals = out.groupby("Date")["Priority_Score"].transform("sum")
    out["Spend_Share"] = (out["Priority_Score"] / totals.where(totals > 0, pd.NA)).fillna(0.0).round(6)
    return out


def build_forecast_bridge() -> pd.DataFrame:
    """Phase 2: 5-day forecast triggers per hub + normalized ad-spend allocation."""
    log("── Phase 2 forecast & ad-spend bridge ──")
    geo = load_city_coordinate_lookup()
    if not geo:
        log("No city coordinates; skipping forecast bridge.")
        out = pd.DataFrame(columns=list(FORECAST_COLUMNS))
        out.to_csv(FORECAST_OUTPUT_PATH, index=False)
        return out

    hubs = [
        {"region": region, "city": city, "state": state, "lat": lat, "lon": lon}
        for (region, city, state), (lat, lon) in geo.items()
    ]
    log(f"Fetching {FORECAST_DAYS}-day forecast for {len(hubs):,} hubs…")

    rows: list[dict[str, Any]] = []
    for start in range(0, len(hubs), FORECAST_BATCH_SIZE):
        batch = hubs[start : start + FORECAST_BATCH_SIZE]
        try:
            rows.extend(_fetch_open_meteo_forecast(batch))
        except Exception as exc:  # noqa: BLE001 — partial forecast beats no forecast
            log(f"WARN forecast batch {start}: {exc}")
        time.sleep(PRECIP_REQUEST_PAUSE_S)

    if not rows:
        log("WARN no forecast rows fetched.")
        out = pd.DataFrame(columns=list(FORECAST_COLUMNS))
        out.to_csv(FORECAST_OUTPUT_PATH, index=False)
        return out

    df = pd.DataFrame(rows)
    # Title-case city keys back for display; lookup keys were casefolded.
    df["City"] = df["City"].astype(str).str.title()
    df["State"] = df["State"].astype(str).str.title()
    actions = df.apply(
        lambda r: decide_forecast_action(r["Forecast_Precip_In"], r["Forecast_UV"]), axis=1
    )
    df["Action"] = [a for a, _ in actions]
    df["Trigger_Reason"] = [t for _, t in actions]

    weights = historical_umbrella_weights()
    if not weights.empty:
        weights["City"] = weights["City"].astype(str).str.title()
        df = df.merge(weights, on=["Region", "City"], how="left")
    else:
        df["Hist_Umbrella_Revenue"] = 0.0
    df["Hist_Umbrella_Revenue"] = pd.to_numeric(
        df["Hist_Umbrella_Revenue"], errors="coerce"
    ).fillna(0.0)

    df = allocate_ad_spend(df)
    out = df[list(FORECAST_COLUMNS)].sort_values(
        ["Date", "Spend_Share"], ascending=[True, False]
    ).reset_index(drop=True)
    out.to_csv(FORECAST_OUTPUT_PATH, index=False)

    triggered = int((out["Action"] != "Baseline").sum())
    log(
        f"Saved {len(out):,} rows → {FORECAST_OUTPUT_PATH.name} · "
        f"{triggered:,} triggered city-days across {out['Date'].nunique()} days"
    )
    return out


def main() -> int:
    try:
        run_pipeline()
        build_verification_bridge()
        build_forecast_bridge()
    except Exception as exc:
        log(f"FATAL: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
