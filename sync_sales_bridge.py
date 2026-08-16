#!/usr/bin/env python3
"""
Sync Looker Studio regional bridge tables into Google Sheets.

Hybrid sales pipeline:
- Historical ledger (local CSV): immutable past rows with real Shipping_City / Shipping_State.
- Live stream (Triple Whale): recent window only; city/state masked as "blocked".
- Geographic weights from data/orders_by_city.csv split live totals into city-level rows.
- Unified append per region before weather marketing joins.

Clears and overwrites sheet1 in:
    Weather_Pulse_Sales_Bridge — combined US/UK/CA transactional SKU bridge (master)
    Weather_Pulse_Bridge_{region} - MM-DD-YY — daily city audit matrix per region

Auth:
- Triple Whale: TRIPLE_WHALE_API_KEY (env var)
- Google Sheets: GOOGLE_CREDS_JSON (raw service-account JSON string), or
  google_creds.json on disk for local runs (used automatically if env JSON is invalid)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np

import gspread
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from gspread.exceptions import APIError, GSpreadException, SpreadsheetNotFound, WorksheetNotFound
from gspread.utils import ValueInputOption, rowcol_to_a1

ROOT = Path(__file__).resolve().parent

SHOP_ID = "4a3474-24.myshopify.com"
API_BASE = "https://api.triplewhale.com/api/v2"
SQL_ENDPOINT = f"{API_BASE}/orcabase/api/sql"
START_DATE = date(2024, 1, 1)
DEFAULT_LIVE_LOOKBACK_DAYS = 7

SPREADSHEET_NAME = "Weather_Pulse_Sales_Bridge"  # legacy single-sheet name

HISTORICAL_LEDGER_FILENAMES = (
    "historical_sales_ledger.csv",
    "weather_pulse_sales_bridge_historical.csv",
    "sales_bridge_historical.csv",
)
HISTORICAL_REGION_FILENAMES = (
    "historical_sales_ledger_us.csv",
    "historical_sales_ledger_uk.csv",
    "historical_sales_ledger_ca.csv",
)
# Project root, WT2 parent, sibling Weather Tool, Projects, Weatherman.
HISTORICAL_SEARCH_DIRS = (
    ROOT / "data",
    ROOT,
    ROOT.parent,
    ROOT.parent.parent / "Weather Tool",
    ROOT.parent.parent,
    ROOT.parent.parent.parent,
)

PREFERRED_CRED_FILENAMES = (
    "google_creds.json",
    "credentials.json",
    "test_creds.json",
    "service_account.json",
    "gmail-milestone-tracker-497517-7162d422269c.json",
)
SKIP_JSON_FILENAMES = frozenset({
    "city_coordinates.json",
    "package.json",
    "package-lock.json",
    "tsconfig.json",
})
# Project root, cwd, WT2, Projects (+ gmail-milestone-tracker), Weatherman.
CREDS_SEARCH_DIRS = (
    ROOT,
    Path.cwd(),
    ROOT.parent,
    ROOT.parent.parent,
    ROOT.parent.parent / "gmail-milestone-tracker",
    ROOT.parent.parent.parent,
)

MARKETING_US_PATH = ROOT / "marketing_weather_report_us.csv"
MARKETING_UK_PATH = ROOT / "marketing_weather_report_uk.csv"
MARKETING_CAN_PATH = ROOT / "marketing_weather_report_can.csv"
ORDERS_BY_CITY_PATH = ROOT / "data" / "orders_by_city.csv"
CI_FRAMEWORK_PATH = ROOT / "data" / "city_framework.csv"
CI_STATE_WEIGHTS_PATH = ROOT / "data" / "city_distribution_weights_state.csv"
CI_COUNTRY_WEIGHTS_PATH = ROOT / "data" / "city_distribution_weights_country.csv"

UK_CODE_TO_REGION: dict[str, str] = {
    "ENG": "England",
    "SCT": "Scotland",
    "WLS": "Wales",
    "NIR": "Northern Ireland",
}
CAN_ABBREV_TO_REGION: dict[str, str] = {
    "ON": "Ontario",
    "BC": "British Columbia",
    "QC": "Quebec",
    "AB": "Alberta",
    "MB": "Manitoba",
    "SK": "Saskatchewan",
    "NS": "Nova Scotia",
    "NB": "New Brunswick",
    "NL": "Newfoundland and Labrador",
    "PE": "Prince Edward Island",
    "YT": "Yukon",
    "NT": "Northwest Territories",
    "NU": "Nunavut",
}
CAN_REGION_NAME_UPPER = {name.upper(): name for name in CAN_ABBREV_TO_REGION.values()}

# Regional bridge targets — one Google Sheet per market.
REGIONS: dict[str, dict[str, object]] = {
    "US": {
        "sales_countries": frozenset({"US"}),
        "marketing_path": MARKETING_US_PATH,
        "spreadsheet": "Weather_Pulse_Bridge_USA",
        "spreadsheet_id_env": "SALES_BRIDGE_SPREADSHEET_ID_USA",
        "audit_spreadsheet_id_env": "AUDIT_BRIDGE_SPREADSHEET_ID_USA",
    },
    "UK": {
        "sales_countries": frozenset({"UK"}),
        "marketing_path": MARKETING_UK_PATH,
        "spreadsheet": "Weather_Pulse_Bridge_UK",
        "spreadsheet_id_env": "SALES_BRIDGE_SPREADSHEET_ID_UK",
        "audit_spreadsheet_id_env": "AUDIT_BRIDGE_SPREADSHEET_ID_UK",
    },
    "CA": {
        "sales_countries": frozenset({"CA"}),
        "marketing_path": MARKETING_CAN_PATH,
        "spreadsheet": "Weather_Pulse_Bridge_CAN",
        "spreadsheet_id_env": "SALES_BRIDGE_SPREADSHEET_ID_CAN",
        "audit_spreadsheet_id_env": "AUDIT_BRIDGE_SPREADSHEET_ID_CAN",
    },
}

MASTER_SALES_BRIDGE_NAME = "Weather_Pulse_Sales_Bridge"
MASTER_SALES_BRIDGE_ID_ENV = "SALES_BRIDGE_SPREADSHEET_ID"

# Google Sheets writes: single-shot below threshold; batched above (master ~335k rows).
SHEET_WRITE_BATCH_ROWS = int(os.getenv("SHEET_WRITE_BATCH_ROWS", "5000"))
SHEET_BATCH_WRITE_THRESHOLD = int(os.getenv("SHEET_BATCH_WRITE_THRESHOLD", "8000"))
SHEET_BATCH_PAUSE_S = float(os.getenv("SHEET_BATCH_PAUSE_S", "0.5"))

# Google Sheets workbook hard limit; master export uses a slimmer column set + rolling window.
GOOGLE_SHEETS_MAX_CELLS = 10_000_000
GOOGLE_SHEETS_CELL_BUFFER = int(os.getenv("GOOGLE_SHEETS_CELL_BUFFER", "500000"))
MASTER_SHEETS_LOOKBACK_DAYS = int(os.getenv("MASTER_SHEETS_LOOKBACK_DAYS", "730"))

SALES_COLUMNS = [
    "Date",
    "Shipping_City",
    "Shipping_State",
    "Shipping_Country",
    "Platform",
    "Source_Name",
    "SKU",
    "Quantity_Sold",
    "Order_Revenue",
]

MARKETING_COLUMNS = [
    "Marketing_Action",
    "Rain_Amount",
    "Yesterday_Rain",
    "Max_UV_Index",
    "Logic_Summary",
]

OUTPUT_COLUMNS = SALES_COLUMNS + MARKETING_COLUMNS

# Google Sheets master export: full ledger stays in repo CSVs; Sheets gets recent rows without Logic_Summary.
MASTER_SHEETS_COLUMNS = SALES_COLUMNS + [c for c in MARKETING_COLUMNS if c != "Logic_Summary"]


class PermanentSyncError(RuntimeError):
    """Data/config limits that hourly CI retries cannot fix (e.g. Sheets cell cap)."""


# Daily marketing audit log (framework-first; one row per orders_by_city hub).
AUDIT_OUTPUT_COLUMNS = [
    "Date",
    "Shipping_City",
    "Shipping_State",
    "Shipping_Country",
    "Marketing_Action",
    "Rain_Amount",
    "Yesterday_Rain",
    "Max_UV_Index",
    "Logic_Summary",
    "Quantity_Sold",
    "Order_Revenue",
]
CITY_SALES_OVERLAY_PATH = ROOT / "data" / "city_sales_overlay.csv"
CITY_SALES_OVERLAY_COLUMNS = [
    "Date",
    "Region",
    "City",
    "State",
    "Lat",
    "Lon",
    "Revenue",
    "Units",
    "Weather_Action",
]

ACTION_RANK = {
    "Sun Protection (Hats/Shirts)": 4,
    "Scale Umbrellas (Residual Demand)": 3,
    "Scale Umbrellas": 2,
    "Baseline": 1,
}

US_STATE_ABBR_TO_NAME: dict[str, str] = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire",
    "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York", "NC": "North Carolina",
    "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania",
    "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee",
    "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}
US_STATE_NAME_UPPER = {name.upper(): name for name in US_STATE_ABBR_TO_NAME.values()}

SALES_QUERY = """
SELECT
  event_date AS date,
  'blocked' AS shipping_city,
  shipping_state_code AS shipping_state,
  shipping_country_code AS shipping_country,
  lower(platform) AS platform,
  source_name AS source_name,
  product.product_sku AS sku,
  sum(product.product_name_quantity_sold) AS quantity_sold,
  sum(
    (product.product_name_price * product.product_name_quantity_sold)
    - ifNull(product.discount_amount_for_product, 0)
  ) AS order_revenue
FROM orders_table
ARRAY JOIN products_info AS product
WHERE event_date BETWEEN @startDate AND @endDate
  AND length(product.product_sku) > 0
GROUP BY
  event_date,
  shipping_city,
  shipping_state_code,
  shipping_country_code,
  platform,
  source_name,
  product.product_sku
"""


def _load_dotenv() -> None:
    """Load .env into os.environ (supports multiline quoted values)."""
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return
    raw = env_path.read_text(encoding="utf-8")
    i = 0
    n = len(raw)
    while i < n:
        while i < n and raw[i] in " \t\r\n":
            i += 1
        if i >= n:
            break
        if raw[i] == "#":
            while i < n and raw[i] != "\n":
                i += 1
            continue
        key_start = i
        while i < n and raw[i] not in "=\n":
            i += 1
        key = raw[key_start:i].strip()
        if i >= n or raw[i] != "=":
            continue
        i += 1
        while i < n and raw[i] in " \t":
            i += 1
        if i >= n:
            break
        if raw[i] in "'\"":
            quote = raw[i]
            i += 1
            chars: list[str] = []
            while i < n:
                ch = raw[i]
                if ch == quote:
                    i += 1
                    break
                if ch == "\\" and i + 1 < n:
                    chars.append(raw[i + 1])
                    i += 2
                    continue
                chars.append(ch)
                i += 1
            value = "".join(chars)
        else:
            val_start = i
            while i < n and raw[i] not in "\n":
                i += 1
            value = raw[val_start:i].strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value
        while i < n and raw[i] != "\n":
            i += 1
        if i < n:
            i += 1


def _looks_like_truncated_json(value: str) -> bool:
    text = value.strip()
    if not text:
        return False
    if text in ("{", "'{", '"{'):
        return True
    if text.startswith("{") and "client_email" not in text:
        return len(text) < 80
    return False


def _sanitize_malformed_google_creds_env() -> None:
    """Drop broken GOOGLE_CREDS_JSON so GOOGLE_CREDS_FILE / auto-discovery can proceed."""
    if "GOOGLE_CREDS_JSON" not in os.environ:
        return
    raw = os.environ["GOOGLE_CREDS_JSON"].strip()
    if not raw:
        return
    if _looks_like_truncated_json(raw):
        print(
            "[WARN] Dropping malformed GOOGLE_CREDS_JSON from environment "
            "(truncated); using GOOGLE_CREDS_FILE / auto-discovery.",
            file=sys.stderr,
        )
        del os.environ["GOOGLE_CREDS_JSON"]
        return
    try:
        info = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(
            f"[WARN] Dropping invalid GOOGLE_CREDS_JSON from environment ({exc}); "
            "using GOOGLE_CREDS_FILE / auto-discovery.",
            file=sys.stderr,
        )
        del os.environ["GOOGLE_CREDS_JSON"]
        return
    if not _is_service_account_payload(info):
        print(
            "[WARN] Dropping GOOGLE_CREDS_JSON from environment (not a service account); "
            "using GOOGLE_CREDS_FILE / auto-discovery.",
            file=sys.stderr,
        )
        del os.environ["GOOGLE_CREDS_JSON"]


def _init_pipeline() -> None:
    """Load config, sanitize shell cred overrides, confirm autonomous targeting is active."""
    _load_dotenv()
    _sanitize_malformed_google_creds_env()
    print(
        "READY FOR RUN: Title-first lookup is active. Shell credential override check is enabled."
    )


def _load_service_account_dict_from_path(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[WARN] Could not read credentials file {path}: {exc}", file=sys.stderr)
        return None
    if not _is_service_account_payload(data):
        print(f"[WARN] {path} is not a Google service-account JSON.", file=sys.stderr)
        return None
    return data


def _load_service_account_from_env() -> dict | None:
    """Resolve service-account JSON from env vars (inline JSON or file path)."""
    _load_dotenv()

    creds_json = os.getenv("GOOGLE_CREDS_JSON", "").strip()
    if creds_json:
        if _looks_like_truncated_json(creds_json):
            print(
                "[WARN] GOOGLE_CREDS_JSON looks truncated (common with single-line .env); "
                "use GOOGLE_CREDS_FILE or place google_creds.json in the project root.",
                file=sys.stderr,
            )
        elif creds_json.endswith(".json") or Path(creds_json).suffix == ".json":
            from_path = _load_service_account_dict_from_path(Path(creds_json).expanduser())
            if from_path is not None:
                return from_path
        else:
            try:
                info = json.loads(creds_json)
                if _is_service_account_payload(info):
                    return info
                print(
                    "[WARN] GOOGLE_CREDS_JSON is valid JSON but not a service account; skipping.",
                    file=sys.stderr,
                )
            except json.JSONDecodeError as exc:
                print(
                    f"[WARN] GOOGLE_CREDS_JSON is invalid JSON ({exc}); "
                    "falling back to GOOGLE_CREDS_FILE / auto-discovery.",
                    file=sys.stderr,
                )

    for env_name in ("GOOGLE_CREDS_FILE", "GOOGLE_SERVICE_ACCOUNT_FILE"):
        env_path = os.getenv(env_name, "").strip()
        if not env_path:
            continue
        from_path = _load_service_account_dict_from_path(Path(env_path).expanduser())
        if from_path is not None:
            return from_path

    return None


def _require_env(name: str) -> str:
    _load_dotenv()
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"Missing required environment variable {name!r}.")
    return v


def _normalize_country(code: object) -> str:
    text = str(code or "").strip().upper()
    if not text or text.lower() == "nan":
        return ""
    if text in {"GB", "UK"}:
        return "UK"
    if text in {"CA", "CAN"}:
        return "CA"
    return text


def _is_blocked_location(value: object) -> bool:
    text = str(value or "").strip().casefold()
    return not text or text in {"blocked", "nan", "none"}


def _normalize_city_key(value: object) -> str:
    text = str(value or "").strip()
    if _is_blocked_location(text):
        return ""
    return text.casefold()


def _normalize_state_key(value: object, country: str = "") -> str:
    text = str(value or "").strip()
    if _is_blocked_location(text):
        return ""
    normalized = _normalize_region_state(text, country)
    return normalized.casefold() if normalized else ""


def _display_city_name(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value or "").strip())
    if not text:
        return ""
    return text.title()


def _resolve_orders_state_to_region(state_raw: object) -> tuple[str, str]:
    """Map orders_by_city state code/name to (Shipping_Country, Shipping_State)."""
    code = str(state_raw or "").strip().upper()
    if not code or code in {"UNKNOWN", "NAN"}:
        return "", ""
    if code in US_STATE_ABBR_TO_NAME:
        return "US", US_STATE_ABBR_TO_NAME[code]
    if code in US_STATE_NAME_UPPER:
        return "US", US_STATE_NAME_UPPER[code]
    if code in UK_CODE_TO_REGION:
        return "UK", UK_CODE_TO_REGION[code]
    if code in CAN_ABBREV_TO_REGION:
        return "CA", CAN_ABBREV_TO_REGION[code]
    if code in CAN_REGION_NAME_UPPER:
        return "CA", CAN_REGION_NAME_UPPER[code]
    return "", ""


def _load_marketing_city_hubs() -> dict[str, set[tuple[str, str]]]:
    """City/state join keys from regional marketing CSVs (weather-aligned hubs)."""
    hubs: dict[str, set[tuple[str, str]]] = {"US": set(), "UK": set(), "CA": set()}
    for country, path in (
        ("US", MARKETING_US_PATH),
        ("UK", MARKETING_UK_PATH),
        ("CA", MARKETING_CAN_PATH),
    ):
        if not path.is_file():
            continue
        df = pd.read_csv(path, dtype=str, usecols=["City", "State"])
        for city, state in zip(df["City"], df["State"], strict=False):
            city_key = _normalize_city_key(city)
            state_key = _normalize_state_key(state, country)
            if city_key and state_key:
                hubs[country].add((city_key, state_key))
    return hubs


def load_city_distribution_weights(
    orders_path: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build state- and country-level city weight tables from orders_by_city.csv.

    Weights use total_orders and total_order_revenue shares, restricted to
    marketing weather hub cities so live splits align with Date + City + State joins.

    Falls back to committed CI index files when orders_by_city.csv is absent (GitHub Actions).
    """
    path = orders_path or ORDERS_BY_CITY_PATH
    if not path.is_file():
        if (
            CI_STATE_WEIGHTS_PATH.is_file()
            and CI_COUNTRY_WEIGHTS_PATH.is_file()
        ):
            state_weights = pd.read_csv(CI_STATE_WEIGHTS_PATH, dtype=str)
            country_weights = pd.read_csv(CI_COUNTRY_WEIGHTS_PATH, dtype=str)
            for frame in (state_weights, country_weights):
                frame["order_weight"] = pd.to_numeric(frame["order_weight"], errors="coerce").fillna(0.0)
                frame["revenue_weight"] = pd.to_numeric(frame["revenue_weight"], errors="coerce").fillna(0.0)
            print(
                f"[INFO] Geographic weight index: {len(state_weights):,} state-level rows, "
                f"{len(country_weights):,} country-level rows (from committed CI index)."
            )
            return state_weights, country_weights
        raise FileNotFoundError(
            f"Geographic weight index not found: {path}. "
            f"Regenerate {CI_STATE_WEIGHTS_PATH.name} via scripts/export_ci_city_index.py "
            "or place orders_by_city.csv locally."
        )

    raw = pd.read_csv(path)
    raw["city_display"] = raw["city"].map(_display_city_name)
    raw["total_orders"] = pd.to_numeric(raw["total_orders"], errors="coerce").fillna(0.0)
    raw["total_order_revenue"] = pd.to_numeric(raw["total_order_revenue"], errors="coerce").fillna(0.0)

    regions = raw["state"].map(_resolve_orders_state_to_region)
    raw["Shipping_Country"] = [r[0] for r in regions]
    raw["Shipping_State"] = [r[1] for r in regions]
    raw = raw[(raw["Shipping_Country"] != "") & (raw["total_orders"] > 0)].copy()

    marketing_hubs = _load_marketing_city_hubs()
    raw["City_Join_Key"] = raw["city_display"].map(_normalize_city_key)
    raw["State_Join_Key"] = [
        _normalize_state_key(st, country)
        for st, country in zip(raw["Shipping_State"], raw["Shipping_Country"], strict=False)
    ]
    raw["In_Marketing_Hub"] = [
        (city_key, state_key) in marketing_hubs.get(country, set())
        for city_key, state_key, country in zip(
            raw["City_Join_Key"], raw["State_Join_Key"], raw["Shipping_Country"], strict=False
        )
    ]
    raw = raw[raw["In_Marketing_Hub"]].copy()
    if raw.empty:
        raise RuntimeError(
            "No overlapping cities between orders_by_city.csv and marketing weather CSVs."
        )

    state_groups = raw.groupby(["Shipping_Country", "Shipping_State"], as_index=False).agg(
        state_orders=("total_orders", "sum"),
        state_revenue=("total_order_revenue", "sum"),
    )
    state_weights = raw.merge(state_groups, on=["Shipping_Country", "Shipping_State"], how="left")
    state_weights["order_weight"] = np.where(
        state_weights["state_orders"] > 0,
        state_weights["total_orders"] / state_weights["state_orders"],
        0.0,
    )
    state_weights["revenue_weight"] = np.where(
        state_weights["state_revenue"] > 0,
        state_weights["total_order_revenue"] / state_weights["state_revenue"],
        0.0,
    )
    state_weights = state_weights.rename(columns={"city_display": "Shipping_City"})[
        [
            "Shipping_Country",
            "Shipping_State",
            "Shipping_City",
            "City_Join_Key",
            "State_Join_Key",
            "order_weight",
            "revenue_weight",
        ]
    ]

    country_groups = raw.groupby("Shipping_Country", as_index=False).agg(
        country_orders=("total_orders", "sum"),
        country_revenue=("total_order_revenue", "sum"),
    )
    country_weights = raw.merge(country_groups, on="Shipping_Country", how="left")
    country_weights["order_weight"] = np.where(
        country_weights["country_orders"] > 0,
        country_weights["total_orders"] / country_weights["country_orders"],
        0.0,
    )
    country_weights["revenue_weight"] = np.where(
        country_weights["country_revenue"] > 0,
        country_weights["total_order_revenue"] / country_weights["country_revenue"],
        0.0,
    )
    country_weights = country_weights.rename(columns={"city_display": "Shipping_City"})[
        [
            "Shipping_Country",
            "Shipping_State",
            "Shipping_City",
            "City_Join_Key",
            "State_Join_Key",
            "order_weight",
            "revenue_weight",
        ]
    ]

    print(
        f"[INFO] Geographic weight index: {len(state_weights):,} city/state pairs across "
        f"{state_weights['Shipping_State'].nunique():,} states "
        f"({len(country_weights):,} country-level hub rows)."
    )
    return state_weights, country_weights


def _allocate_proportional(total: float, weights: np.ndarray, *, as_int: bool = False) -> np.ndarray:
    if total == 0 or len(weights) == 0:
        return np.zeros(len(weights), dtype=int if as_int else float)
    w = np.asarray(weights, dtype=float)
    if w.sum() <= 0:
        w = np.ones(len(w), dtype=float)
    shares = total * w / w.sum()
    if not as_int:
        rounded = np.round(shares, 2)
        drift = round(float(total) - float(rounded.sum()), 2)
        if drift and len(rounded):
            rounded[int(np.argmax(shares))] = round(float(rounded[int(np.argmax(shares))]) + drift, 2)
        return rounded
    target = int(round(total))
    floors = np.floor(shares).astype(int)
    remainder = target - int(floors.sum())
    if remainder > 0:
        for idx in np.argsort(-(shares - floors))[:remainder]:
            floors[idx] += 1
    return floors


def _select_distribution_weights(
    country: str,
    state: str,
    *,
    state_weights: pd.DataFrame,
    country_weights: pd.DataFrame,
) -> pd.DataFrame:
    country = _normalize_country(country)
    state_norm = _normalize_region_state(state, country) if state else ""
    if state_norm:
        subset = state_weights[
            (state_weights["Shipping_Country"] == country)
            & (state_weights["Shipping_State"] == state_norm)
        ]
        if not subset.empty:
            return subset
    return country_weights[country_weights["Shipping_Country"] == country].copy()


def distribute_blocked_live_rows(
    live: pd.DataFrame,
    *,
    state_weights: pd.DataFrame,
    country_weights: pd.DataFrame,
) -> pd.DataFrame:
    """Expand masked live rows into city-level rows using orders_by_city weights."""
    if live.empty:
        return live

    known = live[~live["Shipping_City"].map(_is_blocked_location)].copy()
    blocked = live[live["Shipping_City"].map(_is_blocked_location)].copy()
    if blocked.empty:
        return live

    expanded: list[dict[str, object]] = []
    for row in blocked.itertuples(index=False):
        weights = _select_distribution_weights(
            row.Shipping_Country,
            row.Shipping_State,
            state_weights=state_weights,
            country_weights=country_weights,
        )
        if weights.empty:
            expanded.append(
                {
                    "Date": row.Date,
                    "Shipping_City": row.Shipping_City,
                    "Shipping_State": row.Shipping_State,
                    "Shipping_Country": row.Shipping_Country,
                    "Platform": row.Platform,
                    "Source_Name": row.Source_Name,
                    "SKU": row.SKU,
                    "Quantity_Sold": int(row.Quantity_Sold),
                    "Order_Revenue": float(row.Order_Revenue),
                }
            )
            continue

        qty_parts = _allocate_proportional(
            int(row.Quantity_Sold), weights["order_weight"].to_numpy(), as_int=True
        )
        rev_parts = _allocate_proportional(
            float(row.Order_Revenue), weights["revenue_weight"].to_numpy(), as_int=False
        )
        for w_row, qty, rev in zip(weights.itertuples(index=False), qty_parts, rev_parts, strict=True):
            if qty == 0 and rev == 0:
                continue
            expanded.append(
                {
                    "Date": row.Date,
                    "Shipping_City": w_row.Shipping_City,
                    "Shipping_State": w_row.Shipping_State,
                    "Shipping_Country": row.Shipping_Country,
                    "Platform": row.Platform,
                    "Source_Name": row.Source_Name,
                    "SKU": row.SKU,
                    "Quantity_Sold": int(qty),
                    "Order_Revenue": float(rev),
                }
            )

    if not expanded:
        return known if not known.empty else blocked

    expanded_df = pd.DataFrame(expanded, columns=SALES_COLUMNS)
    combined = pd.concat([known, expanded_df], ignore_index=True)
    combined = (
        combined.groupby(SALES_COLUMNS[:-2], as_index=False, dropna=False)
        .agg({"Quantity_Sold": "sum", "Order_Revenue": "sum"})
        .sort_values(["Date", "Shipping_Country", "Shipping_City", "Platform", "Source_Name", "SKU"])
        .reset_index(drop=True)
    )
    combined["Quantity_Sold"] = combined["Quantity_Sold"].round().astype(int)
    combined["Order_Revenue"] = combined["Order_Revenue"].round(2)

    print(
        f"[INFO] Distributed {len(blocked):,} blocked live rows into "
        f"{len(expanded_df):,} city-level rows ({len(combined):,} after re-aggregation)."
    )
    return combined


def _discover_historical_ledger_path(explicit: Path | None = None) -> Path | None:
    """Find the immutable historical sales ledger (real city/state rows)."""
    candidates: list[Path] = []

    if explicit is not None:
        candidates.append(explicit)

    for env_name in ("HISTORICAL_SALES_LEDGER", "HISTORICAL_SALES_LEDGER_PATH"):
        env_path = os.getenv(env_name, "").strip()
        if env_path:
            candidates.append(Path(env_path))

    for directory in HISTORICAL_SEARCH_DIRS:
        if not directory.is_dir():
            continue
        for name in HISTORICAL_LEDGER_FILENAMES:
            candidates.append(directory / name)

    seen: set[Path] = set()
    for path in candidates:
        resolved = path.expanduser().resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved.is_file():
            return resolved
    return None


def _discover_regional_historical_paths() -> list[Path]:
    """Optional per-region ledger files (US / UK / CA)."""
    found: list[Path] = []
    seen: set[Path] = set()
    for directory in HISTORICAL_SEARCH_DIRS:
        if not directory.is_dir():
            continue
        for name in HISTORICAL_REGION_FILENAMES:
            path = (directory / name).expanduser().resolve()
            if path in seen:
                continue
            seen.add(path)
            if path.is_file():
                found.append(path)
    return found


def _normalize_historical_sales_frame(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalize a loaded ledger or bridge export to SALES_COLUMNS."""
    if raw.empty:
        return pd.DataFrame(columns=SALES_COLUMNS)

    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]

    rename_map = {
        "Units_Sold": "Quantity_Sold",
        "units_sold": "Quantity_Sold",
        "quantity_sold": "Quantity_Sold",
        "shipping_city": "Shipping_City",
        "shipping_state": "Shipping_State",
        "shipping_country": "Shipping_Country",
        "Shipping Country": "Shipping_Country",
        "Shipping City": "Shipping_City",
        "Shipping State": "Shipping_State",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    missing = [col for col in SALES_COLUMNS if col not in df.columns]
    if missing:
        raise RuntimeError(
            f"Historical sales ledger missing required columns {missing}. "
            f"Expected {SALES_COLUMNS}."
        )

    df = df[SALES_COLUMNS].copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["Shipping_Country"] = df["Shipping_Country"].map(_normalize_country)
    df["Shipping_State"] = [
        _normalize_region_state(st, country)
        for st, country in zip(df["Shipping_State"], df["Shipping_Country"], strict=False)
    ]
    df["Shipping_City"] = df["Shipping_City"].fillna("").astype(str).str.strip()
    df["Platform"] = df["Platform"].fillna("").astype(str).str.lower().str.strip()
    df["Source_Name"] = df["Source_Name"].fillna("").astype(str).str.strip()
    df["SKU"] = df["SKU"].fillna("").astype(str).str.strip()
    df["Quantity_Sold"] = pd.to_numeric(df["Quantity_Sold"], errors="coerce").fillna(0)
    df["Order_Revenue"] = pd.to_numeric(df["Order_Revenue"], errors="coerce").fillna(0.0)

    df = (
        df.loc[df["Date"].notna() & (df["SKU"] != "")]
        .groupby(SALES_COLUMNS[:-2], as_index=False, dropna=False)
        .agg({"Quantity_Sold": "sum", "Order_Revenue": "sum"})
        .sort_values(["Date", "Shipping_Country", "Shipping_City", "Platform", "Source_Name", "SKU"])
        .reset_index(drop=True)
    )
    df["Quantity_Sold"] = df["Quantity_Sold"].round().astype(int)
    df["Order_Revenue"] = df["Order_Revenue"].round(2)
    return df


def _normalize_omnichannel_historical_frame(raw: pd.DataFrame) -> pd.DataFrame:
    """Adapt data/cleaned_omnichannel_sales.csv to SALES_COLUMNS (country-level, blocked city)."""
    if raw.empty:
        return pd.DataFrame(columns=SALES_COLUMNS)

    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]
    df = df.rename(
        columns={
            "Units_Sold": "Quantity_Sold",
            "units_sold": "Quantity_Sold",
            "shipping_country": "Shipping_Country",
            "Shipping Country": "Shipping_Country",
        }
    )
    df["Shipping_City"] = "blocked"
    df["Shipping_State"] = ""
    return _normalize_historical_sales_frame(df)


def load_historical_sales(explicit_path: Path | None = None) -> pd.DataFrame:
    """
    Load the immutable historical city ledger from local CSV.

    Falls back to regional ledger files (historical_sales_ledger_{us,uk,ca}.csv)
    when a unified ledger is not present.
    """
    ledger_path = _discover_historical_ledger_path(explicit_path)
    frames: list[pd.DataFrame] = []

    if ledger_path is not None:
        print(f"[INFO] Loading historical sales ledger from {ledger_path}")
        frames.append(_normalize_historical_sales_frame(pd.read_csv(ledger_path, dtype=str)))
    else:
        regional_paths = _discover_regional_historical_paths()
        if regional_paths:
            for path in regional_paths:
                print(f"[INFO] Loading regional historical ledger from {path}")
                frames.append(_normalize_historical_sales_frame(pd.read_csv(path, dtype=str)))
        else:
            print(
                "[WARN] No local historical sales ledger found. "
                "Using committed archives under data/history/ and data/cleaned_omnichannel_sales.csv "
                "when present."
            )

    try:
        from history_archive import historical_source_paths

        for path in historical_source_paths():
            if ledger_path is not None and path.resolve() == ledger_path.resolve():
                continue
            if not path.is_file():
                continue
            if path.name == "cleaned_omnichannel_sales.csv":
                print(f"[INFO] Loading omnichannel historical sales from {path}")
                frames.append(
                    _normalize_omnichannel_historical_frame(pd.read_csv(path, dtype=str))
                )
            else:
                print(f"[INFO] Loading archived sales ledger from {path}")
                frames.append(_normalize_historical_sales_frame(pd.read_csv(path, dtype=str)))
    except ImportError:
        pass

    if not frames:
        return pd.DataFrame(columns=SALES_COLUMNS)

    combined = pd.concat(frames, ignore_index=True)
    combined = _normalize_historical_sales_frame(combined)
    real_city_rows = int((~combined["Shipping_City"].map(_is_blocked_location)).sum())
    print(
        f"[INFO] Historical ledger loaded: {len(combined):,} rows "
        f"({real_city_rows:,} with real Shipping_City)."
    )
    return combined


def _resolve_live_window(end_date: date) -> tuple[date, date]:
    """Return (live_start, live_end) for the Triple Whale stream."""
    _load_dotenv()
    live_end = end_date

    override_start = os.getenv("LIVE_SALES_START_DATE", "").strip()
    if override_start:
        live_start = datetime.strptime(override_start, "%Y-%m-%d").date()
    else:
        lookback_raw = os.getenv("LIVE_LOOKBACK_DAYS", "").strip()
        lookback = int(lookback_raw) if lookback_raw else DEFAULT_LIVE_LOOKBACK_DAYS
        if lookback < 1:
            raise ValueError("LIVE_LOOKBACK_DAYS must be >= 1.")
        live_start = live_end - timedelta(days=lookback - 1)

    if live_start < START_DATE:
        live_start = START_DATE
    if live_start > live_end:
        live_start = live_end

    print(
        f"[INFO] Live Triple Whale window: {live_start.isoformat()} → {live_end.isoformat()} "
        f"({(live_end - live_start).days + 1} day(s))."
    )
    return live_start, live_end


def combine_sales_streams(
    historical: pd.DataFrame,
    live: pd.DataFrame,
    live_start: date,
) -> pd.DataFrame:
    """Append historical ledger (pre-live) with the recent Triple Whale stream."""
    live_start_str = live_start.strftime("%Y-%m-%d")
    hist = historical[historical["Date"] < live_start_str].copy() if not historical.empty else pd.DataFrame(columns=SALES_COLUMNS)
    live_part = live.copy() if not live.empty else pd.DataFrame(columns=SALES_COLUMNS)

    if hist.empty and live_part.empty:
        return pd.DataFrame(columns=SALES_COLUMNS)

    combined = pd.concat([hist, live_part], ignore_index=True)
    combined = (
        combined.groupby(SALES_COLUMNS[:-2], as_index=False, dropna=False)
        .agg({"Quantity_Sold": "sum", "Order_Revenue": "sum"})
        .sort_values(["Date", "Shipping_Country", "Shipping_City", "Platform", "Source_Name", "SKU"])
        .reset_index(drop=True)
    )
    combined["Quantity_Sold"] = combined["Quantity_Sold"].round().astype(int)
    combined["Order_Revenue"] = combined["Order_Revenue"].round(2)

    print(
        f"[INFO] Combined sales: {len(combined):,} rows "
        f"({len(hist):,} historical + {len(live_part):,} live; "
        f"live replaces dates >= {live_start_str})."
    )
    return combined


def load_unified_sales(
    api_key: str,
    end_date: date,
    *,
    historical_path: Path | None = None,
    distribute_blocked: bool = True,
) -> pd.DataFrame:
    """Load historical ledger + live Triple Whale stream into one sales frame."""
    live_start, live_end = _resolve_live_window(end_date)
    try:
        from history_archive import ensure_sales_history_seeded, persist_live_sales_archive

        ensure_sales_history_seeded(live_start)
    except ImportError:
        pass

    historical = load_historical_sales(historical_path)
    live = fetch_live_sales_aggregates(api_key, live_start, live_end)
    if distribute_blocked:
        state_weights, country_weights = load_city_distribution_weights()
        live = distribute_blocked_live_rows(
            live, state_weights=state_weights, country_weights=country_weights
        )
    combined = combine_sales_streams(historical, live, live_start)
    try:
        from history_archive import persist_live_sales_archive

        persist_live_sales_archive(live, live_start=live_start)
    except ImportError:
        pass
    return combined


def load_master_sales_bridge(
    api_key: str,
    end_date: date,
    *,
    historical_path: Path | None = None,
) -> pd.DataFrame:
    """
    Transactional SKU bridge stream: live Triple Whale rows with blocked cities kept intact.
    Weather joins use state/country rollups — never drop or expand blocked ship-to rows.
    """
    return load_unified_sales(
        api_key, end_date, historical_path=historical_path, distribute_blocked=False
    )


def _normalize_state(value: object, country: str = "") -> str:
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none", "blocked"}:
        return ""
    upper = text.upper()
    if len(upper) == 2 and upper in US_STATE_ABBR_TO_NAME:
        return US_STATE_ABBR_TO_NAME[upper]
    if country == "US" and upper in US_STATE_NAME_UPPER:
        return US_STATE_NAME_UPPER[upper]
    return text


def _clean_frame_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Replace NaN/None and literal #N/A with empty strings for Sheets/Looker safety."""
    out = df.copy()
    out = out.replace({pd.NA: "", None: ""})
    out = out.fillna("")
    for col in out.columns:
        if out[col].dtype == object:
            out[col] = (
                out[col]
                .astype(str)
                .str.replace(r"^\s*#N/A\s*$", "", regex=True)
                .str.replace(r"^\s*nan\s*$", "", regex=True, case=False)
            )
    return out


def _extract_sql_rows(body: object) -> list[dict]:
    if isinstance(body, list):
        return body
    if not isinstance(body, dict):
        raise RuntimeError(f"Unexpected Triple Whale response type: {type(body)!r}")
    if body.get("success") is False:
        raise RuntimeError(f"Triple Whale SQL query failed: {body.get('message', body)}")

    data = body.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("rows", "results", "data"):
            nested = data.get(key)
            if isinstance(nested, list):
                return nested
    raise RuntimeError(f"Unexpected Triple Whale response shape: {body!r}")


def _month_chunks(start: date, end: date) -> list[tuple[date, date]]:
    chunks: list[tuple[date, date]] = []
    cursor = start.replace(day=1)
    while cursor <= end:
        chunk_start = max(start, cursor)
        next_month = cursor + relativedelta(months=1)
        chunk_end = min(end, next_month - timedelta(days=1))
        chunks.append((chunk_start, chunk_end))
        cursor = next_month
    return chunks


def _post_sql(
    session: requests.Session,
    api_key: str,
    query: str,
    start: date,
    end: date,
    *,
    max_retries: int = 5,
) -> list[dict]:
    payload = {
        "shopId": SHOP_ID,
        "query": query,
        "currency": "USD",
        "period": {"startDate": start.isoformat(), "endDate": end.isoformat()},
    }
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    for attempt in range(max_retries):
        resp = session.post(SQL_ENDPOINT, json=payload, headers=headers, timeout=120)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", min(32, 2**attempt)))
            print(f"[WARN] Rate limited; retrying in {retry_after}s...")
            time.sleep(retry_after)
            continue
        if resp.status_code >= 500:
            delay = min(32, 2**attempt)
            print(f"[WARN] Server error {resp.status_code}; retrying in {delay}s...")
            time.sleep(delay)
            continue

        try:
            body = resp.json()
        except ValueError as exc:
            raise RuntimeError(f"Triple Whale returned non-JSON response: {resp.text[:500]}") from exc

        if not resp.ok:
            message = body.get("message") if isinstance(body, dict) else resp.text
            raise RuntimeError(f"Triple Whale SQL API error ({resp.status_code}): {message}")

        return _extract_sql_rows(body)

    raise RuntimeError(f"Triple Whale SQL API failed after {max_retries} attempts.")


def fetch_live_sales_aggregates(api_key: str, start: date, end: date) -> pd.DataFrame:
    """Pull the masked live stream from Triple Whale (recent window only)."""
    session = requests.Session()
    frames: list[pd.DataFrame] = []

    for chunk_start, chunk_end in _month_chunks(start, end):
        print(f"[INFO] Live fetch {chunk_start.isoformat()} → {chunk_end.isoformat()}...")
        rows = _post_sql(session, api_key, SALES_QUERY, chunk_start, chunk_end)
        if rows:
            frames.append(pd.DataFrame(rows))
            print(f"[INFO]   {len(rows):,} aggregated rows")
        else:
            print("[INFO]   0 rows")

    if not frames:
        print("[WARN] Live Triple Whale stream returned 0 rows.")
        return pd.DataFrame(columns=SALES_COLUMNS)

    raw = pd.concat(frames, ignore_index=True)
    raw.columns = [str(c).lower() for c in raw.columns]

    df = pd.DataFrame()
    df["Date"] = pd.to_datetime(raw["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["Shipping_City"] = raw.get("shipping_city", "blocked").fillna("blocked").astype(str).str.strip()
    df["Shipping_City"] = df["Shipping_City"].replace("", "blocked")
    df["Shipping_Country"] = raw.get("shipping_country", "").map(_normalize_country)
    df["Shipping_State"] = [
        _normalize_region_state(st, country)
        for st, country in zip(
            raw.get("shipping_state", ""),
            df["Shipping_Country"],
            strict=False,
        )
    ]
    df["Platform"] = raw.get("platform", "").fillna("").astype(str).str.lower().str.strip()
    df["Source_Name"] = raw.get("source_name", "").fillna("").astype(str).str.strip()
    df["SKU"] = raw.get("sku", "").fillna("").astype(str).str.strip()
    df["Quantity_Sold"] = pd.to_numeric(raw.get("quantity_sold", 0), errors="coerce").fillna(0)
    df["Order_Revenue"] = pd.to_numeric(raw.get("order_revenue", 0.0), errors="coerce").fillna(0.0)

    df = (
        df[SALES_COLUMNS]
        .loc[lambda d: d["Date"].notna() & (d["SKU"] != "")]
        .groupby(SALES_COLUMNS[:-2], as_index=False, dropna=False)
        .agg({"Quantity_Sold": "sum", "Order_Revenue": "sum"})
        .sort_values(["Date", "Shipping_Country", "Platform", "Source_Name", "SKU"])
        .reset_index(drop=True)
    )

    df["Quantity_Sold"] = df["Quantity_Sold"].round().astype(int)
    df["Order_Revenue"] = df["Order_Revenue"].round(2)
    print(f"[INFO] Live Triple Whale stream: {len(df):,} rows.")
    return df


def fetch_sales_aggregates(api_key: str, start: date, end: date) -> pd.DataFrame:
    """Deprecated full-history fetch — use load_unified_sales() instead."""
    return fetch_live_sales_aggregates(api_key, start, end)


def _read_marketing_csv(path: Path, country: str) -> pd.DataFrame:
    if not path.is_file():
        print(f"[WARN] Marketing file missing: {path.name}")
        return pd.DataFrame()

    df = pd.read_csv(path, dtype=str)
    df = _clean_frame_strings(df)
    if df.empty or "State" not in df.columns:
        print(f"[WARN] Marketing file empty or invalid: {path.name}")
        return pd.DataFrame()

    required = {"City", "State", "Marketing_Action"}
    if not required.issubset(df.columns):
        print(f"[WARN] Marketing file missing columns in {path.name}: {required - set(df.columns)}")
        return pd.DataFrame()

    df["Marketing_Country"] = country
    for col in ("Rain_Amount", "Yesterday_Rain", "Max_UV_Index"):
        if col not in df.columns:
            df[col] = "0"
    if "Logic_Summary" not in df.columns:
        df["Logic_Summary"] = ""

    numeric_cols = ["Rain_Amount", "Yesterday_Rain", "Max_UV_Index"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["State"] = df["State"].astype(str).str.strip()
    df["Marketing_Action"] = df["Marketing_Action"].astype(str).str.strip()
    df["_rank"] = df["Marketing_Action"].map(ACTION_RANK).fillna(0)
    return df


def _normalize_region_state(value: object, country: str) -> str:
    """Normalize state/province for join keys (never use blocked city)."""
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none", "blocked"}:
        return ""
    if country == "US":
        return _normalize_state(text, "US")
    return text


def _resolve_snapshot_date(end_date: date, marketing_path: Path | None = None) -> date:
    _load_dotenv()
    override = os.getenv("MARKETING_SNAPSHOT_DATE", "").strip()
    if override:
        return datetime.strptime(override, "%Y-%m-%d").date()

    candidates = [marketing_path] if marketing_path else []
    if not candidates:
        candidates = [p for p in (MARKETING_US_PATH, MARKETING_UK_PATH, MARKETING_CAN_PATH) if p.is_file()]
    candidates = [p for p in candidates if p and p.is_file()]
    if candidates:
        latest_mtime = max(p.stat().st_mtime for p in candidates)
        file_date = datetime.fromtimestamp(latest_mtime).date()
        if file_date < end_date:
            print(
                f"[WARN] Marketing CSVs last updated {file_date}; "
                f"using snapshot date {file_date} instead of {end_date}."
            )
            return file_date
    return end_date


def load_region_marketing(marketing_path: Path, sales_country: str, snapshot_date: date) -> pd.DataFrame:
    df = _read_marketing_csv(marketing_path, sales_country)
    snap = snapshot_date.strftime("%Y-%m-%d")
    if df.empty:
        print(f"[WARN] No marketing data in {marketing_path.name}; Baseline fallback for {sales_country}.")
        return pd.DataFrame(
            [
                {
                    "Report_Date": snap,
                    "State": "",
                    "Marketing_Action": "Baseline",
                    "Rain_Amount": 0.0,
                    "Yesterday_Rain": 0.0,
                    "Max_UV_Index": 0.0,
                    "Logic_Summary": "Marketing snapshot unavailable; Baseline fallback applied.",
                    "Marketing_Country": sales_country,
                    "_rank": 1,
                }
            ]
        )
    df["Report_Date"] = snap
    return df


def load_city_framework_matrix(
    country: str,
    orders_path: Path | None = None,
) -> pd.DataFrame:
    """Pure city/state matrix from orders_by_city.csv (one row per ship-to hub)."""
    path = orders_path or ORDERS_BY_CITY_PATH
    if not path.is_file():
        if CI_FRAMEWORK_PATH.is_file():
            framework = pd.read_csv(CI_FRAMEWORK_PATH, dtype=str)
            framework = framework[framework["Shipping_Country"] == country].copy()
            if framework.empty:
                raise RuntimeError(
                    f"No city framework rows for {country} in {CI_FRAMEWORK_PATH.name}."
                )
            print(
                f"[INFO] City framework for {country}: {len(framework):,} rows "
                f"from {CI_FRAMEWORK_PATH.name} (committed CI index)."
            )
            return framework.reset_index(drop=True)

        raise FileNotFoundError(
            f"City framework index not found: {path}. "
            f"Regenerate {CI_FRAMEWORK_PATH.name} via scripts/export_ci_city_index.py "
            "or place orders_by_city.csv locally."
        )

    raw = pd.read_csv(path)
    resolved = raw["state"].map(_resolve_orders_state_to_region)
    raw["Shipping_Country"] = [r[0] for r in resolved]
    raw["Shipping_State"] = [r[1] for r in resolved]
    raw = raw[(raw["Shipping_Country"] == country) & (raw["Shipping_State"] != "")].copy()
    raw["Shipping_City"] = raw["city"].map(_display_city_name)
    raw["City_Join_Key"] = raw["Shipping_City"].map(_normalize_city_key)
    raw["State_Join_Key"] = [
        _normalize_state_key(st, country) for st in raw["Shipping_State"]
    ]
    raw = raw[(raw["City_Join_Key"] != "") & (raw["State_Join_Key"] != "")].copy()

    framework = raw.drop_duplicates(
        subset=["Shipping_Country", "Shipping_State", "City_Join_Key", "State_Join_Key"]
    )[
        ["Shipping_Country", "Shipping_State", "Shipping_City", "City_Join_Key", "State_Join_Key"]
    ].sort_values(["Shipping_State", "Shipping_City"]).reset_index(drop=True)
    print(
        f"[INFO] City framework for {country}: {len(framework):,} rows from {path.name}."
    )
    return framework


def _audit_spreadsheet_title(region_key: str, snapshot_date: date) -> str:
    """e.g. Weather_Pulse_Bridge_USA - 06-01-26"""
    base = str(REGIONS[region_key]["spreadsheet"])
    return f"{base} - {snapshot_date.strftime('%m-%d-%y')}"


def _load_city_coordinate_lookup() -> pd.DataFrame:
    path = ROOT / "data" / "city_coordinates.json"
    if not path.is_file():
        return pd.DataFrame(columns=["Region", "city_key", "Lat", "Lon"])
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("rows", [])
    if not rows:
        return pd.DataFrame(columns=["Region", "city_key", "Lat", "Lon"])
    frame = pd.DataFrame(rows)
    frame["Region"] = frame["region"].astype(str).str.upper()
    frame["State"] = frame["state"].astype(str).str.strip()
    frame["City"] = frame["city"].astype(str).str.strip()
    frame["Lat"] = pd.to_numeric(frame["lat"], errors="coerce")
    frame["Lon"] = pd.to_numeric(frame["lon"], errors="coerce")
    frame["city_key"] = frame["City"].str.casefold() + "|" + frame["State"].str.casefold()
    return frame.dropna(subset=["Lat", "Lon"])[
        ["Region", "city_key", "Lat", "Lon"]
    ].drop_duplicates(subset=["Region", "city_key"])


def _sales_country_to_region(country: object) -> str:
    code = _normalize_country(country)
    return {"US": "US", "UK": "UK", "CA": "CAN"}.get(code, code)


def build_city_sales_overlay(df_sales: pd.DataFrame, *, live_start: date) -> pd.DataFrame:
    """
    City-level sales for the live Triple Whale window (all days, not snapshot-only).

    Uses geographically distributed ship-to rows joined to city_coordinates.json.
    """
    if df_sales.empty:
        return pd.DataFrame(columns=CITY_SALES_OVERLAY_COLUMNS)

    coords = _load_city_coordinate_lookup()
    if coords.empty:
        print("[WARN] city_coordinates.json missing; city sales overlay will be empty.")
        return pd.DataFrame(columns=CITY_SALES_OVERLAY_COLUMNS)

    live_start_str = live_start.strftime("%Y-%m-%d")
    sales = df_sales[df_sales["Date"] >= live_start_str].copy()
    sales["Order_Revenue"] = pd.to_numeric(sales["Order_Revenue"], errors="coerce").fillna(0.0)
    sales["Quantity_Sold"] = pd.to_numeric(sales["Quantity_Sold"], errors="coerce").fillna(0.0)
    sales = sales[(sales["Order_Revenue"] > 0) & (~sales["Shipping_City"].map(_is_blocked_location))].copy()
    if sales.empty:
        return pd.DataFrame(columns=CITY_SALES_OVERLAY_COLUMNS)

    sales["Region"] = sales["Shipping_Country"].map(_sales_country_to_region)
    sales["City"] = sales["Shipping_City"].astype(str).str.strip()
    sales["State"] = sales["Shipping_State"].astype(str).str.strip()
    sales["city_key"] = sales["City"].str.casefold() + "|" + sales["State"].str.casefold()
    merged = sales.merge(coords, on=["Region", "city_key"], how="inner")
    if merged.empty:
        return pd.DataFrame(columns=CITY_SALES_OVERLAY_COLUMNS)

    out = (
        merged.groupby(["Date", "Region", "City", "State", "Lat", "Lon"], as_index=False)
        .agg(Revenue=("Order_Revenue", "sum"), Units=("Quantity_Sold", "sum"))
        .sort_values(["Date", "Region", "Revenue"], ascending=[True, True, False])
    )
    out["Lat"] = out["Lat"].round(3)
    out["Lon"] = out["Lon"].round(3)
    out["Weather_Action"] = ""
    return out[CITY_SALES_OVERLAY_COLUMNS]


def export_city_sales_overlay(df_sales: pd.DataFrame, *, live_start: date) -> Path | None:
    overlay = build_city_sales_overlay(df_sales, live_start=live_start)
    CITY_SALES_OVERLAY_PATH.parent.mkdir(parents=True, exist_ok=True)
    overlay.to_csv(CITY_SALES_OVERLAY_PATH, index=False)
    dates = sorted(overlay["Date"].unique()) if not overlay.empty else []
    print(
        f"[INFO] City sales overlay → {CITY_SALES_OVERLAY_PATH.relative_to(ROOT)} "
        f"({len(overlay):,} rows; dates {dates[0] if dates else 'none'} → {dates[-1] if dates else 'none'})"
    )
    return CITY_SALES_OVERLAY_PATH


def build_marketing_audit_bridge_region(
    region_key: str,
    snapshot_date: date,
    df_sales: pd.DataFrame,
    *,
    sales_countries: frozenset[str],
    marketing_path: Path,
) -> pd.DataFrame:
    """
    Framework-first daily marketing audit log.

    1) Seed every city/state from orders_by_city.csv
    2) LEFT JOIN weather marketing snapshot (City + State)
    3) LEFT JOIN snapshot-day sales totals (never drops framework rows)
    """
    sales_country = next(iter(sales_countries))
    snapshot_date = _resolve_snapshot_date(snapshot_date, marketing_path)
    snap = snapshot_date.strftime("%Y-%m-%d")

    audit = load_city_framework_matrix(sales_country)
    audit["Date"] = snap

    mkt = _read_marketing_csv(marketing_path, sales_country)
    if not mkt.empty:
        mkt = mkt.copy()
        mkt["City_Join_Key"] = mkt["City"].map(_normalize_city_key)
        mkt["State_Join_Key"] = mkt["State"].map(
            lambda s: _normalize_state_key(s, sales_country)
        )
        mkt_lookup = mkt[
            ["City_Join_Key", "State_Join_Key", *MARKETING_COLUMNS]
        ].drop_duplicates(subset=["City_Join_Key", "State_Join_Key"])
        audit = audit.merge(mkt_lookup, on=["City_Join_Key", "State_Join_Key"], how="left")
    else:
        for col in MARKETING_COLUMNS:
            audit[col] = ""

    audit["Marketing_Action"] = audit["Marketing_Action"].fillna("").astype(str).str.strip()
    audit["Logic_Summary"] = audit["Logic_Summary"].fillna("").astype(str)
    for col in ("Rain_Amount", "Yesterday_Rain", "Max_UV_Index"):
        audit[col] = pd.to_numeric(audit[col], errors="coerce").fillna(0.0)

    missing_weather = audit["Marketing_Action"] == ""
    audit.loc[missing_weather, "Marketing_Action"] = "Baseline"
    audit.loc[missing_weather, "Logic_Summary"] = (
        "No weather hub in marketing snapshot; Baseline applied."
    )

    if not df_sales.empty:
        snap_sales = df_sales[
            (df_sales["Shipping_Country"].isin(sales_countries)) & (df_sales["Date"] == snap)
        ].copy()
        if not snap_sales.empty:
            snap_sales = snap_sales[~snap_sales["Shipping_City"].map(_is_blocked_location)]
            snap_sales["City_Join_Key"] = snap_sales["Shipping_City"].map(_normalize_city_key)
            snap_sales["State_Join_Key"] = [
                _normalize_state_key(st, sales_country) for st in snap_sales["Shipping_State"]
            ]
            snap_sales = snap_sales[
                (snap_sales["City_Join_Key"] != "") & (snap_sales["State_Join_Key"] != "")
            ]
            if not snap_sales.empty:
                sales_agg = snap_sales.groupby(
                    ["City_Join_Key", "State_Join_Key"], as_index=False
                ).agg(
                    Quantity_Sold=("Quantity_Sold", "sum"),
                    Order_Revenue=("Order_Revenue", "sum"),
                )
                audit = audit.merge(sales_agg, on=["City_Join_Key", "State_Join_Key"], how="left")

    if "Quantity_Sold" not in audit.columns:
        audit["Quantity_Sold"] = 0
    if "Order_Revenue" not in audit.columns:
        audit["Order_Revenue"] = 0.0
    audit["Quantity_Sold"] = (
        pd.to_numeric(audit["Quantity_Sold"], errors="coerce").fillna(0).round().astype(int)
    )
    audit["Order_Revenue"] = (
        pd.to_numeric(audit["Order_Revenue"], errors="coerce").fillna(0.0).round(2)
    )

    with_weather = int((audit["Marketing_Action"] != "Baseline").sum())
    with_sales = int((audit["Quantity_Sold"] > 0).sum())
    print(
        f"[INFO] {region_key} audit matrix {snap}: {len(audit):,} cities "
        f"({with_weather:,} with weather triggers, {with_sales:,} with snapshot sales)."
    )
    return _clean_frame_strings(audit[AUDIT_OUTPUT_COLUMNS])


def build_region_marketing_keys(
    df_marketing: pd.DataFrame, sales_country: str
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build city, state/province, and country lookup tables for a single region."""
    if df_marketing.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    snap = df_marketing["Report_Date"].iloc[0]
    city_rows: list[dict[str, object]] = []
    state_rows: list[dict[str, object]] = []

    if "City" in df_marketing.columns:
        for (report_date, city, join_state), group in df_marketing.groupby(
            ["Report_Date", "City", "State"], dropna=False
        ):
            city_key = _normalize_city_key(city)
            state_key = _normalize_state_key(join_state, sales_country)
            if not city_key or not state_key:
                continue
            picked = _pick_dominant_action(group)
            city_rows.append(
                {
                    "Report_Date": report_date,
                    "Join_City_Key": city_key,
                    "Join_State_Key": state_key,
                    **picked,
                }
            )

    for (report_date, join_state), group in df_marketing.groupby(["Report_Date", "State"], dropna=False):
        state_key = _normalize_state_key(join_state, sales_country)
        if not state_key:
            continue
        picked = _pick_dominant_action(group)
        state_rows.append(
            {
                "Report_Date": report_date,
                "Join_State": str(join_state).strip(),
                "Join_State_Key": state_key,
                **picked,
            }
        )

    country_picked = _pick_dominant_action(df_marketing)
    country_level = pd.DataFrame(
        [
            {
                "Report_Date": snap,
                "Join_Country": _normalize_country(sales_country),
                **country_picked,
            }
        ]
    )
    return pd.DataFrame(city_rows), pd.DataFrame(state_rows), country_level


def _pick_dominant_action(group: pd.DataFrame) -> dict[str, object]:
    idx = group["_rank"].idxmax()
    row = group.loc[idx]
    return {
        "Marketing_Action": row["Marketing_Action"],
        "Rain_Amount": float(group["Rain_Amount"].max()),
        "Yesterday_Rain": float(group["Yesterday_Rain"].max()),
        "Max_UV_Index": float(group["Max_UV_Index"].max()),
        "Logic_Summary": row["Logic_Summary"],
    }


def build_marketing_join_keys(df_marketing: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Deprecated combined helper — kept for compatibility; prefer build_region_marketing_keys."""
    if df_marketing.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    country = str(df_marketing["Marketing_Country"].iloc[0])
    return build_region_marketing_keys(df_marketing, country)


def _assign_marketing_column(
    target: pd.DataFrame, mask: pd.Series, merged: pd.DataFrame, col: str
) -> None:
    if col in ("Marketing_Action", "Logic_Summary"):
        values = merged[col].fillna("").astype(str).values
    else:
        values = pd.to_numeric(merged[col], errors="coerce").fillna(0.0).values
    target.loc[mask, col] = values


def merge_sales_marketing_region(
    df_sales: pd.DataFrame,
    snapshot_date: date,
    *,
    region_key: str,
    sales_countries: frozenset[str],
    marketing_path: Path,
) -> pd.DataFrame:
    """
    Join regional sales to that region's marketing snapshot.

    Adaptive join priority on snapshot-day rows:
    1) Date + Shipping_City + Shipping_State when city is not masked (historical ledger)
    2) Date + Shipping_State / province for live rows (city = "blocked")
    3) Date + Shipping_Country country-level rollup
    4) Baseline fallback — never emit blank / NaN / #N/A on snapshot rows
    """
    sales_country = next(iter(sales_countries))
    regional = df_sales[df_sales["Shipping_Country"].isin(sales_countries)].copy()
    if regional.empty:
        print(f"[WARN] {region_key}: no sales rows for countries {sorted(sales_countries)}.")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    snapshot_date = _resolve_snapshot_date(snapshot_date, marketing_path)
    df_marketing = load_region_marketing(marketing_path, sales_country, snapshot_date)
    city_level, state_level, country_level = build_region_marketing_keys(df_marketing, sales_country)

    sales = regional.copy()
    sales["Marketing_Action"] = ""
    sales["Logic_Summary"] = ""
    sales["Rain_Amount"] = 0.0
    sales["Yesterday_Rain"] = 0.0
    sales["Max_UV_Index"] = 0.0
    sales["City_Join_Key"] = sales["Shipping_City"].map(_normalize_city_key)
    sales["State_Join_Key"] = [
        _normalize_state_key(st, sales_country) for st in sales["Shipping_State"]
    ]

    snap = snapshot_date.strftime("%Y-%m-%d")
    snap_mask = sales["Date"] == snap
    blocked_mask = sales["Shipping_City"].map(_is_blocked_location)

    # 1) Hyper-local city join for rows with real cities (snapshot day).
    if not city_level.empty and snap_mask.any():
        city_lookup = city_level[
            ["Report_Date", "Join_City_Key", "Join_State_Key", *MARKETING_COLUMNS]
        ]
        city_mask = snap_mask & (~blocked_mask) & (sales["City_Join_Key"] != "")
        if city_mask.any():
            merged = sales.loc[city_mask, SALES_COLUMNS + ["City_Join_Key", "State_Join_Key"]].merge(
                city_lookup,
                left_on=["Date", "City_Join_Key", "State_Join_Key"],
                right_on=["Report_Date", "Join_City_Key", "Join_State_Key"],
                how="left",
            )
            for col in MARKETING_COLUMNS:
                _assign_marketing_column(sales, city_mask, merged, col)

    # 2) State / province join — blocked cities and other unmapped snapshot rows.
    if not state_level.empty and snap_mask.any():
        state_lookup = state_level[["Report_Date", "Join_State_Key", *MARKETING_COLUMNS]]
        state_mask = snap_mask & (sales["Marketing_Action"] == "") & (sales["State_Join_Key"] != "")
        if state_mask.any():
            merged = sales.loc[state_mask, SALES_COLUMNS + ["State_Join_Key"]].merge(
                state_lookup,
                left_on=["Date", "State_Join_Key"],
                right_on=["Report_Date", "Join_State_Key"],
                how="left",
            )
            for col in MARKETING_COLUMNS:
                _assign_marketing_column(sales, state_mask, merged, col)

    # 3) Country-level fallback — required for blocked city/state live stream rows.
    if not country_level.empty and snap_mask.any():
        country_lookup = country_level[["Report_Date", "Join_Country", *MARKETING_COLUMNS]]
        needs_country = snap_mask & (sales["Marketing_Action"] == "")
        if needs_country.any():
            merged = sales.loc[needs_country, SALES_COLUMNS].merge(
                country_lookup,
                left_on=["Date", "Shipping_Country"],
                right_on=["Report_Date", "Join_Country"],
                how="left",
            )
            for col in MARKETING_COLUMNS:
                _assign_marketing_column(sales, needs_country, merged, col)

    # 4) Baseline fallback — never emit blank / NaN / #N/A on snapshot rows.
    sales.loc[snap_mask & (sales["Marketing_Action"] == ""), "Marketing_Action"] = "Baseline"
    sales.loc[snap_mask & (sales["Logic_Summary"] == ""), "Logic_Summary"] = (
        "No explicit weather rule matched; Baseline fallback applied."
    )

    city_matched = int(
        (snap_mask & (~blocked_mask) & (sales["Marketing_Action"] != "Baseline")).sum()
    )
    blocked_matched = int(
        (snap_mask & blocked_mask & (sales["Marketing_Action"] != "Baseline")).sum()
    )
    matched = int((snap_mask & (sales["Marketing_Action"] != "Baseline")).sum())
    snap_total = int(snap_mask.sum())
    print(
        f"[INFO] {region_key} marketing join on {snap}: "
        f"{matched:,}/{snap_total:,} snapshot-day rows matched "
        f"({city_matched:,} city-level, {blocked_matched:,} blocked→state/country, "
        f"{len(sales):,} total regional rows)."
    )
    return _clean_frame_strings(sales[OUTPUT_COLUMNS])


def merge_sales_marketing(df_sales: pd.DataFrame, snapshot_date: date) -> pd.DataFrame:
    """Legacy single-sheet merge (US-only path). Prefer sync_sales_bridge multi-region loop."""
    return merge_sales_marketing_region(
        df_sales,
        snapshot_date,
        region_key="US",
        sales_countries=frozenset({"US"}),
        marketing_path=MARKETING_US_PATH,
    )


def _nonempty(msg: str, exc: BaseException) -> str:
    text = (msg or "").strip()
    if text:
        return text
    return f"{type(exc).__name__}: {exc!r}"


def _gspread_cell_limit_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "10000000" in text and "cell" in text


def _gspread_transient(exc: Exception) -> bool:
    if _gspread_cell_limit_error(exc):
        return False
    if isinstance(exc, APIError):
        resp = getattr(exc, "response", None)
        status = getattr(resp, "status_code", None)
        return status in {429, 500, 502, 503, 504}
    return isinstance(exc, GSpreadException)


def _run_gspread_with_retry(label: str, fn, *, max_retries: int = 5):
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            return fn()
        except (APIError, GSpreadException) as exc:
            last_exc = exc
            if not _gspread_transient(exc) or attempt >= max_retries - 1:
                raise
            delay = min(32, 2**attempt)
            print(f"[WARN] {label}: transient Google Sheets error; retrying in {delay}s...")
            time.sleep(delay)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"{label}: retry loop exited unexpectedly")


def _format_gspread_error(exc: Exception, spreadsheet_name: str = SPREADSHEET_NAME) -> str:
    if isinstance(exc, SpreadsheetNotFound):
        resp = exc.args[0] if exc.args else None
        status = getattr(resp, "status_code", None)
        return _nonempty(
            (
                f"Google Sheet {spreadsheet_name!r} was not found or is not shared with the "
                f"service account (Drive API returned HTTP {status}). "
                "Share the spreadsheet with the service account client_email from your credentials JSON."
            ),
            exc,
        )
    if isinstance(exc, WorksheetNotFound):
        return _nonempty(
            "The first worksheet (index 0) was not found in the target spreadsheet. "
            "Ensure the spreadsheet has at least one tab.",
            exc,
        )
    if isinstance(exc, APIError):
        resp = getattr(exc, "response", None)
        status = getattr(resp, "status_code", "?")
        api_msg = exc.error.get("message") if getattr(exc, "error", None) else None
        return _nonempty(
            f"Google Sheets API error (HTTP {status}): {api_msg or exc!r}",
            exc,
        )
    if isinstance(exc, GSpreadException):
        return _nonempty(f"Google Sheets error ({type(exc).__name__})", exc)
    return _nonempty(str(exc), exc)


def _describe_exception(exc: BaseException) -> str:
    if isinstance(exc, RuntimeError) and exc.__cause__ is not None:
        return _describe_exception(exc.__cause__)
    if isinstance(exc, (SpreadsheetNotFound, WorksheetNotFound, APIError, GSpreadException)):
        return _format_gspread_error(exc)
    return _nonempty(str(exc), exc)


def _is_permanent_sync_error(exc: BaseException) -> bool:
    if isinstance(exc, PermanentSyncError):
        return True
    if _gspread_cell_limit_error(exc):
        return True
    cause = getattr(exc, "__cause__", None)
    if cause is not None and cause is not exc:
        return _is_permanent_sync_error(cause)
    return False


def _is_service_account_payload(data: object) -> bool:
    if not isinstance(data, dict):
        return False
    if data.get("type") == "service_account":
        return True
    return {"project_id", "private_key", "client_email"} <= set(data.keys())


def _json_is_service_account(path: Path) -> bool:
    if not path.is_file():
        return False
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return _is_service_account_payload(data)


def _discover_creds_path(explicit: Path | None = None) -> Path | None:
    """Find a Google service-account JSON near the project (never OAuth client secrets)."""
    candidates: list[Path] = []

    if explicit is not None:
        candidates.append(explicit)

    for env_name in ("GOOGLE_CREDS_FILE", "GOOGLE_SERVICE_ACCOUNT_FILE"):
        env_path = os.getenv(env_name, "").strip()
        if env_path:
            candidates.append(Path(env_path))

    for directory in CREDS_SEARCH_DIRS:
        if not directory.is_dir():
            continue
        for name in PREFERRED_CRED_FILENAMES:
            candidates.append(directory / name)
        for path in sorted(directory.glob("gmail-milestone-tracker*.json")):
            candidates.append(path)
        for path in sorted(directory.glob("*.json")):
            if path.name in SKIP_JSON_FILENAMES:
                continue
            candidates.append(path)

    seen: set[Path] = set()
    for path in candidates:
        resolved = path.expanduser().resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if _json_is_service_account(resolved):
            return resolved
    return None


def _get_gspread_client(creds_path: Path | None = None) -> gspread.Client:
    """Authenticate via GOOGLE_CREDS_JSON / GOOGLE_CREDS_FILE, else auto-discovered JSON."""
    _sanitize_malformed_google_creds_env()

    info = _load_service_account_from_env()
    if info is not None:
        return gspread.service_account_from_dict(info)

    file_path = _discover_creds_path(creds_path)
    if file_path is not None:
        print(f"[INFO] Using Google credentials from {file_path}")
        return gspread.service_account(filename=str(file_path))

    raise RuntimeError(
        "Google service-account credentials not found. Set GOOGLE_CREDS_FILE to your "
        "service-account JSON path, place google_creds.json in the project root, pass "
        f"--creds, or fix GOOGLE_CREDS_JSON in .env. Searched: "
        f"{', '.join(str(d) for d in CREDS_SEARCH_DIRS)}."
    )


def _service_account_email(creds_path: Path | None = None) -> str:
    path = _discover_creds_path(creds_path)
    if path is None:
        return "your-service-account@project.iam.gserviceaccount.com"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return str(data.get("client_email", "")).strip() or path.name
    except (OSError, json.JSONDecodeError):
        return path.name


def _open_spreadsheet(
    creds_path: Path | None,
    spreadsheet_name: str,
    *,
    spreadsheet_id: str = "",
    allow_sales_id_fallback: bool = True,
) -> gspread.Spreadsheet:
    """Open a spreadsheet by ID or exact title (never mix audit vs sales targets)."""
    client = _get_gspread_client(creds_path)
    _load_dotenv()

    sheet_id = spreadsheet_id.strip()
    if not sheet_id and allow_sales_id_fallback:
        sheet_id = os.getenv("SALES_BRIDGE_SPREADSHEET_ID", "").strip()

    email = _service_account_email(creds_path)

    try:
        if sheet_id:
            spreadsheet = client.open_by_key(sheet_id)
        else:
            spreadsheet = client.open(spreadsheet_name)
    except (SpreadsheetNotFound, WorksheetNotFound, APIError, GSpreadException) as exc:
        raise RuntimeError(
            f"{_format_gspread_error(exc, spreadsheet_name)} "
            f"Share {spreadsheet_name!r} with the service account ({email}) "
            "or set its spreadsheet ID in .env (AUDIT_BRIDGE_SPREADSHEET_ID_* for audit workbooks)."
        ) from exc

    print(
        f"[INFO] Opened spreadsheet {spreadsheet.title!r} "
        f"(id={spreadsheet.id}, url=https://docs.google.com/spreadsheets/d/{spreadsheet.id})"
    )
    return spreadsheet


def _open_bridge_sheet(
    creds_path: Path | None,
    spreadsheet_name: str,
    *,
    spreadsheet_id_env: str = "",
    allow_sales_id_fallback: bool = True,
    prefer_title_first: bool = False,
) -> gspread.Worksheet:
    sheet_id = ""
    if spreadsheet_id_env:
        _load_dotenv()
        sheet_id = os.getenv(spreadsheet_id_env, "").strip()

    if sheet_id:
        spreadsheet = _open_spreadsheet(
            creds_path,
            spreadsheet_name,
            spreadsheet_id=sheet_id,
            allow_sales_id_fallback=allow_sales_id_fallback,
        )
        return spreadsheet.sheet1

    if prefer_title_first and spreadsheet_name.strip():
        client = _get_gspread_client(creds_path)
        email = _service_account_email(creds_path)
        try:
            spreadsheet = client.open(spreadsheet_name)
        except (SpreadsheetNotFound, WorksheetNotFound, APIError, GSpreadException) as exc:
            if not sheet_id:
                raise RuntimeError(
                    f"{_format_gspread_error(exc, spreadsheet_name)} "
                    f"Share {spreadsheet_name!r} with the service account ({email}) "
                    "or set its spreadsheet ID in .env (AUDIT_BRIDGE_SPREADSHEET_ID_* for audit workbooks)."
                ) from exc
            print(
                f"[WARN] Title lookup failed for {spreadsheet_name!r}; "
                f"falling back to spreadsheet ID {sheet_id}."
            )
            spreadsheet = _open_spreadsheet(
                creds_path,
                spreadsheet_name,
                spreadsheet_id=sheet_id,
                allow_sales_id_fallback=allow_sales_id_fallback,
            )
        else:
            print(
                f"[INFO] Audit targeting: opened by title {spreadsheet.title!r} "
                f"(id={spreadsheet.id}, url=https://docs.google.com/spreadsheets/d/{spreadsheet.id})"
            )
        return spreadsheet.sheet1

    spreadsheet = _open_spreadsheet(
        creds_path,
        spreadsheet_name,
        spreadsheet_id=sheet_id,
        allow_sales_id_fallback=allow_sales_id_fallback,
    )
    return spreadsheet.sheet1


def _validate_bridge_output(df: pd.DataFrame, *, region_key: str) -> None:
    if df.empty:
        print(f"[WARN] {region_key}: no rows to write; sheet will receive headers only.")
        return
    if (df["SKU"].astype(str).str.strip() == "").all():
        raise RuntimeError(f"{region_key}: refusing to write bridge sheet — all SKU values are blank.")
    na_like = df.astype(str).apply(lambda col: col.str.fullmatch(r"#N/A", case=False, na=False).sum())
    bad_cols = na_like[na_like > 0]
    if not bad_cols.empty:
        raise RuntimeError(
            f"{region_key}: refusing to write bridge sheet — found literal #N/A in "
            f"{bad_cols.index.tolist()}."
        )


def _dataframe_to_sheet_values(
    df: pd.DataFrame, columns: list[str] | None = None
) -> list[list]:
    cols = columns or OUTPUT_COLUMNS
    df = _clean_frame_strings(df)
    rows: list[list] = []
    for row in df.itertuples(index=False):
        rows.append([getattr(row, col) for col in cols])
    return [cols] + rows


def _sheet_range_a1(row_start: int, row_end: int, col_count: int) -> str:
    """Inclusive 1-based row range covering columns 1..col_count."""
    return f"{rowcol_to_a1(row_start, 1)}:{rowcol_to_a1(row_end, col_count)}"


def _ensure_worksheet_grid(
    worksheet: gspread.Worksheet,
    *,
    min_rows: int,
    col_count: int,
    region_key: str,
) -> None:
    """Grow the worksheet grid before batched writes (clear() may leave a small grid)."""
    needed_rows = max(worksheet.row_count, min_rows)
    needed_cols = max(worksheet.col_count, col_count)
    if needed_rows <= worksheet.row_count and needed_cols <= worksheet.col_count:
        return

    def _resize() -> None:
        worksheet.resize(rows=needed_rows, cols=needed_cols)

    _run_gspread_with_retry(f"{region_key} sheet grid expand → {needed_rows:,}×{needed_cols}", _resize)


def _write_sheet_values_batched(
    worksheet: gspread.Worksheet,
    values: list[list],
    *,
    region_key: str,
    batch_size: int = SHEET_WRITE_BATCH_ROWS,
) -> None:
    total_rows = len(values)
    if total_rows == 0:
        raise RuntimeError(f"{region_key}: refusing to write an empty grid.")
    col_count = len(values[0])
    if col_count == 0:
        raise RuntimeError(f"{region_key}: refusing to write zero columns.")

    def _prepare() -> None:
        worksheet.clear()
        # Shrink grid so prior oversized dimensions do not count against the 10M workbook cap.
        try:
            worksheet.resize(rows=1, cols=col_count)
        except APIError as exc:
            if not _gspread_cell_limit_error(exc):
                raise
            print(
                f"[WARN] {region_key}: could not shrink worksheet grid before write; "
                "continuing with batch updates only."
            )
        # Do not pre-resize to the full row count — that pre-allocates the grid and can
        # trip the workbook cell limit even when batch writes would fit.

    _run_gspread_with_retry(f"{region_key} sheet prepare", _prepare)

    batch_size = max(1, batch_size)
    total_batches = (total_rows + batch_size - 1) // batch_size
    for batch_index, start_idx in enumerate(range(0, total_rows, batch_size), start=1):
        chunk = values[start_idx : start_idx + batch_size]
        sheet_row_start = start_idx + 1
        sheet_row_end = sheet_row_start + len(chunk) - 1
        range_name = _sheet_range_a1(sheet_row_start, sheet_row_end, col_count)

        def _write_chunk(
            _chunk: list[list] = chunk,
            _range_name: str = range_name,
            _sheet_row_end: int = sheet_row_end,
        ) -> None:
            _ensure_worksheet_grid(
                worksheet,
                min_rows=_sheet_row_end,
                col_count=col_count,
                region_key=region_key,
            )
            worksheet.update(
                _chunk,
                range_name=_range_name,
                value_input_option=ValueInputOption.raw,
            )

        _run_gspread_with_retry(
            f"{region_key} sheet write batch {batch_index}/{total_batches}",
            _write_chunk,
        )
        if batch_index < total_batches and SHEET_BATCH_PAUSE_S > 0:
            time.sleep(SHEET_BATCH_PAUSE_S)

    print(
        f"[INFO] {region_key}: batched write complete — {total_rows:,} rows "
        f"({total_batches} batch(es) of up to {batch_size:,})."
    )


def _write_sheet_values(
    worksheet: gspread.Worksheet,
    values: list[list],
    *,
    region_key: str,
) -> None:
    data_rows = max(0, len(values) - 1)
    if data_rows > SHEET_BATCH_WRITE_THRESHOLD:
        print(
            f"[INFO] {region_key}: using batched sheet write "
            f"({data_rows:,} data rows > {SHEET_BATCH_WRITE_THRESHOLD:,} threshold)."
        )
        _write_sheet_values_batched(worksheet, values, region_key=region_key)
        return

    def _write() -> None:
        worksheet.clear()
        worksheet.update(
            values,
            range_name="A1",
            value_input_option=ValueInputOption.raw,
        )

    _run_gspread_with_retry(f"{region_key} sheet write", _write)


def _verify_sheet_write(
    worksheet: gspread.Worksheet,
    *,
    region_key: str,
    expected_total_rows: int,
    header: list,
) -> None:
    """Spot-check header + last row; avoid get_all_values() on large sheets."""

    def _verify() -> None:
        if expected_total_rows < 2:
            raise RuntimeError(
                f"{region_key}: expected at least header + 1 data row, got {expected_total_rows}."
            )
        header_row = worksheet.row_values(1)
        if len(header_row) < len(header):
            raise RuntimeError(
                f"{region_key}: header row too short after write "
                f"(got {len(header_row)} cols, expected {len(header)})."
            )
        for idx, col_name in enumerate(header):
            if str(header_row[idx]).strip() != str(col_name).strip():
                raise RuntimeError(
                    f"{region_key}: header mismatch at column {idx + 1}: "
                    f"expected {col_name!r}, got {header_row[idx]!r}."
                )
        last_a = worksheet.acell(rowcol_to_a1(expected_total_rows, 1)).value
        if last_a is None or str(last_a).strip() == "":
            raise RuntimeError(
                f"{region_key}: last row {rowcol_to_a1(expected_total_rows, 1)} is empty after write."
            )

    _run_gspread_with_retry(f"{region_key} sheet verify", _verify)


def _validate_audit_output(df: pd.DataFrame, *, region_key: str, min_rows: int = 1) -> None:
    if len(df) < min_rows:
        raise RuntimeError(
            f"{region_key}: audit bridge has only {len(df)} rows; expected at least {min_rows}."
        )
    na_like = df.astype(str).apply(lambda col: col.str.fullmatch(r"#N/A", case=False, na=False).sum())
    bad_cols = na_like[na_like > 0]
    if not bad_cols.empty:
        raise RuntimeError(
            f"{region_key}: audit bridge contains literal #N/A in {bad_cols.index.tolist()}."
        )


def overwrite_sheet(
    worksheet: gspread.Worksheet,
    df: pd.DataFrame,
    *,
    region_key: str,
    columns: list[str] | None = None,
    validate_sales_sku: bool = True,
) -> None:
    if validate_sales_sku:
        _validate_bridge_output(df, region_key=region_key)
    else:
        _validate_audit_output(df, region_key=region_key, min_rows=min_rows_for_region(region_key))
    cols = columns or OUTPUT_COLUMNS
    values = _dataframe_to_sheet_values(df, columns=cols)

    try:
        _write_sheet_values(worksheet, values, region_key=region_key)
    except (APIError, GSpreadException) as exc:
        raise RuntimeError(_format_gspread_error(exc)) from exc

    row_count = max(0, len(values) - 1)
    print(
        f"[INFO] {region_key}: worksheet {worksheet.title!r} updated with "
        f"{row_count:,} data rows (+ header)."
    )
    if row_count == 0:
        raise RuntimeError(f"{region_key}: refusing to write an empty data grid.")

    try:
        _verify_sheet_write(
            worksheet,
            region_key=region_key,
            expected_total_rows=len(values),
            header=values[0],
        )
    except (APIError, GSpreadException) as exc:
        raise RuntimeError(_format_gspread_error(exc)) from exc


def min_rows_for_region(region_key: str) -> int:
    """Minimum expected audit framework rows per region."""
    return {"US": 1000, "UK": 50, "CA": 50}.get(region_key, 1)


def _sheets_grid_cell_count(data_rows: int, col_count: int) -> int:
    return (data_rows + 1) * col_count


def _master_sheets_max_data_rows(col_count: int) -> int:
    budget = GOOGLE_SHEETS_MAX_CELLS - GOOGLE_SHEETS_CELL_BUFFER
    if col_count <= 0:
        raise PermanentSyncError("MASTER: cannot export zero columns to Google Sheets.")
    return max(0, (budget // col_count) - 1)


def prepare_master_for_google_sheets(df: pd.DataFrame, *, end_date: date) -> pd.DataFrame:
    """
    Build a Sheets-safe master slice: rolling date window, no Logic_Summary, under 10M cells.
    The full df is still written to data/weather_pulse_sales_bridge_master_preview.csv unchanged.
    """
    if df.empty:
        raise PermanentSyncError("MASTER: refusing to export an empty master bridge to Google Sheets.")

    cols = [col for col in MASTER_SHEETS_COLUMNS if col in df.columns]
    if not cols:
        raise PermanentSyncError("MASTER: no exportable columns found for Google Sheets.")

    out = _clean_frame_strings(df)
    if "Date" in out.columns:
        out["_sheet_date"] = pd.to_datetime(out["Date"], errors="coerce")
        if MASTER_SHEETS_LOOKBACK_DAYS > 0:
            cutoff = pd.Timestamp(end_date) - pd.Timedelta(days=MASTER_SHEETS_LOOKBACK_DAYS)
            before = len(out)
            out = out[out["_sheet_date"] >= cutoff]
            print(
                f"[INFO] MASTER Sheets export: {MASTER_SHEETS_LOOKBACK_DAYS}-day lookback "
                f"({cutoff.date()} → {end_date}) — {len(out):,} rows "
                f"(dropped {before - len(out):,} older)."
            )
        out = out.sort_values("_sheet_date", kind="mergesort")
        out = out.drop(columns=["_sheet_date"])
    else:
        out = out.sort_values(list(out.columns)[:1], kind="mergesort")

    max_rows = _master_sheets_max_data_rows(len(cols))
    if len(out) > max_rows:
        dropped = len(out) - max_rows
        out = out.iloc[-max_rows:]
        print(
            f"[WARN] MASTER Sheets export: trimmed {dropped:,} oldest rows to stay under "
            f"Google Sheets {GOOGLE_SHEETS_MAX_CELLS:,}-cell workbook limit."
        )

    out = out[cols]
    cells = _sheets_grid_cell_count(len(out), len(cols))
    if cells > GOOGLE_SHEETS_MAX_CELLS - GOOGLE_SHEETS_CELL_BUFFER:
        raise PermanentSyncError(
            f"MASTER: export grid {len(out):,} rows × {len(cols)} cols = {cells:,} cells still "
            f"exceeds Google Sheets limit ({GOOGLE_SHEETS_MAX_CELLS:,}). "
            "Lower MASTER_SHEETS_LOOKBACK_DAYS or reduce columns."
        )

    print(
        f"[INFO] MASTER Sheets export: {len(out):,} rows × {len(cols)} cols = {cells:,} cells "
        f"(limit {GOOGLE_SHEETS_MAX_CELLS:,}; Logic_Summary omitted). "
        f"Full {len(df):,}-row ledger remains in repo preview CSV."
    )
    return out


def push_to_google_sheet(
    df: pd.DataFrame,
    *,
    region_key: str,
    spreadsheet_name: str,
    creds_path: Path | None = None,
    spreadsheet_id_env: str = "",
    columns: list[str] | None = None,
    validate_sales_sku: bool = True,
    allow_sales_id_fallback: bool = True,
) -> None:
    print(f"[INFO] {region_key}: pushing {len(df):,} rows to {spreadsheet_name!r} (Sheet1)...")
    worksheet = _open_bridge_sheet(
        creds_path,
        spreadsheet_name,
        spreadsheet_id_env=spreadsheet_id_env,
        allow_sales_id_fallback=allow_sales_id_fallback,
        prefer_title_first=not validate_sales_sku,
    )
    overwrite_sheet(
        worksheet,
        df,
        region_key=region_key,
        columns=columns,
        validate_sales_sku=validate_sales_sku,
    )


def sync_sales_bridge(
    *,
    end_date: date | None = None,
    creds_path: Path | None = None,
    historical_path: Path | None = None,
    dry_run: bool = False,
    skip_sheets: bool = False,
    regions: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    api_key = _require_env("TRIPLE_WHALE_API_KEY")
    end = end_date or date.today()
    if end < START_DATE:
        raise ValueError(f"End date {end} is before start date {START_DATE}.")

    region_keys = regions or list(REGIONS.keys())
    unknown = [r for r in region_keys if r not in REGIONS]
    if unknown:
        raise ValueError(f"Unknown region keys: {unknown}. Valid: {list(REGIONS.keys())}")

    df_sales_audit = load_unified_sales(api_key, end, historical_path=historical_path)
    df_sales_master = load_master_sales_bridge(api_key, end, historical_path=historical_path)
    live_start, _live_end = _resolve_live_window(end)
    export_city_sales_overlay(df_sales_audit, live_start=live_start)
    print(
        f"[INFO] Sales load complete: audit stream {len(df_sales_audit):,} rows; "
        f"master bridge {len(df_sales_master):,} rows."
    )

    results: dict[str, pd.DataFrame] = {}
    master_frames: list[pd.DataFrame] = []
    for region_key in region_keys:
        cfg = REGIONS[region_key]
        marketing_path = cfg["marketing_path"]  # type: ignore[assignment]
        sales_countries = cfg["sales_countries"]  # type: ignore[assignment]
        audit_id_env = str(cfg.get("audit_spreadsheet_id_env", ""))

        snapshot_date = _resolve_snapshot_date(end, marketing_path)  # type: ignore[arg-type]
        audit_title = _audit_spreadsheet_title(region_key, snapshot_date)

        print(f"[INFO] === Processing region {region_key} ===")
        print(f"[INFO]   Audit workbook → {audit_title!r} (Sheet1)")

        df_audit = build_marketing_audit_bridge_region(
            region_key,
            end,
            df_sales_audit,
            sales_countries=sales_countries,  # type: ignore[arg-type]
            marketing_path=marketing_path,  # type: ignore[arg-type]
        )
        results[region_key] = df_audit

        df_region_master = merge_sales_marketing_region(
            df_sales_master,
            end,
            region_key=region_key,
            sales_countries=sales_countries,  # type: ignore[arg-type]
            marketing_path=marketing_path,  # type: ignore[arg-type]
        )
        if not df_region_master.empty:
            master_frames.append(df_region_master)

        print(
            f"[INFO] {region_key} master bridge segment: {len(df_region_master):,} rows; "
            f"audit matrix: {len(df_audit):,} rows."
        )

        audit_preview = ROOT / "data" / f"weather_pulse_audit_{region_key.lower()}_preview.csv"
        audit_preview.parent.mkdir(parents=True, exist_ok=True)
        df_audit.to_csv(audit_preview, index=False)
        print(f"[INFO] {region_key}: local audit matrix → {audit_preview} ({len(df_audit):,} rows)")

        if dry_run:
            sales_preview = ROOT / "data" / f"weather_pulse_sales_bridge_{region_key.lower()}_preview.csv"
            df_region_master.to_csv(sales_preview, index=False)
            print(f"[INFO] {region_key}: dry-run master segment → {sales_preview}")

        if not skip_sheets and not dry_run:
            push_to_google_sheet(
                df_audit,
                region_key=region_key,
                spreadsheet_name=audit_title,
                creds_path=creds_path,
                spreadsheet_id_env=audit_id_env,
                columns=AUDIT_OUTPUT_COLUMNS,
                validate_sales_sku=False,
                allow_sales_id_fallback=False,
            )
            time.sleep(3)

    if master_frames:
        df_master = pd.concat(master_frames, ignore_index=True).sort_values(
            ["Date", "Shipping_Country", "Platform", "Source_Name", "SKU"]
        )
    else:
        df_master = pd.DataFrame(columns=OUTPUT_COLUMNS)

    for country in ("US", "UK", "CA"):
        n = int((df_master["Shipping_Country"] == country).sum())
        print(f"[INFO] Master bridge {country}: {n:,} rows.")

    master_preview = ROOT / "data" / "weather_pulse_sales_bridge_master_preview.csv"
    master_preview.parent.mkdir(parents=True, exist_ok=True)
    df_master.to_csv(master_preview, index=False)
    print(f"[INFO] Master bridge preview → {master_preview} ({len(df_master):,} rows)")

    if not skip_sheets and not dry_run:
        df_sheets_master = prepare_master_for_google_sheets(df_master, end_date=end)
        push_to_google_sheet(
            df_sheets_master,
            region_key="MASTER",
            spreadsheet_name=MASTER_SALES_BRIDGE_NAME,
            creds_path=creds_path,
            spreadsheet_id_env=MASTER_SALES_BRIDGE_ID_ENV,
            columns=list(df_sheets_master.columns),
            allow_sales_id_fallback=True,
        )

    return results


def main() -> int:
    _init_pipeline()

    parser = argparse.ArgumentParser(description="Sync sales bridge data into Google Sheets.")
    parser.add_argument("--end-date", type=str, default="", help="Inclusive end date YYYY-MM-DD.")
    parser.add_argument(
        "--regions",
        type=str,
        default="",
        help="Comma-separated region keys to sync (default: US,UK,CA).",
    )
    parser.add_argument(
        "--creds",
        type=Path,
        default=None,
        help="Optional path to service-account JSON (auto-discovered if omitted).",
    )
    parser.add_argument(
        "--historical-ledger",
        type=Path,
        default=None,
        help="Path to immutable historical sales CSV (auto-discovered if omitted).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Write regional preview CSVs; skip Google Sheets.")
    parser.add_argument("--skip-sheets", action="store_true", help="Skip Google Sheets upload.")
    args = parser.parse_args()

    end_date: date | None = None
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()

    regions: list[str] | None = None
    if args.regions.strip():
        regions = [r.strip().upper() for r in args.regions.split(",") if r.strip()]

    try:
        sync_sales_bridge(
            end_date=end_date,
            creds_path=args.creds,
            historical_path=args.historical_ledger,
            dry_run=args.dry_run,
            skip_sheets=args.skip_sheets,
            regions=regions,
        )
    except PermanentSyncError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"[ERROR] {_describe_exception(exc)}", file=sys.stderr)
        if _is_permanent_sync_error(exc):
            return 2
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
