#!/usr/bin/env python3
"""
Weather Pulse dashboard.

  Phase 1 — historical proof (observed rain ↔ umbrella sales)
  Phase 2 — forecast triggers, ad-spend allocation, execution alignment

Run after:  python run_intel_pipeline.py
Launch:     streamlit run dashboard.py
"""

from __future__ import annotations

import html
import json
import re
from datetime import datetime
from pathlib import Path

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

ROOT = Path(__file__).resolve().parent
BRIDGE_PATH = ROOT / "historic_sales_bridge.csv"
VERIFICATION_PATH = ROOT / "historic_verification_bridge.csv"
CITY_SALES_OVERLAY_PATH = ROOT / "data" / "city_sales_overlay.csv"
CITY_COORDS_PATH = ROOT / "data" / "city_coordinates.json"
BRIDGE_COLUMNS: tuple[str, ...] = ("Date", "Region", "Type", "Value", "Lat", "Lon")
INVALID_REGIONS: frozenset[str] = frozenset({"", "NAN", "NONE", "NULL"})

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

FORECAST_PATH = ROOT / "forecast_ad_spend_bridge.csv"
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

# Imperative phrasing — the dashboard tells you what to do, not what it observed.
ACTION_IMPERATIVE: dict[str, str] = {
    "Scale Umbrellas": "Scale Umbrellas",
    "Scale Umbrellas (Residual Demand)": "Scale Umbrellas (residual demand)",
    "Sun Protection (Hats/Shirts)": "Push Sun Protection (Hats/Shirts)",
    "Baseline": "Hold Baseline",
}

RAINY_COLOR = "#2f9e44"
DRY_COLOR = "#868e96"
VERIFY_MATCH_COLOR = "#2f9e44"
VERIFY_DRY_COLOR = "#adb5bd"

SUGGESTED_TYPE = "suggested_strategy"
ACTUAL_TYPE = "actual_action"
SALES_TYPES: frozenset[str] = frozenset({"sales_revenue", "sales_units"})
WEATHER_METRIC_TYPES: frozenset[str] = frozenset({
    "weather_rain",
    "weather_yesterday_rain",
    "weather_uv",
})

# Marketing action palette (matches Weather Pulse engine)
ACTION_ORDER: tuple[str, ...] = (
    "Sun Protection (Hats/Shirts)",
    "Scale Umbrellas (Residual Demand)",
    "Scale Umbrellas",
    "Baseline",
)
ACTION_COLORS: dict[str, str] = {
    "Sun Protection (Hats/Shirts)": "#FFD166",
    "Scale Umbrellas (Residual Demand)": "#4dabf7",
    "Scale Umbrellas": "#339af0",
    "Baseline": "#868e96",
}
ACTION_RANK: dict[str, int] = {
    "Sun Protection (Hats/Shirts)": 4,
    "Scale Umbrellas (Residual Demand)": 3,
    "Scale Umbrellas": 2,
    "Baseline": 1,
}

AUDIT_PREVIEW_GLOB = "weather_pulse_audit_*_preview.csv"
AUDIT_REGION_FROM_NAME = re.compile(r"weather_pulse_audit_(?P<token>[a-z]+)_preview", re.I)
AUDIT_REGION_TOKEN: dict[str, str] = {"us": "US", "uk": "UK", "can": "CAN", "ca": "CAN"}

STATUS_HEX: dict[str, str] = {
    "aligned": "#51cf66",
    "mismatch": "#ff6b6b",
    "sales_only": "#63b5e6",
    "baseline_sales": "#adb5bd",
}

ACTION_TOOLTIPS: dict[str, str] = {
    "Sun Protection (Hats/Shirts)": (
        "Weather trigger: high UV at this hub.\n"
        "Recommended focus: hats, shirts, and sun-related SKUs."
    ),
    "Scale Umbrellas (Residual Demand)": (
        "Weather trigger: significant rain yesterday.\n"
        "Umbrella demand can linger even when today looks dry."
    ),
    "Scale Umbrellas": (
        "Weather trigger: forecast rain meets the QPF threshold.\n"
        "Recommended focus: push umbrellas and rain gear now."
    ),
    "Baseline": (
        "No weather threshold met at this hub.\n"
        "Standard marketing playbook — no special weather push."
    ),
}

OVERLAY_STATUS_TOOLTIPS: dict[str, str] = {
    "aligned": (
        "Green · Matched weather + execution\n"
        "The hub's weather suggested action matches the regional execution "
        "logged in the sales bridge."
    ),
    "mismatch": (
        "Red · Weather trigger, execution differs\n"
        "Weather recommended something other than Baseline, but regional "
        "execution was a different action. Worth reviewing."
    ),
    "sales_only": (
        "Blue · Sales recorded, no execution row\n"
        "City had orders on this date, but there is no regional Actual action "
        "in the bridge to compare against."
    ),
    "baseline_sales": (
        "Grey · Baseline weather + sales\n"
        "Weather at the hub was Baseline and sales exist, but no execution row "
        "is available. Not a mismatch — Baseline + Baseline execution would be green."
    ),
}


# ---------------------------------------------------------------------------
# Data layer
# ---------------------------------------------------------------------------


@st.cache_data(show_spinner="Loading bridge data…")
def load_bridge(bridge_mtime: float) -> pd.DataFrame:
    _ = bridge_mtime
    if not BRIDGE_PATH.is_file():
        return pd.DataFrame(columns=list(BRIDGE_COLUMNS))
    try:
        raw = pd.read_csv(BRIDGE_PATH, low_memory=False)
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame(columns=list(BRIDGE_COLUMNS))
    return clean_bridge(raw) if not raw.empty else pd.DataFrame(columns=list(BRIDGE_COLUMNS))


def bridge_mtime() -> float:
    return BRIDGE_PATH.stat().st_mtime if BRIDGE_PATH.is_file() else 0.0


def bridge_updated_label() -> str:
    if not BRIDGE_PATH.is_file():
        return "never"
    return datetime.fromtimestamp(BRIDGE_PATH.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")


def clean_bridge(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]
    for col in BRIDGE_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Region"] = df["Region"].astype(str).str.strip().str.upper()
    df = df[~df["Region"].isin(INVALID_REGIONS)]
    df["Type"] = df["Type"].astype(str).str.strip()
    df["Lat"] = pd.to_numeric(df["Lat"], errors="coerce")
    df["Lon"] = pd.to_numeric(df["Lon"], errors="coerce")
    df["Action"] = df["Value"].astype(str).str.strip()
    df.loc[~df["Type"].isin({SUGGESTED_TYPE, ACTUAL_TYPE}), "Action"] = ""
    return df[df["Date"].notna() & df["Type"].ne("")].copy()


def available_actions(df: pd.DataFrame) -> list[str]:
    actions = df.loc[df["Type"] == SUGGESTED_TYPE, "Action"].dropna().unique().tolist()
    ranked = sorted(
        [a for a in actions if a],
        key=lambda a: ACTION_RANK.get(a, 0),
        reverse=True,
    )
    for label in ACTION_ORDER:
        if label not in ranked:
            ranked.append(label)
    return list(dict.fromkeys(ranked))


def filter_base(
    df: pd.DataFrame,
    *,
    regions: list[str],
    dates: list[pd.Timestamp],
) -> pd.DataFrame:
    out = df.copy()
    if regions:
        out = out[out["Region"].isin(regions)]
    if dates:
        out = out[out["Date"].isin(dates)]
    return out


def sales_layer(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["Type"].isin(SALES_TYPES)].copy()


def strategy_layer(df: pd.DataFrame, actions: list[str]) -> pd.DataFrame:
    strat = df[df["Type"] == SUGGESTED_TYPE].copy()
    if actions:
        strat = strat[strat["Action"].isin(actions)]
    return strat


def weather_metrics_layer(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["Type"].isin(WEATHER_METRIC_TYPES)].copy()


def actual_layer(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["Type"] == ACTUAL_TYPE].copy()


def build_sales_by_date_region(df: pd.DataFrame) -> pd.DataFrame:
    rev = df[df["Type"] == "sales_revenue"].copy()
    if rev.empty:
        return pd.DataFrame(columns=["Date", "Region", "Revenue"])
    rev["Revenue"] = pd.to_numeric(rev["Value"], errors="coerce").fillna(0.0)
    return rev.groupby(["Date", "Region"], as_index=False)["Revenue"].sum().sort_values("Date")


def build_sales_by_date(df: pd.DataFrame) -> pd.DataFrame:
    daily = build_sales_by_date_region(df)
    if daily.empty:
        return pd.DataFrame(columns=["Date", "Revenue"])
    return daily.groupby("Date", as_index=False)["Revenue"].sum().sort_values("Date")


def build_strategy_counts(df: pd.DataFrame) -> pd.DataFrame:
    strat = df[df["Type"] == SUGGESTED_TYPE].copy()
    if strat.empty:
        return pd.DataFrame(columns=["Action", "Hubs"])
    counts = strat.groupby("Action", as_index=False).size().rename(columns={"size": "Hubs"})
    counts["sort"] = counts["Action"].map(lambda a: ACTION_RANK.get(a, 0))
    return counts.sort_values("sort", ascending=False).drop(columns="sort")


def build_focus_table(df: pd.DataFrame) -> pd.DataFrame:
    """Non-Baseline weather hubs — where marketing should concentrate."""
    strat = df[(df["Type"] == SUGGESTED_TYPE) & (df["Action"] != "Baseline")].copy()
    if strat.empty:
        return pd.DataFrame(columns=["Date", "Region", "Action", "Lat", "Lon"])
    return strat.sort_values(["Action", "Region"])[
        ["Date", "Region", "Action", "Lat", "Lon"]
    ]


def build_strategy_hub_table(strat: pd.DataFrame) -> pd.DataFrame:
    """Strategy hub list — same columns as the Focus tab table."""
    if strat.empty:
        return pd.DataFrame(columns=["Date", "Region", "Action", "Lat", "Lon"])
    return strat.sort_values(["Action", "Region"])[["Date", "Region", "Action", "Lat", "Lon"]].copy()


def build_dashboard_export(strat: pd.DataFrame, sales: pd.DataFrame) -> str:
    """CSV with strategy hubs + sales rows for current sidebar filters."""
    export_cols = ["Section", "Date", "Region", "Action", "Lat", "Lon", "Type", "Value"]
    parts: list[pd.DataFrame] = []

    hubs = build_strategy_hub_table(strat)
    if not hubs.empty:
        hub_rows = hubs.copy()
        hub_rows["Date"] = pd.to_datetime(hub_rows["Date"]).dt.strftime("%Y-%m-%d")
        hub_rows.insert(0, "Section", "strategy_hub")
        hub_rows["Type"] = ""
        hub_rows["Value"] = ""
        parts.append(hub_rows[export_cols])

    if not sales.empty:
        sales_rows = sales[["Date", "Region", "Type", "Value"]].copy()
        sales_rows["Date"] = pd.to_datetime(sales_rows["Date"]).dt.strftime("%Y-%m-%d")
        sales_rows.insert(0, "Section", "sales")
        sales_rows["Action"] = ""
        sales_rows["Lat"] = ""
        sales_rows["Lon"] = ""
        parts.append(sales_rows[export_cols])

    if not parts:
        return pd.DataFrame(columns=export_cols).to_csv(index=False)
    return pd.concat(parts, ignore_index=True).to_csv(index=False)


def export_filename(dates: list[pd.Timestamp]) -> str:
    if not dates:
        return "weather_pulse_export.csv"
    labels = sorted({d.strftime("%Y%m%d") for d in dates})
    if len(labels) == 1:
        return f"weather_pulse_export_{labels[0]}.csv"
    return f"weather_pulse_export_{labels[0]}_{labels[-1]}.csv"


def data_window_caption(df: pd.DataFrame) -> str:
    if df.empty:
        return "No bridge data loaded."
    all_dates = sorted(df["Date"].unique())
    min_date = pd.Timestamp(all_dates[0]).strftime("%Y-%m-%d")
    max_date = pd.Timestamp(all_dates[-1]).strftime("%Y-%m-%d")
    return (
        f"Dates in bridge: {min_date} → {max_date} · "
        f"Sales history from 2024 (omnichannel) + rolling archive · "
        f"Weather strategy: archived daily snapshots when available"
    )


def build_alignment(df: pd.DataFrame) -> pd.DataFrame:
    suggested = df[df["Type"] == SUGGESTED_TYPE].copy()
    actual = df[df["Type"] == ACTUAL_TYPE].copy()
    if suggested.empty or actual.empty:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []

    hub_actual = actual.dropna(subset=["Lat", "Lon"]).copy()
    if not hub_actual.empty:
        hub_suggested = suggested.dropna(subset=["Lat", "Lon"]).copy()
        hub_suggested["Lat"] = hub_suggested["Lat"].round(3)
        hub_suggested["Lon"] = hub_suggested["Lon"].round(3)
        hub_actual["Lat"] = hub_actual["Lat"].round(3)
        hub_actual["Lon"] = hub_actual["Lon"].round(3)
        hub_merge = hub_suggested.merge(
            hub_actual,
            on=["Date", "Region", "Lat", "Lon"],
            how="inner",
            suffixes=("_suggested", "_actual"),
        )
        if not hub_merge.empty:
            hub_merge = hub_merge.rename(columns={"Action_suggested": "Suggested", "Action_actual": "Actual"})
            hub_merge["aligned"] = hub_merge["Suggested"] == hub_merge["Actual"]
            hub_merge["autopilot"] = hub_merge["Suggested"].ne("Baseline") & hub_merge["Actual"].eq("Baseline")
            hub_merge["priority"] = hub_merge["Suggested"].map(lambda a: ACTION_RANK.get(a, 0))
            hub_merge["Match_Level"] = "hub"
            frames.append(hub_merge)

    regional = regional_actual_actions(df)
    if not regional.empty:
        regional_merge = suggested.merge(regional, on=["Date", "Region"], how="inner")
        if not regional_merge.empty:
            regional_merge = regional_merge.rename(columns={"Action": "Suggested"})
            regional_merge["aligned"] = regional_merge["Suggested"] == regional_merge["Actual"]
            regional_merge["autopilot"] = regional_merge["Suggested"].ne("Baseline") & regional_merge["Actual"].eq(
                "Baseline"
            )
            regional_merge["priority"] = regional_merge["Suggested"].map(lambda a: ACTION_RANK.get(a, 0))
            regional_merge["Match_Level"] = "regional"
            frames.append(regional_merge)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def prepare_strategy_map_data(strat: pd.DataFrame) -> pd.DataFrame:
    geo = strat.dropna(subset=["Lat", "Lon"]).copy()
    if geo.empty:
        return geo
    geo = geo.drop_duplicates(subset=["Region", "Lat", "Lon", "Action"])
    geo["Action"] = geo["Action"].astype(str)
    geo["Region"] = geo["Region"].astype(str)
    return geo


def audit_files_signature() -> str:
    paths = sorted((ROOT / "data").glob(AUDIT_PREVIEW_GLOB))
    parts = [f"{path.name}:{path.stat().st_mtime_ns}" for path in paths]
    if CITY_SALES_OVERLAY_PATH.is_file():
        parts.append(f"overlay:{CITY_SALES_OVERLAY_PATH.stat().st_mtime_ns}")
    if not parts:
        return "none"
    return "|".join(parts)


def _normalize_city_sales_raw(raw: pd.DataFrame, *, default_region: str = "") -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(
            columns=["Date", "Region", "City", "State", "Lat", "Lon", "Revenue", "Units", "Weather_Action"]
        )

    frame = raw.copy()
    if "Region" not in frame.columns and default_region:
        frame["Region"] = default_region
    frame["Date"] = pd.to_datetime(frame.get("Date"), errors="coerce")
    frame["Region"] = frame["Region"].astype(str).str.upper()
    frame["City"] = frame.get("City", frame.get("Shipping_City", pd.Series(dtype=str))).astype(str).str.strip()
    frame["State"] = frame.get("State", frame.get("Shipping_State", pd.Series(dtype=str))).astype(str).str.strip()
    frame["Lat"] = pd.to_numeric(frame.get("Lat"), errors="coerce")
    frame["Lon"] = pd.to_numeric(frame.get("Lon"), errors="coerce")
    frame["Revenue"] = pd.to_numeric(frame.get("Revenue", frame.get("Order_Revenue")), errors="coerce").fillna(0.0)
    frame["Units"] = pd.to_numeric(frame.get("Units", frame.get("Quantity_Sold")), errors="coerce").fillna(0.0)
    frame["Weather_Action"] = frame.get("Weather_Action", frame.get("Marketing_Action", pd.Series(dtype=str))).astype(
        str
    ).str.strip()
    frame = frame[frame["Date"].notna() & (frame["Revenue"] > 0)].copy()
    if frame.empty:
        return pd.DataFrame(
            columns=["Date", "Region", "City", "State", "Lat", "Lon", "Revenue", "Units", "Weather_Action"]
        )
    return frame[
        ["Date", "Region", "City", "State", "Lat", "Lon", "Revenue", "Units", "Weather_Action"]
    ].copy()


@st.cache_data(show_spinner=False)
def load_city_coordinates(_coords_mtime: float, _audit_sig: str) -> pd.DataFrame:
    _ = _audit_sig
    if not CITY_COORDS_PATH.is_file():
        return pd.DataFrame(columns=["Region", "State", "City", "Lat", "Lon"])
    payload = json.loads(CITY_COORDS_PATH.read_text(encoding="utf-8"))
    rows = payload.get("rows", [])
    if not rows:
        return pd.DataFrame(columns=["Region", "State", "City", "Lat", "Lon"])
    frame = pd.DataFrame(rows)
    frame["Region"] = frame["region"].astype(str).str.upper()
    frame["State"] = frame["state"].astype(str).str.strip()
    frame["City"] = frame["city"].astype(str).str.strip()
    frame["Lat"] = pd.to_numeric(frame["lat"], errors="coerce")
    frame["Lon"] = pd.to_numeric(frame["lon"], errors="coerce")
    frame["city_key"] = frame["City"].str.casefold() + "|" + frame["State"].str.casefold()
    return frame.dropna(subset=["Lat", "Lon"])[
        ["Region", "State", "City", "Lat", "Lon", "city_key"]
    ].drop_duplicates(subset=["Region", "city_key"])


@st.cache_data(show_spinner=False)
def load_audit_city_sales(_audit_sig: str) -> pd.DataFrame:
    _ = _audit_sig
    frames: list[pd.DataFrame] = []

    if CITY_SALES_OVERLAY_PATH.is_file():
        try:
            overlay_raw = pd.read_csv(CITY_SALES_OVERLAY_PATH, dtype=str, low_memory=False)
        except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError):
            overlay_raw = pd.DataFrame()
        overlay = _normalize_city_sales_raw(overlay_raw)
        if not overlay.empty:
            frames.append(overlay)

    if not frames:
        coords = load_city_coordinates(
            CITY_COORDS_PATH.stat().st_mtime if CITY_COORDS_PATH.is_file() else 0.0,
            _audit_sig,
        )
        for path in sorted((ROOT / "data").glob(AUDIT_PREVIEW_GLOB)):
            match = AUDIT_REGION_FROM_NAME.search(path.name)
            token = match.group("token") if match else path.stem
            region = AUDIT_REGION_TOKEN.get(token.lower(), token.upper())
            try:
                raw = pd.read_csv(path, dtype=str, low_memory=False)
            except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError):
                continue
            if raw.empty:
                continue

            parsed = _normalize_city_sales_raw(raw, default_region=region)
            if parsed.empty:
                continue
            parsed["city_key"] = parsed["City"].str.casefold() + "|" + parsed["State"].str.casefold()
            merged = parsed.merge(coords, on=["Region", "city_key"], how="inner", suffixes=("", "_coord"))
            if merged.empty:
                continue
            frames.append(
                merged.groupby(
                    ["Date", "Region", "City", "State", "Lat", "Lon", "Weather_Action"],
                    as_index=False,
                ).agg(Revenue=("Revenue", "sum"), Units=("Units", "sum"))
            )

    if not frames:
        return pd.DataFrame(
            columns=["Date", "Region", "City", "State", "Lat", "Lon", "Revenue", "Units", "Weather_Action"]
        )

    out = pd.concat(frames, ignore_index=True)
    out = (
        out.groupby(["Date", "Region", "City", "State", "Lat", "Lon", "Weather_Action"], as_index=False)
        .agg(Revenue=("Revenue", "sum"), Units=("Units", "sum"))
        .sort_values(["Date", "Revenue"], ascending=[True, False])
    )
    out["Lat"] = out["Lat"].round(3)
    out["Lon"] = out["Lon"].round(3)
    return out


def regional_actual_actions(base: pd.DataFrame) -> pd.DataFrame:
    actual = base[base["Type"] == ACTUAL_TYPE].copy()
    if actual.empty:
        return pd.DataFrame(columns=["Date", "Region", "Actual", "Actual_Orders"])
    actual["Action"] = actual["Action"].astype(str).str.strip()
    grouped = (
        actual.groupby(["Date", "Region"], as_index=False)
        .agg(
            Actual=("Action", lambda s: s.value_counts().index[0]),
            Actual_Orders=("Action", "size"),
        )
    )
    return grouped


def scale_revenue_pixels(revenue: pd.Series) -> pd.Series:
    """Pixel radius for sales dots (st.map meters are too subtle at country zoom)."""
    values = revenue.fillna(0.0)
    peak = float(values.max()) if len(values) else 0.0
    if peak <= 0:
        return pd.Series(22.0, index=values.index, dtype="float64")
    return 14.0 + (values / peak) * 22.0


def hex_to_rgba(hex_color: str, alpha: int = 215) -> list[int]:
    cleaned = str(hex_color).strip().lstrip("#")
    if len(cleaned) != 6:
        return [173, 181, 189, alpha]
    return [
        int(cleaned[0:2], 16),
        int(cleaned[2:4], 16),
        int(cleaned[4:6], 16),
        alpha,
    ]


def sales_overlay_status(row: pd.Series) -> str:
    suggested = str(row.get("Suggested", "")).strip()
    actual = row.get("Actual")
    if pd.isna(actual) or not str(actual).strip():
        return "baseline_sales" if suggested == "Baseline" else "sales_only"
    if suggested == str(actual).strip():
        return "aligned"
    return "mismatch"


def build_sales_overlay(
    base: pd.DataFrame,
    strat: pd.DataFrame,
    bridge_full: pd.DataFrame,
) -> tuple[pd.DataFrame, str]:
    """
    City sales for the live window, aligned to weather hubs when possible.

    Returns (overlay_frame, empty_reason). empty_reason is blank when rows exist.
    """
    if not CITY_SALES_OVERLAY_PATH.is_file():
        return pd.DataFrame(), "missing_overlay_file"

    city_sales = load_city_sales_overlay(sales_overlay_mtime())
    if city_sales.empty:
        return pd.DataFrame(), "empty_overlay_file"

    valid_regions = set(base["Region"].drop_duplicates())
    city_sales = city_sales[city_sales["Region"].isin(valid_regions)].copy()
    if city_sales.empty:
        return pd.DataFrame(), "no_regional_sales"

    # Sales overlay uses the live city-sales window — not limited to sidebar date picks.
    overlay_dates = set(city_sales["Date"].drop_duplicates())

    hubs_same_day = strat.dropna(subset=["Lat", "Lon"]).copy()
    hubs_same_day = hubs_same_day.rename(columns={"Action": "Suggested"})
    hubs_same_day["Lat"] = hubs_same_day["Lat"].round(3)
    hubs_same_day["Lon"] = hubs_same_day["Lon"].round(3)
    hub_keys = hubs_same_day[["Date", "Region", "Lat", "Lon", "Suggested"]].drop_duplicates(
        subset=["Date", "Region", "Lat", "Lon"]
    )

    overlay = city_sales.merge(hub_keys, on=["Date", "Region", "Lat", "Lon"], how="left")

    if overlay["Suggested"].isna().any():
        strat_all = strategy_layer(
            filter_base(bridge_full, regions=sorted(valid_regions), dates=[]),
            available_actions(bridge_full),
        )
        latest_hubs = (
            strat_all.dropna(subset=["Lat", "Lon"])
            .sort_values("Date")
            .drop_duplicates(subset=["Region", "Lat", "Lon"], keep="last")
            .rename(columns={"Action": "Suggested_Latest"})
        )
        latest_hubs["Lat"] = latest_hubs["Lat"].round(3)
        latest_hubs["Lon"] = latest_hubs["Lon"].round(3)
        overlay = overlay.merge(
            latest_hubs[["Region", "Lat", "Lon", "Suggested_Latest"]],
            on=["Region", "Lat", "Lon"],
            how="left",
        )
        overlay["Suggested"] = overlay["Suggested"].fillna(overlay["Suggested_Latest"])
        overlay = overlay.drop(columns=["Suggested_Latest"])

    overlay["Suggested"] = overlay["Suggested"].fillna(overlay["Weather_Action"]).fillna("Baseline")

    actual_base = filter_base(
        bridge_full,
        regions=sorted(valid_regions),
        dates=sorted(overlay_dates, key=lambda d: pd.Timestamp(d)),
    )
    overlay = overlay.merge(regional_actual_actions(actual_base), on=["Date", "Region"], how="left")
    overlay["Status"] = overlay.apply(sales_overlay_status, axis=1)
    overlay["Aligned"] = overlay["Status"] == "aligned"
    return overlay.sort_values("Revenue", ascending=False).reset_index(drop=True), ""


def build_combined_map_layers(geo: pd.DataFrame, sales: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split weather-only hubs and sales cities for a two-layer pydeck map."""
    sales_keys = {
        (str(region), round(float(lat), 3), round(float(lon), 3))
        for region, lat, lon in zip(sales["Region"], sales["Lat"], sales["Lon"], strict=True)
    }
    weather = geo.copy()
    weather["_key"] = weather.apply(
        lambda row: (str(row["Region"]), round(float(row["Lat"]), 3), round(float(row["Lon"]), 3)),
        axis=1,
    )
    weather_only = weather[~weather["_key"].isin(sales_keys)].copy()
    weather_pts = weather_only.assign(
        color_hex=weather_only["Action"].map(lambda action: ACTION_COLORS.get(str(action), "#adb5bd")),
    )
    weather_pts["color_rgba"] = weather_pts["color_hex"].map(hex_to_rgba)

    sales_pts = sales.assign(
        color_hex=sales["Status"].map(lambda status: STATUS_HEX.get(status, STATUS_HEX["sales_only"])),
        radius_px=scale_revenue_pixels(sales["Revenue"]),
    )
    sales_pts["color_rgba"] = sales_pts["color_hex"].map(hex_to_rgba)
    return weather_pts, sales_pts


def render_combined_pydeck_map(weather_pts: pd.DataFrame, sales_pts: pd.DataFrame) -> None:
    """Pixel-sized layers so weather-only hubs read clearly smaller than sales cities."""
    layers: list[pdk.Layer] = []
    if not weather_pts.empty:
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=weather_pts,
                get_position=["Lon", "Lat"],
                get_fill_color="color_rgba",
                get_radius=1,
                radius_min_pixels=5,
                radius_max_pixels=5,
                pickable=False,
            )
        )
    if not sales_pts.empty:
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=sales_pts,
                get_position=["Lon", "Lat"],
                get_fill_color="color_rgba",
                get_radius="radius_px",
                radius_min_pixels=14,
                radius_max_pixels=36,
                stroked=True,
                get_line_color=[255, 255, 255, 90],
                line_width_min_pixels=1,
                pickable=False,
            )
        )
    if not layers:
        st.info("No map points for current filters.")
        return

    combined = pd.concat(
        [
            weather_pts[["Lat", "Lon"]] if not weather_pts.empty else pd.DataFrame(columns=["Lat", "Lon"]),
            sales_pts[["Lat", "Lon"]] if not sales_pts.empty else pd.DataFrame(columns=["Lat", "Lon"]),
        ],
        ignore_index=True,
    )
    center_lat = float(combined["Lat"].mean())
    center_lon = float(combined["Lon"].mean())
    deck = pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(
            latitude=center_lat,
            longitude=center_lon,
            zoom=3.6,
            pitch=0,
        ),
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    )
    st.pydeck_chart(deck, use_container_width=True, height=520)


def render_legend_item(container, label: str, color: str, tooltip: str) -> None:
    tip = html.escape(tooltip, quote=True)
    safe_label = html.escape(label)
    container.markdown(
        (
            f'<span class="legend-tip" data-tip="{tip}">'
            f'<span style="color:{color};font-size:1.2em;">●</span> '
            f"<strong>{safe_label}</strong></span>"
        ),
        unsafe_allow_html=True,
    )


def render_action_legend(actions: list[str]) -> None:
    ordered = sorted(actions, key=lambda a: ACTION_RANK.get(a, 0), reverse=True)
    cols = st.columns(min(len(ordered), 4))
    for idx, action in enumerate(ordered):
        color = ACTION_COLORS.get(action, "#adb5bd")
        tooltip = ACTION_TOOLTIPS.get(
            action,
            "Recommended marketing action for this weather hub.",
        )
        render_legend_item(cols[idx % len(cols)], action, color, tooltip)


def render_sales_overlay_legend() -> None:
    labels = {
        "aligned": ("Matched weather + execution", "#51cf66"),
        "mismatch": ("Weather trigger, execution differs", "#ff6b6b"),
        "sales_only": ("Sales recorded, no execution row", "#63b5e6"),
        "baseline_sales": ("Baseline weather + sales", "#adb5bd"),
    }
    cols = st.columns(len(labels))
    for col, (key, (label, color)) in zip(cols, labels.items(), strict=True):
        render_legend_item(col, label, color, OVERLAY_STATUS_TOOLTIPS[key])


def action_color_scale(actions: list[str]) -> alt.Scale:
    domain = [a for a in ACTION_ORDER if a in actions] + [a for a in actions if a not in ACTION_ORDER]
    range_ = [ACTION_COLORS.get(a, "#adb5bd") for a in domain]
    return alt.Scale(domain=domain, range=range_)


# ---------------------------------------------------------------------------
# Visualization
# ---------------------------------------------------------------------------


def sales_overlay_mtime() -> float:
    return CITY_SALES_OVERLAY_PATH.stat().st_mtime if CITY_SALES_OVERLAY_PATH.is_file() else 0.0


@st.cache_data(show_spinner=False)
def load_city_sales_overlay(_mtime: float) -> pd.DataFrame:
    if not CITY_SALES_OVERLAY_PATH.is_file():
        return pd.DataFrame(
            columns=["Date", "Region", "City", "State", "Lat", "Lon", "Revenue", "Units", "Weather_Action"]
        )
    try:
        raw = pd.read_csv(CITY_SALES_OVERLAY_PATH, dtype=str, low_memory=False)
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame(
            columns=["Date", "Region", "City", "State", "Lat", "Lon", "Revenue", "Units", "Weather_Action"]
        )
    return _normalize_city_sales_raw(raw)


def sales_overlay_date_labels() -> list[str]:
    frame = load_city_sales_overlay(sales_overlay_mtime())
    if frame.empty:
        return []
    return sorted(pd.to_datetime(frame["Date"]).dt.strftime("%Y-%m-%d").unique().tolist())


def default_date_labels(df: pd.DataFrame, *, recent_days: int = 14) -> list[str]:
    """Default date pickers: weather snapshot days + live city-sales window."""
    all_dates = sorted(df["Date"].unique())
    if not all_dates:
        return []
    if len(all_dates) <= 31:
        return [d.strftime("%Y-%m-%d") for d in all_dates]

    strategy_labels = {
        pd.Timestamp(d).strftime("%Y-%m-%d")
        for d in df.loc[df["Type"] == SUGGESTED_TYPE, "Date"].dropna().unique()
    }
    sales_labels = set(sales_overlay_date_labels())
    latest = pd.Timestamp(all_dates[-1])
    cutoff = latest - pd.Timedelta(days=max(recent_days, 1) - 1)
    recent_labels = {pd.Timestamp(d).strftime("%Y-%m-%d") for d in all_dates if pd.Timestamp(d) >= cutoff}
    return sorted(strategy_labels | sales_labels | recent_labels)


def init_date_filter(df: pd.DataFrame) -> None:
    if "bridge_date_filter" not in st.session_state:
        st.session_state["bridge_date_filter"] = default_date_labels(df)


def inject_dark_theme() -> None:
    st.markdown(
        """
        <style>
        .stApp { background: #0b0f14; color: #e8eef5; }
        [data-testid="stSidebar"] { background: #111820; border-right: 1px solid #1e2a38; }
        div[data-testid="metric-container"] {
            background: #151c26; border: 1px solid #243041; border-radius: 10px; padding: 10px;
        }
        h1, h2, h3 { color: #f4f7fb !important; }
        .subtitle { color: #8fa3b8; margin-top: -0.6rem; }
        .legend-tip {
            position: relative;
            display: inline-block;
            cursor: help;
            border-bottom: 1px dotted #5c7089;
        }
        .legend-tip::after {
            content: attr(data-tip);
            position: absolute;
            left: 0;
            bottom: calc(100% + 8px);
            z-index: 1000;
            width: max-content;
            max-width: 320px;
            white-space: pre-wrap;
            padding: 10px 12px;
            background: #1a2433;
            border: 1px solid #3d5166;
            border-radius: 8px;
            color: #e8eef5;
            font-size: 0.85rem;
            line-height: 1.45;
            box-shadow: 0 8px 24px rgba(0, 0, 0, 0.45);
            opacity: 0;
            visibility: hidden;
            pointer-events: none;
            transition: opacity 0.15s ease;
        }
        .legend-tip:hover::after {
            opacity: 1;
            visibility: visible;
        }
        .hero-proof {
            font-size: clamp(1.35rem, 2.4vw, 2.1rem);
            font-weight: 700;
            line-height: 1.25;
            color: #f4f7fb;
            margin: 0.4rem 0 0.8rem 0;
            padding: 1.1rem 1.25rem;
            border-left: 4px solid #2f9e44;
            background: linear-gradient(90deg, #132018 0%, #0b0f14 70%);
            border-radius: 0 12px 12px 0;
        }
        .hero-muted { color: #8fa3b8; font-size: 0.95rem; margin-bottom: 1.2rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_filters(df: pd.DataFrame) -> tuple[list[str], list[pd.Timestamp], list[str], bool]:
    st.sidebar.header("Filters")
    regions = st.sidebar.multiselect("Region", sorted(df["Region"].unique()), default=sorted(df["Region"].unique()))
    date_labels = [d.strftime("%Y-%m-%d") for d in sorted(df["Date"].unique())]
    init_date_filter(df)
    preset_cols = st.sidebar.columns(3)
    if preset_cols[0].button("Strategy dates", help="Days with weather strategy snapshots"):
        st.session_state["bridge_date_filter"] = sorted(
            {
                pd.Timestamp(d).strftime("%Y-%m-%d")
                for d in df.loc[df["Type"] == SUGGESTED_TYPE, "Date"].dropna().unique()
            }
        )
        st.rerun()
    if preset_cols[1].button("Last 30d", help="Recent sales window"):
        latest = pd.Timestamp(sorted(df["Date"].unique())[-1])
        cutoff = latest - pd.Timedelta(days=29)
        st.session_state["bridge_date_filter"] = [
            d for d in date_labels if pd.Timestamp(d) >= cutoff
        ]
        st.rerun()
    if preset_cols[2].button("All dates", help="Full history (may be slow)"):
        st.session_state["bridge_date_filter"] = date_labels
        st.rerun()

    picked = st.sidebar.multiselect(
        "Date",
        date_labels,
        key="bridge_date_filter",
        help="Default: weather snapshot days + last 14 days of sales.",
    )
    if not picked:
        picked = default_date_labels(df)
    dates = [pd.Timestamp(d) for d in picked]
    st.sidebar.caption(data_window_caption(df))

    st.sidebar.divider()
    st.sidebar.subheader("Marketing actions to focus on")
    actions = available_actions(df)
    selected_actions = st.sidebar.multiselect(
        "Weather strategy hubs",
        options=actions,
        default=actions,
        help="Include Baseline to see all hubs. Active triggers = everything except Baseline.",
        key="weather_actions_multiselect",
    )
    if "Baseline" in actions and "Baseline" not in selected_actions:
        if st.sidebar.button("Include Baseline hubs"):
            st.session_state["weather_actions_multiselect"] = list(
                dict.fromkeys([*selected_actions, "Baseline"])
            )
            st.rerun()

    st.sidebar.divider()
    st.sidebar.subheader("Map layers")
    show_sales_overlay = st.sidebar.toggle(
        "Sales overlay",
        value=False,
        help="City sales for the live ~7-day window on the map. "
        "Works independently of the Date filter — weather dots still follow Date. "
        "Green = strategy matched execution; red = trigger but execution differed.",
    )
    if show_sales_overlay:
        overlay_dates = sales_overlay_date_labels()
        if overlay_dates:
            st.sidebar.caption(f"Sales overlay dates: {overlay_dates[0]} → {overlay_dates[-1]}")
        elif not CITY_SALES_OVERLAY_PATH.is_file():
            st.sidebar.warning("Missing data/city_sales_overlay.csv — run Intelligence Pipeline.")

    st.sidebar.caption(f"Updated {bridge_updated_label()}")
    if st.sidebar.button("Reload data"):
        st.cache_data.clear()
        st.rerun()

    return regions, dates, selected_actions, show_sales_overlay


def render_sidebar_export(strat: pd.DataFrame, sales: pd.DataFrame, dates: list[pd.Timestamp]) -> None:
    export_csv = build_dashboard_export(strat, sales)
    hub_count = len(build_strategy_hub_table(strat))
    sales_count = len(sales)
    st.sidebar.download_button(
        label="Export filtered CSV",
        data=export_csv,
        file_name=export_filename(dates),
        mime="text/csv",
        help="Strategy hub list + sales rows for the current Region / Date / Action filters.",
    )
    st.sidebar.caption(f"Export: {hub_count:,} strategy hubs · {sales_count:,} sales rows")


def render_action_kpis(base: pd.DataFrame, strat: pd.DataFrame, sales: pd.DataFrame) -> None:
    revenue = pd.to_numeric(sales.loc[sales["Type"] == "sales_revenue", "Value"], errors="coerce").sum()
    triggers = strat[strat["Action"] != "Baseline"]
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Sales revenue", f"${revenue:,.0f}")
    c2.metric("Baseline hubs", int((strat["Action"] == "Baseline").sum()))
    c3.metric("Weather triggers", f"{len(triggers):,}", help="Hubs where weather ≠ Baseline")
    c4.metric("Scale Umbrellas", int((strat["Action"] == "Scale Umbrellas").sum()))
    c5.metric("Umbrella residual", int((strat["Action"] == "Scale Umbrellas (Residual Demand)").sum()))
    c6.metric("Sun protection", int((strat["Action"] == "Sun Protection (Hats/Shirts)").sum()))


def render_strategy_map(
    strat: pd.DataFrame,
    *,
    all_actions: list[str],
    base: pd.DataFrame,
    bridge_full: pd.DataFrame,
    show_sales_overlay: bool,
) -> None:
    st.subheader("Weather strategy map")
    if show_sales_overlay:
        st.caption(
            "Dark basemap · **small dots (5px)** = weather-only hubs · "
            "**large dots (14–36px)** = city sales by order date (live ~7-day window, sized by revenue) · "
            "color = alignment. Execution is regional when ship-to is blocked."
        )
    else:
        st.caption("OpenStreetMap · point color = recommended marketing action · gray = Baseline")

    if "Baseline" in all_actions and "Baseline" not in strat["Action"].values:
        st.warning(
            "Baseline hubs are hidden by the sidebar filter. "
            "Click **Include Baseline hubs** in the sidebar, or add **Baseline** to the multiselect."
        )

    geo = prepare_strategy_map_data(strat)
    if geo.empty:
        st.info("No strategy coordinates for current filters.")
        return

    render_action_legend(sorted(geo["Action"].unique(), key=lambda a: ACTION_RANK.get(a, 0), reverse=True))

    if show_sales_overlay:
        sales, empty_reason = build_sales_overlay(base, strat, bridge_full)
        if sales.empty:
            if empty_reason == "missing_overlay_file":
                st.info(
                    "City sales overlay file is missing. Re-run **Intelligence Pipeline** on GitHub "
                    "so `data/city_sales_overlay.csv` is committed."
                )
            elif empty_reason == "empty_overlay_file":
                st.info(
                    "`data/city_sales_overlay.csv` is present but has no usable rows. "
                    "Re-run **Intelligence Pipeline** to regenerate it."
                )
            else:
                st.info("No city-level sales in the overlay for the selected regions.")
            show_sales_overlay = False
        else:
            overlay_range = (
                f"{pd.Timestamp(sales['Date'].min()).strftime('%Y-%m-%d')} → "
                f"{pd.Timestamp(sales['Date'].max()).strftime('%Y-%m-%d')}"
            )
            st.caption(f"Sales overlay showing {len(sales):,} city rows ({overlay_range}).")
            render_sales_overlay_legend()
            aligned = int(sales["Aligned"].sum())
            mismatches = int((sales["Status"] == "mismatch").sum())
            c1, c2, c3 = st.columns(3)
            c1.metric(
                "Sales cities",
                f"{len(sales):,}",
                help="City-level order locations in the live sales overlay window (~7 days).",
            )
            c2.metric(
                "Matched actions",
                f"{aligned:,}",
                help="Sales cities where regional execution matched the hub's weather suggested action.",
            )
            c3.metric(
                "Weather vs execution gaps",
                f"{mismatches:,}",
                help="Sales cities where weather triggered a non-Baseline action but regional execution differed.",
            )

            combined_weather, combined_sales = build_combined_map_layers(geo, sales)
            render_combined_pydeck_map(combined_weather, combined_sales)
            table = sales[
                ["Date", "Region", "City", "State", "Suggested", "Actual", "Revenue", "Units", "Status"]
            ].copy()
            table["Date"] = pd.to_datetime(table["Date"]).dt.strftime("%Y-%m-%d")
            table["Revenue"] = table["Revenue"].map(lambda value: f"${value:,.2f}")
            st.dataframe(table.head(100), use_container_width=True, hide_index=True)
            return

    geo["color_hex"] = geo["Action"].map(lambda a: ACTION_COLORS.get(str(a), "#adb5bd"))
    geo = geo.assign(_rank=geo["Action"].map(lambda a: ACTION_RANK.get(a, 0)))
    geo = geo.sort_values("_rank", ascending=True).drop(columns="_rank").reset_index(drop=True)
    st.map(
        geo,
        latitude="Lat",
        longitude="Lon",
        color="color_hex",
        size=120,
    )


def render_strategy_breakdown(strat: pd.DataFrame) -> None:
    st.subheader("Strategy mix — where to concentrate")
    counts = build_strategy_counts(strat)
    if counts.empty:
        st.info("No strategy rows.")
        return
    chart = (
        alt.Chart(counts)
        .mark_bar()
        .encode(
            x=alt.X("Hubs:Q", title="City hubs"),
            y=alt.Y("Action:N", sort=list(ACTION_ORDER), title="Marketing action"),
            color=alt.Color("Action:N", scale=action_color_scale(counts["Action"].tolist()), legend=None),
            tooltip=["Action:N", "Hubs:Q"],
        )
        .properties(height=260)
    )
    st.altair_chart(chart, use_container_width=True)


def render_sales_panel(sales: pd.DataFrame) -> None:
    st.subheader("Sales layer")
    st.caption("Triple Whale revenue — separate from weather strategy points")
    by_region = build_sales_by_date_region(sales)
    if by_region.empty:
        st.info("No sales_revenue rows for current filters.")
        return

    chart = (
        alt.Chart(by_region)
        .mark_line(point=True)
        .encode(
            x=alt.X("Date:T", title="Date"),
            y=alt.Y("Revenue:Q", title="Order revenue ($)"),
            color="Region:N",
            tooltip=["Date:T", "Region:N", "Revenue:Q"],
        )
        .properties(height=300)
    )
    st.altair_chart(chart, use_container_width=True)

    total_by_region = by_region.groupby("Region", as_index=False)["Revenue"].sum().sort_values("Revenue", ascending=False)
    st.dataframe(total_by_region, use_container_width=True, hide_index=True)


def render_focus_table(strat: pd.DataFrame) -> None:
    st.subheader("Strategy hub list")
    st.caption("All selected marketing actions including Baseline")
    table = build_strategy_hub_table(strat)
    if table.empty:
        st.info("No strategy rows for current filters.")
        return
    st.dataframe(table, use_container_width=True, hide_index=True, height=320)


def render_alignment_panel(base: pd.DataFrame) -> None:
    st.subheader("Strategy vs execution alignment")
    st.caption(
        "Hub weather strategy compared to marketing execution. "
        "Ship-to is blocked in Triple Whale, so execution is matched at region level."
    )
    suggested = base[base["Type"] == SUGGESTED_TYPE]
    actual = base[base["Type"] == ACTUAL_TYPE]
    if suggested.empty:
        st.warning("No weather strategy rows for current filters. Pick a strategy snapshot date (e.g. 2026-06-02).")
        return
    if actual.empty:
        st.warning(
            "No execution rows for current filters. actual_action comes from the live sales bridge "
            "(recent dates only). Try **Last 30d** or a recent date."
        )
        return

    alignment = build_alignment(base)
    if alignment.empty:
        st.warning("Could not match strategy and execution for the selected filters.")
        return

    by_action = (
        alignment.groupby("Suggested", as_index=False)
        .agg(total=("aligned", "count"), aligned=("aligned", "sum"), autopilot=("autopilot", "sum"))
        .assign(
            gap=lambda d: d["total"] - d["aligned"],
            aligned_pct=lambda d: (d["aligned"] / d["total"] * 100).round(1),
        )
        .sort_values("aligned_pct")
    )

    c1, c2 = st.columns([1, 1])
    with c1:
        chart = (
            alt.Chart(by_action)
            .mark_bar()
            .encode(
                x=alt.X("total:Q", title="Matched rows"),
                y=alt.Y("Suggested:N", sort=list(ACTION_ORDER), title="Suggested action"),
                color=alt.Color("Suggested:N", scale=action_color_scale(by_action["Suggested"].tolist()), legend=None),
                tooltip=["Suggested:N", "total:Q", "aligned:Q", "gap:Q", "aligned_pct:Q"],
            )
            .properties(height=280)
        )
        st.altair_chart(chart, use_container_width=True)
    with c2:
        st.metric("Overall alignment", f"{alignment['aligned'].mean() * 100:.1f}%")
        st.metric("Autopilot risk", f"{alignment['autopilot'].mean() * 100:.1f}%", help="Weather triggered but execution stayed Baseline or mismatched")
        gaps = alignment[~alignment["aligned"]].sort_values("priority", ascending=False)
        if not gaps.empty:
            gap_cols = ["Date", "Region", "Suggested", "Actual", "Match_Level", "Lat", "Lon"]
            gap_cols = [col for col in gap_cols if col in gaps.columns]
            st.dataframe(
                gaps[gap_cols].head(100),
                use_container_width=True,
                hide_index=True,
            )



# ---------------------------------------------------------------------------
# Phase 1 — Historical proof UI
# ---------------------------------------------------------------------------


def verification_mtime() -> float:
    return VERIFICATION_PATH.stat().st_mtime if VERIFICATION_PATH.is_file() else 0.0


def verification_updated_label() -> str:
    if not VERIFICATION_PATH.is_file():
        return "never"
    return datetime.fromtimestamp(VERIFICATION_PATH.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")


@st.cache_data(show_spinner="Loading verification bridge…")
def load_verification(mtime: float) -> pd.DataFrame:
    _ = mtime
    if not VERIFICATION_PATH.is_file():
        return pd.DataFrame(columns=list(VERIFICATION_COLUMNS))
    try:
        raw = pd.read_csv(VERIFICATION_PATH, low_memory=False)
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame(columns=list(VERIFICATION_COLUMNS))
    if raw.empty:
        return pd.DataFrame(columns=list(VERIFICATION_COLUMNS))

    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]
    for col in VERIFICATION_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Region"] = df["Region"].astype(str).str.strip().str.upper()
    df = df[~df["Region"].isin(INVALID_REGIONS)]
    for col in (
        "Lat",
        "Lon",
        "Umbrella_Revenue",
        "Umbrella_Units",
        "Non_Umbrella_Revenue",
        "Non_Umbrella_Units",
        "Observed_Precip_In",
    ):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["Is_Rainy"] = df["Is_Rainy"].astype(str).str.strip().str.lower().isin({"1", "true", "yes"})
    if "Verified_Match" in df.columns:
        df["Verified_Match"] = (
            df["Verified_Match"].astype(str).str.strip().str.lower().isin({"1", "true", "yes"})
        )
    else:
        df["Verified_Match"] = (df["Umbrella_Revenue"] > 0) & df["Is_Rainy"]
    return df[df["Date"].notna()].copy()


def filter_verification(
    df: pd.DataFrame,
    *,
    regions: list[str],
    date_start: pd.Timestamp | None,
    date_end: pd.Timestamp | None,
) -> pd.DataFrame:
    out = df.copy()
    if regions:
        out = out[out["Region"].isin(regions)]
    if date_start is not None:
        out = out[out["Date"] >= date_start]
    if date_end is not None:
        out = out[out["Date"] <= date_end]
    return out


def proof_metrics(df: pd.DataFrame) -> dict[str, float]:
    umbrella_rev = float(pd.to_numeric(df["Umbrella_Revenue"], errors="coerce").fillna(0).sum())
    rainy_rev = float(
        pd.to_numeric(df.loc[df["Is_Rainy"], "Umbrella_Revenue"], errors="coerce").fillna(0).sum()
    )
    dry_rev = float(
        pd.to_numeric(df.loc[~df["Is_Rainy"], "Umbrella_Revenue"], errors="coerce").fillna(0).sum()
    )
    pct = (100.0 * rainy_rev / umbrella_rev) if umbrella_rev > 0 else 0.0

    rainy_days = max(int(df.loc[df["Is_Rainy"]].shape[0]), 0)
    dry_days = max(int(df.loc[~df["Is_Rainy"]].shape[0]), 0)
    rainy_avg = (rainy_rev / rainy_days) if rainy_days else 0.0
    dry_avg = (dry_rev / dry_days) if dry_days else 0.0
    lift = (rainy_avg / dry_avg) if dry_avg > 0 else (float("inf") if rainy_avg > 0 else 0.0)

    return {
        "umbrella_rev": umbrella_rev,
        "rainy_rev": rainy_rev,
        "dry_rev": dry_rev,
        "pct_on_rainy": pct,
        "rainy_days": float(rainy_days),
        "dry_days": float(dry_days),
        "rainy_avg": rainy_avg,
        "dry_avg": dry_avg,
        "lift": lift if lift != float("inf") else 0.0,
        "lift_infinite": float(lift == float("inf")),
    }


def build_rainy_dry_chart(df: pd.DataFrame) -> alt.Chart:
    rainy_rev = float(df.loc[df["Is_Rainy"], "Umbrella_Revenue"].sum())
    dry_rev = float(df.loc[~df["Is_Rainy"], "Umbrella_Revenue"].sum())
    chart_df = pd.DataFrame(
        {
            "Day type": ["Rainy day", "Dry day"],
            "Umbrella revenue": [rainy_rev, dry_rev],
            "Color": [RAINY_COLOR, DRY_COLOR],
        }
    )
    return (
        alt.Chart(chart_df)
        .mark_bar(size=60, cornerRadiusEnd=4)
        .encode(
            x=alt.X("Day type:N", title=None, sort=["Rainy day", "Dry day"]),
            y=alt.Y("Umbrella revenue:Q", title="Umbrella revenue ($)"),
            color=alt.Color("Color:N", scale=None, legend=None),
            tooltip=[
                alt.Tooltip("Day type:N"),
                alt.Tooltip("Umbrella revenue:Q", format="$,.0f"),
            ],
        )
        .properties(height=280, title="Umbrella revenue: rainy vs dry (same city, same day)")
    )


def build_lift_chart(metrics: dict[str, float]) -> alt.Chart:
    chart_df = pd.DataFrame(
        {
            "Day type": ["Rainy day", "Dry day"],
            "Avg umbrella $ / city-day": [metrics["rainy_avg"], metrics["dry_avg"]],
            "Color": [RAINY_COLOR, DRY_COLOR],
        }
    )
    return (
        alt.Chart(chart_df)
        .mark_bar(size=60, cornerRadiusEnd=4)
        .encode(
            x=alt.X("Day type:N", title=None, sort=["Rainy day", "Dry day"]),
            y=alt.Y("Avg umbrella $ / city-day:Q", title="Avg umbrella $ per city-day"),
            color=alt.Color("Color:N", scale=None, legend=None),
            tooltip=[
                alt.Tooltip("Day type:N"),
                alt.Tooltip("Avg umbrella $ / city-day:Q", format="$,.2f"),
            ],
        )
        .properties(height=280, title="Umbrella lift (avg $ on rainy vs dry city-days)")
    )


def render_verification_map(df: pd.DataFrame) -> None:
    st.subheader("Verified historical map")
    st.caption(
        "Green = umbrella sold on an observed rainy day (same city, same day). "
        "Gray = umbrella sold on a dry day."
    )
    geo = df.dropna(subset=["Lat", "Lon"]).copy()
    geo = geo[geo["Umbrella_Revenue"] > 0]
    if geo.empty:
        st.info("No umbrella city-days with coordinates for the current filters.")
        return

    agg = (
        geo.groupby(["Region", "City", "State", "Lat", "Lon"], as_index=False)
        .agg(
            Umbrella_Revenue=("Umbrella_Revenue", "sum"),
            Verified_Match=("Verified_Match", "max"),
        )
    )

    agg["color_hex"] = agg["Verified_Match"].map(
        lambda matched: VERIFY_MATCH_COLOR if matched else VERIFY_DRY_COLOR
    )
    rev = agg["Umbrella_Revenue"].clip(lower=0)
    agg["size"] = (40 + 200 * (rev / rev.max())).astype(int) if rev.max() > 0 else 60

    cols = st.columns(2)
    cols[0].markdown(
        f'<span style="color:{VERIFY_MATCH_COLOR};font-size:1.2em;">●</span> '
        "**Umbrella + observed rain**",
        unsafe_allow_html=True,
    )
    cols[1].markdown(
        f'<span style="color:{VERIFY_DRY_COLOR};font-size:1.2em;">●</span> '
        "**Umbrella on dry day**",
        unsafe_allow_html=True,
    )

    map_df = agg.rename(columns={"Lat": "lat", "Lon": "lon"})
    try:
        st.map(
            map_df,
            latitude="lat",
            longitude="lon",
            size="size",
            color="color_hex",
            zoom=3,
        )
    except TypeError:
        st.map(map_df[["lat", "lon"]])

    st.dataframe(
        agg.sort_values("Umbrella_Revenue", ascending=False)[
            ["Region", "City", "State", "Umbrella_Revenue", "Verified_Match"]
        ].head(50),
        use_container_width=True,
        hide_index=True,
    )


def render_phase1_sidebar(df: pd.DataFrame) -> tuple[list[str], pd.Timestamp | None, pd.Timestamp | None]:
    st.sidebar.header("Phase 1 filters")
    regions_available = sorted(df["Region"].dropna().unique().tolist())
    regions = st.sidebar.multiselect(
        "Region",
        options=regions_available,
        default=regions_available,
        key="phase1_regions",
    )
    min_d = pd.Timestamp(df["Date"].min())
    max_d = pd.Timestamp(df["Date"].max())
    picked = st.sidebar.date_input(
        "Date range",
        value=(min_d.date(), max_d.date()),
        min_value=min_d.date(),
        max_value=max_d.date(),
        key="phase1_dates",
    )
    if isinstance(picked, tuple) and len(picked) == 2:
        date_start = pd.Timestamp(picked[0])
        date_end = pd.Timestamp(picked[1])
    else:
        date_start, date_end = min_d, max_d

    st.sidebar.caption(f"Verification updated {verification_updated_label()}")
    if st.sidebar.button("Reload data", key="phase1_reload"):
        st.cache_data.clear()
        st.rerun()
    return regions, date_start, date_end


def render_phase1_export(df: pd.DataFrame) -> None:
    export_df = df.copy()
    export_df["Date"] = pd.to_datetime(export_df["Date"]).dt.strftime("%Y-%m-%d")
    csv_bytes = export_df.to_csv(index=False)
    st.sidebar.download_button(
        label="Export verification CSV",
        data=csv_bytes,
        file_name="historic_verification_bridge_export.csv",
        mime="text/csv",
        help="City-day umbrella sales matched to observed Open-Meteo archive rain.",
        key="phase1_export",
    )
    st.sidebar.caption(f"{len(export_df):,} city-day rows in export")


def render_phase1(df: pd.DataFrame) -> None:
    regions, date_start, date_end = render_phase1_sidebar(df)
    filtered = filter_verification(df, regions=regions, date_start=date_start, date_end=date_end)
    render_phase1_export(filtered)

    if filtered.empty:
        st.warning("No verification rows match the current filters.")
        return

    metrics = proof_metrics(filtered)
    pct = metrics["pct_on_rainy"]
    st.markdown(
        f'<div class="hero-proof">'
        f"{pct:.0f}% of umbrella sales happened on rainy days "
        f"(same city, same day)."
        f"</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        '<p class="hero-muted">'
        "Observed historical rain from Open-Meteo archive · matched to umbrella SKU sales "
        "on the exact ship-to city and date · US / UK / CAN."
        "</p>",
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Umbrella revenue", f"${metrics['umbrella_rev']:,.0f}")
    c2.metric("On rainy days", f"${metrics['rainy_rev']:,.0f}")
    c3.metric("On dry days", f"${metrics['dry_rev']:,.0f}")
    if metrics["lift_infinite"]:
        c4.metric("Rainy / dry lift", "∞", help="No dry-day umbrella revenue in filter window.")
    else:
        c4.metric(
            "Rainy / dry lift",
            f"{metrics['lift']:.2f}×",
            help="Avg umbrella $ per rainy city-day ÷ avg on dry city-days.",
        )

    left, right = st.columns(2)
    with left:
        st.altair_chart(build_rainy_dry_chart(filtered), use_container_width=True)
    with right:
        st.altair_chart(build_lift_chart(metrics), use_container_width=True)

    render_verification_map(filtered)


# ---------------------------------------------------------------------------
# Phase 2 — Forecast triggers, ad-spend allocation, execution alignment
# ---------------------------------------------------------------------------


def forecast_mtime() -> float:
    return FORECAST_PATH.stat().st_mtime if FORECAST_PATH.is_file() else 0.0


def forecast_updated_label() -> str:
    if not FORECAST_PATH.is_file():
        return "never"
    return datetime.fromtimestamp(FORECAST_PATH.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")


@st.cache_data(show_spinner="Loading forecast bridge…")
def load_forecast(mtime: float) -> pd.DataFrame:
    _ = mtime
    empty = pd.DataFrame(columns=list(FORECAST_COLUMNS))
    if not FORECAST_PATH.is_file():
        return empty
    try:
        raw = pd.read_csv(FORECAST_PATH, low_memory=False)
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError):
        return empty
    if raw.empty:
        return empty

    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]
    for col in FORECAST_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Region"] = df["Region"].astype(str).str.strip().str.upper()
    df = df[~df["Region"].isin(INVALID_REGIONS)]
    for col in (
        "Lat",
        "Lon",
        "Forecast_Precip_In",
        "Forecast_UV",
        "Hist_Umbrella_Revenue",
        "Priority_Score",
        "Spend_Share",
    ):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["Action"] = df["Action"].astype(str).str.strip()
    return df[df["Date"].notna()].copy()


def build_allocation_plan(df: pd.DataFrame, budget: float) -> pd.DataFrame:
    """Triggered cities only, re-normalized to the selected slice, with dollar amounts."""
    plan = df[df["Action"].ne("Baseline") & (df["Priority_Score"] > 0)].copy()
    if plan.empty:
        plan["Spend"] = pd.Series(dtype=float)
        plan["Directive"] = pd.Series(dtype=str)
        plan["Allocation"] = pd.Series(dtype=float)
        return plan
    total = float(plan["Priority_Score"].sum())
    plan["Allocation"] = plan["Priority_Score"] / total if total > 0 else 0.0
    plan["Spend"] = (plan["Allocation"] * budget).round(2)
    plan["Directive"] = plan.apply(
        lambda r: f"{ACTION_IMPERATIVE.get(r['Action'], r['Action'])} in {r['City']}, {r['State']}",
        axis=1,
    )
    return plan.sort_values("Spend", ascending=False).reset_index(drop=True)


def render_forecast_map(df: pd.DataFrame) -> None:
    st.subheader("Forecast trigger map")
    st.caption("Point color = forecast action · size = allocated spend share.")
    geo = df.dropna(subset=["Lat", "Lon"]).copy()
    if geo.empty:
        st.info("No forecast coordinates for current filters.")
        return

    geo["color_hex"] = geo["Action"].map(lambda a: ACTION_COLORS.get(str(a), "#adb5bd"))
    share = geo["Spend_Share"].clip(lower=0)
    geo["size"] = (50 + 250 * (share / share.max())).astype(int) if share.max() > 0 else 60

    render_action_legend(sorted(geo["Action"].unique(), key=lambda a: ACTION_RANK.get(a, 0), reverse=True))
    try:
        st.map(
            geo.rename(columns={"Lat": "lat", "Lon": "lon"}),
            latitude="lat",
            longitude="lon",
            size="size",
            color="color_hex",
            zoom=3,
        )
    except TypeError:
        st.map(geo.rename(columns={"Lat": "lat", "Lon": "lon"})[["lat", "lon"]])


def render_gap_visualizer(bridge: pd.DataFrame) -> None:
    """Suggested weather strategy vs actual marketing execution — compliance + autopilot risk."""
    st.subheader("Alignment & gap visualizer")
    st.caption(
        "Weather-driven suggestion vs what marketing actually executed. "
        "Autopilot risk = weather triggered but execution stayed Baseline or mismatched."
    )
    if bridge.empty:
        st.info("No historic_sales_bridge.csv yet. Run: python run_intel_pipeline.py")
        return

    if bridge[bridge["Type"] == ACTUAL_TYPE].empty:
        st.warning(
            "**No execution data recorded yet.** The sales bridge has weather suggestions but "
            "zero `actual_action` rows, so compliance and autopilot risk cannot be measured."
        )
        st.markdown(
            "Feed execution one of two ways:\n"
            "1. Populate `Marketing_Action` in the Triple Whale sync output, or\n"
            "2. Drop a `marketing_execution*.csv` (`Date`, `Region`, `Actual_Action`) into the repo root "
            "and re-run `python pipeline.py`."
        )
        suggested = bridge[bridge["Type"] == SUGGESTED_TYPE]
        if not suggested.empty:
            st.caption(
                f"Ready to compare as soon as execution lands: {len(suggested):,} suggestion rows "
                f"across {suggested['Date'].nunique()} days."
            )
        return

    regions = sorted(bridge["Region"].dropna().unique().tolist())
    picked_regions = st.multiselect(
        "Region", options=regions, default=regions, key="gap_regions"
    )
    date_labels = [d.strftime("%Y-%m-%d") for d in sorted(bridge["Date"].dropna().unique())]
    window = st.slider(
        "Recent days to audit",
        min_value=1,
        max_value=min(90, max(len(date_labels), 1)),
        value=min(30, max(len(date_labels), 1)),
        key="gap_window",
    )
    dates = [pd.Timestamp(d) for d in date_labels[-window:]]
    base = filter_base(bridge, regions=picked_regions, dates=dates)
    if base.empty:
        st.warning("No rows in the selected audit window.")
        return

    alignment = build_alignment(base)
    if alignment.empty:
        st.warning(
            "Could not match strategy to execution in this window. "
            "Execution rows (actual_action) only exist for recent live sales dates."
        )
        return

    aligned_pct = alignment["aligned"].mean() * 100
    autopilot_pct = alignment["autopilot"].mean() * 100
    gaps = alignment[~alignment["aligned"]].sort_values("priority", ascending=False)

    c1, c2, c3 = st.columns(3)
    c1.metric("Execution alignment", f"{aligned_pct:.1f}%")
    c2.metric("Autopilot risk", f"{autopilot_pct:.1f}%")
    c3.metric("Open gaps", f"{len(gaps):,}")

    by_action = (
        alignment.groupby("Suggested", as_index=False)
        .agg(total=("aligned", "count"), aligned=("aligned", "sum"))
        .assign(
            gap=lambda d: d["total"] - d["aligned"],
            aligned_pct=lambda d: (d["aligned"] / d["total"] * 100).round(1),
        )
        .sort_values("aligned_pct")
    )
    chart = (
        alt.Chart(by_action)
        .mark_bar()
        .encode(
            x=alt.X("aligned_pct:Q", title="Aligned (%)", scale=alt.Scale(domain=[0, 100])),
            y=alt.Y("Suggested:N", sort=list(ACTION_ORDER), title=None),
            color=alt.Color(
                "Suggested:N",
                scale=action_color_scale(by_action["Suggested"].tolist()),
                legend=None,
            ),
            tooltip=["Suggested:N", "total:Q", "aligned:Q", "gap:Q", "aligned_pct:Q"],
        )
        .properties(height=240)
    )
    st.altair_chart(chart, use_container_width=True)

    if not gaps.empty:
        gap_cols = [c for c in ("Date", "Region", "Suggested", "Actual", "Match_Level") if c in gaps.columns]
        st.dataframe(gaps[gap_cols].head(100), use_container_width=True, hide_index=True)


def render_phase2(forecast: pd.DataFrame, bridge: pd.DataFrame) -> None:
    if forecast.empty:
        st.info("Build the Phase 2 forecast bridge first:")
        st.code("python pipeline.py\nstreamlit run dashboard.py", language="bash")
        st.caption(f"Expected file: `{FORECAST_PATH.name}`")
        render_gap_visualizer(bridge)
        return

    date_labels = [d.strftime("%Y-%m-%d") for d in sorted(forecast["Date"].unique())]
    regions = sorted(forecast["Region"].dropna().unique().tolist())

    c1, c2, c3 = st.columns([2, 2, 1])
    picked_dates = c1.multiselect(
        "Forecast days",
        options=date_labels,
        default=date_labels[:2],
        key="p2_dates",
        help="Default: today + tomorrow. Add days to plan the full 5-day window.",
    )
    picked_regions = c2.multiselect("Region", options=regions, default=regions, key="p2_regions")
    budget = c3.number_input(
        "Daily ad budget ($)", min_value=0.0, value=5000.0, step=500.0, key="p2_budget"
    )

    view = forecast[
        forecast["Date"].isin([pd.Timestamp(d) for d in (picked_dates or date_labels)])
        & forecast["Region"].isin(picked_regions or regions)
    ].copy()
    if view.empty:
        st.warning("No forecast rows match the current filters.")
        return

    plan = build_allocation_plan(view, budget * max(len(picked_dates or date_labels), 1))
    umbrella_cities = int((view["Action"] == "Scale Umbrellas").sum())
    sun_cities = int((view["Action"] == "Sun Protection (Hats/Shirts)").sum())

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Triggered city-days", f"{len(plan):,}")
    k2.metric("Scale Umbrellas", f"{umbrella_cities:,}")
    k3.metric("Sun Protection", f"{sun_cities:,}")
    k4.metric("Budget allocated", f"${plan['Spend'].sum():,.0f}" if not plan.empty else "$0")

    st.subheader("Today's execution list")
    if plan.empty:
        st.info("No weather triggers in this window — hold Baseline everywhere.")
    else:
        table = plan[
            ["Date", "Directive", "Region", "Forecast_Precip_In", "Forecast_UV", "Spend", "Trigger_Reason"]
        ].copy()
        table["Date"] = table["Date"].dt.strftime("%Y-%m-%d")
        table = table.rename(
            columns={
                "Forecast_Precip_In": "Rain (in)",
                "Forecast_UV": "UV",
                "Spend": "Ad spend ($)",
                "Trigger_Reason": "Why",
            }
        )
        st.dataframe(
            table,
            use_container_width=True,
            hide_index=True,
            height=420,
            column_config={
                "Ad spend ($)": st.column_config.NumberColumn(format="$%.2f"),
                "Rain (in)": st.column_config.NumberColumn(format="%.2f"),
                "UV": st.column_config.NumberColumn(format="%.1f"),
            },
        )
        st.download_button(
            "Export today's ad plan (CSV)",
            data=table.to_csv(index=False),
            file_name="forecast_ad_plan.csv",
            mime="text/csv",
            key="p2_export",
        )

    render_forecast_map(view)
    st.divider()
    render_gap_visualizer(bridge)



def main() -> None:
    st.set_page_config(page_title="Weather Pulse", page_icon="☔", layout="wide")
    inject_dark_theme()

    st.title("Weather Pulse")
    st.markdown(
        '<p class="subtitle">Proof that weather drives sales — and where to spend against tomorrow.</p>',
        unsafe_allow_html=True,
    )

    verification = load_verification(verification_mtime())
    forecast = load_forecast(forecast_mtime())
    bridge = load_bridge(bridge_mtime())

    tab_proof, tab_spend = st.tabs(
        ["Phase 1 – Historical Proof", "Phase 2 – Forecast & Ad Spend"]
    )

    with tab_proof:
        if verification.empty:
            st.info("Build the Phase 1 verification bridge first:")
            st.code(
                "python pipeline.py\n# or\npython run_intel_pipeline.py\nstreamlit run dashboard.py",
                language="bash",
            )
            st.caption(f"Expected file: `{VERIFICATION_PATH.name}`")
        else:
            render_phase1(verification)

    with tab_spend:
        st.caption(f"Forecast updated {forecast_updated_label()}")
        render_phase2(forecast, bridge)


if __name__ == "__main__":
    main()
