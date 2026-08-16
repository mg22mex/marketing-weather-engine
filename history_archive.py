#!/usr/bin/env python3
"""
Daily history archives for sales + weather strategy snapshots.

Committed under data/history/:
  sales_ledger.csv          — rolling archive of live Triple Whale rows (city-distributed)
  weather/YYYY-MM-DD/*.csv  — daily marketing_weather_report_* copies

Bulk pre-live history comes from data/cleaned_omnichannel_sales.csv (loaded by sync_sales_bridge).
"""

from __future__ import annotations

import os
import re
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
HISTORY_DIR = ROOT / "data" / "history"
SALES_LEDGER_PATH = HISTORY_DIR / "sales_ledger.csv"
WEATHER_HISTORY_DIR = HISTORY_DIR / "weather"
OMNICHANNEL_HISTORICAL_PATH = ROOT / "data" / "cleaned_omnichannel_sales.csv"

MARKETING_REPORTS: tuple[Path, ...] = (
    ROOT / "marketing_weather_report_us.csv",
    ROOT / "marketing_weather_report_uk.csv",
    ROOT / "marketing_weather_report_can.csv",
)

WEATHER_HISTORY_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _archive_disabled() -> bool:
    return os.getenv("DISABLE_HISTORY_ARCHIVE", "").strip().lower() in {"1", "true", "yes"}


def weather_snapshot_date(path: Path) -> str | None:
    """Parse YYYY-MM-DD from data/history/weather/YYYY-MM-DD/… paths."""
    parts = path.parts
    for idx, part in enumerate(parts):
        if part == "weather" and idx + 1 < len(parts):
            candidate = parts[idx + 1]
            if WEATHER_HISTORY_DATE.fullmatch(candidate):
                return candidate
    return None


def archive_weather_reports(snapshot_date: date | None = None) -> list[Path]:
    """Copy today's regional weather CSVs into data/history/weather/YYYY-MM-DD/."""
    if _archive_disabled():
        return []

    snap = snapshot_date or date.today()
    dest_dir = WEATHER_HISTORY_DIR / snap.isoformat()
    dest_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    for src in MARKETING_REPORTS:
        if not src.is_file():
            continue
        dest = dest_dir / src.name
        if dest.is_file() and dest.read_bytes() == src.read_bytes():
            continue
        shutil.copy2(src, dest)
        written.append(dest)

    if written:
        print(
            f"[history] Weather snapshot {snap.isoformat()}: "
            f"archived {len(written)} file(s) → {dest_dir.relative_to(ROOT)}"
        )
    return written


def upsert_sales_ledger(frame: pd.DataFrame) -> Path | None:
    """Merge sales rows into data/history/sales_ledger.csv (deduped by line keys)."""
    if _archive_disabled() or frame.empty:
        return None

    import sync_sales_bridge as sb

    incoming = sb._normalize_historical_sales_frame(frame)
    if incoming.empty:
        return None

    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    if SALES_LEDGER_PATH.is_file():
        existing = sb._normalize_historical_sales_frame(pd.read_csv(SALES_LEDGER_PATH, dtype=str))
        merged = pd.concat([existing, incoming], ignore_index=True)
    else:
        merged = incoming

    merged = sb._normalize_historical_sales_frame(merged)
    merged.to_csv(SALES_LEDGER_PATH, index=False)
    print(
        f"[history] Sales ledger updated: {len(merged):,} rows → "
        f"{SALES_LEDGER_PATH.relative_to(ROOT)}"
    )
    return SALES_LEDGER_PATH


def persist_live_sales_archive(live: pd.DataFrame, *, live_start: date | None = None) -> Path | None:
    """Append the latest live Triple Whale window into the committed sales ledger."""
    if live.empty:
        return None
    cutoff = live_start
    if cutoff is None:
        lookback = int(os.getenv("LIVE_LOOKBACK_DAYS", "7") or "7")
        cutoff = date.today() - timedelta(days=max(lookback, 1) - 1)
    # Archive the full live pull; combine_sales_streams dedupes vs ledger at read time.
    _ = cutoff
    return upsert_sales_ledger(live)


def bootstrap_sales_ledger_from_omnichannel(*, before_date: date | None = None) -> Path | None:
    """
    One-time seed of sales_ledger.csv from cleaned_omnichannel_sales.csv
    for dates strictly before the live window (avoids duplicating recent TW pulls).
    """
    if _archive_disabled() or not OMNICHANNEL_HISTORICAL_PATH.is_file():
        return None
    if SALES_LEDGER_PATH.is_file() and SALES_LEDGER_PATH.stat().st_size > 0:
        return SALES_LEDGER_PATH

    import sync_sales_bridge as sb

    raw = pd.read_csv(OMNICHANNEL_HISTORICAL_PATH, dtype=str)
    frame = sb._normalize_omnichannel_historical_frame(raw)
    if before_date is not None:
        cutoff = before_date.strftime("%Y-%m-%d")
        frame = frame[frame["Date"] < cutoff].copy()

    if frame.empty:
        return None

    return upsert_sales_ledger(frame)


def bootstrap_sales_ledger_from_staged(*, before_date: date | None = None) -> Path | None:
    """Fallback seed from raw_data/sales_data_bridge.csv when omnichannel is absent."""
    if SALES_LEDGER_PATH.is_file() and SALES_LEDGER_PATH.stat().st_size > 0:
        return SALES_LEDGER_PATH

    staged = ROOT / "raw_data" / "sales_data_bridge.csv"
    if not staged.is_file():
        return None

    import sync_sales_bridge as sb

    raw = pd.read_csv(staged, dtype=str)
    frame = sb._normalize_historical_sales_frame(raw)
    if before_date is not None:
        cutoff = before_date.strftime("%Y-%m-%d")
        frame = frame[frame["Date"] < cutoff].copy()
    if frame.empty:
        return None
    return upsert_sales_ledger(frame)


def ensure_sales_history_seeded(live_start: date) -> None:
    """Populate committed ledger on first run so CI/dashboard have pre-window history."""
    if SALES_LEDGER_PATH.is_file() and SALES_LEDGER_PATH.stat().st_size > 0:
        return
    path = bootstrap_sales_ledger_from_omnichannel(before_date=live_start)
    if path is None:
        bootstrap_sales_ledger_from_staged(before_date=live_start)


def historical_source_paths() -> list[Path]:
    """Extra committed historical CSV paths (checked before live window merge)."""
    paths: list[Path] = []
    if SALES_LEDGER_PATH.is_file():
        paths.append(SALES_LEDGER_PATH)
    if OMNICHANNEL_HISTORICAL_PATH.is_file():
        paths.append(OMNICHANNEL_HISTORICAL_PATH)
    return paths


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Bootstrap or refresh data/history archives.")
    parser.add_argument(
        "--bootstrap-sales",
        action="store_true",
        help="Seed sales_ledger.csv from omnichannel / staged sales (before live window).",
    )
    parser.add_argument(
        "--archive-weather",
        action="store_true",
        help="Archive current marketing_weather_report_*.csv into data/history/weather/.",
    )
    parser.add_argument("--date", type=str, default="", help="Snapshot date YYYY-MM-DD (weather).")
    args = parser.parse_args()

    snap = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today()

    if args.bootstrap_sales:
        import sync_sales_bridge as sb

        live_start, _ = sb._resolve_live_window(date.today())
        ensure_sales_history_seeded(live_start)
        print(f"[history] Sales ledger ready at {SALES_LEDGER_PATH}")

    if args.archive_weather:
        archive_weather_reports(snapshot_date=snap)

    if not args.bootstrap_sales and not args.archive_weather:
        parser.print_help()
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
