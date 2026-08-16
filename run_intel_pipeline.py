#!/usr/bin/env python3
"""
One-command decision-intelligence refresh.

  python run_intel_pipeline.py

Steps (fully automated):
  1. Weather Pulse  → marketing_weather_report_{us,uk,can}.csv
  2. Triple Whale     → live sales + weather join (sync_sales_bridge --dry-run)
  3. ETL              → historic_sales_bridge.csv + historic_verification_bridge.csv
                       + forecast_ad_spend_bridge.csv
  4. Dashboard        → streamlit run dashboard.py  (launch separately or use --serve)
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VENV_PYTHON = ROOT / ".venv" / "bin" / "python"
PYTHON = str(VENV_PYTHON if VENV_PYTHON.is_file() else sys.executable)

WEATHER_SCRIPT = ROOT / "weatherman_weather_pulse.py"
SALES_SCRIPT = ROOT / "sync_sales_bridge.py"
PIPELINE_SCRIPT = ROOT / "pipeline.py"
DASHBOARD_SCRIPT = ROOT / "dashboard.py"

RAW_DATA_DIR = ROOT / "raw_data"
MASTER_PREVIEW = ROOT / "data" / "weather_pulse_sales_bridge_master_preview.csv"
RAW_SALES_OUT = RAW_DATA_DIR / "sales_data_bridge.csv"
BRIDGE_OUT = ROOT / "historic_sales_bridge.csv"
VERIFICATION_OUT = ROOT / "historic_verification_bridge.csv"
FORECAST_OUT = ROOT / "forecast_ad_spend_bridge.csv"


def log(msg: str) -> None:
    print(f"[intel] {msg}", flush=True)


def run_step(label: str, cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    log(f"── {label} ──")
    merged_env = os.environ.copy()
    merged_env.pop("GOOGLE_CREDS_JSON", None)
    if env:
        merged_env.update(env)
    result = subprocess.run(cmd, cwd=ROOT, env=merged_env)
    if result.returncode != 0:
        raise RuntimeError(f"{label} failed (exit {result.returncode})")


def stage_sales_for_pipeline() -> None:
    if not MASTER_PREVIEW.is_file():
        raise RuntimeError(
            f"Expected sales preview at {MASTER_PREVIEW} after sync_sales_bridge --dry-run."
        )
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy2(MASTER_PREVIEW, RAW_SALES_OUT)
    log(f"Staged sales → {RAW_SALES_OUT.relative_to(ROOT)}")


def run_intel_pipeline(*, skip_weather: bool = False, skip_sales: bool = False) -> Path:
    log("Starting automated intelligence pipeline…")

    if not skip_weather:
        run_step("Weather Pulse", [PYTHON, str(WEATHER_SCRIPT)])
        run_step(
            "Archive weather snapshot",
            [
                PYTHON,
                "-c",
                "from history_archive import archive_weather_reports; archive_weather_reports()",
            ],
        )
    else:
        log("Skipping weather step and history archive (--skip-weather).")

    if not skip_sales:
        run_step(
            "Triple Whale sales + weather join",
            [PYTHON, str(SALES_SCRIPT), "--dry-run"],
        )
        stage_sales_for_pipeline()
    else:
        log("Skipping sales step (--skip-sales).")

    run_step("ETL bridge build", [PYTHON, str(PIPELINE_SCRIPT)])

    if not BRIDGE_OUT.is_file():
        raise RuntimeError(f"Pipeline did not produce {BRIDGE_OUT.name}.")

    if VERIFICATION_OUT.is_file():
        log(
            f"Phase 1 verification ready: {VERIFICATION_OUT} "
            f"({VERIFICATION_OUT.stat().st_size:,} bytes)"
        )
    else:
        log(f"WARN: pipeline did not produce {VERIFICATION_OUT.name}")

    if FORECAST_OUT.is_file():
        log(
            f"Phase 2 forecast ready: {FORECAST_OUT} "
            f"({FORECAST_OUT.stat().st_size:,} bytes)"
        )
    else:
        log(f"WARN: pipeline did not produce {FORECAST_OUT.name}")

    log(f"Done. Bridge ready: {BRIDGE_OUT} ({BRIDGE_OUT.stat().st_size:,} bytes)")
    log("Launch dashboard:  streamlit run dashboard.py")
    return BRIDGE_OUT


def serve_dashboard() -> None:
    run_step("Streamlit dashboard", [PYTHON, "-m", "streamlit", "run", str(DASHBOARD_SCRIPT)])


def main() -> int:
    parser = argparse.ArgumentParser(description="Automated weather + sales + ETL pipeline.")
    parser.add_argument("--skip-weather", action="store_true", help="Skip weather pulse step.")
    parser.add_argument("--skip-sales", action="store_true", help="Skip Triple Whale sales step.")
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Launch Streamlit dashboard after pipeline completes.",
    )
    args = parser.parse_args()

    try:
        run_intel_pipeline(skip_weather=args.skip_weather, skip_sales=args.skip_sales)
        if args.serve:
            serve_dashboard()
    except Exception as exc:
        log(f"ERROR: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
