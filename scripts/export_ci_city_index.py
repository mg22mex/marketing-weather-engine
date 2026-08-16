#!/usr/bin/env python3
"""
Build committed city index files for GitHub Actions (no orders_by_city.csv in repo).

Requires local data/orders_by_city.csv (gitignored). Outputs:
  data/city_framework.csv
  data/city_distribution_weights_state.csv
  data/city_distribution_weights_country.csv

Usage:  python scripts/export_ci_city_index.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import sync_sales_bridge as bridge

OUT_FRAMEWORK = ROOT / "data" / "city_framework.csv"
OUT_STATE = ROOT / "data" / "city_distribution_weights_state.csv"
OUT_COUNTRY = ROOT / "data" / "city_distribution_weights_country.csv"


def main() -> int:
    if not bridge.ORDERS_BY_CITY_PATH.is_file():
        print(f"Missing {bridge.ORDERS_BY_CITY_PATH}. Place orders_by_city.csv first.", file=sys.stderr)
        return 1

    frames = [bridge.load_city_framework_matrix(c) for c in ("US", "UK", "CA")]
    framework = __import__("pandas").concat(frames, ignore_index=True)
    state_w, country_w = bridge.load_city_distribution_weights()

    OUT_FRAMEWORK.parent.mkdir(parents=True, exist_ok=True)
    framework.to_csv(OUT_FRAMEWORK, index=False)
    state_w.to_csv(OUT_STATE, index=False)
    country_w.to_csv(OUT_COUNTRY, index=False)

    print(f"Wrote {OUT_FRAMEWORK.name}: {len(framework):,} rows")
    print(f"Wrote {OUT_STATE.name}: {len(state_w):,} rows")
    print(f"Wrote {OUT_COUNTRY.name}: {len(country_w):,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
