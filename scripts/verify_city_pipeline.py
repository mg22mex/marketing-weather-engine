#!/usr/bin/env python3
"""
Offline checks: orders rollup → aggregation/picks (same logic as generate_city_coordinates),
plus structure checks on data/city_coordinates.json and weatherman_weather_pulse import.

Run from repo root:
  python scripts/verify_city_pipeline.py
"""

from __future__ import annotations

import importlib.util
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _load_gen():
    path = ROOT / "scripts" / "generate_city_coordinates.py"
    spec = importlib.util.spec_from_file_location("gen_city", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    g = _load_gen()
    errors: list[str] = []

    if not g.ORDERS_CSV.is_file():
        print("FAIL: missing", g.ORDERS_CSV)
        return 1

    rows = g._load_orders()
    if not rows or "total_orders" not in rows[0]:
        errors.append("orders CSV missing or no total_orders column")

    us_agg = g._aggregate_us(rows)
    uk_agg = g._aggregate_uk(rows)
    ca_agg = g._aggregate_ca(rows)

    # DC: no non-Washington keys after aggregation
    dc_keys = set(us_agg.get("DC", {}).keys())
    if any("WASHINGTON" not in k for k in dc_keys):
        errors.append(f"DC aggregation should only keep WASHINGTON* keys, got {dc_keys}")
    if "BOGOTA" in str(dc_keys).upper() or "BOGOT" in str(dc_keys).upper():
        errors.append("DC keys should not include Bogotá-like tokens")

    # All 50 states + DC present in picks (even if orders empty, fallback fills)
    picks_us = g._pick_us_cities_per_state(us_agg, max_cities=5, min_cities=3)
    by_abbrev: dict[str, list] = defaultdict(list)
    for abbrev, city, _ in picks_us:
        by_abbrev[abbrev].append(city)
    if set(by_abbrev.keys()) != g.US_ABBREVS:
        missing = g.US_ABBREVS - set(by_abbrev.keys())
        extra = set(by_abbrev.keys()) - g.US_ABBREVS
        errors.append(f"US pick set mismatch. Missing {missing!r} extra {extra!r}")

    for abbrev, cities in by_abbrev.items():
        n = len(cities)
        if abbrev == "DC":
            if n != 1 or cities[0].lower() != "washington":
                errors.append(f"DC expected single Washington pick, got {cities!r}")
        else:
            if n < 3 or n > 5:
                errors.append(f"{abbrev}: expected 3–5 cities, got {n}: {cities!r}")

    # NI: Londonderry merged into Derry key — no standalone LONDONDERRY in aggregates
    nir = uk_agg.get("NIR", {})
    if "LONDONDERRY" in nir:
        errors.append("NIR aggregate should merge Londonderry into DERRY, not keep LONDONDERRY")

    picks_uk = g._pick_uk_cities(uk_agg, per_nation=8)
    for code in g.UK_CODE_TO_REGION:
        n = len([p for p in picks_uk if p[0] == code])
        if n < 1 or n > 8:
            errors.append(f"UK {code}: expected 1–8 picks, got {n}")

    picks_ca = g._pick_ca_cities(ca_agg, per_province=5)
    for abbrev in g.CAN_ABBREV_TO_REGION:
        n = len([p for p in picks_ca if p[0] == abbrev])
        if n < 1 or n > 5:
            errors.append(f"CA {abbrev}: expected 1–5 picks, got {n}")

    # city_coordinates.json
    jpath = ROOT / "data" / "city_coordinates.json"
    if not jpath.is_file():
        errors.append(f"missing {jpath}")
    else:
        payload = json.loads(jpath.read_text(encoding="utf-8"))
        jr = payload.get("rows", [])
        if not jr:
            errors.append("city_coordinates.json has no rows")
        for i, r in enumerate(jr):
            for k in ("region", "state", "city", "lat", "lon"):
                if k not in r:
                    errors.append(f"row {i} missing {k}")
            try:
                lat, lon = float(r["lat"]), float(r["lon"])
                if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                    errors.append(f"row {i} bad lat/lon {lat} {lon}")
            except (TypeError, ValueError):
                errors.append(f"row {i} non-numeric lat/lon")

        us_states = {r["state"] for r in jr if r.get("region") == "US"}
        if len(us_states) != 51:
            errors.append(f"JSON US unique states expected 51, got {len(us_states)}")

        uk_n = sum(1 for r in jr if r.get("region") == "UK")
        ca_n = sum(1 for r in jr if r.get("region") == "CA")
        if uk_n < 4:
            errors.append(f"JSON UK rows suspiciously low: {uk_n}")
        if ca_n < 10:
            errors.append(f"JSON CA rows suspiciously low: {ca_n}")

        print("city_coordinates.json:", Counter(r["region"] for r in jr), "total", len(jr))

    # Pulse module loads
    sys.path.insert(0, str(ROOT))
    import weatherman_weather_pulse as w

    n = len(w.CITY_ROWS)
    if n < 300:
        errors.append(f"CITY_ROWS count unexpectedly low: {n}")
    print("weatherman_weather_pulse.CITY_ROWS:", n)

    if errors:
        print("\nFAILURES:")
        for e in errors:
            print(" ", e)
        return 1

    print("\nAll checks passed.")
    print("  US picks:", len(picks_us), "| UK picks:", len(picks_uk), "| CA picks:", len(picks_ca))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
