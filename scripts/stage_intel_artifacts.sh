#!/usr/bin/env bash
# Stage lean intelligence-pipeline artifacts for git.
# Never stages blobs at/over GitHub's hard 100MB limit (safe ceiling 95MB).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

MAX_BYTES=$((95 * 1024 * 1024))

stage_if_safe() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    return 0
  fi
  local size
  size=$(wc -c <"$path" | tr -d ' ')
  if (( size > MAX_BYTES )); then
    echo "SKIP oversized (>$MAX_BYTES bytes): $path ($size bytes)"
    # Drop from index if previously tracked — full SKU ledger lives in Sheets.
    git rm --cached -f --ignore-unmatch -- "$path" >/dev/null 2>&1 || true
    return 0
  fi
  git add -- "$path"
  echo "STAGE $path ($size bytes)"
}

# Always untrack full-SKU ledgers (grow past 100MB; Sheets owns the full copy).
for heavy in \
  data/weather_pulse_sales_bridge_master_preview.csv \
  data/weather_pulse_sales_bridge_us_preview.csv \
  data/weather_pulse_sales_bridge_preview.csv \
  data/history/sales_ledger.csv \
  raw_data/sales_data_bridge.csv
do
  if git ls-files --error-unmatch -- "$heavy" >/dev/null 2>&1; then
    echo "UNTRACK $heavy (full SKU / local-only)"
    git rm --cached -f --ignore-unmatch -- "$heavy" >/dev/null 2>&1 || true
  fi
done

# Lean dashboard + ops artifacts (Phase 1 / Phase 2 + audit / city index)
for path in \
  historic_sales_bridge.csv \
  historic_verification_bridge.csv \
  forecast_ad_spend_bridge.csv \
  data/city_sales_overlay.csv \
  data/city_framework.csv \
  data/city_distribution_weights_state.csv \
  data/city_distribution_weights_country.csv \
  data/weather_pulse_audit_us_preview.csv \
  data/weather_pulse_audit_uk_preview.csv \
  data/weather_pulse_audit_ca_preview.csv \
  data/weather_pulse_sales_bridge_uk_preview.csv \
  data/weather_pulse_sales_bridge_ca_preview.csv
do
  stage_if_safe "$path"
done

# Also stage any future small audit previews by size gate (not master/us SKU dumps).
shopt -s nullglob
for path in data/weather_pulse_audit_*_preview.csv; do
  stage_if_safe "$path"
done
shopt -u nullglob
