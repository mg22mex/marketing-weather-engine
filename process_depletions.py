#!/usr/bin/env python3
"""
Flatten the Weatherman IP Tracker multi-channel depletion export for Looker Studio.

Raw layout (IP Tracker export):
  Row 0: preamble (ignored)
  Row N-1: years (forward-filled across month columns from column index 6)
  Row N: metadata headers + month abbreviations (Jan, Feb, ...)
  Row N+1+: product rows

Output: data/cleaned_omnichannel_sales.csv (tidy long format)
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
DEFAULT_INPUT = ROOT.parent / "Weatherman IP Tracker US - 5_25_26 - Shopify Depletion.csv"
OUTPUT_PATH = ROOT / "data" / "cleaned_omnichannel_sales.csv"

METADATA_COLUMNS = ["3PL-SKU", "Old SKU", "Style", "Category", "Color", "Status"]
METADATA_START = 0
METADATA_END = 6  # exclusive; monthly depletion begins at index 6
MONTH_NAMES = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}


def _normalize_year(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", ""}:
        return None
    try:
        return str(int(float(text)))
    except ValueError:
        return text


def _find_header_row(raw: pd.DataFrame) -> int:
    for idx in range(raw.shape[0]):
        if str(raw.iloc[idx, METADATA_START]).strip() == METADATA_COLUMNS[0]:
            return idx
    raise ValueError(
        f"Could not find metadata header row starting with {METADATA_COLUMNS[0]!r}."
    )


def _build_headers(raw: pd.DataFrame) -> tuple[list[str], int]:
    header_idx = _find_header_row(raw)
    if header_idx < 1:
        raise ValueError("Input file must contain a year row above the metadata header row.")

    year_row = raw.iloc[header_idx - 1, METADATA_END:].copy()
    year_row = year_row.map(_normalize_year).ffill()

    month_row = raw.iloc[header_idx, METADATA_END:].astype(str).str.strip()
    meta_headers = [str(v).strip() for v in raw.iloc[header_idx, METADATA_START:METADATA_END]]

    if meta_headers != METADATA_COLUMNS:
        raise ValueError(
            f"Unexpected metadata headers in row {header_idx + 1}: {meta_headers!r} "
            f"(expected {METADATA_COLUMNS!r})"
        )

    month_headers: list[str] = []
    started = False
    for year, month in zip(year_row, month_row, strict=False):
        if month not in MONTH_NAMES or year is None:
            if started:
                break
            month_headers.append("_drop")
            continue
        started = True
        month_headers.append(f"{year}-{month}")

    return meta_headers + month_headers, header_idx + 1


def process_depletions(input_path: Path, output_path: Path = OUTPUT_PATH) -> pd.DataFrame:
    if not input_path.is_file():
        raise FileNotFoundError(f"Raw depletion file not found: {input_path}")

    raw = pd.read_csv(input_path, header=None, dtype=object)
    headers, data_start = _build_headers(raw)

    body = raw.iloc[data_start:, : len(headers)].copy()
    body.columns = headers
    body = body.reset_index(drop=True)

    month_cols = [c for c in body.columns if c not in METADATA_COLUMNS and c != "_drop"]
    body = body.drop(columns=[c for c in body.columns if c == "_drop"], errors="ignore")

    long_df = body.melt(
        id_vars=METADATA_COLUMNS,
        value_vars=month_cols,
        var_name="Date_Raw",
        value_name="Units_Sold",
    )

    long_df["Date"] = pd.to_datetime(
        long_df["Date_Raw"] + "-01",
        format="%Y-%b-%d",
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")

    long_df["Units_Sold"] = pd.to_numeric(long_df["Units_Sold"], errors="coerce").fillna(0).astype(int)
    long_df = long_df.drop(columns=["Date_Raw"])
    long_df = long_df[long_df["Date"].notna()].copy()

    for col in METADATA_COLUMNS:
        long_df[col] = long_df[col].astype(str).str.strip()

    long_df = long_df[
        METADATA_COLUMNS + ["Date", "Units_Sold"]
    ].sort_values(METADATA_COLUMNS + ["Date"]).reset_index(drop=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    long_df.to_csv(output_path, index=False)
    print(f"[INFO] Wrote {len(long_df):,} rows to {output_path}")
    return long_df


def git_commit_and_push(output_path: Path = OUTPUT_PATH) -> None:
    """Stage cleaned output, commit if changed, rebase, and push to origin/main."""
    rel = output_path.relative_to(ROOT)
    subprocess.run(["git", "add", str(rel)], cwd=ROOT, check=True)

    diff = subprocess.run(
        ["git", "diff", "--staged", "--quiet"],
        cwd=ROOT,
    )
    if diff.returncode == 0:
        print("[INFO] No changes to commit.")
        return

    msg = f"Auto-update: cleaned omnichannel depletion ({output_path.name})"
    subprocess.run(["git", "commit", "-m", msg], cwd=ROOT, check=True)

    subprocess.run(["git", "fetch", "origin", "main"], cwd=ROOT, check=True)
    subprocess.run(["git", "pull", "--rebase", "origin", "main"], cwd=ROOT, check=True)
    subprocess.run(["git", "push", "origin", "main"], cwd=ROOT, check=True)
    print("[INFO] Committed and pushed to origin/main.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Flatten IP Tracker depletion CSV for Looker Studio.")
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Path to raw export (default: {DEFAULT_INPUT.name})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_PATH,
        help=f"Path for cleaned CSV (default: {OUTPUT_PATH.relative_to(ROOT)})",
    )
    parser.add_argument(
        "--no-git",
        action="store_true",
        help="Skip git add/commit/pull --rebase/push after writing output.",
    )
    args = parser.parse_args()

    try:
        process_depletions(args.input, args.output)
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1

    if not args.no_git:
        try:
            git_commit_and_push(args.output)
        except subprocess.CalledProcessError as exc:
            print(f"[ERROR] Git automation failed: {exc}", file=sys.stderr)
            return exc.returncode or 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
