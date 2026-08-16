#!/usr/bin/env python3
"""
Pull live omnichannel order line data from Triple Whale and flatten it for Looker Studio.

Uses the Orcabase SQL API against orders_table (line items via ARRAY JOIN on products_info).
Aggregates at Date + Shipping_Country + Platform + Source_Name + SKU — no city column.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

ROOT = Path(__file__).resolve().parent
OUTPUT_PATH = ROOT / "data" / "cleaned_omnichannel_sales.csv"

SHOP_ID = "4a3474-24.myshopify.com"
API_BASE = "https://api.triplewhale.com/api/v2"
SQL_ENDPOINT = f"{API_BASE}/orcabase/api/sql"
START_DATE = date(2024, 1, 1)

OUTPUT_COLUMNS = [
    "Date",
    "Shipping_Country",
    "Platform",
    "Source_Name",
    "SKU",
    "Units_Sold",
    "Order_Revenue",
]

SALES_QUERY = """
SELECT
  event_date AS date,
  shipping_country_code AS shipping_country,
  lower(platform) AS platform,
  source_name AS source_name,
  product.product_sku AS sku,
  sum(product.product_name_quantity_sold) AS units_sold,
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
  shipping_country_code,
  platform,
  source_name,
  product.product_sku
"""


def _load_dotenv() -> None:
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _require_api_key() -> str:
    _load_dotenv()
    api_key = os.getenv("TRIPLE_WHALE_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "Missing TRIPLE_WHALE_API_KEY. Set it in the environment or in .env "
            "(see .env.example)."
        )
    return api_key


def _normalize_country(code: object) -> str:
    text = str(code or "").strip().upper()
    if not text or text.lower() == "nan":
        return ""
    if text == "GB":
        return "UK"
    return text


def _normalize_platform(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text or text == "nan":
        return ""
    if "amazon" in text:
        return "amazon"
    if "shopify" in text:
        return "shopify"
    return text


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


def _extract_sql_rows(body: object) -> list[dict]:
    """Normalize Triple Whale SQL responses (list or {success, data})."""
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
        "period": {
            "startDate": start.isoformat(),
            "endDate": end.isoformat(),
        },
    }
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    for attempt in range(max_retries):
        response = session.post(SQL_ENDPOINT, json=payload, headers=headers, timeout=120)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", min(32, 2 ** attempt)))
            print(f"[WARN] Rate limited; retrying in {retry_after}s...")
            time.sleep(retry_after)
            continue
        if response.status_code >= 500:
            delay = min(32, 2 ** attempt)
            print(f"[WARN] Server error {response.status_code}; retrying in {delay}s...")
            time.sleep(delay)
            continue

        try:
            body = response.json()
        except ValueError as exc:
            raise RuntimeError(f"Triple Whale returned non-JSON response: {response.text[:500]}") from exc

        if not response.ok:
            message = body.get("message") if isinstance(body, dict) else response.text
            raise RuntimeError(f"Triple Whale SQL API error ({response.status_code}): {message}")

        return _extract_sql_rows(body)

    raise RuntimeError(f"Triple Whale SQL API failed after {max_retries} attempts.")


def fetch_sales_rows(api_key: str, start: date, end: date) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    session = requests.Session()

    for chunk_start, chunk_end in _month_chunks(start, end):
        label = f"{chunk_start.isoformat()} → {chunk_end.isoformat()}"
        print(f"[INFO] Fetching {label}...")
        rows = _post_sql(session, api_key, SALES_QUERY, chunk_start, chunk_end)
        if rows:
            frames.append(pd.DataFrame(rows))
            print(f"[INFO]   {len(rows):,} aggregated rows")
        else:
            print("[INFO]   0 rows")

    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    raw = pd.concat(frames, ignore_index=True)
    raw.columns = [str(c).lower() for c in raw.columns]

    raw["Date"] = pd.to_datetime(raw["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    raw["Shipping_Country"] = raw["shipping_country"].map(_normalize_country)
    raw["Platform"] = raw["platform"].map(_normalize_platform)
    raw["Source_Name"] = raw["source_name"].fillna("").astype(str).str.strip()
    raw["SKU"] = raw["sku"].fillna("").astype(str).str.strip()
    raw["Units_Sold"] = pd.to_numeric(raw["units_sold"], errors="coerce").fillna(0)
    raw["Order_Revenue"] = pd.to_numeric(raw["order_revenue"], errors="coerce").fillna(0.0)

    cleaned = (
        raw[OUTPUT_COLUMNS]
        .loc[lambda df: df["Date"].notna() & (df["SKU"] != "")]
        .groupby(
            ["Date", "Shipping_Country", "Platform", "Source_Name", "SKU"],
            as_index=False,
            dropna=False,
        )
        .agg({"Units_Sold": "sum", "Order_Revenue": "sum"})
    )

    cleaned["Units_Sold"] = cleaned["Units_Sold"].round().astype(int)
    cleaned["Order_Revenue"] = cleaned["Order_Revenue"].round(2)

    return cleaned.sort_values(
        ["Date", "Shipping_Country", "Platform", "Source_Name", "SKU"]
    ).reset_index(drop=True)


def sync_triplewhale_sales(output_path: Path = OUTPUT_PATH, end_date: date | None = None) -> pd.DataFrame:
    api_key = _require_api_key()
    end = end_date or date.today()

    if end < START_DATE:
        raise ValueError(f"End date {end} is before start date {START_DATE}.")

    df = fetch_sales_rows(api_key, START_DATE, end)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"[INFO] Wrote {len(df):,} rows to {output_path}")
    return df


def git_commit_and_push(output_path: Path = OUTPUT_PATH) -> None:
    rel = output_path.relative_to(ROOT)
    subprocess.run(["git", "add", str(rel)], cwd=ROOT, check=True)

    diff = subprocess.run(["git", "diff", "--staged", "--quiet"], cwd=ROOT)
    if diff.returncode == 0:
        print("[INFO] No changes to commit.")
        return

    subprocess.run(
        ["git", "commit", "-m", "Auto-update: Live Triple Whale omnichannel sales data"],
        cwd=ROOT,
        check=True,
    )
    subprocess.run(["git", "fetch", "origin", "main"], cwd=ROOT, check=True)
    subprocess.run(["git", "pull", "--rebase", "origin", "main"], cwd=ROOT, check=True)
    subprocess.run(["git", "push", "origin", "main"], cwd=ROOT, check=True)
    print("[INFO] Committed and pushed to origin/main.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync live Triple Whale omnichannel sales into cleaned_omnichannel_sales.csv."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_PATH,
        help=f"Output CSV path (default: {OUTPUT_PATH.relative_to(ROOT)})",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="",
        help="Inclusive end date YYYY-MM-DD (default: today).",
    )
    parser.add_argument(
        "--no-git",
        action="store_true",
        help="Skip git add/commit/pull --rebase/push after writing output.",
    )
    args = parser.parse_args()

    end_date: date | None = None
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()

    try:
        sync_triplewhale_sales(args.output, end_date=end_date)
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
