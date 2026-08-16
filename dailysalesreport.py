import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests


def _load_dotenv() -> None:
    """Load key=value pairs from .env next to this script (does not override existing os.environ)."""
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.is_file():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(
            f"Missing required environment variable {name!r}. "
            f"Copy .env.example to .env and set your Shopify credentials."
        )
    return v


def _build_stores() -> dict:
    client_id = _require_env("SHOPIFY_CUSTOM_APP_CLIENT_ID")
    client_secret = _require_env("SHOPIFY_CUSTOM_APP_CLIENT_SECRET")
    return {
        "AU-NZ": {
            "url": _require_env("SHOPIFY_AU_NZ_STORE_URL"),
            "id": client_id,
            "secret": client_secret,
        },
        "UK": {
            "url": _require_env("SHOPIFY_UK_STORE_URL"),
            "id": client_id,
            "secret": client_secret,
        },
        "B2B": {
            "url": _require_env("SHOPIFY_B2B_STORE_URL"),
            "id": client_id,
            "secret": client_secret,
        },
        "WM3": {
            "url": _require_env("SHOPIFY_WM3_STORE_URL"),
            "id": client_id,
            "secret": client_secret,
        },
    }


_load_dotenv()
STORES = _build_stores()


def get_token(url: str, client_id: str, client_secret: str):
    """Auto-refreshes the 24-hour token"""
    auth_url = f"https://{url}/admin/oauth/access_token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }
    r = requests.post(auth_url, data=payload)
    return r.json().get("access_token")


def fetch_sales_data(store_name, store_url, token):
    """Pulls orders and calculates Net Sales per line item"""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")

    endpoint = f"https://{store_url}/admin/api/2024-04/orders.json"
    params = {"created_at_min": yesterday, "status": "any", "limit": 250}
    headers = {"X-Shopify-Access-Token": token}

    response = requests.get(endpoint, headers=headers, params=params)
    orders = response.json().get("orders", [])

    items_list = []
    for order in orders:
        for item in order.get("line_items", []):
            qty = int(item.get("quantity", 0))
            price = float(item.get("price", 0))
            discount = float(item.get("total_discount", 0))
            net_sales = (price * qty) - discount

            items_list.append(
                {
                    "Store": store_name,
                    "Product": item.get("title"),
                    "SKU": item.get("sku"),
                    "Qty": qty,
                    "Net Sales": net_sales,
                    "Currency": order.get("currency"),
                }
            )
    return items_list


# --- EXECUTION ---
master_data = []

for name, config in STORES.items():
    print(f"Syncing {name}...")
    try:
        temp_token = get_token(config["url"], config["id"], config["secret"])
        if temp_token:
            store_data = fetch_sales_data(name, config["url"], temp_token)
            master_data.extend(store_data)
        else:
            print(f"Failed to get token for {name}. Check Credentials/Domain.")
    except Exception as e:
        print(f"Error processing {name}: {e}")

# --- AGGREGATION & EXPORT ---
df = pd.DataFrame(master_data)

if not df.empty:
    final_report = (
        df.groupby(["Store", "Product", "SKU", "Currency"])
        .agg({"Qty": "sum", "Net Sales": "sum"})
        .reset_index()
    )

    filename = f"daily_sales_{datetime.now().strftime('%Y-%m-%d')}.csv"
    final_report.to_csv(filename, index=False)
    print(f"\nSuccess! Report saved as: {filename}")
    print(final_report.head(10))
else:
    print("\nNo sales found for yesterday.")
