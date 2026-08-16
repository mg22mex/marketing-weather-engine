#!/usr/bin/env python3
"""
Build data/city_coordinates.json from data/orders_by_city.csv using sales rank,
then resolve lat/lon via Open-Meteo geocoding (run locally or in CI with network).

Re-run when the orders rollup changes:
  python scripts/generate_city_coordinates.py
"""

from __future__ import annotations

import csv
import json
import re
import time
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests

ROOT = Path(__file__).resolve().parent.parent
ORDERS_CSV = ROOT / "data" / "orders_by_city.csv"
OUTPUT_JSON = ROOT / "data" / "city_coordinates.json"

STATE_MAP = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming",
}

UK_CODE_TO_REGION = {"ENG": "England", "SCT": "Scotland", "WLS": "Wales", "NIR": "Northern Ireland"}

CAN_ABBREV_TO_REGION = {
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

US_ABBREVS = frozenset(STATE_MAP.keys())

# When a state has fewer than this many sales-backed picks, pad with these metros (Title Case).
US_FALLBACK_CITIES: Dict[str, List[str]] = {
    "AL": ["Birmingham", "Huntsville", "Montgomery", "Mobile", "Tuscaloosa", "Hoover"],
    "AK": ["Anchorage", "Fairbanks", "Juneau", "Sitka", "Ketchikan", "Wasilla"],
    "AZ": ["Phoenix", "Tucson", "Mesa", "Chandler", "Scottsdale", "Gilbert"],
    "AR": ["Little Rock", "Fort Smith", "Fayetteville", "Springdale", "Jonesboro", "Rogers"],
    "CA": ["Los Angeles", "San Diego", "San Jose", "San Francisco", "Fresno", "Sacramento"],
    "CO": ["Denver", "Colorado Springs", "Aurora", "Fort Collins", "Lakewood", "Thornton"],
    "CT": ["Bridgeport", "New Haven", "Hartford", "Stamford", "Waterbury", "Norwalk"],
    "DE": ["Wilmington", "Dover", "Newark", "Middletown", "Smyrna", "Milford"],
    "DC": ["Washington"],
    "FL": ["Jacksonville", "Miami", "Tampa", "Orlando", "St. Petersburg", "Hialeah"],
    "GA": ["Atlanta", "Columbus", "Augusta", "Savannah", "Athens", "Sandy Springs"],
    "HI": ["Honolulu", "Hilo", "Pearl City", "Kailua", "Kaneohe", "Mililani"],
    "ID": ["Boise", "Meridian", "Nampa", "Idaho Falls", "Pocatello", "Caldwell"],
    "IL": ["Chicago", "Aurora", "Rockford", "Joliet", "Naperville", "Springfield"],
    "IN": ["Indianapolis", "Fort Wayne", "Evansville", "South Bend", "Carmel", "Bloomington"],
    "IA": ["Des Moines", "Cedar Rapids", "Davenport", "Sioux City", "Iowa City", "Waterloo"],
    "KS": ["Wichita", "Overland Park", "Kansas City", "Olathe", "Topeka", "Lawrence"],
    "KY": ["Louisville", "Lexington", "Bowling Green", "Owensboro", "Covington", "Hopkinsville"],
    "LA": ["New Orleans", "Baton Rouge", "Shreveport", "Lafayette", "Lake Charles", "Kenner"],
    "ME": ["Portland", "Lewiston", "Bangor", "South Portland", "Auburn", "Biddeford"],
    "MD": ["Baltimore", "Frederick", "Rockville", "Gaithersburg", "Bowie", "Annapolis"],
    "MA": ["Boston", "Worcester", "Springfield", "Cambridge", "Lowell", "New Bedford"],
    "MI": ["Detroit", "Grand Rapids", "Warren", "Sterling Heights", "Ann Arbor", "Lansing"],
    "MN": ["Minneapolis", "Saint Paul", "Rochester", "Duluth", "Bloomington", "Brooklyn Park"],
    "MS": ["Jackson", "Gulfport", "Southaven", "Hattiesburg", "Biloxi", "Meridian"],
    "MO": ["Kansas City", "Saint Louis", "Springfield", "Columbia", "Independence", "Lee's Summit"],
    "MT": ["Billings", "Missoula", "Great Falls", "Bozeman", "Butte", "Helena"],
    "NE": ["Omaha", "Lincoln", "Bellevue", "Grand Island", "Kearney", "Fremont"],
    "NV": ["Las Vegas", "Henderson", "Reno", "North Las Vegas", "Sparks", "Carson City"],
    "NH": ["Manchester", "Nashua", "Concord", "Derry", "Rochester", "Salem"],
    "NJ": ["Newark", "Jersey City", "Paterson", "Elizabeth", "Edison", "Woodbridge"],
    "NM": ["Albuquerque", "Las Cruces", "Rio Rancho", "Santa Fe", "Roswell", "Farmington"],
    "NY": ["New York", "Buffalo", "Rochester", "Yonkers", "Syracuse", "Albany"],
    "NC": ["Charlotte", "Raleigh", "Greensboro", "Durham", "Winston-Salem", "Fayetteville"],
    "ND": ["Fargo", "Bismarck", "Grand Forks", "Minot", "West Fargo", "Williston"],
    "OH": ["Columbus", "Cleveland", "Cincinnati", "Toledo", "Akron", "Dayton"],
    "OK": ["Oklahoma City", "Tulsa", "Norman", "Broken Arrow", "Lawton", "Edmond"],
    "OR": ["Portland", "Salem", "Eugene", "Gresham", "Hillsboro", "Bend"],
    "PA": ["Philadelphia", "Pittsburgh", "Allentown", "Erie", "Reading", "Scranton"],
    "RI": ["Providence", "Warwick", "Cranston", "Pawtucket", "East Providence", "Woonsocket"],
    "SC": ["Charleston", "Columbia", "North Charleston", "Mount Pleasant", "Rock Hill", "Greenville"],
    "SD": ["Sioux Falls", "Rapid City", "Aberdeen", "Brookings", "Watertown", "Mitchell"],
    "TN": ["Nashville", "Memphis", "Knoxville", "Chattanooga", "Clarksville", "Murfreesboro"],
    "TX": ["Houston", "San Antonio", "Dallas", "Austin", "Fort Worth", "El Paso"],
    "UT": ["Salt Lake City", "West Valley City", "Provo", "West Jordan", "Orem", "Sandy"],
    "VT": ["Burlington", "South Burlington", "Rutland", "Barre", "Montpelier", "St. Albans"],
    "VA": ["Virginia Beach", "Norfolk", "Chesapeake", "Richmond", "Newport News", "Alexandria"],
    "WA": ["Seattle", "Spokane", "Tacoma", "Vancouver", "Bellevue", "Kent"],
    "WV": ["Charleston", "Huntington", "Morgantown", "Parkersburg", "Wheeling", "Weirton"],
    "WI": ["Milwaukee", "Madison", "Green Bay", "Kenosha", "Racine", "Appleton"],
    "WY": ["Cheyenne", "Casper", "Laramie", "Gillette", "Rock Springs", "Sheridan"],
}

UK_FALLBACK = {
    "ENG": ["London", "Birmingham", "Manchester", "Leeds", "Liverpool", "Sheffield", "Bristol", "Leicester"],
    "SCT": ["Glasgow", "Edinburgh", "Aberdeen", "Dundee", "Paisley", "East Kilbride", "Livingston", "Hamilton"],
    "WLS": ["Cardiff", "Swansea", "Newport", "Wrexham", "Barry", "Neath", "Cwmbran", "Bridgend"],
    "NIR": ["Belfast", "Derry", "Lisburn", "Newry", "Bangor", "Craigavon", "Castlereagh", "Omagh"],
}

CAN_FALLBACK = {
    "ON": ["Toronto", "Ottawa", "Mississauga", "Brampton", "Hamilton", "London"],
    "BC": ["Vancouver", "Surrey", "Burnaby", "Richmond", "Abbotsford", "Coquitlam"],
    "QC": ["Montreal", "Quebec City", "Laval", "Gatineau", "Longueuil", "Sherbrooke"],
    "AB": ["Calgary", "Edmonton", "Red Deer", "Lethbridge", "St. Albert", "Medicine Hat"],
    "MB": ["Winnipeg", "Brandon", "Steinbach", "Thompson", "Portage la Prairie", "Winkler"],
    "SK": ["Saskatoon", "Regina", "Prince Albert", "Moose Jaw", "Swift Current", "Yorkton"],
    "NS": ["Halifax", "Dartmouth", "Sydney", "Truro", "New Glasgow", "Glace Bay"],
    "NB": ["Moncton", "Saint John", "Fredericton", "Dieppe", "Miramichi", "Edmundston"],
    "NL": ["St. John's", "Mount Pearl", "Corner Brook", "Conception Bay South", "Paradise", "Grand Falls-Windsor"],
    "PE": ["Charlottetown", "Summerside", "Stratford", "Cornwall", "Montague", "Kensington"],
    "YT": ["Whitehorse", "Dawson City", "Watson Lake", "Haines Junction", "Mayo", "Carmacks"],
    "NT": ["Yellowknife", "Hay River", "Inuvik", "Fort Smith", "Behchokǫ̀", "Fort Simpson"],
    "NU": ["Iqaluit", "Rankin Inlet", "Arviat", "Baker Lake", "Cambridge Bay", "Pond Inlet"],
}

GEOCODE_DELAY_S = 0.08
REQUEST_TIMEOUT_S = 25
OPEN_METEO_GEO = "https://geocoding-api.open-meteo.com/v1/search"

# Display name from sales keys -> Open-Meteo query string (US state abbrev, picked display name).
US_GEOCODE_QUERY: Dict[Tuple[str, str], str] = {
    ("ID", "Coeur D Alene"): "Coeur d'Alene",
    ("UT", "Eaglemountain"): "Eagle Mountain",
    ("WA", "Subiaco"): "Spokane",
}

CA_GEOCODE_QUERY: Dict[Tuple[str, str], str] = {
    ("NL", "Grand Falls Windsor"): "Grand Falls-Windsor",
}


def _ascii_key(s: str) -> str:
    t = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return " ".join(t.upper().replace(",", " ").split())


def _normalize_us_city_key(city_raw: str, state_abbrev: str) -> str:
    c = _ascii_key(city_raw)
    if state_abbrev == "DC" and "WASHINGTON" in c:
        return "WASHINGTON"
    return c


def _title_city(name: str) -> str:
    parts = re.split(r"(\s+|-)", name.strip())
    out: List[str] = []
    for p in parts:
        if p in (" ", "-"):
            out.append(p)
            continue
        if not p:
            continue
        if p.upper() in ("MC", "ST", "ST."):
            out.append(p.capitalize() + ("" if p.endswith(".") else "."))
            continue
        out.append(p[:1].upper() + p[1:].lower() if len(p) > 1 else p.upper())
    return "".join(out) if out else name.strip().title()


def _geocode_us(city: str, state_abbrev: str) -> Optional[Tuple[float, float]]:
    admin1 = STATE_MAP[state_abbrev]
    url = f"{OPEN_METEO_GEO}?name={quote(city)}&country=United+States&count=20&language=en&format=json"
    try:
        res = requests.get(url, timeout=REQUEST_TIMEOUT_S)
        if res.status_code != 200:
            return None
        for r in res.json().get("results") or []:
            if r.get("country_code") == "US" and r.get("admin1") == admin1:
                return float(r["latitude"]), float(r["longitude"])
    except Exception:
        return None
    return None


def _geocode_uk(city: str, nation: str) -> Optional[Tuple[float, float]]:
    url = f"{OPEN_METEO_GEO}?name={quote(city)}&country=United+Kingdom&count=20&language=en&format=json"
    try:
        res = requests.get(url, timeout=REQUEST_TIMEOUT_S)
        if res.status_code != 200:
            return None
        for r in res.json().get("results") or []:
            if r.get("country_code") == "GB" and r.get("admin1") == nation:
                return float(r["latitude"]), float(r["longitude"])
    except Exception:
        return None
    return None


def _geocode_ca(city: str, province: str) -> Optional[Tuple[float, float]]:
    url = f"{OPEN_METEO_GEO}?name={quote(city)}&country=Canada&count=20&language=en&format=json"
    try:
        res = requests.get(url, timeout=REQUEST_TIMEOUT_S)
        if res.status_code != 200:
            return None
        for r in res.json().get("results") or []:
            if r.get("country_code") == "CA" and r.get("admin1") == province:
                return float(r["latitude"]), float(r["longitude"])
    except Exception:
        return None
    return None


def _load_orders() -> List[Dict[str, str]]:
    with ORDERS_CSV.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _aggregate_us(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, float]]:
    """state_abbrev -> normalized_city_key -> total_orders."""
    acc: DefaultDict[str, Dict[str, float]] = defaultdict(dict)
    for row in rows:
        st = row["state"].strip().upper()
        if st not in US_ABBREVS:
            continue
        try:
            orders = float(row.get("total_orders") or 0)
        except ValueError:
            orders = 0.0
        ck = _normalize_us_city_key(row["city"], st)
        if st == "DC" and "WASHINGTON" not in ck:
            continue
        d = acc[st]
        d[ck] = d.get(ck, 0.0) + orders
    return {k: v for k, v in acc.items()}


def _aggregate_uk(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, float]]:
    acc: DefaultDict[str, Dict[str, float]] = defaultdict(dict)
    for row in rows:
        st = row["state"].strip().upper()
        if st not in UK_CODE_TO_REGION:
            continue
        try:
            orders = float(row.get("total_orders") or 0)
        except ValueError:
            orders = 0.0
        ck = _ascii_key(row["city"])
        if st == "NIR" and ck == "LONDONDERRY":
            ck = "DERRY"
        d = acc[st]
        d[ck] = d.get(ck, 0.0) + orders
    return {k: v for k, v in acc.items()}


def _aggregate_ca(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, float]]:
    acc: DefaultDict[str, Dict[str, float]] = defaultdict(dict)
    for row in rows:
        st = row["state"].strip().upper()
        if st not in CAN_ABBREV_TO_REGION:
            continue
        try:
            orders = float(row.get("total_orders") or 0)
        except ValueError:
            orders = 0.0
        ck = _ascii_key(row["city"])
        d = acc[st]
        d[ck] = d.get(ck, 0.0) + orders
    return {k: v for k, v in acc.items()}


def _pick_ranked_keys(orders_by_key: Dict[str, float], max_n: int) -> List[str]:
    ranked = sorted(orders_by_key.items(), key=lambda kv: (-kv[1], kv[0]))
    return [k for k, _ in ranked[:max_n]]


def _pick_us_cities_per_state(
    agg: Dict[str, Dict[str, float]], max_cities: int, min_cities: int
) -> List[Tuple[str, str, str]]:
    """List of (state_abbrev, display_city, source_key) for geocoding."""
    out: List[Tuple[str, str, str]] = []
    for abbrev in sorted(US_ABBREVS, key=lambda x: (x == "DC", x)):
        ob = agg.get(abbrev, {})
        keys = _pick_ranked_keys(ob, max_cities)
        display: List[str] = []
        seen: set[str] = set()
        min_need = 1 if abbrev == "DC" else min_cities
        for k in keys:
            name = _title_city(k)
            lk = name.lower()
            if lk not in seen:
                seen.add(lk)
                display.append(name)
        if len(display) < min_need:
            for fb in US_FALLBACK_CITIES.get(abbrev, []):
                if fb.lower() not in seen:
                    seen.add(fb.lower())
                    display.append(fb)
                if len(display) >= min_need:
                    break
        for name in display[:max_cities]:
            out.append((abbrev, name, name.upper()))
    return out


def _pick_uk_cities(agg: Dict[str, Dict[str, float]], per_nation: int) -> List[Tuple[str, str, str]]:
    out: List[Tuple[str, str, str]] = []
    for code, nation in UK_CODE_TO_REGION.items():
        ob = agg.get(code, {})
        keys = _pick_ranked_keys(ob, per_nation)
        display: List[str] = []
        seen: set[str] = set()
        for k in keys:
            name = _title_city(k)
            if name.lower() not in seen:
                seen.add(name.lower())
                display.append(name)
        for fb in UK_FALLBACK.get(code, []):
            if len(display) >= per_nation:
                break
            if fb.lower() not in seen:
                seen.add(fb.lower())
                display.append(fb)
        for name in display[:per_nation]:
            out.append((code, name, nation))
    return out


def _pick_ca_cities(agg: Dict[str, Dict[str, float]], per_province: int) -> List[Tuple[str, str, str]]:
    out: List[Tuple[str, str, str]] = []
    for abbrev, province in sorted(CAN_ABBREV_TO_REGION.items()):
        ob = agg.get(abbrev, {})
        keys = _pick_ranked_keys(ob, per_province)
        display: List[str] = []
        seen: set[str] = set()
        for k in keys:
            name = _title_city(k)
            if name.lower() not in seen:
                seen.add(name.lower())
                display.append(name)
        for fb in CAN_FALLBACK.get(abbrev, []):
            if len(display) >= per_province:
                break
            if fb.lower() not in seen:
                seen.add(fb.lower())
                display.append(fb)
        for name in display[:per_province]:
            out.append((abbrev, name, province))
    return out


def main() -> None:
    if not ORDERS_CSV.is_file():
        raise SystemExit(f"Missing orders CSV: {ORDERS_CSV}")

    rows = _load_orders()
    us_agg = _aggregate_us(rows)
    uk_agg = _aggregate_uk(rows)
    ca_agg = _aggregate_ca(rows)

    max_us = 5
    min_us = 3
    picks_us = _pick_us_cities_per_state(us_agg, max_us, min_us)
    picks_uk = _pick_uk_cities(uk_agg, 8)
    picks_ca = _pick_ca_cities(ca_agg, 5)

    json_rows: List[Dict[str, Any]] = []
    failures: List[str] = []

    for abbrev, city, _ in picks_us:
        time.sleep(GEOCODE_DELAY_S)
        q = US_GEOCODE_QUERY.get((abbrev, city), city)
        coords = _geocode_us(q, abbrev)
        if coords is None:
            failures.append(f"US {abbrev} {city}")
            continue
        json_rows.append(
            {
                "region": "US",
                "state": STATE_MAP[abbrev],
                "city": city,
                "lat": round(coords[0], 6),
                "lon": round(coords[1], 6),
            }
        )

    for code, city, nation in picks_uk:
        time.sleep(GEOCODE_DELAY_S)
        coords = _geocode_uk(city, nation)
        if coords is None:
            failures.append(f"UK {code} {city}")
            continue
        json_rows.append(
            {
                "region": "UK",
                "state": UK_CODE_TO_REGION[code],
                "city": city,
                "lat": round(coords[0], 6),
                "lon": round(coords[1], 6),
            }
        )

    for abbrev, city, province in picks_ca:
        time.sleep(GEOCODE_DELAY_S)
        q = CA_GEOCODE_QUERY.get((abbrev, city), city)
        coords = _geocode_ca(q, province)
        if coords is None:
            failures.append(f"CA {abbrev} {city}")
            continue
        json_rows.append(
            {
                "region": "CA",
                "state": CAN_ABBREV_TO_REGION[abbrev],
                "city": city,
                "lat": round(coords[0], 6),
                "lon": round(coords[1], 6),
            }
        )

    deduped: List[Dict[str, Any]] = []
    seen_pairs: set[Tuple[str, str]] = set()
    for row in json_rows:
        key = (row["state"].lower(), row["city"].lower())
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        deduped.append(row)

    payload = {
        "generated_by": "scripts/generate_city_coordinates.py",
        "orders_source": str(ORDERS_CSV.relative_to(ROOT)),
        "row_count": len(deduped),
        "geocode_failures": failures,
        "rows": deduped,
    }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {len(json_rows)} coordinates to {OUTPUT_JSON}")
    if failures:
        print("Failures (no coordinates written for these picks):")
        for f in failures:
            print(" ", f)


if __name__ == "__main__":
    main()
