#!/usr/bin/env python3
"""
Pizza Hut Canada – Pizza-Only Scraper (Pricing Pipeline Compatible)

Purpose
-------
Scrape Pizza Hut Canada store data and pizza menu prices for all huts,
and export an analytics-ready CSV compatible with the broader pricing
pipeline schema:

    chain_key, store_key, city, province, category,
    recipe, product_key, size, crust, price,
    date_key, month, year

The script:
- Retrieves all Canadian huts (stores) from the public Pizza Hut API.
- Fetches pizza products and variants per hut with simple throttling.
- Normalizes size, crust, recipe, and price into a clean, consistent format.
- Emits a single CSV suitable for downstream modelling (e.g., DuckDB + Power BI).

Usage
-----
    python pizza_hut_scraper.py

Output
------
A CSV file in the local `data/raw/` folder:

    data/raw/pizzahut_pizza_<YYYYMMDD>.csv

Dependencies
------------
- requests
- urllib3
- tqdm
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests
import urllib3
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm

# allow large fields in CSV
csv.field_size_limit(sys.maxsize)

# ── paths & runtime config ────────────────────────────────────────

DATE_STR = datetime.today().strftime("%Y%m%d")

# Portfolio-friendly: relative output folder inside the repo
OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUTPUT_DIR / f"pizzahut_pizza_{DATE_STR}.csv"

COOLDOWN_SECS = 15
CONNECT_TO, READ_TO = 3, 15
CHAIN_KEY = "PH"  # constant chain identifier (Pizza Hut)

# ── endpoints ────────────────────────────────────────────────────

SECTORS = ["ca-1", "ca-2"]
HUTS_URL = "https://api.pizzahut.io/v1/huts/"
PIZZAS_URL = "https://api.pizzahut.io/v2/products/pizzas"

# ── logging ──────────────────────────────────────────────────────

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "pizza_hut_scraper.log"

logging.basicConfig(
    filename=str(LOG_FILE),
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s – %(message)s",
)

STOP_FLAG = threading.Event()

# ── zero-retry HTTP session (per-thread) ─────────────────────────

retry_cfg = Retry(total=0)
thread_local = threading.local()


def get_sess() -> requests.Session:
    """Return a thread-local Session with basic headers and no retries."""
    if not hasattr(thread_local, "sess"):
        s = requests.Session()
        s.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (compatible; PizzaPricingScraper/1.0)",
                "Accept": "application/json",
                "Accept-Language": "en-CA,en;q=0.9",
            }
        )
        s.mount("https://", HTTPAdapter(max_retries=retry_cfg))
        thread_local.sess = s
    return thread_local.sess


class Throttled(Exception):
    """
    Raised when the API asks us to slow down (429/403) or drops the connection.

    Used to coordinate "blast" cycles with a cooldown period.
    """


def fetch_json(url: str, params: Dict[str, Any] | None = None) -> Any:
    """
    Return parsed JSON or None.

    Raises Throttled if the API signals we should slow down (429/403) or a
    connection error occurs while blasting.
    """
    if STOP_FLAG.is_set():
        raise Throttled

    try:
        r = get_sess().get(url, params=params, timeout=(CONNECT_TO, READ_TO))
    except requests.exceptions.ConnectionError:
        STOP_FLAG.set()
        raise Throttled
    except (requests.exceptions.RetryError, urllib3.exceptions.MaxRetryError) as e:
        logging.warning("RetryError on %s – %s", url, e)
        return None

    if r.status_code in (429, 403):
        STOP_FLAG.set()
        raise Throttled
    if r.status_code in (400, 404) or r.status_code >= 500:
        return None

    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        return None


# ── huts fetch with simple backoff ───────────────────────────────


def fetch_huts_with_backoff(max_attempts: int = 6) -> List[Dict[str, Any]]:
    """
    Fetch all Canadian huts with basic backoff & throttle handling.
    """
    params = {"sector": ",".join(SECTORS), "featureRole": "none"}

    for attempt in range(1, max_attempts + 1):
        try:
            STOP_FLAG.clear()
            huts = fetch_json(HUTS_URL, params)
            if isinstance(huts, list) and huts:
                return huts
            time.sleep(2 + attempt)
        except Throttled:
            print(f"[huts] Throttled – cooling down {COOLDOWN_SECS}s …")
            time.sleep(COOLDOWN_SECS)
        except Exception as e:  # pragma: no cover - defensive logging
            logging.warning("Huts fetch error (attempt %s): %s", attempt, e)
            time.sleep(2 + attempt)

    return []


# ── field cleaners / normalizers ─────────────────────────────────

_NUM_INCH = re.compile(r"(?<!\d)(8|9|10|11|12|13|14|15|16|18|20|22|24)(?!\d)")


def normalize_size(size_val: Any, fallback_from_text: str = "") -> str:
    """
    Normalize size into a human-readable token.

    Examples:
        "12"         -> 12"
        "medium"     -> Medium
        "x-large"    -> X-Large

    Defaults to "Medium" so size is never null.
    """
    s = (str(size_val) if size_val else "").strip().lower().replace("_", "-")
    if not s and fallback_from_text:
        s = fallback_from_text.strip().lower()

    m = _NUM_INCH.search(s)
    if m:
        return f'{m.group(1)}"'

    if "personal" in s:
        return "Personal"
    if "small" in s:
        return "Small"
    if "medium" in s:
        return "Medium"
    if "large" in s:
        return "Large"
    if any(x in s for x in ("x-large", "xlarge", "xl")):
        return "X-Large"

    return "Medium"


def _crust_from_keyish(txt: str) -> str:
    """
    Extract crust when variant keys look like:
        '12 Medium 1P2.Crusts.Handcrafted'
    """
    t = txt or ""
    if "Crusts." in t:
        cand = t.split("Crusts.", 1)[-1]
    else:
        parts = re.split(r"[.\s]+", t.strip())
        cand = parts[-1] if parts else ""
    cand = cand.replace("-", " ").strip().title()
    return cand


def normalize_crust(crust_val: Any, fallback_from_key: str = "") -> str:
    """Normalize crust into a small controlled vocabulary."""
    if crust_val:
        s = str(crust_val).strip()
    elif fallback_from_key:
        s = _crust_from_keyish(fallback_from_key)
    else:
        return "Regular Dough"

    s = re.sub(r"^.*Crusts\.", "", s, flags=re.I)
    s = s.replace(".", " ").strip()
    low = s.lower()

    if not low:
        return "Regular Dough"

    mapping = {
        "hand tossed": "Hand Tossed",
        "handcrafted": "Handcrafted",
        "original pan": "Original Pan",
        "pan": "Pan",
        "stuffed": "Stuffed Crust",
        "stuffed crust": "Stuffed Crust",
        "thin ’n’ crispy": "Thin ’n’ Crispy",
        "thin 'n' crispy": "Thin ’n’ Crispy",
        "thin n crispy": "Thin ’n’ Crispy",
        "thin": "Thin ’n’ Crispy",
        "gluten free": "Gluten Free",
        "regular": "Regular Dough",
        "regular dough": "Regular Dough",
    }
    return mapping.get(low, s.title())


def clean_recipe(name: str) -> str:
    """Clean recipe / pizza name for consistent grouping."""
    if not name:
        return ""
    s = str(name).strip()
    # Remove trailing short alphanumeric codes like "2X", "2J", "28", "25"
    s = re.sub(r"\s+[A-Z0-9]{1,3}$", "", s, flags=re.I)
    # Normalize BBQ casing
    s = re.sub(r"\bbbq\b", "BBQ", s, flags=re.I)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def cents_to_dollars(v: Any) -> float | None:
    try:
        iv = int(v)
        return round(iv / 100.0, 2)
    except Exception:
        return None


def price_from_variant(v: Dict[str, Any], prod: Dict[str, Any] | None = None) -> Any:
    """
    Best-effort price extraction from a variant, with product-level fallback.
    """
    cand = v.get("price")
    if isinstance(cand, (int, float)):
        return round(float(cand), 2)

    if isinstance(cand, dict):
        for key in ("amount", "value", "price"):
            if key in cand and isinstance(cand[key], (int, float)):
                return round(float(cand[key]), 2)
        if "cents" in cand:
            got = cents_to_dollars(cand["cents"])
            if got is not None:
                return got

    for key in ("unitPrice", "value", "amount"):
        if isinstance(v.get(key), (int, float)):
            return round(float(v[key]), 2)

    if "priceCents" in v:
        got = cents_to_dollars(v["priceCents"])
        if got is not None:
            return got

    if prod:
        if isinstance(prod.get("price"), (int, float)):
            return round(float(prod["price"]), 2)
        if "priceCents" in prod and isinstance(prod["priceCents"], int):
            got = cents_to_dollars(prod["priceCents"])
            if got is not None:
                return got

    return ""


# --- product_key helpers (underscores only, no store in key) -----

_SLUG_BAD_FOR_KEY = re.compile(r"[^a-z0-9]+")


def key_slug_from_recipe(recipe: str, prod_id_like: str = "") -> str:
    """
    Build a compact base key from the cleaned recipe (preferred).

    Falls back to product id/slug with cleanup (strip 'pizza.'/'pizza,'
    and tiny code tails).
    """
    base = (recipe or "").strip().lower()
    base = _SLUG_BAD_FOR_KEY.sub("_", base).strip("_")
    base = re.sub(r"_+", "_", base)
    if base:
        return base[:40]

    raw = (prod_id_like or "").lower()
    raw = raw.replace("pizza,", "pizza.").replace("pizza..", "pizza.")
    raw = re.sub(r"^pizza\.", "", raw)          # drop leading pizza.
    raw = raw.split(".", 1)[0]                  # drop .12-medium-... tail
    raw = re.sub(r"[-._][a-z0-9]{1,3}$", "", raw)  # drop short code suffix
    raw = _SLUG_BAD_FOR_KEY.sub("_", raw).strip("_")
    raw = re.sub(r"_+", "_", raw)
    return (raw or "recipe")[:40]


def abbrev_size_token(size: str) -> str:
    if not size:
        return "m"
    s = size.upper().replace('"', "")
    mapping = {
        "PERSONAL": "p",
        "SMALL": "s",
        "MEDIUM": "m",
        "LARGE": "l",
        "X-LARGE": "xl",
        "XXL": "xxl",
    }
    if s.isdigit():
        return s
    return mapping.get(s, re.sub(r"[^A-Z0-9]", "", s).lower()[:6] or "m")


def abbrev_crust_token(crust: str) -> str:
    s = (crust or "").lower()
    mapping = {
        "hand tossed": "ht",
        "handcrafted": "hc",
        "original pan": "pan",
        "pan": "pan",
        "stuffed crust": "stf",
        "thin ’n’ crispy": "thn",
        "thin 'n' crispy": "thn",
        "thin n crispy": "thn",
        "gluten free": "gf",
        "regular dough": "reg",
    }
    return mapping.get(s, re.sub(r"[^a-z0-9]", "", s)[:6] or "reg")


def product_key_for_variant(
    recipe: str,
    prod: Dict[str, Any],
    size: str,
    crust: str,
    variant: Dict[str, Any],
) -> str:
    """
    Build a store-agnostic product key that combines:
        - recipe
        - size
        - crust
        - a variant identifier (if present)
    """
    # Prefer explicit variant identifier when present
    for k in ("id", "sku", "code", "variantId", "variant_id"):
        v = variant.get(k)
        if v is not None and str(v).strip():
            vid = _SLUG_BAD_FOR_KEY.sub("_", str(v).lower())
            vid = re.sub(r"_+", "_", vid).strip("_")[:32]
            if vid:
                return f"ph_{vid}_{abbrev_size_token(size)}_{abbrev_crust_token(crust)}"

    base = key_slug_from_recipe(
        recipe,
        prod_id_like=str(
            prod.get("id") or prod.get("code") or prod.get("slug") or ""
        ),
    )
    return f"ph_{base}_{abbrev_size_token(size)}_{abbrev_crust_token(crust)}"


def _hut_city_province(h: Dict[str, Any]) -> Tuple[str, str]:
    city, prov = "", ""
    addr = h.get("address") or {}
    city = (addr.get("city") or "").strip()
    prov = (
        addr.get("region")
        or addr.get("province")
        or addr.get("state")
        or ""
    ).strip()
    lines = addr.get("lines")

    # fallback if city is embedded in the address lines
    if not city and isinstance(lines, list) and len(lines) >= 2:
        try:
            city = str(lines[-2]).strip()
        except Exception:
            pass

    return city, prov


@dataclass
class StoreResult:
    store_key: str
    rows: List[Dict[str, Any]]


# ── worker (one hut) ─────────────────────────────────────────────


def scrape_store(hut: Dict[str, Any]) -> StoreResult:
    """
    Scrape a single Pizza Hut store (hut) and return its pizza rows.
    """
    sid = hut.get("id") or ""
    store_key_raw = str(sid)
    store_key_prefixed = f"PH_{store_key_raw}"

    sector = hut.get("sector")
    city, province = _hut_city_province(hut)

    params_primary = {"hutid": sid, "collection": "true"}
    if sector:
        params_primary["sector"] = sector

    rows: List[Dict[str, Any]] = []

    # We try primary params, and fall back to a minimal set if needed
    for params in (params_primary, {"hutid": sid, "collection": "true"}):
        try:
            data = fetch_json(PIZZAS_URL, params=params)
        except Throttled:
            raise
        if not data:
            continue

        if isinstance(data, dict) and "data" in data and isinstance(
            data["data"], dict
        ):
            products = data["data"].get("products") or []
        else:
            products = data.get("products") if isinstance(data, dict) else data

        if not isinstance(products, list):
            continue

        today = datetime.today()
        date_key = today.strftime("%Y-%m-%d")
        month = today.strftime("%B")
        year = str(today.year)

        for prod in products:
            if not isinstance(prod, dict):
                continue

            recipe = clean_recipe(
                prod.get("name") or prod.get("displayName") or ""
            )
            if not recipe:
                recipe = clean_recipe(
                    str(prod.get("kind") or prod.get("slug") or "")
                )

            # gather variants
            variants = None
            for k in (
                "priceVariants",
                "sizePrices",
                "variants",
                "variantPrices",
                "sizes",
                "items",
            ):
                v = prod.get(k)
                if isinstance(v, list) and v:
                    variants = v
                    break

            if not variants:
                # synthesize a "variant" from product-level fields if necessary
                v = {}
                for k in (
                    "size",
                    "sizeKey",
                    "sizeName",
                    "crust",
                    "dough",
                    "style",
                    "price",
                    "unitPrice",
                    "priceCents",
                    "key",
                    "label",
                    "id",
                ):
                    if k in prod:
                        v[k] = prod[k]
                variants = [v] if v else []

            for v in variants:
                keyish = str(
                    v.get("key") or v.get("label") or v.get("id") or ""
                )
                size = normalize_size(
                    v.get("size")
                    or v.get("sizeKey")
                    or v.get("sizeSlug")
                    or v.get("sizeName")
                    or "",
                    fallback_from_text=keyish,
                )
                crust = normalize_crust(
                    v.get("crust")
                    or v.get("dough")
                    or v.get("style")
                    or "",
                    fallback_from_key=keyish,
                )
                if not crust:
                    crust = "Regular Dough"

                price = price_from_variant(v, prod)
                base_product_key = product_key_for_variant(
                    recipe, prod, size, crust, v
                )
                product_key_prefixed = (
                    f"PH_{base_product_key}"  # pipeline-wide prefix
                )

                rows.append(
                    {
                        "chain_key": CHAIN_KEY,
                        "store_key": store_key_prefixed,
                        "city": city,
                        "province": province,
                        "category": "Pizza",
                        "recipe": recipe,
                        "product_key": product_key_prefixed,
                        "size": size,
                        "crust": crust,
                        "price": (
                            price if isinstance(price, (int, float)) else ""
                        ),
                        "date_key": date_key,
                        "month": month,
                        "year": year,
                    }
                )

        break  # stop after first successful response

    return StoreResult(store_key=store_key_prefixed, rows=rows)


# ── blast loop ──────────────────────────────────────────────────

def blast_all(huts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Blast the huts in waves, respecting throttle signals from the API.
    """
    remaining = {h.get("id") for h in huts if h.get("id")}
    pizzas_rows: List[Dict[str, Any]] = []

    while remaining:
        STOP_FLAG.clear()
        batch = [h for h in huts if h.get("id") in remaining]
        print(f"\nBlasting {len(batch):,} huts …")

        with ThreadPoolExecutor(max_workers=32) as exe:
            futs = {exe.submit(scrape_store, h): h for h in batch}
            for fut in tqdm(
                as_completed(futs),
                total=len(batch),
                desc="Huts",
            ):
                hut = futs[fut]
                sid = hut.get("id")
                try:
                    result: StoreResult = fut.result()
                    pizzas_rows.extend(result.rows)
                except Throttled:
                    # Cancel outstanding futures and back off
                    for f in futs:
                        f.cancel()
                    STOP_FLAG.set()
                    break
                except Exception as e:  # pragma: no cover - defensive
                    print(f"[warn] hut {sid} failed: {e.__class__.__name__}")
                finally:
                    remaining.discard(sid)

        if remaining and STOP_FLAG.is_set():
            print(f"\nThrottle – cooling down {COOLDOWN_SECS}s …\n")
            time.sleep(COOLDOWN_SECS)

    return pizzas_rows


# ── CSV writer ───────────────────────────────────────────────────

OUT_HEADER = [
    "chain_key",
    "store_key",
    "city",
    "province",
    "category",
    "recipe",
    "product_key",
    "size",
    "crust",
    "price",
    "date_key",
    "month",
    "year",
]


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    """Write rows to CSV using the fixed OUT_HEADER schema."""
    if not rows:
        print(f"[warn] No data for {path.name}")
        return

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=OUT_HEADER,
            extrasaction="ignore",
        )
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in OUT_HEADER})

    print(f"✓ Wrote {len(rows):,} rows → {path}")


# ── main ─────────────────────────────────────────────────────────

def main() -> None:
    start = datetime.now()

    huts = fetch_huts_with_backoff()
    if not huts:
        print("Could not retrieve hut list – aborting.")
        sys.exit(1)

    print(f"Total huts: {len(huts):,}")

    pizzas_rows = blast_all(huts)
    write_csv(OUT_FILE, pizzas_rows)

    elapsed = (datetime.now() - start).seconds / 60
    hut_count = len({r["store_key"] for r in pizzas_rows})
    print(f"\n✓ Finished in {elapsed:.1f} min (huts processed: {hut_count:,})")


if __name__ == "__main__":
    main()
