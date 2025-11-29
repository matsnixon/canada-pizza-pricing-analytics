#!/usr/bin/env python3
"""
Pizza Pizza Canada – Pizza-Only Scraper (Configurator Aware)

Purpose
-------
Scrape Pizza Pizza Canada store data and pizza menu prices, expanding the
configurator to incorporate per-size + per-crust price differences, and
export an analytics-ready CSV compatible with the pricing pipeline:

    chain_key, store_key, city, province, category,
    recipe, product_key, size, crust, price,
    date_key, month, year

Key behaviours
--------------
- Only the **Pizza** category subtree is scraped.
- For each pizza, the configurator is parsed to:
  - Expand all size × crust combinations.
  - Apply crust upcharges on top of per-size base prices.
- `product_key` is unique per variant (product_id + size + crust),
  e.g. `"collection_12900_XL_REG"`, and prefixed with `PP_`.
- Final filter drops rows where `product_key` contains `"product"`.

Usage
-----
Default (scrape a range of store IDs and write to `data/raw/`):

    python pizza_pizza_scraper.py

Optional arguments:

    -o, --outfile   Custom CSV path
    -p, --pool      Number of concurrent stores (default: 32)
    --ids           Store IDs (e.g. "1-500", "1,2,3", or "10")
    --token         Override Pizza Pizza session token (pp-mw-session)

Dependencies
------------
- requests
- tqdm
"""

from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import json
import os
import random
import re
import threading
import time
import uuid
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.exceptions import (
    ConnectionError as ConnErr,
    HTTPError,
    ReadTimeout,
)

# tqdm fallback if not installed
try:  # pragma: no cover - optional dependency
    from tqdm import tqdm
except Exception:  # pragma: no cover
    def tqdm(x=None, total=None, desc=None):
        return x


# ---------------- destination + constants ----------------

OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CHAIN_KEY = "PP"

# Session token:
# - In practice this comes from a real browser session cookie.
# - For portfolio use, you can set PIZZA_PIZZA_SESSION_TOKEN in your env.
SESSION_TOKEN = os.getenv(
    "PIZZA_PIZZA_SESSION_TOKEN",
    "3587a0ae-de37-4d2d-a50e-e0e7a106bd86",  # example token; may expire
)

ID_RANGE_DEFAULT = range(1, 2001)  # default store-ID range (override with --ids)
MAX_RETRIES = 10
RETRY_STATUS = {415, 429, 500, 502, 503}
REQ_TIMEOUT = 20
POOL_SIZE_DEFAULT = 32

BASE = "https://www.pizzapizza.ca"
PIZZA_ROOT_ID = 10020  # "Pizza" category root

CATEGORY_LIST = "/ajax/catalog/api/v1/category_list/{store_id}"
PRODUCT_LIST = "/ajax/catalog/api/v1/product_list/{store_id}/{mode}"
CONFIGURATOR = "/ajax/catalog/api/v1/product/config/{store_id}"
STORE_DETAILS = "/ajax/store/api/v1/store_details/?store_id={store_id}"

# ---------------- session (thread-local) ----------------

_thread = threading.local()


def make_session(sess_token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "Accept": "application/json, text/plain, */*",
            "App-Web-Version": "1419",
            "Lang": "en",
            "User-Agent": "Mozilla/5.0 (PizzaPricingScraper/1.0)",
            "Session-Token": sess_token,
        }
    )
    s.cookies.set("pp-mw-session", sess_token, domain="pizzapizza.ca")
    return s


def session() -> requests.Session:
    if not hasattr(_thread, "s"):
        _thread.s = make_session(SESSION_TOKEN)
    return _thread.s


# ---------------- http helper with retry/backoff ----------------

def get_json(
    url: str,
    *,
    params: Optional[dict] = None,
    refer_store: Optional[int] = None,
) -> Any:
    """
    Basic GET+JSON wrapper with retry/backoff for throttling and transient errors.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            hdrs = {"X-Request-ID": str(uuid.uuid4())}
            if refer_store is not None:
                hdrs["Referer"] = f"{BASE}/store/{refer_store}/delivery"

            r = session().get(
                url,
                headers=hdrs,
                params=params,
                timeout=REQ_TIMEOUT,
            )
            if r.status_code in RETRY_STATUS:
                raise HTTPError(f"retryable {r.status_code}", response=r)
            r.raise_for_status()
            return r.json()

        except (ReadTimeout, ConnErr, HTTPError) as e:
            # 400/404 => permanent for this request
            if (
                isinstance(e, HTTPError)
                and e.response is not None
                and e.response.status_code in (400, 404)
            ):
                raise
            if attempt == MAX_RETRIES:
                raise
            time.sleep(0.35 * attempt + random.uniform(0, 0.25))


# ---------------- small utils ----------------

def today_toronto() -> dt.date:
    """Return today's date in the America/Toronto timezone."""
    try:
        from zoneinfo import ZoneInfo

        now = dt.datetime.now(ZoneInfo("America/Toronto"))
    except Exception:
        now = dt.datetime.now()
    return now.date()


def parse_id_range(arg: Optional[str]) -> Iterable[int]:
    """Parse --ids argument into a list/range of ints."""
    if arg is None:
        return ID_RANGE_DEFAULT
    s = arg.strip()
    if "-" in s:
        a, b = s.split("-", 1)
        return range(int(a), int(b) + 1)
    if "," in s:
        return [int(x) for x in s.split(",") if x.strip()]
    return [int(s)]


def num(x: Any) -> Optional[float]:
    """Best-effort numeric conversion, returning float or None."""
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        z = re.sub(r"[^0-9.\-]", "", x)
        if z and re.match(r"^-?\d+(\.\d+)?$", z):
            try:
                return float(z)
            except Exception:
                return None
    return None


def norm_label(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


# ---------------- store meta ----------------

def fetch_store_meta(store_id: int) -> Optional[Tuple[int, str, str]]:
    """Return (store_id, city, province) for a given store, or None."""
    try:
        d = get_json(
            BASE + STORE_DETAILS.format(store_id=store_id),
            refer_store=store_id,
        )
    except Exception:
        return None
    d = d or {}
    return store_id, d.get("city", "") or "", d.get("province", "") or ""


# ---------------- categories: only Pizza subtree ----------------

def fetch_categories(store_id: int) -> List[dict]:
    """Fetch raw category list for a store."""
    try:
        cats = get_json(
            BASE + CATEGORY_LIST.format(store_id=store_id),
            refer_store=store_id,
        )
    except Exception:
        return []
    if isinstance(cats, dict):
        cats = cats.get("categories") or cats.get("items") or []
    return cats if isinstance(cats, list) else []


def pizza_category_ids_and_names(
    categories: List[dict],
) -> Tuple[List[int], Dict[int, str]]:
    """
    Determine which categories belong to the Pizza subtree for a store.
    """
    id_to_parent: Dict[int, int] = {}
    id_to_name: Dict[int, str] = {}

    for c in categories:
        cid = c.get("id")
        if isinstance(cid, int):
            id_to_parent[cid] = int(c.get("parent_id") or 0)
            id_to_name[cid] = c.get("name") or ""

    def is_descendant(cid: int, root: int) -> bool:
        seen = set()
        while cid and cid not in seen:
            seen.add(cid)
            pid = id_to_parent.get(cid, 0)
            if pid == root:
                return True
            cid = pid
        return False

    pizza_ids: List[int] = []
    for c in categories:
        cid = c.get("id")
        if not isinstance(cid, int):
            continue
        if cid == PIZZA_ROOT_ID or is_descendant(cid, PIZZA_ROOT_ID):
            if c.get("products_available"):
                pizza_ids.append(cid)

    # fallback: just the root
    if not pizza_ids:
        for c in categories:
            if c.get("id") == PIZZA_ROOT_ID and c.get("products_available"):
                pizza_ids.append(PIZZA_ROOT_ID)
                break

    return sorted(set(pizza_ids)), id_to_name


# ---------------- products (from product_list) ----------------

def fetch_products_for_category(store_id: int, category_id: int) -> List[dict]:
    prods: List[dict] = []
    params = {"category_id": str(category_id)}
    for mode in ("delivery", "pickup"):
        try:
            data = get_json(
                BASE + PRODUCT_LIST.format(store_id=store_id, mode=mode),
                params=params,
                refer_store=store_id,
            )
            items = (data or {}).get("products") or (data or {}).get("items") or []
            if isinstance(items, list):
                prods.extend(items)
        except Exception:
            continue
    return prods


def discover_pizza_products(store_id: int) -> List[dict]:
    """
    Build a unique product index limited to Pizza categories.

    Returns list of dicts:
        {pid, slug, name, start_price, cat_names: set[str]}
    """
    cats = fetch_categories(store_id)
    pizza_ids, id_to_name = pizza_category_ids_and_names(cats)
    if not pizza_ids:
        return []

    idx: Dict[str, dict] = {}
    for cid in pizza_ids:
        cat_name = id_to_name.get(cid, "")
        for p in fetch_products_for_category(store_id, cid):
            pid = str(p.get("product_id") or "")
            if not pid:
                continue
            slug = str(p.get("seo_title") or "").strip()
            name = norm_label(p.get("name") or "")
            start_price = _starting_price_from_product(p)

            entry = idx.get(pid)
            if not entry:
                entry = {
                    "pid": pid,
                    "slug": slug,
                    "name": name,
                    "start_price": start_price,
                    "cat_names": set(),
                }
                idx[pid] = entry

            if cat_name:
                entry["cat_names"].add(cat_name)

    return list(idx.values())


# ---------------- configurator & parsing ----------------

SIZE_KEYS = {
    "size",
    "pizza size",
    "select size",
    "choose size",
    "sizes",
    "size selection",
}
CRUST_KEYS = {
    "dough",
    "crust",
    "pizza dough",
    "choose dough",
    "choose crust",
}


def fetch_config(store_id: int, slug: str) -> Optional[dict]:
    """Pull configurator JSON for a given product slug."""
    url = BASE + CONFIGURATOR.format(store_id=store_id)
    try:
        data = get_json(
            url,
            params={"product_slug": slug},
            refer_store=store_id,
        )
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _walk(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk(v)


def _is_group(d: dict) -> bool:
    return (
        isinstance(d, dict)
        and "options" in d
        and isinstance(d["options"], list)
        and (d.get("name") or d.get("title") or d.get("label"))
    )


def _group_name(d: dict) -> str:
    return str(d.get("name") or d.get("title") or d.get("label") or "")


def _option_label(opt: dict) -> str:
    return str(
        opt.get("name")
        or opt.get("label")
        or opt.get("title")
        or opt.get("display_name")
        or ""
    )


def _option_price_fields(opt: dict) -> List[float]:
    vals: List[float] = []
    if not isinstance(opt, dict):
        return vals
    for k, v in opt.items():
        kl = str(k).lower()
        if any(x in kl for x in ("price", "amount", "delta", "difference", "value", "final")):
            f = num(v)
            if f is not None:
                vals.append(f)
    return vals


def _find_group(config: dict, keyset: set[str]) -> Optional[dict]:
    # exact
    for d in _walk(config):
        if _is_group(d):
            gname = _group_name(d).strip().lower()
            if gname in keyset:
                return d
    # fuzzy contains
    for d in _walk(config):
        if _is_group(d):
            gname = _group_name(d).strip().lower()
            if any(k in gname for k in keyset):
                return d
    return None


# Canonical human-facing size
_INCHES_RX = re.compile(r'(?<!\d)(10|11|12|13|14|15|16|18|20|22|24)\s*["”]')


def _canonical_size(label: str) -> str:
    s = label.strip().lower()
    if "xxl" in s or "x x l" in s:
        return "Xxl"
    if s in ("party", "party size", "party xl", "party pizza", "square party"):
        return "Party"
    if "x-large" in s or "x large" in s or s == "xl":
        return "X-Large"
    if "large" in s and "x" not in s:
        return "Large"
    if "medium" in s:
        return "Medium"
    if "small" in s:
        return "Small"
    if "twin" in s:
        return "Twin"
    if "single" in s:
        return "Single"
    m = _INCHES_RX.search(label)
    if m:
        return f'{m.group(1)}"'
    return label.strip() or "Small"


def _canonical_crust(label: str) -> str:
    s = (label or "").strip().lower()
    if not s:
        return "Regular Dough"
    if "stuffed" in s:
        return "Stuffed Crust"
    if "gourmet thin" in s or "gourmet thins" in s or ("thin" in s and "crust" in s):
        return "Thin"
    if "brooklyn" in s:
        return "Brooklyn"
    if "new-york" in s or "new york" in s:
        return "New York Style"
    if "cauliflower" in s:
        return "Cauliflower"
    if "gluten free" in s or "gluten-free" in s:
        return "Gluten Free"
    if "regular" in s or "original" in s or "classic" in s:
        return "Regular Dough"
    if "hand tossed" in s or "hand-tossed" in s:
        return "Hand Tossed"
    return (label or "Regular Dough").strip().title()


def _starting_price_from_product(p: dict) -> Optional[float]:
    pt = p.get("price_text")
    if isinstance(pt, dict):
        f = num(pt.get("price_value"))
        if f is not None:
            return f
    return num(p.get("price"))


def _try_price_matrix(config: dict) -> Dict[Tuple[str, str], float]:
    """
    Fallback parser that hunts for embedded price matrices in the raw config JSON
    if js_data fails. Returns {(size, crust): price}.
    """
    result: Dict[Tuple[str, str], float] = {}
    for d in _walk(config):
        if isinstance(d, dict):
            keys = set(k.lower() for k in d.keys())
            if {"price"} & keys or {"price_value"} & keys or {"amount"} & keys or {"final_price"} & keys:
                price = num(d.get("price") or d.get("price_value") or d.get("amount") or d.get("final_price"))
                if price is None:
                    continue
                size_label = None
                crust_label = None
                for k, v in d.items():
                    kl = str(k).lower()
                    if "size" in kl and isinstance(v, str):
                        size_label = v
                    if ("crust" in kl or "dough" in kl) and isinstance(v, str):
                        crust_label = v
                if size_label:
                    size = _canonical_size(size_label)
                    crust = _canonical_crust(crust_label or "Regular Dough")
                    result[(size, crust)] = price
    return result


# Context-based crust inference from categories and product name
def infer_crust_from_context(
    recipe: str,
    cat_names: Iterable[str],
) -> Optional[str]:
    ctx = " ".join([recipe or ""] + [c or "" for c in cat_names or []]).lower()
    if "stuffed" in ctx:
        return "Stuffed Crust"
    if "gourmet thin" in ctx or "gourmet thins" in ctx or ("thin" in ctx and "crust" in ctx):
        return "Thin"
    if "brooklyn" in ctx:
        return "Brooklyn"
    if "new-york" in ctx or "new york" in ctx:
        return "New York Style"
    if "cauliflower" in ctx:
        return "Cauliflower"
    if "gluten free" in ctx or "gluten-free" in ctx:
        return "Gluten Free"
    if "hand tossed" in ctx or "hand-tossed" in ctx:
        return "Hand Tossed"
    return None


def _decode_js_data(js_data: str) -> Optional[dict]:
    """Decode base64+zlib js_data into a dict, if present."""
    if not js_data:
        return None
    try:
        raw = base64.b64decode(js_data)
        dec = zlib.decompress(raw, zlib.MAX_WBITS).decode("utf-8")
        return json.loads(dec)
    except Exception:
        return None


def parse_config_prices(
    config: dict,
    starting_price: Optional[float],
    *,
    fallback_crust: Optional[str],
) -> List[Tuple[str, str, float]]:
    """
    Return a list of (size, crust, price) tuples using configurator data
    with safe fallbacks.

    Priority order:
        1) js_data (per-size base + per-crust deltas)
        2) Explicit price matrices in config
        3) Size group + crust group with deltas
        4) Last-resort: one size + one crust at starting_price
    """
    variants: List[Tuple[str, str, float]] = []

    # ---------- 0) Try js_data (authoritative for per-size crust deltas) ----------
    js_obj = _decode_js_data(config.get("js_data", ""))
    try:
        og_prod = (config.get("data") or {}).get("products")[0]
    except Exception:
        og_prod = None

    if isinstance(js_obj, dict):
        try:
            prod_js = (js_obj.get("products") or [None])[0] or {}
            js_sizes = prod_js.get("product_options") or {}
            cfg_opts = prod_js.get("configuration_options") or {}

            # map id -> title using original configuration_options
            id2title: Dict[str, str] = {}
            if og_prod and isinstance(og_prod.get("configuration_options"), list):
                for o in og_prod["configuration_options"]:
                    oid = o.get("id")
                    title = o.get("title") or o.get("name") or o.get("label")
                    if oid and title:
                        id2title[str(oid)] = str(title)

            # sizes index
            size_idx: Dict[int, Tuple[str, float]] = {}
            for sid, sd in js_sizes.items():
                try:
                    size_idx[int(sid)] = (
                        _canonical_size(
                            (sd.get("size_name") or {}).get("en", "")
                            or str(sd.get("size") or "")
                        ),
                        float(sd.get("base_price")),
                    )
                except Exception:
                    continue

            # dough/crust deltas per size
            for opt_id, od in cfg_opts.items():
                if str(od.get("subconfiguration_id", "")).lower() != "dough":
                    continue
                crust_label = id2title.get(opt_id) or opt_id
                crust_label = _canonical_crust(crust_label)
                per_size = od.get("product_options") or {}
                for sid, (sz_name, base_price) in size_idx.items():
                    ps = per_size.get(str(sid))
                    if ps is None:
                        continue
                    delta = num(ps.get("price"))
                    delta = 0.0 if delta is None else float(delta)
                    variants.append(
                        (
                            sz_name,
                            crust_label,
                            round(max(0.0, base_price + delta), 2),
                        )
                    )

            if variants:
                return variants
        except Exception:
            pass  # fall through

    # ---------- 1) Explicit matrix if present ----------
    matrix = _try_price_matrix(config)
    if matrix:
        for (sz, cr), pr in matrix.items():
            variants.append((sz, cr, pr))
        return variants

    # ---------- 2) Group-based fallback ----------
    size_group = _find_group(config, SIZE_KEYS)
    crust_group = _find_group(config, CRUST_KEYS)

    size_opts = (size_group or {}).get("options") or []
    crust_opts = (crust_group or {}).get("options") or []

    size_price_abs: Dict[str, float] = {}
    size_delta: Dict[str, float] = {}

    if size_opts:
        raw_vals: List[Tuple[str, float]] = []
        for opt in size_opts:
            label = _canonical_size(_option_label(opt))
            vals = _option_price_fields(opt)
            if vals:
                raw_vals.append((label, vals[0]))
        if raw_vals:
            vals_only = [v for _, v in raw_vals]
            if any(v >= 6.0 for v in vals_only):
                for label, v in raw_vals:
                    size_price_abs[label] = v
            else:
                base = starting_price or 0.0
                for label, v in raw_vals:
                    size_delta[label] = v
                for label in set(l for l, _ in raw_vals):
                    size_price_abs[label] = base + size_delta.get(label, 0.0)

    if not size_price_abs and size_opts and starting_price is not None:
        # assume starting_price is for all sizes
        for opt in size_opts:
            label = _canonical_size(_option_label(opt))
            size_price_abs[label] = float(starting_price)

    crust_adj: Dict[str, float] = {}
    if crust_opts:
        for opt in crust_opts:
            label = _canonical_crust(_option_label(opt))
            vals = _option_price_fields(opt)
            if not vals:
                crust_adj[label] = 0.0
            else:
                cand = sorted(vals, key=lambda x: abs(x))[0]
                crust_adj[label] = 0.0 if cand >= 6.0 else cand

    if size_price_abs:
        if crust_opts:
            for sz, base_p in size_price_abs.items():
                for cr, adj in (crust_adj.items() or [("Regular Dough", 0.0)]):
                    variants.append((sz, cr, max(0.0, base_p + adj)))
        else:
            chosen_crust = _canonical_crust(fallback_crust or "Regular Dough")
            for sz, base_p in size_price_abs.items():
                variants.append((sz, chosen_crust, base_p))

    if not variants and starting_price is not None:
        default_size = "Small"
        if size_opts:
            default_size = _canonical_size(
                _option_label(size_opts[0]) or "Small"
            )
        chosen_crust = _canonical_crust(fallback_crust or "Regular Dough")
        variants.append((default_size, chosen_crust, float(starting_price)))

    return variants


# ---------------- key coding for uniqueness ----------------

def _size_code(sz: str) -> str:
    s = (sz or "").strip()
    if s.endswith('"') and s[:-1].isdigit():
        return f"{s[:-1]}IN"
    m = re.match(r'^(\d{2})"\s*$', s)
    if m:
        return f"{m.group(1)}IN"
    sl = s.lower()
    if sl == "x-large":
        return "XL"
    if sl in ("xxl", "x x l"):
        return "XXL"
    if sl == "large":
        return "LG"
    if sl == "medium":
        return "MD"
    if sl == "small":
        return "SM"
    if sl == "party":
        return "PARTY"
    if sl == "twin":
        return "TWIN"
    if sl == "single":
        return "SINGLE"
    return re.sub(r"[^A-Z0-9]+", "", s.upper())


def _crust_code(cr: str) -> str:
    c = (cr or "").strip().lower()
    if c in ("regular dough", "regular", "original", "classic"):
        return "REG"
    if "stuffed" in c:
        return "STUFFED"
    if "gourmet thin" in c or ("thin" in c and "crust" in c):
        return "THIN"
    if "brooklyn" in c:
        return "BROOKLYN"
    if "new york" in c or "new-york" in c:
        return "NY"
    if "cauliflower" in c:
        return "CAULI"
    if "gluten free" in c or "gluten-free" in c:
        return "GF"
    if "hand tossed" in c or "hand-tossed" in c:
        return "HANDTOSSED"
    return re.sub(r"[^A-Z0-9]+", "", (cr or "REG").upper()) or "REG"


def variant_product_key(pid: str, size: str, crust: str) -> str:
    """Create compact, stable, human-readable variant key."""
    return f"{pid}_{_size_code(size)}_{_crust_code(crust)}"


# ---------------- main worker ----------------

def scrape_store(store_id: int, today_local: dt.date) -> List[dict]:
    """
    Scrape a single Pizza Pizza store: meta + pizza products + configurator
    -> list of normalized rows ready for CSV.
    """
    meta = fetch_store_meta(store_id)
    if not meta:
        return []
    store_key, city, province = meta

    products = discover_pizza_products(store_id)
    if not products:
        return []

    rows: List[dict] = []
    seen_variant_keys: set[str] = set()  # de-dupe per store (pid,size,crust)

    for entry in products:
        pid = entry["pid"]
        slug = entry["slug"]
        name = entry["name"]
        start_price = entry["start_price"]
        cat_names = entry["cat_names"]

        recipe = (
            "CYO"
            if name.lower()
            in {"create your own", "create your own pizza"}
            else name
        )

        # Contextual crust inference from categories + recipe (fallback only)
        context_crust = infer_crust_from_context(recipe, cat_names)

        config = fetch_config(store_id, slug) if slug else None
        variants = parse_config_prices(
            config or {},
            start_price,
            fallback_crust=context_crust,
        )

        date_key = today_local.isoformat()
        month = today_local.strftime("%B")
        year = str(today_local.year)

        for sz, cr, price in variants:
            size_val = norm_label(sz) or "Small"
            crust_val = norm_label(cr) or "Regular Dough"
            pk = variant_product_key(pid, size_val, crust_val)

            if pk in seen_variant_keys:
                continue
            seen_variant_keys.add(pk)

            rows.append(
                {
                    "chain_key": CHAIN_KEY,
                    "store_key": f"PP_{store_key}",
                    "city": city,
                    "province": province,
                    "category": "Pizza",
                    "recipe": recipe,
                    "product_key": f"PP_{pk}",  # prefixed for pipeline
                    "size": size_val,
                    "crust": crust_val,
                    "price": f"{float(price):.2f}",
                    "date_key": date_key,
                    "month": month,
                    "year": year,
                }
            )

    return rows


# ---------------- entrypoint ----------------

def main() -> None:
    global SESSION_TOKEN

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    ap = argparse.ArgumentParser(
        description=(
            "Pizza Pizza -> Pizza-only CSV scraper "
            "(configurator-aware, with crust upcharges, PP prefixes)."
        )
    )
    ap.add_argument(
        "-o",
        "--outfile",
        default=None,
        help=(
            "CSV output path "
            "(default: data/raw/menu_pizzapizza_ca_menu_YYYYMMDD.csv)"
        ),
    )
    ap.add_argument(
        "-p",
        "--pool",
        type=int,
        default=POOL_SIZE_DEFAULT,
        help=f"Concurrent stores (default {POOL_SIZE_DEFAULT})",
    )
    ap.add_argument(
        "--ids",
        default=None,
        help='Store ids: "1-2000", "1,2,3", or "10". Omit to use default range.',
    )
    ap.add_argument(
        "--token",
        default=None,
        help="Override session token (pp-mw-session). If omitted, uses env/default.",
    )
    args = ap.parse_args()

    if args.token:
        SESSION_TOKEN = args.token
        if hasattr(_thread, "s"):
            delattr(_thread, "s")  # force new session with new token

    ids = list(parse_id_range(args.ids))
    pool = max(1, args.pool)
    today_local = today_toronto()
    today_tag = today_local.strftime("%Y%m%d")

    out_cols = [
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

    default_out = OUTPUT_DIR / f"menu_pizzapizza_ca_menu_{today_tag}.csv"
    outfile = Path(args.outfile) if args.outfile else default_out

    all_rows: List[dict] = []
    print(
        f"Starting Pizza Pizza (Pizza only) scrape… "
        f"stores={len(ids)} pool={pool}"
    )

    with ThreadPoolExecutor(max_workers=pool) as ex:
        futs = {
            ex.submit(scrape_store, sid, today_local): sid for sid in ids
        }
        for fut in tqdm(
            as_completed(futs),
            total=len(futs),
            desc="Stores",
        ):
            try:
                all_rows.extend(fut.result())
            except Exception:
                # keep going if a single store fails
                pass

    # FINAL FILTER: drop any rows whose product_key contains "product" (case-insensitive)
    filtered_rows = [
        r
        for r in all_rows
        if "product"
        not in str(r.get("product_key", "")).lower()
    ]

    with outfile.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_cols)
        writer.writeheader()
        for r in filtered_rows:
            writer.writerow({k: r.get(k, "") for k in out_cols})

    stores_done = len({r["store_key"] for r in filtered_rows})
    print(
        f"DONE → {outfile}  rows={len(filtered_rows)}  "
        f"stores={stores_done}  pool={pool}"
    )


if __name__ == "__main__":
    main()
