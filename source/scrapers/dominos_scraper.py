#!/usr/bin/env python3
"""
Domino's Canada – Full Menu Scraper (Pizza Only)

Purpose
-------
Scrape the full Domino's Canada menu for all stores, extract pizza-only
items, and export an analytics-ready CSV with standardized columns:

    chain_key, store_key, city, province, category,
    recipe, product_key, size, crust, price,
    date_key, month, year

The script is designed to be:
- Robust to messy / nested JSON from Domino's "power" API.
- Reusable as part of a pricing analytics pipeline (e.g., DuckDB + Power BI).

Usage
-----
    python dominos_scraper.py

Output
------
A CSV file in the local `data/raw/` folder:

    data/raw/menu_dominos_ca_menu_<YYYYMMDD>.csv

Dependencies
------------
- requests
- pandas
- tqdm

"""

from __future__ import annotations

import concurrent.futures
import datetime
import html
import os
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
import requests
from tqdm import tqdm

# -------------------- Configuration --------------------

BASE_URL = "https://order.dominos.ca/power"
STORES_URL = "https://pizza.dominos.ca/"
MAX_WORKERS = 15
TIMEOUT = 20

# Constant chain key for Domino's
CHAIN_KEY = "DP"

# Portfolio-friendly output: relative path inside the repo
OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# -------------------- Tiny utilities --------------------


def tidy(t: Any) -> str:
    """Normalise whitespace/Unicode and strip."""
    if t is None:
        return ""
    return re.sub(
        r"\s+",
        " ",
        unicodedata.normalize("NFKC", html.unescape(str(t))),
    ).strip()


def as_float(x: Any) -> Optional[float]:
    try:
        return float(str(x).replace(",", ""))
    except Exception:
        return None


def price_of(d: dict) -> Any:
    """
    Best-effort extraction of a price from a nested dict.

    Domino's structures price information in a variety of ways; this
    function tries multiple common patterns and returns either a float
    or "" if nothing reliable is found.
    """
    if not isinstance(d, dict):
        return ""

    if "Price" in d:
        f = as_float(d["Price"])
        if f is not None:
            return f

    for k in ("Prices", "Pricing", "PriceInfo", "Amount", "Amounts"):
        if k in d:
            v = d[k]
            if isinstance(v, dict):
                for val in v.values():
                    f = as_float(val)
                    if f is not None:
                        return f
            else:
                f = as_float(v)
                if f is not None:
                    return f

    for k in ("PriceCents", "AmountCents", "Cents"):
        if k in d:
            f = as_float(d[k])
            if f is not None:
                return round(f / 100.0, 2)

    return ""


def extract_province(flat: dict) -> str:
    for k, v in flat.items():
        if (
            isinstance(v, str)
            and len(v.strip()) == 2
            and any(x in k.lower() for x in ("province", "region", "state"))
        ):
            return v.strip()
    return ""


def extract_city(flat: dict) -> str:
    for k, v in flat.items():
        if isinstance(v, str) and "city" in k.lower():
            return tidy(v)
    return ""


def extract_store_name(flat: dict) -> str:
    for key in ("StoreName", "PublicName", "StorePublicName", "Name", "profile_name"):
        if key in flat and isinstance(flat[key], str):
            return tidy(flat[key])

    for k, v in flat.items():
        if isinstance(v, str) and ("store" in k.lower() or "name" in k.lower()):
            return tidy(v)
    return ""


# -------- size/crust parsing from names/codes --------

CRUST_PATTERNS = [
    "Parmesan Stuffed Crust",
    "Crunchy Thin Crust",
    "New-York Style",
    "Hand Tossed",
    "Brooklyn",
    "Gluten Free",
    "Pan Pizza",
]
CRUST_PATTERNS_LOWER = [c.lower() for c in CRUST_PATTERNS]

PIZZAISH_RX = re.compile(r"(pizza|crust|hand\s*tossed|thin|brooklyn|new-?york|fingers)", re.I)
PIECEY_NAME_RX = re.compile(r"(stix|sticks|bread|bites|brownie|wings|fingers)", re.I)
BEV_NAME_RX = re.compile(r"(coke|sprite|ginger|fanta|root\s*beer|water|fuz|zero|diet)", re.I)


def parse_crust_from_name(name: str) -> str:
    low = name.lower()
    for pat, pat_low in zip(CRUST_PATTERNS, CRUST_PATTERNS_LOWER):
        if pat_low in low:
            return pat
    return ""


def parse_size_from_text(name: str) -> str:
    if not name:
        return ""
    s = name.strip()

    # 10"
    m = re.search(r'\b(\d{1,2})\s*(?:"|”)\s*(?=\D|$)', s)
    if m:
        return f'{m.group(1)}"'

    # 10 inch
    m = re.search(r"\b(\d{1,2})\s*(?:inch|in)\b", s, flags=re.I)
    if m:
        return f'{m.group(1)}"'

    # 500 mL
    m = re.search(r"\b(\d{2,4})\s*m\s*l\b", s, flags=re.I)
    if m:
        return f"{m.group(1)} mL"

    # 2 litre / 2 liter
    m = re.search(
        r"\b(\d+(?:\.\d+)?)\s*l(?:itre|iter|iters|itres)?\b",
        s,
        flags=re.I,
    )
    if m:
        return f"{m.group(1)} L"

    # 2 L
    m = re.search(r"\b(\d+(?:\.\d+)?)\s*l\b", s, flags=re.I)
    if m:
        return f"{m.group(1)} L"

    # 8-Piece / 8 pc / 8 pieces
    m = re.search(r"\b(\d{1,3})\s*-\s*piece\b", s, flags=re.I)
    if m:
        return f"{m.group(1)}-Piece"

    m = re.search(r"\b(\d{1,3})\s*pc\b", s, flags=re.I)
    if m:
        return f"{m.group(1)} pc"

    m = re.search(r"\b(\d{1,3})\s*pieces\b", s, flags=re.I)
    if m:
        return f"{m.group(1)}-Piece"

    return ""


def parse_size_from_code(code: str, v_name: str, p_name: str) -> str:
    if not code:
        return ""
    code = str(code)

    # 12SOMETHING -> 12"
    m = re.match(r"^(\d{1,2})(?=[A-Z])", code)
    if m:
        n = int(m.group(1))
        if 6 <= n <= 20 and (
            PIZZAISH_RX.search(v_name)
            or PIZZAISH_RX.search(p_name)
            or "GARFIN" in code
        ):
            return f'{n}"'

    # B8PC..., 12PC...
    m = re.search(r"(\d{1,3})PC", code, flags=re.I)
    if m:
        return f"{m.group(1)} pc"

    if PIECEY_NAME_RX.search(v_name) or PIECEY_NAME_RX.search(p_name):
        # 6MARBRWNE, 8BRDSTIX
        m = re.match(r"^(\d{1,3})", code)
        if m:
            return f"{m.group(1)} pc"
        # CINNASTIX8
        m = re.search(r"(\d{1,3})$", code)
        if m:
            return f"{m.group(1)} pc"

    if BEV_NAME_RX.search(v_name) or BEV_NAME_RX.search(p_name):
        # 500COKE
        m = re.match(r"^(\d{3,4})", code)
        if m:
            return f"{m.group(1)} mL"
        # 2LCOKE
        m = re.match(r"^(\d+(?:\.\d+)?)L", code, flags=re.I)
        if m:
            return f"{m.group(1)} L"

    return ""


def fallback_size_from_keywords(name: str) -> str:
    if not name:
        return ""
    if re.search(r"\bcup\b", name, flags=re.I):
        return "Cup"
    if re.search(r"\bbag\b", name, flags=re.I):
        return "Bag"
    return ""


def _codes_from_mixed(value: Any) -> List[str]:
    out: List[str] = []
    if value is None:
        return out

    if isinstance(value, list):
        for x in value:
            if isinstance(x, str):
                out.append(x)
            elif isinstance(x, dict):
                code = (
                    x.get("Code")
                    or x.get("ProductCode")
                    or x.get("Id")
                    or x.get("ID")
                )
                if code:
                    out.append(str(code))

    elif isinstance(value, dict):
        out.extend([str(k) for k in value.keys()])

    elif isinstance(value, str):
        out.append(value)

    return out


# ------------- Category collection (product OR variant codes) -------------


def collect_category_index(menu: dict) -> Tuple[Dict[str, set], Dict[str, set]]:
    code_to_cat_names: Dict[str, set] = {}
    code_to_cat_codes: Dict[str, set] = {}

    def add_map(code: str, cname: str, ccode: str) -> None:
        if not code:
            return
        code_to_cat_names.setdefault(code, set()).add(tidy(cname))
        code_to_cat_codes.setdefault(code, set()).add(tidy(ccode))

    def walk_category(cat: dict) -> None:
        if not isinstance(cat, dict):
            return
        cname = tidy(cat.get("Name", ""))
        ccode = tidy(cat.get("Code", "") or cat.get("Id", "") or "")

        for key in (
            "Products",
            "Items",
            "FeaturedProducts",
            "PopularProducts",
            "RecommendedProducts",
        ):
            for code in _codes_from_mixed(cat.get(key)):
                add_map(code, cname, ccode)

        for child_key in ("Categories", "Children", "Subcategories"):
            kids = cat.get(child_key)
            if isinstance(kids, list):
                for ch in kids:
                    walk_category(ch)

    cat = (menu or {}).get("Categorization") or {}
    for section in ("Food", "PreconfiguredProducts"):
        node = cat.get(section)
        if isinstance(node, dict) and isinstance(node.get("Categories"), list):
            for c in node["Categories"]:
                walk_category(c)

    return code_to_cat_names, code_to_cat_codes


# ------------- Products & Variants -------------


def collect_products_and_variants(
    menu: dict,
) -> Tuple[Dict[str, dict], Dict[str, dict], Dict[str, List[str]]]:
    products_by_code: Dict[str, dict] = {}
    variants_by_code: Dict[str, dict] = {}
    product_to_variants: Dict[str, List[str]] = {}

    if isinstance(menu.get("Products"), dict):
        for pcode, prod in menu["Products"].items():
            if isinstance(prod, dict):
                products_by_code[str(pcode)] = prod

    PRODUCT_HINT_KEYS = {
        "AvailableToppings",
        "DefaultToppings",
        "ProductType",
        "DefaultSides",
        "Variants",
    }
    VARIANT_HINT_KEYS = {
        "Price",
        "PriceInfo",
        "Size",
        "Crust",
        "VariantCode",
        "VariantName",
        "CrustName",
        "SizeName",
        "BreadType",
        "Portion",
        "PortionName",
        "Flavor",
        "FlavorCode",
    }

    def add_variant(vcode: Optional[str], v: dict, pcode: Optional[str]) -> None:
        if not pcode:
            return
        if vcode:
            vcode = str(vcode)
        else:
            # synthesize a variant code if missing
            idx = len(product_to_variants.get(str(pcode), []))
            vcode = f"{pcode}__v{idx + 1:03d}"
            v = dict(v)
            v["__Synthesized"] = True

        variants_by_code[vcode] = v
        product_to_variants.setdefault(str(pcode), []).append(vcode)

    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
            has_product_code = "ProductCode" in obj
            looks_like_product = (not has_product_code) and any(
                k in obj for k in PRODUCT_HINT_KEYS
            )
            looks_like_variant = has_product_code and any(
                k in obj for k in VARIANT_HINT_KEYS
            )

            if looks_like_product:
                pcode = obj.get("Code") or obj.get("Id") or obj.get("ID")
                if pcode:
                    pcode = str(pcode)
                    if pcode not in products_by_code:
                        products_by_code[pcode] = obj
                    else:
                        # fill in any missing fields from this occurrence
                        for k, v in obj.items():
                            if products_by_code[pcode].get(k) in (None, ""):
                                products_by_code[pcode][k] = v

            if looks_like_variant:
                add_variant(
                    obj.get("Code") or obj.get("VariantCode"),
                    obj,
                    obj.get("ProductCode"),
                )

            if "Variants" in obj and isinstance(
                obj["Variants"], (list, dict)
            ):
                pcode_inline = (
                    obj.get("Code")
                    or obj.get("Id")
                    or obj.get("ID")
                    or obj.get("ProductCode")
                )
                if isinstance(obj["Variants"], list):
                    for v in obj["Variants"]:
                        if isinstance(v, dict):
                            add_variant(
                                v.get("Code") or v.get("VariantCode"),
                                v,
                                v.get("ProductCode") or pcode_inline,
                            )
                else:
                    for vc, v in obj["Variants"].items():
                        if isinstance(v, dict):
                            add_variant(
                                vc,
                                v,
                                v.get("ProductCode") or pcode_inline,
                            )

            for _, v in obj.items():
                walk(v)

        elif isinstance(obj, list):
            for el in obj:
                walk(el)

    walk(menu)

    # de-dupe per product
    for p, lst in list(product_to_variants.items()):
        seen: set[str] = set()
        uniq: List[str] = []
        for vc in lst:
            if vc not in seen:
                uniq.append(vc)
                seen.add(vc)
        product_to_variants[p] = uniq

    return products_by_code, variants_by_code, product_to_variants


# ----- size derivation -----


def derive_size(
    v: dict, prod: dict, v_name: str, p_name: str, vcode: str
) -> str:
    size = tidy(v.get("Size") or v.get("SizeName") or "")
    if not size:
        size = tidy(prod.get("Size") or prod.get("SizeName") or "")
    if size:
        return size

    size = parse_size_from_text(v_name) or parse_size_from_text(p_name)
    if size:
        return size

    size = parse_size_from_code(vcode or "", v_name, p_name)
    if size:
        return size

    size = fallback_size_from_keywords(v_name) or fallback_size_from_keywords(
        p_name
    )
    return size


# -------------------- per-store scrape --------------------

WORK_COLS = [
    "store_id",
    "store_name",
    "city",
    "province",
    "category_names",
    "category_codes",
    "product_code",
    "product_name",
    "product_desc",
    "variant_code",
    "variant_name",
    "size",
    "crust",
    "flavor",
    "portion",
    "price",
]


def collect_category_strings(
    pcode: str,
    v_codes: List[str],
    code_to_cat_names: Dict[str, set],
    code_to_cat_codes: Dict[str, set],
) -> Tuple[str, str]:
    cat_names = set(code_to_cat_names.get(pcode, set()))
    cat_codes = set(code_to_cat_codes.get(pcode, set()))
    for vcode in v_codes:
        cat_names |= set(code_to_cat_names.get(vcode, set()))
        cat_codes |= set(code_to_cat_codes.get(vcode, set()))
    return (
        " | ".join(sorted([c for c in cat_names if c])),
        " | ".join(sorted([c for c in cat_codes if c])),
    )


def scrape_store(store_id: int) -> List[dict]:
    """
    Scrape a single Domino's store and return a list of work-rows.
    """
    try:
        menu = requests.get(
            f"{BASE_URL}/store/{store_id}/menu?lang=en&structured=true",
            timeout=TIMEOUT,
        ).json()
        profile = requests.get(
            f"{BASE_URL}/store/{store_id}/profile",
            timeout=TIMEOUT,
        ).json()
        flat = pd.json_normalize(profile, sep="_").iloc[0].to_dict()

        province = extract_province(flat)
        city = extract_city(flat)
        store_name = extract_store_name(flat)

        products_by_code, variants_by_code, product_to_variants = (
            collect_products_and_variants(menu)
        )
        code_to_cat_names, code_to_cat_codes = collect_category_index(menu)

        rows: List[dict] = []
        prod_codes = set(products_by_code.keys()) | {
            str(v.get("ProductCode"))
            for v in variants_by_code.values()
            if v.get("ProductCode")
        }

        for pcode in sorted(prod_codes):
            prod = products_by_code.get(pcode, {})
            p_name = tidy(prod.get("Name") or prod.get("ProductName") or "")
            p_desc = tidy(prod.get("Description") or "")
            v_codes = product_to_variants.get(pcode, [])
            cat_names_str, cat_codes_str = collect_category_strings(
                pcode, v_codes, code_to_cat_names, code_to_cat_codes
            )

            # No-variant products
            if not v_codes:
                price = price_of(prod)
                size = derive_size({}, prod, "", p_name, "")
                crust = (
                    tidy(
                        prod.get("Crust")
                        or prod.get("CrustName")
                        or prod.get("BreadType")
                        or ""
                    )
                    or parse_crust_from_name(p_name)
                )
                flavor = tidy(
                    prod.get("Flavor")
                    or prod.get("FlavorName")
                    or prod.get("FlavorCode")
                    or ""
                )
                rows.append(
                    {
                        "store_id": store_id,
                        "store_name": store_name,
                        "city": city,
                        "province": province,
                        "category_names": cat_names_str,
                        "category_codes": cat_codes_str,
                        "product_code": pcode,
                        "product_name": p_name,
                        "product_desc": p_desc,
                        "variant_code": "",
                        "variant_name": "",
                        "size": size,
                        "crust": crust,
                        "flavor": flavor,
                        "portion": tidy(prod.get("Portion") or ""),
                        "price": price if price != "" else "",
                    }
                )
                continue

            # Products with variants
            for vcode in v_codes:
                v = variants_by_code.get(vcode, {}) if vcode else {}
                v_name = tidy(
                    v.get("Name") or v.get("VariantName") or ""
                )

                size = derive_size(v, prod, v_name, p_name, vcode)
                crust = tidy(
                    v.get("Crust")
                    or v.get("CrustName")
                    or v.get("BreadType")
                    or ""
                )
                if not crust:
                    crust = (
                        tidy(
                            prod.get("Crust")
                            or prod.get("CrustName")
                            or prod.get("BreadType")
                            or ""
                        )
                        or parse_crust_from_name(v_name)
                        or parse_crust_from_name(p_name)
                    )
                portion = tidy(
                    v.get("Portion")
                    or v.get("PortionName")
                    or ""
                )
                flavor = tidy(
                    v.get("Flavor")
                    or v.get("FlavorName")
                    or v.get("FlavorCode")
                    or prod.get("Flavor")
                    or ""
                )
                price = price_of(v) if v else ""
                if price == "":
                    price = price_of(prod)

                rows.append(
                    {
                        "store_id": store_id,
                        "store_name": store_name,
                        "city": city,
                        "province": province,
                        "category_names": cat_names_str,
                        "category_codes": cat_codes_str,
                        "product_code": pcode,
                        "product_name": p_name,
                        "product_desc": p_desc,
                        "variant_code": vcode,
                        "variant_name": v_name,
                        "size": size,
                        "crust": crust,
                        "flavor": flavor,
                        "portion": portion,
                        "price": price,
                    }
                )

        # basic keep rule
        cleaned: List[dict] = []
        for r in rows:
            if any(
                [
                    r.get("product_name"),
                    r.get("variant_name"),
                    r.get("price") not in ("", None),
                ]
            ):
                cleaned.append({k: r.get(k, "") for k in WORK_COLS})
        return cleaned

    except Exception as e:  # pragma: no cover - defensive logging
        sys.stderr.write(f"[store {store_id}] error: {e}\n")
        return []


# -------------------- main orchestration --------------------


def main() -> None:
    today = datetime.date.today()
    today_tag = f"{today:%Y%m%d}"
    out_file = OUTPUT_DIR / f"menu_dominos_ca_menu_{today_tag}.csv"

    print("Fetching Domino's store list …")
    html_txt = requests.get(STORES_URL, timeout=TIMEOUT).text
    store_ids = sorted(set(map(int, re.findall(r"\(#(\d{4,5})\)", html_txt))))
    print(f"→ {len(store_ids):,} stores\n")

    all_rows: List[dict] = []
    with concurrent.futures.ThreadPoolExecutor(
        MAX_WORKERS
    ) as executor:
        for batch in tqdm(
            executor.map(scrape_store, store_ids),
            total=len(store_ids),
            unit="store",
        ):
            all_rows.extend(batch)

    df = pd.DataFrame(all_rows)

    # ---- Pizza-only filter + normalize category ----
    if not df.empty:
        mask = df["category_names"].str.contains(
            "pizza", case=False, na=False
        )
        df = df.loc[mask].copy()
        df.loc[:, "category_names"] = "Pizza"

    # ---- Keep only requested columns and rename ----
    df = df[
        [
            "store_id",
            "city",
            "province",
            "category_names",
            "product_name",
            "variant_code",
            "size",
            "flavor",
            "price",
        ]
    ].rename(
        columns={
            "store_id": "store_key",
            "category_names": "category",
            "product_name": "recipe",
            "variant_code": "product_key",
            "flavor": "crust",
        }
    )

    # ---- Recipe exact "Pizza" -> "CYO" ----
    if not df.empty:
        mask_exact_pizza = df["recipe"].fillna("").str.strip().eq("Pizza")
        df.loc[mask_exact_pizza, "recipe"] = "CYO"

    # ---- Add date facts ----
    df["date_key"] = today.strftime("%Y-%m-%d")
    df["month"] = today.strftime("%B")  # use "%m" for 01-12 if preferred
    df["year"] = today.strftime("%Y")

    # ---- Add chain_key + prefixes ----
    df["chain_key"] = CHAIN_KEY
    df["store_key"] = "DP_" + df["store_key"].astype(str)

    non_empty_pk = df["product_key"].notna() & (
        df["product_key"].astype(str).str.len() > 0
    )
    df.loc[non_empty_pk, "product_key"] = (
        "DP_" + df.loc[non_empty_pk, "product_key"].astype(str)
    )

    # ---- Final column order ----
    df = df[
        [
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
    ]

    df.to_csv(out_file, index=False, encoding="utf-8-sig")

    print("✓ Domino's menu (pizza only) written →", out_file)
    if not df.empty:
        print(
            f"Rows: {len(df):,} | Stores: {df['store_key'].nunique()} "
            f"| Recipes: {df['recipe'].nunique()} "
            f"| Product Keys: {df['product_key'].nunique()}"
        )


if __name__ == "__main__":
    main()
