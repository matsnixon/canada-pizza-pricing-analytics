#!/usr/bin/env python3
"""
Pizza Price Analytics – DuckDB Dimensional Model Builder

Purpose
-------
Build a simple star schema from the raw menu CSVs produced by the
scrapers (Domino’s / Pizza Hut / Pizza Pizza) using DuckDB, and export
clean dimension + fact tables as CSV files.

Input
-----
All CSVs in:

    data/raw/*.csv

(Each file is expected to follow the unified schema:
 chain_key, store_key, city, province, category,
 recipe, product_key, size, crust, price, date_key, month, year)

Output
------
Dimension CSVs:

    data/clean/dimensions/dim_chain.csv
    data/clean/dimensions/dim_store.csv
    data/clean/dimensions/dim_product.csv
    data/clean/dimensions/dim_date.csv

Fact CSV:

    data/clean/fact/fact_menu_price.csv

Usage
-----
    python model.py

This script:
- Reads all raw CSVs with DuckDB (union_by_name).
- Applies light type casting / cleanup.
- Builds dim_chain, dim_store, dim_product, dim_date.
- Builds fact_menu_price referencing those dimensions via hashed keys.
- Exports each table to the clean folders for downstream BI (e.g. Power BI).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

# Ensure duckdb is available ---------------------------------------------------
try:
    import duckdb  # type: ignore
except ImportError:  # pragma: no cover - install-on-first-run
    subprocess.check_call([sys.executable, "-m", "pip", "install", "duckdb"])
    import duckdb  # type: ignore


# ---- Paths (repo-relative; adjust if your layout differs) --------------------

RAW_DIR = Path("data/raw")
DIM_DIR = Path("data/clean/dimensions")
FACT_DIR = Path("data/clean/fact")

DIM_DIR.mkdir(parents=True, exist_ok=True)
FACT_DIR.mkdir(parents=True, exist_ok=True)


def build_model(
    raw_dir: Path = RAW_DIR,
    dim_dir: Path = DIM_DIR,
    fact_dir: Path = FACT_DIR,
) -> None:
    """
    Build dimensions + fact from raw CSVs and export them as clean CSV files.
    """

    # Normalize to forward slashes for DuckDB's globbing
    raw_glob = str(raw_dir / "*.csv").replace("\\", "/")
    dim_dir_str = str(dim_dir).replace("\\", "/")
    fact_dir_str = str(fact_dir).replace("\\", "/")

    sql = f"""
    -- Stage: read ALL raw CSVs (Pizza Pizza, Domino's, Pizza Hut) from the Raw folder
    CREATE OR REPLACE VIEW stg AS
    SELECT *
    FROM read_csv_auto('{raw_glob}', union_by_name=TRUE, header=TRUE)
    -- Global cleanup rule already used in your scrapers, but keep it again for safety:
    WHERE lower(coalesce(product_key, '')) NOT LIKE '%product%';

    -- Cast/standardize types (helps EXTRACT/strftime and numeric ops)
    CREATE OR REPLACE VIEW stg_typed AS
    SELECT
      chain_key,
      CAST(store_key   AS VARCHAR) AS store_key,
      CAST(city        AS VARCHAR) AS city,
      CAST(province    AS VARCHAR) AS province,
      CAST(category    AS VARCHAR) AS category,
      CAST(recipe      AS VARCHAR) AS recipe,
      CAST(product_key AS VARCHAR) AS product_key,
      CAST(size        AS VARCHAR) AS size,
      CAST(crust       AS VARCHAR) AS crust,
      TRY_CAST(price   AS DOUBLE)  AS price,
      TRY_CAST(date_key AS DATE)   AS date_key,
      CAST(month       AS VARCHAR) AS month,
      CAST(year        AS VARCHAR) AS year
    FROM stg;

    -- Dimensions -------------------------------------------------------------

    CREATE OR REPLACE TABLE dim_chain AS
    SELECT DISTINCT
      hash(chain_key) AS chain_id,
      chain_key
    FROM stg_typed
    WHERE chain_key IS NOT NULL;

    CREATE OR REPLACE TABLE dim_store AS
    SELECT DISTINCT
      hash(chain_key, store_key) AS store_id,
      hash(chain_key)            AS chain_id,
      store_key,
      city,
      province
    FROM stg_typed;

    CREATE OR REPLACE TABLE dim_product AS
    SELECT DISTINCT
      hash(chain_key, product_key) AS product_id,
      hash(chain_key)              AS chain_id,
      product_key,
      recipe,
      size,
      crust,
      category
    FROM stg_typed;

    CREATE OR REPLACE TABLE dim_date AS
    SELECT DISTINCT
      hash(date_key)                    AS date_id,
      date_key,
      EXTRACT(YEAR  FROM date_key)::INT AS year_num,
      EXTRACT(MONTH FROM date_key)::INT AS month_num,
      strftime(date_key, '%B')          AS month_name,
      EXTRACT(DAY   FROM date_key)::INT AS day_num,
      EXTRACT(DOW   FROM date_key)::INT AS dow_num
    FROM stg_typed
    WHERE date_key IS NOT NULL;

    -- Fact -------------------------------------------------------------------

    CREATE OR REPLACE TABLE fact_menu_price AS
    SELECT
      hash(chain_key)              AS chain_id,
      hash(chain_key, store_key)   AS store_id,
      hash(chain_key, product_key) AS product_id,
      hash(date_key)               AS date_id,
      price
    FROM stg_typed
    WHERE price IS NOT NULL
      AND date_key IS NOT NULL;

    -- Export CSVs to Clean folders -------------------------------------------

    COPY (
      SELECT * FROM dim_chain ORDER BY chain_id
    ) TO '{dim_dir_str}/dim_chain.csv'
      WITH (HEADER, DELIMITER ',');

    COPY (
      SELECT * FROM dim_store ORDER BY store_id
    ) TO '{dim_dir_str}/dim_store.csv'
      WITH (HEADER, DELIMITER ',');

    COPY (
      SELECT * FROM dim_product ORDER BY product_id
    ) TO '{dim_dir_str}/dim_product.csv'
      WITH (HEADER, DELIMITER ',');

    COPY (
      SELECT * FROM dim_date ORDER BY date_key
    ) TO '{dim_dir_str}/dim_date.csv'
      WITH (HEADER, DELIMITER ',');

    COPY (
      SELECT * FROM fact_menu_price
    ) TO '{fact_dir_str}/fact_menu_price.csv'
      WITH (HEADER, DELIMITER ',');
    """

    con = duckdb.connect(database=":memory:")
    con.execute(sql)
    con.close()

    print("✅ Built dimensions & fact and exported CSVs to:")
    print("  DIMS →", dim_dir)
    print("  FACT →", fact_dir)


def main() -> None:
    build_model()


if __name__ == "__main__":
    main()
