# Pizza Price Analytics – Canada

End-to-end pricing analytics pipeline for major pizza chains in Canada (Domino’s, Pizza Hut, Pizza Pizza).  
Python scrapers collect live menu prices, a SQL-style data model in DuckDB standardizes them, and a Power BI dashboard compares pricing by size, recipe, and region to surface pricing and tiering opportunities.

---

## Tech Stack

- **Python** – web scraping & data cleaning (`requests`, `pandas`, concurrency)
- **DuckDB / SQL** – in-process data modelling (dimensions + fact table)
- **Power BI** – interactive pricing dashboard and insights
- **Git / GitHub** – version control and portfolio hosting

---

## What’s in the Repo

- `src/dominos_scraper.py`  
  Domino’s Canada pizza menu scraper. Scrapes store-level menus, normalizes size/crust, and outputs a unified schema.

- `src/pizza_hut_scraper.py`  
  Pizza Hut Canada pizza menu scraper. Uses a throttling-aware “blast” pattern to pull pizzas for all huts across sectors.

- `src/pizza_pizza_scraper.py`  
  Pizza Pizza Canada pizza scraper. Configurator-aware: expands size × crust combinations and applies crust upcharges before writing prices.

- `src/model.py`  
  DuckDB model that unions all raw CSVs, builds dimensions + fact (`dim_chain`, `dim_store`, `dim_product`, `dim_date`, `fact_menu_price`), and exports clean CSVs.

- `data/raw/`  
  Raw CSV exports from scrapers (gitignored). Each file has the same column schema.

- `data/clean/dimensions/`  
  Clean dimension tables (`dim_chain`, `dim_store`, `dim_product`, `dim_date`).

- `data/clean/fact/`  
  Fact table (`fact_menu_price`) ready for BI tools.

- `dashboard/canada_pizza_pricing_dashboard.pdf`  
  Power BI dashboard export based on the star schema.

- `assets/canada_pizza_pricing_model_schema.png`  
  Power BI model view screenshot showing the schema (chains → stores → products → dates).

---

## Data Pipeline

### 1. Data Collection (Python scrapers)

Scrapers for **Domino’s**, **Pizza Hut**, and **Pizza Pizza** pull:

- Store metadata: `store_key`, `city`, `province`
- Product information: `recipe`, `size`, `crust`, `product_key`
- List prices by store and day

All three scrapers write out a **unified schema** to `data/raw/`:

```text
chain_key, store_key, city, province, category,
recipe, product_key, size, crust, price,
date_key, month, year
```

Pizza Pizza is scraped via the **configurator API**, expanding all size × crust combinations and applying crust upcharges before writing final prices.

Each script:

- Normalizes **size** (e.g. `12"`, `Medium`, `X-Large`) into consistent tokens.
- Normalizes **crust** labels (`Regular Dough`, `Hand Tossed`, `Thin ’n’ Crispy`, etc.).
- Derives a stable, chain-scoped **`product_key`** to identify unique variants.
- Tags data by date (`date_key`, `month`, `year`) for time-based analysis.

Output examples (written to `data/raw/`):

- `menu_dominos_ca_menu_YYYYMMDD.csv`
- `pizzahut_pizza_YYYYMMDD.csv`
- `menu_pizzapizza_ca_menu_YYYYMMDD.csv`

---

### 2. Data Modelling (DuckDB + SQL)

`src/model.py` loads **all** raw CSVs (`data/raw/*.csv`) into DuckDB and builds a simple star schema for analytics.

#### 2.1 Ingest & Clean

- Uses `read_csv_auto(..., union_by_name=TRUE)` to union all raw files by column name.
- Re-applies a global safety filter to drop any rows where `product_key` contains `"product"` (defensive clean-up).
- Casts key fields into consistent types:
  - `price` → `DOUBLE`
  - `date_key` → `DATE`
  - Strings → `VARCHAR`

#### 2.2 Dimensions

- `dim_chain`  
  - Grain: chain (Domino’s, Pizza Hut, Pizza Pizza)  
  - Columns:
    - `chain_id` (hash of `chain_key`)
    - `chain_key`

- `dim_store`  
  - Grain: chain + store  
  - Columns:
    - `store_id` (hash of `chain_key`, `store_key`)
    - `chain_id`
    - `store_key`
    - `city`
    - `province`

- `dim_product`  
  - Grain: chain + product variant (recipe × size × crust)  
  - Columns:
    - `product_id` (hash of `chain_key`, `product_key`)
    - `chain_id`
    - `product_key`
    - `recipe`
    - `size`
    - `crust`
    - `category`

- `dim_date`  
  - Grain: calendar day  
  - Columns:
    - `date_id` (hash of `date_key`)
    - `date_key`
    - `year_num`
    - `month_num`
    - `month_name`
    - `day_num`
    - `dow_num`

#### 2.3 Fact Table

- `fact_menu_price`  
  - Grain: **chain × store × product × date**  
  - Columns:
    - `chain_id`
    - `store_id`
    - `product_id`
    - `date_id`
    - `price`

All keys are generated using DuckDB’s `hash(...)` on business keys, which keeps the logic simple and reproducible across runs.

#### 2.4 Export

The script exports the tables to CSV:

```text
data/clean/dimensions/dim_chain.csv
data/clean/dimensions/dim_store.csv
data/clean/dimensions/dim_product.csv
data/clean/dimensions/dim_date.csv
data/clean/fact/fact_menu_price.csv
```

You can see the final model as used in Power BI in:

- `assets/canada_pizza_pricing_model_schema.png`

---

### 3. Visualization & Analysis (Power BI)

The clean DuckDB model is imported into **Power BI**, where the relationships mirror the star schema above.

Exported report:

- `dashboard/canada_pizza_pricing_dashboard.pdf`

Key report elements:

- **Median price by size & chain**  
  - Small, Medium, Large, X-Large price ladders by chain.

- **Median price gap by recipe (Medium pizzas)**  
  - Pizza Hut vs Domino’s vs Pizza Pizza by recipe.

- **Price premium table**  
  - Dollar and % premiums for Pizza Hut vs each competitor.

- Slicers for:
  - `size`, `crust`, `recipe`
  - `province`, `city`
  - `year`, `month`

This setup makes it easy to drill into specific combinations (e.g., “Medium pepperoni, Ontario only, last 3 months”).

---

## Key Insights from the Dashboard

From the current dataset and time period analysed:

- **Pizza Hut prices are above Domino’s and Pizza Pizza across all four core sizes.**
- **Medium and Large pizzas show the strongest premiums** for Pizza Hut:
  - Medium: roughly **+15.8% vs Domino’s** and **+19.6% vs Pizza Pizza**
  - Large: roughly **+13.0% vs Domino’s** and **+15.3% vs Pizza Pizza**
- **Price gaps are largest on entry recipes** (e.g., core pepperoni) and **narrow at the top end**, suggesting:
  - Entry products may be **over-premiumized** vs competitors.
  - There may be **insufficient price tiering** between entry and premium recipes.

These findings point to potential opportunities to:

- Revisit **entry price points** where competitive gaps are widest.
- Expand **tiering between entry and premium recipes** to match consumer willingness to pay.
- Use the dashboard as a starting point for **targeted price tests** by **size, recipe, and province**.
