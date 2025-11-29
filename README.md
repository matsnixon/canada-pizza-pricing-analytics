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

- `source/dominos_scraper.py` – Domino’s Canada pizza menu scraper  
- `source/pizza_hut_scraper.py` – Pizza Hut Canada pizza menu scraper  
- `source/pizza_pizza_scraper.py` – Pizza Pizza Canada pizza menu scraper (configurator-aware, crust upcharges)  
- `source/model.py` – DuckDB model that builds dim/fact tables from all raw CSVs  
- `data/raw/` – Raw CSV exports from scrapers (gitignored)  
- `data/clean/dimensions/` – `dim_chain`, `dim_store`, `dim_product`, `dim_date`  
- `data/clean/fact/` – `fact_menu_price`  
- `dashboard/canada_pizza_pricing_dashboard.pdf` – Power BI dashboard export  
- `dashboard/canada_pizza_pricing__schema.png` – Power BI data model (star schema)

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
