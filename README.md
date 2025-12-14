# Pizza Price Analytics – Canada

End-to-end pricing analytics pipeline for major pizza chains in Canada (**Domino’s, Pizza Hut, Pizza Pizza**).  
Python scrapers collect live menu prices, a simple star schema in DuckDB standardizes them, and a Power BI dashboard compares pricing by **size, recipe, chain, and region** to surface pricing and tiering opportunities.

---

## Project Highlights

- Built a **chain-agnostic price model** across 3 brands with different APIs and menu structures.
- Designed a **star schema** (stores, products, dates, fact_prices) for clean analysis in BI tools.
- Delivered a **Power BI dashboard** that quantifies price ladders and premiums by size and recipe.
- Framed findings as **pricing / margin opportunities**, not just charts.

---

## Tech Stack

- **Python** – web scraping & data prep (`requests`, `pandas`, concurrency)
- **DuckDB / SQL** – in-process data modelling (dimensions + fact table)
- **Power BI** – interactive pricing dashboard and insights
- **Git / GitHub** – version control and portfolio hosting

---

## Repository Structure

- `source/dominos_scraper.py`  
  Scrapes Domino’s Canada menus at the **store level**, normalizes size/crust, and writes a standard CSV.

- `source/pizza_hut_scraper.py`  
  Scrapes Pizza Hut Canada, handling **API throttling** and pulling pizzas for all huts across sectors.

- `source/pizza_pizza_scraper.py`  
  Scrapes Pizza Pizza using the **configurator API**, expanding size × crust combinations and applying crust upcharges before writing prices.

- `source/model.py`  
  DuckDB script that unions all raw CSVs, builds a **star schema**  
  (`dim_chain`, `dim_store`, `dim_product`, `dim_date`, `fact_menu_price`), and exports clean CSVs.

- `data/raw/`  
  Raw scraper outputs (gitignored). Each file uses a **unified schema**, e.g.:

    chain_key, store_key, city, province, category,
    recipe, product_key, size, crust, price,
    date_key, month, year

- `data/clean/dimensions/` & `data/clean/fact/`  
  Cleaned dimensions and fact table, ready for Power BI.

- `dashboard/canada_pizza_pricing_dashboard.pdf`  
  Export of the Power BI report (price ladders, gaps, and premiums).

- `dashboard/canada_pizza_pricing_model_schema.png`  
  Power BI model view showing the star schema (chains → stores → products → dates).

---

## How It Works (End-to-End)

1. **Scrape prices (Python)**  
   - Three separate scripts call each chain’s public endpoints to pull:
     - Store metadata: `store_key`, `city`, `province`
     - Product details: `recipe`, `size`, `crust`, `product_key`
     - List prices: one row per **store–product–day**
   - All scrapers output the **same column set**, so they can be combined.

2. **Model data (DuckDB / SQL)**  
   - `model.py` reads `data/raw/*.csv` into DuckDB.
   - Builds a **star schema**:
     - `dim_store` – store, city, province per chain  
     - `dim_product` – recipe, size, crust, category per chain  
     - `dim_date` – calendar attributes from `date_key`  
     - `fact_menu_price` – one row per chain–store–product–date price point
   - Exports the clean tables back to `data/clean/` for BI.

3. **Visualize & analyse (Power BI)**  
   - The star schema is imported into Power BI.
   - Measures and visuals focus on **median prices** and **price premiums** to avoid being skewed by outliers.

---

## Business Questions Answered

The dashboard is designed to help answer:

- **How do list prices compare by size across chains?**  
  Price ladders (Small, Medium, Large, X-Large) for each chain.

- **Where does Pizza Hut sit vs Domino’s and Pizza Pizza on core recipes?**  
  Median price gap by recipe (e.g., pepperoni, supreme) at a fixed size.

- **How large are Pizza Hut’s price premiums?**  
  Dollar and % price premiums vs Domino’s and Pizza Pizza by size and recipe.

- **Are entry products over- or under-premiumized?**  
  Compare gaps on entry recipes vs premium SKUs to judge tiering.

---

## Example Insights

From one snapshot of the data:

- Pizza Hut’s prices are **above Domino’s and Pizza Pizza** across the four core pizza sizes.
- **Medium and Large pizzas show the strongest premiums** for Pizza Hut:
  - Medium: roughly **+15–20%** vs competitors  
  - Large: roughly **+13–15%** vs competitors
- **Price gaps are widest on entry recipes** and **narrow at the top end**, suggesting:
  - Entry products may be **over-premiumized** vs the market.
  - There is room to improve **price tiering** between entry and premium recipes.

These outputs can feed into:

- Re-setting **entry price points** where gaps are widest.
- Designing **price tests** by size / recipe / region.
- Ongoing monitoring of **competitive pricing** as menus change over time.
