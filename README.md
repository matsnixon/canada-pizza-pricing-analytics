# Pizza Price Analytics – Canada

End-to-end pricing analytics pipeline for major pizza chains in Canada (Domino’s, Pizza Hut, Pizza Pizza).  
Python scrapers collect live menu prices, a SQL-style data model in DuckDB standardizes them, and a Power BI dashboard compares pricing by size, recipe, and region to surface pricing and tiering opportunities.

---

## Tech Stack

- **Python** – web scraping, data cleaning (`requests`, `pandas`, etc.)
- **DuckDB** – SQL-style data modelling (dimensions + fact table)
- **Power BI** – interactive pricing dashboard and insights
- **Git / GitHub** – version control and portfolio hosting

---

## Data Pipeline

1. **Data Collection (Python scrapers)**  
   - Scrapers for Domino’s, Pizza Hut, and Pizza Pizza pull:
     - Store metadata (city, province)
     - Product information (size, crust, recipe)
     - List prices
   - Output: standardized CSVs with consistent columns across chains.

2. **Data Modelling (DuckDB + SQL-style transforms)**  
   - Raw CSVs are loaded into DuckDB.
   - Built a **star-schema style model** with:
     - `dim_store` – store / city / province
     - `dim_product` – chain, recipe, size, crust
     - `dim_date` – year / month / day keys
     - `fact_price` – one row per chain–store–product–date price point
   - Clean tables are exported for downstream BI use.

3. **Visualization & Analysis (Power BI)**  
   - The model feeds a Power BI report with:
     - **Median Price by Size & Chain** (Small, Medium, Large, X-Large)
     - **Median Price Gap by Recipe – Pizza Hut vs competitors (Medium pizzas)**
     - **Pizza Hut Price Premiums** table: $ and % gaps vs Domino’s and Pizza Pizza
   - Slicers allow filtering by **size, crust, recipe, province, city, year, and month** for ad-hoc analysis. :contentReference[oaicite:0]{index=0}

---

## Key Insights from the Dashboard

From the current dataset and time period:

- **Pizza Hut prices are above Domino’s and Pizza Pizza across all four core sizes.**
- **Medium and Large pizzas carry the strongest premiums** for Pizza Hut:
  - Medium: roughly **+15.8%** vs Domino’s and **+19.6%** vs Pizza Pizza.
  - Large: roughly **+13.0%** vs Domino’s and **+15.3%** vs Pizza Pizza.
- **Price gaps are largest on entry recipes** (e.g., core pepperoni) and **narrow at the top end**, suggesting:
  - Entry products may be **over-premiumized vs competitors**.
  - There may be **insufficient price tiering** between entry and premium recipes.

These results point to potential opportunities to:
- Re-examine **entry price points** where gaps are widest.
- Tighten **tiering between entry and premium recipes** so that step-ups feel more justified.
- Use the dashboard as a starting point for **targeted price tests** by size / recipe / province.
