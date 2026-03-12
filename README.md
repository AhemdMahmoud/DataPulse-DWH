# 🏗️ SQL Data Warehouse Project

> A fully layered data warehouse built with SQL Server, implementing the **Medallion Architecture** (Bronze → Silver → Gold) to transform raw CRM and ERP data into clean, analytics-ready views.

📖 **Full  Project Phases and Tasks:** [Notion Project Page](https://www.notion.so/dailyahmed/SQL-Data-Warehouse-Project-2f85e937bc9e80c2be89cae261665e55?source=copy_link)

---

## 📌 Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Layer Breakdown](#layer-breakdown)
  - [Bronze Layer](#bronze-layer-raw-data)
  - [Silver Layer](#silver-layer-cleaned--standardized)
  - [Gold Layer](#gold-layer-business-ready)
- [Data Model (Star Schema)](#data-model-star-schema)
- [Data Lineage](#data-lineage)
- [Integration Model](#integration-model)
- [Data Catalog](#data-catalog)
- [Quality Checks](#quality-checks)
- [Naming Conventions](#naming-conventions)
- [Repository Structure](#repository-structure)
- [How to Run](#how-to-run)

---

## Project Overview

This project implements a **3-layer Medallion Data Warehouse** on Microsoft SQL Server. Raw data from two source systems — a **CRM** and an **ERP** — is ingested via CSV files, progressively cleaned and standardized, and ultimately exposed as business-ready views in a **Star Schema** for reporting and analytics (Power BI, Dashboards, ML).

**Key goals:**
- Centralize data from multiple source systems
- Apply data quality and standardization rules
- Serve clean, integrated data to downstream consumers

---

## Architecture

```
Sources (CSV)
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│                    DataWarehouse (SQL Server)            │
│                                                         │
│  ┌──────────────┐   ┌──────────────┐   ┌─────────────┐ │
│  │ Bronze Layer │──▶│ Silver Layer │──▶│  Gold Layer │ │
│  │  (Raw Data)  │   │  (Cleaned)   │   │  (Views)    │ │
│  └──────────────┘   └──────────────┘   └─────────────┘ │
└─────────────────────────────────────────────────────────┘
    │
    ▼
Consume: Power BI | Dashboards | ML
```

| Layer  | Object Type | Load Strategy           | Transformation                             |
|--------|-------------|-------------------------|--------------------------------------------|
| Bronze | Table       | Batch / Truncate+Insert | None (as-is)                               |
| Silver | Table       | Batch / Truncate+Insert | Standardization, Cleansing, Derived Columns |
| Gold   | View        | No load (virtual)       | Integration, Aggregation, Business Logic   |

[![Data Architecture](https://github.com/AhemdMahmoud/DataPulse-DWH/blob/main/docs/data_architecture.png)
---

## Data Sources

| Source | Format     | Tables Loaded                                       |
|--------|------------|-----------------------------------------------------|
| CRM    | CSV Files  | `cust_info`, `prd_info`, `sales_details`            |
| ERP    | CSV Files  | `loc_a101`, `cust_az12`, `px_cat_g1v2`              |

CSV files are loaded from the server path:  
`/var/opt/mssql/backup/P_datasets/source_crm/` and `/source_erp/`

---

## Layer Breakdown

### Bronze Layer — Raw Data

The bronze layer ingests source data **as-is** with no transformation. Tables mirror the exact structure of the source files.

**Tables:**

| Table                      | Source  | Description                          |
|---------------------------|---------|--------------------------------------|
| `bronze.crm_cust_info`    | CRM     | Customer information                 |
| `bronze.crm_prd_info`     | CRM     | Product information                  |
| `bronze.crm_sales_details`| CRM     | Sales transactions                   |
| `bronze.erp_loc_a101`     | ERP     | Customer location data               |
| `bronze.erp_cust_az12`    | ERP     | Customer demographics (age, gender)  |
| `bronze.erp_px_cat_g1v2`  | ERP     | Product category hierarchy           |

**Load Procedure:** `EXEC bronze.load_bronze`  
Uses `BULK INSERT` to load from CSV. Truncates tables before each load to prevent duplicates.

---

### Silver Layer — Cleaned & Standardized

The silver layer receives data from bronze and applies transformation, standardization, and cleansing rules. A `dwh_create_date` metadata column is added to every table.

**Key transformations applied:**

| Table                       | Transformations                                                                 |
|-----------------------------|---------------------------------------------------------------------------------|
| `silver.crm_cust_info`      | Deduplication (latest record per `cst_id`), trim whitespace, normalize marital status (`S`→`Single`, `M`→`Married`) and gender (`M`→`Male`, `F`→`Female`) |
| `silver.crm_prd_info`       | Extract `cat_id` from `prd_key`, convert product line codes to labels, derive `prd_end_dt` via `LEAD()` window function |
| `silver.crm_sales_details`  | Convert integer date columns (`YYYYMMDD`) to proper `DATE` type, fix invalid sales/price/quantity with recalculation logic |
| `silver.erp_cust_az12`      | Strip `NAS` prefix from `cid`, null out future birth dates, normalize gender values |
| `silver.erp_loc_a101`       | Remove dashes from `cid`, normalize country codes (`DE`→`Germany`, `US`/`USA`→`United States`) |
| `silver.erp_px_cat_g1v2`    | Loaded as-is from bronze                                                        |

**Load Procedure:** `EXEC silver.load_silver`

---

### Gold Layer — Business-Ready

The gold layer consists of **SQL Views** built on top of silver tables. No data is physically loaded — views are computed at query time. They implement the Star Schema for analytics.

**Views:**

| View                    | Type      | Description                                              |
|-------------------------|-----------|----------------------------------------------------------|
| `gold.dim_customers`    | Dimension | Customer details enriched from CRM + ERP (location, demographics) |
| `gold.dim_products`     | Dimension | Product attributes joined with category hierarchy        |
| `gold.fact_sales`       | Fact      | Sales transactions with surrogate keys to dimensions     |

**Refresh Procedure:** `EXEC gold.sp_refresh_views` (drops and recreates all three views dynamically)

---

## Data Model (Star Schema)

```
                    ┌──────────────────────┐
                    │   gold.dim_products  │
                    │  ────────────────    │
                    │  PK: product_key     │
                    │  product_id          │
                    │  product_number      │
                    │  product_name        │
                    │  category_id         │
                    │  category            │
                    │  subcategory         │
                    │  maintenance         │
                    │  cost                │
                    │  product_line        │
                    │  start_date          │
                    └──────────┬───────────┘
                               │ FK1
                    ┌──────────▼───────────┐        ┌──────────────────────┐
                    │   gold.fact_sales    │        │  gold.dim_customers  │
                    │  ────────────────    │        │  ────────────────    │
                    │  PK: order_number    │  FK2   │  PK: customer_key    │
                    │  product_key  ───────┼────────▶ customer_id          │
                    │  customer_key ───────┼────────▶ customer_number      │
                    │  order_date          │        │  first_name           │
                    │  shipping_date       │        │  last_name            │
                    │  due_date            │        │  country              │
                    │  sales_amount        │        │  marital_status       │
                    │  quantity            │        │  gender               │
                    │  price               │        │  birthdate            │
                    └──────────────────────┘        │  create_date          │
                                                    └──────────────────────┘

Business Rule:  Sales = Quantity × Price
```

---
[![data Marts_scheama design](https://github.com/AhemdMahmoud/DataPulse-DWH/blob/main/docs/data_marts_model1.gif)
## Data Lineage

```
Sources          Bronze Layer         Silver Layer         Gold Layer
────────         ────────────         ────────────         ──────────
CRM ──┬──────▶  crm_sales_details ──▶ crm_sales_details ──┐
      │                                                    │
      ├──────▶  crm_cust_info ──────▶ crm_cust_info ──────┼──▶ gold.dim_customers
      │                                                    │
      └──────▶  crm_prd_info ───────▶ crm_prd_info ───────┼──▶ gold.dim_products
                                                           │
ERP ──┬──────▶  erp_cust_az12 ──────▶ erp_cust_az12 ──────┤──▶ gold.dim_customers
      │                                                    │
      ├──────▶  erp_loc_a101 ───────▶ erp_loc_a101 ───────┤──▶ gold.dim_customers
      │                                                    │
      └──────▶  erp_px_cat_g1v2 ────▶ erp_px_cat_g1v2 ───┘──▶ gold.dim_products
                                                                gold.fact_sales
```

---
[![Data Lineage](https://github.com/AhemdMahmoud/DataPulse-DWH/blob/main/docs/data_linage_flow.png)
## Integration Model

The Gold layer integrates data from both CRM and ERP sources using the following join keys:

| Join                            | Left Table          | Right Table          | Key                        |
|---------------------------------|---------------------|----------------------|----------------------------|
| Customer ↔ Demographics         | `crm_cust_info`     | `erp_cust_az12`      | `cst_key = cid`            |
| Customer ↔ Location             | `crm_cust_info`     | `erp_loc_a101`       | `cst_key = cid`            |
| Product ↔ Category              | `crm_prd_info`      | `erp_px_cat_g1v2`    | `cat_id = id`              |
| Sales ↔ Product (surrogate key) | `crm_sales_details` | `gold.dim_products`  | `sls_prd_key = product_number` |
| Sales ↔ Customer (surrogate key)| `crm_sales_details` | `gold.dim_customers` | `sls_cust_id = customer_id` |

**Gender resolution logic (dim_customers):**  
CRM gender is used when available and not `N/A`. ERP gender (`erp_cust_az12.gen`) is used as fallback. Defaults to `n/a` if both are missing.

---
[![Integration Model](https://github.com/AhemdMahmoud/DataPulse-DWH/blob/main/docs/data_integration_gold_layer.png)
## Data Catalog

### `gold.dim_customers`
Stores customer details enriched with demographic and geographic data.

| Column          | Type         | Description                                              |
|-----------------|--------------|----------------------------------------------------------|
| customer_key    | INT          | Surrogate key (auto-generated via ROW_NUMBER)            |
| customer_id     | INT          | Source customer ID from CRM                             |
| customer_number | NVARCHAR(50) | Alphanumeric customer identifier                        |
| first_name      | NVARCHAR(50) | Customer first name                                     |
| last_name       | NVARCHAR(50) | Customer last name                                      |
| country         | NVARCHAR(50) | Country of residence (e.g., `Australia`)                |
| marital_status  | NVARCHAR(50) | `Married` or `Single`                                   |
| gender          | NVARCHAR(50) | `Male`, `Female`, or `n/a`                              |
| birthdate       | DATE         | Date of birth (YYYY-MM-DD)                              |
| create_date     | DATE         | Record creation date in source system                   |

### `gold.dim_products`
Provides product information and category hierarchy.

| Column               | Type         | Description                                       |
|----------------------|--------------|---------------------------------------------------|
| product_key          | INT          | Surrogate key                                     |
| product_id           | INT          | Source product ID                                 |
| product_number       | NVARCHAR(50) | Alphanumeric product code                        |
| product_name         | NVARCHAR(50) | Descriptive product name                         |
| category_id          | NVARCHAR(50) | Category identifier                              |
| category             | NVARCHAR(50) | Top-level category (e.g., `Bikes`, `Components`) |
| subcategory          | NVARCHAR(50) | Sub-level classification                         |
| maintenance_required | NVARCHAR(50) | `Yes` or `No`                                    |
| cost                 | INT          | Base product cost                                |
| product_line         | NVARCHAR(50) | Product line (e.g., `Road`, `Mountain`)          |
| start_date           | DATE         | Product availability start date                  |

> **Note:** Only active products (`prd_end_dt IS NULL`) are included in `gold.dim_products`.

### `gold.fact_sales`
Stores transactional sales data.

| Column        | Type         | Description                                      |
|---------------|--------------|--------------------------------------------------|
| order_number  | NVARCHAR(50) | Unique sales order identifier (e.g., `SO54496`) |
| product_key   | INT          | FK → `gold.dim_products`                        |
| customer_key  | INT          | FK → `gold.dim_customers`                       |
| order_date    | DATE         | Date order was placed                           |
| shipping_date | DATE         | Date order was shipped                          |
| due_date      | DATE         | Payment due date                                |
| sales_amount  | INT          | Total monetary value of the line item           |
| quantity      | INT          | Number of units ordered                         |
| price         | INT          | Price per unit                                  |

---

## Quality Checks

The silver layer includes data quality validation checks covering:

- **Uniqueness** — no duplicate primary keys in customer and product tables
- **Referential integrity** — sales records have matching customers and products
- **Date validity** — no invalid date formats, no future birthdates, proper ordering of `order_date < ship_date < due_date`
- **Numeric consistency** — `sales_amount = quantity × price` cross-validation
- **Completeness** — no NULLs in critical fields
- **Gender & status normalization** — standardized categorical values

---

## Naming Conventions

| Scope             | Convention                     | Example                    |
|-------------------|--------------------------------|----------------------------|
| General           | `snake_case`                   | `customer_key`             |
| Bronze/Silver tables | `<sourcesystem>_<entity>`   | `crm_cust_info`            |
| Gold dimension    | `dim_<entity>`                 | `dim_customers`            |
| Gold fact         | `fact_<entity>`                | `fact_sales`               |
| Gold report       | `report_<entity>`              | `report_sales_monthly`     |
| Surrogate keys    | `<table>_key`                  | `customer_key`             |
| Technical columns | `dwh_<column_name>`            | `dwh_create_date`          |
| Load procedures   | `load_<layer>`                 | `load_bronze`, `load_silver` |

---

## Repository Structure

```
📁 sql-data-warehouse/
│
├── 📄 init_database.sql           # Create DB + 3 schemas (bronze, silver, gold)
│
├── 📁 bronze/
│   ├── ddl_bronze.sql             # DDL: Create bronze tables
│   └── proc_load_bronze.sql       # Stored procedure: Load CSV → Bronze
│
├── 📁 silver/
│   ├── ddl_silver.sql             # DDL: Create silver tables
│   ├── proc_load_silver.sql       # Stored procedure: Bronze → Silver (ETL)
│   └── quality_checks_silver.sql  # Data quality validation queries
│
├── 📁 gold/
│   ├── ddl_gold.sql               # Views: dim_customers, dim_products, fact_sales
│   └── dynamic_SQL_automation_gold.sql  # sp_refresh_views stored procedure
│
├── 📄 executer.sql                # Master execution script (runs all layers)
│
└── 📁 docs/
    ├── data_catalog.md            # Full data catalog
    ├── naming_conventions.md      # Naming convention guide
    ├── data_architecture.png      # Architecture overview diagram
    ├── data_linage_flow.png        # Data lineage flow diagram
    ├── data_integration_gold_layer.png  # Integration model diagram
    └── data_marts_model.gif        # Star schema data mart diagram
```

---

## How to Run

### Prerequisites

- Microsoft SQL Server (with SQL Server Agent or manual execution)
- CSV source files placed in:
  - `/var/opt/mssql/backup/P_datasets/source_crm/ 'you can get it form Datasets Folder'` 
  - `/var/opt/mssql/backup/P_datasets/source_erp/ 'you can get it form Datasets Folder'`

### Step-by-Step Execution

```sql
-- 1. Initialize database and schemas
-- ⚠️ WARNING: Drops and recreates the entire DataWarehouse database
source: init_database.sql

-- 2. Create bronze tables
source: bronze/ddl_bronze.sql

-- 3. Create silver tables
source: silver/ddl_silver.sql

-- 4. Create gold views + refresh procedure
source: gold/ddl_gold.sql
source: gold/dynamic_SQL_automation_gold.sql

-- 5. Run full ETL pipeline
USE DataWarehouse;
EXEC bronze.load_bronze;
EXEC silver.load_silver;
EXEC gold.sp_refresh_views;
```

Or run the **master executor** directly:

```sql
-- executer.sql runs all three layers sequentially
source: executer.sql
```

### Verify Results

```sql
SELECT COUNT(*) FROM gold.dim_customers;
SELECT COUNT(*) FROM gold.dim_products;
SELECT COUNT(*) FROM gold.fact_sales;
```

---

## Downstream Consumers

The Gold layer views are ready to connect directly to:

- **Power BI** — Import or DirectQuery mode
- **Dashboards** — Any SQL-compatible BI tool
- **Machine Learning** — Feature extraction from clean, joined data

---

## 📖 Full Notes & Documentation

For Full  Project Phases and Tasks, visit the Notion page:  
🔗 [SQL Data Warehouse Project — Notion](https://www.notion.so/dailyahmed/SQL-Data-Warehouse-Project-2f85e937bc9e80c2be89cae261665e55?source=copy_link)
