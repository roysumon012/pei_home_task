# PEI Home Task

## Overview

This repository contains my solution for the PEI Lead Data Engineer home assessment. The pipeline implements a Medallion Architecture (Bronze → Silver → Gold) on Databricks using Delta Lake.

## Solution Overview

| Step | Notebook | Layer | Description |
|------|----------|-------|-------------|
| 1 | `00_config.py` | — | Global configuration, catalog/schema and table name definitions |
| 2 | `01_ddl_setup.py` | — | Creates schema, Delta tables, and semantic views with explicit DDL (including clustering), using config constants from `00_config.py` |
| 3 | `02_raw_ingestion.py` | Bronze | Reads source files, validates, adds audit columns, appends raw Delta tables |
| 4 | `03_customer_enrichment.py` | Silver | Reads latest Bronze batch, deduplicates customers, validates, upserts silver table |
| 5 | `04_product_enrichment.py` | Silver | Reads latest Bronze batch, deduplicates products, validates, upserts silver table |
| 6 | `05_order_enrichment.py` | Silver | Reads latest Bronze batch, joins with Silver dims, quarantines orphans, upserts silver table |
| 7 | `06_aggregation.py` | Gold | Recomputes profit aggregation from Silver and overwrites Gold table |
| 8 | `07_sql_queries.sql` | Reporting | Ad-hoc SQL reporting queries against the silver and gold tables |

## Medallion Architecture

```text
Bronze (raw)          Silver (curated)               Gold (aggregated)
──────────────        ───────────────────────        ─────────────────────
bronze_customer  →    silver_customer                gold_profit_summary
bronze_product   →    silver_product
bronze_order     →    silver_order
                       quarantine_orphan_orders
```

## Data Write Strategy

- Bronze: `append` mode to preserve raw batch history.
- Silver: incremental `MERGE` (`upsert_delta`) keyed by business keys for idempotent reruns.
- Silver input scope: each Silver notebook processes only the latest Bronze batch using `load_timestamp`.
- Gold: full recompute and `overwrite` because aggregates can change based on the latest curated base.

## Project Structure

```text
pei_home_task/
├── README.md
├── requirements.txt
├── data/
│   ├── Customer.xlsx
│   ├── Products.csv
│   └── Orders.json
├── notebooks/
│   ├── 00_config.py
│   ├── 01_ddl_setup.py
│   ├── 02_raw_ingestion.py
│   ├── 03_customer_enrichment.py
│   ├── 04_product_enrichment.py
│   ├── 05_order_enrichment.py
│   ├── 06_aggregation.py
│   └── 07_sql_queries.sql
├── src/
│   ├── readers.py
│   ├── schemas.py
│   ├── transformations.py
│   ├── validations.py
│   ├── writers.py
│   └── utils.py
├── tests/
│   ├── conftest.py
│   ├── test_readers.py
│   ├── test_transformations.py
│   ├── test_validations.py
│   ├── test_aggregations.py
│   └── test_writers.py
└── docs/
    ├── Architecture.md
    ├── Assumptions.md
    ├── Databricks_Testing.md
    └── Production_Considerations.md
```

## Execution Order

```
00_config.py
  └── 01_ddl_setup.py
        └── 02_raw_ingestion.py
              └── 03_customer_enrichment.py
              └── 04_product_enrichment.py
                    └── 05_order_enrichment.py
                          └── 06_aggregation.py
                                └── 07_sql_queries.sql
```

## Unit Tests

Tests are written with `pytest` and cover positive (happy-path) and negative (error/edge-case) scenarios for every module.

| Test File | Scope |
|-----------|-------|
| `test_readers.py` | Schema, row count, and non-empty assertions for all readers |
| `test_transformations.py` | Deduplication, enrichment joins, aggregation (positive & negative) |
| `test_validations.py` | Required columns, null checks, duplicate checks, business rules, referential integrity |
| `test_aggregations.py` | Profit aggregation grouping and rounding |
| `test_writers.py` | Schema creation, Delta write, and Parquet write guard-rails |

To run tests (requires Databricks Connect configured):

```powershell
# Set credentials first
$env:DATABRICKS_CONFIG_PROFILE = "your-profile"
$env:DATABRICKS_CLUSTER_ID = "your-cluster-id"

pytest tests/ -v
```

See [docs/Databricks_Testing.md](docs/Databricks_Testing.md) for full setup instructions.

## Technologies

- Python 3.12
- PySpark / Databricks Connect
- Databricks (Unity Catalog)
- Delta Lake
- SQL
- Pytest
