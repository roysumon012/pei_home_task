# Assumptions

## Data
- Customer, Product, and Orders source files are provided and accessible.
- Required columns exist in each source file.
- Customer ID and Product ID uniquely identify records after deduplication.
- Source data is assumed to be valid UTF-8 where applicable.
- Order dates are formatted as `d/M/yyyy` in the source JSON file; this pattern is applied during parsing.
- Column names containing spaces or special characters (e.g., `Sub-Category`, `Customer ID`) are sanitised to snake_case at read time via `sanitize_column_names`.
- Explicit Spark schemas are applied when reading Products (CSV) and Orders (JSON) to enforce data types at ingestion. Customer data is read using the Databricks built-in Excel reader (`spark.read.format("excel")`) and the schema is inferred by Spark.

## Source Data Quality
- The source data was found to contain quality issues. As no business rules were defined in the task requirements to govern how these should be cleaned, record-level cleansing was not performed. The known issues are:
  - **204 order records** reference product IDs absent from the product dimension; these are quarantined in the `quarantine_orphan_orders` Silver table and excluded from the curated dataset.
  - Optional customer fields (e.g., `email`, `phone`, `address`) may contain missing or malformed values; only the mandatory key (`customer_id`) is validated for nulls.

## Medallion Architecture
- The pipeline follows the Bronze → Silver → Gold medallion pattern.
- **Bronze** tables store raw ingested data with audit columns, no transformations applied.
- **Silver** tables store cleansed and enriched data: duplicates removed, referential integrity enforced, orphan records quarantined.
- **Gold** tables store aggregated reporting datasets derived from Silver.
- Each layer is a separate notebook step; notebooks are run in order (`00` → `06`).

## Processing
- Bronze ingestion is append-only to preserve raw batch history.
- Silver processing is incremental and idempotent: each Silver notebook reads only the latest Bronze batch (`max(load_timestamp)`) and merges into target Silver tables by business key.
- Gold aggregation is recomputed from curated Silver data and overwritten each run.
- Enrichment notebooks are separated by domain: customer enrichment (`02`), product enrichment (`03`), and order enrichment (`04`) each run independently.
- Duplicate dimension records are removed using `dropDuplicates()` on the primary key column. An empty DataFrame after reading is treated as a pipeline error — `enrich_customer`, `enrich_product`, and `enrich_orders` all raise `ValueError` on empty input.
- Records with missing mandatory keys fail validation and halt the pipeline.
- Referential integrity is validated before order enrichment; orphan orders are written to a quarantine table (`quarantine_orphan_orders`) and excluded from the curated dataset rather than silently dropped.
- Profit values are rounded to two decimal places before aggregation.

## Storage
- Data is stored as Delta tables under catalog `teo_dev`, schema `sales_analytics`.
- Bronze tables are written in `append` mode.
- Silver tables are written using key-based Delta `MERGE` (`upsert`) for idempotent incremental behavior.
- Gold tables are written in `overwrite` mode.
- Audit columns (`load_timestamp`, `load_type`) are added to all output datasets at every layer, with `load_type` reflecting the write strategy per layer: `BATCH` for Bronze, `INCREMENTAL` for Silver, `FULL` for Gold.
- Table naming follows the medallion convention: `bronze_*`, `silver_*`, `gold_*`, `quarantine_*`.

## Reporting
- Profit aggregation (`gold_profit_summary`) is grouped by:
  - Year (derived from `order_date`)
  - Category
  - Sub-Category
  - Customer Name
- Additional ad-hoc SQL queries in `07_sql_queries.sql` report profit by year, by category, and by customer.
