# Production Considerations

Although this solution is intentionally lightweight for the assessment, the following engineering practices have been applied:

## Architecture
- **Medallion Architecture** (Bronze → Silver → Gold) implemented across dedicated notebooks, each with a single responsibility:
  - `01_ddl_setup.py` — schema, table, and semantic-view DDL (including Liquid Clustering)
  - `02_raw_ingestion.py` — raw ingest to Bronze
  - `03_customer_enrichment.py` — Bronze → Silver customer
  - `04_product_enrichment.py` — Bronze → Silver product
  - `05_order_enrichment.py` — Bronze orders + Silver dims → Silver order
  - `06_aggregation.py` — Silver order → Gold profit summary
- Separation of concerns: each enrichment domain (customer, product, order) has its own notebook rather than a combined step, making individual reruns and debugging easier.
- Layer-specific write strategy is intentional:
  - Bronze uses append-only raw history.
  - Silver uses key-based Delta `MERGE` upserts for idempotent incremental loads.
  - Gold uses overwrite full recompute for deterministic aggregates — acceptable for the assessment dataset but not recommended at scale (see Future Enhancements).

## Code Quality
- Modular code using reusable helper modules (`src/`).
- Configuration centralised in `00_config.py` with named constants for all catalog, schema, and table references.
- Explicit Spark schema enforcement for Products (CSV) and Orders (JSON) at ingest.
- Column name sanitisation applied at read time to normalise headers to snake_case.

## Validation & Reliability
- Input validation applied at every layer:
  - Required column checks at Bronze ingestion.
  - Non-empty DataFrame guards on `enrich_customer`, `enrich_product`, and `enrich_orders` — each raises `ValueError` with a named argument if the input is empty, halting the pipeline early.
  - Null checks on mandatory primary keys at Silver.
  - Duplicate checks on dimension primary keys at Silver.
- Referential integrity checks on `customer_id` and `product_id` before order enrichment.
- Orphan record quarantine: orders failing referential integrity are written to `quarantine_orphan_orders` (Silver layer) and excluded from the curated dataset rather than silently dropped.
- Latest-batch scope for Silver processing (`max(load_timestamp)` from Bronze) prevents reprocessing all historical Bronze rows each run.
- Upsert semantics on Silver tables ensure reruns are idempotent and prevent duplicate records for the same business keys.
- Exception handling with errors re-raised after logging.

## Observability
- Structured logging for major processing steps (`log_job_start`, `log_job_end`).
- Record count logging at each stage (`log_record_count`).
- Audit columns (`load_timestamp`, `load_type`) added to every output table at every layer, with `load_type` set to `BATCH` (Bronze), `INCREMENTAL` (Silver), or `FULL` (Gold) to reflect the write strategy.

## Performance
- Broadcast joins used for small dimension tables (customer, product) when enriching orders.
- Delta Lake used as the storage format for all tables, enabling ACID transactions and time travel.
- Incremental Silver processing reduces compute compared to full Bronze re-scan on each execution.

## Testing
- Test-driven development (TDD) approach with **pytest** covering all modules.
- Tests are organised into **positive** (valid input, correct output) and **negative** (invalid/empty input, expected `ValueError`) categories for every function.
- 59 tests across 5 test files: readers, transformations, validations, aggregations, and writers.
- Tests run against a live Databricks cluster via Databricks Connect (see `docs/Databricks_Testing.md`).

## Known Source Data Quality Issues

The following data quality issues were identified in the source data. No remediation rules were defined in the task requirements, so the issues are documented here rather than silently corrected:

- **Orphan product orders**: 204 order records reference `product_id` values absent from the product dimension. These are quarantined in `quarantine_orphan_orders` on every run.
- **Optional customer fields**: Fields such as `email`, `phone`, and `address` may contain missing or malformed values. Validation is applied only on the mandatory key (`customer_id`); optional field cleansing is deferred until business rules are defined.

## Known Code Gaps

- **`validate_business_rules` is not invoked in any pipeline notebook.** The function exists in `src/validations.py` with correct sanitised column references (`sales`, `quantity`) and is covered by unit tests, but is not called anywhere in the pipeline. It should be wired into `02_raw_ingestion.py` once the applicable business rules are confirmed.

## Future Enhancements
- **Incremental Gold aggregation via Change Data Feed (CDF):** At scale, overwriting the entire Gold table on each run is expensive — it requires scanning and recomputing the full Silver dataset regardless of how many records actually changed. A more efficient pattern is to enable Delta Change Data Feed on the Silver order table (`ALTER TABLE silver_order SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`), then in `06_aggregation.py` read only the changed rows since the last processed version using `spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", last_processed_version)`. From the CDF output, derive the set of affected aggregation keys (e.g. year + category + sub-category + customer), recompute aggregates for those keys only against the full Silver table, and then `MERGE` the results into the Gold table by those keys. This reduces the Gold compute cost from O(full dataset) to O(changed keys) per run, which scales significantly better for large or high-frequency pipelines.
- Define and implement data cleansing rules for the known source quality issues listed above.
- Wire `validate_business_rules` into `02_raw_ingestion.py` once applicable business rules are confirmed.
- Add explicit `batch_id`/ingestion-run identifier in Bronze to make latest-batch filtering and lineage stronger than timestamp-only selection.
- Add CDC delete handling strategy (hard delete vs soft delete tombstone) for Silver merge pipelines where source systems emit delete events.
- Workflow orchestration using Lakeflow Jobs.
- Monitoring and alerting on pipeline failures and data quality thresholds.
- CI/CD pipeline using Declarative Automation Bundles.
- Data quality dashboards.
- Table partitioning and optimisation for larger datasets.
- Parameterise source file paths and target catalog/schema to support environment promotion (dev / staging / prod).
- Schema evolution strategy for Bronze tables as source file formats change.
