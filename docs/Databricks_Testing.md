# Databricks Test Environment

The test suite uses **Databricks Connect** to run PySpark tests against a live Databricks cluster. The `spark` fixture in `tests/conftest.py` automatically uses `DatabricksSession.builder` when credentials are detected, falling back to a standard `SparkSession` otherwise.

## Prerequisites

1. Databricks CLI installed and authenticated (`databricks configure`).
2. A running Databricks cluster (All-Purpose or Job cluster) with Databricks Runtime 13.x or higher.
3. `databricks-connect` installed in the local virtual environment (included via `requirements.txt`).

## Running Tests Locally (PowerShell)

```powershell
# 1. Set credentials (do NOT commit these values)
$env:DATABRICKS_CONFIG_PROFILE = "your-profile"   # matches ~/.databrickscfg [profile]
$env:DATABRICKS_CLUSTER_ID    = "your-cluster-id"

# 2. Activate the virtual environment
.venv\Scripts\Activate.ps1

# 3. Run all tests
pytest tests/ -v

# 4. Run a specific test file
pytest tests/test_transformations.py -v

# 5. Run only negative tests
pytest tests/ -v -k "raises or failure"
```

## Test Structure

All test files follow a consistent **positive / negative** pattern:

| Category | Purpose | Example |
|----------|---------|--------|
| **Positive** | Verify correct behaviour with valid inputs | `test_enrich_customer_removes_duplicates` |
| **Negative** | Verify expected errors or edge-case handling | `test_enrich_customer_empty_dataframe_raises` |

Negative tests assert that a `ValueError` is raised (with a specific message) when the pipeline receives invalid data such as an empty DataFrame or missing required columns.

## Test Coverage Summary

| Test File | Tests | Scope |
|-----------|-------|-------|
| `test_readers.py` | 9 | Schema, row count, non-empty assertions for all three readers |
| `test_transformations.py` | 35 | Deduplication, joins, aggregation — positive & negative for each function |
| `test_validations.py` | 16 | Required columns, nulls, duplicates, business rules, referential integrity |
| `test_aggregations.py` | 4 | Profit aggregation grouping, rounding, empty input guard |
| `test_writers.py` | 4 | Schema creation, Delta/Parquet write guard-rails |
| **Total** | **59** | |

