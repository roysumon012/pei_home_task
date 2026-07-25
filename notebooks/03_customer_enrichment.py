# Databricks notebook source
# MAGIC %md
# MAGIC 03_customer_enrichment
# MAGIC
# MAGIC Reads bronze customer Delta table, performs deduplication
# MAGIC and validation, and writes the silver customer table.

# COMMAND ----------

# DBTITLE 1,Config
# Run Config
%run ./00_config

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from src.transformations import enrich_customer
from src.validations import (
    check_duplicates,
    check_nulls,
)
from src.utils import (
    add_audit_columns,
    log_job_start,
    log_job_end,
    log_record_count,
)
from src.writers import upsert_delta

# COMMAND ----------

# DBTITLE 1,Customer Enrichment
log_job_start("Customer Enrichment")

# Read latest bronze customer batch
customer_bronze_df = spark.table(BRONZE_CUSTOMER_TABLE)
latest_customer_ts = customer_bronze_df.agg(
    F.max("load_timestamp").alias("latest_ts")
).first()["latest_ts"]
customer_df = customer_bronze_df.filter(F.col("load_timestamp") == F.lit(latest_customer_ts))

# Enrich: deduplicate on customer_id
customer_df = enrich_customer(customer_df)

# Validate
check_nulls(customer_df, ["customer_id"])
check_duplicates(customer_df, ["customer_id"])

# Add audit columns
customer_df = add_audit_columns(customer_df, load_type="INCREMENTAL")

# Log record count
log_record_count(customer_df, "Silver Customer")

# Write silver Delta table
upsert_delta(customer_df, SILVER_CUSTOMER_TABLE, merge_keys=["customer_id"])

log_job_end("Customer Enrichment")