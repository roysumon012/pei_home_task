# Databricks notebook source
# MAGIC %md
# MAGIC 04_product_enrichment
# MAGIC
# MAGIC Reads bronze product Delta table, performs deduplication
# MAGIC and validation, and writes the silver product table.

# COMMAND ----------

# DBTITLE 1,Config
# Run Config
%run ./00_config

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from src.transformations import enrich_product
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

# DBTITLE 1,Product Enrichment
log_job_start("Product Enrichment")

# Read latest bronze product batch
product_bronze_df = spark.table(BRONZE_PRODUCT_TABLE)
latest_product_ts = product_bronze_df.agg(
    F.max("load_timestamp").alias("latest_ts")
).first()["latest_ts"]
product_df = product_bronze_df.filter(F.col("load_timestamp") == F.lit(latest_product_ts))

# Enrich: deduplicate on product_id
product_df = enrich_product(product_df)

# Validate
check_nulls(product_df, ["product_id"])
check_duplicates(product_df, ["product_id"])

# Add audit columns
product_df = add_audit_columns(product_df, load_type="INCREMENTAL")

# Log record count
log_record_count(product_df, "Silver Product")

# Write silver Delta table
upsert_delta(product_df, SILVER_PRODUCT_TABLE, merge_keys=["product_id"])

log_job_end("Product Enrichment")