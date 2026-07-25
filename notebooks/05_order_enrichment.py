# Databricks notebook source
# MAGIC %md
# MAGIC 05_order_enrichment
# MAGIC
# MAGIC Reads bronze orders along with silver customer and product tables,
# MAGIC enriches the orders, validates referential integrity,
# MAGIC quarantines orphan records, and writes the silver orders Delta table.

# COMMAND ----------

# Run config
%run ./00_config

# COMMAND ----------

from src.transformations import enrich_orders
from pyspark.sql import functions as F
from src.validations import (
    validate_referential_integrity,
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

# DBTITLE 1,Cell 4
log_job_start("Order Enrichment")

# Read latest bronze order batch + current silver dimensions
order_bronze_df = spark.table(BRONZE_ORDER_TABLE)
latest_order_ts = order_bronze_df.agg(
    F.max("load_timestamp").alias("latest_ts")
).first()["latest_ts"]
order_df = order_bronze_df.filter(F.col("load_timestamp") == F.lit(latest_order_ts))
customer_df = spark.table(SILVER_CUSTOMER_TABLE)
product_df = spark.table(SILVER_PRODUCT_TABLE)

# --- Referential integrity: customer_id ---
# Pre-compute orphan orders (customer_id not found in customer dimension)
orphan_customer_order_df = order_df.join(
    customer_df.select("customer_id"), "customer_id", "left_anti"
)

try:
    validate_referential_integrity(
        order_df,
        customer_df,
        "customer_id",
        "customer_id"
    )
except ValueError as e:
    logger.warning(str(e))
    upsert_delta(
        add_audit_columns(orphan_customer_order_df, load_type="INCREMENTAL"),
        QUARANTINE_ORPHAN_ORDERS_TABLE,
        merge_keys=["order_id"]
    )
    order_df = order_df.join(
        customer_df.select("customer_id"), "customer_id", "inner"
    )

# Drop audit columns from dimension tables to avoid duplicates in the enriched fact table
customer_df = customer_df.drop("load_timestamp", "load_type")
product_df = product_df.drop("load_timestamp", "load_type", "state")

# --- Referential integrity: product_id ---
# Pre-compute orphan orders (product_id not found in product dimension)
orphan_order_df = order_df.join(
    product_df.select("product_id"), "product_id", "left_anti"
)

try:
    validate_referential_integrity(
        order_df,
        product_df,
        "product_id",
        "product_id"
    )
except ValueError as e:
    logger.warning(str(e))
    upsert_delta(
        add_audit_columns(orphan_order_df, load_type="INCREMENTAL"),
        QUARANTINE_ORPHAN_ORDERS_TABLE,
        merge_keys=["order_id"]
    )
    order_df = order_df.join(
        product_df.select("product_id"), "product_id", "inner"
    )

# Enrich orders
order_df = enrich_orders(
    order_df,
    customer_df,
    product_df
)

# Basic validation
check_nulls(order_df, ["order_id"])

# Add audit columns
order_df = add_audit_columns(order_df, load_type="INCREMENTAL")

# Log record count
log_record_count(order_df, "Curated Orders")

# Write curated Delta table
upsert_delta(order_df, SILVER_ORDER_TABLE, merge_keys=["order_id"])

log_job_end("Order Enrichment")