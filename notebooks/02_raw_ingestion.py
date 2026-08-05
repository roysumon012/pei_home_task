# Databricks notebook source
# MAGIC %md
# MAGIC 02_raw_ingestion
# MAGIC
# MAGIC Reads source files, performs basic validations,
# MAGIC adds audit columns and writes raw Delta tables.

# COMMAND ----------

# Run Config
%run ./00_config




# COMMAND ----------

from src.readers import read_customers, read_products, read_orders
from src.validations import (
    validate_required_columns,
    validate_dataframe_not_empty
)
from src.utils import add_audit_columns, log_job_start, log_job_end, log_record_count
from src.writers import write_delta

# COMMAND ----------

# DBTITLE 1,Cell 4
log_job_start("Raw Ingestion")

# Read source files
customer_df = read_customers(spark, CUSTOMER_FILE)
product_df = read_products(spark, PRODUCT_FILE)
order_df = read_orders(spark, ORDER_FILE)

# Basic validations
validate_dataframe_not_empty(customer_df, "Customer")
validate_dataframe_not_empty(product_df, "Product")
validate_dataframe_not_empty(order_df, "Order")

validate_required_columns(customer_df, ["customer_id"])
validate_required_columns(product_df, ["product_id"])
validate_required_columns(order_df, ["order_id", "customer_id", "product_id"])

# Audit columns
customer_df = add_audit_columns(customer_df, load_type="BATCH")
product_df = add_audit_columns(product_df, load_type="BATCH")
order_df = add_audit_columns(order_df, load_type="BATCH")

# Log counts
log_record_count(customer_df, "Customer")
log_record_count(product_df, "Product")
log_record_count(order_df, "Order")

# Write raw Delta tables
write_delta(customer_df, BRONZE_CUSTOMER_TABLE, mode="append")
write_delta(product_df, BRONZE_PRODUCT_TABLE, mode="append")
write_delta(order_df, BRONZE_ORDER_TABLE, mode="append")

log_job_end("Raw Ingestion")