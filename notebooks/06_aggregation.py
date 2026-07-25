# Databricks notebook source
# MAGIC %md
# MAGIC 06_aggregation
# MAGIC
# MAGIC Creates the gold profit summary aggregation from the silver
# MAGIC orders table and stores it as a Delta table.

# COMMAND ----------

# Run Config
%run ./00_config

# COMMAND ----------

from src.transformations import create_profit_aggregation
from src.validations import validate_dataframe_not_empty
from src.utils import (
    add_audit_columns,
    log_job_start,
    log_job_end,
    log_record_count,
)
from src.writers import write_delta
from pyspark.sql.functions import year as spark_year

# COMMAND ----------

# DBTITLE 1,Cell 5
log_job_start("Profit Aggregation")

# Read curated orders
orders_df = spark.table(SILVER_ORDER_TABLE)

# Ensure data exists
validate_dataframe_not_empty(orders_df, "Orders")

# Derive year from order_date for aggregation
orders_df = orders_df.withColumn("year", spark_year("order_date"))

# Create aggregation
profit_summary_df = create_profit_aggregation(orders_df)

# Add audit columns
profit_summary_df = add_audit_columns(profit_summary_df, load_type="FULL")

# Log record count
log_record_count(profit_summary_df, "Profit Summary")

# Write Delta table
write_delta(
    profit_summary_df,
    GOLD_PROFIT_SUMMARY_TABLE,
    mode="overwrite"
)

log_job_end("Profit Aggregation")