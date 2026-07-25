# Databricks notebook source
# MAGIC %md
# MAGIC ## 01_ddl_setup
# MAGIC
# MAGIC Creates schema and all Delta tables using explicit DDL.
# MAGIC Table names, catalog and schema are resolved from `00_config.py`
# MAGIC so no values are hardcoded here.
# MAGIC
# MAGIC **Run this once before any ingestion or enrichment notebook.**

# COMMAND ----------

# Run Config — loads CATALOG, SCHEMA and all TABLE name constants
%run ./00_config

# COMMAND ----------

import logging

logger = logging.getLogger(__name__)

# COMMAND ----------

# DBTITLE 1,Create Schema
logger.info("Creating schema if not exists: %s.%s", CATALOG, SCHEMA)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
logger.info("Schema ready: %s.%s", CATALOG, SCHEMA)

# COMMAND ----------

# DBTITLE 1,Bronze Tables (append-only raw history)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_CUSTOMER_TABLE} (
    customer_id         STRING,
    customer_name       STRING,
    email               STRING,
    phone               STRING,
    address             STRING,
    segment             STRING,
    country             STRING,
    city                STRING,
    state               STRING,
    postal_code         STRING,
    region              STRING,
    load_timestamp      TIMESTAMP,
    load_type           STRING
) USING DELTA
""")
logger.info("Table ready: %s", BRONZE_CUSTOMER_TABLE)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_PRODUCT_TABLE} (
    product_id          STRING,
    category            STRING,
    sub_category        STRING,
    product_name        STRING,
    state               STRING,
    price_per_product   DOUBLE,
    load_timestamp      TIMESTAMP,
    load_type           STRING
) USING DELTA
""")
logger.info("Table ready: %s", BRONZE_PRODUCT_TABLE)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_ORDER_TABLE} (
    row_id              INT,
    order_id            STRING,
    order_date          DATE,
    ship_date           DATE,
    ship_mode           STRING,
    customer_id         STRING,
    product_id          STRING,
    quantity            INT,
    price               DOUBLE,
    discount            DOUBLE,
    profit              DOUBLE,
    load_timestamp      TIMESTAMP,
    load_type           STRING
) USING DELTA
""")
logger.info("Table ready: %s", BRONZE_ORDER_TABLE)

# COMMAND ----------

# DBTITLE 1,Silver Tables (idempotent merge targets)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_CUSTOMER_TABLE} (
    customer_id         STRING,
    customer_name       STRING,
    email               STRING,
    phone               STRING,
    address             STRING,
    segment             STRING,
    country             STRING,
    city                STRING,
    state               STRING,
    postal_code         STRING,
    region              STRING,
    load_timestamp      TIMESTAMP,
    load_type           STRING
) USING DELTA
CLUSTER BY (customer_id, country)
""")
logger.info("Table ready: %s", SILVER_CUSTOMER_TABLE)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_PRODUCT_TABLE} (
    product_id          STRING,
    category            STRING,
    sub_category        STRING,
    product_name        STRING,
    state               STRING,
    price_per_product   DOUBLE,
    load_timestamp      TIMESTAMP,
    load_type           STRING
) USING DELTA
CLUSTER BY (product_id, category)
""")
logger.info("Table ready: %s", SILVER_PRODUCT_TABLE)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_ORDER_TABLE} (
    row_id              INT,
    order_id            STRING,
    order_date          DATE,
    ship_date           DATE,
    ship_mode           STRING,
    customer_id         STRING,
    product_id          STRING,
    quantity            INT,
    price               DOUBLE,
    discount            DOUBLE,
    profit              DOUBLE,
    load_timestamp      TIMESTAMP,
    load_type           STRING,
    customer_name       STRING,
    country             STRING,
    category            STRING,
    sub_category        STRING
) USING DELTA
CLUSTER BY (customer_id, product_id, order_date)
""")
logger.info("Table ready: %s", SILVER_ORDER_TABLE)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUARANTINE_ORPHAN_ORDERS_TABLE} (
    row_id              INT,
    order_id            STRING,
    order_date          DATE,
    ship_date           DATE,
    ship_mode           STRING,
    customer_id         STRING,
    product_id          STRING,
    quantity            INT,
    price               DOUBLE,
    discount            DOUBLE,
    profit              DOUBLE,
    load_timestamp      TIMESTAMP,
    load_type           STRING
) USING DELTA
""")
logger.info("Table ready: %s", QUARANTINE_ORPHAN_ORDERS_TABLE)

# COMMAND ----------

# DBTITLE 1,Gold Tables (overwrite recompute)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_PROFIT_SUMMARY_TABLE} (
    year                INT,
    category            STRING,
    sub_category        STRING,
    customer_name       STRING,
    total_profit        DOUBLE,
    load_timestamp      TIMESTAMP,
    load_type           STRING
) USING DELTA
CLUSTER BY (year, category, customer_name)
""")
logger.info("Table ready: %s", GOLD_PROFIT_SUMMARY_TABLE)

# COMMAND ----------

# DBTITLE 1,Semantic Views (Silver + Gold)
spark.sql(f"""
CREATE OR REPLACE VIEW {SILVER_CUSTOMER_VIEW} AS
SELECT
    customer_id,
    customer_name,
    email,
    phone,
    address,
    segment,
    country,
    city,
    state,
    postal_code,
    region
FROM {SILVER_CUSTOMER_TABLE}
""")
logger.info("View ready: %s", SILVER_CUSTOMER_VIEW)

spark.sql(f"""
CREATE OR REPLACE VIEW {SILVER_PRODUCT_VIEW} AS
SELECT
    product_id,
    category,
    sub_category,
    product_name,
    state,
    price_per_product
FROM {SILVER_PRODUCT_TABLE}
""")
logger.info("View ready: %s", SILVER_PRODUCT_VIEW)

spark.sql(f"""
CREATE OR REPLACE VIEW {SILVER_ORDER_VIEW} AS
SELECT
    row_id,
    order_id,
    order_date,
    ship_date,
    ship_mode,
    customer_id,
    product_id,
    quantity,
    price,
    discount,
    profit,
    customer_name,
    country,
    category,
    sub_category
FROM {SILVER_ORDER_TABLE}
""")
logger.info("View ready: %s", SILVER_ORDER_VIEW)

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD_PROFIT_SUMMARY_VIEW} AS
SELECT
    year,
    category,
    sub_category,
    customer_name,
    total_profit
FROM {GOLD_PROFIT_SUMMARY_TABLE}
""")
logger.info("View ready: %s", GOLD_PROFIT_SUMMARY_VIEW)

# COMMAND ----------

logger.info("DDL setup complete. All tables are ready.")
