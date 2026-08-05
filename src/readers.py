"""
Reusable data readers.
"""
import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import to_date
from src.schemas import (
    product_schema,
    order_schema,
)
from src.utils import sanitize_column_names

logger = logging.getLogger(__name__)

def read_customers(spark: SparkSession, path: str) -> DataFrame:
    """Read customer Excel file using Databricks built-in Excel reader."""
    try:
        logger.info("Reading customer file: %s", path)
        df = (spark.read.format("excel")
              .schema(customer_schema)
              .option("header", "true")
              .load(path))
        df = sanitize_column_names(df)
        logger.info("Customer records loaded: %s", df.count())
        return df
    except Exception:
        logger.exception("Failed to read customer file")
        raise

def read_products(spark: SparkSession, path: str) -> DataFrame:
    try:
        logger.info("Reading products file: %s", path)
        df=(spark.read.schema(product_schema)
            .option("header","true")
            .option("inferSchema","true")
            .csv(path))
        df=sanitize_column_names(df)
        logger.info("Product records loaded: %s", df.count())
        return df
    except Exception:
        logger.exception("Failed to read products file")
        raise

def read_orders(spark: SparkSession, path: str) -> DataFrame:
    try:
        logger.info("Reading orders file: %s", path)
        df=spark.read.schema(order_schema).option("multiLine","true").json(path)
        df=sanitize_column_names(df)
        df = (df.withColumn("order_date", to_date("order_date", "d/M/yyyy"))
          .withColumn("ship_date", to_date("ship_date", "d/M/yyyy"))
          )
        logger.info("Order records loaded: %s", df.count())
        return df
    except Exception:
        logger.exception("Failed to read orders file")
        raise
