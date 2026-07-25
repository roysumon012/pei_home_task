"""
Common utility functions.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit
import re

logger = logging.getLogger(__name__)


def log_record_count(df: DataFrame, dataset_name: str) -> None:
    """Log the number of records in a DataFrame."""
    logger.info("%s record count: %s", dataset_name, df.count())


def add_audit_columns(df: DataFrame, load_type: str = "FULL") -> DataFrame:
    """
    Add standard audit columns.

    Parameters
    ----------
    load_type:
        BATCH     — Bronze append (raw batch history)
        INCREMENTAL — Silver upsert (latest-batch merge)
        FULL      — Gold overwrite (full recompute)
    """
    allowed = {"BATCH", "INCREMENTAL", "FULL"}
    if load_type not in allowed:
        raise ValueError(f"Invalid load_type '{load_type}'. Allowed: {sorted(allowed)}")
    return (
        df.withColumn("load_timestamp", current_timestamp())
          .withColumn("load_type", lit(load_type))
    )


def display_schema(df: DataFrame) -> None:
    """Print the DataFrame schema."""
    logger.info("Schema for DataFrame:")
    df.printSchema()


def validate_dataframe_not_empty(df: DataFrame, dataset_name: str) -> None:
    """Raise an exception if the DataFrame is empty."""
    if df.rdd.isEmpty():
        raise ValueError(f"{dataset_name} is empty.")
    logger.info("%s contains data.", dataset_name)

 
def sanitize_column_names(df):
    columns = [
        re.sub(r'[^a-zA-Z0-9]', '_', c.strip())
          .lower()
          .replace("__", "_")
        for c in df.columns
    ]
    return df.toDF(*columns)


def log_job_start(job_name: str) -> None:
    logger.info("=" * 80)
    logger.info("Starting Job: %s", job_name)
    logger.info("=" * 80)


def log_job_end(job_name: str) -> None:
    logger.info("=" * 80)
    logger.info("Completed Job: %s", job_name)
    logger.info("=" * 80)
