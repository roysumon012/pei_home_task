"""
Shared pytest fixtures.
"""

import sys
import os
from pathlib import Path

# Add parent directory to sys.path so src imports work
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

# Use DatabricksSession when Databricks Connect is configured, otherwise fall back to SparkSession
try:
    from databricks.connect import DatabricksSession
    USE_DATABRICKS = os.getenv("DATABRICKS_CONFIG_PROFILE") or os.getenv("DATABRICKS_CLUSTER_ID")
except ImportError:
    USE_DATABRICKS = False

if USE_DATABRICKS:
    from databricks.connect import DatabricksSession
else:
    from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    if USE_DATABRICKS:
        # Use Databricks Connect when configured
        spark = DatabricksSession.builder.getOrCreate()
    else:
        # Fall back to local Spark session for standalone testing
        spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("PEI-Assessment-Tests")
            .getOrCreate()
        )
    yield spark
    spark.stop()


@pytest.fixture
def customer_df(spark):
    data = [
        ("C001", "Alice", "USA"),
        ("C002", "Bob", "Canada")
    ]
    return spark.createDataFrame(data, ["customer_id", "customer_name", "country"])


@pytest.fixture
def product_df(spark):
    data = [
        ("P001", "Furniture", "Chair"),
        ("P002", "Technology", "Laptop")
    ]
    return spark.createDataFrame(
        data,
        ["product_id", "category", "sub_category"]
    )


@pytest.fixture
def order_df(spark):
    data = [
        ("O001", "C001", "P001", 2024, 100.50, 2),
        ("O002", "C002", "P002", 2024, 250.75, 1),
    ]
    return spark.createDataFrame(
        data,
        ["order_id", "customer_id", "product_id", "year", "profit", "quantity"]
    )
