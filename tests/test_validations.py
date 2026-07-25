"""
Unit tests for validations.py

Positive tests: verify valid inputs pass without raising.
Negative tests: verify invalid inputs raise ValueError with correct messages.
"""

import pytest
from pyspark.sql import Row

from src.validations import (
    validate_required_columns,
    validate_dataframe_not_empty,
    check_nulls,
    check_duplicates,
    validate_business_rules,
    validate_referential_integrity,
)


# ==================================================================
# Positive Tests
# ==================================================================


def test_validate_required_columns_success(customer_df):
    """Positive: all required columns present passes without error."""
    validate_required_columns(customer_df, ["customer_id", "customer_name"])


def test_check_nulls_success(customer_df):
    """Positive: no nulls in checked columns passes without error."""
    check_nulls(customer_df, ["customer_id"])


def test_check_duplicates_success(customer_df):
    """Positive: no duplicates in key columns passes without error."""
    check_duplicates(customer_df, ["customer_id"])


def test_validate_business_rules_success(order_df):
    """Positive: valid sales and quantity passes without error."""
    validate_business_rules(order_df)


def test_validate_referential_integrity_success(order_df, customer_df):
    """Positive: all fact keys exist in dimension passes without error."""
    validate_referential_integrity(
        order_df,
        customer_df,
        "customer_id",
        "customer_id"
    )


# ==================================================================
# Negative Tests
# ==================================================================


def test_validate_required_columns_failure(customer_df):
    """Negative: missing column raises ValueError."""
    with pytest.raises(ValueError):
        validate_required_columns(customer_df, ["Missing Column"])


def test_validate_required_columns_multiple_missing(customer_df):
    """Negative: multiple missing columns are reported."""
    with pytest.raises(ValueError) as exc_info:
        validate_required_columns(customer_df, ["email", "segment"])
    assert "email" in str(exc_info.value)
    assert "segment" in str(exc_info.value)


def test_validate_required_columns_all_missing(spark):
    """Negative: all requested columns are missing."""
    df = spark.createDataFrame(
        [("x", "y")],
        ["col_a", "col_b"],
    )
    with pytest.raises(ValueError, match="Missing required columns"):
        validate_required_columns(df, ["id", "name", "email"])


def test_check_nulls_failure(spark):
    """Negative: null in required column raises ValueError."""
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True)
    ])
    df = spark.createDataFrame(
        [Row(**{"customer_id": None, "customer_name": "Alice"})],
        schema=schema
    )
    with pytest.raises(ValueError):
        check_nulls(df, ["customer_id"])


def test_check_nulls_all_rows_null(spark):
    """Negative: every row has null in the checked column."""
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([StructField("customer_id", StringType(), True)])
    df = spark.createDataFrame([(None,), (None,), (None,)], schema)
    with pytest.raises(ValueError):
        check_nulls(df, ["customer_id"])


def test_check_duplicates_failure(spark):
    """Negative: duplicate key raises ValueError."""
    df = spark.createDataFrame(
        [("C001", "Alice"), ("C001", "Alice")],
        ["customer_id", "customer_name"]
    )
    with pytest.raises(ValueError):
        check_duplicates(df, ["customer_id"])


def test_check_duplicates_all_rows_same(spark):
    """Negative: every row is a duplicate."""
    df = spark.createDataFrame(
        [("C001", "Alice")] * 10,
        ["customer_id", "customer_name"],
    )
    with pytest.raises(ValueError, match="Duplicate records"):
        check_duplicates(df, ["customer_id"])


def test_validate_dataframe_not_empty_failure(spark):
    """Negative: empty DataFrame raises ValueError."""
    from pyspark.sql.types import StructField, StringType, StructType
    schema = StructType([StructField("customer_id", StringType(), True)])
    df = spark.createDataFrame([], schema)
    with pytest.raises(ValueError, match="empty"):
        validate_dataframe_not_empty(df, "customers")


def test_validate_business_rules_failure(spark):
    """Negative: negative sales AND zero quantity."""
    df = spark.createDataFrame(
        [("O001", -10.0, 0)],
        ["order_id", "sales", "quantity"]
    )
    with pytest.raises(ValueError):
        validate_business_rules(df)


def test_validate_business_rules_sales_only_failure(spark):
    """Negative: negative sales alone raises."""
    df = spark.createDataFrame(
        [("O001", -10.0, 2)],
        ["order_id", "sales", "quantity"],
    )
    with pytest.raises(ValueError, match="Sales cannot be negative"):
        validate_business_rules(df)


def test_validate_business_rules_quantity_only_failure(spark):
    """Negative: zero quantity alone raises."""
    df = spark.createDataFrame(
        [("O001", 10.0, 0)],
        ["order_id", "sales", "quantity"],
    )
    with pytest.raises(ValueError, match="Quantity must be greater than zero"):
        validate_business_rules(df)


def test_validate_referential_integrity_failure(spark, customer_df):
    """Negative: orphan order raises ValueError."""
    orders = spark.createDataFrame(
        [("O003", "C999")],
        ["order_id", "customer_id"]
    )
    with pytest.raises(ValueError):
        validate_referential_integrity(
            orders, customer_df, "customer_id", "customer_id"
        )


def test_validate_referential_integrity_reports_orphan_count(spark, customer_df):
    """Negative: orphan count is reported in the error message."""
    orders = spark.createDataFrame(
        [("O003", "C999"), ("O004", "C998")],
        ["order_id", "customer_id"],
    )
    with pytest.raises(ValueError, match="2 orphan records"):
        validate_referential_integrity(
            orders, customer_df, "customer_id", "customer_id",
        )


def test_validate_referential_integrity_all_orphans(spark):
    """Negative: every fact record is an orphan."""
    fact = spark.createDataFrame(
        [("O001", "C999"), ("O002", "C888"), ("O003", "C777")],
        ["order_id", "customer_id"],
    )
    dim = spark.createDataFrame(
        [("C001",), ("C002",)],
        ["customer_id"],
    )
    with pytest.raises(ValueError, match="3 orphan records"):
        validate_referential_integrity(fact, dim, "customer_id", "customer_id")


def test_validate_referential_integrity_empty_dimension(spark):
    """Negative: empty dimension means all facts are orphans."""
    from pyspark.sql.types import StructType, StructField, StringType
    fact = spark.createDataFrame(
        [("O001", "C001")],
        ["order_id", "customer_id"],
    )
    dim_schema = StructType([StructField("customer_id", StringType(), True)])
    dim = spark.createDataFrame([], dim_schema)
    with pytest.raises(ValueError, match="1 orphan records"):
        validate_referential_integrity(fact, dim, "customer_id", "customer_id")
