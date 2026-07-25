"""
Unit tests for aggregation logic.

Positive tests: verify correct aggregation behavior with valid inputs.
Negative tests: verify expected failures on invalid or empty inputs.
"""

import pytest
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.transformations import create_profit_aggregation


# ==================================================================
# Positive Tests
# ==================================================================


def test_profit_aggregation(order_df, customer_df, product_df):
    """Positive: aggregation produces correct column set and row count."""
    # Enrich order data with required columns for aggregation
    enriched = (
        order_df.join(customer_df, "customer_id", "left")
                .join(product_df, "product_id", "left")
    )

    result = create_profit_aggregation(enriched)

    assert result.count() == 2

    expected_columns = {
        "year",
        "category",
        "sub_category",
        "customer_name",
        "total_profit"
    }

    assert expected_columns.issubset(set(result.columns))


def test_profit_sum(order_df, customer_df, product_df):
    """Positive: total_profit sum matches the raw profit sum."""
    enriched = (
        order_df.join(customer_df, "customer_id", "left")
                .join(product_df, "product_id", "left")
    )

    result = create_profit_aggregation(enriched)

    total_profit = (
        result.agg({"total_profit": "sum"})
              .collect()[0][0]
    )

    expected = sum(r["profit"] for r in order_df.collect())

    assert round(total_profit, 2) == round(expected, 2)


def test_profit_aggregation_combines_same_group(spark):
    """Positive: rows with same group keys are summed and rounded."""
    df = spark.createDataFrame(
        [
            (2024, "Furniture", "Chair", "Alice", 10.126),
            (2024, "Furniture", "Chair", "Alice", 1.111),
            (2024, "Technology", "Laptop", "Bob", 3.0),
        ],
        ["year", "category", "sub_category", "customer_name", "profit"],
    )

    result = create_profit_aggregation(df)
    row = result.filter(
        (result.year == 2024)
        & (result.category == "Furniture")
        & (result.sub_category == "Chair")
        & (result.customer_name == "Alice")
    ).first()

    assert result.count() == 2
    assert row["total_profit"] == 11.24



# ==================================================================
# Negative Tests
# ==================================================================


def test_profit_aggregation_empty_input_raises(spark):
    """Negative: empty input should halt the flow with ValueError."""
    schema = StructType([
        StructField("year", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("profit", DoubleType(), True),
    ])
    df = spark.createDataFrame([], schema)

    with pytest.raises(ValueError, match="order_df"):
        create_profit_aggregation(df)
