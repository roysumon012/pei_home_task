"""
Unit tests for transformations.py

Positive tests: verify correct behavior with valid inputs.
Negative tests: verify expected failures or edge-case handling.
"""

import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)

from src.transformations import (
    enrich_customer,
    enrich_product,
    enrich_orders,
    create_profit_aggregation,
)


# ==================================================================
# enrich_customer: Positive Tests
# ==================================================================


def test_enrich_customer_removes_duplicates(spark):
    """Positive: duplicate customer_ids are deduplicated."""
    df = spark.createDataFrame(
        [("C001", "Alice"), ("C001", "Alice"), ("C002", "Bob")],
        ["customer_id", "customer_name"],
    )
    result = enrich_customer(df)
    assert result.count() == 2


def test_enrich_customer_no_duplicates_unchanged(spark):
    """Positive: unique records pass through unchanged."""
    df = spark.createDataFrame(
        [("C001", "Alice"), ("C002", "Bob")],
        ["customer_id", "customer_name"],
    )
    result = enrich_customer(df)
    assert result.count() == 2


# ==================================================================
# enrich_customer: Negative Tests
# ==================================================================


def test_enrich_customer_empty_dataframe_raises(spark):
    """Negative: empty input should halt the flow with ValueError."""
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
    ])
    df = spark.createDataFrame([], schema)
    with pytest.raises(ValueError, match="customer_df"):
        enrich_customer(df)


def test_enrich_customer_all_null_keys(spark):
    """Negative: all-null keys collapse into a single row."""
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
    ])
    df = spark.createDataFrame([(None, "Alice"), (None, "Bob")], schema)
    result = enrich_customer(df)
    assert result.count() == 1


def test_enrich_customer_all_duplicates(spark):
    """Negative: all identical records reduce to one."""
    df = spark.createDataFrame(
        [("C001", "Alice")] * 5,
        ["customer_id", "customer_name"],
    )
    result = enrich_customer(df)
    assert result.count() == 1


# ==================================================================
# enrich_product: Positive Tests
# ==================================================================


def test_enrich_product_removes_duplicates(spark):
    """Positive: duplicate product_ids are deduplicated."""
    df = spark.createDataFrame(
        [("P001", "Furniture", "Chair"), ("P001", "Furniture", "Chair"), ("P002", "Technology", "Laptop")],
        ["product_id", "category", "sub_category"],
    )
    result = enrich_product(df)
    assert result.count() == 2


def test_enrich_product_no_duplicates_unchanged(spark):
    """Positive: unique records pass through unchanged."""
    df = spark.createDataFrame(
        [("P001", "Furniture", "Chair"), ("P002", "Technology", "Laptop")],
        ["product_id", "category", "sub_category"],
    )
    result = enrich_product(df)
    assert result.count() == 2


# ==================================================================
# enrich_product: Negative Tests
# ==================================================================


def test_enrich_product_empty_dataframe_raises(spark):
    """Negative: empty input should halt the flow with ValueError."""
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
    ])
    df = spark.createDataFrame([], schema)
    with pytest.raises(ValueError, match="product_df"):
        enrich_product(df)


def test_enrich_product_all_null_keys(spark):
    """Negative: all-null keys collapse into a single row."""
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
    ])
    df = spark.createDataFrame([(None, "Furniture"), (None, "Technology")], schema)
    result = enrich_product(df)
    assert result.count() == 1


def test_enrich_product_all_duplicates(spark):
    """Negative: all identical records reduce to one."""
    df = spark.createDataFrame(
        [("P001", "Furniture", "Chair")] * 4,
        ["product_id", "category", "sub_category"],
    )
    result = enrich_product(df)
    assert result.count() == 1


# ==================================================================
# enrich_orders: Positive Tests
# ==================================================================


def test_enrich_orders_joins_correctly(spark):
    """Positive: orders are enriched with customer and product dimensions."""
    orders = spark.createDataFrame(
        [("O001", "C001", "P001", 2024, 100.125, 2)],
        ["order_id", "customer_id", "product_id", "year", "profit", "quantity"],
    )
    customers = spark.createDataFrame(
        [("C001", "Alice", "USA")],
        ["customer_id", "customer_name", "country"],
    )
    products = spark.createDataFrame(
        [("P001", "Furniture", "Chair")],
        ["product_id", "category", "sub_category"],
    )
    result = enrich_orders(orders, customers, products)

    assert result.count() == 1
    row = result.first()
    assert row["customer_name"] == "Alice"
    assert row["category"] == "Furniture"
    assert row["profit"] == 100.13  # rounded to 2 decimals


def test_enrich_orders_preserves_all_columns(spark):
    """Positive: enriched result contains expected dimension columns."""
    orders = spark.createDataFrame(
        [("O001", "C001", "P001", 2024, 10.0, 1)],
        ["order_id", "customer_id", "product_id", "year", "profit", "quantity"],
    )
    customers = spark.createDataFrame(
        [("C001", "Alice", "USA")],
        ["customer_id", "customer_name", "country"],
    )
    products = spark.createDataFrame(
        [("P001", "Furniture", "Chair")],
        ["product_id", "category", "sub_category"],
    )
    result = enrich_orders(orders, customers, products)
    expected_cols = {"customer_name", "country", "category", "sub_category"}
    assert expected_cols.issubset(set(result.columns))


# ==================================================================
# enrich_orders: Negative Tests
# ==================================================================


def test_enrich_orders_empty_orders_raises(spark):
    """Negative: empty orders should halt the flow with ValueError."""
    order_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("profit", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
    ])
    orders = spark.createDataFrame([], order_schema)
    customers = spark.createDataFrame(
        [("C001", "Alice", "USA")],
        ["customer_id", "customer_name", "country"],
    )
    products = spark.createDataFrame(
        [("P001", "Furniture", "Chair")],
        ["product_id", "category", "sub_category"],
    )
    with pytest.raises(ValueError, match="order_df"):
        enrich_orders(orders, customers, products)


def test_enrich_orders_unmatched_keys_produce_nulls(spark):
    """Negative: orders with no matching dimension get null values (left join)."""
    orders = spark.createDataFrame(
        [("O001", "C999", "P999", 2024, 10.0, 1)],
        ["order_id", "customer_id", "product_id", "year", "profit", "quantity"],
    )
    customers = spark.createDataFrame(
        [("C001", "Alice", "USA")],
        ["customer_id", "customer_name", "country"],
    )
    products = spark.createDataFrame(
        [("P001", "Furniture", "Chair")],
        ["product_id", "category", "sub_category"],
    )
    result = enrich_orders(orders, customers, products)
    row = result.first()
    assert result.count() == 1
    assert row["customer_name"] is None
    assert row["category"] is None


def test_enrich_orders_null_join_keys(spark):
    """Negative: null join keys in orders produce null dimension values."""
    order_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("profit", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
    ])
    orders = spark.createDataFrame(
        [("O001", None, None, 2024, 5.0, 1)],
        schema=order_schema,
    )
    customers = spark.createDataFrame(
        [("C001", "Alice", "USA")],
        ["customer_id", "customer_name", "country"],
    )
    products = spark.createDataFrame(
        [("P001", "Furniture", "Chair")],
        ["product_id", "category", "sub_category"],
    )
    result = enrich_orders(orders, customers, products)
    row = result.first()
    assert row["customer_name"] is None
    assert row["category"] is None


# ==================================================================
# create_profit_aggregation: Positive Tests
# ==================================================================


def test_profit_aggregation_groups_correctly(spark):
    """Positive: rows with same group keys are summed."""
    df = spark.createDataFrame(
        [
            (2024, "Furniture", "Chair", "Alice", 10.0),
            (2024, "Furniture", "Chair", "Alice", 20.0),
            (2024, "Technology", "Laptop", "Bob", 30.0),
        ],
        ["year", "category", "sub_category", "customer_name", "profit"],
    )
    result = create_profit_aggregation(df)
    assert result.count() == 2
    alice_row = result.filter(result.customer_name == "Alice").first()
    assert alice_row["total_profit"] == 30.0


def test_profit_aggregation_rounds_to_2_decimals(spark):
    """Positive: total_profit is rounded to 2 decimal places."""
    df = spark.createDataFrame(
        [
            (2024, "Furniture", "Chair", "Alice", 10.126),
            (2024, "Furniture", "Chair", "Alice", 1.111),
        ],
        ["year", "category", "sub_category", "customer_name", "profit"],
    )
    result = create_profit_aggregation(df)
    row = result.first()
    assert row["total_profit"] == 11.24


def test_profit_aggregation_has_expected_columns(spark):
    """Positive: output has the correct aggregation columns."""
    df = spark.createDataFrame(
        [(2024, "Furniture", "Chair", "Alice", 10.0)],
        ["year", "category", "sub_category", "customer_name", "profit"],
    )
    result = create_profit_aggregation(df)
    expected = {"year", "category", "sub_category", "customer_name", "total_profit"}
    assert expected == set(result.columns)


# ==================================================================
# create_profit_aggregation: Negative Tests
# ==================================================================


def test_profit_aggregation_empty_input_raises(spark):
    """Negative: empty DataFrame should halt the flow with ValueError."""
    schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("profit", DoubleType(), True),
    ])
    df = spark.createDataFrame([], schema)
    with pytest.raises(ValueError, match="order_df"):
        create_profit_aggregation(df)


def test_profit_aggregation_all_null_profits(spark):
    """Negative: all-null profits result in null total."""
    schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("profit", DoubleType(), True),
    ])
    df = spark.createDataFrame(
        [(2024, "Furniture", "Chair", "Alice", None)],
        schema=schema,
    )
    result = create_profit_aggregation(df)
    assert result.first()["total_profit"] is None


def test_profit_aggregation_negative_profits(spark):
    """Negative: negative profits are summed correctly (loss scenario)."""
    df = spark.createDataFrame(
        [
            (2024, "Furniture", "Chair", "Alice", -50.0),
            (2024, "Furniture", "Chair", "Alice", 30.0),
        ],
        ["year", "category", "sub_category", "customer_name", "profit"],
    )
    result = create_profit_aggregation(df)
    assert result.first()["total_profit"] == -20.0


def test_profit_aggregation_null_group_keys(spark):
    """Negative: null group keys are treated as a single group."""
    schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("profit", DoubleType(), True),
    ])
    df = spark.createDataFrame(
        [(None, None, None, None, 10.0), (None, None, None, None, 20.0)],
        schema=schema,
    )
    result = create_profit_aggregation(df)
    assert result.count() == 1
    assert result.first()["total_profit"] == 30.0
