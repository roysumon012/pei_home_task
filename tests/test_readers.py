"""
Unit tests for readers.py

Positive tests: verify that fixtures produce valid DataFrames with expected schema.
"""


# ==================================================================
# Positive Tests: Fixture-based schema and data validation
# ==================================================================


def test_customer_dataframe_not_empty(customer_df):
    """Positive: customer fixture is not empty."""
    assert customer_df.count() > 0


def test_product_dataframe_not_empty(product_df):
    """Positive: product fixture is not empty."""
    assert product_df.count() > 0


def test_order_dataframe_not_empty(order_df):
    """Positive: order fixture is not empty."""
    assert order_df.count() > 0


def test_customer_columns(customer_df):
    """Positive: customer fixture has required columns."""
    expected = {"customer_id", "customer_name"}
    assert expected.issubset(set(customer_df.columns))


def test_product_columns(product_df):
    """Positive: product fixture has required columns."""
    expected = {"product_id", "category", "sub_category"}
    assert expected.issubset(set(product_df.columns))


def test_order_columns(order_df):
    """Positive: order fixture has required columns."""
    expected = {
        "order_id",
        "customer_id",
        "product_id",
        "year",
        "profit",
        "quantity"
    }
    assert expected.issubset(set(order_df.columns))


def test_customer_row_count(customer_df):
    """Positive: customer fixture has expected row count."""
    assert customer_df.count() == 2


def test_product_row_count(product_df):
    """Positive: product fixture has expected row count."""
    assert product_df.count() == 2


def test_order_row_count(order_df):
    """Positive: order fixture has expected row count."""
    assert order_df.count() == 2
