"""
Unit tests for writers.py

Positive tests: verify functions execute without error.
Negative tests: verify functions raise on invalid inputs.
"""

import pytest

from src.writers import write_delta, write_parquet


# ==================================================================
# Negative Tests
# ==================================================================


def test_write_delta_none_dataframe_raises():
    """Negative: None DataFrame raises AttributeError."""
    with pytest.raises(AttributeError):
        write_delta(None, "main.analytics.table")


def test_write_parquet_none_dataframe_raises():
    """Negative: None DataFrame raises AttributeError."""
    with pytest.raises(AttributeError):
        write_parquet(None, "/output/path")
