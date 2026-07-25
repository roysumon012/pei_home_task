"""
Transformation functions.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast,col,round,sum

from src.validations import validate_dataframe_not_empty

def enrich_customer(customer_df: DataFrame) -> DataFrame:
    validate_dataframe_not_empty(customer_df, "customer_df")
    return customer_df.dropDuplicates(["customer_id"])

def enrich_product(product_df: DataFrame) -> DataFrame:
    validate_dataframe_not_empty(product_df, "product_df")
    return product_df.dropDuplicates(["product_id"])

def enrich_orders(order_df: DataFrame,
                  customer_df: DataFrame,
                  product_df: DataFrame) -> DataFrame:
    validate_dataframe_not_empty(order_df, "order_df")
    # Select only the required columns from each dimension table
    customer_cols = customer_df.select("customer_id", "customer_name", "country")
    product_cols = product_df.select("product_id", "category", "sub_category")
    df = (order_df
          .join(broadcast(customer_cols), "customer_id", "left")
          .join(broadcast(product_cols), "product_id", "left")
          .withColumn("profit", round(col("profit"), 2)))
    return df

def create_profit_aggregation(order_df: DataFrame)->DataFrame:
    validate_dataframe_not_empty(order_df, "order_df")
    return (
        order_df.groupBy(
            "year",
            "category",
            "sub_category",
            "customer_name"
        ).agg(
            round(sum("profit"),2).alias("total_profit")
        )
    )
