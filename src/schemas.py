"""
Schema defintion for all the datasets.
"""

from pyspark.sql.types import *
 
customer_schema = StructType([
    StructField("Customer ID", StringType(), False),
    StructField("Customer Name", StringType(), False),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("Segment", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Postal Code", StringType(), True),
    StructField("Region", StringType(), True)
])
 
product_schema = StructType([
    StructField("Product ID", StringType(), False),
    StructField("Category", StringType(), True),
    StructField("Sub-Category", StringType(), True),
    StructField("Product Name", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Price per product", DoubleType(), True)
])
 
order_schema = StructType([
    StructField("Row ID", IntegerType(), False),
    StructField("Order ID", StringType(), False),
    StructField("Order Date", StringType(), True),
    StructField("Ship Date", StringType(), True),
    StructField("Ship Mode", StringType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Product ID", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Profit", DoubleType(), True)
])