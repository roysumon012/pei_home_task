"""
Validation utilities.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,count

def validate_required_columns(df: DataFrame, required_columns: list[str]) -> None:
    missing=[c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

def validate_dataframe_not_empty(df: DataFrame, name: str = "") -> None:
    if df.count()==0:
        label = f" '{name}'" if name else ""
        raise ValueError(f"DataFrame{label} is empty")

def check_nulls(df: DataFrame, columns: list[str]) -> None:
    for c in columns:
        if df.filter(col(c).isNull()).count()>0:
            raise ValueError(f"Null values found in column: {c}")

def check_duplicates(df: DataFrame, key_columns: list[str]) -> None:
    dup=df.groupBy(*key_columns).agg(count("*").alias("cnt")).filter(col("cnt")>1).count()
    if dup>0:
        raise ValueError(f"Duplicate records found for keys: {key_columns}")

def validate_business_rules(df: DataFrame) -> None:
    if "sales" in df.columns and df.filter(col("sales")<0).count()>0:
        raise ValueError("Sales cannot be negative")
    if "quantity" in df.columns and df.filter(col("quantity")<=0).count()>0:
        raise ValueError("Quantity must be greater than zero")

def validate_referential_integrity(fact_df:DataFrame,dim_df:DataFrame,fact_key:str,dim_key:str)->None:
    missing=fact_df.join(dim_df,fact_df[fact_key]==dim_df[dim_key],"left_anti").count()
    if missing>0:
        raise ValueError(f"{missing} orphan records found for {fact_key}")
