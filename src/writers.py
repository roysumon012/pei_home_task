"""
Reusable Delta table writer functions.
"""

import logging
from typing import Iterable, Optional
import uuid

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def _normalize_keys(keys: Iterable[str]) -> list[str]:
    key_list = [k.strip() for k in keys if k and k.strip()]
    if not key_list:
        raise ValueError("At least one merge key is required")
    return key_list


def _merge_condition(target_alias: str, source_alias: str, keys: Iterable[str]) -> str:
    key_list = _normalize_keys(keys)
    return " AND ".join(
        f"{target_alias}.{k} <=> {source_alias}.{k}" for k in key_list
    )


def _latest_per_key(df: DataFrame, keys: Iterable[str], sequence_by: Optional[str]) -> DataFrame:
    key_list = _normalize_keys(keys)
    if sequence_by and sequence_by in df.columns:
        w = Window.partitionBy(*key_list).orderBy(F.col(sequence_by).desc())
        return (
            df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn")
        )
    return df.dropDuplicates(key_list)


def write_delta(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite"
) -> None:
    """
    Write DataFrame into an existing Delta table.

    This function assumes table DDL is managed separately (for example,
    clustering keys and table properties are pre-defined). Data writes use
    INSERT INTO/OVERWRITE semantics to avoid replacing table metadata.
    """

    try:
        allowed_modes = {"append", "overwrite"}
        if mode not in allowed_modes:
            raise ValueError(
                f"Unsupported mode '{mode}'. Supported modes: {sorted(allowed_modes)}"
            )

        spark = df.sparkSession
        if not spark.catalog.tableExists(table_name):
            raise ValueError(
                f"Target table does not exist: {table_name}. "
                "Create it first via explicit DDL."
            )

        logger.info(f"Writing Delta table: {table_name}")

        temp_view = f"tmp_write_{uuid.uuid4().hex}"
        df.createOrReplaceTempView(temp_view)
        try:
            if mode == "append":
                spark.sql(f"INSERT INTO {table_name} SELECT * FROM {temp_view}")
            else:
                spark.sql(f"INSERT OVERWRITE {table_name} SELECT * FROM {temp_view}")
        finally:
            try:
                spark.catalog.dropTempView(temp_view)
            except Exception:
                pass

        logger.info(
            f"Successfully wrote {df.count()} records to {table_name}"
        )

    except Exception:
        logger.exception(
            f"Failed to write Delta table: {table_name}"
        )
        raise


def upsert_delta(
    df: DataFrame,
    table_name: str,
    merge_keys: list[str],
    sequence_by: Optional[str] = "load_timestamp",
) -> None:
    """
    Idempotent Delta merge for Silver-style incremental loads.
    """
    try:
        from delta.tables import DeltaTable

        spark = df.sparkSession
        key_list = _normalize_keys(merge_keys)
        staged_df = _latest_per_key(df, key_list, sequence_by)

        if not spark.catalog.tableExists(table_name):
            raise ValueError(
                f"Target table does not exist: {table_name}. "
                "Create it first via explicit SQL DDL."
            )

        merge_cond = _merge_condition("t", "s", key_list)
        (
            DeltaTable.forName(spark, table_name)
            .alias("t")
            .merge(staged_df.alias("s"), merge_cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        logger.info("Upsert completed for table: %s", table_name)

    except ImportError as exc:
        raise ImportError(
            "delta-spark is required for upsert_delta. "
            "Run in Databricks or install delta-spark."
        ) from exc
    except Exception:
        logger.exception("Failed to upsert Delta table: %s", table_name)
        raise


def write_parquet(
    df: DataFrame,
    output_path: str,
    mode: str = "overwrite"
) -> None:
    """
    Optional helper to write parquet files.
    """

    try:

        logger.info(f"Writing parquet files to {output_path}")

        (
            df.write
            .mode(mode)
            .parquet(output_path)
        )

        logger.info("Parquet files written successfully.")

    except Exception:
        logger.exception("Failed to write parquet files.")
        raise
