"""
Bronze Layer — Raw ingestion into Delta Lake.

Responsibilities:
  - Accept a raw Spark DataFrame (already enriched with metadata)
  - Write / MERGE to a Delta table without modifying business data
  - Prevent re-ingestion of the same batch (idempotency via _batch_id)

Write strategy
--------------
We use MERGE on (primary_key, _batch_id) so that:
  - Re-running with the same batch ID is a no-op (idempotent)
  - New batches append new rows
  - Existing rows from prior batches are never overwritten

Business data is preserved exactly as received from the source.
No validation or cleansing happens here.
"""
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.config import BRONZE_PATH
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def _table_path(entity: str) -> str:
    """Return the Delta table path for a given entity name."""
    return f"{BRONZE_PATH}/{entity}"


def write_bronze(
    spark: SparkSession,
    df: DataFrame,
    entity: str,
    primary_key: str,
) -> int:
    """
    Merge raw records into the Bronze Delta table.

    Parameters
    ----------
    spark       : active SparkSession
    df          : raw DataFrame with _batch_id and _ingestion_timestamp columns
    entity      : table name (e.g. 'orders', 'customers', 'products')
    primary_key : business key column name for MERGE condition

    Returns
    -------
    int : number of rows written to the table
    """
    path = _table_path(entity)
    row_count = df.count()
    logger.info(
        "Writing bronze entity=%s rows=%d path=%s", entity, row_count, path
    )

    if DeltaTable.isDeltaTable(spark, path):
        # MERGE — skip rows already in this batch to ensure idempotency
        delta_table = DeltaTable.forPath(spark, path)
        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                f"target.{primary_key} = source.{primary_key} "
                f"AND target._batch_id = source._batch_id",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("MERGE complete entity=%s", entity)
    else:
        # First write — create the Delta table
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(path)
        )
        logger.info("Initial write complete entity=%s", entity)

    return row_count


def ingest_orders(spark: SparkSession, df: DataFrame) -> int:
    return write_bronze(spark, df, "orders", "order_id")


def ingest_customers(spark: SparkSession, df: DataFrame) -> int:
    return write_bronze(spark, df, "customers", "customer_id")


def ingest_products(spark: SparkSession, df: DataFrame) -> int:
    return write_bronze(spark, df, "products", "product_id")
