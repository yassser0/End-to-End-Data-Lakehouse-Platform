"""
CSV extraction module.
Reads source CSV files into Spark DataFrames with schema inference.
Adds ingestion metadata required for the Bronze layer.
"""
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.utils.config import DATA_PATH
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def _resolve_path(filename: str, base: str = DATA_PATH) -> str:
    """Resolve a filename against the configured data path."""
    p = Path(base) / filename
    if not p.exists():
        raise FileNotFoundError(
            f"Source file not found: {p}. "
            "Run scripts/generate_data.py first."
        )
    return str(p)


def read_csv(
    spark: SparkSession,
    filename: str,
    source_path: str | None = None,
    batch_id: str | None = None,
) -> DataFrame:
    """
    Read a CSV file into a Spark DataFrame.

    Adds the following ingestion metadata columns:
      _ingestion_timestamp  — UTC timestamp when data was loaded
      _source_file          — original filename (no directory path)
      _batch_id             — pipeline run identifier (UUID4 if not supplied)

    Parameters
    ----------
    spark        : active SparkSession
    filename     : filename inside DATA_PATH (e.g. 'orders.csv')
    source_path  : override the full path (for testing)
    batch_id     : pipeline batch identifier; auto-generated if None
    """
    path = source_path or _resolve_path(filename)
    bid = batch_id or str(uuid.uuid4())
    now = datetime.now(tz=timezone.utc).isoformat()

    logger.info("Reading CSV file=%s batch_id=%s", filename, bid)

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("nullValue", "")
        .option("emptyValue", "")
        .csv(path)
    )

    # Add Bronze metadata columns
    df = (
        df.withColumn("_ingestion_timestamp", F.lit(now).cast("timestamp"))
        .withColumn("_source_file", F.lit(Path(filename).name))
        .withColumn("_batch_id", F.lit(bid))
    )

    row_count = df.count()
    logger.info(
        "Loaded file=%s rows=%d batch_id=%s", filename, row_count, bid
    )
    return df


def read_orders(spark: SparkSession, batch_id: str | None = None) -> DataFrame:
    return read_csv(spark, "orders.csv", batch_id=batch_id)


def read_customers(spark: SparkSession, batch_id: str | None = None) -> DataFrame:
    return read_csv(spark, "customers.csv", batch_id=batch_id)


def read_products(spark: SparkSession, batch_id: str | None = None) -> DataFrame:
    return read_csv(spark, "products.csv", batch_id=batch_id)
