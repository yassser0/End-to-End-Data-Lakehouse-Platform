"""
Silver Layer — Validated and cleansed data.

Responsibilities:
  - Normalise column names and data types
  - Validate required fields (null checks)
  - Deduplicate records using documented business rules
  - Validate dates, quantities, prices, statuses
  - Check referential integrity (customer_id, product_id)
  - Separate invalid records into quarantine tables with rejection reasons
  - Preserve traceability to the Bronze source

Business rules (documented)
----------------------------
Deduplication
  For orders: keep the most recently ingested row per order_id
  For customers/products: keep the first row per primary key

Status canonicalisation
  Valid order_status values  : pending, paid, cancelled, refunded
  Valid payment_status values: unpaid, paid, failed, refunded
  Case is normalised to lowercase. Unrecognised values are rejected.

Date validation
  order_date must be parseable as yyyy-MM-dd and in the range 2000-01-01 to today.

Quantity / Price
  quantity must be > 0
  unit_price must be > 0
"""
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from src.utils.config import BRONZE_PATH, QUARANTINE_PATH, SILVER_PATH
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Canonical valid values
_VALID_ORDER_STATUS = {"pending", "paid", "cancelled", "refunded"}
_VALID_PAYMENT_STATUS = {"unpaid", "paid", "failed", "refunded"}


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _normalise_column_names(df: DataFrame) -> DataFrame:
    """Lower-case all column names, replace spaces/dashes with underscores."""
    renamed = {c: c.lower().replace(" ", "_").replace("-", "_") for c in df.columns}
    for old, new in renamed.items():
        if old != new:
            df = df.withColumnRenamed(old, new)
    return df


def _add_rejection(df: DataFrame, reason_expr) -> DataFrame:
    """
    Add or concatenate a rejection reason to the _rejection_reason column.
    Accepts a Column or string expression.
    """
    if "_rejection_reason" not in df.columns:
        df = df.withColumn("_rejection_reason", F.lit(None).cast(T.StringType()))
    if isinstance(reason_expr, str):
        reason_col = F.lit(reason_expr)
    else:
        reason_col = reason_expr

    df = df.withColumn(
        "_rejection_reason",
        F.when(
            F.col("_rejection_reason").isNull(),
            reason_col,
        ).otherwise(
            F.concat_ws(" | ", F.col("_rejection_reason"), reason_col)
        ),
    )
    return df


def _split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split DataFrame into (valid, invalid) based on _rejection_reason."""
    invalid = df.filter(F.col("_rejection_reason").isNotNull())
    valid = df.filter(F.col("_rejection_reason").isNull()).drop("_rejection_reason")
    return valid, invalid


def _write_silver(spark: SparkSession, df: DataFrame, path: str, key: str) -> int:
    if DeltaTable.isDeltaTable(spark, path):
        dt = DeltaTable.forPath(spark, path)
        (
            dt.alias("t")
            .merge(df.alias("s"), f"t.{key} = s.{key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(path)
    cnt = df.count()
    logger.info("Silver write path=%s rows=%d", path, cnt)
    return cnt


def _write_quarantine(df: DataFrame, entity: str) -> int:
    path = f"{QUARANTINE_PATH}/{entity}"
    if df.rdd.isEmpty():
        return 0
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
    cnt = df.count()
    logger.info("Quarantine write entity=%s rows=%d", entity, cnt)
    return cnt


# ─── Orders ──────────────────────────────────────────────────────────────────


def transform_silver_orders(
    spark: SparkSession,
    valid_customer_ids: set[str],
    valid_product_ids: set[str],
) -> tuple[int, int]:
    """
    Read Bronze orders, apply all validations, write Silver and quarantine.

    Returns (valid_count, rejected_count)
    """
    bronze_path = f"{BRONZE_PATH}/orders"
    silver_path = f"{SILVER_PATH}/orders"

    logger.info("Starting Silver transform for orders")

    df = spark.read.format("delta").load(bronze_path)
    df = _normalise_column_names(df)

    # 1. Cast types
    df = (
        df.withColumn("quantity", F.col("quantity").cast(T.IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(T.DoubleType()))
        .withColumn(
            "order_date",
            F.to_date(F.col("order_date"), "yyyy-MM-dd"),
        )
        .withColumn(
            "order_status", F.lower(F.trim(F.col("order_status")))
        )
        .withColumn(
            "payment_status", F.lower(F.trim(F.col("payment_status")))
        )
    )

    # 2. Compute revenue (quantity * unit_price)
    df = df.withColumn("revenue", F.col("quantity") * F.col("unit_price"))

    # 3. Deduplication — keep latest per order_id (by _ingestion_timestamp)
    w = Window.partitionBy("order_id").orderBy(F.col("_ingestion_timestamp").desc())
    df = (
        df.withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    # 4. Null checks on required fields
    for col in ("order_id", "customer_id", "product_id"):
        df = df.withColumn(
            "_rejection_reason",
            F.when(
                F.col(col).isNull() & F.col("_rejection_reason").isNull()
                if "_rejection_reason" in df.columns
                else F.col(col).isNull(),
                F.lit(f"NULL_{col.upper()}"),
            ).otherwise(
                F.col("_rejection_reason") if "_rejection_reason" in df.columns else F.lit(None)
            ),
        )

    df = _add_rejection(
        df,
        F.when(F.col("order_id").isNull(), F.lit("NULL_ORDER_ID")).otherwise(None),
    )
    df = _add_rejection(
        df,
        F.when(F.col("customer_id").isNull(), F.lit("NULL_CUSTOMER_ID")).otherwise(None),
    )
    df = _add_rejection(
        df,
        F.when(F.col("product_id").isNull(), F.lit("NULL_PRODUCT_ID")).otherwise(None),
    )

    # 5. Date validation
    df = _add_rejection(
        df,
        F.when(
            F.col("order_date").isNull()
            | (F.col("order_date") < F.lit("2000-01-01").cast(T.DateType()))
            | (F.col("order_date") > F.current_date()),
            F.lit("INVALID_ORDER_DATE"),
        ).otherwise(None),
    )

    # 6. Quantity / price
    df = _add_rejection(
        df,
        F.when(
            F.col("quantity").isNull() | (F.col("quantity") <= 0),
            F.lit("INVALID_QUANTITY"),
        ).otherwise(None),
    )
    df = _add_rejection(
        df,
        F.when(
            F.col("unit_price").isNull() | (F.col("unit_price") <= 0),
            F.lit("INVALID_UNIT_PRICE"),
        ).otherwise(None),
    )

    # 7. Status validation
    valid_status_set = F.array(*[F.lit(s) for s in _VALID_ORDER_STATUS])
    df = _add_rejection(
        df,
        F.when(
            ~F.col("order_status").isin(*list(_VALID_ORDER_STATUS)),
            F.lit("INVALID_ORDER_STATUS"),
        ).otherwise(None),
    )
    df = _add_rejection(
        df,
        F.when(
            ~F.col("payment_status").isin(*list(_VALID_PAYMENT_STATUS)),
            F.lit("INVALID_PAYMENT_STATUS"),
        ).otherwise(None),
    )

    # 8. Referential integrity
    if valid_customer_ids:
        cust_bc = spark.sparkContext.broadcast(valid_customer_ids)
        cust_check_udf = F.udf(
            lambda cid: None if cid in cust_bc.value else "INVALID_CUSTOMER_REF",
            T.StringType(),
        )
        df = _add_rejection(
            df,
            F.when(
                F.col("customer_id").isNotNull(),
                cust_check_udf(F.col("customer_id")),
            ).otherwise(None),
        )

    if valid_product_ids:
        prod_bc = spark.sparkContext.broadcast(valid_product_ids)
        prod_check_udf = F.udf(
            lambda pid: None if pid in prod_bc.value else "INVALID_PRODUCT_REF",
            T.StringType(),
        )
        df = _add_rejection(
            df,
            F.when(
                F.col("product_id").isNotNull(),
                prod_check_udf(F.col("product_id")),
            ).otherwise(None),
        )

    valid_df, rejected_df = _split_valid_invalid(df)
    valid_count = _write_silver(spark, valid_df, silver_path, "order_id")
    rejected_count = _write_quarantine(rejected_df, "orders")

    logger.info(
        "Silver orders complete valid=%d rejected=%d", valid_count, rejected_count
    )
    return valid_count, rejected_count


# ─── Customers ───────────────────────────────────────────────────────────────


def transform_silver_customers(spark: SparkSession) -> tuple[int, int]:
    bronze_path = f"{BRONZE_PATH}/customers"
    silver_path = f"{SILVER_PATH}/customers"

    logger.info("Starting Silver transform for customers")
    df = spark.read.format("delta").load(bronze_path)
    df = _normalise_column_names(df)

    # Dedup — keep first occurrence per customer_id
    w = Window.partitionBy("customer_id").orderBy(F.col("_ingestion_timestamp"))
    df = (
        df.withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    # Cast signup_date
    df = df.withColumn(
        "signup_date", F.to_date(F.col("signup_date"), "yyyy-MM-dd")
    )

    df = _add_rejection(
        df,
        F.when(F.col("customer_id").isNull(), F.lit("NULL_CUSTOMER_ID")).otherwise(None),
    )
    df = _add_rejection(
        df,
        F.when(
            F.col("first_name").isNull() & F.col("last_name").isNull(),
            F.lit("NULL_NAME"),
        ).otherwise(None),
    )

    valid_df, rejected_df = _split_valid_invalid(df)
    valid_count = _write_silver(spark, valid_df, silver_path, "customer_id")
    rejected_count = _write_quarantine(rejected_df, "customers")

    logger.info(
        "Silver customers complete valid=%d rejected=%d", valid_count, rejected_count
    )
    return valid_count, rejected_count


# ─── Products ────────────────────────────────────────────────────────────────


def transform_silver_products(spark: SparkSession) -> tuple[int, int]:
    bronze_path = f"{BRONZE_PATH}/products"
    silver_path = f"{SILVER_PATH}/products"

    logger.info("Starting Silver transform for products")
    df = spark.read.format("delta").load(bronze_path)
    df = _normalise_column_names(df)

    # Dedup — keep first occurrence per product_id
    w = Window.partitionBy("product_id").orderBy(F.col("_ingestion_timestamp"))
    df = (
        df.withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    df = df.withColumn("unit_price", F.col("unit_price").cast(T.DoubleType()))

    df = _add_rejection(
        df,
        F.when(F.col("product_id").isNull(), F.lit("NULL_PRODUCT_ID")).otherwise(None),
    )
    df = _add_rejection(
        df,
        F.when(
            F.col("unit_price").isNull() | (F.col("unit_price") <= 0),
            F.lit("INVALID_UNIT_PRICE"),
        ).otherwise(None),
    )

    valid_df, rejected_df = _split_valid_invalid(df)
    valid_count = _write_silver(spark, valid_df, silver_path, "product_id")
    rejected_count = _write_quarantine(rejected_df, "products")

    logger.info(
        "Silver products complete valid=%d rejected=%d", valid_count, rejected_count
    )
    return valid_count, rejected_count


def get_valid_ids(spark: SparkSession, entity: str, id_col: str) -> set[str]:
    """Return set of valid IDs from a Silver Delta table."""
    path = f"{SILVER_PATH}/{entity}"
    try:
        df = spark.read.format("delta").load(path)
        ids = {row[id_col] for row in df.select(id_col).collect()}
        logger.info("Loaded valid %s ids count=%d", entity, len(ids))
        return ids
    except Exception as e:
        logger.warning("Could not load Silver %s IDs: %s", entity, e)
        return set()
