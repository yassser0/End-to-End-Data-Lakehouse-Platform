"""
Gold Layer — Analytical aggregations.

Business definitions
--------------------
revenue
    quantity * unit_price for records with order_status = 'paid'
    Pending, cancelled, and refunded orders are NOT counted as revenue.

paid_orders
    Orders with order_status = 'paid'.

refunds
    Orders with order_status = 'refunded'.

cancellations
    Orders with order_status = 'cancelled'.

Average Order Value (AOV)
    SUM(revenue) / COUNT(paid_orders)

Gold tables
-----------
1. daily_sales           — revenue and paid order count by calendar day
2. product_performance   — quantities sold and revenue by product
3. customer_analytics    — order counts and spending by customer
4. order_status_summary  — counts and amounts by order status
5. data_quality_metrics  — ingestion / quality statistics per batch run
"""
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.config import GOLD_PATH, QUARANTINE_PATH, SILVER_PATH
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def _write_gold(spark: SparkSession, df: DataFrame, table_name: str) -> int:
    """Overwrite a Gold Delta table (analytical tables are fully replaced each run)."""
    path = f"{GOLD_PATH}/{table_name}"
    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(path)
    cnt = df.count()
    logger.info("Gold table=%s rows=%d", table_name, cnt)
    return cnt


def _load_silver_orders(spark: SparkSession) -> DataFrame:
    path = f"{SILVER_PATH}/orders"
    df = spark.read.format("delta").load(path)
    # Work only with paid orders for revenue calculations
    return df


def build_daily_sales(spark: SparkSession) -> int:
    """
    daily_sales — revenue and paid order count by calendar day.

    Columns: order_date, total_revenue, paid_order_count, avg_order_value
    """
    df = _load_silver_orders(spark)
    paid = df.filter(F.col("order_status") == "paid")

    result = (
        paid.groupBy("order_date")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.count("order_id").alias("paid_order_count"),
            F.avg("revenue").alias("avg_order_value"),
        )
        .withColumn("total_revenue", F.round(F.col("total_revenue"), 2))
        .withColumn("avg_order_value", F.round(F.col("avg_order_value"), 2))
        .orderBy("order_date")
    )
    return _write_gold(spark, result, "daily_sales")


def build_product_performance(spark: SparkSession) -> int:
    """
    product_performance — quantities sold and revenue by product.

    Columns: product_id, total_quantity, total_revenue, order_count
    Only paid orders contribute to revenue.
    """
    orders = _load_silver_orders(spark)
    products = spark.read.format("delta").load(f"{SILVER_PATH}/products")

    paid = orders.filter(F.col("order_status") == "paid")

    agg = paid.groupBy("product_id").agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("revenue").alias("total_revenue"),
        F.count("order_id").alias("order_count"),
    )

    result = (
        agg.join(
            products.select("product_id", "product_name", "category"),
            on="product_id",
            how="left",
        )
        .withColumn("total_revenue", F.round(F.col("total_revenue"), 2))
        .orderBy(F.col("total_revenue").desc())
    )
    return _write_gold(spark, result, "product_performance")


def build_customer_analytics(spark: SparkSession) -> int:
    """
    customer_analytics — order counts and spending by customer.

    Columns: customer_id, first_name, last_name, total_orders,
             paid_orders, total_spent, avg_order_value
    Only paid orders contribute to total_spent.
    """
    orders = _load_silver_orders(spark)
    customers = spark.read.format("delta").load(f"{SILVER_PATH}/customers")

    paid = orders.filter(F.col("order_status") == "paid")

    total_orders = orders.groupBy("customer_id").agg(
        F.count("order_id").alias("total_orders")
    )
    paid_agg = paid.groupBy("customer_id").agg(
        F.count("order_id").alias("paid_orders"),
        F.sum("revenue").alias("total_spent"),
        F.avg("revenue").alias("avg_order_value"),
    )

    result = (
        total_orders.join(paid_agg, on="customer_id", how="left")
        .join(
            customers.select("customer_id", "first_name", "last_name", "segment"),
            on="customer_id",
            how="left",
        )
        .fillna(0, subset=["paid_orders", "total_spent", "avg_order_value"])
        .withColumn("total_spent", F.round(F.col("total_spent"), 2))
        .withColumn("avg_order_value", F.round(F.col("avg_order_value"), 2))
        .orderBy(F.col("total_spent").desc())
    )
    return _write_gold(spark, result, "customer_analytics")


def build_order_status_summary(spark: SparkSession) -> int:
    """
    order_status_summary — counts and amounts by order status.

    Columns: order_status, order_count, total_amount
    """
    df = _load_silver_orders(spark)

    result = (
        df.groupBy("order_status")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("revenue").alias("total_amount"),
        )
        .withColumn("total_amount", F.round(F.col("total_amount"), 2))
        .orderBy("order_status")
    )
    return _write_gold(spark, result, "order_status_summary")


def build_data_quality_metrics(
    spark: SparkSession,
    batch_id: str,
    bronze_counts: dict[str, int],
    silver_counts: dict[str, int],
    rejected_counts: dict[str, int],
) -> int:
    """
    data_quality_metrics — ingestion quality summary per batch.

    Columns: batch_id, entity, ingested_count, valid_count,
             rejected_count, quality_pct, run_timestamp
    """
    from datetime import datetime, timezone

    rows = []
    for entity in ("orders", "customers", "products"):
        ingested = bronze_counts.get(entity, 0)
        valid = silver_counts.get(entity, 0)
        rejected = rejected_counts.get(entity, 0)
        quality_pct = round((valid / ingested * 100) if ingested > 0 else 0.0, 2)
        rows.append(
            (
                batch_id,
                entity,
                ingested,
                valid,
                rejected,
                quality_pct,
                datetime.now(tz=timezone.utc).isoformat(),
            )
        )

    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("batch_id", StringType(), False),
            StructField("entity", StringType(), False),
            StructField("ingested_count", IntegerType(), False),
            StructField("valid_count", IntegerType(), False),
            StructField("rejected_count", IntegerType(), False),
            StructField("quality_pct", DoubleType(), False),
            StructField("run_timestamp", StringType(), False),
        ]
    )

    df = spark.createDataFrame(rows, schema=schema)
    return _write_gold(spark, df, "data_quality_metrics")


def build_all_gold(
    spark: SparkSession,
    batch_id: str,
    bronze_counts: dict[str, int],
    silver_counts: dict[str, int],
    rejected_counts: dict[str, int],
) -> dict[str, int]:
    """Run all Gold table builds and return row counts."""
    results = {}
    results["daily_sales"] = build_daily_sales(spark)
    results["product_performance"] = build_product_performance(spark)
    results["customer_analytics"] = build_customer_analytics(spark)
    results["order_status_summary"] = build_order_status_summary(spark)
    results["data_quality_metrics"] = build_data_quality_metrics(
        spark, batch_id, bronze_counts, silver_counts, rejected_counts
    )
    logger.info("Gold layer complete tables=%s", list(results.keys()))
    return results
