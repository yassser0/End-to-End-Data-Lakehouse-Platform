"""
End-to-end pipeline runner.
Executes all stages in sequence: Bronze -> Silver -> Gold -> PostgreSQL.

Usage:
    python scripts/run_pipeline.py [--batch-id BATCH_ID] [--skip-publish]

The script is also used by the Airflow DAG via BashOperator / PythonOperator.
"""
import argparse
import sys
import uuid
from pathlib import Path

# Make sure src/ is importable when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ingestion.extract_csv import read_customers, read_orders, read_products
from src.quality.metrics import PipelineMetrics
from src.transformations.bronze import ingest_customers, ingest_orders, ingest_products
from src.transformations.gold import build_all_gold
from src.transformations.silver import (
    get_valid_ids,
    transform_silver_customers,
    transform_silver_orders,
    transform_silver_products,
)
from src.utils.logging_config import get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger(__name__)


def validate_input_files() -> None:
    """Fail fast if source files are missing before starting Spark."""
    from src.utils.config import DATA_PATH
    required = ["orders.csv", "customers.csv", "products.csv"]
    missing = [f for f in required if not (Path(DATA_PATH) / f).exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing input files: {missing}. "
            "Run scripts/generate_data.py first."
        )
    logger.info("Input file validation passed files=%s", required)


def run_pipeline(batch_id: str | None = None, skip_publish: bool = False) -> bool:
    """
    Run the full pipeline end-to-end.

    Returns True on success, False on failure.
    """
    bid = batch_id or str(uuid.uuid4())[:8]
    metrics = PipelineMetrics(bid)
    spark = None
    success = False

    try:
        # ── Step 1: Validate input files ──────────────────────────────────────
        logger.info("Step 1: Validating input files batch_id=%s", bid)
        validate_input_files()

        # ── Step 2: Start Spark ───────────────────────────────────────────────
        logger.info("Step 2: Initialising Spark")
        spark = get_spark_session()

        # ── Step 3: Bronze ingestion ──────────────────────────────────────────
        logger.info("Step 3: Bronze ingestion batch_id=%s", bid)

        raw_customers = read_customers(spark, batch_id=bid)
        bronze_customers = ingest_customers(spark, raw_customers)
        metrics.record_bronze("customers", bronze_customers)

        raw_products = read_products(spark, batch_id=bid)
        bronze_products = ingest_products(spark, raw_products)
        metrics.record_bronze("products", bronze_products)

        raw_orders = read_orders(spark, batch_id=bid)
        bronze_orders = ingest_orders(spark, raw_orders)
        metrics.record_bronze("orders", bronze_orders)

        # ── Step 4: Silver transformation ────────────────────────────────────
        logger.info("Step 4: Silver transformation")

        cust_valid, cust_rejected = transform_silver_customers(spark)
        metrics.record_silver("customers", cust_valid, cust_rejected)

        prod_valid, prod_rejected = transform_silver_products(spark)
        metrics.record_silver("products", prod_valid, prod_rejected)

        # Load valid IDs for referential integrity check in orders
        valid_customer_ids = get_valid_ids(spark, "customers", "customer_id")
        valid_product_ids = get_valid_ids(spark, "products", "product_id")

        order_valid, order_rejected = transform_silver_orders(
            spark, valid_customer_ids, valid_product_ids
        )
        metrics.record_silver("orders", order_valid, order_rejected)

        # ── Step 5: Data quality check ────────────────────────────────────────
        logger.info("Step 5: Data quality checks")
        for entity in ("customers", "products", "orders"):
            pct = metrics._data["quality_pct"].get(entity, 0)
            if pct < 50.0:
                raise RuntimeError(
                    f"Quality check FAILED for {entity}: only {pct:.1f}% valid. "
                    "Pipeline aborted. Check quarantine tables for details."
                )
            logger.info("Quality check PASSED entity=%s quality=%.1f%%", entity, pct)

        # ── Step 6: Gold tables ────────────────────────────────────────────────
        logger.info("Step 6: Building Gold tables")
        gold_counts = build_all_gold(
            spark,
            bid,
            metrics.bronze_counts,
            metrics.silver_counts,
            metrics.rejected_counts,
        )
        for table, count in gold_counts.items():
            metrics.record_gold(table, count)

        # ── Step 7: Publish to PostgreSQL ─────────────────────────────────────
        if not skip_publish:
            logger.info("Step 7: Publishing to PostgreSQL")
            try:
                from src.publishing.postgres_loader import publish_all_gold
                publish_counts = publish_all_gold(spark)
                logger.info("PostgreSQL publish complete counts=%s", publish_counts)
            except Exception as pub_exc:
                logger.warning(
                    "PostgreSQL publish failed (pipeline still succeeds): %s", pub_exc
                )
        else:
            logger.info("Step 7: PostgreSQL publish skipped (--skip-publish)")

        success = True

    except Exception as exc:
        metrics.record_error(str(exc))
        logger.error("Pipeline failed: %s", exc)
        success = False

    finally:
        metrics.finish(success=success)
        if spark:
            spark.stop()

    return success


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full data lakehouse pipeline")
    parser.add_argument("--batch-id", help="Override batch ID (auto-generated if not set)")
    parser.add_argument(
        "--skip-publish",
        action="store_true",
        help="Skip PostgreSQL publishing step",
    )
    args = parser.parse_args()

    ok = run_pipeline(batch_id=args.batch_id, skip_publish=args.skip_publish)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
