"""
Apache Airflow DAG: ecommerce_data_pipeline

Pipeline tasks (in order):
  1. validate_input_files     — Check source CSVs exist
  2. ingest_bronze            — Load raw CSVs into Delta Bronze
  3. transform_silver         — Validate and cleanse into Silver
  4. run_quality_checks       — Assert minimum quality thresholds
  5. build_gold               — Aggregate Silver into Gold tables
  6. publish_to_postgres      — Load Gold into PostgreSQL
  7. pipeline_summary         — Log execution summary

Configuration
-------------
All secrets are read from Airflow Variables / environment variables.
No credentials are hardcoded in this file.

Retry policy
------------
- Bronze / Silver: 2 retries, 5-minute backoff (idempotent operations)
- Quality checks: 1 retry (may be transient Spark issue)
- Gold / Publish: 2 retries, 10-minute backoff

Timeouts
--------
- Individual tasks: 30 minutes
- Overall DAG: 2 hours
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Default args ──────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,  # Set to True and configure SMTP for prod alerts
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="ecommerce_data_pipeline",
    description="End-to-end e-commerce data lakehouse pipeline (Bronze->Silver->Gold->Postgres)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "ecommerce", "batch"],
    dagrun_timeout=timedelta(hours=2),
) as dag:

    # ── Task callables ────────────────────────────────────────────────────────

    def _validate_input_files(**context):
        """Task 1: Validate that source CSV files exist."""
        import sys
        from pathlib import Path

        sys.path.insert(0, "/opt/lakehouse/src")
        from src.utils.config import DATA_PATH
        from src.utils.logging_config import get_logger

        logger = get_logger("airflow.validate")
        required = ["orders.csv", "customers.csv", "products.csv"]
        missing = [f for f in required if not (Path(DATA_PATH) / f).exists()]
        if missing:
            raise FileNotFoundError(
                f"Missing input files: {missing}. Run generate_data.py first."
            )
        logger.info("Input validation passed files=%s", required)
        context["ti"].xcom_push(key="data_path", value=DATA_PATH)

    def _ingest_bronze(**context):
        """Task 2: Ingest raw CSV data into Bronze Delta tables."""
        import sys
        sys.path.insert(0, "/opt/lakehouse/src")

        from src.ingestion.extract_csv import read_customers, read_orders, read_products
        from src.transformations.bronze import ingest_customers, ingest_orders, ingest_products
        from src.utils.logging_config import get_logger
        from src.utils.spark_session import get_spark_session

        logger = get_logger("airflow.bronze")
        batch_id = context["run_id"].replace(":", "_")[:20]

        spark = get_spark_session("airflow_bronze")
        try:
            counts = {}
            counts["customers"] = ingest_customers(spark, read_customers(spark, batch_id=batch_id))
            counts["products"] = ingest_products(spark, read_products(spark, batch_id=batch_id))
            counts["orders"] = ingest_orders(spark, read_orders(spark, batch_id=batch_id))

            logger.info("Bronze ingestion complete counts=%s", counts)
            context["ti"].xcom_push(key="bronze_counts", value=counts)
            context["ti"].xcom_push(key="batch_id", value=batch_id)
        finally:
            spark.stop()

    def _transform_silver(**context):
        """Task 3: Validate and cleanse data into Silver Delta tables."""
        import sys
        sys.path.insert(0, "/opt/lakehouse/src")

        from src.transformations.silver import (
            get_valid_ids,
            transform_silver_customers,
            transform_silver_orders,
            transform_silver_products,
        )
        from src.utils.logging_config import get_logger
        from src.utils.spark_session import get_spark_session

        logger = get_logger("airflow.silver")
        spark = get_spark_session("airflow_silver")
        try:
            silver_counts = {}
            rejected_counts = {}

            v, r = transform_silver_customers(spark)
            silver_counts["customers"], rejected_counts["customers"] = v, r

            v, r = transform_silver_products(spark)
            silver_counts["products"], rejected_counts["products"] = v, r

            valid_cust_ids = get_valid_ids(spark, "customers", "customer_id")
            valid_prod_ids = get_valid_ids(spark, "products", "product_id")

            v, r = transform_silver_orders(spark, valid_cust_ids, valid_prod_ids)
            silver_counts["orders"], rejected_counts["orders"] = v, r

            logger.info("Silver transform complete valid=%s rejected=%s",
                        silver_counts, rejected_counts)
            context["ti"].xcom_push(key="silver_counts", value=silver_counts)
            context["ti"].xcom_push(key="rejected_counts", value=rejected_counts)
        finally:
            spark.stop()

    def _run_quality_checks(**context):
        """Task 4: Assert minimum data quality thresholds."""
        from src.utils.logging_config import get_logger

        logger = get_logger("airflow.quality")
        bronze_counts = context["ti"].xcom_pull(key="bronze_counts", task_ids="ingest_bronze")
        silver_counts = context["ti"].xcom_pull(key="silver_counts", task_ids="transform_silver")

        MIN_QUALITY_PCT = float(os.environ.get("MIN_QUALITY_PCT", "50.0"))
        failures = []

        for entity in ("customers", "products", "orders"):
            ingested = bronze_counts.get(entity, 0)
            valid = silver_counts.get(entity, 0)
            pct = (valid / ingested * 100) if ingested > 0 else 0.0
            logger.info("Quality entity=%s pct=%.1f%%", entity, pct)
            if pct < MIN_QUALITY_PCT:
                failures.append(
                    f"{entity}: {pct:.1f}% valid (minimum: {MIN_QUALITY_PCT}%)"
                )

        if failures:
            raise ValueError(
                f"Data quality check failed: {failures}. "
                "Check quarantine tables for rejection details."
            )
        logger.info("All quality checks passed")

    def _build_gold(**context):
        """Task 5: Build Gold analytical tables."""
        import sys
        sys.path.insert(0, "/opt/lakehouse/src")

        from src.transformations.gold import build_all_gold
        from src.utils.logging_config import get_logger
        from src.utils.spark_session import get_spark_session

        logger = get_logger("airflow.gold")
        batch_id = context["ti"].xcom_pull(key="batch_id", task_ids="ingest_bronze")
        bronze_counts = context["ti"].xcom_pull(key="bronze_counts", task_ids="ingest_bronze")
        silver_counts = context["ti"].xcom_pull(key="silver_counts", task_ids="transform_silver")
        rejected_counts = context["ti"].xcom_pull(key="rejected_counts", task_ids="transform_silver")

        spark = get_spark_session("airflow_gold")
        try:
            gold_counts = build_all_gold(
                spark, batch_id, bronze_counts, silver_counts, rejected_counts
            )
            logger.info("Gold build complete counts=%s", gold_counts)
            context["ti"].xcom_push(key="gold_counts", value=gold_counts)
        finally:
            spark.stop()

    def _publish_to_postgres(**context):
        """Task 6: Load Gold results into PostgreSQL."""
        import sys
        sys.path.insert(0, "/opt/lakehouse/src")

        from src.publishing.postgres_loader import publish_all_gold
        from src.utils.logging_config import get_logger
        from src.utils.spark_session import get_spark_session

        logger = get_logger("airflow.publish")
        spark = get_spark_session("airflow_publish")
        try:
            counts = publish_all_gold(spark)
            logger.info("PostgreSQL publish complete counts=%s", counts)
        finally:
            spark.stop()

    def _pipeline_summary(**context):
        """Task 7: Log the pipeline execution summary."""
        from src.quality.metrics import PipelineMetrics
        from src.utils.logging_config import get_logger

        logger = get_logger("airflow.summary")
        metrics = PipelineMetrics.load_latest()
        if metrics:
            logger.info(
                "Pipeline summary batch=%s status=%s duration=%.1fs",
                metrics.get("batch_id"),
                metrics.get("status"),
                metrics.get("duration_seconds", 0),
            )
        logger.info(
            "DAG run_id=%s execution_date=%s",
            context["run_id"],
            context["execution_date"],
        )

    # ── Task definitions ──────────────────────────────────────────────────────

    t1_validate = PythonOperator(
        task_id="validate_input_files",
        python_callable=_validate_input_files,
        retries=0,  # Input validation should fail immediately
    )

    t2_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=_ingest_bronze,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    t3_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=_transform_silver,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    t4_quality = PythonOperator(
        task_id="run_quality_checks",
        python_callable=_run_quality_checks,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )

    t5_gold = PythonOperator(
        task_id="build_gold",
        python_callable=_build_gold,
        retries=2,
        retry_delay=timedelta(minutes=10),
    )

    t6_publish = PythonOperator(
        task_id="publish_to_postgres",
        python_callable=_publish_to_postgres,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    t7_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=_pipeline_summary,
        trigger_rule="all_done",  # Always run summary even on partial failures
        retries=0,
    )

    # ── Task dependencies ─────────────────────────────────────────────────────
    t1_validate >> t2_bronze >> t3_silver >> t4_quality >> t5_gold >> t6_publish >> t7_summary
