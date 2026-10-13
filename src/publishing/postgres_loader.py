"""
PostgreSQL publishing module.
Loads Gold layer Delta results into PostgreSQL analytical tables.

Security notes
--------------
- All queries use psycopg2 parameterized statements (%s placeholders)
- Table names are validated against an allow-list before use
- Passwords are read from environment variables only
- SQL errors are caught and logged without exposing details to callers

Write strategy
--------------
Each run TRUNCATES then INSERTs the analytical tables.
This is a full refresh: the tables always reflect the latest Gold state.
Re-running is idempotent — no duplicate analytical records accumulate.
"""
from __future__ import annotations

import contextlib
import os
from typing import Generator

import psycopg2
import psycopg2.extras
from pyspark.sql import DataFrame, SparkSession

from src.utils.config import (
    GOLD_PATH,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Allow-list of tables that can be published (prevents injection via table name)
_PUBLISHABLE_TABLES = frozenset(
    {
        "gold_daily_sales",
        "gold_product_performance",
        "gold_customer_analytics",
        "gold_order_status_summary",
        "gold_data_quality_metrics",
    }
)


@contextlib.contextmanager
def _get_connection() -> Generator:
    """Return a psycopg2 connection as a context manager."""
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=10,
        # TODO(security): Enable sslmode='verify-full' and provide CA cert
        # for mTLS in production deployments.
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _validate_table_name(table: str) -> None:
    """Raise ValueError if table is not in the allow-list."""
    if table not in _PUBLISHABLE_TABLES:
        raise ValueError(
            f"Table '{table}' is not in the publishable allow-list. "
            f"Allowed: {_PUBLISHABLE_TABLES}"
        )


def publish_dataframe(
    df: DataFrame,
    table: str,
    columns: list[str],
    truncate_first: bool = True,
) -> int:
    """
    Publish a Spark DataFrame to a PostgreSQL table.

    Collects DataFrame to driver, then bulk-inserts via psycopg2.
    Not suitable for very large DataFrames (use JDBC for those).

    Parameters
    ----------
    df           : Gold DataFrame to publish
    table        : target PostgreSQL table name (must be in allow-list)
    columns      : ordered list of column names to insert
    truncate_first: if True, truncate target before insert (full refresh)

    Returns
    -------
    int : number of rows inserted
    """
    _validate_table_name(table)

    # Collect to driver
    rows = [tuple(row[c] for c in columns) for row in df.collect()]
    if not rows:
        logger.info("No rows to publish table=%s", table)
        return 0

    # Build parameterized INSERT — no f-string SQL with user data
    # Table name validated against allow-list above; column names are code-defined
    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(columns)
    insert_sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"  # nosec

    with _get_connection() as conn:
        with conn.cursor() as cur:
            if truncate_first:
                cur.execute(f"TRUNCATE TABLE {table}")  # nosec — allow-listed
                logger.info("Truncated table=%s", table)
            psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=500)

    logger.info("Published table=%s rows=%d", table, len(rows))
    return len(rows)


def publish_all_gold(spark: SparkSession) -> dict[str, int]:
    """Load all Gold Delta tables and publish to PostgreSQL."""
    results = {}

    tables_config = [
        (
            "gold_daily_sales",
            f"{GOLD_PATH}/daily_sales",
            ["order_date", "total_revenue", "paid_order_count", "avg_order_value"],
        ),
        (
            "gold_product_performance",
            f"{GOLD_PATH}/product_performance",
            ["product_id", "product_name", "category", "total_quantity",
             "total_revenue", "order_count"],
        ),
        (
            "gold_customer_analytics",
            f"{GOLD_PATH}/customer_analytics",
            ["customer_id", "first_name", "last_name", "segment",
             "total_orders", "paid_orders", "total_spent", "avg_order_value"],
        ),
        (
            "gold_order_status_summary",
            f"{GOLD_PATH}/order_status_summary",
            ["order_status", "order_count", "total_amount"],
        ),
        (
            "gold_data_quality_metrics",
            f"{GOLD_PATH}/data_quality_metrics",
            ["batch_id", "entity", "ingested_count", "valid_count",
             "rejected_count", "quality_pct", "run_timestamp"],
        ),
    ]

    for pg_table, delta_path, cols in tables_config:
        try:
            df = spark.read.format("delta").load(delta_path)
            count = publish_dataframe(df, pg_table, cols)
            results[pg_table] = count
        except Exception as exc:
            logger.error("Failed to publish table=%s error=%s", pg_table, exc)
            results[pg_table] = -1

    return results
