"""
PostgreSQL extraction module.
Reads reference tables (customers, products) from PostgreSQL using JDBC.
Uses parameterized JDBC properties — no SQL injection risk.
"""
from pyspark.sql import DataFrame, SparkSession

from src.utils.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def _jdbc_url() -> str:
    return f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


def _jdbc_props() -> dict:
    return {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }


def read_table(spark: SparkSession, table: str) -> DataFrame:
    """
    Read a PostgreSQL table into a Spark DataFrame via JDBC.
    Table name is restricted to an allow-list to prevent injection.

    Allowed tables: customers, products
    """
    _ALLOWED_TABLES = {"customers", "products"}
    if table not in _ALLOWED_TABLES:
        raise ValueError(
            f"Table '{table}' is not in the allowed table list: {_ALLOWED_TABLES}. "
            "This restriction prevents SQL injection via table-name parameters."
        )

    logger.info("Reading PostgreSQL table=%s host=%s", table, POSTGRES_HOST)
    df = spark.read.jdbc(
        url=_jdbc_url(),
        table=table,
        properties=_jdbc_props(),
    )
    logger.info("Loaded table=%s rows=%d", table, df.count())
    return df
