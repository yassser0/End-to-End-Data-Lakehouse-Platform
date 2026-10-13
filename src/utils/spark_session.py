"""
Spark session factory with Delta Lake extensions.
Creates a properly configured SparkSession for the lakehouse pipeline.
"""
from pyspark.sql import SparkSession
from src.utils.config import SPARK_MASTER, SPARK_APP_NAME
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def get_spark_session(app_name: str | None = None) -> SparkSession:
    """
    Return a configured SparkSession with Delta Lake support.

    Delta Lake 3.2 requires Spark 3.5.x and the delta-spark package.
    The session is a singleton — multiple calls return the same instance.
    """
    name = app_name or SPARK_APP_NAME
    builder = (
        SparkSession.builder.appName(name)
        .master(SPARK_MASTER)
        # Delta Lake extensions
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Keep Delta Lake jars consistent with installed delta-spark package
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.1",
        )
        # Suppress noisy INFO logs from Spark internals
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        # Disable UI to reduce port conflicts in CI
        .config("spark.ui.enabled", "false")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created app=%s master=%s", name, SPARK_MASTER)
    return spark
