"""
Spark session factory with Delta Lake extensions.
Creates a properly configured SparkSession for the lakehouse pipeline.

Windows note
------------
PySpark on Windows requires winutils.exe and HADOOP_HOME to be set.
This module sets HADOOP_HOME automatically if it is not already configured.
See: https://wiki.apache.org/hadoop/WindowsProblems
"""
import os
from pathlib import Path

from pyspark.sql import SparkSession

from src.utils.config import SPARK_MASTER, SPARK_APP_NAME
from src.utils.logging_config import get_logger


def _configure_windows_spark_env() -> None:
    """
    Fix Windows-specific Spark environment issues:

    1. HADOOP_HOME + winutils.exe
       Spark on Windows needs winutils.exe for filesystem operations.
       We point HADOOP_HOME to C:\\hadoop where winutils.exe was installed.

    2. PYSPARK_PYTHON
       On Windows the Python executable is 'python.exe', not 'python3'.
       A pre-installed standalone Spark (e.g. C:\\Program Files\\Spark) tries
       to launch 'python3' for worker processes, which fails.
       We set PYSPARK_PYTHON to the current interpreter's path.

    3. SPARK_HOME
       If a system-wide Spark is installed, PySpark may pick up the wrong
       SPARK_HOME. We override it to the PySpark package's bundled Spark so
       all versions stay consistent.

    This function is a no-op on Linux/macOS.
    """
    if os.name != "nt":
        return  # Linux/macOS — no action needed

    log = get_logger(__name__)

    # ── 1. HADOOP_HOME ────────────────────────────────────────────────────────
    if not os.environ.get("HADOOP_HOME"):
        default_hadoop = Path("C:/hadoop")
        if (default_hadoop / "bin" / "winutils.exe").exists():
            os.environ["HADOOP_HOME"] = str(default_hadoop)
            log.info("Auto-set HADOOP_HOME=%s", default_hadoop)
        else:
            log.warning(
                "winutils.exe not found at C:\\hadoop\\bin\\winutils.exe. "
                "Spark may fail. See https://github.com/cdarlint/winutils"
            )

    # ── 2. PYSPARK_PYTHON — use current interpreter, not 'python3' ────────────
    import sys
    current_python = sys.executable  # e.g. C:\Python313\python.exe
    if not os.environ.get("PYSPARK_PYTHON"):
        os.environ["PYSPARK_PYTHON"] = current_python
        log.info("Auto-set PYSPARK_PYTHON=%s", current_python)

    # Same for the driver — must match worker
    if not os.environ.get("PYSPARK_DRIVER_PYTHON"):
        os.environ["PYSPARK_DRIVER_PYTHON"] = current_python

    # ── 3. SPARK_HOME — force PySpark's bundled Spark, not a system install ───
    import pyspark
    pyspark_spark_home = Path(pyspark.__file__).parent
    current_spark_home = os.environ.get("SPARK_HOME", "")
    if current_spark_home and "Program Files" in current_spark_home:
        # A system Spark is set — override with PySpark's bundled version
        os.environ["SPARK_HOME"] = str(pyspark_spark_home)
        log.info(
            "Overrode SPARK_HOME from '%s' to PySpark bundled '%s'",
            current_spark_home,
            pyspark_spark_home,
        )


logger = get_logger(__name__)


def get_spark_session(app_name: str | None = None) -> SparkSession:
    """
    Return a configured SparkSession with Delta Lake support.

    Delta Lake 3.2 requires Spark 3.5.x and the delta-spark package.
    The session is a singleton — multiple calls return the same instance.
    """
    # Must be called BEFORE SparkSession.builder to take effect on Windows
    _configure_windows_spark_env()

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
