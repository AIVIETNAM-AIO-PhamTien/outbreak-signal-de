"""Shared local-mode SparkSession builder with Delta Lake configured.

Every ingestion job (OpenDengue, News RSS, WHO GHO) calls `build_spark_session()`
to get a SparkSession pointed at Delta Lake, runs its own bronze-write, and lets
the caller stop the session. One session per job invocation, not a long-lived
shared session, so jobs don't hold JVM resources between scheduler runs.
"""

import os
import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Spark on Windows needs winutils.exe/hadoop.dll even to write to a local disk,
# because Windows has no POSIX equivalent for the chmod calls Hadoop makes.
# POSIX systems use native syscalls and need neither. The binaries are
# gitignored (third-party builds) — see README for how to obtain them.
if os.name == "nt":
    _HADOOP_HOME = Path(__file__).resolve().parents[2] / ".hadoop"
    os.environ.setdefault("HADOOP_HOME", str(_HADOOP_HOME))
    os.environ["PATH"] = f"{_HADOOP_HOME / 'bin'}{os.pathsep}{os.environ['PATH']}"

# Spark launches Python worker processes via whatever `python` is on PATH unless
# told otherwise — on this machine that resolves to a system Python without
# pyspark installed, and the workers die on startup. Pinning to the interpreter
# running this code keeps driver and workers on the same venv. Must be set
# before the JVM gateway launches, so SparkConf .config() is too late.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


def build_spark_session(app_name: str) -> SparkSession:
    """Build a local-mode SparkSession with Delta Lake SQL extensions enabled.

    Args:
        app_name: Spark application name, shown in logs/UI. Use the ingestion
            job's name (e.g. "gdelt_news_ingest") so multiple jobs' logs are
            distinguishable.

    Returns:
        A SparkSession configured with Delta's catalog and SQL extensions,
        running against all local CPU cores (`local[*]`).
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Moc thoi gian trong metadata deu la UTC; ep session dung UTC luon de
        # cot timestamp trong Bronze khong lech theo may cua tung nguoi.
        .config("spark.sql.session.timeZone", "UTC")
        # Thanh tien do ve de len log, lam bang tong ket cuoi batch kho doc.
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    # Chi giu log tu ERROR tro len - INFO cua Spark at het log cua pipeline.
    spark.sparkContext.setLogLevel("ERROR")
    return spark
