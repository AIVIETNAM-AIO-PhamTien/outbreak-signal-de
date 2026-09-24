"""Shared local-mode SparkSession builder with Delta Lake configured.

Every ingestion job (GDELT, Singapore NEA, WHO GHO) calls `build_spark_session()`
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
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
