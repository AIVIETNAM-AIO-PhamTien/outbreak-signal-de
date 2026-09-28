"""Exploratory analysis of the Singapore NEA bronze table.

Answers the questions that drive the remaining design decisions: what columns
actually carry data, how often our own polling ran, and — separately — how
often the source itself publishes a change. Those last two are easy to
conflate and mean very different things.

Run with:
    python scripts/eda_bronze_sg_nea.py
"""

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingestion.common.spark_session import build_spark_session
from ingestion.common.paths import bronze_path

BRONZE_PATH = bronze_path("sg_nea")


def describe_schema(frame: DataFrame) -> None:
    """Print each column's type and how much of it is actually populated.

    A column that is always null is a column worth questioning: either the
    source stopped sending it, or it was mapped wrongly.

    Args:
        frame: The bronze table.
    """
    total = frame.count()
    print(f"\n=== SCHEMA ({total} rows) ===")
    print(f"{'column':<24}{'type':<14}{'nulls':>8}{'distinct':>10}")
    for field in frame.schema.fields:
        nulls = frame.filter(F.col(field.name).isNull()).count()
        distinct = frame.select(field.name).distinct().count()
        print(
            f"{field.name:<24}{field.dataType.simpleString():<14}"
            f"{nulls:>8}{distinct:>10}"
        )


def describe_polling(frame: DataFrame) -> None:
    """Print how often *we* polled — a property of the scheduler, not the data.

    Args:
        frame: The bronze table.
    """
    print("\n=== OUR POLLING (batch_id / fetched_at) ===")
    batches = (
        frame.groupBy("batch_id")
        .agg(F.count("*").alias("rows"), F.min("fetched_at").alias("fetched_at"))
        .orderBy("batch_id")
        .collect()
    )
    previous: datetime | None = None
    for row in batches:
        gap = ""
        if previous is not None:
            hours = (row["fetched_at"] - previous).total_seconds() / 3600
            gap = f"  (+{hours:.1f}h)"
        print(f"  {row['batch_id']}  rows={row['rows']:>3}  {row['fetched_at']}{gap}")
        previous = row["fetched_at"]


def describe_source_updates(frame: DataFrame) -> None:
    """Print how often the SOURCE publishes changes, via `FMEL_UPD_D`.

    This is the number that should drive the polling cadence. Polling far more
    often than the source changes just records the same state repeatedly.

    Args:
        frame: The bronze table.
    """
    print("\n=== SOURCE UPDATE CADENCE (FMEL_UPD_D) ===")
    stamps = sorted(
        row["cluster_updated_at_raw"]
        for row in frame.select("cluster_updated_at_raw").distinct().collect()
        if row["cluster_updated_at_raw"]
    )
    previous: datetime | None = None
    for raw in stamps:
        moment = datetime.strptime(raw, "%Y%m%d%H%M%S")
        clusters = frame.filter(F.col("cluster_updated_at_raw") == raw).select(
            "locality"
        )
        gap = ""
        if previous is not None:
            gap = f"  (+{(moment - previous).total_seconds() / 86400:.1f} days)"
        print(
            f"  {moment:%Y-%m-%d %H:%M}  {clusters.distinct().count():>2} cluster(s){gap}"
        )
        previous = moment


def describe_clusters(frame: DataFrame) -> None:
    """Print per-cluster case-count statistics from the newest batch.

    Args:
        frame: The bronze table.
    """
    newest = frame.agg(F.max("batch_id")).collect()[0][0]
    latest = frame.filter(F.col("batch_id") == newest)
    print(f"\n=== CLUSTERS IN NEWEST BATCH ({newest}) ===")
    latest.select("locality", "case_count", "cluster_updated_at_raw").orderBy(
        F.col("case_count").desc()
    ).show(50, truncate=46)

    stats = latest.agg(
        F.count("*").alias("clusters"),
        F.sum("case_count").alias("total_cases"),
        F.min("case_count").alias("min"),
        F.max("case_count").alias("max"),
        F.round(F.avg("case_count"), 1).alias("mean"),
    ).collect()[0]
    print(
        f"  clusters={stats['clusters']}  total_cases={stats['total_cases']}  "
        f"min={stats['min']}  max={stats['max']}  mean={stats['mean']}"
    )


def describe_changes_between_batches(frame: DataFrame) -> None:
    """Print how much actually changed from one batch to the next.

    Compares `inc_crc`, the source's own content checksum, keyed on locality
    rather than `object_id` — the latter is renumbered on every publish and so
    cannot be used to follow a cluster across batches.

    Args:
        frame: The bronze table.
    """
    print("\n=== CHANGE BETWEEN BATCHES ===")
    batch_ids = sorted(row["batch_id"] for row in frame.select("batch_id").distinct().collect())
    if len(batch_ids) < 2:
        print("  need at least 2 batches to compare")
        return

    for earlier_id, later_id in zip(batch_ids, batch_ids[1:]):
        earlier = frame.filter(F.col("batch_id") == earlier_id).select(
            "locality", F.col("inc_crc").alias("crc_before")
        )
        later = frame.filter(F.col("batch_id") == later_id).select(
            "locality", F.col("inc_crc").alias("crc_after")
        )
        joined = earlier.join(later, on="locality", how="full_outer")

        appeared = joined.filter(F.col("crc_before").isNull()).count()
        vanished = joined.filter(F.col("crc_after").isNull()).count()
        changed = joined.filter(
            F.col("crc_before").isNotNull()
            & F.col("crc_after").isNotNull()
            & (F.col("crc_before") != F.col("crc_after"))
        ).count()
        same = joined.filter(F.col("crc_before") == F.col("crc_after")).count()

        print(
            f"  {earlier_id} -> {later_id}:  unchanged={same}  changed={changed}  "
            f"new={appeared}  gone={vanished}"
        )


def main() -> None:
    """Load the bronze table and print every section of the report."""
    spark: SparkSession = build_spark_session("eda_bronze_sg_nea")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        frame = spark.read.format("delta").load(str(BRONZE_PATH)).cache()
        describe_schema(frame)
        describe_polling(frame)
        describe_source_updates(frame)
        describe_clusters(frame)
        describe_changes_between_batches(frame)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
