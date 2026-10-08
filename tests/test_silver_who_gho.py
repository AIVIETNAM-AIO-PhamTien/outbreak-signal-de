"""Data-quality and Delta integration tests for WHO GHO Silver."""

from datetime import datetime, timezone

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from ingestion.common import paths
from ingestion.common.validation import IngestionValidationError
from ingestion.silver import who_gho as silver


def row(**changes):
    base = {
        "COUNTRY": " VIET NAM ",
        "ISO3": "vnm",
        "YEAR": "2024",
        "DATE_TYPE": "month",
        "DATE_NUM": "1",
        "START_DATE": "2024-01-01",
        "CASES": "120",
        "_source": "who_gho",
        "_source_file": "who_gho/2026-10-01.json",
        "_ingested_at": "2026-10-01T00:00:00+00:00",
        "ingestion_date": "2026-10-01",
    }
    return {**base, **changes}


def frame(spark, rows):
    schema = StructType([
        StructField(name, StringType(), True) for name in sorted(silver.BRONZE_COLUMNS)
    ])
    return spark.createDataFrame(rows, schema=schema)


@pytest.mark.integration
def test_fill_missing_month_dates_and_skip_future_or_invalid(spark):
    bronze = frame(spark, [
        row(YEAR="2007", DATE_NUM="3", START_DATE=None, CASES="0"),
        row(DATE_TYPE="isoweek", DATE_NUM="2", START_DATE="2024-01-08"),
        row(DATE_TYPE="epiweek", DATE_NUM="3", START_DATE="2024-01-14"),
        row(YEAR="2029", START_DATE="2029-01-01"),
        row(DATE_TYPE="isoweek", DATE_NUM="4", START_DATE=None),
        row(DATE_NUM="5", START_DATE="2024-05-01", CASES="-1"),
    ])
    result, metrics = silver.transform(
        bronze, silver_ingested_at=datetime(2026, 10, 5, tzinfo=timezone.utc)
    )
    assert metrics == {
        "bronze_rows": 6,
        "derived_start_date_rows": 1,
        "future_rows_skipped": 1,
        "invalid_rows": 2,
        "deduplicated_rows": 0,
        "silver_rows": 3,
    }
    assert result.columns == list(silver.SILVER_COLUMNS)
    assert {field.name: field.dataType.simpleString() for field in result.schema.fields} == silver.EXPECTED_TYPES
    got = {r.t_res: r for r in result.collect()}
    assert got["month"].start_date.isoformat() == "2007-03-01"
    assert got["month"].adm_0_name == "Viet Nam"
    assert got["month"].adm_1_name is None and got["month"].p_code is None
    assert got["month"].s_res == "Admin0" and got["month"].dengue_total == 0
    assert got["isoweek"].start_date.isoformat() == "2024-01-08"
    assert got["epiweek"].start_date.isoformat() == "2024-01-14"
    assert bronze.where(F.col("START_DATE").isNull()).count() == 2


@pytest.mark.integration
def test_exact_duplicates_are_removed(spark):
    result, metrics = silver.transform(frame(spark, [row(), row()]))
    assert result.count() == 1
    assert metrics["deduplicated_rows"] == 1


@pytest.mark.integration
def test_conflicting_cases_for_same_period_fail(spark):
    with pytest.raises(IngestionValidationError, match="conflicting CASES"):
        silver.transform(frame(spark, [row(), row(CASES="121")]))


@pytest.mark.integration
def test_latest_snapshot_and_partition_safe_rerun(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(paths, "BRONZE_ROOT", tmp_path / "bronze")
    monkeypatch.setattr(paths, "SILVER_ROOT", tmp_path / "silver")
    bronze = frame(spark, [
        row(CASES="10"),
        row(CASES="20", ingestion_date="2026-10-02"),
    ])
    bronze.write.format("delta").mode("overwrite").partitionBy("ingestion_date").save(
        str(paths.bronze_path("who_gho"))
    )
    target = str(paths.silver_path("dengue_unified"))
    existing, _ = silver.transform(frame(spark, [row()]))
    (existing.withColumn("_source", F.lit("opendengue"))
     .write.format("delta").mode("overwrite").partitionBy("_source").save(target))

    first = silver.ingest(spark)
    assert first["bronze_snapshot_date"] == "2026-10-02"
    assert first["silver_rows"] == 1
    second = silver.ingest(spark)
    assert second["silver_rows"] == 1
    all_rows = spark.read.format("delta").load(target)
    counts = {r["_source"]: r["count"] for r in all_rows.groupBy("_source").count().collect()}
    assert counts == {"opendengue": 1, "who_gho": 1}
    assert all_rows.where(F.col("_source") == "who_gho").first().dengue_total == 20
