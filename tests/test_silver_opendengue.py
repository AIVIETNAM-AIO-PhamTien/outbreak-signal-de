"""Data-quality and Delta integration tests for OpenDengue Silver."""

from datetime import datetime, timezone

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from ingestion.common import paths
from ingestion.common.validation import IngestionValidationError
from ingestion.silver import opendengue as silver


def row(**changes):
    base = {
        "adm_0_name": " VIET NAM ",
        "adm_1_name": "NA",
        "ISO_A0": "vnm",
        "RNE_iso_code": "VNM",
        "calendar_start_date": "2025-01-01",
        "Year": "2025",
        "dengue_total": "120",
        "S_res": "Admin0",
        "T_res": "Year",
        "case_definition_standardised": "Total",
        "_source": "opendengue",
        "_source_file": "opendengue/V1.3/spatial.csv",
        "_ingested_at": "2026-10-01T00:00:00+00:00",
        "release": "V1.3",
    }
    return {**base, **changes}


def frame(spark, rows):
    schema = StructType([StructField(name, StringType(), True) for name in sorted(silver.BRONZE_COLUMNS)])
    return spark.createDataFrame(rows, schema=schema)


@pytest.mark.integration
def test_clean_missing_duplicates_and_types(spark):
    rows = [
        row(),
        row(),  # exact duplicate
        row(dengue_total="100", case_definition_standardised="Confirmed"),
        row(adm_1_name=" HANOI ", RNE_iso_code="VN-HN", S_res="Admin1",
            T_res="Month", dengue_total="0"),
        row(S_res="Admin2", adm_1_name="CENTRAL LUZON"),
        row(dengue_total="-1"),
        row(ISO_A0=None),
        row(calendar_start_date="not-a-date"),
    ]
    result, metrics = silver.transform(
        frame(spark, rows), silver_ingested_at=datetime(2026, 10, 5, tzinfo=timezone.utc)
    )
    assert metrics == {
        "bronze_rows": 8,
        "excluded_admin2_or_unknown": 1,
        "invalid_rows": 3,
        "deduplicated_rows": 2,
        "silver_rows": 2,
    }
    assert result.columns == list(silver.SILVER_COLUMNS)
    assert {field.name: field.dataType.simpleString() for field in result.schema.fields} == silver.EXPECTED_TYPES
    got = {(r.s_res, r.adm_1_name): r for r in result.collect()}
    assert got["Admin0", None].dengue_total == 120
    assert got["Admin0", None].adm_0_name == "Viet Nam"
    assert got["Admin0", None].iso3 == "VNM"
    assert got["Admin0", None].p_code == "VNM"
    assert got["Admin0", None].t_res == "year"
    assert got["Admin1", "Hanoi"].p_code == "VN-HN"
    assert got["Admin1", "Hanoi"].dengue_total == 0
    assert got["Admin1", "Hanoi"].t_res == "month"


@pytest.mark.integration
def test_country_name_title_case_with_hyphen_and_apostrophe(spark):
    bronze = frame(spark, [
        row(adm_0_name="TIMOR-LESTE", ISO_A0="TLS", RNE_iso_code="TLS"),
        row(adm_0_name="LAO PEOPLE'S DEMOCRATIC REPUBLIC", ISO_A0="LAO",
            RNE_iso_code="LAO"),
    ])
    result, _ = silver.transform(bronze)
    names = {r["adm_0_name"] for r in result.select("adm_0_name").collect()}
    assert names == {"Timor-Leste", "Lao People's Democratic Republic"}
    assert {r["adm_0_name"] for r in bronze.select("adm_0_name").collect()} == {
        "TIMOR-LESTE", "LAO PEOPLE'S DEMOCRATIC REPUBLIC"
    }


@pytest.mark.integration
def test_admin1_name_is_stored_without_whitespace(spark):
    bronze = frame(spark, [row(
        adm_1_name="  HAI   PHONG  ", RNE_iso_code="VN-HP",
        S_res="Admin1", T_res="Month",
    )])
    result, _ = silver.transform(bronze)
    assert result.first().adm_1_name == "HaiPhong"
    assert bronze.first().adm_1_name == "  HAI   PHONG  "


@pytest.mark.integration
def test_week_becomes_epiweek_without_shifting_period_or_cases(spark):
    bronze = frame(spark, [
        row(calendar_start_date="2024-12-29", Year="2024", T_res="Week",
            dengue_total="42"),
    ])
    result, metrics = silver.transform(bronze)
    record = result.first()
    assert metrics["silver_rows"] == 1
    assert record.t_res == "epiweek"
    assert record.start_date.isoformat() == "2024-12-29"
    assert record.year == 2024
    assert record.dengue_total == 42
    assert bronze.first()["T_res"] == "Week"


@pytest.mark.integration
def test_same_definition_conflicting_cases_fails(spark):
    bronze = frame(spark, [row(), row(dengue_total="121")])
    with pytest.raises(IngestionValidationError, match="conflicting dengue_total"):
        silver.transform(bronze)


@pytest.mark.integration
def test_latest_release_and_partition_safe_rerun(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(paths, "BRONZE_ROOT", tmp_path / "bronze")
    monkeypatch.setattr(paths, "SILVER_ROOT", tmp_path / "silver")
    source = paths.bronze_path("opendengue")
    frame(spark, [
        row(release="V1.9", dengue_total="1"),
        row(release="V1.10", dengue_total="2"),
    ]).write.format("delta").mode("overwrite").partitionBy("release").save(str(source))

    first = silver.ingest(spark)
    assert first["release"] == "V1.10"
    assert first["silver_rows"] == 1
    target = str(paths.silver_path("dengue_unified"))
    initial = spark.read.format("delta").load(target)
    assert initial.first()["dengue_total"] == 2

    # Simulate a future WHO partition: re-running OpenDengue must retain it.
    (initial.withColumn("_source", F.lit("who_gho"))
     .write.format("delta").mode("append").save(target))
    second = silver.ingest(spark)
    assert second["silver_rows"] == 1
    counts = {r["_source"]: r["count"] for r in spark.read.format("delta").load(target)
              .groupBy("_source").count().collect()}
    assert counts == {"opendengue": 1, "who_gho": 1}


@pytest.mark.integration
def test_legacy_rne_column_is_migrated_without_losing_delta_history(
    spark, tmp_path, monkeypatch
):
    monkeypatch.setattr(paths, "BRONZE_ROOT", tmp_path / "bronze")
    monkeypatch.setattr(paths, "SILVER_ROOT", tmp_path / "silver")
    bronze = frame(spark, [row()])
    bronze.write.format("delta").mode("overwrite").partitionBy("release").save(
        str(paths.bronze_path("opendengue"))
    )
    target = str(paths.silver_path("dengue_unified"))
    old, _ = silver.transform(bronze)
    (old.withColumnRenamed("p_code", "rne_iso_code")
     .write.format("delta").mode("overwrite").partitionBy("_source").save(target))

    assert silver.ingest(spark)["silver_rows"] == 1
    current = spark.read.format("delta").load(target)
    assert "p_code" in current.columns and "rne_iso_code" not in current.columns
    assert current.first()["p_code"] == "VNM"
    historic = spark.read.format("delta").option("versionAsOf", 0).load(target)
    assert "rne_iso_code" in historic.columns
