"""Selection rules for national and OpenDengue Admin1 dengue history."""

from datetime import date, datetime, timezone

import pytest
from pyspark.sql.types import (
    DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType,
    TimestampType,
)

from ingestion.common import paths
from ingestion.common.validation import IngestionValidationError
from ingestion.silver import administrative_boundaries, dengue_history, opendengue, who_gho
from ingestion.silver.schema import SILVER_COLUMNS

TYPES = {
    "string": StringType(), "date": DateType(), "int": IntegerType(),
    "bigint": LongType(), "double": DoubleType(), "timestamp": TimestampType(),
}


def row(start, resolution, source, cases, **changes):
    base = {
        "adm_0_name": "Viet Nam", "adm_1_name": None, "iso3": "VNM",
        "p_code": None, "start_date": date.fromisoformat(start),
        "year": int(start[:4]), "dengue_total": cases, "s_res": "Admin0",
        "t_res": resolution, "_source": source, "_source_file": f"{source}/file",
        "_bronze_ingested_at": datetime(2026, 10, 1, tzinfo=timezone.utc),
        "_silver_ingested_at": datetime(2026, 10, 5, tzinfo=timezone.utc),
    }
    return {**base, **changes}


def frame(spark, rows):
    schema = StructType([
        StructField(name, TYPES[kind], True)
        for name, kind in zip(SILVER_COLUMNS, (
            "string", "string", "string", "string", "date", "int", "bigint",
            "string", "string", "string", "string", "timestamp", "timestamp",
        ))
    ])
    return spark.createDataFrame(rows, schema=schema)


def bronze_frame(spark, columns, rows):
    schema = StructType([StructField(name, StringType(), True) for name in sorted(columns)])
    return spark.createDataFrame(rows, schema=schema)


def boundary_frame(spark, rows):
    schema = StructType([
        StructField(name, TYPES[kind], True)
        for name, kind in administrative_boundaries.EXPECTED_TYPES.items()
    ])
    return spark.createDataFrame(rows, schema=schema)


def boundary(name, code, *, iso3="VNM", country="Viet Nam"):
    return {
        "adm_0_name": country, "adm_1_name": name, "iso3": iso3,
        "p_code": code, "valid_on": date(2025, 9, 25),
        "x_coord": None if name is None else 105.0,
        "y_coord": None if name is None else 21.0,
        "_source": "hdx_cod_ab", "_source_file": "hdx_cod_ab/admin1.csv",
        "_bronze_ingested_at": datetime(2026, 10, 1, tzinfo=timezone.utc),
        "_silver_ingested_at": datetime(2026, 10, 5, tzinfo=timezone.utc),
    }


@pytest.mark.integration
def test_compact_name_key_and_historical_vietnam_skip(spark):
    history = frame(spark, [
        row("2024-01-01", "month", "opendengue", 10, p_code="VNM"),
        row("2024-01-01", "month", "opendengue", 5,
            s_res="Admin1", adm_1_name="Hanoi", p_code="VN-HN"),
        row("2024-01-01", "month", "opendengue", 7, iso3="THA",
            adm_0_name="Thailand", s_res="Admin1", adm_1_name="LOP BURI", p_code="TH-16"),
    ])
    lookup = boundary_frame(spark, [
        boundary(None, "VN"), boundary("HANOI", "VN01"),
        boundary("Lop Buri", "TH16", iso3="THA", country="Thailand"),
    ])
    mapped, metrics = dengue_history.map_p_codes(history, lookup)
    got = {(r.iso3, r.adm_1_name): r.p_code for r in mapped.collect()}
    assert got == {("VNM", None): "VN", ("VNM", "Hanoi"): None,
                   ("THA", "LopBuri"): "TH16"}
    assert metrics == {
        "admin1_p_codes_mapped": 1,
        "admin1_p_codes_unmatched": 1,
        "admin0_p_codes_mapped": 1,
        "admin1_p_codes_compact_name": 1,
        "admin1_p_codes_source_code": 0,
        "admin1_vnm_historical_unmapped": 1,
    }
    mapped_again, _ = dengue_history.map_p_codes(mapped, lookup)
    assert {(r.iso3, r.adm_1_name): r.p_code for r in mapped_again.collect()} == got
    assert history.where("adm_1_name = 'Hanoi'").first().p_code == "VN-HN"

    duplicate = boundary_frame(spark, [
        boundary("Lop Buri", "TH16", iso3="THA", country="Thailand"),
        boundary("Lopburi", "TH99", iso3="THA", country="Thailand"),
    ])
    with pytest.raises(IngestionValidationError, match="ambiguous"):
        dengue_history.map_p_codes(history, duplicate)


@pytest.mark.integration
def test_verified_numeric_code_formula_and_no_global_conversion(spark):
    history = frame(spark, [
        row("2024-01-01", "month", "opendengue", 5, iso3="THA",
            adm_0_name="Thailand", s_res="Admin1", adm_1_name="Bungkan", p_code="TH-38"),
        row("2024-01-01", "month", "opendengue", 6, iso3="KHM",
            adm_0_name="Cambodia", s_res="Admin1", adm_1_name="Kampong-Thom", p_code="KH-6"),
        row("2024-01-01", "month", "opendengue", 7, iso3="IDN",
            adm_0_name="Indonesia", s_res="Admin1", adm_1_name="Babel", p_code="ID-BB"),
        row("2024-01-01", "month", "opendengue", 8, iso3="MYS",
            adm_0_name="Malaysia", s_res="Admin1", adm_1_name="Melaka", p_code="MY-04"),
        row("2024-01-01", "month", "opendengue", 9, iso3="SGP",
            adm_0_name="Singapore", s_res="Admin1", adm_1_name="Central Singapore",
            p_code="SG-01"),
    ])
    lookup = boundary_frame(spark, [
        boundary("Bueng Kan", "TH38", iso3="THA", country="Thailand"),
        boundary("Kampong Thom", "KH06", iso3="KHM", country="Cambodia"),
        boundary("Kepulauan Bangka Belitung", "ID19", iso3="IDN", country="Indonesia"),
        boundary("Melaka", "MY06", iso3="MYS", country="Malaysia"),
        boundary("W.P. Kuala Lumpur", "MY04", iso3="MYS", country="Malaysia"),
    ])
    mapped, metrics = dengue_history.map_p_codes(history, lookup)
    got = {(r.iso3, r.adm_1_name): r.p_code for r in mapped.collect()}
    assert got == {
        ("THA", "Bungkan"): "TH38", ("KHM", "Kampong-Thom"): "KH06",
        ("IDN", "Babel"): None, ("MYS", "Melaka"): "MY06",
        ("SGP", "CentralSingapore"): None,
    }
    assert metrics["admin1_p_codes_source_code"] == 2
    assert metrics["admin1_p_codes_compact_name"] == 1
    assert metrics["admin1_p_codes_unmatched"] == 2

    conflicting_history = frame(spark, [
        row("2024-01-01", "month", "opendengue", 5, iso3="THA",
            adm_0_name="Thailand", s_res="Admin1", adm_1_name="Lop Buri", p_code="TH-11"),
    ])
    conflicting_lookup = boundary_frame(spark, [
        boundary("Lop Buri", "TH16", iso3="THA", country="Thailand"),
        boundary("Samut Prakan", "TH11", iso3="THA", country="Thailand"),
    ])
    with pytest.raises(IngestionValidationError, match="conflicting COD P-code"):
        dengue_history.map_p_codes(conflicting_history, conflicting_lookup)


@pytest.mark.integration
def test_hdx_country_iso_validates_both_sources_and_skips_missing_hdx(spark):
    lookup = boundary_frame(spark, [
        boundary(None, "VN"),
        boundary(None, "TH", iso3="THA", country="Thailand"),
    ])
    source = frame(spark, [
        row("2024-01-01", "month", "opendengue", 1,
            adm_0_name="VIET NAM"),
        row("2024-01-01", "month", "who_gho", 2, iso3="SGP",
            adm_0_name="Singapore"),
    ])
    assert dengue_history.validate_country_iso(source, lookup, "test") == {
        "hdx_iso3_matched_rows": 1, "hdx_iso3_absent_rows": 1,
    }
    wrong = frame(spark, [row("2024-01-01", "month", "who_gho", 1,
                              iso3="THA", adm_0_name="Viet Nam")])
    with pytest.raises(IngestionValidationError, match="conflicts with HDX ISO3"):
        dengue_history.validate_country_iso(wrong, lookup, "WHO")


@pytest.mark.integration
def test_opendengue_priority_uses_full_period_overlap(spark):
    source = frame(spark, [
        row("2022-01-01", "year", "opendengue", 100),
        row("2024-01-01", "month", "opendengue", 30),
        row("2024-01-07", "epiweek", "opendengue", 7),
        row("2022-06-01", "month", "who_gho", 60),  # OD year overlaps
        row("2024-01-01", "month", "who_gho", 31),  # OD month overlaps
        row("2024-02-05", "isoweek", "who_gho", 8),
        row("2024-02-01", "month", "who_gho", 32),  # WHO week wins
        row("2024-02-11", "epiweek", "who_gho", 9),  # ISO week shares Sunday
        row("2024-03-01", "month", "who_gho", 33),
        row("2024-04-01", "month", "opendengue", 40,
            s_res="Admin1", adm_1_name="Hanoi", p_code="VN-HN"),
        row("2024-01-01", "year", "opendengue", 400,
            s_res="Admin1", adm_1_name="Hanoi", p_code="VN-HN"),
        row("2024-04-01", "month", "opendengue", 50,
            s_res="Admin1", adm_1_name="HAI PHONG", p_code="VN-HP"),
    ])
    result, metrics = dengue_history.transform(source)
    got = {(r.s_res, r.adm_1_name, r.start_date.isoformat(), r._source): r.dengue_total
           for r in result.collect()}
    assert got == {
        ("Admin0", None, "2022-01-01", "opendengue"): 100,
        ("Admin0", None, "2024-01-07", "opendengue"): 7,
        ("Admin0", None, "2024-02-05", "who_gho"): 8,
        ("Admin0", None, "2024-03-01", "who_gho"): 33,
        ("Admin1", "Hanoi", "2024-04-01", "opendengue"): 40,
        ("Admin1", "HaiPhong", "2024-04-01", "opendengue"): 50,
    }
    assert metrics == {
        "opendengue_national_rows": 3,
        "opendengue_kept": 2,
        "opendengue_overlapping_grains_skipped": 1,
        "opendengue_admin1_rows": 3,
        "opendengue_admin1_kept": 2,
        "opendengue_admin1_overlapping_grains_skipped": 1,
        "who_national_rows": 6,
        "who_kept": 2,
        "who_overlapping_periods_skipped": 4,
        "history_rows": 6,
    }


@pytest.mark.integration
def test_touching_periods_are_not_overlapping(spark):
    source = frame(spark, [
        row("2024-01-01", "month", "opendengue", 10),
        row("2024-02-01", "month", "who_gho", 20),
    ])
    result, _ = dengue_history.transform(source)
    assert result.count() == 2


@pytest.mark.integration
def test_rebuild_reads_latest_bronze_without_silver_intermediary(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(paths, "BRONZE_ROOT", tmp_path / "bronze")
    monkeypatch.setattr(paths, "SILVER_ROOT", tmp_path / "silver")
    od_row = {
        "adm_0_name": "VIET NAM", "adm_1_name": "NA", "ISO_A0": "VNM",
        "RNE_iso_code": "VNM", "calendar_start_date": "2024-01-01",
        "Year": "2024", "dengue_total": "10", "S_res": "Admin0",
        "T_res": "Month", "case_definition_standardised": "Total",
        "_source": "opendengue", "_source_file": "opendengue/spatial.csv",
        "_ingested_at": "2026-10-01T00:00:00+00:00", "release": "V1.4",
    }
    bronze_frame(spark, opendengue.BRONZE_COLUMNS, [
        {**od_row, "release": "V1.3", "dengue_total": "1"}, od_row,
        {**od_row, "adm_1_name": "HANOI", "RNE_iso_code": "VN-HN",
         "S_res": "Admin1", "dengue_total": "5"},
    ]).write.format("delta").mode("overwrite").partitionBy("release").save(
        str(paths.bronze_path("opendengue"))
    )
    who_row = {
        "COUNTRY": "Viet Nam", "ISO3": "VNM", "YEAR": "2024",
        "DATE_TYPE": "month", "DATE_NUM": "2", "START_DATE": None,
        "CASES": "30", "_source": "who_gho",
        "_source_file": "who_gho/2026-10-02.json",
        "_ingested_at": "2026-10-02T00:00:00+00:00",
        "ingestion_date": "2026-10-02",
    }
    bronze_frame(spark, who_gho.BRONZE_COLUMNS, [
        {**who_row, "ingestion_date": "2026-10-01", "CASES": "99"},
        {**who_row, "DATE_NUM": "1", "START_DATE": "2024-01-01", "CASES": "20"},
        who_row,
    ]).write.format("delta").mode("overwrite").partitionBy("ingestion_date").save(
        str(paths.bronze_path("who_gho"))
    )
    with pytest.raises(IngestionValidationError, match="ingest administrative_boundaries first"):
        dengue_history.ingest(spark)
    boundary_frame(spark, [boundary(None, "VN"), boundary("Hanoi", "VN01")]).write.format(
        "delta"
    ).mode("overwrite").save(str(paths.silver_path("administrative_boundaries")))
    first = dengue_history.ingest(spark)
    assert first["opendengue_release"] == "V1.4"
    assert first["who_snapshot_date"] == "2026-10-02"
    assert first["who_clean_derived_start_date_rows"] == 1
    assert first["history_rows"] == 3
    assert first["opendengue_admin1_kept"] == 1
    assert first["admin1_p_codes_mapped"] == 0
    assert first["admin1_vnm_historical_unmapped"] == 1
    assert dengue_history.ingest(spark)["history_rows"] == 3
    history = spark.read.format("delta").load(str(paths.silver_path("dengue_history")))
    assert {(r._source, r.s_res, r.adm_1_name, r.start_date.isoformat(), r.dengue_total)
            for r in history.collect()} == {
                ("opendengue", "Admin0", None, "2024-01-01", 10),
                ("opendengue", "Admin1", "Hanoi", "2024-01-01", 5),
                ("who_gho", "Admin0", None, "2024-02-01", 30),
            }
    assert {(r.s_res, r.adm_1_name, r.p_code) for r in history.collect()} == {
        ("Admin0", None, "VN"), ("Admin1", "Hanoi", None),
    }
    assert history.columns == list(SILVER_COLUMNS)
    assert not paths.silver_path("dengue_unified").exists()
