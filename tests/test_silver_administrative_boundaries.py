"""Bronze COD-AB to Silver map-location lookup tests."""

from datetime import date

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from ingestion.common import paths
from ingestion.common.validation import IngestionValidationError
from ingestion.silver import administrative_boundaries as silver


def row(**changes):
    base = {
        "iso3": "VNM", "_sheet": "admin1", "admin_level": None,
        "adm0_name": "VIET NAM", "adm0_pcode": "VN",
        "adm1_name": "An Giang", "adm1_pcode": "VN91",
        "valid_on": "2025-09-25 00:00:00",
        "center_lat": None, "center_lon": None,
        "x_coord": None, "y_coord": None,
        "_source": "hdx_cod_ab", "_source_file": "hdx_cod_ab/VNM/admin1.csv",
        "_ingested_at": "2026-10-01T00:00:00+00:00",
    }
    return {**base, **changes}


def point(**changes):
    return row(**{
        "_sheet": "adminpoints", "admin_level": "1",
        "x_coord": "105.0977097", "y_coord": "10.17824072",
        "_source_file": "hdx_cod_ab/VNM/adminpoints.csv", **changes,
    })


def frame(spark, rows):
    schema = StructType([
        StructField(name, StringType(), True) for name in sorted(silver.BRONZE_COLUMNS)
    ])
    return spark.createDataFrame(rows, schema=schema)


@pytest.mark.integration
def test_admin0_admin1_coordinates_types_and_lineage(spark):
    bronze = frame(spark, [
        row(adm1_name="  AN   GIANG "), point(),
        row(iso3="THA", adm0_name="Thailand", adm0_pcode="TH",
            adm1_name="Bangkok", adm1_pcode="TH10", center_lat="13.75",
            center_lon="100.5", valid_on="2022-01-22 00:00:00"),
        point(iso3="PHL", admin_level="2", adm1_pcode="PH01"),
    ])
    result, metrics = silver.transform(bronze)
    assert metrics == {
        "bronze_rows": 4, "admin1_rows": 2, "admin1_points": 1,
        "admin1_center_fallback_rows": 1, "country_rows": 2, "silver_rows": 4,
    }
    assert result.columns == list(silver.SILVER_COLUMNS)
    assert {field.name: field.dataType.simpleString() for field in result.schema} == silver.EXPECTED_TYPES
    rows = {(r.iso3, r.adm_1_name): r for r in result.collect()}
    assert rows["VNM", "AnGiang"].p_code == "VN91"
    assert rows["VNM", "AnGiang"].valid_on == date(2025, 9, 25)
    assert rows["VNM", "AnGiang"].x_coord == pytest.approx(105.0977097)
    assert rows["VNM", "AnGiang"].y_coord == pytest.approx(10.17824072)
    assert rows["THA", "Bangkok"].x_coord == 100.5
    assert rows["THA", "Bangkok"].y_coord == 13.75
    assert rows["VNM", None].adm_0_name == "Viet Nam"
    assert rows["VNM", None].p_code == "VN"
    assert rows["VNM", None].x_coord is None and rows["VNM", None].y_coord is None
    assert {r._source for r in rows.values()} == {"hdx_cod_ab"}
    assert all(r._source_file and r._bronze_ingested_at and r._silver_ingested_at
               for r in rows.values())


@pytest.mark.integration
def test_duplicate_or_missing_admin1_coordinates_fail(spark):
    with pytest.raises(IngestionValidationError, match="duplicate adminpoints key"):
        silver.transform(frame(spark, [row(), point(), point()]))
    with pytest.raises(IngestionValidationError, match="coordinates missing or invalid"):
        silver.transform(frame(spark, [row(), point(x_coord=None, y_coord=None)]))


@pytest.mark.integration
def test_delta_rerun_from_bronze_is_idempotent(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(paths, "BRONZE_ROOT", tmp_path / "bronze")
    monkeypatch.setattr(paths, "SILVER_ROOT", tmp_path / "silver")
    frame(spark, [row(), point()]).write.format("delta").mode("overwrite").partitionBy(
        "iso3"
    ).save(str(paths.bronze_path("hdx_cod_ab")))

    assert silver.ingest(spark)["silver_rows"] == 2
    assert silver.ingest(spark)["silver_rows"] == 2
    actual = spark.read.format("delta").load(str(paths.silver_path("administrative_boundaries")))
    assert actual.count() == 2
    assert {r.p_code for r in actual.collect()} == {"VN", "VN91"}
    assert not paths.silver_path("dengue_history").exists()
