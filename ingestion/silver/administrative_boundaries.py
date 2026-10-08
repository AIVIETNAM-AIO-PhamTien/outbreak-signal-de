"""Build the Admin0/Admin1 location lookup from HDX COD-AB Bronze.

The requested table contains representative points, not boundary polygons.
Country coordinates stay null because COD-AB has no country-level point.
"""

from __future__ import annotations

from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ingestion.common.paths import bronze_path, silver_path
from ingestion.common.validation import IngestionValidationError
from ingestion.silver.schema import clean_text, compact_title_name, title_case_words

SOURCE = "hdx_cod_ab"
TABLE = "administrative_boundaries"
BRONZE_COLUMNS = {
    "iso3", "_sheet", "admin_level", "adm0_name", "adm0_pcode",
    "adm1_name", "adm1_pcode", "valid_on", "center_lat", "center_lon",
    "x_coord", "y_coord", "_source", "_source_file", "_ingested_at",
}
SILVER_COLUMNS = (
    "adm_0_name", "adm_1_name", "iso3", "p_code", "valid_on",
    "x_coord", "y_coord", "_source", "_source_file",
    "_bronze_ingested_at", "_silver_ingested_at",
)
EXPECTED_TYPES = dict(zip(SILVER_COLUMNS, (
    "string", "string", "string", "string", "date", "double", "double",
    "string", "string", "timestamp", "timestamp",
)))


def _has_duplicates(rows: DataFrame, *keys: str) -> bool:
    return bool(rows.groupBy(*keys).count().where(F.col("count") > 1).limit(1).count())


def transform(
    bronze: DataFrame, *, silver_ingested_at: datetime | None = None
) -> tuple[DataFrame, dict[str, int]]:
    """Join Admin1 labels to COD points and derive one Admin0 row per country."""
    missing = sorted(BRONZE_COLUMNS - set(bronze.columns))
    if missing:
        raise IngestionValidationError(
            f"[silver/administrative_boundaries] missing Bronze columns: {missing}"
        )

    bronze_rows = bronze.count()
    admin1 = bronze.where(F.col("_sheet") == "admin1").select(
        F.upper(clean_text("iso3")).alias("iso3"),
        title_case_words("adm0_name").alias("adm_0_name"),
        compact_title_name("adm1_name").alias("adm_1_name"),
        clean_text("adm0_pcode").alias("_adm0_pcode"),
        clean_text("adm1_pcode").alias("p_code"),
        F.to_date(clean_text("valid_on")).alias("valid_on"),
        clean_text("center_lon").cast("double").alias("_center_x"),
        clean_text("center_lat").cast("double").alias("_center_y"),
        clean_text("_source").alias("_source"),
        clean_text("_source_file").alias("_source_file"),
        F.to_timestamp(clean_text("_ingested_at")).alias("_bronze_ingested_at"),
    )
    point_rows = bronze.where(
        (F.col("_sheet") == "adminpoints") & (F.col("admin_level") == "1")
    ).select(
        F.upper(clean_text("iso3")).alias("iso3"),
        clean_text("adm1_pcode").alias("p_code"),
        clean_text("x_coord").cast("double").alias("_point_x"),
        clean_text("y_coord").cast("double").alias("_point_y"),
    )
    admin1_count, point_count = admin1.count(), point_rows.count()
    if not admin1_count or not point_count:
        raise IngestionValidationError(
            "[silver/administrative_boundaries] missing admin1 or adminpoints rows"
        )
    invalid_admin1 = admin1.where(
        F.col("iso3").isNull() | ~F.col("iso3").rlike(r"^[A-Z]{3}$")
        | F.col("adm_0_name").isNull() | F.col("adm_1_name").isNull()
        | F.col("_adm0_pcode").isNull() | F.col("p_code").isNull()
        | F.col("valid_on").isNull() | F.col("_source").isNull()
        | (F.col("_source") != SOURCE)
        | F.col("_source_file").isNull()
        | F.col("_bronze_ingested_at").isNull()
    )
    if invalid_admin1.limit(1).count() or _has_duplicates(admin1, "iso3", "p_code"):
        raise IngestionValidationError(
            "[silver/administrative_boundaries] invalid or duplicate admin1 key"
        )
    if point_rows.where(F.col("iso3").isNull() | F.col("p_code").isNull()).limit(1).count() \
            or _has_duplicates(point_rows, "iso3", "p_code"):
        raise IngestionValidationError(
            "[silver/administrative_boundaries] invalid or duplicate adminpoints key"
        )
    unmatched_points = point_rows.join(
        admin1.select("iso3", "p_code"), ["iso3", "p_code"], "left_anti"
    )
    if unmatched_points.limit(1).count():
        raise IngestionValidationError(
            "[silver/administrative_boundaries] adminpoints key without admin1 row"
        )

    country_variants = admin1.groupBy("iso3").agg(
        F.countDistinct("adm_0_name").alias("names"),
        F.countDistinct("_adm0_pcode").alias("codes"),
        F.countDistinct("valid_on").alias("dates"),
    ).where((F.col("names") != 1) | (F.col("codes") != 1) | (F.col("dates") != 1))
    if country_variants.limit(1).count():
        raise IngestionValidationError(
            "[silver/administrative_boundaries] inconsistent Admin0 values within country"
        )

    joined = admin1.join(point_rows, ["iso3", "p_code"], "left").withColumn(
        "x_coord", F.coalesce("_point_x", "_center_x")
    ).withColumn("y_coord", F.coalesce("_point_y", "_center_y"))
    invalid_coordinates = joined.where(
        F.col("x_coord").isNull() | F.col("y_coord").isNull()
        | F.isnan("x_coord") | F.isnan("y_coord")
        | ~F.col("x_coord").between(-180, 180)
        | ~F.col("y_coord").between(-90, 90)
    )
    if invalid_coordinates.limit(1).count():
        raise IngestionValidationError(
            "[silver/administrative_boundaries] Admin1 coordinates missing or invalid"
        )

    timestamp = silver_ingested_at or datetime.now(timezone.utc)
    provinces = joined.withColumn(
        "_silver_ingested_at", F.lit(timestamp).cast("timestamp")
    ).select(*SILVER_COLUMNS)
    country_window = Window.partitionBy("iso3").orderBy("p_code")
    countries = (
        admin1.withColumn("_row_number", F.row_number().over(country_window))
        .where(F.col("_row_number") == 1)
        .select(
            "adm_0_name", F.lit(None).cast("string").alias("adm_1_name"),
            "iso3", F.col("_adm0_pcode").alias("p_code"), "valid_on",
            F.lit(None).cast("double").alias("x_coord"),
            F.lit(None).cast("double").alias("y_coord"),
            "_source", "_source_file", "_bronze_ingested_at",
            F.lit(timestamp).cast("timestamp").alias("_silver_ingested_at"),
        )
    )
    result = countries.unionByName(provinces).select(*SILVER_COLUMNS)
    country_count = countries.count()
    return result, {
        "bronze_rows": bronze_rows,
        "admin1_rows": admin1_count,
        "admin1_points": point_count,
        "admin1_center_fallback_rows": joined.where(
            F.col("_point_x").isNull() | F.col("_point_y").isNull()
        ).count(),
        "country_rows": country_count,
        "silver_rows": country_count + admin1_count,
    }


def ingest(spark: SparkSession) -> dict[str, int]:
    """Overwrite the Silver lookup from the current HDX COD-AB Bronze table."""
    source_path = bronze_path(SOURCE)
    if not DeltaTable.isDeltaTable(spark, str(source_path)):
        raise IngestionValidationError(
            f"[silver/administrative_boundaries] missing Bronze Delta: {source_path}"
        )
    bronze = spark.read.format("delta").load(str(source_path))
    cleaned, metrics = transform(bronze)
    target = silver_path(TABLE)
    if target.exists():
        if not DeltaTable.isDeltaTable(spark, str(target)):
            raise IngestionValidationError(
                f"[silver/administrative_boundaries] {target} exists but is not Delta"
            )
        existing = spark.read.format("delta").load(str(target))
        types = {field.name: field.dataType.simpleString() for field in existing.schema}
        if types != EXPECTED_TYPES:
            raise IngestionValidationError(
                "[silver/administrative_boundaries] existing schema is incompatible"
            )
    cleaned.write.format("delta").mode("overwrite").save(str(target))
    return metrics
