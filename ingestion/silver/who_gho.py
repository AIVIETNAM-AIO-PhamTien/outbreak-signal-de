"""Clean the latest WHO GHO Bronze snapshot for dengue Silver tables.

WHO observations are national only. ``transform`` feeds dengue_history directly;
``ingest`` is the legacy source-level dengue_unified writer.
"""

from __future__ import annotations

from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ingestion.common.paths import bronze_path, silver_path
from ingestion.common.validation import IngestionValidationError
from ingestion.silver.schema import EXPECTED_TYPES, SILVER_COLUMNS, clean_text, title_case_words

SOURCE = "who_gho"
TABLE = "dengue_unified"
BRONZE_COLUMNS = {
    "COUNTRY", "ISO3", "YEAR", "DATE_TYPE", "DATE_NUM", "START_DATE",
    "CASES", "_source", "_source_file", "_ingested_at", "ingestion_date",
}
SOURCE_KEY = ("iso3", "year", "t_res", "_date_num")


def latest_snapshot(bronze: DataFrame) -> str:
    """A daily WHO partition is a complete snapshot, not an increment."""
    if "ingestion_date" not in bronze.columns:
        raise IngestionValidationError("[silver/who_gho] Bronze missing ingestion_date")
    latest = bronze.agg(F.max("ingestion_date")).first()[0]
    if not latest:
        raise IngestionValidationError("[silver/who_gho] Bronze has no snapshot")
    return latest


def transform(bronze: DataFrame, *, silver_ingested_at: datetime | None = None
              ) -> tuple[DataFrame, dict[str, int]]:
    """Fill missing monthly dates, reject future/invalid rows, and deduplicate."""
    missing = sorted(BRONZE_COLUMNS - set(bronze.columns))
    if missing:
        raise IngestionValidationError(f"[silver/who_gho] missing columns: {missing}")

    source_rows = bronze.count()
    if source_rows == 0:
        raise IngestionValidationError("[silver/who_gho] empty Bronze snapshot")

    year_text = clean_text("YEAR")
    number_text = clean_text("DATE_NUM")
    cases_text = clean_text("CASES")
    date_text = clean_text("START_DATE")
    year = F.when(year_text.rlike(r"^[0-9]{4}$"), year_text.cast("int"))
    date_num = F.when(number_text.rlike(r"^[0-9]{1,2}$"), number_text.cast("int"))
    resolution = F.lower(clean_text("DATE_TYPE"))
    # All 36 missing START_DATE values in the current snapshot are month rows.
    # Do not invent weekly dates: ISO and epi numbering can differ at year-end.
    derived_month = F.when(
        date_text.isNull() & (resolution == "month") & date_num.between(1, 12),
        F.make_date(year, date_num, F.lit(1)),
    )
    start_date = F.when(date_text.isNull(), derived_month).otherwise(
        F.to_date(date_text, "yyyy-MM-dd")
    )
    prepared = bronze.select(
        title_case_words("COUNTRY").alias("adm_0_name"),
        F.lit(None).cast("string").alias("adm_1_name"),
        F.upper(clean_text("ISO3")).alias("iso3"),
        F.lit(None).cast("string").alias("p_code"),
        start_date.alias("start_date"),
        year.alias("year"),
        F.when(cases_text.rlike(r"^[0-9]+$"), cases_text.cast("long"))
        .alias("dengue_total"),
        F.lit("Admin0").alias("s_res"),
        resolution.alias("t_res"),
        clean_text("_source").alias("_source"),
        clean_text("_source_file").alias("_source_file"),
        F.to_timestamp(clean_text("_ingested_at")).alias("_bronze_ingested_at"),
        date_num.alias("_date_num"),
        (date_text.isNull() & derived_month.isNotNull()).alias("_start_derived"),
    ).localCheckpoint(eager=True)
    future_rows = prepared.where(F.col("start_date") > F.current_date()).count()
    valid = prepared.where(
        F.col("adm_0_name").isNotNull()
        & F.col("iso3").rlike(r"^[A-Z]{3}$")
        & F.col("year").isNotNull()
        & F.col("_date_num").isNotNull()
        & (
            ((F.col("t_res") == "month") & F.col("_date_num").between(1, 12))
            | (F.col("t_res").isin("isoweek", "epiweek")
               & F.col("_date_num").between(1, 53))
        )
        & F.col("start_date").isNotNull()
        & (F.col("start_date") <= F.current_date())
        & F.col("dengue_total").isNotNull()
        & (F.col("_source") == SOURCE)
        & F.col("_source_file").isNotNull()
        & F.col("_bronze_ingested_at").isNotNull()
    )
    valid_rows = valid.count()
    if valid_rows == 0:
        raise IngestionValidationError("[silver/who_gho] all Bronze rows failed validation")

    conflicts = (
        valid.groupBy(*SOURCE_KEY)
        .agg(F.countDistinct("dengue_total").alias("case_variants"),
             F.countDistinct("start_date").alias("date_variants"))
        .where((F.col("case_variants") > 1) | (F.col("date_variants") > 1))
    )
    if conflicts.limit(1).count():
        raise IngestionValidationError(
            "[silver/who_gho] conflicting CASES/START_DATE for one "
            "(ISO3, YEAR, DATE_TYPE, DATE_NUM) key"
        )

    window = Window.partitionBy(*SOURCE_KEY).orderBy(
        F.col("_bronze_ingested_at").desc(), F.col("_source_file").asc()
    )
    timestamp = silver_ingested_at or datetime.now(timezone.utc)
    result = (
        valid.withColumn("_row_number", F.row_number().over(window))
        .where(F.col("_row_number") == 1)
        .withColumn("_silver_ingested_at", F.lit(timestamp).cast("timestamp"))
        .select(*SILVER_COLUMNS)
    )
    output_rows = result.count()
    return result, {
        "bronze_rows": source_rows,
        "derived_start_date_rows": valid.where(F.col("_start_derived")).count(),
        "future_rows_skipped": future_rows,
        "invalid_rows": source_rows - future_rows - valid_rows,
        "deduplicated_rows": valid_rows - output_rows,
        "silver_rows": output_rows,
    }


def ingest(spark: SparkSession) -> dict[str, int | str]:
    """Replace only the who_gho partition in Silver dengue_unified."""
    source_path = bronze_path(SOURCE)
    if not DeltaTable.isDeltaTable(spark, str(source_path)):
        raise IngestionValidationError(f"[silver/who_gho] missing Bronze Delta: {source_path}")
    bronze = spark.read.format("delta").load(str(source_path))
    missing = sorted(BRONZE_COLUMNS - set(bronze.columns))
    if missing:
        raise IngestionValidationError(f"[silver/who_gho] missing columns: {missing}")
    # WHO Bronze contains many extra API fields; prune them before Spark builds
    # the cleaning plan to keep generated code small.
    bronze = bronze.select(*sorted(BRONZE_COLUMNS))
    snapshot_date = latest_snapshot(bronze)
    cleaned, metrics = transform(bronze.where(F.col("ingestion_date") == snapshot_date))

    target = silver_path(TABLE)
    if target.exists():
        if not DeltaTable.isDeltaTable(spark, str(target)):
            raise IngestionValidationError(f"[silver/who_gho] {target} exists but is not Delta")
        partitions = DeltaTable.forPath(spark, str(target)).detail().first()["partitionColumns"]
        existing = spark.read.format("delta").load(str(target))
        types = {field.name: field.dataType.simpleString() for field in existing.schema}
        if list(partitions) != ["_source"] or types != EXPECTED_TYPES:
            raise IngestionValidationError(
                "[silver/who_gho] existing dengue_unified schema/partition is incompatible"
            )

    (
        cleaned.write.format("delta").mode("overwrite")
        .partitionBy("_source")
        .option("replaceWhere", "_source = 'who_gho'")
        .save(str(target))
    )
    return {"bronze_snapshot_date": snapshot_date, **metrics}
