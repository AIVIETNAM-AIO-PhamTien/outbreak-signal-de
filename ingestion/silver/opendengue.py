"""Clean the latest OpenDengue Bronze release for dengue Silver tables.

The requested Silver schema has no adm_2_name. Admin2 observations therefore
cannot be represented without losing their location and are deliberately left
in Bronze. ``transform`` feeds dengue_history directly; ``ingest`` is the
legacy writer for the source-level dengue_unified table.
"""

from __future__ import annotations

from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ingestion.common.paths import bronze_path, silver_path
from ingestion.common.validation import IngestionValidationError
from ingestion.opendengue import parse_release
from ingestion.silver.schema import EXPECTED_TYPES, SILVER_COLUMNS
from ingestion.silver.schema import clean_text as _text
from ingestion.silver.schema import compact_title_name, title_case_words

SOURCE = "opendengue"
TABLE = "dengue_unified"

BRONZE_COLUMNS = {
    "adm_0_name", "adm_1_name", "ISO_A0", "RNE_iso_code",
    "calendar_start_date", "Year", "dengue_total", "S_res", "T_res",
    "case_definition_standardised", "_source", "_source_file",
    "_ingested_at", "release",
}
GRAIN = (
    "adm_0_name", "adm_1_name", "iso3", "p_code", "start_date",
    "year", "s_res", "t_res",
)


def latest_release(bronze: DataFrame) -> str:
    """Use numeric release order (V1.10 newer than V1.9)."""
    if "release" not in bronze.columns:
        raise IngestionValidationError("[silver/opendengue] Bronze missing release")
    releases = [row[0] for row in bronze.select("release").distinct().collect()]
    valid = [value for value in releases if value and parse_release(value) is not None]
    if not valid:
        raise IngestionValidationError("[silver/opendengue] no valid Bronze release")
    return max(valid, key=parse_release)


def transform(bronze: DataFrame, *, silver_ingested_at: datetime | None = None
              ) -> tuple[DataFrame, dict[str, int]]:
    """Normalize types, reject bad rows and select one case series per grain.

    Duplicate case definitions are ranked Total > Suspected and confirmed >
    Confirmed. Conflicting case counts within the same definition/grain fail
    instead of silently choosing one. Exact duplicates are kept once.
    """
    missing = sorted(BRONZE_COLUMNS - set(bronze.columns))
    if missing:
        raise IngestionValidationError(f"[silver/opendengue] missing columns: {missing}")

    source_rows = bronze.count()
    scoped = bronze.where(F.upper(F.trim(F.col("S_res"))).isin("ADMIN0", "ADMIN1"))
    scope_rows = scoped.count()
    if scope_rows == 0:
        raise IngestionValidationError("[silver/opendengue] no Admin0/Admin1 rows")

    cases_text = _text("dengue_total")
    year_text = _text("Year")
    resolution = F.lower(_text("T_res"))
    normalized = scoped.select(
        title_case_words("adm_0_name").alias("adm_0_name"),
        compact_title_name("adm_1_name").alias("adm_1_name"),
        F.upper(_text("ISO_A0")).alias("iso3"),
        # Name requested by Silver schema; values are still raw RNE/ISO codes,
        # not verified COD-AB P-codes.
        _text("RNE_iso_code").alias("p_code"),
        F.to_date(_text("calendar_start_date"), "yyyy-MM-dd").alias("start_date"),
        F.when(year_text.rlike(r"^[0-9]{4}$"), year_text.cast("int")).alias("year"),
        F.when(cases_text.rlike(r"^[0-9]+$"), cases_text.cast("long")).alias("dengue_total"),
        F.when(F.upper(_text("S_res")) == "ADMIN0", F.lit("Admin0"))
        .otherwise(F.lit("Admin1")).alias("s_res"),
        # OpenDengue Week records use Sunday-Saturday periods. Keep their
        # original dates/counts; only label this source's weekly type epiweek.
        F.when(resolution == "week", F.lit("epiweek"))
        .otherwise(resolution).alias("t_res"),
        _text("_source").alias("_source"),
        _text("_source_file").alias("_source_file"),
        F.to_timestamp(_text("_ingested_at")).alias("_bronze_ingested_at"),
        F.lower(_text("case_definition_standardised")).alias("_case_definition"),
    )
    valid = normalized.where(
        F.col("adm_0_name").isNotNull()
        & F.col("iso3").rlike(r"^[A-Z]{3}$")
        & F.col("start_date").isNotNull()
        & (F.col("start_date") <= F.current_date())
        & F.col("year").isNotNull()
        & F.col("dengue_total").isNotNull()
        & F.col("t_res").isin("epiweek", "month", "year")
        & ((F.col("s_res") == "Admin0") | F.col("adm_1_name").isNotNull())
        & (F.col("_source") == SOURCE)
        & F.col("_source_file").isNotNull()
        & F.col("_bronze_ingested_at").isNotNull()
    )
    valid_rows = valid.count()
    if valid_rows == 0:
        raise IngestionValidationError("[silver/opendengue] all scoped rows failed validation")

    ranked = valid.withColumn(
        "_definition_priority",
        F.when(F.col("_case_definition") == "total", 0)
        .when(F.col("_case_definition") == "suspected and confirmed", 1)
        .when(F.col("_case_definition") == "confirmed", 2)
        .otherwise(3),
    )
    conflicts = (
        ranked.groupBy(*GRAIN, "_definition_priority")
        .agg(F.countDistinct("dengue_total").alias("case_variants"))
        .where(F.col("case_variants") > 1)
    )
    if conflicts.limit(1).count():
        raise IngestionValidationError(
            "[silver/opendengue] conflicting dengue_total within one grain and "
            "case definition; no arbitrary dedup performed"
        )

    window = Window.partitionBy(*GRAIN).orderBy(
        F.col("_definition_priority").asc(),
        F.col("_bronze_ingested_at").desc(),
        F.col("_source_file").asc(),
    )
    timestamp = silver_ingested_at or datetime.now(timezone.utc)
    result = (
        ranked.withColumn("_row_number", F.row_number().over(window))
        .where(F.col("_row_number") == 1)
        .withColumn("_silver_ingested_at", F.lit(timestamp).cast("timestamp"))
        .select(*SILVER_COLUMNS)
    )
    output_rows = result.count()
    metrics = {
        "bronze_rows": source_rows,
        "excluded_admin2_or_unknown": source_rows - scope_rows,
        "invalid_rows": scope_rows - valid_rows,
        "deduplicated_rows": valid_rows - output_rows,
        "silver_rows": output_rows,
    }
    return result, metrics


def _validate_target(spark: SparkSession, target) -> bool:
    """Return True only for the old single-source schema needing migration."""
    if not target.exists():
        return False
    if not DeltaTable.isDeltaTable(spark, str(target)):
        raise IngestionValidationError(f"[silver/opendengue] {target} exists but is not Delta")
    partitions = DeltaTable.forPath(spark, str(target)).detail().first()["partitionColumns"]
    existing = spark.read.format("delta").load(str(target))
    existing_types = {field.name: field.dataType.simpleString() for field in existing.schema}
    if list(partitions) != ["_source"]:
        raise IngestionValidationError(
            "[silver/opendengue] existing dengue_unified partition is incompatible"
        )
    if existing_types == EXPECTED_TYPES:
        return False
    if existing_types == LEGACY_TYPES:
        other_sources = existing.where(
            F.col("_source").isNull() | (F.col("_source") != SOURCE)
        ).limit(1).count()
        if other_sources:
            raise IngestionValidationError(
                "[silver/opendengue] cannot auto-rename legacy schema with other sources; "
                "migrate them to p_code first"
            )
        return True
    else:
        raise IngestionValidationError(
            "[silver/opendengue] existing dengue_unified schema/partition is incompatible"
        )


LEGACY_TYPES = {("rne_iso_code" if name == "p_code" else name): kind
                for name, kind in EXPECTED_TYPES.items()}


def ingest(spark: SparkSession) -> dict[str, int | str]:
    """Read latest Bronze release and replace only the OpenDengue Silver partition."""
    source_path = bronze_path(SOURCE)
    if not DeltaTable.isDeltaTable(spark, str(source_path)):
        raise IngestionValidationError(f"[silver/opendengue] missing Bronze Delta: {source_path}")
    bronze = spark.read.format("delta").load(str(source_path))
    release = latest_release(bronze)
    cleaned, metrics = transform(bronze.where(F.col("release") == release))

    target = silver_path(TABLE)
    legacy = _validate_target(spark, target)
    writer = cleaned.write.format("delta").mode("overwrite").partitionBy("_source")
    if legacy:
        # One-time, transactional schema migration. No other source is present;
        # old Delta versions remain available through time travel.
        writer.option("overwriteSchema", "true").save(str(target))
    else:
        writer.option("replaceWhere", "_source = 'opendengue'").save(str(target))
    return {"release": release, **metrics}
