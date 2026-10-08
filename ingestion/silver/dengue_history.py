"""Build non-overlapping national and OpenDengue Admin1 history from Bronze.

OpenDengue wins over WHO for *any* intersecting reporting interval. Within a
source, finer periods win (weekly, then monthly, then yearly). The result keeps
the original observations and their lineage; case counts are never prorated.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ingestion.common.paths import bronze_path, silver_path
from ingestion.common.validation import IngestionValidationError
from ingestion.silver import administrative_boundaries, opendengue, who_gho
from ingestion.silver.schema import (
    EXPECTED_TYPES, SILVER_COLUMNS, compact_name_key, compact_title_name,
)

HISTORY_TABLE = "dengue_history"
KNOWN_RESOLUTIONS = ("epiweek", "isoweek", "month", "year")
NATIONAL_KEY = ("iso3",)
ADMIN1_KEY = ("iso3", "adm_1_name", "p_code")
CODE_FALLBACK_COUNTRIES = ("THA", "KHM")


def _with_interval(rows: DataFrame) -> DataFrame:
    """Use [start_date, end) so touching periods do not count as overlap."""
    return rows.withColumn(
        "_period_end",
        F.when(F.col("t_res").isin("epiweek", "isoweek"),
               F.date_add("start_date", 7))
        .when(F.col("t_res") == "month", F.add_months("start_date", 1))
        .when(F.col("t_res") == "year", F.add_months("start_date", 12)),
    )


def _without_overlap(
    candidates: DataFrame, blockers: DataFrame, location_key=NATIONAL_KEY
) -> DataFrame:
    """Keep a candidate only if its whole period is disjoint from blockers."""
    candidate = candidates.alias("candidate")
    blocker = blockers.select(*location_key, "start_date", "_period_end").alias("blocker")
    same_location = F.lit(True)
    for name in location_key:
        same_location = same_location & F.col(f"candidate.{name}").eqNullSafe(
            F.col(f"blocker.{name}")
        )
    return candidate.join(
        blocker,
        same_location & (F.col("candidate.start_date") < F.col("blocker._period_end"))
        & (F.col("blocker.start_date") < F.col("candidate._period_end")),
        "left_anti",
    )


def _assert_no_overlap(rows: DataFrame, label: str, location_key=NATIONAL_KEY) -> None:
    previous = Window.partitionBy(*location_key).orderBy(
        "start_date", "_period_end"
    ).rowsBetween(Window.unboundedPreceding, -1)
    collisions = (
        rows.withColumn("_previous_end", F.max("_period_end").over(previous))
        .where(F.col("start_date") < F.col("_previous_end"))
    )
    if collisions.limit(1).count():
        raise IngestionValidationError(f"[silver/dengue_history] overlapping {label} periods")


def _select_opendengue(rows: DataFrame, label: str, location_key) -> DataFrame:
    """Keep the finest available OpenDengue period at each location."""
    week = rows.where(F.col("t_res").isin("epiweek", "isoweek"))
    _assert_no_overlap(week, f"{label} weekly", location_key)
    month = _without_overlap(rows.where(F.col("t_res") == "month"), week, location_key)
    selected = week.unionByName(month)
    year = _without_overlap(rows.where(F.col("t_res") == "year"), selected, location_key)
    selected = selected.unionByName(year)
    _assert_no_overlap(selected, f"{label} selected", location_key)
    return selected


def transform(source: DataFrame) -> tuple[DataFrame, dict[str, int]]:
    """Select non-overlapping periods per national or Admin1 location."""
    types = {field.name: field.dataType.simpleString() for field in source.schema}
    if types != EXPECTED_TYPES:
        raise IngestionValidationError("[silver/dengue_history] incompatible source schema")

    eligible = _with_interval(source.withColumn(
        "adm_1_name", compact_title_name("adm_1_name")
    ).where(
        (F.col("s_res") == "Admin0")
        | ((F.col("s_res") == "Admin1") & (F.col("_source") == "opendengue"))
    ))
    if eligible.where(
        F.col("iso3").isNull() | F.col("start_date").isNull()
        | F.col("_period_end").isNull()
        | ~F.col("t_res").isin(*KNOWN_RESOLUTIONS)
        | ((F.col("s_res") == "Admin1") & F.col("adm_1_name").isNull())
        | ((F.col("t_res") == "month") & (F.dayofmonth("start_date") != 1))
        | ((F.col("t_res") == "year")
           & ((F.dayofmonth("start_date") != 1) | (F.month("start_date") != 1)))
    ).limit(1).count():
        raise IngestionValidationError("[silver/dengue_history] invalid reporting period")

    national = eligible.where(F.col("s_res") == "Admin0")
    od = national.where(F.col("_source") == "opendengue")
    who = national.where(F.col("_source") == "who_gho")
    od_admin1 = eligible.where(F.col("s_res") == "Admin1")
    od_rows, who_rows, admin1_rows = od.count(), who.count(), od_admin1.count()
    if not od_rows or not who_rows:
        raise IngestionValidationError("[silver/dengue_history] both sources are required")

    # National and province periods are different spatial grains. They may
    # coexist on the same date; overlaps are checked within each grain only.
    od_selected = _select_opendengue(od, "OpenDengue national", NATIONAL_KEY)
    admin1_selected = _select_opendengue(od_admin1, "OpenDengue Admin1", ADMIN1_KEY)

    # WHO is allowed only where *no original OpenDengue interval* intersects.
    # ISO and epi weeks may overlap on their shared Sunday; ISO wins that tie.
    who_iso = _without_overlap(who.where(F.col("t_res") == "isoweek"), od)
    _assert_no_overlap(who_iso, "WHO ISO weekly")
    who_epi = _without_overlap(
        _without_overlap(who.where(F.col("t_res") == "epiweek"), od), who_iso
    )
    _assert_no_overlap(who_epi, "WHO epi weekly")
    who_selected = who_iso.unionByName(who_epi)
    who_month = _without_overlap(
        _without_overlap(who.where(F.col("t_res") == "month"), od), who_selected
    )
    who_selected = who_selected.unionByName(who_month)
    who_year = _without_overlap(
        _without_overlap(who.where(F.col("t_res") == "year"), od), who_selected
    )
    who_selected = who_selected.unionByName(who_year)
    _assert_no_overlap(who_selected, "WHO selected")

    national_history = od_selected.unionByName(who_selected)
    _assert_no_overlap(national_history, "national final")
    history = national_history.unionByName(admin1_selected)
    od_kept, who_kept, admin1_kept = (
        od_selected.count(), who_selected.count(), admin1_selected.count()
    )
    return history.select(*SILVER_COLUMNS), {
        "opendengue_national_rows": od_rows,
        "opendengue_kept": od_kept,
        "opendengue_overlapping_grains_skipped": od_rows - od_kept,
        "opendengue_admin1_rows": admin1_rows,
        "opendengue_admin1_kept": admin1_kept,
        "opendengue_admin1_overlapping_grains_skipped": admin1_rows - admin1_kept,
        "who_national_rows": who_rows,
        "who_kept": who_kept,
        "who_overlapping_periods_skipped": who_rows - who_kept,
        "history_rows": od_kept + admin1_kept + who_kept,
    }


def validate_country_iso(
    source: DataFrame, boundaries: DataFrame, label: str
) -> dict[str, int]:
    """Check source ISO3/country-name pairs against HDX without dropping cases.

    Countries absent from HDX remain in the dengue series and cannot receive a
    COD P-code. A conflicting name for a *known* ISO3 is a validation error.
    """
    countries = boundaries.where(F.col("adm_1_name").isNull()).select(
        "iso3", compact_name_key("adm_0_name").alias("_hdx_country_key")
    )
    if countries.groupBy("iso3").count().where("count > 1").limit(1).count():
        raise IngestionValidationError("[silver/dengue_history] duplicate HDX country ISO3")
    checked = source.withColumn(
        "_source_country_key", compact_name_key("adm_0_name")
    ).join(F.broadcast(countries), "iso3", "left")
    if checked.where(
        F.col("_hdx_country_key").isNotNull()
        & (F.col("_source_country_key") != F.col("_hdx_country_key"))
    ).limit(1).count():
        raise IngestionValidationError(
            f"[silver/dengue_history] {label} country name conflicts with HDX ISO3"
        )
    counts = checked.agg(
        F.sum(F.col("_hdx_country_key").isNotNull().cast("int")).alias("known"),
        F.sum(F.col("_hdx_country_key").isNull().cast("int")).alias("unknown"),
    ).first()
    return {"hdx_iso3_matched_rows": counts.known, "hdx_iso3_absent_rows": counts.unknown}


def map_p_codes(history: DataFrame, boundaries: DataFrame) -> tuple[DataFrame, dict[str, int]]:
    """Map only HDX-confirmed names or country-verified numeric RNE patterns.

    OD `TH-10` -> COD `TH10` and `KH-1` -> `KH01` are validated against the
    HDX lookup. Other RNE formats are not interchangeable with COD P-codes.
    Historical Vietnamese Admin1 is intentionally left unmapped because the
    available HDX province snapshot uses new boundaries.
    """
    types = {field.name: field.dataType.simpleString() for field in boundaries.schema}
    if types != administrative_boundaries.EXPECTED_TYPES:
        raise IngestionValidationError(
            "[silver/dengue_history] incompatible administrative_boundaries schema"
        )
    lookup = boundaries.select(
        F.col("iso3"),
        compact_name_key("adm_1_name").alias("name_key"),
        F.col("p_code").alias("cod_p_code"),
    )
    if lookup.where(F.col("iso3").isNull() | F.col("cod_p_code").isNull()).limit(1).count():
        raise IngestionValidationError(
            "[silver/dengue_history] administrative_boundaries has null location key/code"
        )
    duplicates = lookup.groupBy("iso3", "name_key").count().where(F.col("count") > 1)
    if duplicates.limit(1).count():
        raise IngestionValidationError(
            "[silver/dengue_history] ambiguous administrative_boundaries compact name"
        )
    duplicate_codes = lookup.groupBy("iso3", "cod_p_code").count().where(F.col("count") > 1)
    if duplicate_codes.limit(1).count():
        raise IngestionValidationError(
            "[silver/dengue_history] ambiguous administrative_boundaries P-code"
        )

    provinces = lookup.where(F.col("name_key").isNotNull())
    source = history.withColumn(
        "adm_1_name", compact_title_name("adm_1_name")
    ).withColumn("_name_key", compact_name_key("adm_1_name")).alias("source")
    by_name = F.broadcast(lookup).alias("by_name")
    by_code = F.broadcast(provinces.select("iso3", "cod_p_code")).alias("by_code")
    code_candidate = F.when(
        (F.col("source.s_res") == "Admin1")
        & (F.col("source._source") == "opendengue")
        & F.col("source.iso3").isin(*CODE_FALLBACK_COUNTRIES)
        & F.col("source.p_code").rlike(r"^(TH|KH)-[0-9]{1,2}$"),
        F.concat(
            F.substring(F.col("source.p_code"), 1, 2),
            F.lpad(F.regexp_extract(F.col("source.p_code"), r"^[A-Z]{2}-([0-9]{1,2})$", 1), 2, "0"),
        ),
    )
    joined = (source.join(
        by_name,
        (F.col("source.iso3") == F.col("by_name.iso3"))
        & F.col("source._name_key").eqNullSafe(F.col("by_name.name_key"))
        & ((F.col("source.s_res") == "Admin0")
           | ((F.col("source.s_res") == "Admin1") & (F.col("source.iso3") != "VNM"))),
        "left",
    ).join(
        by_code,
        (F.col("source.iso3") == F.col("by_code.iso3"))
        & (code_candidate == F.col("by_code.cod_p_code")),
        "left",
    ))
    conflicts = joined.where(
        (F.col("by_name.cod_p_code").isNotNull()
         & F.col("by_code.cod_p_code").isNotNull()
         & (F.col("by_name.cod_p_code") != F.col("by_code.cod_p_code")))
    )
    if conflicts.limit(1).count():
        raise IngestionValidationError("[silver/dengue_history] conflicting COD P-code matches")

    mapped_code = F.coalesce(F.col("by_name.cod_p_code"), F.col("by_code.cod_p_code"))
    method = (F.when(F.col("by_name.cod_p_code").isNotNull(), "compact_name")
              .when(F.col("by_code.cod_p_code").isNotNull(), "source_code")
              .otherwise("unmatched"))
    prepared = joined.select(*(
        mapped_code.alias("p_code") if name == "p_code"
        else F.col(f"source.{name}") for name in SILVER_COLUMNS
    ), method.alias("_p_code_method"))
    counts = prepared.agg(
        F.sum((F.col("s_res") == "Admin1").cast("int")).alias("admin1_rows"),
        F.sum(((F.col("s_res") == "Admin1") & F.col("p_code").isNotNull())
              .cast("int")).alias("admin1_mapped"),
        F.sum(((F.col("s_res") == "Admin0") & F.col("p_code").isNotNull())
              .cast("int")).alias("admin0_mapped"),
        F.sum(((F.col("s_res") == "Admin1") & (F.col("_p_code_method") == "compact_name"))
              .cast("int")).alias("compact_name"),
        F.sum((F.col("_p_code_method") == "source_code").cast("int")).alias("source_code"),
        F.sum(((F.col("s_res") == "Admin1") & (F.col("iso3") == "VNM"))
              .cast("int")).alias("vnm_admin1_skipped"),
    ).first()
    return prepared.select(*SILVER_COLUMNS), {
        "admin1_p_codes_mapped": counts.admin1_mapped,
        "admin1_p_codes_unmatched": counts.admin1_rows - counts.admin1_mapped,
        "admin0_p_codes_mapped": counts.admin0_mapped,
        "admin1_p_codes_compact_name": counts.compact_name,
        "admin1_p_codes_source_code": counts.source_code,
        "admin1_vnm_historical_unmapped": counts.vnm_admin1_skipped,
    }


def ingest(spark: SparkSession) -> dict[str, int | str]:
    """Build history from Bronze, using the prebuilt COD-AB Silver lookup."""
    boundary_path = silver_path(administrative_boundaries.TABLE)
    if not DeltaTable.isDeltaTable(spark, str(boundary_path)):
        raise IngestionValidationError(
            f"[silver/dengue_history] missing Silver Delta: {boundary_path}; "
            "ingest administrative_boundaries first"
        )
    boundaries = spark.read.format("delta").load(str(boundary_path))
    od_path, who_path = bronze_path("opendengue"), bronze_path("who_gho")
    for source_path in (od_path, who_path):
        if not DeltaTable.isDeltaTable(spark, str(source_path)):
            raise IngestionValidationError(
                f"[silver/dengue_history] missing Bronze Delta: {source_path}"
            )

    od_bronze = spark.read.format("delta").load(str(od_path))
    who_bronze = spark.read.format("delta").load(str(who_path))
    for name, rows, required in (
        ("opendengue", od_bronze, opendengue.BRONZE_COLUMNS),
        ("who_gho", who_bronze, who_gho.BRONZE_COLUMNS),
    ):
        missing = sorted(required - set(rows.columns))
        if missing:
            raise IngestionValidationError(
                f"[silver/dengue_history] {name} Bronze missing columns: {missing}"
            )

    # Prune the wide Bronze schemas before building cleaning and joining plans.
    od_bronze = od_bronze.select(*sorted(opendengue.BRONZE_COLUMNS))
    who_bronze = who_bronze.select(*sorted(who_gho.BRONZE_COLUMNS))
    release = opendengue.latest_release(od_bronze)
    snapshot = who_gho.latest_snapshot(who_bronze)
    od_clean, od_metrics = opendengue.transform(
        od_bronze.where(F.col("release") == release)
    )
    who_clean, who_metrics = who_gho.transform(
        who_bronze.where(F.col("ingestion_date") == snapshot)
    )

    # Truncate the large cleaning plans before repeatedly joining intervals.
    # localCheckpoint is temporary Spark storage, not a Silver Delta table.
    od_clean = od_clean.localCheckpoint(eager=True)
    who_clean = who_clean.localCheckpoint(eager=True)
    try:
        od_iso_metrics = validate_country_iso(od_clean, boundaries, "OpenDengue")
        who_iso_metrics = validate_country_iso(who_clean, boundaries, "WHO")
        history, metrics = transform(od_clean.unionByName(who_clean))
        history, p_code_metrics = map_p_codes(history, boundaries)
        target = silver_path(HISTORY_TABLE)
        if target.exists() and not DeltaTable.isDeltaTable(spark, str(target)):
            raise IngestionValidationError(f"[silver/dengue_history] {target} is not Delta")
        history.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            str(target)
        )
    finally:
        od_clean.unpersist()
        who_clean.unpersist()

    return {
        "opendengue_release": release,
        "who_snapshot_date": snapshot,
        **{f"opendengue_clean_{key}": value for key, value in od_metrics.items()},
        **{f"who_clean_{key}": value for key, value in who_metrics.items()},
        **{f"opendengue_{key}": value for key, value in od_iso_metrics.items()},
        **{f"who_{key}": value for key, value in who_iso_metrics.items()},
        **metrics,
        **p_code_metrics,
    }
