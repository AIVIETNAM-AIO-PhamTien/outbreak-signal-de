"""Shared dengue Silver contract and text normalization."""

from pyspark.sql import functions as F

SILVER_COLUMNS = (
    "adm_0_name", "adm_1_name", "iso3", "p_code", "start_date",
    "year", "dengue_total", "s_res", "t_res", "_source",
    "_source_file", "_bronze_ingested_at", "_silver_ingested_at",
)
EXPECTED_TYPES = dict(zip(SILVER_COLUMNS, (
    "string", "string", "string", "string", "date", "int", "bigint",
    "string", "string", "string", "string", "timestamp", "timestamp",
)))


def clean_text(column: str):
    """Trim/collapse whitespace and turn common source placeholders into null."""
    value = F.trim(F.regexp_replace(F.col(column), r"\s+", " "))
    return F.when(
        value.isNull() | (value == "") |
        F.upper(value).isin("NA", "N/A", "NULL", "NONE"),
        F.lit(None).cast("string"),
    ).otherwise(value)


def title_case_words(column: str):
    """Capitalize country words on either side of a hyphen."""
    value = clean_text(column)
    titled = F.concat_ws(
        "-", F.transform(F.split(F.lower(value), "-"), lambda part: F.initcap(part))
    )
    return F.when(value.isNull(), F.lit(None).cast("string")).otherwise(titled)


def compact_title_name(column: str):
    """Store Admin1 labels in title case without whitespace (Hai Phong -> HaiPhong)."""
    source = clean_text(column)
    # Restore word boundaries in already-compact PascalCase names so this
    # normalization remains idempotent when history is passed to the mapper.
    spaced = F.regexp_replace(source, r"(?<=[a-z])(?=[A-Z])", " ")
    titled = F.concat_ws(
        "-", F.transform(F.split(F.lower(spaced), "-"), lambda part: F.initcap(part))
    )
    value = F.when(source.isNull(), F.lit(None).cast("string")).otherwise(titled)
    return F.when(value.isNull(), F.lit(None).cast("string")).otherwise(
        F.regexp_replace(value, r"\s+", "")
    )


def compact_name_key(column: str):
    """Case-insensitive ISO-scoped join key for spaced or compact labels."""
    value = compact_title_name(column)
    return F.when(value.isNull(), F.lit(None).cast("string")).otherwise(
        F.lower(value)
    )
