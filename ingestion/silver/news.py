"""Clean Google News RSS Bronze into an independent Silver news table.

``iso3`` is the country of the *feed*, not a location inferred from article
text. The same article may legitimately occur in multiple country feeds.
"""

from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from html.parser import HTMLParser
import re

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

from ingestion.common.paths import bronze_path, silver_path
from ingestion.common.validation import IngestionValidationError
from ingestion.silver.schema import clean_text

SOURCE = "news_rss"
TABLE = "news"
BRONZE_COLUMNS = {
    "guid", "title", "description", "feed_country", "feed_gl", "feed_query",
    "link", "pubDate", "source_url", "_source", "_source_file",
    "_ingested_at", "_fetched_at",
}
SILVER_COLUMNS = (
    "guid", "title", "description", "iso3", "feed_gl", "feed_query",
    "link", "pubDate", "source_url", "_source_file", "_source",
    "_bronze_ingested_at", "_silver_ingested_at",
)
EXPECTED_TYPES = dict(zip(SILVER_COLUMNS, (
    "string", "string", "string", "string", "string", "string",
    "string", "timestamp", "string", "string", "string",
    "timestamp", "timestamp",
)))
FEED_KEY = ("guid", "iso3", "feed_gl", "feed_query")


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"br", "p", "div", "li"}:
            self.parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag in {"p", "div", "li"}:
            self.parts.append(" ")


def _collapse(value: str | None) -> str | None:
    if value is None:
        return None
    result = re.sub(r"\s+", " ", value).strip()
    return result or None


def _clean_title(value: str | None) -> str | None:
    return _collapse(unescape(value)) if value is not None else None


def _clean_description(value: str | None) -> str | None:
    if value is None:
        return None
    parser = _TextExtractor()
    parser.feed(value)
    parser.close()
    return _collapse(" ".join(parser.parts))


def _parse_pub_date(value: str | None) -> datetime | None:
    """Parse timezone-aware RFC 822 dates; malformed dates remain null."""
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value.strip())
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except (TypeError, ValueError, OverflowError):
        return None


_TITLE_UDF = F.udf(_clean_title, StringType())
_DESCRIPTION_UDF = F.udf(_clean_description, StringType())
_PUB_DATE_UDF = F.udf(_parse_pub_date, TimestampType())


def transform(
    bronze: DataFrame, *, silver_ingested_at: datetime | None = None
) -> tuple[DataFrame, dict[str, int]]:
    """Normalize RSS text/dates and deduplicate per article and country feed."""
    missing = sorted(BRONZE_COLUMNS - set(bronze.columns))
    if missing:
        raise IngestionValidationError(f"[silver/news] Bronze missing columns: {missing}")
    bronze_rows = bronze.count()
    if not bronze_rows:
        raise IngestionValidationError("[silver/news] empty Bronze table")

    source_url = clean_text("source_url")
    prepared = bronze.select(
        clean_text("guid").alias("guid"),
        _TITLE_UDF(F.col("title")).alias("title"),
        _DESCRIPTION_UDF(F.col("description")).alias("description"),
        F.upper(clean_text("feed_country")).alias("iso3"),
        F.upper(clean_text("feed_gl")).alias("feed_gl"),
        clean_text("feed_query").alias("feed_query"),
        clean_text("link").alias("link"),
        _PUB_DATE_UDF(F.col("pubDate")).alias("pubDate"),
        F.when(source_url.rlike(r"^https?://[^\s]+$"), source_url)
        .alias("source_url"),
        clean_text("_source_file").alias("_source_file"),
        clean_text("_source").alias("_source"),
        F.to_timestamp(clean_text("_ingested_at")).alias("_bronze_ingested_at"),
        F.to_timestamp(clean_text("_fetched_at")).alias("_fetched_at"),
    )
    valid = prepared.where(
        F.col("guid").isNotNull()
        & F.col("title").isNotNull()
        & F.col("iso3").rlike(r"^[A-Z]{3}$")
        & F.col("feed_gl").rlike(r"^[A-Z]{2}$")
        & F.col("feed_query").isNotNull()
        & F.col("link").rlike(r"^https?://[^\s]+$")
        & F.col("pubDate").isNotNull()
        & (F.col("_source") == SOURCE)
        & F.col("_source_file").isNotNull()
        & F.col("_bronze_ingested_at").isNotNull()
        & F.col("_fetched_at").isNotNull()
    )
    valid_rows = valid.count()
    if not valid_rows:
        raise IngestionValidationError("[silver/news] all Bronze rows failed validation")

    # An article in two country feeds is two useful observations. Repeated
    # fetches of the same feed/article are one observation; keep the newest.
    window = Window.partitionBy(*FEED_KEY).orderBy(
        F.col("_fetched_at").desc(),
        F.col("_bronze_ingested_at").desc(),
        F.col("_source_file").asc(),
        F.col("link").asc(),
        F.col("title").asc(),
    )
    timestamp = silver_ingested_at or datetime.now(timezone.utc)
    result = (
        valid.withColumn("_row_number", F.row_number().over(window))
        .where(F.col("_row_number") == 1)
        .withColumn("_silver_ingested_at", F.lit(timestamp).cast("timestamp"))
        .select(*SILVER_COLUMNS)
    )
    silver_rows = result.count()
    return result, {
        "bronze_rows": bronze_rows,
        "invalid_rows": bronze_rows - valid_rows,
        "deduplicated_rows": valid_rows - silver_rows,
        "silver_rows": silver_rows,
    }


def ingest(spark: SparkSession) -> dict[str, int]:
    """Replace only Silver news; never read or write another Silver table."""
    source_path = bronze_path(SOURCE)
    if not DeltaTable.isDeltaTable(spark, str(source_path)):
        raise IngestionValidationError(f"[silver/news] missing Bronze Delta: {source_path}")
    bronze = spark.read.format("delta").load(str(source_path))
    cleaned, metrics = transform(bronze)

    target = silver_path(TABLE)
    if target.exists():
        if not DeltaTable.isDeltaTable(spark, str(target)):
            raise IngestionValidationError(f"[silver/news] {target} exists but is not Delta")
        existing = spark.read.format("delta").load(str(target))
        types = {field.name: field.dataType.simpleString() for field in existing.schema}
        if types != EXPECTED_TYPES:
            raise IngestionValidationError("[silver/news] existing schema is incompatible")
    cleaned.write.format("delta").mode("overwrite").save(str(target))
    return metrics
