"""Bronze news_rss to independent Silver news table."""

from datetime import datetime, timezone

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from ingestion.common import paths
from ingestion.common.validation import IngestionValidationError
from ingestion.silver import news


def row(**changes):
    base = {
        "guid": "article-1", "title": " Dengue &amp; response ",
        "description": '<a href="https://example.org">Case &amp; alert</a>&nbsp;<font>Source</font>',
        "feed_country": "VNM", "feed_gl": "vn", "feed_query": " dengue when:7d ",
        "link": "https://news.google.com/rss/articles/1",
        "pubDate": "Sat, 28 Sep 2024 07:00:00 +0700",
        "source_url": " https://example.org ", "_source": "news_rss",
        "_source_file": "news_rss/2024-09-29/feed.jsonl",
        "_ingested_at": "2024-09-29T08:00:00+00:00",
        "_fetched_at": "2024-09-29T07:00:00+00:00",
        "ingestion_date": "2024-09-29",
    }
    return {**base, **changes}


def frame(spark, rows):
    columns = sorted(news.BRONZE_COLUMNS | {"ingestion_date"})
    schema = StructType([StructField(name, StringType(), True) for name in columns])
    return spark.createDataFrame(rows, schema=schema)


@pytest.mark.integration
def test_clean_parse_and_keep_same_article_across_country_feeds(spark):
    bronze = frame(spark, [
        row(),
        row(title=" Updated title ", _fetched_at="2024-09-29T09:00:00+00:00"),
        row(feed_country="SGP", feed_gl="SG", feed_query="dengue Singapore when:7d",
            pubDate="Sat, 28 Sep 2024 00:00:00 GMT"),
        row(guid=None),
        row(guid="article-2", pubDate="not a date"),
        row(guid="article-3", description="", source_url="bad-url"),
    ])
    cleaned, metrics = news.transform(
        bronze, silver_ingested_at=datetime(2024, 9, 30, tzinfo=timezone.utc)
    )
    assert metrics == {
        "bronze_rows": 6, "invalid_rows": 2,
        "deduplicated_rows": 1, "silver_rows": 3,
    }
    assert cleaned.columns == list(news.SILVER_COLUMNS)
    assert {field.name: field.dataType.simpleString()
            for field in cleaned.schema} == news.EXPECTED_TYPES
    by_feed = {(item.guid, item.iso3): item for item in cleaned.collect()}
    assert set(by_feed) == {("article-1", "VNM"), ("article-1", "SGP"),
                            ("article-3", "VNM")}
    vietnam = by_feed["article-1", "VNM"]
    assert vietnam.title == "Updated title"
    assert vietnam.description == "Case & alert Source"
    assert vietnam.feed_gl == "VN" and vietnam.feed_query == "dengue when:7d"
    utc_pub_date = cleaned.where(
        (F.col("guid") == "article-1") & (F.col("iso3") == "VNM")
    ).select(F.date_format("pubDate", "yyyy-MM-dd HH:mm:ss").alias("utc")).first().utc
    assert utc_pub_date == "2024-09-28 00:00:00"
    assert vietnam.source_url == "https://example.org"
    assert vietnam._source == "news_rss"
    assert vietnam._bronze_ingested_at and vietnam._silver_ingested_at
    assert by_feed["article-3", "VNM"].description is None
    assert by_feed["article-3", "VNM"].source_url is None
    assert bronze.count() == 6  # Bronze remains untouched.


@pytest.mark.integration
def test_missing_schema_and_all_invalid_rows_fail(spark):
    incomplete = frame(spark, [row()]).drop("guid")
    with pytest.raises(IngestionValidationError, match="missing columns"):
        news.transform(incomplete)
    with pytest.raises(IngestionValidationError, match="all Bronze rows failed validation"):
        news.transform(frame(spark, [row(pubDate="bad date")]))


@pytest.mark.integration
def test_ingest_is_idempotent_and_does_not_touch_other_silver_tables(
    spark, tmp_path, monkeypatch
):
    monkeypatch.setattr(paths, "BRONZE_ROOT", tmp_path / "bronze")
    monkeypatch.setattr(paths, "SILVER_ROOT", tmp_path / "silver")
    frame(spark, [row(), row()]).write.format("delta").mode("overwrite").partitionBy(
        "ingestion_date"
    ).save(str(paths.bronze_path("news_rss")))

    assert news.ingest(spark)["silver_rows"] == 1
    assert news.ingest(spark)["silver_rows"] == 1
    saved = spark.read.format("delta").load(str(paths.silver_path("news")))
    assert saved.count() == 1
    assert saved.first().iso3 == "VNM"
    assert not paths.silver_path("dengue_history").exists()
    assert not paths.silver_path("administrative_boundaries").exists()
