"""Silver: bang tin tuc `news_articles` - moi bai bao mot dong.

Bronze `news_rss` ghi MOI lan fetch thanh mot quan sat rieng, nen cung mot bai
lap lai qua nhieu lan fetch va qua nhieu feed (co y, de Silver xu ly). O day:
  - dedup theo `guid` (khoa tu nhien cua Google News); dong cu truoc khi Bronze
    giu guid thi lui ve `link`
  - thoi diem thay bai lan dau / lan cuoi: cot `_fetched_at` cua Bronze; dong cu
    chua co cot nay thi lay run_id trong ten file landing, cuoi cung moi toi
    `_ingested_at` (bi ghi de moi lan dung lai partition, kem co)
  - parse pubDate (RFC-822) thanh timestamp, bo the HTML khoi description
  - nuoc cua bai = nuoc cua feed da tra ve bai + nuoc duoc nhac toi trong
    tieu de/tom tat (tu khoa, transform.reference)
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transform.reference import COUNTRIES, country_pattern

# run_id trong ten file landing: google_news_20260929T131604Z.jsonl
RUN_ID_REGEX = r"google_news_(\d{8}T\d{6}Z)"
RUN_ID_FORMAT = "yyyyMMdd'T'HHmmss'Z'"
# pubDate dang "Mon, 14 Sep 2026 07:00:00 GMT". Bo tien to thu truoc khi parse:
# formatter cua Spark 3 khong can ten thu, va bo di thi khong phu thuoc locale.
PUBDATE_FORMAT = "dd MMM yyyy HH:mm:ss z"
RECENT_DAYS = 7

# Cot Bronze co the thieu o du lieu cu (truoc khi sua Bronze 29/9/2026).
OPTIONAL_COLUMNS = ("guid", "source_url", "feed_country", "_fetched_at")

FLAG_PUBDATE_UNPARSED = "pubdate_unparsed"
FLAG_FETCH_TIME_FALLBACK = "fetch_time_from_ingested_at"


def with_optional_columns(frame: DataFrame) -> DataFrame:
    """Them cac cot tuy chon con thieu (gia tri null) de xu ly dong nhat.

    Args:
        frame: Bang Bronze news_rss.

    Returns:
        DataFrame co du OPTIONAL_COLUMNS.
    """
    for column in OPTIONAL_COLUMNS:
        if column not in frame.columns:
            frame = frame.withColumn(column, F.lit(None).cast("string"))
    return frame


def fetched_at() -> F.Column:
    """Thoi diem fetch cua mot dong Bronze: _fetched_at > run_id ten file > _ingested_at.

    Returns:
        Cot timestamp.
    """
    from_file = F.to_timestamp(
        F.regexp_extract("_source_file", RUN_ID_REGEX, 1), RUN_ID_FORMAT
    )
    return F.coalesce(F.to_timestamp("_fetched_at"), from_file, F.to_timestamp("_ingested_at"))


def parse_pubdate(column: str = "pubDate") -> F.Column:
    """Parse pubDate RFC-822 thanh timestamp UTC.

    Args:
        column: Ten cot pubDate.

    Returns:
        Cot timestamp (null neu khong parse duoc).
    """
    without_weekday = F.regexp_replace(F.col(column), r"^[A-Za-z]{3},\s*", "")
    return F.to_timestamp(without_weekday, PUBDATE_FORMAT)


def strip_html(column: str) -> F.Column:
    """Bo the HTML va thu gon khoang trang.

    Args:
        column: Ten cot chua HTML.

    Returns:
        Cot chuoi thuan van ban.
    """
    no_tags = F.regexp_replace(F.col(column), r"<[^>]+>", " ")
    no_entities = F.regexp_replace(no_tags, r"&nbsp;|&#160;", " ")
    return F.trim(F.regexp_replace(no_entities, r"\s+", " "))


def countries_in(text: F.Column) -> F.Column:
    """Mang ma ISO3 cac nuoc duoc nhac toi trong van ban.

    Args:
        text: Cot van ban.

    Returns:
        Cot array<string>, rong neu khong khop nuoc nao.
    """
    return F.array_compact(
        F.array(
            *[F.when(text.rlike(country_pattern(c)), F.lit(c.iso3)) for c in COUNTRIES]
        )
    )


def build_news_articles(bronze: DataFrame) -> DataFrame:
    """Dung bang Silver news_articles tu MOI partition Bronze news_rss.

    Args:
        bronze: Toan bo bang Bronze news_rss.

    Returns:
        DataFrame moi bai mot dong.
    """
    frame = with_optional_columns(bronze)
    guid = F.when(F.trim("guid") != "", F.col("guid"))
    observed = frame.select(
        F.coalesce(guid, F.col("link")).alias("article_key"),
        "link", "guid", "title", "source", "source_url", "description", "pubDate",
        "feed_country", "_source_file",
        fetched_at().alias("fetched_at"),
        (F.to_timestamp("_fetched_at").isNull()
         & (F.regexp_extract("_source_file", RUN_ID_REGEX, 1) == "")).alias("_fallback"),
    )
    # Noi dung bai lay theo lan fetch MOI NHAT (max_by), vi Google co the sua
    # tieu de/tom tat giua cac lan.
    latest = lambda column: F.max_by(column, "fetched_at")  # noqa: E731
    articles = observed.groupBy("article_key").agg(
        latest("link").alias("link"),
        latest("guid").alias("guid"),
        latest("title").alias("title"),
        latest("source").alias("publisher"),
        latest("source_url").alias("publisher_url"),
        latest("description").alias("description"),
        latest("pubDate").alias("pubDate"),
        F.array_sort(F.collect_set("feed_country")).alias("feed_countries"),
        F.min("fetched_at").alias("first_seen_at"),
        F.max("fetched_at").alias("last_seen_at"),
        F.count("*").alias("seen_count"),
        F.max("_fallback").alias("_any_fallback"),
    )

    published = parse_pubdate()
    description_text = strip_html("description")
    mentioned = countries_in(F.concat_ws(" ", F.col("title"), description_text))
    return articles.select(
        F.sha1("article_key").alias("article_id"),
        "guid",
        "title",
        "link",
        "publisher",
        "publisher_url",
        description_text.alias("description_text"),
        published.alias("published_at"),
        "first_seen_at",
        "last_seen_at",
        "seen_count",
        "feed_countries",
        mentioned.alias("mentioned_countries"),
        F.array_sort(F.array_distinct(F.concat("feed_countries", mentioned))).alias("countries"),
        (published >= F.col("first_seen_at") - F.expr(f"INTERVAL {RECENT_DAYS} DAYS"))
        .alias("is_recent"),
        F.array_compact(
            F.array(
                F.when(published.isNull(), F.lit(FLAG_PUBDATE_UNPARSED)),
                F.when(F.col("_any_fallback"), F.lit(FLAG_FETCH_TIME_FALLBACK)),
            )
        ).alias("dq_flags"),
    )
