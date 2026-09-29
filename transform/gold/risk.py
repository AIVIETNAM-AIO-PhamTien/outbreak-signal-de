"""Gold - cac bang mart cho app HealthMap.

    country_risk          moi nuoc 1 dong: thang moi nhat so voi baseline mua vu
                          + tin hieu tin tuc 30 ngay
    news_feed             danh sach bai bao cho app

Baseline mua vu = trung binh / do lech chuan so ca CUNG THANG LICH trong toi da
5 nam truoc. So sanh cung thang de loai yeu to mua (sot xuat huyet luon tang
vao mua mua) - chi con lai phan "cao bat thuong so voi mua".

Lich su dung CHUOI HOP NHAT hai nguon: moi (nuoc, nam, thang) lay WHO neu co,
khong thi OpenDengue. Ly do (do tren du lieu that 29/9/2026): WHO chi co tu 2024
cho 6/9 nuoc, nhung o 162 thang ca hai nguon cung co so lieu, do lech trung vi
la 0% va 140/162 thang lech <= 20% - hai nguon du nhat quan de noi chuoi.

Day la thong ke mo ta don gian, khong phai mo hinh du bao.
"""

from datetime import date, timedelta

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

BASELINE_YEARS = 5
MIN_BASELINE_YEARS = 3
INCOMPLETE_RATIO = 0.5
NEWS_WINDOW_DAYS = 30

LEVEL_HIGH = "cao"
LEVEL_MEDIUM = "trung bình"
LEVEL_LOW = "thấp"
LEVEL_NOT_ENOUGH = "không đủ dữ liệu"
LEVEL_NO_DATA = "không có dữ liệu"
SOURCE_PRIORITY = ("who_gho", "opendengue")


def risk_level(z_score: F.Column, n_years: F.Column) -> F.Column:
    """Quy z-score ra muc nguy co.

    Do lech chuan baseline = 0 (cac nam truoc bang nhau) thi z-score null: khi
    do so thang so ca voi trung binh - vuot trung binh la cao, khong thi thap.

    Args:
        z_score: Cot z-score (null neu do lech chuan = 0).
        n_years: So nam baseline.

    Returns:
        Cot chuoi muc nguy co.
    """
    return (
        F.when(n_years < MIN_BASELINE_YEARS, LEVEL_NOT_ENOUGH)
        .when(z_score.isNull() & (F.col("cases") > F.col("baseline_mean")), LEVEL_HIGH)
        .when(z_score >= 2, LEVEL_HIGH)
        .when(z_score >= 1, LEVEL_MEDIUM)
        .otherwise(LEVEL_LOW)
    )


def _source_rank() -> F.Column:
    """Diem uu tien nguon (nho hon = uu tien hon) theo SOURCE_PRIORITY."""
    rank = F.lit(len(SOURCE_PRIORITY))
    for index, source in reversed(list(enumerate(SOURCE_PRIORITY))):
        rank = F.when(F.col("source") == source, F.lit(index)).otherwise(rank)
    return rank


def _baseline_stats(current: DataFrame, history: DataFrame, keys: list[str]) -> DataFrame:
    """Tinh baseline cung thang (cua toi da BASELINE_YEARS nam truoc) cho moi dong.

    Args:
        current: Dong can danh gia; co keys + year, month, cases.
        history: Chuoi lich su; co keys + year, month, cases, source.

    Returns:
        `current` kem baseline_mean, baseline_std, n_baseline_years, baseline_sources.
    """
    hist = history.select(
        *keys, "month",
        F.col("year").alias("h_year"), F.col("cases").alias("h_cases"),
        F.col("source").alias("h_source"),
    )
    joined = (
        current.select(*keys, "year", "month")
        .join(hist, [*keys, "month"])
        .where(
            (F.col("h_year") < F.col("year"))
            & (F.col("h_year") >= F.col("year") - BASELINE_YEARS)
        )
    )
    stats = joined.groupBy(*keys, "year", "month").agg(
        F.avg("h_cases").alias("baseline_mean"),
        F.stddev_samp("h_cases").alias("baseline_std"),
        F.count("h_year").alias("n_baseline_years"),
        F.concat_ws("+", F.array_sort(F.collect_set("h_source"))).alias("baseline_sources"),
    )
    return current.join(stats, [*keys, "year", "month"], "left")


def _z_score() -> F.Column:
    """z = (ca - trung binh) / do lech chuan; null neu do lech chuan = 0 hoac null."""
    return F.when(
        F.col("baseline_std") > 0,
        (F.col("cases") - F.col("baseline_mean")) / F.col("baseline_std"),
    )


def unified_history(series: DataFrame, keys: list[str]) -> DataFrame:
    """Chuoi lich su hop nhat: moi (keys, nam, thang) giu 1 nguon theo uu tien.

    Args:
        series: Thang hoan chinh cua moi nguon; co keys + source, year, month, cases.
        keys: Cot dia diem.

    Returns:
        DataFrame cung cot, moi (keys, year, month) mot dong.
    """
    window = Window.partitionBy(*keys, "year", "month").orderBy(_source_rank())
    return (
        series.withColumn("_rank", F.row_number().over(window))
        .where("_rank = 1")
        .drop("_rank")
    )


def build_country_risk(
    fact_monthly: DataFrame,
    fact_news: DataFrame,
    dim_country: DataFrame,
    dim_date: DataFrame,
    today: date,
) -> DataFrame:
    """Mart country_risk: 1 dong moi nuoc (tru UNK).

    Thang danh gia: thang hoan chinh moi nhat cua nguon uu tien nhat ma nuoc
    co (WHO, khong co thi OpenDengue). Baseline: chuoi lich su hop nhat.

    Args:
        fact_monthly: fact_monthly_cases.
        fact_news: fact_daily_news.
        dim_country: dim_country.
        dim_date: dim_date.
        today: Ngay tinh cua so tin tuc 30 ngay.

    Returns:
        DataFrame country_risk.
    """
    months = dim_date.select(
        F.col("date_key").alias("month_date_key"), "year", "month",
        F.col("date").alias("month_start"),
    )
    series = fact_monthly.where("is_complete").join(months, "month_date_key")

    # Do tre bao cao: thang moi nhat thuong chua nhan du so lieu (VD Indonesia
    # 29/9/2026: thang 6 = 9.264, thang 7 = 3.514, thang 8 = 67). Thang giam
    # > 50% so voi thang truoc bi coi la chua du; danh gia thang on dinh gan nhat.
    by_month = Window.partitionBy("country_key", "source").orderBy("month_start")
    flagged = series.withColumn(
        "possibly_incomplete",
        F.coalesce(F.col("cases") < INCOMPLETE_RATIO * F.lag("cases").over(by_month), F.lit(False)),
    )
    reported = flagged.groupBy("country_key", "source").agg(
        F.max("month_start").alias("latest_reported_month")
    )
    latest = (
        flagged.where(~F.col("possibly_incomplete"))
        .groupBy("country_key", "source")
        .agg(F.max("month_start").alias("month_start"))
        .join(reported, ["country_key", "source"])
        .withColumn(
            "skipped_recent_months",
            F.months_between("latest_reported_month", "month_start").cast("int"),
        )
        .withColumn("_rank", F.row_number().over(
            Window.partitionBy("country_key").orderBy(_source_rank())))
        .where("_rank = 1")
        .drop("_rank")
    )
    current = series.join(latest, ["country_key", "source", "month_start"])
    history = unified_history(series, ["country_key"])
    scored = _baseline_stats(current, history, ["country_key"]).withColumn(
        "z_score", _z_score()
    )

    start_30 = today - timedelta(days=NEWS_WINDOW_DAYS)
    start_60 = today - timedelta(days=2 * NEWS_WINDOW_DAYS)
    news_days = fact_news.join(dim_date.select("date_key", "date"), "date_key").where(
        F.col("date") >= F.lit(start_60)
    )
    news = news_days.groupBy("country_key").agg(
        F.sum(F.when(F.col("date") >= F.lit(start_30), F.col("article_count")).otherwise(0))
        .alias("news_30d"),
        F.sum(F.when(F.col("date") < F.lit(start_30), F.col("article_count")).otherwise(0))
        .alias("news_prev_30d"),
    )

    countries = dim_country.where(F.col("iso3") != "UNK").select(
        "country_key", "iso3", "country_name"
    )
    n_years = F.coalesce("n_baseline_years", F.lit(0))
    return (
        countries.join(scored, "country_key", "left")
        .join(news, "country_key", "left")
        .select(
            "country_key",
            "iso3",
            "country_name",
            "source",
            F.col("month_start").alias("data_as_of"),
            "latest_reported_month",
            F.coalesce("skipped_recent_months", F.lit(0)).alias("skipped_recent_months"),
            "cases",
            F.round("baseline_mean", 1).alias("baseline_mean"),
            F.round("baseline_std", 1).alias("baseline_std"),
            n_years.alias("n_baseline_years"),
            "baseline_sources",
            F.round("z_score", 2).alias("z_score"),
            F.round(F.col("cases") / F.col("baseline_mean"), 2).alias("ratio_to_baseline"),
            F.when(F.col("cases").isNull(), LEVEL_NO_DATA)
            .otherwise(risk_level(F.col("z_score"), n_years))
            .alias("risk_level"),
            F.coalesce("news_30d", F.lit(0)).alias("news_30d"),
            F.coalesce("news_prev_30d", F.lit(0)).alias("news_prev_30d"),
        )
    )


def build_news_feed(silver_news: DataFrame) -> DataFrame:
    """Mart news_feed: danh sach bai bao cho app (moi bai 1 dong, moi nhat truoc).

    Args:
        silver_news: Silver news_articles.

    Returns:
        DataFrame news_feed.
    """
    return silver_news.select(
        "article_id", "title", "link", "publisher", "published_at",
        "first_seen_at", "is_recent", "countries",
    ).orderBy(F.col("published_at").desc_nulls_last())
