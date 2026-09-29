"""Gold - cac bang fact cua star schema.

    fact_monthly_cases           nuoc x thang x nguon (cap quoc gia)
    fact_daily_news              nuoc x ngay dang

Van de chinh cua so ca: cung mot nuoc/thang co the co NHIEU chuoi song song
(bao theo thang VA theo tuan; tuan ISO VA tuan dich te; nhieu dinh nghia ca).
Cong het se dem trung. Quy tac: moi (nguon, dia diem, thang) chon DUNG MOT chuoi
theo thu tu uu tien, roi moi cong:
    1. loai ky: month > week          (year bi bo - qua tho cho chuoi thang)
    2. he tuan: isoweek > epiweek > khong ro
    3. dinh nghia ca: Total > Suspected and confirmed > Confirmed > khac
Tuan duoc gan vao thang theo ngay bat dau tuan.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from transform.gold.dims import UNKNOWN_COUNTRY_KEY, date_key
from transform.reference import UNKNOWN_ISO3
from transform.silver.cases import FLAG_FUTURE, FLAG_IRREGULAR, FLAG_PERIOD_MISSING

EXCLUDED_FLAGS = (FLAG_FUTURE, FLAG_IRREGULAR, FLAG_PERIOD_MISSING)
MIN_WEEKS_COMPLETE = 4


def _rank(column: str, order: list[str]) -> F.Column:
    """Diem uu tien cua mot gia tri (nho hon = uu tien hon).

    Args:
        column: Ten cot.
        order: Cac gia tri theo thu tu uu tien.

    Returns:
        Cot int; gia tri khong co trong danh sach xep cuoi.
    """
    expr = F.lit(len(order))
    for index, value in reversed(list(enumerate(order))):
        expr = F.when(F.col(column) == value, F.lit(index)).otherwise(expr)
    return expr


def usable_cases(silver_cases: DataFrame) -> DataFrame:
    """Loc dong du dieu kien vao fact: co so ca, ky hop le, khong o tuong lai.

    Args:
        silver_cases: Silver dengue_cases.

    Returns:
        DataFrame con.
    """
    has_bad_flag = F.exists("dq_flags", lambda f: f.isin(*EXCLUDED_FLAGS))
    return silver_cases.where(
        F.col("cases").isNotNull()
        & F.col("period_type").isin("month", "week")
        & ~has_bad_flag
    )


def monthly_series(cases: DataFrame, location_cols: list[str]) -> DataFrame:
    """Gom so ca theo thang sau khi chon DUNG MOT chuoi cho moi thang.

    Args:
        cases: Dong da qua usable_cases().
        location_cols: Cot xac dinh dia diem (vd ["country_iso3"]).

    Returns:
        DataFrame: location_cols + source, month_start, cases, deaths,
        series_used, periods_in_month, is_complete.
    """
    group = [*location_cols, "source", "month_start"]
    ranked = (
        cases.withColumn("month_start", F.trunc("period_start", "month"))
        .withColumn(
            "_priority",
            F.struct(
                _rank("period_type", ["month", "week"]).alias("p"),
                _rank("week_system", ["isoweek", "epiweek"]).alias("w"),
                _rank("case_definition", ["Total", "Suspected and confirmed", "Confirmed"])
                .alias("c"),
            ),
        )
        .withColumn("_best", F.min("_priority").over(Window.partitionBy(*group)))
        .where(F.col("_priority") == F.col("_best"))
    )
    series = F.concat_ws(
        "/", "period_type", F.coalesce("week_system", F.lit("-")), "case_definition"
    )
    return ranked.groupBy(*group).agg(
        F.sum("cases").alias("cases"),
        F.sum("deaths").alias("deaths"),
        F.first(series).alias("series_used"),
        F.count("*").alias("periods_in_month"),
        F.first("period_type").alias("_ptype"),
    ).withColumn(
        "is_complete",
        (F.col("_ptype") == "month") | (F.col("periods_in_month") >= MIN_WEEKS_COMPLETE),
    ).drop("_ptype")


def build_fact_monthly_cases(silver_cases: DataFrame, dim_country: DataFrame) -> DataFrame:
    """fact_monthly_cases: grain nuoc x thang x nguon, chi cap quoc gia.

    Args:
        silver_cases: Silver dengue_cases.
        dim_country: dim_country.

    Returns:
        DataFrame fact.
    """
    national = usable_cases(silver_cases).where(F.col("adm_level") == 0)
    monthly = monthly_series(national, ["country_iso3"])
    return monthly.join(
        dim_country.select("country_key", F.col("iso3").alias("country_iso3")),
        "country_iso3",
        "left",
    ).select(
        "country_key",
        date_key(F.col("month_start")).alias("month_date_key"),
        "source",
        "cases",
        "deaths",
        "series_used",
        "periods_in_month",
        "is_complete",
    )


def build_fact_daily_news(silver_news: DataFrame, dim_country: DataFrame) -> DataFrame:
    """fact_daily_news: grain nuoc x ngay dang.

    Bai nhac nhieu nuoc duoc dem cho MOI nuoc; bai khong gan duoc nuoc nao
    dem cho UNK. Bai khong parse duoc ngay dang bi bo (co kiem tra rieng).

    Args:
        silver_news: Silver news_articles.
        dim_country: dim_country.

    Returns:
        DataFrame fact.
    """
    exploded = silver_news.where(F.col("published_at").isNotNull()).select(
        F.explode(
            F.when(F.size("countries") > 0, F.col("countries")).otherwise(
                F.array(F.lit(UNKNOWN_ISO3))
            )
        ).alias("iso3"),
        F.to_date("published_at").alias("published_date"),
        "is_recent",
    )
    daily = exploded.groupBy("iso3", "published_date").agg(
        F.count("*").alias("article_count"),
        F.sum(F.col("is_recent").cast("int")).alias("recent_article_count"),
    )
    return daily.join(dim_country.select("country_key", "iso3"), "iso3", "left").select(
        F.coalesce("country_key", F.lit(UNKNOWN_COUNTRY_KEY)).alias("country_key"),
        date_key(F.col("published_date")).alias("date_key"),
        "article_count",
        "recent_article_count",
    )
