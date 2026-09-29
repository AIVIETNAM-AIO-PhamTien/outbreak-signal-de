"""Gold cap tinh (star schema): don vi hanh chinh hien hanh la dimension.

    dim_admin_unit           don vi HIEN HANH (VN: 34 tinh moi), dan so, toa do tam
    bridge_map_polygon       polygon Natural Earth -> don vi hien hanh (VN: 63 polygon
                             tinh cu to theo tinh moi chua no) - de ve ban do
    fact_unit_monthly_cases  don vi x thang x nguon
    fact_unit_daily_news     don vi x ngay dang: so bai nhac ten don vi
    unit_risk                mart: moi don vi 1 dong - so ca / 100.000 dan thang moi
                             nhat, so voi cung thang cac nam truoc, tin tuc 30 ngay

Khong cong trung: cung mot nguon, cung mot don vi goc (unit_id), cung ky - co the
co nhieu dong do OpenDengue viet ten mot tinh nhieu kieu (DI YOGYAKARTA / DAERAH
ISTIMEWA YOGYAKARTA) -> lay GIA TRI LON NHAT. Sau do moi CONG cac don vi goc KHAC
NHAU vao don vi hien hanh (63 tinh cu -> 34 tinh moi: cac tinh cu roi nhau, cong la dung).
"""

from datetime import date, timedelta

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from transform.gold.dims import date_key
from transform.gold.facts import monthly_series, usable_cases
from transform.gold.risk import (
    LEVEL_NO_DATA,
    NEWS_WINDOW_DAYS,
    _baseline_stats,
    _z_score,
    risk_level,
)

CASE_SOURCE_PRIORITY = ("trends_th", "ph_doh", "opendengue")
RECENT_CASE_MONTHS = 24  # so ca moi hon moc nay moi coi la tin hieu "hien tai"
# So ca cu hon RECENT_CASE_MONTHS khong xep muc nguy co: vd Ninh Binh 12/2010 co 4 ca
# tung bi xep "cao" (baseline cac nam truoc = 0) - dung ve so hoc nhung vo nghia hom nay.
LEVEL_STALE = "số liệu cũ"


def build_dim_admin_unit(admin_units: DataFrame, population: DataFrame,
                         current: DataFrame, dim_country: DataFrame) -> DataFrame:
    """dim_admin_unit: moi don vi HIEN HANH mot dong.

    Dan so cua tinh moi VN = tong dan so cac tinh cu gop vao (COD-PS VN con 63 tinh).

    Args:
        admin_units: Silver admin_units.
        population: Silver unit_population.
        current: (unit_id, current_unit_id) - tu province.current_units.
        dim_country: Gold dim_country.

    Returns:
        DataFrame dim_admin_unit.
    """
    pop = (
        population.join(current, "unit_id")
        .groupBy("current_unit_id")
        .agg(F.sum("population").alias("population"),
             F.max("reference_year").alias("population_year"),
             F.count("*").alias("population_units"))
    )
    units = admin_units.where(F.col("valid_to").isNull())
    window = Window.orderBy("iso3", "unit_id")
    return (
        units.join(pop.withColumnRenamed("current_unit_id", "unit_id"), "unit_id", "left")
        .join(dim_country.select("country_key", F.col("iso3")), "iso3", "left")
        .select(
            F.row_number().over(window).alias("unit_key"),
            "country_key", "iso3", "unit_id", "unit_system", "admin_level", "unit_name",
            "local_name", "region_name", "x", "y", "valid_from", "population",
            "population_year",
        )
    )


def build_bridge_map_polygon(crosswalk: DataFrame, current: DataFrame,
                             dim_unit: DataFrame) -> DataFrame:
    """Polygon Natural Earth (ma ISO) -> unit_key hien hanh, de to mau ban do.

    Args:
        crosswalk: Silver admin_crosswalk.
        current: (unit_id, current_unit_id).
        dim_unit: Gold dim_admin_unit.

    Returns:
        DataFrame (iso_3166_2, unit_key).
    """
    cod = crosswalk.where(F.col("method") == "cod_centroid_in_ne").select(
        "iso_3166_2", F.col("pcode").alias("current_unit_id"))
    old_vn = crosswalk.where(F.col("method") == "ne_centroid_in_cod").select(
        "iso_3166_2", F.col("pcode").alias("current_unit_id"))
    pairs = cod.unionByName(old_vn)
    # Mot polygon co the chua tam nhieu don vi COD (vd quan cua Metro Manila) ->
    # to theo don vi co ma nho nhat, tat dinh.
    window = Window.partitionBy("iso_3166_2").orderBy("current_unit_id")
    return (
        pairs.withColumn("_r", F.row_number().over(window)).where("_r = 1")
        .join(dim_unit.select("unit_key", F.col("unit_id").alias("current_unit_id")),
              "current_unit_id")
        .select("iso_3166_2", "unit_key")
    )


def build_fact_unit_monthly_cases(province_cases: DataFrame, dim_unit: DataFrame) -> DataFrame:
    """fact_unit_monthly_cases: don vi hien hanh x thang x nguon.

    Args:
        province_cases: Silver province_cases.
        dim_unit: Gold dim_admin_unit.

    Returns:
        DataFrame fact.
    """
    rows = usable_cases(province_cases.where(F.col("unit_id").isNotNull()))
    period = ["source", "unit_id", "current_unit_id", "period_type", "week_system",
              "case_definition", "period_start", "period_end"]
    # Bien the ten cua cung don vi goc -> max; khong cong.
    collapsed = rows.groupBy(*period).agg(
        F.max("cases").alias("cases"), F.max("deaths").alias("deaths"))
    monthly = monthly_series(collapsed, ["current_unit_id"])
    return monthly.join(
        dim_unit.select("unit_key", F.col("unit_id").alias("current_unit_id")), "current_unit_id"
    ).select(
        "unit_key", date_key(F.col("month_start")).alias("month_date_key"), "source",
        "cases", "deaths", "series_used", "periods_in_month", "is_complete",
    )


def build_fact_unit_daily_news(mentions: DataFrame, dim_unit: DataFrame) -> DataFrame:
    """fact_unit_daily_news: don vi hien hanh x ngay dang.

    Args:
        mentions: Silver news_unit_mentions.
        dim_unit: Gold dim_admin_unit.

    Returns:
        DataFrame fact.
    """
    return (
        mentions.where(F.col("published_at").isNotNull())
        .groupBy("current_unit_id", F.to_date("published_at").alias("d"))
        .agg(F.countDistinct("article_id").alias("article_count"))
        .join(dim_unit.select("unit_key", F.col("unit_id").alias("current_unit_id")),
              "current_unit_id")
        .select("unit_key", date_key(F.col("d")).alias("date_key"), "article_count")
    )


def build_unit_risk(fact_monthly: DataFrame, fact_news: DataFrame, dim_unit: DataFrame,
                    dim_date: DataFrame, today: date) -> DataFrame:
    """Mart unit_risk: moi don vi hien hanh mot dong.

    Thang danh gia: thang hoan chinh moi nhat cua don vi (nguon uu tien:
    TRENDS > DOH > OpenDengue). Baseline: cung thang toi da 5 nam truoc, cung nguon.
    `signal`: "ca bệnh" neu so ca moi trong RECENT_CASE_MONTHS thang, nguoc lai
    "tin tức" (VN/KHM/LAO: so ca cap tinh chi toi 2010).

    Args:
        fact_monthly: fact_unit_monthly_cases.
        fact_news: fact_unit_daily_news.
        dim_unit: dim_admin_unit.
        dim_date: dim_date.
        today: Ngay hien tai.

    Returns:
        DataFrame unit_risk.
    """
    months = dim_date.select(F.col("date_key").alias("month_date_key"), "year", "month",
                             F.col("date").alias("month_start"))
    series = fact_monthly.where("is_complete").join(months, "month_date_key")
    rank = F.lit(len(CASE_SOURCE_PRIORITY))
    for index, source in reversed(list(enumerate(CASE_SOURCE_PRIORITY))):
        rank = F.when(F.col("source") == source, F.lit(index)).otherwise(rank)
    latest = (
        series.groupBy("unit_key", "source").agg(F.max("month_start").alias("month_start"))
        .withColumn("_rank", F.row_number().over(
            Window.partitionBy("unit_key").orderBy(F.col("month_start").desc(), rank)))
        .where("_rank = 1").drop("_rank")
    )
    current = series.join(latest, ["unit_key", "source", "month_start"])
    scored = _baseline_stats(current, series, ["unit_key", "source"]).withColumn(
        "z_score", _z_score())

    start_30 = today - timedelta(days=NEWS_WINDOW_DAYS)
    start_60 = today - timedelta(days=2 * NEWS_WINDOW_DAYS)
    news = fact_news.join(dim_date.select("date_key", "date"), "date_key").where(
        F.col("date") >= F.lit(start_60)
    ).groupBy("unit_key").agg(
        F.sum(F.when(F.col("date") >= F.lit(start_30), F.col("article_count")).otherwise(0))
        .alias("news_30d"),
        F.sum(F.when(F.col("date") < F.lit(start_30), F.col("article_count")).otherwise(0))
        .alias("news_prev_30d"),
    )
    recent_cut = F.add_months(F.lit(today), -RECENT_CASE_MONTHS)
    n_years = F.coalesce("n_baseline_years", F.lit(0))
    return (
        dim_unit.select("unit_key", "country_key", "iso3", "unit_id", "unit_name",
                        "local_name", "population")
        .join(scored.drop("country_key"), "unit_key", "left")
        .join(news, "unit_key", "left")
        .select(
            "unit_key", "country_key", "iso3", "unit_id", "unit_name", "local_name",
            "population", "source", F.col("month_start").alias("data_as_of"), "cases",
            F.round(F.col("cases") / F.col("population") * 100_000, 2).alias("cases_per_100k"),
            F.round("baseline_mean", 1).alias("baseline_mean"),
            n_years.alias("n_baseline_years"),
            F.round("z_score", 2).alias("z_score"),
            F.when(F.col("cases").isNull(), LEVEL_NO_DATA)
            .when(F.col("month_start") < recent_cut, LEVEL_STALE)
            .otherwise(risk_level(F.col("z_score"), n_years)).alias("case_level"),
            F.coalesce("news_30d", F.lit(0)).alias("news_30d"),
            F.coalesce("news_prev_30d", F.lit(0)).alias("news_prev_30d"),
            F.when(F.col("month_start") >= recent_cut, F.lit("ca bệnh"))
            .otherwise(F.lit("tin tức")).alias("signal"),
        )
    )
