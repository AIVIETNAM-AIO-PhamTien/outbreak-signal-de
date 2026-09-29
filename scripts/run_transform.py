"""Dung tang Silver + Gold tu Bronze va xuat bao cao chat luong du lieu.

    python scripts/run_transform.py                 # silver + gold
    python scripts/run_transform.py --layer silver  # chi silver
    python scripts/run_transform.py --layer gold    # chi gold (doc silver da co)

Moi lan chay ghi de toan bo bang Silver/Gold (idempotent) va de lai
data/quality/quality_<run_id>.json + .md. Exit code 1 neu co kiem tra ERROR.
"""

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from ingestion.common.config import source_config  # noqa: E402
from ingestion.common.logging import get_logger  # noqa: E402
from ingestion.common.paths import run_id, utc_now  # noqa: E402
from ingestion.common.spark_session import build_spark_session  # noqa: E402
from ingestion.opendengue import RELEASE_COLUMN, parse_release  # noqa: E402
from transform import checks  # noqa: E402
from transform.boundaries import FEATURE_SCHEMA, ensure_boundaries, feature_rows, load_boundaries  # noqa: E402
from transform.common import (  # noqa: E402
    QualityReport,
    gold_path,
    latest_partition,
    read_bronze_all,
    read_delta,
    silver_path,
    small_frame,
    write_table,
)
from transform.gold import dims, facts, risk, units  # noqa: E402
from transform.silver.cases import build_dengue_cases  # noqa: E402
from transform.silver import admin, province  # noqa: E402
from transform.silver.news import build_news_articles  # noqa: E402

log = get_logger("transform")


def run_silver(spark: SparkSession, report: QualityReport, today: date) -> None:
    """Kiem tra Bronze roi dung 2 bang Silver.

    Args:
        spark: SparkSession.
        report: Bao cao chat luong.
        today: Ngay hien tai (UTC).
    """
    bronze_all = {s: read_bronze_all(spark, s) for s in ("opendengue", "news_rss", "who_gho")}
    latest = {}
    for source, frame in bronze_all.items():
        checks.check_bronze_contract(report, source, frame)
        # OpenDengue phan vung theo ban phat hanh: lay release co version lon
        # nhat (so theo so, "V1.10" > "V1.2"), khong theo ngay chay.
        column = RELEASE_COLUMN if source == "opendengue" else "ingestion_date"
        if source == "opendengue":
            releases = [r[0] for r in frame.select(column).distinct().collect()]
            value = max(releases, key=lambda r: parse_release(r) or ())
        else:
            value = latest_partition(frame)
        latest[source] = frame.where(F.col(column) == value)
        checks.check_bronze_partitions(report, source, frame, value, column)
        log.info("Bronze %s: partition moi nhat %s=%s", source, column, value)

    checks.check_bronze_opendengue(report, latest["opendengue"])
    checks.check_bronze_news(report, bronze_all["news_rss"])
    checks.check_bronze_who(report, latest["who_gho"], source_config("who_gho")["top"])

    cases, dropped = build_dengue_cases(latest["opendengue"], latest["who_gho"], today)
    cases = cases.cache()
    checks.check_silver_cases(report, cases, dropped)
    log.info("silver.dengue_cases: %s dong", f"{write_table(cases, silver_path('dengue_cases')):,}")

    news = build_news_articles(bronze_all["news_rss"]).cache()
    checks.check_silver_news(report, news)
    log.info("silver.news_articles: %s dong",
             f"{write_table(news, silver_path('news_articles')):,}")

    # ---- cap tinh ----
    ne = small_frame(spark, feature_rows(load_boundaries(ensure_boundaries())), FEATURE_SCHEMA)
    admin_units = admin.build_admin_units(read_bronze_all(spark, "hdx_cod_ab"), ne)
    write_table(admin_units, silver_path("admin_units"))
    admin_units = read_delta(spark, silver_path("admin_units")).cache()
    crosswalk = admin.build_crosswalk(admin_units, ne, read_bronze_all(spark, "hdx_cod_ab_geometry"))
    write_table(crosswalk, silver_path("admin_crosswalk"))
    crosswalk = read_delta(spark, silver_path("admin_crosswalk")).cache()
    population = province.build_unit_population(read_bronze_all(spark, "hdx_cod_ps"), admin_units)
    write_table(population, silver_path("unit_population"))
    population = read_delta(spark, silver_path("unit_population"))
    checks.check_silver_admin(report, admin_units, crosswalk, population)

    province_cases = province.build_province_cases(
        cases, read_bronze_all(spark, "trends_th_weekly"), read_bronze_all(spark, "ph_doh"),
        admin_units, crosswalk)
    log.info("silver.province_cases: %s dong",
             f"{write_table(province_cases, silver_path('province_cases')):,}")
    checks.check_silver_province_cases(report, read_delta(spark, silver_path("province_cases")))

    mentions = province.build_news_unit_mentions(
        read_delta(spark, silver_path("news_articles")), admin_units, crosswalk)
    log.info("silver.news_unit_mentions: %s dong",
             f"{write_table(mentions, silver_path('news_unit_mentions')):,}")
    checks.check_silver_news_mentions(report, news, read_delta(spark, silver_path("news_unit_mentions")))


def run_gold(spark: SparkSession, report: QualityReport, today: date) -> None:
    """Dung star schema + mart tu Silver da ghi.

    Args:
        spark: SparkSession.
        report: Bao cao chat luong.
        today: Ngay hien tai (UTC).
    """
    cases = read_delta(spark, silver_path("dengue_cases")).cache()
    news = read_delta(spark, silver_path("news_articles")).cache()

    bounds = cases.agg(F.min("period_start")).first()[0]
    news_min = news.agg(F.min(F.to_date("published_at"))).first()[0]
    start = min(d for d in (bounds, news_min) if d is not None)

    tables = {}
    tables["dim_country"] = dims.build_dim_country(spark, cases)
    tables["dim_date"] = dims.build_dim_date(spark, date(start.year, 1, 1), today)
    # Ghi dim truoc roi doc lai: fact noi vao dim DA GHI, khoa luon khop nhau.
    for name in ("dim_country", "dim_date"):
        write_table(tables[name], gold_path(name))
        tables[name] = read_delta(spark, gold_path(name)).cache()

    tables["fact_monthly_cases"] = facts.build_fact_monthly_cases(cases, tables["dim_country"])
    tables["fact_daily_news"] = facts.build_fact_daily_news(news, tables["dim_country"])
    for name in ("fact_monthly_cases", "fact_daily_news"):
        write_table(tables[name], gold_path(name))
        tables[name] = read_delta(spark, gold_path(name)).cache()

    tables["country_risk"] = risk.build_country_risk(
        tables["fact_monthly_cases"], tables["fact_daily_news"], tables["dim_country"],
        tables["dim_date"], today)
    tables["news_feed"] = risk.build_news_feed(news)
    for name in ("country_risk", "news_feed"):
        write_table(tables[name], gold_path(name))
        tables[name] = read_delta(spark, gold_path(name))

    # ---- cap tinh ----
    admin_units = read_delta(spark, silver_path("admin_units")).cache()
    crosswalk = read_delta(spark, silver_path("admin_crosswalk")).cache()
    current = province.current_units(admin_units, crosswalk).cache()
    tables["dim_admin_unit"] = units.build_dim_admin_unit(
        admin_units, read_delta(spark, silver_path("unit_population")), current,
        tables["dim_country"])
    write_table(tables["dim_admin_unit"], gold_path("dim_admin_unit"))
    tables["dim_admin_unit"] = read_delta(spark, gold_path("dim_admin_unit")).cache()
    tables["bridge_map_polygon"] = units.build_bridge_map_polygon(
        crosswalk, current, tables["dim_admin_unit"])
    tables["fact_unit_monthly_cases"] = units.build_fact_unit_monthly_cases(
        read_delta(spark, silver_path("province_cases")), tables["dim_admin_unit"])
    tables["fact_unit_daily_news"] = units.build_fact_unit_daily_news(
        read_delta(spark, silver_path("news_unit_mentions")), tables["dim_admin_unit"])
    for name in ("bridge_map_polygon", "fact_unit_monthly_cases", "fact_unit_daily_news"):
        write_table(tables[name], gold_path(name))
        tables[name] = read_delta(spark, gold_path(name)).cache()
    tables["unit_risk"] = units.build_unit_risk(
        tables["fact_unit_monthly_cases"], tables["fact_unit_daily_news"],
        tables["dim_admin_unit"], tables["dim_date"], today)
    write_table(tables["unit_risk"], gold_path("unit_risk"))
    tables["unit_risk"] = read_delta(spark, gold_path("unit_risk"))

    for name, frame in tables.items():
        log.info("gold.%s: %s dong", name, f"{frame.count():,}")
    checks.check_gold(report, tables)


def main() -> int:
    """Diem vao CLI.

    Returns:
        0 neu khong co kiem tra ERROR, 1 neu co.
    """
    parser = argparse.ArgumentParser(description="Dung Silver + Gold cho OutbreakSignal DE")
    parser.add_argument("--layer", choices=("silver", "gold", "all"), default="all")
    args = parser.parse_args()

    current_run = run_id()
    today = utc_now().date()
    report = QualityReport(current_run)
    spark = build_spark_session("outbreak_signal_transform")
    try:
        if args.layer in ("silver", "all"):
            run_silver(spark, report, today)
        if args.layer in ("gold", "all"):
            run_gold(spark, report, today)
    finally:
        json_path, md_path = report.write()
        spark.stop()

    log.info("Bao cao chat luong: %s", md_path)
    log.info("Ket qua kiem tra: %s", report.counts())
    return 1 if report.has_errors else 0


if __name__ == "__main__":
    sys.exit(main())
