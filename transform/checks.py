"""Kiem tra chat luong du lieu cho ca 3 tang - trong tam cua viec "test Bronze".

Moi ham check_* nhan DataFrame + QualityReport, them cac Check vao bao cao.
Muc do (severity) chi co y nghia khi check that bai (failed > 0):
    ERROR  du lieu sai den muc ket qua Gold khong dang tin - batch exit 1
    WARN   van de that cua Bronze / nguon, nen sua hoac ghi ro
    INFO   so do de biet (do phu, ti le khop...), khong phai loi

Schema hop dong cua Bronze lay dung theo data dictionary da kiem chung
29/9/2026 (docs/handout-silver-layer.md, muc 4).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ingestion.common.paths import PARTITION_COLUMN
from transform.common import SEVERITY_ERROR, SEVERITY_INFO, SEVERITY_WARN, QualityReport
from transform.reference import BY_ISO3, BY_OPENDENGUE_NAME
from transform.silver import cases as silver_cases
from transform.silver import news as silver_news

LINEAGE = {c: "string" for c in (
    "_source", "_ingested_at", "_source_file", "_fetched_at", PARTITION_COLUMN)}

BRONZE_CONTRACT: dict[str, dict[str, str]] = {
    "opendengue": {
        **{c: "string" for c in (
            "adm_0_name", "adm_1_name", "adm_2_name", "full_name", "ISO_A0",
            "FAO_GAUL_code", "RNE_iso_code", "IBGE_code", "calendar_start_date",
            "calendar_end_date", "Year", "dengue_total", "case_definition_standardised",
            "S_res", "T_res", "UUID", "release", "_file_sha",
        )},
        **LINEAGE,
    },
    "news_rss": {
        **{c: "string" for c in (
            "title", "link", "guid", "pubDate", "source", "source_url", "description",
            "raw_payload", "feed_country", "feed_hl", "feed_gl", "feed_query",
        )},
        **LINEAGE,
    },
    "who_gho": {
        # Moi cot la chuoi (primitivesAsString) - Bronze khong ep kieu.
        **{c: "string" for c in (
            "COUNTRY", "ISO3", "WHO_REGION", "YEAR", "DATE_TYPE", "DATE_NUM", "START_DATE",
            "CASES", "CONFIRMED_CASES", "DEATHS", "SEVERE_CASES", "SERO_1", "SERO_2",
            "SERO_3", "SERO_4", "POPULATION",
        )},
        **LINEAGE,
    },
}

SAMPLE_LIMIT = 5


def _sample(frame: DataFrame, columns: list[str], limit: int = SAMPLE_LIMIT) -> str:
    """Lay vai dong vi du de dua vao chi tiet bao cao.

    Args:
        frame: Cac dong vi pham.
        columns: Cot can hien.
        limit: So dong toi da.

    Returns:
        Chuoi ngan, vi du "(MYS, 2029, 2029-06-03); (...)".
    """
    rows = frame.select(*columns).limit(limit).collect()
    return "; ".join("(" + ", ".join(str(v) for v in row) + ")" for row in rows)


def check_bronze_contract(
    report: QualityReport, source: str, frame: DataFrame
) -> None:
    """So schema Bronze voi hop dong: thieu cot = ERROR, lech kieu / cot la = WARN.

    Args:
        report: Bao cao.
        source: Ten bang Bronze.
        frame: Bang Bronze da doc.
    """
    expected = BRONZE_CONTRACT[source]
    actual = dict(frame.dtypes)
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    drift = sorted(c for c in expected if c in actual and actual[c] != expected[c])
    report.add("bronze", source, "schema_missing_columns", SEVERITY_ERROR, len(missing),
               len(expected), f"thieu: {missing}" if missing else "du cot theo data dictionary")
    report.add("bronze", source, "schema_type_drift", SEVERITY_WARN, len(drift), len(expected),
               "; ".join(f"{c}: {expected[c]} -> {actual[c]}" for c in drift))
    report.add("bronze", source, "schema_unexpected_columns", SEVERITY_WARN, len(extra),
               len(actual), f"cot moi: {extra}" if extra else "")


def check_bronze_partitions(
    report: QualityReport, source: str, frame: DataFrame, latest: str,
    partition_column: str = PARTITION_COLUMN,
) -> None:
    """Kiem tra partition: partition moi nhat khong rong, snapshot co bi lap y het.

    OpenDengue / WHO ghi lai snapshot day du moi ngay. Neu partition moi nhat
    giong het partition truoc do (cung so dong, khong khac dong nao) thi day la
    ban sao thua - chi ton dung luong (van de 1.1 trong plan sua Bronze).

    Args:
        report: Bao cao.
        source: Ten bang.
        frame: Toan bo bang Bronze.
        latest: Gia tri partition moi nhat.
        partition_column: Cot phan vung (OpenDengue: release).
    """
    per_day = {r[0]: r[1] for r in frame.groupBy(partition_column).count().collect()}
    report.add("bronze", source, "latest_partition_not_empty", SEVERITY_ERROR,
               int(per_day.get(latest, 0) == 0), per_day.get(latest, 0),
               f"partition {latest}, {len(per_day)} partition tong cong")
    # news: moi ngay la quan sat moi. OpenDengue: partition theo release, moi
    # release la du lieu khac nhau theo dinh nghia - khong co "snapshot lap".
    if source in ("news_rss", "opendengue"):
        return

    days = sorted(per_day)
    if len(days) < 2:
        report.add("bronze", source, "snapshot_duplicate_partitions", SEVERITY_INFO, 0,
                   len(days), "moi co 1 partition - chua kiem duoc snapshot lap (can >= 2 ngay)")
        return

    data_cols = [c for c in frame.columns if not c.startswith("_") and c != PARTITION_COLUMN]
    identical = 0
    for previous, current in zip(days, days[1:]):
        a = frame.where(F.col(PARTITION_COLUMN) == previous).select(*data_cols)
        b = frame.where(F.col(PARTITION_COLUMN) == current).select(*data_cols)
        if per_day[previous] == per_day[current] and a.exceptAll(b).isEmpty():
            identical += 1
    report.add("bronze", source, "snapshot_duplicate_partitions", SEVERITY_WARN, identical,
               len(days) - 1,
               f"{identical}/{len(days) - 1} cap partition lien tiep giong het nhau - "
               "ban sao thua cua cung du lieu nguon")


def check_bronze_opendengue(report: QualityReport, latest: DataFrame) -> None:
    """Kiem tra rieng OpenDengue: ten nuoc khop tham chieu, ISO_A0 khop ten nuoc.

    Args:
        report: Bao cao.
        latest: Partition moi nhat.
    """
    names = [r[0] for r in latest.select("adm_0_name").distinct().collect()]
    unknown = sorted(n for n in names if n not in BY_OPENDENGUE_NAME)
    report.add("bronze", "opendengue", "country_name_in_reference", SEVERITY_ERROR,
               len(unknown), len(names), f"ten khong co trong reference: {unknown}")
    pairs = latest.select("adm_0_name", "ISO_A0").distinct().collect()
    wrong = [(n, i) for n, i in pairs if n in BY_OPENDENGUE_NAME and BY_OPENDENGUE_NAME[n].iso3 != i]
    report.add("bronze", "opendengue", "iso_a0_matches_country", SEVERITY_ERROR, len(wrong),
               len(pairs), f"{wrong}")
    total = latest.count()
    uuids = latest.select("UUID").distinct().count()
    report.add("bronze", "opendengue", "uuid_is_not_row_key", SEVERITY_INFO, total - uuids,
               total, f"{uuids:,} UUID cho {total:,} dong - UUID la ma tai lieu nguon, "
               "khong dung lam khoa dong")


def check_bronze_news(report: QualityReport, frame: DataFrame) -> None:
    """Kiem tra rieng news_rss: guid, _ingested_at bi ghi de, lap bai giua cac fetch.

    Args:
        report: Bao cao.
        frame: Toan bo bang Bronze news_rss.
    """
    rows = frame.count()
    no_guid = frame.where(F.col("guid").isNull() | (F.trim("guid") == "")).count()         if "guid" in frame.columns else rows
    report.add("bronze", "news_rss", "guid_kept", SEVERITY_WARN, no_guid, rows,
               "dong khong co guid (khoa tu nhien cua bai) - dong cu truoc khi Bronze giu "
               "guid; Silver lui ve link")
    no_fetch_time = frame.where(F.col("_fetched_at").isNull()).count()         if "_fetched_at" in frame.columns else rows
    report.add("bronze", "news_rss", "fetched_at_present", SEVERITY_WARN, no_fetch_time, rows,
               "dong khong co _fetched_at - _ingested_at bi ghi de khi dung lai partition, "
               "Silver phai lay thoi diem tu ten file")
    if "feed_country" in frame.columns:
        per_feed = frame.groupBy("feed_country").count().collect()
        report.add("bronze", "news_rss", "articles_per_feed", SEVERITY_INFO, 0, rows,
                   ", ".join(f"{r[0]}: {r[1]}" for r in sorted(per_feed, key=lambda r: str(r[0]))))
    links = frame.select("link").distinct().count()
    report.add("bronze", "news_rss", "repeated_across_fetches", SEVERITY_INFO, rows - links,
               rows, f"{links:,} bai khac nhau trong {rows:,} dong (co y - dedup o Silver)")


def check_bronze_who(report: QualityReport, latest: DataFrame, top: int) -> None:
    """Kiem tra rieng WHO: cham tran $top, cot toan null (rui ro doi kieu).

    Args:
        report: Bao cao.
        latest: Partition moi nhat.
        top: Gia tri $top trong config.
    """
    rows = latest.count()
    report.add("bronze", "who_gho", "below_odata_top", SEVERITY_ERROR, int(rows >= top), rows,
               f"{rows:,} dong / $top={top:,} - cham tran nghia la co the bi cat bot")
    all_null = [
        c for c in ("POPULATION", "SEVERE_CASES", "CONFIRMED_CASES", "DEATHS")
        if c in latest.columns and latest.where(F.col(c).isNotNull()).isEmpty()
    ]
    report.add("bronze", "who_gho", "all_null_columns", SEVERITY_INFO, len(all_null), 4,
               f"{all_null} toan null (doc la string nen khong con rui ro doi kieu)")


def _flag_count(frame: DataFrame, flag: str) -> DataFrame:
    """Loc dong co mot co cu the."""
    return frame.where(F.array_contains("dq_flags", flag))


def check_silver_cases(report: QualityReport, cases: DataFrame, dropped: DataFrame) -> None:
    """Kiem tra Silver dengue_cases.

    Args:
        report: Bao cao.
        cases: Bang Silver da dedup.
        dropped: Dong bi loai vi trung khoa.
    """
    total = cases.count()
    specs = [
        (silver_cases.FLAG_CASES_NOT_NUMERIC, SEVERITY_ERROR,
         "so ca khong ep duoc sang so nguyen", ["source", "country_iso3", "period_start"]),
        (silver_cases.FLAG_NEGATIVE_CASES, SEVERITY_WARN, "so ca am",
         ["source", "country_iso3", "period_start", "cases"]),
        (silver_cases.FLAG_PERIOD_MISSING, SEVERITY_ERROR, "khong xac dinh duoc ngay bat dau ky",
         ["source", "country_iso3", "year"]),
        (silver_cases.FLAG_FUTURE, SEVERITY_WARN, "ky bao cao o tuong lai - loai khoi Gold",
         ["source", "country_iso3", "week_system", "year", "period_start", "cases"]),
        (silver_cases.FLAG_IRREGULAR, SEVERITY_WARN,
         "ky ghi 'month'/'week' nhung do dai khong khop - loai khoi Gold",
         ["source", "country_iso3", "period_type", "period_start", "period_end"]),
        (silver_cases.FLAG_START_DERIVED, SEVERITY_INFO,
         "START_DATE trong, da suy tu YEAR + DATE_NUM", ["country_iso3", "year", "period_start"]),
        (silver_cases.FLAG_START_INCONSISTENT, SEVERITY_WARN,
         "START_DATE khong khop YEAR + DATE_NUM", ["country_iso3", "year", "period_start"]),
        (silver_cases.FLAG_CONFIRMED_GT_CASES, SEVERITY_WARN, "ca xac nhan > tong so ca",
         ["country_iso3", "year", "period_start", "confirmed_cases", "cases"]),
    ]
    for flag, severity, meaning, columns in specs:
        bad = _flag_count(cases, flag)
        failed = bad.count()
        detail = meaning + (f". Vi du: {_sample(bad, columns)}" if failed else "")
        report.add("silver", "dengue_cases", flag, severity, failed, total, detail)

    unknown = cases.where(~F.col("country_iso3").isin(*BY_ISO3)).select("country_iso3").distinct()
    report.add("silver", "dengue_cases", "iso3_in_reference", SEVERITY_ERROR, unknown.count(),
               None, _sample(unknown, ["country_iso3"]))
    report.add("silver", "dengue_cases", "duplicate_keys", SEVERITY_WARN, dropped.count(), total,
               "dong trung khoa Silver bi loai (giu dong so ca lon nhat)"
               + (f". Vi du: {_sample(dropped, ['source', 'country_iso3', 'period_start'])}"
                  if not dropped.isEmpty() else ""))

    months = cases.where(F.col("period_type").isin("month", "week")).withColumn(
        "m", F.trunc("period_start", "month"))
    overlap = (
        months.groupBy("source", "country_iso3", "location_name", "m")
        # countDistinct nhieu cot bo qua dong co null (week_system null o chuoi
        # thang) - gop thanh 1 chuoi truoc de khong dem sot.
        .agg(F.countDistinct(F.concat_ws(
            "/", "period_type", F.coalesce("week_system", F.lit("-")), "case_definition"
        )).alias("n"))
        .where("n > 1")
    )
    report.add("silver", "dengue_cases", "parallel_series_same_month", SEVERITY_INFO,
               overlap.count(), None,
               "cung dia diem/thang co nhieu chuoi song song (thang+tuan, 2 he tuan, "
               "nhieu dinh nghia ca) - Gold chon 1 chuoi de khong cong trung")


def check_silver_news(report: QualityReport, news: DataFrame) -> None:
    """Kiem tra Silver news_articles.

    Args:
        report: Bao cao.
        news: Bang Silver.
    """
    total = news.count()
    unparsed = _flag_count(news, silver_news.FLAG_PUBDATE_UNPARSED)
    report.add("silver", "news_articles", "pubdate_parsed", SEVERITY_ERROR, unparsed.count(),
               total, _sample(unparsed, ["link"]))
    fallback = _flag_count(news, silver_news.FLAG_FETCH_TIME_FALLBACK).count()
    report.add("silver", "news_articles", "fetch_time_from_filename", SEVERITY_WARN, fallback,
               total, "ten file khong co run_id, phai lui ve _ingested_at")
    unmatched = news.where(F.size("countries") == 0).count()
    report.add("silver", "news_articles", "country_matched", SEVERITY_INFO, unmatched, total,
               f"{total - unmatched:,}/{total:,} bai gan duoc it nhat 1 nuoc bang tu khoa")
    old = news.where(~F.col("is_recent")).count()
    report.add("silver", "news_articles", "recent_articles", SEVERITY_INFO, old, total,
               f"{old:,} bai dang truoc lan thay dau > 7 ngay - khong phai tin moi")


def check_gold(report: QualityReport, tables: dict[str, DataFrame]) -> None:
    """Kiem tra Gold: toan ven tham chieu fact -> dim, noi ranh gioi, lech 2 nguon.

    Args:
        report: Bao cao.
        tables: Cac bang Gold theo ten.
    """
    dim_country = tables["dim_country"].select("country_key")
    dim_date = tables["dim_date"].select("date_key")
    dim_unit = tables["dim_admin_unit"].select("unit_key")
    refs = [
        ("fact_monthly_cases", "country_key", dim_country, "country_key"),
        ("fact_monthly_cases", "month_date_key", dim_date, "date_key"),
        ("fact_daily_news", "country_key", dim_country, "country_key"),
        ("fact_daily_news", "date_key", dim_date, "date_key"),
        ("fact_unit_monthly_cases", "unit_key", dim_unit, "unit_key"),
        ("fact_unit_monthly_cases", "month_date_key", dim_date, "date_key"),
        ("fact_unit_daily_news", "unit_key", dim_unit, "unit_key"),
        ("fact_unit_daily_news", "date_key", dim_date, "date_key"),
    ]
    for fact, column, dim, dim_column in refs:
        frame = tables[fact]
        orphans = frame.join(dim, frame[column] == dim[dim_column], "left_anti").count()
        report.add("gold", fact, f"fk_{column}", SEVERITY_ERROR, orphans, frame.count(),
                   f"dong co {column} khong ton tai trong dim")

    units = tables["unit_risk"]
    per_country = units.groupBy("iso3").agg(
        F.count("*").alias("n"),
        F.sum((F.col("signal") == "ca bệnh").cast("int")).alias("cases"),
        F.sum((F.col("news_30d") > 0).cast("int")).alias("news"),
    ).orderBy("iso3").collect()
    no_signal = units.where((F.col("signal") != "ca bệnh") & (F.col("news_30d") == 0)).count()
    report.add("gold", "unit_risk", "unit_signal_coverage", SEVERITY_INFO, no_signal, units.count(),
               "don vi khong co ca so ca gan day lan tin tuc 30 ngay. Theo nuoc (tong / co so ca "
               "gan day / co tin 30 ngay): "
               + ", ".join(f"{r['iso3']} {r['n']}/{r['cases']}/{r['news']}" for r in per_country))

    monthly = tables["fact_monthly_cases"].where("is_complete")
    who = monthly.where(F.col("source") == "who_gho").select(
        "country_key", "month_date_key", F.col("cases").alias("who"))
    od = monthly.where(F.col("source") == "opendengue").select(
        "country_key", "month_date_key", F.col("cases").alias("od"))
    both = who.join(od, ["country_key", "month_date_key"])
    gap = both.where(
        F.abs(F.col("who") - F.col("od")) > 0.5 * F.greatest("who", "od")
    )
    report.add("gold", "fact_monthly_cases", "who_vs_opendengue_gap_gt_50pct", SEVERITY_INFO,
               gap.count(), both.count(),
               "thang ca 2 nguon cung co so lieu nhung lech > 50%. Vi du (country_key, "
               "thang, WHO, OpenDengue): "
               + _sample(gap, ["country_key", "month_date_key", "who", "od"]))

    risk = tables["country_risk"]
    lagged = risk.where("skipped_recent_months > 0")
    report.add("gold", "country_risk", "reporting_lag_skipped_months", SEVERITY_INFO,
               lagged.count(), risk.count(),
               "thang moi nhat giam > 50% so voi thang truoc -> coi la chua bao cao du, "
               "danh gia thang on dinh gan nhat. (iso3, thang danh gia, thang moi nhat, so thang "
               "bo qua): " + _sample(lagged, ["iso3", "data_as_of", "latest_reported_month",
                                             "skipped_recent_months"], 11))
    no_level = risk.where(~F.col("risk_level").isin("cao", "trung bình", "thấp"))
    report.add("gold", "country_risk", "risk_level_available", SEVERITY_INFO,
               no_level.count(), risk.count(),
               "nuoc chua danh gia duoc nguy co: "
               + _sample(no_level, ["iso3", "risk_level", "source", "data_as_of"], 11))


def check_silver_admin(report: QualityReport, units: DataFrame, crosswalk: DataFrame,
                       population: DataFrame) -> None:
    """Kiem tra don vi hanh chinh, bang noi ma va dan so.

    Args:
        report: Bao cao.
        units: Silver admin_units.
        crosswalk: Silver admin_crosswalk.
        population: Silver unit_population.
    """
    no_xy = units.where(F.col("x").isNull() | F.col("y").isNull())
    report.add("silver", "admin_units", "centroid_present", SEVERITY_WARN, no_xy.count(),
               units.count(), "don vi thieu toa do tam - khong noi duoc ma. Vi du: "
               + _sample(no_xy, ["iso3", "unit_id", "unit_name"]))

    old_vn = units.where(F.col("unit_system") == "iso_3166_2")
    mapped = crosswalk.where(F.col("method") == "ne_centroid_in_cod").groupBy("iso_3166_2").count()
    orphan = old_vn.join(mapped.withColumnRenamed("iso_3166_2", "unit_id"), "unit_id", "left_anti")
    multi = mapped.where("count > 1").count()
    report.add("silver", "admin_crosswalk", "vn_old_to_new_one_to_one", SEVERITY_ERROR,
               orphan.count() + multi, old_vn.count(),
               f"63 tinh cu -> 34 tinh moi: {orphan.count()} tinh cu khong vao tinh moi nao, "
               f"{multi} tinh cu vao >1 tinh moi. Vi du: " + _sample(orphan, ["unit_id", "unit_name"]))
    new_vn = crosswalk.where(F.col("method") == "ne_centroid_in_cod").select("pcode").distinct().count()
    report.add("silver", "admin_crosswalk", "vn_new_units_covered", SEVERITY_WARN,
               units.where((F.col("iso3") == "VNM") & (F.col("unit_system") == "pcode")).count()
               - new_vn, None, f"{new_vn} tinh moi nhan it nhat 1 tinh cu")

    current = units.where((F.col("unit_system") == "pcode") & (F.col("iso3") != "VNM"))
    no_ne = current.join(crosswalk.where(F.col("method") == "cod_centroid_in_ne")
                         .select(F.col("pcode").alias("unit_id")), "unit_id", "left_anti")
    report.add("silver", "admin_crosswalk", "cod_unit_has_map_polygon", SEVERITY_WARN,
               no_ne.count(), current.count(),
               "tam don vi COD khong roi vao polygon Natural Earth nao (khong ve duoc len ban do). "
               "Vi du: " + _sample(no_ne, ["iso3", "unit_id", "unit_name"]))

    covered = population.select("unit_id").distinct()
    missing = units.where(F.col("valid_to").isNull() | (F.col("unit_system") == "iso_3166_2")) \
        .join(covered, "unit_id", "left_anti")
    per_country = missing.groupBy("iso3").count().orderBy("iso3").collect()
    report.add("silver", "unit_population", "population_coverage", SEVERITY_INFO,
               missing.count(), None, "don vi khong co dan so theo nuoc: "
               + ", ".join(f"{r[0]}: {r[1]}" for r in per_country))


def check_silver_province_cases(report: QualityReport, cases: DataFrame) -> None:
    """Kiem tra so ca cap tinh: ti le noi duoc vao don vi, theo nguon va cach noi.

    Args:
        report: Bao cao.
        cases: Silver province_cases.
    """
    for source in [r[0] for r in cases.select("source").distinct().collect()]:
        rows = cases.where(F.col("source") == source)
        total = rows.count()
        unmatched = rows.where(F.col("unit_id").isNull())
        locations = unmatched.select("source_location").distinct()
        report.add("silver", "province_cases", f"{source}_matched_to_unit", SEVERITY_WARN,
                   unmatched.count(), total,
                   f"{locations.count()} dia danh khong noi duoc vao don vi (ten khac han). "
                   "Vi du: " + _sample(locations, ["source_location"], 8))
        by_name = rows.where(F.col("match_method") == "name").select(
            "source_location", "unit_name", "match_score").distinct()
        if source == "opendengue":
            report.add("silver", "province_cases", "opendengue_code_wrong_matched_by_name",
                       SEVERITY_WARN, by_name.count(), None,
                       "dia danh OpenDengue co RNE_iso_code khong khop ten don vi -> noi theo ten "
                       "(loi ma cua nguon). Vi du: " + _sample(by_name.orderBy("match_score"),
                                                               ["source_location", "unit_name"], 8))


def check_silver_news_mentions(report: QualityReport, news: DataFrame, mentions: DataFrame) -> None:
    """Ti le bai bao gan duoc it nhat 1 tinh, theo nuoc.

    Args:
        report: Bao cao.
        news: Silver news_articles.
        mentions: Silver news_unit_mentions.
    """
    per = news.select("article_id", F.explode("countries").alias("iso3")).join(
        mentions.select("article_id", "iso3").distinct().withColumn("hit", F.lit(1)),
        ["article_id", "iso3"], "left",
    ).groupBy("iso3").agg(F.count("*").alias("n"), F.sum(F.coalesce("hit", F.lit(0))).alias("hit"))
    rows = per.orderBy("iso3").collect()
    total = sum(r["n"] for r in rows)
    hit = sum(r["hit"] for r in rows)
    report.add("silver", "news_unit_mentions", "articles_with_province", SEVERITY_INFO,
               total - hit, total, "bai gan duoc tinh / bai cua nuoc: "
               + ", ".join(f"{r['iso3']} {r['hit']}/{r['n']}" for r in rows))
