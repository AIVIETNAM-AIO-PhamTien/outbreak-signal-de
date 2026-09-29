"""Silver cap tinh: dan so, so ca cap tinh hop nhat, bai bao nhac toi tinh.

    unit_population     dan so moi don vi (P-code / tinh cu VN), nam tham chieu
    province_cases      so ca cap tinh tu OpenDengue + TRENDS (THA) + DOH (PHL),
                        cung mot khoa don vi (unit_id) va don vi hien hanh
                        (current_unit_id)
    news_unit_mentions  bai bao x don vi duoc nhac ten (gazetteer rule-based)

Noi vao don vi hanh chinh (admin_units) theo muc tin cay giam dan:
    1. ma trung thang (TRENDS p_code = P-code COD; OpenDengue VN = ma ISO tinh cu)
    2. ma qua bang noi (OpenDengue RNE_iso_code -> P-code) VA ten khop
    3. chi theo ten, trong cung nuoc (PH DOH chi co ten; ma OpenDengue sai o PH)
Diem giong ten < MIN_SIMILARITY -> khong noi (unit_id null), co kiem tra rieng.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from transform.names import MIN_SIMILARITY, best_name_match, name_similarity

CASE_COLUMNS = (
    "source", "iso3", "unit_id", "current_unit_id", "unit_name", "source_location",
    "match_method", "match_score", "period_type", "week_system", "period_start",
    "period_end", "year", "cases", "deaths", "case_definition", "dq_flags",
)


def _units_for_match(admin_units: DataFrame) -> DataFrame:
    """Don vi dung de so ten: cot iso3, unit_id, unit_name."""
    return admin_units.select("iso3", "unit_id", "unit_name")


def _units_before_reform(admin_units: DataFrame) -> DataFrame:
    """Don vi co hieu luc TRUOC cac lan doi don vi (bo 34 tinh moi VN tu 7/2025).

    Dan so COD-PS (2024) va so ca OpenDengue cap tinh (VN toi 2010) deu thuoc
    thoi ky truoc sap nhap: so ten voi ca tinh moi co the noi nham khi ten trung
    ("Ha Noi" co o ca hai bo).

    Args:
        admin_units: Silver admin_units.

    Returns:
        DataFrame con.
    """
    return admin_units.where(F.col("valid_from").isNull())


def current_units(admin_units: DataFrame, crosswalk: DataFrame) -> DataFrame:
    """unit_id -> current_unit_id: tinh cu VN -> tinh moi; don vi khac -> chinh no.

    Args:
        admin_units: Silver admin_units.
        crosswalk: Silver admin_crosswalk.

    Returns:
        DataFrame (unit_id, current_unit_id).
    """
    old_to_new = crosswalk.where(F.col("method") == "ne_centroid_in_cod").select(
        F.col("iso_3166_2").alias("unit_id"), F.col("pcode").alias("mapped"))
    return admin_units.select("unit_id").join(old_to_new, "unit_id", "left").select(
        "unit_id", F.coalesce("mapped", "unit_id").alias("current_unit_id"))


# ------------------------------------------------------------------ dan so --


def build_unit_population(bronze_ps: DataFrame, admin_units: DataFrame) -> DataFrame:
    """Dan so tong (moi gioi, moi tuoi) cua moi don vi.

    - global_admin1: P-code trung thang voi COD-AB (THA, IDN, KHM, LAO, MYS, TLS).
      VNM dung P-code cua 63 tinh CU (VN805...) -> noi theo ten vao tinh cu.
      PHL cap 1 la vung -> bo, dung file cap 2.
    - phl_admin2: P-code PSGC cu (PH150700000) khac COD-AB moi (PH01028) -> noi theo ten.

    Args:
        bronze_ps: Bronze hdx_cod_ps.
        admin_units: Silver admin_units.

    Returns:
        DataFrame (iso3, unit_id, population, reference_year, match_method, match_score).
    """
    total = bronze_ps.where((F.col("Gender") == "all") & (F.col("Age_range") == "all"))
    level2 = F.col("_resource") == "phl_admin2"
    rows = total.where(level2 | (F.col("ISO3") != "PHL")).select(
        F.col("ISO3").alias("iso3"),
        F.when(level2, F.col("ADM2_PCODE")).otherwise(F.col("ADM1_PCODE")).alias("ps_pcode"),
        F.when(level2, F.col("ADM2_NAME")).otherwise(F.col("ADM1_NAME")).alias("ps_name"),
        F.col("Population").cast("double").cast("long").alias("population"),
        F.col("Reference_year").cast("int").alias("reference_year"),
    )
    units = _units_for_match(_units_before_reform(admin_units))
    by_code = rows.join(units.withColumnRenamed("unit_id", "ps_pcode"),
                        ["iso3", "ps_pcode"], "left_semi")
    direct = by_code.select("iso3", F.col("ps_pcode").alias("unit_id"), "population",
                            "reference_year", F.lit("pcode").alias("match_method"),
                            F.lit(1.0).alias("match_score"))
    by_name = best_name_match(
        rows.join(by_code.select("iso3", "ps_pcode"), ["iso3", "ps_pcode"], "left_anti"),
        "ps_name", units, "unit_name", ["iso3"],
    ).where(F.col("match_score") >= MIN_SIMILARITY).select(
        "iso3", "unit_id", "population", "reference_year",
        F.lit("name").alias("match_method"), "match_score")
    return direct.unionByName(by_name)


# ---------------------------------------------------------------- so ca --


def _opendengue_cases(silver_cases: DataFrame, admin_units: DataFrame,
                      crosswalk: DataFrame) -> DataFrame:
    """So ca cap tinh cua OpenDengue, noi vao don vi.

    VN: RNE_iso_code chinh la ma tinh cu (unit_id he iso_3166_2) - noi thang.
    Nuoc khac: RNE -> P-code qua bang noi; ten phai khop, khong thi noi theo ten.

    Args:
        silver_cases: Silver dengue_cases.
        admin_units: Silver admin_units.
        crosswalk: Silver admin_crosswalk.

    Returns:
        DataFrame co them unit_id, match_method, match_score, source_location.
    """
    rows = silver_cases.where(
        (F.col("source") == "opendengue") & F.col("province_name").isNotNull()
    ).withColumn("source_location", F.col("province_name"))
    locations = rows.select("country_iso3", "source_location", "rne_iso_code").distinct() \
        .withColumnRenamed("country_iso3", "iso3")
    units = _units_for_match(_units_before_reform(admin_units))

    code_units = (
        crosswalk.where(F.col("method") == "cod_centroid_in_ne")
        .select("iso3", F.col("iso_3166_2").alias("rne_iso_code"), F.col("pcode").alias("unit_id"))
        .unionByName(admin_units.where(F.col("unit_system") == "iso_3166_2")
                     .select("iso3", F.col("unit_id").alias("rne_iso_code"), "unit_id"))
        .join(units, ["iso3", "unit_id"])
    )
    # Ung vien = don vi co MA khop; trong do chon ten giong nhat.
    by_code = locations.alias("l").join(
        code_units.alias("c"),
        (F.col("l.iso3") == F.col("c.iso3")) & (F.col("l.rne_iso_code") == F.col("c.rne_iso_code")),
    ).select("l.iso3", "l.source_location", "l.rne_iso_code", "c.unit_id", "c.unit_name")
    by_code = by_code.withColumn(
        "match_score", F.round(name_similarity(F.col("source_location"), F.col("unit_name")), 3))
    window = Window.partitionBy("iso3", "source_location", "rne_iso_code").orderBy(
        F.col("match_score").desc())
    code_ok = (by_code.withColumn("_r", F.row_number().over(window)).where("_r = 1")
               .where(F.col("match_score") >= MIN_SIMILARITY)
               .select("iso3", "source_location", "rne_iso_code", "unit_id", "match_score")
               .withColumn("match_method", F.lit("code+name")))

    rest = locations.join(code_ok, ["iso3", "source_location", "rne_iso_code"], "left_anti")
    name_ok = best_name_match(rest, "source_location", units, "unit_name", ["iso3"]).where(
        F.col("match_score") >= MIN_SIMILARITY
    ).select("iso3", "source_location", "rne_iso_code", "unit_id", "match_score") \
        .withColumn("match_method", F.lit("name"))

    mapping = code_ok.unionByName(name_ok)
    return rows.withColumnRenamed("country_iso3", "iso3").join(
        mapping, ["iso3", "source_location", "rne_iso_code"], "left")


def _trends_cases(bronze_weekly: DataFrame) -> DataFrame:
    """So ca theo tuan x tinh cua TRENDS Thai Lan (p_code = P-code COD).

    Args:
        bronze_weekly: Bronze trends_th_weekly (moi cot la chuoi).

    Returns:
        DataFrame theo khuon province_cases (chua co unit_name/current_unit_id).
    """
    start = F.to_date(F.substring("week_start", 1, 10))
    end = F.to_date(F.substring("week_end", 1, 10))
    cases = F.col("dengue_total").cast("double").cast("long")
    return bronze_weekly.select(
        F.lit("trends_th").alias("source"), F.lit("THA").alias("iso3"),
        F.col("p_code").alias("unit_id"), F.col("province").alias("source_location"),
        F.lit("pcode").alias("match_method"), F.lit(1.0).alias("match_score"),
        F.lit("week").alias("period_type"), F.lit("epiweek").alias("week_system"),
        start.alias("period_start"), end.alias("period_end"),
        F.col("year").cast("int").alias("year"), cases.alias("cases"),
        F.lit(None).cast("long").alias("deaths"), F.lit("Total").alias("case_definition"),
        F.array_compact(F.array(F.when(cases.isNull(), F.lit("cases_not_numeric")))).alias("dq_flags"),
    )


def _ph_doh_cases(bronze_doh: DataFrame, admin_units: DataFrame) -> DataFrame:
    """So ca theo tuan cua DOH Philippines, noi ten tinh vao P-code cap 2.

    `loc` gom ca tinh lan thanh pho doc lap (ANGELES CITY...). Thanh pho khong
    phai tinh se khong noi duoc - co kiem tra ti le rieng.

    Args:
        bronze_doh: Bronze ph_doh.
        admin_units: Silver admin_units.

    Returns:
        DataFrame theo khuon province_cases.
    """
    rows = bronze_doh.withColumn("source_location", F.trim("loc")).withColumn("iso3", F.lit("PHL"))
    locations = rows.select("iso3", "source_location").distinct()
    units = _units_for_match(admin_units.where(F.col("iso3") == "PHL"))
    mapping = best_name_match(locations, "source_location", units, "unit_name", ["iso3"]).select(
        "iso3", "source_location",
        F.when(F.col("match_score") >= MIN_SIMILARITY, F.col("unit_id")).alias("unit_id"),
        "match_score")
    start = F.to_date("date", "M/d/yyyy")
    cases = F.col("cases").cast("double").cast("long")
    return rows.join(mapping, ["iso3", "source_location"], "left").select(
        F.lit("ph_doh").alias("source"), "iso3", "unit_id", "source_location",
        F.lit("name").alias("match_method"), "match_score",
        F.lit("week").alias("period_type"), F.lit(None).cast("string").alias("week_system"),
        start.alias("period_start"), F.date_add(start, 6).alias("period_end"),
        F.year(start).alias("year"), cases.alias("cases"),
        F.col("deaths").cast("double").cast("long").alias("deaths"),
        F.lit("Total").alias("case_definition"),
        F.array_compact(F.array(
            F.when(cases.isNull(), F.lit("cases_not_numeric")),
            F.when(start.isNull(), F.lit("period_start_missing")),
        )).alias("dq_flags"),
    )


def build_province_cases(silver_cases: DataFrame, bronze_trends: DataFrame,
                         bronze_doh: DataFrame, admin_units: DataFrame,
                         crosswalk: DataFrame) -> DataFrame:
    """Bang Silver province_cases: 3 nguon, cung khoa don vi.

    Args:
        silver_cases: Silver dengue_cases (OpenDengue).
        bronze_trends: Bronze trends_th_weekly.
        bronze_doh: Bronze ph_doh.
        admin_units: Silver admin_units.
        crosswalk: Silver admin_crosswalk.

    Returns:
        DataFrame theo CASE_COLUMNS.
    """
    od = _opendengue_cases(silver_cases, admin_units, crosswalk).select(
        "source", "iso3", "unit_id", "source_location", "match_method", "match_score",
        "period_type", "week_system", "period_start", "period_end", "year", "cases",
        "deaths", "case_definition", "dq_flags")
    combined = od.unionByName(_trends_cases(bronze_trends)).unionByName(
        _ph_doh_cases(bronze_doh, admin_units))
    names = admin_units.select("unit_id", "unit_name")
    return (
        combined.join(names, "unit_id", "left")
        .join(current_units(admin_units, crosswalk), "unit_id", "left")
        .select(*CASE_COLUMNS)
    )


# -------------------------------------------------------------- tin tuc --


def build_news_unit_mentions(silver_news: DataFrame, admin_units: DataFrame,
                             crosswalk: DataFrame) -> DataFrame:
    """Bai bao x don vi hanh chinh duoc nhac ten (gazetteer rule-based).

    Ten dung de so: ten tieng Anh + ten ban xu (Thai, Viet...) cua moi don vi,
    ke ca 63 tinh cu cua VN - bao chi van dung ten cu sau sap nhap. Moi ten chi
    so voi bai cua CUNG nuoc (nuoc cua feed hoac nuoc duoc nhac), tranh trung ten
    giua cac nuoc. Chu Latin: bat bien tu (khong khop giua mot tu dai hon); chu
    Thai khong co dau cach giua tu -> so chuoi con. Ten ngan hon 4 ky tu bi bo
    (qua de khop nham).

    Don vi tra ve la don vi HIEN HANH vao ngay dang bai: ten tinh cu VN trong bai
    sau 1/7/2025 duoc quy ve tinh moi chua no.

    Args:
        silver_news: Silver news_articles.
        admin_units: Silver admin_units.
        crosswalk: Silver admin_crosswalk.

    Returns:
        DataFrame (article_id, iso3, unit_id, current_unit_id, matched_name, published_at).
    """
    names = (
        admin_units.select("iso3", "unit_id", "valid_to", F.col("unit_name").alias("n"))
        .unionByName(admin_units.select("iso3", "unit_id", "valid_to",
                                        F.col("local_name").alias("n")))
        .where(F.col("n").isNotNull() & (F.length(F.trim("n")) >= 4))
        .withColumn("n", F.trim("n"))
        .distinct()
    )
    latin = F.col("n").rlike(r"^[\p{IsLatin}\s\-'.]+$")
    # Mau regex: ten da escape; bien tu Unicode cho chu Latin.
    quoted = F.concat(F.lit(r"\Q"), F.col("n"), F.lit(r"\E"))
    pattern = F.when(latin, F.concat(F.lit(r"(?iu)(?<!\p{L})"), quoted, F.lit(r"(?!\p{L})"))) \
        .otherwise(quoted)
    names = names.withColumn("pattern", pattern)

    articles = silver_news.select(
        "article_id", "published_at",
        F.explode("countries").alias("iso3"),
        F.concat_ws(" ", "title", "description_text").alias("text"),
    )
    hits = articles.join(names, "iso3").where(F.expr("text rlike pattern")).select(
        "article_id", "iso3", "unit_id", "valid_to", F.col("n").alias("matched_name"),
        "published_at")
    to_current = current_units(admin_units, crosswalk)
    return (
        hits.join(to_current, "unit_id", "left")
        .select("article_id", "iso3", "unit_id",
                F.coalesce("current_unit_id", "unit_id").alias("current_unit_id"),
                "matched_name", "published_at")
        .dropDuplicates(["article_id", "current_unit_id"])
    )
