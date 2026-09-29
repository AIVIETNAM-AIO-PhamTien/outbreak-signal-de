"""Silver: bang so ca thong nhat `dengue_cases` tu OpenDengue + WHO GHO.

Grain: nguon x dia diem x ky bao cao x dinh nghia ca. Moi dong co ma ISO3,
cap hanh chinh, ngay bat dau/ket thuc ky (kieu date), so ca (kieu long).

Nhung viec Silver lam o day (Bronze co y khong lam):
  - ep kieu: dengue_total (chuoi) -> long, chuoi ngay -> date
  - chuoi "NA" cua OpenDengue -> null
  - thong nhat cap hanh chinh: o Philippines, OpenDengue dat VUNG o adm_1 va
    TINH o adm_2; cac nuoc khac TINH o adm_1 -> Silver tach ro region/province
  - WHO: START_DATE null (Indonesia 2007-2009, theo thang) -> suy tu YEAR +
    DATE_NUM, gan co `start_date_derived`
  - gan co (khong xoa) dong co van de: ky trong tuong lai, ca xac nhan > tong
    ca, ky bat thuong... Gold tu quyet dinh loai dong nao.
"""

from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ingestion.common.paths import PARTITION_COLUMN

SOURCE_OPENDENGUE = "opendengue"
SOURCE_WHO = "who_gho"

# Thu tu cot cua bang Silver - hai nguon deu dua ve dung khuon nay.
COLUMNS = (
    "source", "country_iso3", "adm_level", "region_name", "province_name",
    "location_name", "rne_iso_code", "period_type", "week_system",
    "period_start", "period_end", "year", "cases", "deaths", "confirmed_cases",
    "case_definition", "source_ref", "bronze_ingestion_date", "dq_flags",
)

# Khoa duy nhat cua mot dong Silver.
KEY_COLUMNS = (
    "source", "country_iso3", "adm_level", "location_name", "period_type",
    "week_system", "period_start", "period_end", "case_definition",
)

FLAG_CASES_NOT_NUMERIC = "cases_not_numeric"
FLAG_NEGATIVE_CASES = "negative_cases"
FLAG_PERIOD_MISSING = "period_start_missing"
FLAG_FUTURE = "future_period"
FLAG_IRREGULAR = "irregular_period"
FLAG_START_DERIVED = "start_date_derived"
FLAG_START_INCONSISTENT = "start_date_inconsistent"
FLAG_CONFIRMED_GT_CASES = "confirmed_gt_cases"


def _na_to_null(column: str) -> F.Column:
    """Doi chuoi "NA" (quy uoc thieu du lieu cua OpenDengue) thanh null.

    Args:
        column: Ten cot chuoi.

    Returns:
        Bieu thuc cot.
    """
    return F.when(F.col(column) == "NA", F.lit(None)).otherwise(F.col(column))


def _flags(*conditions: tuple[F.Column, str]) -> F.Column:
    """Gop cac co thanh mot mang, bo phan tu null.

    Args:
        conditions: Cac cap (dieu kien, ten co).

    Returns:
        Cot array<string>.
    """
    return F.array_compact(
        F.array(*[F.when(cond, F.lit(name)) for cond, name in conditions])
    )


def _irregular(period_type: F.Column, start: F.Column, end: F.Column) -> F.Column:
    """Dieu kien ky bao cao khong dung do dai ghi tren nhan.

    `month` phai nam tron trong mot thang lich; `week` phai dai dung 7 ngay.

    Args:
        period_type: Cot loai ky (week / month / year).
        start: Cot ngay bat dau.
        end: Cot ngay ket thuc.

    Returns:
        Cot boolean.
    """
    return (
        (period_type == "month") & (F.trunc(start, "month") != F.trunc(end, "month"))
    ) | ((period_type == "week") & (F.datediff(end, start) != 6))


def opendengue_to_cases(bronze: DataFrame, today: date) -> DataFrame:
    """Chuyen mot snapshot Bronze OpenDengue sang khuon Silver.

    Args:
        bronze: Mot partition Bronze `opendengue` (moi cot deu la chuoi).
        today: Ngay hien tai, de gan co ky trong tuong lai.

    Returns:
        DataFrame theo COLUMNS.
    """
    adm_level = F.regexp_extract("S_res", r"Admin(\d)", 1).cast("int")
    is_ph = F.col("ISO_A0") == "PHL"
    adm_1 = _na_to_null("adm_1_name")
    adm_2 = _na_to_null("adm_2_name")
    start = F.to_date("calendar_start_date", "yyyy-MM-dd")
    end = F.to_date("calendar_end_date", "yyyy-MM-dd")
    period_type = F.lower("T_res")
    cases = F.col("dengue_total").cast("long")

    return bronze.select(
        F.lit(SOURCE_OPENDENGUE).alias("source"),
        F.col("ISO_A0").alias("country_iso3"),
        adm_level.alias("adm_level"),
        F.when(is_ph, adm_1).alias("region_name"),
        F.when(is_ph, adm_2).otherwise(adm_1).alias("province_name"),
        F.col("full_name").alias("location_name"),
        _na_to_null("RNE_iso_code").alias("rne_iso_code"),
        period_type.alias("period_type"),
        F.lit(None).cast("string").alias("week_system"),
        start.alias("period_start"),
        end.alias("period_end"),
        F.col("Year").cast("int").alias("year"),
        cases.alias("cases"),
        F.lit(None).cast("long").alias("deaths"),
        F.lit(None).cast("long").alias("confirmed_cases"),
        F.col("case_definition_standardised").alias("case_definition"),
        F.col("UUID").alias("source_ref"),
        F.col(PARTITION_COLUMN).alias("bronze_ingestion_date"),
        _flags(
            (cases.isNull(), FLAG_CASES_NOT_NUMERIC),
            (cases < 0, FLAG_NEGATIVE_CASES),
            (start.isNull() | end.isNull(), FLAG_PERIOD_MISSING),
            (start > F.lit(today), FLAG_FUTURE),
            (_irregular(period_type, start, end), FLAG_IRREGULAR),
        ).alias("dq_flags"),
    )


def who_to_cases(bronze: DataFrame, today: date) -> DataFrame:
    """Chuyen mot snapshot Bronze WHO GHO sang khuon Silver.

    WHO khong co cot dinh nghia ca: CASES la tong so ca bao cao, nen gan
    case_definition = "Total" de cung thang do voi OpenDengue.

    Args:
        bronze: Mot partition Bronze `who_gho`.
        today: Ngay hien tai.

    Returns:
        DataFrame theo COLUMNS.
    """
    # Bronze WHO doc moi cot la chuoi (primitivesAsString) - ep kieu o day.
    year = F.col("YEAR").cast("int")
    date_num = F.col("DATE_NUM").cast("int")
    cases = F.col("CASES").cast("long")
    confirmed = F.col("CONFIRMED_CASES").cast("long")
    date_type = F.col("DATE_TYPE")
    is_month = date_type == "month"
    given = F.to_date("START_DATE", "yyyy-MM-dd")
    from_parts = F.when(is_month, F.make_date(year, date_num, F.lit(1)))
    start = F.coalesce(given, from_parts)
    period_type = F.when(is_month, "month").otherwise("week")
    end = F.when(is_month, F.last_day(start)).otherwise(F.date_add(start, 6))

    return bronze.select(
        F.lit(SOURCE_WHO).alias("source"),
        F.col("ISO3").alias("country_iso3"),
        F.lit(0).alias("adm_level"),
        F.lit(None).cast("string").alias("region_name"),
        F.lit(None).cast("string").alias("province_name"),
        F.col("COUNTRY").alias("location_name"),
        F.lit(None).cast("string").alias("rne_iso_code"),
        period_type.alias("period_type"),
        F.when(~is_month, date_type).alias("week_system"),
        start.alias("period_start"),
        end.alias("period_end"),
        year.alias("year"),
        cases.alias("cases"),
        F.col("DEATHS").cast("long").alias("deaths"),
        confirmed.alias("confirmed_cases"),
        F.lit("Total").alias("case_definition"),
        F.lit(None).cast("string").alias("source_ref"),
        F.col(PARTITION_COLUMN).alias("bronze_ingestion_date"),
        _flags(
            (cases.isNull(), FLAG_CASES_NOT_NUMERIC),
            (cases < 0, FLAG_NEGATIVE_CASES),
            (start.isNull(), FLAG_PERIOD_MISSING),
            (start > F.lit(today), FLAG_FUTURE),
            (given.isNull() & start.isNotNull(), FLAG_START_DERIVED),
            (is_month & given.isNotNull() & (given != from_parts), FLAG_START_INCONSISTENT),
            (confirmed > cases, FLAG_CONFIRMED_GT_CASES),
        ).alias("dq_flags"),
    )


def deduplicate(cases: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Giu mot dong cho moi khoa KEY_COLUMNS.

    Neu trung khoa, giu dong co so ca lon nhat (roi theo source_ref) de ket
    qua tat dinh - chay lai luon ra cung mot dong.

    Args:
        cases: DataFrame Silver chua dedup.

    Returns:
        Bo (DataFrame da dedup, DataFrame cac dong bi loai).
    """
    window = Window.partitionBy(*KEY_COLUMNS).orderBy(
        F.col("cases").desc_nulls_last(), F.col("source_ref").asc_nulls_last()
    )
    ranked = cases.withColumn("_rank", F.row_number().over(window))
    kept = ranked.where("_rank = 1").drop("_rank")
    dropped = ranked.where("_rank > 1").drop("_rank")
    return kept, dropped


def build_dengue_cases(
    opendengue: DataFrame, who: DataFrame, today: date
) -> tuple[DataFrame, DataFrame]:
    """Dung bang Silver dengue_cases tu hai snapshot Bronze.

    Args:
        opendengue: Partition moi nhat cua Bronze `opendengue`.
        who: Partition moi nhat cua Bronze `who_gho`.
        today: Ngay hien tai.

    Returns:
        Bo (bang Silver da dedup, cac dong trung khoa bi loai).
    """
    combined = opendengue_to_cases(opendengue, today).unionByName(
        who_to_cases(who, today)
    )
    return deduplicate(combined.select(*COLUMNS))
