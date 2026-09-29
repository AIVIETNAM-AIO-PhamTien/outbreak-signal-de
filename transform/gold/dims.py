"""Gold - cac bang dimension cua star schema.

    dim_country   11 nuoc SEA + dong UNK (tin tuc khong gan duoc nuoc)
    dim_date      moi ngay, khoa yyyymmdd

Khoa thay the (surrogate key) duoc gan TAT DINH theo thu tu sap xep cua khoa
tu nhien, nen chay lai luon ra cung khoa - fact cu khong bi lech dim moi.
"""

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from transform.common import small_frame
from transform.reference import COUNTRIES, UNKNOWN_ISO3

UNKNOWN_COUNTRY_KEY = 0


def date_key(column: F.Column) -> F.Column:
    """Khoa ngay dang so nguyen yyyymmdd.

    Args:
        column: Cot date.

    Returns:
        Cot int.
    """
    return F.date_format(column, "yyyyMMdd").cast("int")


def build_dim_country(spark: SparkSession, silver_cases: DataFrame) -> DataFrame:
    """Dung dim_country tu bang tham chieu.

    Args:
        spark: SparkSession.
        silver_cases: Silver dengue_cases, de biet nuoc nao co du lieu WHO.

    Returns:
        DataFrame dim_country.
    """
    schema = StructType(
        [
            StructField("country_key", IntegerType(), False),
            StructField("iso3", StringType(), False),
            StructField("country_name", StringType(), False),
            StructField("opendengue_name", StringType(), True),
            StructField("who_name", StringType(), True),
        ]
    )
    rows = [(UNKNOWN_COUNTRY_KEY, UNKNOWN_ISO3, "Unknown", None, None)] + [
        (index, c.iso3, c.name, c.opendengue_name, c.who_name)
        for index, c in enumerate(sorted(COUNTRIES, key=lambda c: c.iso3), start=1)
    ]
    countries = small_frame(spark, rows, schema)
    who_iso3 = (
        silver_cases.where(F.col("source") == "who_gho")
        .select(F.col("country_iso3").alias("iso3"))
        .distinct()
        .withColumn("has_who_data", F.lit(True))
    )
    return countries.join(who_iso3, "iso3", "left").select(
        "country_key", "iso3", "country_name", "opendengue_name", "who_name",
        F.coalesce("has_who_data", F.lit(False)).alias("has_who_data"),
    )


def build_dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    """Dung dim_date moi ngay trong khoang [start, end].

    Args:
        spark: SparkSession.
        start: Ngay dau.
        end: Ngay cuoi.

    Returns:
        DataFrame dim_date.
    """
    days = spark.sql(
        f"SELECT explode(sequence(DATE'{start.isoformat()}', "
        f"DATE'{end.isoformat()}', INTERVAL 1 DAY)) AS date"
    )
    return days.select(
        date_key(F.col("date")).alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        F.weekofyear("date").alias("iso_week"),
        F.dayofweek("date").alias("day_of_week"),
        F.trunc("date", "month").alias("month_start"),
    )
