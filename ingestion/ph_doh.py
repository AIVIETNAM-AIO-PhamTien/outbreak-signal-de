"""Nguon cap tinh: Philippines - so ca + tu vong dengue theo tuan x tinh/thanh.

"Philippine Dengue Cases and Deaths" - DOH Epidemiology Bureau, qua HDX (CC-BY).
~126 tinh/thanh (cap 2 hanh chinh cua PH), theo tuan, 2016-2020 (2021 chi 2 tuan).
Lap cho trong cua OpenDengue: PH cap tinh chi toi 2010 va RNE_iso_code bi gan sai
hang loat (do 29/9/2026: chi 17/65 tinh khop ranh gioi).

Bay: dong thu 2 cua CSV la the HXL (#adm2+name, #affected+infected...) - chuan
chu thich cot cua HDX, KHONG phai du lieu. Dong nay bi bo khi nap (la phan
header); file goc trong landing van con nguyen.

Idempotency: `_version` = last_modified cua resource HDX.
"""

import sys

from pyspark.sql import functions as F

from ingestion.common import hdx, http
from ingestion.common.bronze import add_bronze_columns, ingested_rows, write_bronze
from ingestion.common.config import source_config
from ingestion.common.logging import get_logger
from ingestion.common.metadata import IngestionMetadata, new_metadata, write_or_log
from ingestion.common.paths import landing_dir, run_id, today_str, utc_now
from ingestion.common.spark_session import build_spark_session
from ingestion.common.validation import check_dataframe_readable, check_raw_file

SOURCE = "ph_doh"
VERSION_COLUMN = "_version"
log = get_logger(SOURCE)


def ingest(spark=None) -> IngestionMetadata:
    """Chay mot lan ingestion PH DOH.

    Args:
        spark: SparkSession dung lai; khong truyen thi tu tao va tu dong.

    Returns:
        Metadata lan chay (skipped neu da nap dung phien ban).
    """
    cfg = source_config(SOURCE)
    meta = new_metadata(cfg, today_str(), run_id(), cfg["url"])
    owns_session = spark is None
    try:
        resource = hdx.find_resource(hdx.package_show(cfg["package"], cfg), cfg["pattern"])
        version = hdx.version_of(resource)
        meta.source_version, meta.source_url = version, resource["url"]
        spark = spark or build_spark_session(f"{SOURCE}_ingest")
        existing = ingested_rows(spark, SOURCE, **{VERSION_COLUMN: version})
        if existing:
            meta.skip(f"{version} da nap {existing:,} dong", existing)
            return meta

        path = landing_dir(SOURCE, version) / resource["name"]
        if not path.exists():
            http.download(resource["url"], path, cfg["retries"], cfg["timeout_seconds"])
        check_raw_file(path, SOURCE)
        meta.add_raw_file(path)

        frame = spark.read.csv(str(path), header=True, inferSchema=False)
        first = frame.columns[0]
        frame = frame.where(~F.coalesce(F.col(first), F.lit("")).startswith("#"))  # the HXL
        meta.record_count = check_dataframe_readable(frame, SOURCE)
        enriched = add_bronze_columns(frame, SOURCE, meta.ingestion_date, utc_now()).withColumn(
            VERSION_COLUMN, F.lit(version))
        write_bronze(enriched, SOURCE, version, partition_column=VERSION_COLUMN)
        log.info("Da ghi %s dong (%s)", f"{meta.record_count:,}", version)
        return meta
    except Exception as error:
        meta.fail(error)
        log.error("=== That bai: %s ===", meta.error_message)
        raise
    finally:
        write_or_log(meta, log)
        if owns_session and spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(0 if ingest().status != "failed" else 1)
