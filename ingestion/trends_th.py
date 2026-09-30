"""Nguon cap tinh: TRENDS (Zenodo) - so ca dengue THEO TUAN x 77 tinh Thai Lan.

"TRENDS: Temporal Recent Epidemiology of Notifiable Diseases in Southeast Asia",
CC-BY-4.0. Thai Lan 77 tinh (co P-code), tuan dich te 2016 -> 2025, tach DF/DHF/DSS,
kem dan so tung nam va toa do tam tinh. Lap cho trong cua OpenDengue (Thai Lan cap
tinh chi theo thang, toi 2022).

Pipeline:
    Buoc 1 (Python): hoi Zenodo ban moi nhat -> tai TRENDS.xlsx (kiem md5) -> moi
                     sheet thanh CSV trong landing (Spark khong doc xlsx)
    Buoc 2 (Spark):  doc CSV -> ghi Delta: trends_th_weekly, trends_th_province

Idempotency: `_release` = record id Zenodo (moi version mot id), kem `_file_md5`.
Cung record + cung md5 -> bo qua.
"""

import hashlib
import sys
from pathlib import Path

from pyspark.sql import functions as F

from ingestion.common import http
from ingestion.common.bronze import (
    add_bronze_columns,
    ingested_rows,
    read_csv_strings,
    write_bronze,
)
from ingestion.common.config import source_config
from ingestion.common.excel import sheet_to_csv
from ingestion.common.logging import get_logger
from ingestion.common.metadata import IngestionMetadata, new_metadata, write_or_log
from ingestion.common.paths import landing_dir, run_id, today_str, utc_now
from ingestion.common.spark_session import build_spark_session
from ingestion.common.validation import (
    IngestionValidationError,
    check_dataframe_readable,
    check_raw_file,
)

SOURCE = "trends_th"
TABLES = {"weekly": "trends_th_weekly", "province": "trends_th_province"}
log = get_logger(SOURCE)


def md5_of(path: Path) -> str:
    """md5 cua file (Zenodo cong bo checksum dang md5).

    Args:
        path: File.

    Returns:
        Chuoi hex.
    """
    digest = hashlib.md5()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def latest_file(record: dict) -> tuple[str, dict]:
    """Record id va file .xlsx cua ban Zenodo moi nhat.

    Args:
        record: JSON cua `records/<id>/versions/latest`.

    Returns:
        Bo (record id, mo ta file: key, size, checksum, links).

    Raises:
        IngestionValidationError: Neu record khong co file .xlsx.
    """
    for entry in record.get("files", []):
        if entry["key"].lower().endswith(".xlsx"):
            return str(record["id"]), entry
    raise IngestionValidationError(f"[{SOURCE}] record {record.get('id')} khong co file .xlsx")


def ingest(spark=None) -> IngestionMetadata:
    """Chay mot lan ingestion TRENDS.

    Args:
        spark: SparkSession dung lai; khong truyen thi tu tao va tu dong.

    Returns:
        Metadata lan chay (skipped neu da nap dung ban nay).
    """
    cfg = source_config(SOURCE)
    meta = new_metadata(cfg, today_str(), run_id(), cfg["url"])
    owns_session = spark is None
    try:
        record_id, entry = latest_file(http.get(cfg["url"], cfg["retries"],
                                                cfg["timeout_seconds"]).json())
        md5 = entry["checksum"].split(":", 1)[-1]
        meta.source_version = record_id
        spark = spark or build_spark_session(f"{SOURCE}_ingest")
        # Bo qua chi khi MOI bang da co phien ban nay: bang weekly ghi truoc, neu
        # bang province loi thi lan sau phai nap lai (ghi de theo _release, khong nhan doi).
        counts = {kind: ingested_rows(spark, table, _release=record_id, _file_md5=md5)
                  for kind, table in TABLES.items()}
        existing = counts["weekly"] if all(counts.values()) else 0
        if existing:
            meta.skip(f"record {record_id} (md5 {md5[:10]}) da nap {existing:,} dong", existing)
            return meta

        dest = landing_dir(SOURCE, record_id)
        xlsx = dest / entry["key"]
        if not xlsx.exists() or md5_of(xlsx) != md5:
            http.download(entry["links"]["self"], xlsx, cfg["retries"], cfg["timeout_seconds"])
            if md5_of(xlsx) != md5:
                raise IngestionValidationError(f"[{SOURCE}] {xlsx.name} lech md5 voi Zenodo")
        check_raw_file(xlsx, SOURCE)
        meta.add_raw_file(xlsx)

        total = 0
        for kind, sheet in cfg["sheets"].items():
            csv_path = dest / f"{kind}.csv"
            sheet_to_csv(xlsx, sheet, csv_path)
            frame = read_csv_strings(spark, csv_path)
            count = check_dataframe_readable(frame, SOURCE)
            enriched = (
                add_bronze_columns(frame, TABLES[kind], meta.ingestion_date, utc_now())
                .withColumn("_release", F.lit(record_id))
                .withColumn("_file_md5", F.lit(md5))
            )
            write_bronze(enriched, TABLES[kind], record_id, partition_column="_release")
            log.info("%s: %s dong", TABLES[kind], f"{count:,}")
            total += count
        meta.record_count = total
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
