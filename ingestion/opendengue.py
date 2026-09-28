"""Nguon MVP 1: OpenDengue - so ca sot xuat huyet theo quoc gia.

Pipeline 2 buoc:
    Buoc 1 (Python thuan): tai zip -> giai nen CSV -> data/landing/
    Buoc 2 (PySpark):      doc CSV cua ngay -> ghi Delta vao data/bronze/

Vi sao tach 2 buoc: Spark KHONG goi duoc API hay tai duoc file tu Internet,
no chi doc duoc file co san tren dia. Nen phai dung requests tai ve truoc.

Lich chay: 1 lan/ngay. OpenDengue khong phai live data - ho phat hanh theo
version (V1.3, V1.2...), vai thang moi co ban moi, chay day hon cung chi tai
lai dung mot file khong doi.
"""

import sys
import zipfile
from pathlib import Path

import requests

from ingestion.common.bronze import add_bronze_columns, write_bronze
from ingestion.common.config import source_config
from ingestion.common.logging import get_logger
from ingestion.common.metadata import IngestionMetadata, new_metadata
from ingestion.common.paths import landing_dir, run_id, today_str, utc_now
from ingestion.common.spark_session import build_spark_session
from ingestion.common.validation import (
    check_dataframe_readable,
    check_landing_not_empty,
    check_raw_file,
    check_response_ok,
)

SOURCE = "opendengue"
log = get_logger(SOURCE)


def fetch_raw(cfg: dict, ingestion_date: str, meta: IngestionMetadata) -> Path:
    """Buoc 1: tai zip ve landing va giai nen lay file CSV.

    Zip da tai roi thi dung lai, khong tai lai - dataset chi doi vai thang
    mot lan nen tai lai moi lan chay la phi bang thong vo ich.

    Args:
        cfg: Config nguon tu configs/sources.yaml.
        ingestion_date: Ngay phan vung dang YYYY-MM-DD.
        meta: Ban ghi metadata cua lan chay, duoc cap nhat tai cho.

    Returns:
        Duong dan file CSV da giai nen.

    Raises:
        IngestionValidationError: Neu tai ve that bai hoac file rong.
        RuntimeError: Neu trong zip khong co file CSV nao.
    """
    dest = landing_dir(SOURCE, ingestion_date)
    zip_path = dest / Path(cfg["url"]).name

    if zip_path.exists():
        log.info("Da co file tai truoc do, dung lai: %s", zip_path.name)
    else:
        log.info("Dang tai %s", cfg["url"])
        response = requests.get(cfg["url"], timeout=cfg["timeout_seconds"])
        check_response_ok(response, SOURCE)
        zip_path.write_bytes(response.content)
        log.info("Da luu %s bytes vao %s", f"{len(response.content):,}", zip_path.name)

    check_raw_file(zip_path, SOURCE)
    meta.add_raw_file(zip_path)

    with zipfile.ZipFile(zip_path) as archive:
        names = [n for n in archive.namelist() if n.lower().endswith(".csv")]
        if not names:
            raise RuntimeError(f"khong tim thay CSV trong zip: {archive.namelist()}")
        archive.extract(names[0], dest)

    csv_path = dest / names[0]
    check_raw_file(csv_path, SOURCE)
    log.info("Da giai nen ra %s", csv_path.name)
    return csv_path


def load_to_bronze(spark, ingestion_date: str) -> int:
    """Buoc 2: Spark doc CSV cua ngay va ghi vao Bronze.

    Args:
        spark: SparkSession dang hoat dong.
        ingestion_date: Ngay phan vung dang YYYY-MM-DD.

    Returns:
        So dong da ghi.

    Raises:
        IngestionValidationError: Neu landing rong hoac Spark doc ra 0 dong.
    """
    landing = landing_dir(SOURCE, ingestion_date)
    check_landing_not_empty(landing, "*.csv", SOURCE)

    # inferSchema=False -> moi cot deu la string. CO Y nhu vay: Bronze giu
    # nguyen trang, khong ep kieu. Ep kieu la viec cua Silver.
    frame = spark.read.csv(str(landing / "*.csv"), header=True, inferSchema=False)
    count = check_dataframe_readable(frame, SOURCE)

    # KHONG loc rieng Dong Nam A o day - Bronze giu toan bo du lieu goc.
    enriched = add_bronze_columns(frame, SOURCE, ingestion_date, utc_now())
    target = write_bronze(enriched, SOURCE, ingestion_date)

    log.info("Da ghi %s dong vao %s", f"{count:,}", target)
    return count


def ingest(spark=None) -> IngestionMetadata:
    """Chay mot lan ingestion day du cho OpenDengue.

    Args:
        spark: SparkSession de dung lai. Neu khong truyen, ham tu tao va tu
            dong lai - cach scheduler goi.

    Returns:
        Ban ghi metadata cua lan chay, da duoc ghi ra dia. Status la "failed"
        neu co loi, va loi duoc nem tiep len cho nguoi goi xu ly.
    """
    cfg = source_config(SOURCE)
    ingestion_date = today_str()
    meta = new_metadata(cfg, ingestion_date, run_id(), cfg["url"])

    log.info("=== Bat dau ingestion | ingestion_date=%s ===", ingestion_date)
    owns_session = spark is None
    try:
        fetch_raw(cfg, ingestion_date, meta)
        spark = spark or build_spark_session(f"{SOURCE}_ingest")
        meta.record_count = load_to_bronze(spark, ingestion_date)
        log.info("=== Hoan tat: %s dong ===", f"{meta.record_count:,}")
        return meta
    except Exception as error:
        meta.fail(error)
        log.error("=== That bai: %s ===", meta.error_message)
        raise
    finally:
        path = meta.write()
        log.info("Da ghi metadata: %s", path.name)
        if owns_session and spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(0 if ingest().record_count else 1)
