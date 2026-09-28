"""Nguon MVP 2: Google News RSS - tin tuc ve sot xuat huyet.

Pipeline 2 buoc, giong OpenDengue:
    Buoc 1 (Python thuan): goi RSS -> luu .xml goc + .jsonl -> data/landing/
    Buoc 2 (PySpark):      doc cac .jsonl cua ngay -> ghi Delta vao data/bronze/

Vi sao luu ca .xml lan .jsonl:
    .xml la du lieu GOC y nguyen server tra ve, giu de doi chieu sau nay.
    .jsonl la ban chuyen doi de Spark doc duoc (Spark khong doc XML neu khong
    cai them spark-xml). Chi doi dinh dang chua, khong sua noi dung.

Lich chay: moi 30 phut. Moi lan chay tao them mot cap file trong landing cua
ngay, va phan vung Bronze cua ngay duoc dung lai tu TOAN BO file trong ngay -
nen chay lai nhieu lan khong nhan doi du lieu.

Han che da biet: RSS khong co truong quoc gia hay dia diem nao ca. Viec suy
ra quoc gia tu tieu de la cua tang Silver, khong lam o day.
"""

import json
import sys
import xml.etree.ElementTree as ET
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

SOURCE = "news_rss"
log = get_logger(SOURCE)

# Cac the lay tu moi <item> cua RSS. Giu nguyen ten the lam ten cot - Bronze
# khong doi ten truong.
ITEM_FIELDS = ("title", "link", "pubDate", "source", "description")


def fetch_raw(
    cfg: dict, ingestion_date: str, current_run_id: str, meta: IngestionMetadata
) -> Path:
    """Buoc 1: goi RSS, luu ban .xml goc va ban .jsonl cho Spark.

    Args:
        cfg: Config nguon tu configs/sources.yaml.
        ingestion_date: Ngay phan vung dang YYYY-MM-DD.
        current_run_id: Ma lan chay, dung dat ten file de nhieu lan chay trong
            ngay khong ghi de len nhau.
        meta: Ban ghi metadata cua lan chay, duoc cap nhat tai cho.

    Returns:
        Duong dan file .jsonl vua ghi.

    Raises:
        IngestionValidationError: Neu goi that bai hoac file rong.
    """
    dest = landing_dir(SOURCE, ingestion_date)

    log.info("Goi Google News RSS, query=%r", cfg["params"]["q"])
    response = requests.get(
        cfg["url"],
        params=cfg["params"],
        headers={"User-Agent": cfg["user_agent"]},
        timeout=cfg["timeout_seconds"],
    )
    check_response_ok(response, SOURCE)
    log.info("HTTP %s, %s bytes", response.status_code, f"{len(response.content):,}")

    xml_path = dest / f"google_news_{current_run_id}.xml"
    xml_path.write_bytes(response.content)
    check_raw_file(xml_path, SOURCE)
    meta.add_raw_file(xml_path)

    items = [
        {field: (item.findtext(field) or "").strip() for field in ITEM_FIELDS}
        for item in ET.fromstring(response.content).findall(".//item")
    ]

    jsonl_path = dest / f"google_news_{current_run_id}.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as handle:
        for item in items:
            handle.write(json.dumps(item, ensure_ascii=False) + "\n")

    check_raw_file(jsonl_path, SOURCE)
    log.info("Lay duoc %d bai, luu vao %s (kem ban .xml goc)", len(items), jsonl_path.name)
    return jsonl_path


def load_to_bronze(spark, ingestion_date: str) -> int:
    """Buoc 2: Spark doc TAT CA file .jsonl cua ngay va ghi vao Bronze.

    Doc ca thu muc cua ngay chu khong chi file vua tai: mot ngay chay nhieu
    lan nen phan vung cua ngay phai gom du moi lan chay. Cach nay cung la ly
    do chay lai khong bi nhan doi.

    Args:
        spark: SparkSession dang hoat dong.
        ingestion_date: Ngay phan vung dang YYYY-MM-DD.

    Returns:
        So dong da ghi.

    Raises:
        IngestionValidationError: Neu landing rong hoac Spark doc ra 0 dong.
    """
    landing = landing_dir(SOURCE, ingestion_date)
    check_landing_not_empty(landing, "*.jsonl", SOURCE)

    frame = spark.read.json(str(landing / "*.jsonl"))
    count = check_dataframe_readable(frame, SOURCE)

    # KHONG loc theo quoc gia o day - suy ra quoc gia tu tieu de la viec cua Silver.
    enriched = add_bronze_columns(frame, SOURCE, ingestion_date, utc_now())
    target = write_bronze(enriched, SOURCE, ingestion_date)

    log.info("Da ghi %s dong vao %s", f"{count:,}", target)
    return count


def ingest(spark=None) -> IngestionMetadata:
    """Chay mot lan ingestion day du cho Google News RSS.

    Args:
        spark: SparkSession de dung lai. Neu khong truyen, ham tu tao va tu
            dong lai.

    Returns:
        Ban ghi metadata cua lan chay, da duoc ghi ra dia.
    """
    cfg = source_config(SOURCE)
    ingestion_date = today_str()
    current_run_id = run_id()
    meta = new_metadata(cfg, ingestion_date, current_run_id, cfg["url"])

    log.info("=== Bat dau ingestion | ingestion_date=%s ===", ingestion_date)
    owns_session = spark is None
    try:
        fetch_raw(cfg, ingestion_date, current_run_id, meta)
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
