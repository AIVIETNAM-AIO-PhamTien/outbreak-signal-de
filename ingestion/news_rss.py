"""Nguon MVP 2: Google News RSS - tin tuc ve sot xuat huyet, moi nuoc mot feed.

Pipeline 2 buoc, giong OpenDengue:
    Buoc 1 (Python thuan): goi tung feed -> luu .xml goc moi feed + 1 .jsonl -> data/landing/
    Buoc 2 (PySpark):      doc cac .jsonl cua ngay -> ghi Delta vao data/bronze/

Moi nuoc mot feed, bang tieng ban xu neu Google ho tro (xem configs/sources.yaml),
kem bo loc thoi gian `when:7d`. Query cu (1 cau tieng Anh, khong loc thoi gian)
tra ve toan bai cu: do 29/9/2026, 0/64 bai dang trong 7 ngay, cu nhat tu 2013.

Moi bai giu:
    - cac truong chinh, giu nguyen ten the: title, link, guid, pubDate, source,
      description; them source_url (thuoc tinh url cua the <source>)
    - raw_payload: nguyen van XML cua <item> - khong bao gio mat truong nao, ke ca
      truong Google them sau nay
    - feed_*: feed (nuoc / ngon ngu / query) da tra ve bai - la metadata cua
      request, khong phai suy dien tu noi dung
    - _fetched_at: thoi diem goi feed (UTC). Khac `_ingested_at` (thoi diem Spark
      nap): partition cua ngay duoc dung lai moi lan chay nen `_ingested_at` bi
      ghi de, con `_fetched_at` giu dung luc thay bai.

Mot feed loi (sau khi da thu lai) chi ghi canh bao vao metadata; ca lan chay chi
that bai khi MOI feed deu loi.
"""

import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

from ingestion.common import http
from ingestion.common.bronze import add_bronze_columns, write_bronze
from ingestion.common.config import source_config
from ingestion.common.logging import get_logger
from ingestion.common.metadata import IngestionMetadata, new_metadata, write_or_log
from ingestion.common.paths import landing_dir, run_id, today_str, utc_now
from ingestion.common.spark_session import build_spark_session
from ingestion.common.validation import (
    IngestionValidationError,
    check_dataframe_readable,
    check_landing_not_empty,
    check_raw_file,
    check_response_ok,
)

SOURCE = "news_rss"
log = get_logger(SOURCE)

# Cac the lay tu moi <item>. Giu nguyen ten the lam ten cot.
ITEM_FIELDS = ("title", "link", "guid", "pubDate", "source", "description")


def feed_params(feed: dict, recency: str) -> dict[str, str]:
    """Query string Google News cho mot feed.

    Args:
        feed: Mot phan tu `feeds` trong config (country, gl, hl, q).
        recency: Bo loc thoi gian, vd "when:7d".

    Returns:
        Dict tham so q / hl / gl / ceid.
    """
    return {
        "q": f"{feed['q']} {recency}".strip(),
        "hl": feed["hl"],
        "gl": feed["gl"],
        "ceid": f"{feed['gl']}:{feed['hl']}",
    }


def parse_items(xml_bytes: bytes, feed: dict, query: str, fetched_at: str) -> list[dict]:
    """Tach cac <item> cua mot feed thanh ban ghi phang. Khong sua noi dung.

    Args:
        xml_bytes: XML goc cua feed.
        feed: Cau hinh feed.
        query: Query thuc te da goi (gom bo loc thoi gian).
        fetched_at: Thoi diem goi feed, ISO-8601 UTC.

    Returns:
        Moi bai mot dict, moi gia tri la chuoi.

    Raises:
        ET.ParseError: Neu noi dung khong phai XML.
    """
    records = []
    for item in ET.fromstring(xml_bytes).findall(".//item"):
        record = {field: (item.findtext(field) or "").strip() for field in ITEM_FIELDS}
        source_tag = item.find("source")
        record["source_url"] = source_tag.get("url", "") if source_tag is not None else ""
        record["raw_payload"] = ET.tostring(item, encoding="unicode")
        record.update(
            feed_country=feed["country"], feed_hl=feed["hl"], feed_gl=feed["gl"],
            feed_query=query, _fetched_at=fetched_at,
        )
        records.append(record)
    return records


def fetch_raw(
    cfg: dict, ingestion_date: str, current_run_id: str, meta: IngestionMetadata
) -> Path:
    """Buoc 1: goi tung feed, luu .xml goc moi feed va mot .jsonl cho ca lan chay.

    Args:
        cfg: Config nguon.
        ingestion_date: Ngay phan vung YYYY-MM-DD.
        current_run_id: Ma lan chay, dung dat ten file.
        meta: Metadata lan chay, cap nhat tai cho.

    Returns:
        Duong dan file .jsonl vua ghi.

    Raises:
        IngestionValidationError: Neu moi feed deu loi.
    """
    dest = landing_dir(SOURCE, ingestion_date)
    records: list[dict] = []
    failed: list[str] = []

    for feed in cfg["feeds"]:
        params = feed_params(feed, cfg.get("recency", ""))
        fetched_at = utc_now().isoformat()
        try:
            response = http.get(
                cfg["url"], cfg["retries"], cfg["timeout_seconds"], params=params,
                headers={"User-Agent": cfg["user_agent"]},
            )
            check_response_ok(response, SOURCE)
            items = parse_items(response.content, feed, params["q"], fetched_at)
        except (requests.RequestException, IngestionValidationError, ET.ParseError) as error:
            failed.append(feed["country"])
            meta.warnings.append(f"feed {feed['country']} loi: {type(error).__name__}: {error}")
            log.warning("Feed %s loi, bo qua: %s", feed["country"], error)
            continue

        xml_path = dest / f"google_news_{current_run_id}_{feed['country']}.xml"
        xml_path.write_bytes(response.content)
        meta.add_raw_file(xml_path)
        if len(items) >= cfg.get("max_items", 100):
            meta.warnings.append(
                f"feed {feed['country']} tra {len(items)} bai = tran cua Google, co the bi cat"
            )
        log.info("Feed %s: %d bai", feed["country"], len(items))
        records.extend(items)

    if len(failed) == len(cfg["feeds"]):
        raise IngestionValidationError(f"[{SOURCE}] moi feed deu loi: {failed}")

    jsonl_path = dest / f"google_news_{current_run_id}.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    check_raw_file(jsonl_path, SOURCE)
    log.info("Tong %d bai tu %d feed, luu vao %s", len(records),
             len(cfg["feeds"]) - len(failed), jsonl_path.name)
    return jsonl_path


def load_to_bronze(spark, ingestion_date: str) -> int:
    """Buoc 2: Spark doc TAT CA file .jsonl cua ngay va ghi vao Bronze.

    Doc ca thu muc cua ngay chu khong chi file vua tai: phan vung cua ngay phai
    gom du moi lan chay - cung la ly do chay lai khong bi nhan doi.

    Args:
        spark: SparkSession dang hoat dong.
        ingestion_date: Ngay phan vung YYYY-MM-DD.

    Returns:
        So dong da ghi.

    Raises:
        IngestionValidationError: Neu landing rong hoac Spark doc ra 0 dong.
    """
    landing = landing_dir(SOURCE, ingestion_date)
    check_landing_not_empty(landing, "*.jsonl", SOURCE)

    frame = spark.read.option("primitivesAsString", True).json(str(landing / "*.jsonl"))
    count = check_dataframe_readable(frame, SOURCE)

    # KHONG gan nuoc/tinh o day - suy tu noi dung la viec cua Silver.
    enriched = add_bronze_columns(frame, SOURCE, ingestion_date, utc_now())
    target = write_bronze(enriched, SOURCE, ingestion_date)

    log.info("Da ghi %s dong vao %s", f"{count:,}", target)
    return count


def ingest(spark=None) -> IngestionMetadata:
    """Chay mot lan ingestion day du cho Google News RSS.

    Args:
        spark: SparkSession de dung lai. Neu khong truyen, ham tu tao va tu dong lai.

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
        write_or_log(meta, log)
        if owns_session and spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(0 if ingest().record_count else 1)
