"""Nguon bo sung: WHO GHO xMart - so ca dengue, kiem chung boi WHO.

Pipeline 2 buoc, giong cac nguon khac:
    Buoc 1 (Python thuan): goi OData API -> luu response goc + .jsonl -> data/landing/
    Buoc 2 (PySpark):      doc .jsonl cua ngay -> ghi Delta vao data/bronze/

Vi sao them nguon nay: WHO GHO la ground truth CAP QUOC GIA, cung loai voi
OpenDengue nhung tu mot to chuc khac va MOI HON RAT NHIEU - du lieu toi tuan
24/08/2026 (da xac nhan 29/9/2026), trong khi OpenDengue tre toi thang
4/2025 (~17 thang). Gia tri chinh khong nam o "them du lieu" ma o kha nang
PHAT HIEN BAT DONG: neu WHO va OpenDengue bao khac nhau cho cung mot
nuoc/tuan, do la tin hieu chat luong du lieu dang de Silver gan co, khong
phai chi don thuan "co them so."

Loc pham vi SEA O TANG REQUEST (OData $filter=ISO3 in (...)), KHAC voi
OpenDengue phai tai toan cau roi moi loc o Bronze. Ly do khac nhau: day la
REST API co ho tro loc server-side that su (tham so $filter), nen khong co
ly do gi phai tai du lieu ngoai pham vi ve may roi bo di - day la lua chon
PHAM VI THU THAP tu dau, sach hon truong hop OpenDengue (buoc "tai roi loc"
o do la bat buoc vi file zip tinh khong co tham so loc o URL).

Han che da biet (xem spikes/test_who_gho.py): WHO GHO chi phu 9/11 nuoc SEA
- thieu Philippines va Brunei. Da xac nhan khong phai loi query, nguon thuc
su khong co du lieu cho hai nuoc nay.

Lich chay: 1 lan/ngay - cung nhip voi OpenDengue, vi day cung la ground
truth cap quoc gia cap nhat theo tuan/thang, khong phai tin tuc can chay day.
"""

import json
import sys
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

SOURCE = "who_gho"
log = get_logger(SOURCE)


def build_odata_filter(iso3_codes: list[str]) -> str:
    """Dung cu phap OData de loc theo danh sach ma ISO3.

    Tach thanh ham rieng vi day la logic thuan, kiem thu duoc ma khong can
    goi mang - xem tests/test_who_gho.py.

    Args:
        iso3_codes: Danh sach ma ISO3 3 ky tu, vi du ["VNM", "THA"].

    Returns:
        Bieu thuc OData, vi du "ISO3 in ('VNM','THA')".
    """
    quoted = ",".join(f"'{code}'" for code in iso3_codes)
    return f"ISO3 in ({quoted})"


def fetch_raw(
    cfg: dict, ingestion_date: str, meta: IngestionMetadata
) -> Path:
    """Buoc 1: goi OData API, luu ban goc + ban .jsonl cho Spark.

    Ten file dat theo NGAY, khong theo run_id (khac news_rss). Ly do: WHO
    GHO la snapshot ground-truth - goi lai trong cung mot ngay voi cung
    $filter tra ve GAN NHU CUNG mot tap du lieu (cac tuan/thang da cong bo
    khong doi trong ngay), khac han news RSS noi moi lan fetch la bai viet
    MOI thuc su. Neu dat ten theo run_id (moi lan chay mot file rieng), glob
    "*.jsonl" luc doc se cong don nhieu ban COPY cua gan nhu cung du lieu -
    da tung gay loi that: chay 3 lan trong ngay ra 9 dong thay vi 3.

    Vi sao luu ca hai file:
      - .json la RESPONSE GOC y nguyen server tra ve, ca phan envelope
        "@odata.context" - dung tinh than Bronze giu nguyen trang.
      - .jsonl la ban chuyen doi de Spark doc de dang: tach mang "value" ra
        khoi envelope, moi dong mot ban ghi. Khong sua NOI DUNG tung ban
        ghi, chi doi cau truc chua chung.

    Args:
        cfg: Config nguon tu configs/sources.yaml.
        ingestion_date: Ngay phan vung dang YYYY-MM-DD.
        meta: Ban ghi metadata cua lan chay, duoc cap nhat tai cho.

    Returns:
        Duong dan file .jsonl vua ghi.

    Raises:
        IngestionValidationError: Neu goi that bai hoac body rong.
    """
    dest = landing_dir(SOURCE, ingestion_date)
    params = {
        "$filter": build_odata_filter(cfg["filter_iso3"]),
        "$top": cfg["top"],
    }

    log.info("Goi WHO GHO xMart, loc %d ma ISO3", len(cfg["filter_iso3"]))
    response = requests.get(cfg["url"], params=params, timeout=cfg["timeout_seconds"])
    check_response_ok(response, SOURCE)
    log.info("HTTP %s, %s bytes", response.status_code, f"{len(response.content):,}")

    raw_path = dest / f"who_gho_{ingestion_date}.json"
    raw_path.write_bytes(response.content)
    check_raw_file(raw_path, SOURCE)
    meta.add_raw_file(raw_path)

    envelope = response.json()
    records = envelope.get("value", [])

    # Canh bao neu so dong dung dung $top - dau hieu co the bi server cat
    # bot (chua thay xay ra voi SEA, nhung API co the doi hanh vi sau nay).
    if len(records) >= cfg["top"]:
        log.warning(
            "So dong (%d) bang hoac vuot $top=%d - co the bi server cat bot, "
            "nen tang $top trong configs/sources.yaml",
            len(records),
            cfg["top"],
        )

    jsonl_path = dest / f"who_gho_{ingestion_date}.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    check_raw_file(jsonl_path, SOURCE)
    log.info(
        "Lay duoc %d ban ghi, luu vao %s (kem ban response goc)",
        len(records),
        jsonl_path.name,
    )
    return jsonl_path


def load_to_bronze(spark, ingestion_date: str) -> int:
    """Buoc 2: Spark doc TAT CA file .jsonl cua ngay va ghi vao Bronze.

    Doc ca thu muc cua ngay chu khong chi file vua tai, giong quy uoc cua
    news_rss: phan vung cua ngay phai duoc dung lai DAY DU moi lan chay de
    idempotency dung, chay lai nhieu lan trong ngay khong bi nhan doi.

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

    enriched = add_bronze_columns(frame, SOURCE, ingestion_date, utc_now())
    target = write_bronze(enriched, SOURCE, ingestion_date)

    log.info("Da ghi %s dong vao %s", f"{count:,}", target)
    return count


def ingest(spark=None) -> IngestionMetadata:
    """Chay mot lan ingestion day du cho WHO GHO.

    Args:
        spark: SparkSession de dung lai. Neu khong truyen, ham tu tao va tu
            dong lai - cach scheduler goi.

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
