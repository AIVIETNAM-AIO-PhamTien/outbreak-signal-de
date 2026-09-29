"""Nguon duoi cap quoc gia: Singapore NEA - cum dich sot xuat huyet dang hoat dong.

data.gov.sg dataset d_dbfabf16158d1b0e1c420627c0819168 (NEA). Moi cum: polygon,
so ca (CASE_SIZE), dia danh (LOCALITY). Truy cap 2 buoc: `poll-download` tra URL
S3 ky san (song ngan) -> URL do tra GeoJSON that.

Day la snapshot "cac cum dang hoat dong luc goi", khong phai chuoi lich su: moi
ngay mot partition, file dat ten theo ngay (goi lai trong ngay thi thay the,
giong WHO). OBJECTID KHONG phai khoa: NEA danh so lai moi lan cong bo.

0 cum dich la trang thai hop le (het dich), khong phai loi va khong phai "khong co
gi moi": snapshot cu trong ngay phai bi xoa, neu khong Bronze van bao cac cum da het.
Ngay co 0 cum thi partition ngay do rong - Silver can doc metadata (status success,
record_count 0) de phan biet "0 cum" voi "khong chay".
"""

import json
import sys
from pathlib import Path

from ingestion.common import http
from ingestion.common.bronze import add_bronze_columns, write_bronze
from ingestion.common.config import source_config
from ingestion.common.logging import get_logger
from ingestion.common.metadata import IngestionMetadata, new_metadata, write_or_log
from ingestion.common.paths import (
    PARTITION_COLUMN,
    bronze_path,
    landing_dir,
    run_id,
    today_str,
    utc_now,
)
from ingestion.common.spark_session import build_spark_session
from ingestion.common.validation import (
    IngestionValidationError,
    check_dataframe_readable,
    check_raw_file,
)

SOURCE = "sg_nea"
log = get_logger(SOURCE)


def feature_to_record(feature: dict, fetched_at: str) -> dict[str, str]:
    """Lam phang mot feature: moi thuoc tinh giu nguyen ten, geometry thanh chuoi.

    Args:
        feature: Feature GeoJSON.
        fetched_at: Thoi diem goi API, ISO-8601 UTC.

    Returns:
        Dict toan chuoi.
    """
    record = {key: "" if value is None else str(value)
              for key, value in feature.get("properties", {}).items()}
    record["geometry"] = json.dumps(feature.get("geometry"))
    record["raw_payload"] = json.dumps(feature, ensure_ascii=False)
    record["_fetched_at"] = fetched_at
    return record


def clear_day(spark, jsonl: Path, day: str) -> None:
    """Xoa snapshot cu cua ngay (file jsonl trong landing + dong Bronze cua partition).

    Goi khi lan fetch moi nhat tra 0 cum: snapshot cua ngay phai phan anh lan fetch
    moi nhat. File GeoJSON goc cua lan fetch 0 cum van nam trong landing.

    Args:
        spark: SparkSession.
        jsonl: File jsonl cua ngay trong landing.
        day: Ngay partition YYYY-MM-DD.
    """
    from delta.tables import DeltaTable

    jsonl.unlink(missing_ok=True)
    target = str(bronze_path(SOURCE))
    if DeltaTable.isDeltaTable(spark, target):
        DeltaTable.forPath(spark, target).delete(f"{PARTITION_COLUMN} = '{day}'")


def ingest(spark=None) -> IngestionMetadata:
    """Chay mot lan ingestion SG NEA.

    Args:
        spark: SparkSession dung lai; khong truyen thi tu tao va tu dong.

    Returns:
        Metadata lan chay.
    """
    cfg = source_config(SOURCE)
    day = today_str()
    meta = new_metadata(cfg, day, run_id(), cfg["url"])
    owns_session = spark is None
    try:
        fetched_at = utc_now().isoformat()
        envelope = http.get(cfg["url"], cfg["retries"], cfg["timeout_seconds"]).json()
        if envelope.get("code") != 0:
            # data.gov.sg bao loi ung dung bang `code` du HTTP 200.
            raise IngestionValidationError(
                f"[{SOURCE}] poll-download code={envelope.get('code')} {envelope.get('errorMsg')}")
        response = http.get(envelope["data"]["url"], cfg["retries"], cfg["timeout_seconds"])

        dest = landing_dir(SOURCE, day)
        raw_path = dest / f"sg_nea_{day}.geojson"
        raw_path.write_bytes(response.content)
        check_raw_file(raw_path, SOURCE)
        meta.add_raw_file(raw_path)

        features = response.json().get("features", [])
        jsonl = dest / f"sg_nea_{day}.jsonl"
        spark = spark or build_spark_session(f"{SOURCE}_ingest")
        if not features:
            clear_day(spark, jsonl, day)
            meta.record_count = 0
            meta.warnings.append("0 cum dich dang hoat dong - da xoa snapshot cu trong ngay")
            return meta
        with open(jsonl, "w", encoding="utf-8") as handle:
            for feature in features:
                handle.write(json.dumps(feature_to_record(feature, fetched_at),
                                        ensure_ascii=False) + "\n")

        frame = spark.read.option("primitivesAsString", True).json(str(dest / "*.jsonl"))
        meta.record_count = check_dataframe_readable(frame, SOURCE)
        write_bronze(add_bronze_columns(frame, SOURCE, day, utc_now()), SOURCE, day)
        log.info("Da ghi %d cum dich", meta.record_count)
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
