"""Nguon bo sung: Singapore NEA - cum dich sot xuat huyet.

Data source: dataset `d_dbfabf16158d1b0e1c420627c0819168` tren data.gov.sg,
do National Environment Agency (NEA) cong bo. Day la nguon DUY NHAT trong
project vua co so ca that vua co toa do that (polygon cum dich).

Cach truy cap 2 chang: endpoint `poll-download` tra ve mot URL S3 presigned
song ngan, URL do moi phuc vu file GeoJSON that.

Pipeline 2 buoc, giong hai nguon kia:
    Buoc 1 (Python thuan): goi API -> luu .json goc -> data/landing/
    Buoc 2 (PySpark):      doc cac .json cua ngay -> ghi Delta vao data/bronze/

Lich chay: 1 lan/ngay. Theo docs/data-profile-sg-nea.md, NEA publish quanh
15:00 SGT va noi dung chi doi moi 1-4 ngay, nen poll 60 phut mot lan la qua
day - khoang 99% so dong se la lap lai.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.common.bronze import add_bronze_columns, write_bronze
from ingestion.common.config import source_config
from ingestion.common.logging import get_logger
from ingestion.common.metadata import IngestionMetadata, new_metadata
from ingestion.common.paths import landing_dir, run_id, today_str, utc_now
from ingestion.common.spark_session import build_spark_session
from ingestion.common.validation import (
    IngestionValidationError,
    check_landing_not_empty,
    check_raw_file,
    check_response_ok,
)

SOURCE = "sg_nea"
log = get_logger(SOURCE)

RUN_ID_FORMAT = "%Y%m%dT%H%M%SZ"

BRONZE_SCHEMA = StructType(
    [
        # Dinh danh cua LAN FETCH, khong phai cua cum dich - xem build_rows().
        StructField("batch_id", StringType(), nullable=False),
        StructField("fetched_at", TimestampType(), nullable=False),
        # Truong cua NEA, giu sat nguon. OBJECTID CO Y khong dung lam khoa:
        # no bi danh so lai moi lan NEA publish (da quan sat 527703 -> 528001
        # cho mot cum ma noi dung khong he thay doi).
        StructField("object_id", StringType(), nullable=True),
        StructField("locality", StringType(), nullable=True),
        StructField("case_count", IntegerType(), nullable=True),
        StructField("cluster_updated_at_raw", StringType(), nullable=True),
        StructField("inc_crc", StringType(), nullable=True),
        StructField("polygon_geojson", StringType(), nullable=True),
        # Feature goc day du, de khong mat gi da bi bo o tren.
        StructField("raw_payload", StringType(), nullable=False),
        # Lineage: nguon nay dung createDataFrame nen Spark khong tu biet file
        # goc, phai tu dien (xem add_bronze_columns(with_input_file=False)).
        StructField("_source_file", StringType(), nullable=False),
    ]
)


def poll_slot_id(moment: datetime, slot_minutes: int = 60) -> str:
    """Tra ve dinh danh cua khe poll ma mot moc thoi gian roi vao.

    Cat ve khe la cach lam job idempotent o muc mot lan poll: hai lan chay
    trong cung mot khe sinh ra cung `batch_id` nen thay the nhau, con mot lan
    poll that su muon hon thi duoc khe moi du payload co giong het.

    Args:
        moment: Thoi diem can phan loai, ky vong theo UTC.
        slot_minutes: Do rong mot khe, tinh bang phut.

    Returns:
        Dinh danh khe, vi du "20260925T0600Z".
    """
    minutes_into_day = moment.hour * 60 + moment.minute
    slot_start = minutes_into_day - (minutes_into_day % slot_minutes)
    return f"{moment:%Y%m%d}T{slot_start // 60:02d}{slot_start % 60:02d}Z"


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
def _get_json(url: str, timeout: int) -> dict[str, Any]:
    """GET mot URL va parse JSON, tu thu lai khi loi tam thoi.

    Retry boc o ham cap thap nay chu khong boc `fetch_clusters`, de mot lan
    loi o request thu hai khong bat phai lam lai ca request thu nhat.

    Args:
        url: URL day du can goi.
        timeout: Timeout tinh bang giay.

    Returns:
        Body JSON da parse.

    Raises:
        IngestionValidationError: Neu server tra 4xx/5xx hoac body rong.
    """
    response = requests.get(url, timeout=timeout)
    check_response_ok(response, SOURCE)
    return response.json()


def fetch_clusters(cfg: dict) -> dict[str, Any]:
    """Lay GeoJSON cum dich hien tai tu data.gov.sg.

    Args:
        cfg: Config nguon tu configs/sources.yaml.

    Returns:
        Dict GeoJSON FeatureCollection.

    Raises:
        RuntimeError: Neu envelope cua poll-download bao `code` khac 0.
            data.gov.sg bao loi muc ung dung kieu nay ngay ca khi HTTP van la
            200, nen chi nhin status thoi thi khong du.
    """
    url = cfg["url"].format(dataset_id=cfg["dataset_id"])
    envelope = _get_json(url, cfg["timeout_seconds"])
    if envelope.get("code") != 0:
        raise RuntimeError(
            f"data.gov.sg poll-download that bai: code={envelope.get('code')!r} "
            f"errorMsg={envelope.get('errorMsg')!r}"
        )
    return _get_json(envelope["data"]["url"], cfg["timeout_seconds"])


def validate_snapshot(geojson: dict[str, Any]) -> None:
    """Tu choi payload sai cau truc truoc khi ghi bat cu thu gi.

    Chi loi CAU TRUC moi raise o day - payload khong phai FeatureCollection,
    hoac khong co feature nao, nghia la lan fetch da hong (URL het han, trang
    loi, nguon sap) va ghi no xuong se ghi lai mot trang thai "Singapore khong
    co cum dich nao" khong co that.

    Nhung bat thuong BEN TRONG mot feature thi co y khong chan: viec cua
    Bronze la ghi lai dung nhung gi nguon noi, va `raw_payload` van giu ban
    goc de soi lai. Lam sach la viec cua Silver.

    Args:
        geojson: Payload tra ve tu fetch_clusters().

    Raises:
        IngestionValidationError: Neu khong phai FeatureCollection co feature.
    """
    if geojson.get("type") != "FeatureCollection":
        raise IngestionValidationError(
            f"[{SOURCE}] doi FeatureCollection, nhan duoc {geojson.get('type')!r}"
        )
    if not geojson.get("features"):
        raise IngestionValidationError(
            f"[{SOURCE}] FeatureCollection khong co feature nao - tu choi ghi"
        )


def feature_to_row(
    feature: dict[str, Any],
    batch_id: str,
    fetched_at: datetime,
    source_file: str = "",
) -> dict[str, Any]:
    """Lam phang mot GeoJSON feature thanh mot dong Bronze.

    Args:
        feature: Mot phan tu trong danh sach `features` cua FeatureCollection.
        batch_id: Dinh danh khe poll, dung chung cho moi dong cua lan fetch nay.
        fetched_at: Gio fetch thuc te, giu canh batch_id de van truy duoc thoi
            diem chay that.
        source_file: Ten file raw trong landing ma dong nay den tu do.

    Returns:
        Dict khop BRONZE_SCHEMA.
    """
    properties = feature.get("properties", {})
    geometry = feature.get("geometry")
    locality = properties.get("LOCALITY")

    return {
        "batch_id": batch_id,
        "fetched_at": fetched_at,
        "object_id": str(properties["OBJECTID"])
        if properties.get("OBJECTID") is not None
        else None,
        # Nguon them khoang trang thua o cuoi locality khong dong deu; cat
        # khoang trang la du an toan de lam o day, khac voi viec nan lai gia tri.
        "locality": locality.strip() if locality else None,
        "case_count": properties.get("CASE_SIZE"),
        "cluster_updated_at_raw": properties.get("FMEL_UPD_D"),
        "inc_crc": properties.get("INC_CRC"),
        "polygon_geojson": json.dumps(geometry, sort_keys=True) if geometry else None,
        "raw_payload": json.dumps(feature, sort_keys=True, ensure_ascii=False),
        "_source_file": source_file,
    }


def build_rows(
    geojson: dict[str, Any],
    fetched_at: datetime | None = None,
    source_file: str = "",
    slot_minutes: int = 60,
) -> list[dict[str, Any]]:
    """Chuyen mot FeatureCollection da fetch thanh danh sach dong Bronze.

    Args:
        geojson: FeatureCollection tra ve tu fetch_clusters().
        fetched_at: Ghi de gio fetch; mac dinh la bay gio (UTC). Chu yeu la
            diem tiem cho test.
        source_file: Ten file raw trong landing ma cac dong nay den tu do.
        slot_minutes: Do rong khe poll, dung tinh batch_id.

    Returns:
        Moi feature mot dong, tat ca dung chung mot `batch_id`.
    """
    fetched_at = fetched_at or datetime.now(timezone.utc).replace(tzinfo=None)
    batch_id = poll_slot_id(fetched_at, slot_minutes)
    return [
        feature_to_row(feature, batch_id, fetched_at, source_file)
        for feature in geojson.get("features", [])
    ]


def newest_snapshot_per_slot(
    raw_files: list[Path], slot_minutes: int
) -> list[tuple[datetime, Path]]:
    """Moi khe poll chi giu lai snapshot MOI NHAT cua khe do.

    Day la cho hien thuc hoa y nghia cua `poll_slot_id`: hai lan fetch roi vao
    cung mot khe la hai lan quan sat CUNG mot trang thai, nen lan sau thay the
    lan truoc chu khong cong don. Neu giu ca hai thi moi thong ke sau nay deu
    bi dem gap doi - EDA se bao 22 cum va 234 ca trong khi Singapore that su
    chi co 11 cum va 117 ca.

    Fetch o khe khac thi van giu rieng, vi do la quan sat that su muon hon.

    Args:
        raw_files: Cac file raw trong landing cua ngay, ten dang
            clusters_<run_id>.json.
        slot_minutes: Do rong mot khe poll.

    Returns:
        Danh sach (fetched_at, path), moi khe mot phan tu, sap theo thoi gian.
    """
    newest: dict[str, tuple[datetime, Path]] = {}
    for raw_path in raw_files:
        stamp = raw_path.stem.removeprefix("clusters_")
        fetched_at = datetime.strptime(stamp, RUN_ID_FORMAT)
        slot = poll_slot_id(fetched_at, slot_minutes)
        if slot not in newest or fetched_at > newest[slot][0]:
            newest[slot] = (fetched_at, raw_path)
    return sorted(newest.values())


def fetch_raw(
    cfg: dict, ingestion_date: str, current_run_id: str, meta: IngestionMetadata
) -> Path:
    """Buoc 1: goi API va luu GeoJSON goc xuong landing.

    Truoc day nguon nay ghi thang tu bo nho vao Delta, khong giu ban raw nao.
    Gio luu xuong landing giong hai nguon kia, de ban goc van con doi chieu duoc.

    Args:
        cfg: Config nguon tu configs/sources.yaml.
        ingestion_date: Ngay phan vung dang YYYY-MM-DD.
        current_run_id: Ma lan chay, dung dat ten file.
        meta: Ban ghi metadata cua lan chay, duoc cap nhat tai cho.

    Returns:
        Duong dan file .json vua ghi.
    """
    geojson = fetch_clusters(cfg)
    validate_snapshot(geojson)

    dest = landing_dir(SOURCE, ingestion_date)
    raw_path = dest / f"clusters_{current_run_id}.json"
    raw_path.write_text(json.dumps(geojson, ensure_ascii=False), encoding="utf-8")

    check_raw_file(raw_path, SOURCE)
    meta.add_raw_file(raw_path)
    log.info(
        "Lay duoc %d cum dich, luu vao %s",
        len(geojson["features"]),
        raw_path.name,
    )
    return raw_path


def load_to_bronze(spark, cfg: dict, ingestion_date: str) -> int:
    """Buoc 2: doc moi snapshot cua ngay trong landing va ghi vao Bronze.

    Dung createDataFrame voi schema khai bao san thay vi spark.read.json:
    GeoJSON la mot object long nhau, va giu schema tuong minh o day cho phep
    `case_count` la so nguyen va `fetched_at` la timestamp thay vi string.
    Volume chi vai chuc dong moi ngay nen khong co van de ve hieu nang.

    Doc lai TOAN BO snapshot cua ngay chu khong chi file vua tai: phan vung
    cua ngay phai duoc dung lai day du moi lan chay thi `replaceWhere` moi
    idempotent. Xem ingestion/common/bronze.py.

    Args:
        spark: SparkSession dang hoat dong.
        cfg: Config nguon tu configs/sources.yaml.
        ingestion_date: Ngay phan vung dang YYYY-MM-DD.

    Returns:
        So dong da ghi.

    Raises:
        IngestionValidationError: Neu landing rong hoac khong dung duoc dong nao.
    """
    landing = landing_dir(SOURCE, ingestion_date)
    raw_files = check_landing_not_empty(landing, "clusters_*.json", SOURCE)
    snapshots = newest_snapshot_per_slot(raw_files, cfg["poll_slot_minutes"])

    rows: list[dict[str, Any]] = []
    for fetched_at, raw_path in snapshots:
        geojson = json.loads(raw_path.read_text(encoding="utf-8"))
        rows.extend(
            build_rows(
                geojson,
                fetched_at=fetched_at,
                source_file=raw_path.name,
                slot_minutes=cfg["poll_slot_minutes"],
            )
        )

    if not rows:
        raise IngestionValidationError(
            f"[{SOURCE}] doc {len(raw_files)} file raw nhung khong dung duoc dong nao"
        )

    # coalesce(1): nguon nay chi vai chuc dong. Mac dinh Spark chia theo so
    # core (local[*] = 8 tren may nay), tuc la ghi 11 dong ra 8 file ti hon va
    # spawn 8 Python worker cung luc - tren Windows co worker khong kip
    # connect back, job chet voi "Python worker failed to connect back".
    frame = spark.createDataFrame(rows, schema=BRONZE_SCHEMA).coalesce(1)
    # with_input_file=False: DataFrame nay tao tu bo nho nen input_file_name()
    # se tra ve chuoi rong; cot _source_file da duoc dien trong feature_to_row.
    enriched = add_bronze_columns(
        frame, SOURCE, ingestion_date, utc_now(), with_input_file=False
    )
    target = write_bronze(enriched, SOURCE, ingestion_date)

    log.info(
        "Da ghi %s dong tu %d snapshot vao %s", f"{len(rows):,}", len(raw_files), target
    )
    return len(rows)


def ingest(spark=None) -> IngestionMetadata:
    """Chay mot lan ingestion day du cho Singapore NEA.

    Args:
        spark: SparkSession de dung lai. Neu khong truyen, ham tu tao va tu
            dong lai - cach scheduler goi.

    Returns:
        Ban ghi metadata cua lan chay, da duoc ghi ra dia.
    """
    cfg = source_config(SOURCE)
    ingestion_date = today_str()
    current_run_id = run_id()
    meta = new_metadata(
        cfg,
        ingestion_date,
        current_run_id,
        cfg["url"].format(dataset_id=cfg["dataset_id"]),
    )

    log.info("=== Bat dau ingestion | ingestion_date=%s ===", ingestion_date)
    owns_session = spark is None
    try:
        fetch_raw(cfg, ingestion_date, current_run_id, meta)
        spark = spark or build_spark_session(f"{SOURCE}_ingest")
        meta.record_count = load_to_bronze(spark, cfg, ingestion_date)
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
