"""Ranh gioi hanh chinh tu geoBoundaries - VA LAP cho nuoc HDX COD-AB khong co.

Vi sao can: `hdx_cod_ab` chi phu 9/11 nuoc SEA. Da kiem chung 08/10/2026, ca ba
ten dataset deu tra HTTP 404:

    cod-ab-brn, cod-ab-bru, cod-ab-brunei-darussalam

HDX co 84 dataset gan nhom quoc gia `brn` nhung khong cai nao la COD-AB. Day la
LO HONG THAT cua nguon, khong phai loi cau hinh - nen khong vay duoc bang cach
them "BRN" vao `countries` cua hdx_cod_ab: `_load_ab_country()` goi thang
`cod-ab-{iso3}`, se 404 moi lan chay va chi de lai mot canh bao im lang.

geoBoundaries (gbOpen, Dai hoc William & Mary) la bo ranh gioi mo phu toan cau.
Voi Brunei: 4 district cap ADM1, khop 4/4 voi `adm_1_name` cua OpenDengue sau khi
bo hau to "DISTRICT". License Public Domain.

VI SAO GHI BANG RIENG chu khong ghi chung vao hdx_cod_ab: do tin cay va quy trinh
bien tap cua hai nguon khac han. COD-AB duoc OCHA kiem dinh va gan P-code chuan
(`VN01`); geoBoundaries dung ma rieng (`shapeID` dang 89281809B69825435871977),
khong phai P-code OCHA. Tron chung mot bang se khien tang Silver tuong moi dong
deu co P-code chuan - dung kieu nham am tham ma du an nay da gap mot lan voi
chuyen 34/63 tinh. Silver union hai bang mot cach tuong minh, doc cot `_provider`
de biet dong nao la COD chinh thuc, dong nao la nguon va lap.

Pham vi: chi cac nuoc liet ke trong `countries` cua config - CO Y giu hep, day la
nguon va lap chu khong phai nguon chinh.
"""

import json
import sys

from pyspark.sql import functions as F

from ingestion.common import http
from ingestion.common.bronze import add_bronze_columns, ingested_rows, write_bronze
from ingestion.common.config import source_config
from ingestion.common.logging import get_logger
from ingestion.common.metadata import IngestionMetadata, new_metadata, write_or_log
from ingestion.common.paths import landing_dir, run_id, today_str, utc_now
from ingestion.common.validation import IngestionValidationError, check_raw_file

SOURCE = "geoboundaries_adm"
VERSION_COLUMN = "_version"
log = get_logger(SOURCE)


def metadata_url(cfg: dict, iso3: str, level: str) -> str:
    """URL metadata cua mot bo ranh gioi.

    Args:
        cfg: Config nguon (khoa `url` la mau co {release}, {iso3}, {level}).
        iso3: Ma nuoc, vd "BRN".
        level: Cap hanh chinh, vd "ADM1".

    Returns:
        URL day du.
    """
    return cfg["url"].format(release=cfg["release"], iso3=iso3.upper(), level=level.upper())


def version_of(meta_json: dict) -> str:
    """Phien ban cua bo ranh gioi, dang an toan lam ten thu muc.

    `boundaryID` (vd "BRN-ADM1-89281809") doi moi khi geoBoundaries dung lai bo
    du lieu, nen dung lam khoa idempotency - giong cach hdx.version_of() dung
    `last_modified`.

    Args:
        meta_json: Body JSON cua endpoint metadata.

    Returns:
        Chuoi phien ban.

    Raises:
        IngestionValidationError: Neu thieu boundaryID.
    """
    boundary_id = str(meta_json.get("boundaryID") or "").strip()
    if not boundary_id:
        raise IngestionValidationError(f"[{SOURCE}] metadata thieu boundaryID: {meta_json}")
    return boundary_id.replace("/", "_")


def features_to_jsonl(collection: dict, iso3: str, level: str, meta_json: dict, path) -> int:
    """Doi FeatureCollection thanh JSONL, moi don vi hanh chinh mot dong.

    Giu nguyen trang nguon theo dung quy tac Bronze: `raw_payload` chua toan bo
    properties goc, cac cot phang ben canh chi de query cho tien. Geometry luu
    duoi dang chuoi GeoJSON (Spark khong co kieu hinh hoc san).

    Args:
        collection: FeatureCollection da parse.
        iso3: Ma nuoc.
        level: Cap hanh chinh.
        meta_json: Metadata cua bo du lieu, de gan nguon va license vao tung dong.
        path: File JSONL dich.

    Returns:
        So feature da ghi.

    Raises:
        IngestionValidationError: Neu khong co feature nao.
    """
    features = collection.get("features") or []
    if not features:
        raise IngestionValidationError(f"[{SOURCE}] {iso3}/{level}: FeatureCollection rong")

    with open(path, "w", encoding="utf-8") as handle:
        for feature in features:
            props = feature.get("properties") or {}
            handle.write(json.dumps({
                "shape_id": str(props.get("shapeID", "")),
                "shape_name": str(props.get("shapeName", "")),
                "shape_iso": str(props.get("shapeISO", "")),
                "shape_group": str(props.get("shapeGroup", "")),
                "shape_type": str(props.get("shapeType", "")),
                "boundary_canonical": str(meta_json.get("boundaryCanonical", "")),
                "boundary_year": str(meta_json.get("boundaryYearRepresented", "")),
                "boundary_license": str(meta_json.get("boundaryLicense", "")),
                "geometry": json.dumps(feature.get("geometry")),
                "raw_payload": json.dumps(props, ensure_ascii=False),
            }, ensure_ascii=False) + "\n")
    return len(features)


def _load_country(spark, cfg: dict, iso3: str, level: str, meta: IngestionMetadata) -> int | None:
    """Nap mot cap hanh chinh cua mot nuoc.

    Args:
        spark: SparkSession.
        cfg: Config nguon.
        iso3: Ma nuoc.
        level: Cap hanh chinh, vd "ADM1".
        meta: Metadata lan chay.

    Returns:
        So don vi da ghi; None neu da nap dung phien ban nay roi.
    """
    body = http.get(metadata_url(cfg, iso3, level), cfg["retries"], cfg["timeout_seconds"]).json()
    version = version_of(body)
    partition = f"{iso3}_{level}"
    if ingested_rows(spark, SOURCE, _partition=partition, **{VERSION_COLUMN: version}) > 0:
        meta.warnings.append(f"{partition}: {version} da nap - bo qua")
        return None

    dest = landing_dir(SOURCE, f"{partition}/{version}")
    geojson = dest / f"{iso3}_{level}.geojson"
    if not geojson.exists():
        http.download(body["gjDownloadURL"], geojson, cfg["retries"], cfg["timeout_seconds"])
    check_raw_file(geojson, SOURCE)
    meta.add_raw_file(geojson)

    jsonl = dest / f"{iso3}_{level}.jsonl"
    count = features_to_jsonl(json.loads(geojson.read_text(encoding="utf-8")),
                              iso3, level, body, jsonl)

    expected = body.get("admUnitCount")
    if expected and str(expected).isdigit() and int(expected) != count:
        # Khong chan: so don vi la thong tin cua nguon, Bronze ghi dung cai nguon
        # tra ve. Nhung lech thi phai thay duoc trong metadata.
        meta.warnings.append(f"{partition}: metadata bao {expected} don vi, GeoJSON co {count}")

    frame = spark.read.option("primitivesAsString", True).json(str(jsonl))
    enriched = (
        add_bronze_columns(frame, SOURCE, meta.ingestion_date, utc_now())
        .withColumn(VERSION_COLUMN, F.lit(version))
        .withColumn("iso3", F.lit(iso3))
        .withColumn("admin_level", F.lit(level))
        .withColumn("_provider", F.lit("geoboundaries"))
        .withColumn("_partition", F.lit(partition))
    )
    write_bronze(enriched, SOURCE, partition, partition_column="_partition")
    log.info("%s %s: %d don vi, phien ban %s", iso3, level, count, version)
    return count


def ingest(spark=None) -> IngestionMetadata:
    """Nap ranh gioi va lap cho moi nuoc trong config. Mot nuoc loi khong keo do nuoc khac.

    Args:
        spark: SparkSession dung lai; khong truyen thi tu tao va tu dong.

    Returns:
        Metadata lan chay.
    """
    from ingestion.common.spark_session import build_spark_session

    cfg = source_config(SOURCE)
    meta = new_metadata(cfg, today_str(), run_id(), cfg["url"])
    owns_session = spark is None
    try:
        spark = spark or build_spark_session(f"{SOURCE}_ingest")
        targets = [(iso3, level)
                   for iso3, levels in cfg["countries"].items()
                   for level in levels]
        total, failed, skipped = 0, [], []
        for iso3, level in targets:
            try:
                count = _load_country(spark, cfg, iso3, level, meta)
                if count is None:
                    skipped.append(f"{iso3}_{level}")
                total += count or 0
            except Exception as error:  # noqa: BLE001 - co lap loi theo nuoc
                failed.append(f"{iso3}_{level}")
                meta.warnings.append(f"{iso3}_{level} loi: {type(error).__name__}: {error}")
                log.exception("geoBoundaries %s %s loi", iso3, level)
        if failed and len(failed) == len(targets):
            raise RuntimeError(f"moi muc tieu deu loi: {failed}")
        meta.record_count = total
        if len(skipped) == len(targets):
            meta.skip("moi muc tieu deu da nap dung phien ban", ingested_rows(spark, SOURCE))
        return meta
    except Exception as error:
        meta.fail(error)
        raise
    finally:
        write_or_log(meta, log)
        if owns_session and spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(0 if ingest().status != "failed" else 1)
