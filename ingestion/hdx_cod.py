"""Du lieu tham chieu cap tinh tu HDX (UN OCHA Common Operational Datasets).

Hai nguon, ba bang Bronze:
    hdx_cod_ab           ten + P-code + ngay hieu luc + toa do tam cua don vi hanh
                         chinh (COD-AB, ban XLSX), 9/11 nuoc SEA (SGP, BRN khong co)
    hdx_cod_ab_geometry  polygon cap tinh - CHI cac nuoc trong `geometry_countries`
    hdx_cod_ps           dan so cap tinh (COD-PS), loc ve 11 nuoc SEA

Vi sao XLSX ma khong phai GeoJSON: goi GeoJSON chua moi cap hanh chinh o do phan
giai day du - PHL 1,06 GB, IDN 456 MB, THA 437 MB (do 29/9/2026). Ban XLSX
(0,04-14 MB) du P-code, ten ban xu (adm1_name1..3), valid_on/valid_to va toa do
tam - du de lam khoa, gazetteer va noi voi ranh gioi Natural Earth. Polygon COD
chi tai cho nuoc can that (VNM: 34 tinh moi tu 1/7/2025, de noi 63 tinh cu -> moi).

Pham vi: chi 11 nuoc SEA. File dan so toan cau duoc LOC NGAY O DAY (giong
OpenDengue): landing giu nguyen file goc, Bronze chi giu 11 nuoc.

Idempotency theo phien ban resource (last_modified cua HDX): cung phien ban da
nap -> bo qua, khong tai lai.
"""

import json
import sys
import zipfile
from pathlib import Path

from pyspark.sql import functions as F

from ingestion.common import hdx, http
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
from ingestion.common.validation import check_dataframe_readable, check_raw_file

SOURCE_AB = "hdx_cod_ab"
SOURCE_GEOMETRY = "hdx_cod_ab_geometry"
SOURCE_PS = "hdx_cod_ps"
VERSION_COLUMN = "_version"
log = get_logger("hdx_cod")


def _download_once(resource: dict, dest: Path, cfg: dict, meta: IngestionMetadata) -> Path:
    """Tai resource ve landing neu chua co (thu muc da mang phien ban).

    Args:
        resource: Resource HDX.
        dest: File dich.
        cfg: Config nguon.
        meta: Metadata lan chay.

    Returns:
        Duong dan file.
    """
    if not dest.exists():
        log.info("Dang tai %s", resource["name"])
        http.download(resource["url"], dest, cfg["retries"], cfg["timeout_seconds"])
    check_raw_file(dest, meta.source)
    meta.add_raw_file(dest)
    return dest


# ------------------------------------------------------------------ COD-AB --


def _load_ab_country(spark, cfg: dict, iso3: str, meta: IngestionMetadata) -> int | None:
    """Nap COD-AB (XLSX, va polygon neu can) cua mot nuoc.

    Args:
        spark: SparkSession.
        cfg: Config nguon hdx_cod_ab.
        iso3: Ma nuoc.
        meta: Metadata lan chay.

    Returns:
        So dong ghi vao hdx_cod_ab; 0 neu chi nap lai polygon; None neu bo qua
        vi moi bang da co phien ban nay.
    """
    package = hdx.package_show(f"cod-ab-{iso3.lower()}", cfg)
    xlsx = hdx.find_resource(package, r"\.xlsx$")
    version = hdx.version_of(xlsx)
    same = {"iso3": iso3, VERSION_COLUMN: version}
    ab_done = ingested_rows(spark, SOURCE_AB, **same) > 0
    # Polygon ghi SAU bang don vi: kiem tra rieng, de lan tai polygon loi duoc thu lai.
    geometry_done = (iso3 not in cfg.get("geometry_countries", [])
                     or ingested_rows(spark, SOURCE_GEOMETRY, **same) > 0)
    if ab_done and geometry_done:
        meta.warnings.append(f"{iso3}: COD-AB {version} da nap - bo qua")
        return None
    if ab_done:
        _load_geometry(spark, cfg, iso3, package, version, meta)
        return 0

    dest = landing_dir(SOURCE_AB, f"{iso3}/{version}")
    xlsx_path = _download_once(xlsx, dest / xlsx["name"], cfg, meta)
    levels = cfg["admin_levels"].get(iso3, cfg["admin_levels"]["default"])

    frames = []
    for level in levels:
        csv_path = dest / f"admin{level}.csv"
        sheet_to_csv(xlsx_path, f"{iso3.lower()}_admin{level}", csv_path)
        frames.append(read_csv_strings(spark, csv_path).withColumn("_sheet", F.lit(f"admin{level}")))
    points_csv = dest / "adminpoints.csv"
    sheet_to_csv(xlsx_path, f"{iso3.lower()}_adminpoints", points_csv)
    # Chi giu diem cua cac cap dang nap (chon pham vi, khong sua gia tri).
    frames.append(
        read_csv_strings(spark, points_csv)
        .where(F.col("admin_level").isin(*[str(level) for level in levels]))
        .withColumn("_sheet", F.lit("adminpoints"))
    )
    frame = frames[0]
    for other in frames[1:]:
        frame = frame.unionByName(other, allowMissingColumns=True)
    count = check_dataframe_readable(frame, SOURCE_AB)

    enriched = (
        add_bronze_columns(frame, SOURCE_AB, meta.ingestion_date, utc_now())
        .withColumn(VERSION_COLUMN, F.lit(version))
        .withColumn("iso3", F.lit(iso3))
    )
    write_bronze(enriched, SOURCE_AB, iso3, partition_column="iso3")

    if not geometry_done:
        _load_geometry(spark, cfg, iso3, package, version, meta)
    log.info("%s: COD-AB %s, %s dong", iso3, version, f"{count:,}")
    return count


def _load_geometry(spark, cfg: dict, iso3: str, package: dict, version: str,
                   meta: IngestionMetadata) -> None:
    """Nap polygon cap 1 cua mot nuoc (GeoJSON trong zip) vao hdx_cod_ab_geometry.

    Moi feature mot dong: P-code, ten, geometry (chuoi GeoJSON) + raw_payload.

    Args:
        spark: SparkSession.
        cfg: Config nguon.
        iso3: Ma nuoc.
        package: Ket qua package_show.
        version: Phien ban cua bo COD-AB.
        meta: Metadata lan chay.
    """
    resource = hdx.find_resource(package, r"geojson")
    dest = landing_dir(SOURCE_AB, f"{iso3}/{version}")
    zip_path = _download_once(resource, dest / resource["name"], cfg, meta)
    with zipfile.ZipFile(zip_path) as archive:
        name = next(n for n in archive.namelist() if n.lower().endswith("admin1.geojson"))
        collection = json.loads(archive.read(name).decode("utf-8"))

    jsonl = dest / "admin1_geometry.jsonl"
    with open(jsonl, "w", encoding="utf-8") as handle:
        for feature in collection["features"]:
            props = feature["properties"]
            handle.write(json.dumps({
                "adm1_pcode": str(props.get("adm1_pcode", "")),
                "adm1_name": str(props.get("adm1_name", "")),
                "valid_on": str(props.get("valid_on", "")),
                "geometry": json.dumps(feature["geometry"]),
                "raw_payload": json.dumps(props, ensure_ascii=False),
            }, ensure_ascii=False) + "\n")
    frame = spark.read.option("primitivesAsString", True).json(str(jsonl))
    enriched = (
        add_bronze_columns(frame, SOURCE_GEOMETRY, meta.ingestion_date, utc_now())
        .withColumn(VERSION_COLUMN, F.lit(version))
        .withColumn("iso3", F.lit(iso3))
    )
    write_bronze(enriched, SOURCE_GEOMETRY, iso3, partition_column="iso3")
    log.info("%s: %d polygon cap 1", iso3, len(collection["features"]))


def ingest_ab(spark=None) -> IngestionMetadata:
    """Nap COD-AB cho moi nuoc trong config. Mot nuoc loi khong lam hong nuoc khac.

    Args:
        spark: SparkSession dung lai; khong truyen thi tu tao va tu dong.

    Returns:
        Metadata lan chay.
    """
    cfg = source_config(SOURCE_AB)
    meta = new_metadata(cfg, today_str(), run_id(), cfg["url"])
    owns_session = spark is None
    try:
        spark = spark or build_spark_session(f"{SOURCE_AB}_ingest")
        total, failed, skipped = 0, [], []
        for iso3 in cfg["countries"]:
            try:
                count = _load_ab_country(spark, cfg, iso3, meta)
                if count is None:
                    skipped.append(iso3)
                total += count or 0
            except Exception as error:  # noqa: BLE001 - co lap loi theo nuoc
                failed.append(iso3)
                meta.warnings.append(f"{iso3} loi: {type(error).__name__}: {error}")
                log.exception("COD-AB %s loi", iso3)
        if len(failed) == len(cfg["countries"]):
            raise RuntimeError(f"moi nuoc deu loi: {failed}")
        meta.record_count = total
        if len(skipped) == len(cfg["countries"]):
            meta.skip("moi nuoc deu da nap dung phien ban", ingested_rows(spark, SOURCE_AB))
        return meta
    except Exception as error:
        meta.fail(error)
        raise
    finally:
        write_or_log(meta, log)
        if owns_session and spark is not None:
            spark.stop()


# ------------------------------------------------------------------ COD-PS --


def _load_ps_resource(spark, cfg: dict, item: dict, meta: IngestionMetadata) -> int:
    """Nap mot file dan so (loc 11 nuoc) vao hdx_cod_ps, partition theo ten file.

    Args:
        spark: SparkSession.
        cfg: Config nguon hdx_cod_ps.
        item: Phan tu `resources` trong config (package, pattern, key, admin_level).
        meta: Metadata lan chay.

    Returns:
        So dong ghi (0 neu bo qua).
    """
    package = hdx.package_show(item["package"], cfg)
    # Nhieu nam (vd phl_admpop_adm2_2022..2025) -> lay ten lon nhat = nam moi nhat.
    resource = max(hdx.find_resources(package, item["pattern"]), key=lambda r: r["name"])
    version = hdx.version_of(resource)
    if ingested_rows(spark, SOURCE_PS, _resource=item["key"], **{VERSION_COLUMN: version}):
        meta.warnings.append(f"{item['key']}: {version} da nap - bo qua")
        return 0

    dest = landing_dir(SOURCE_PS, f"{item['key']}/{version}")
    path = _download_once(resource, dest / resource["name"], cfg, meta)
    frame = read_csv_strings(spark, path)
    if item.get("filter_to_sea"):
        # Chon pham vi thu thap: file toan cau -> chi giu 11 nuoc SEA.
        frame = frame.where(F.col("ISO3").isin(*cfg["filter_iso3"]))
    count = check_dataframe_readable(frame, SOURCE_PS)
    enriched = (
        add_bronze_columns(frame, SOURCE_PS, meta.ingestion_date, utc_now())
        .withColumn(VERSION_COLUMN, F.lit(version))
        .withColumn("_source_resource", F.lit(resource["name"]))
        .withColumn("_admin_level", F.lit(str(item["admin_level"])))
        .withColumn("_resource", F.lit(item["key"]))
    )
    write_bronze(enriched, SOURCE_PS, item["key"], partition_column="_resource")
    log.info("%s: %s dong (%s)", item["key"], f"{count:,}", resource["name"])
    return count


def ingest_ps(spark=None) -> IngestionMetadata:
    """Nap cac file dan so COD-PS trong config.

    Args:
        spark: SparkSession dung lai; khong truyen thi tu tao va tu dong.

    Returns:
        Metadata lan chay.
    """
    cfg = source_config(SOURCE_PS)
    meta = new_metadata(cfg, today_str(), run_id(), cfg["url"])
    owns_session = spark is None
    try:
        spark = spark or build_spark_session(f"{SOURCE_PS}_ingest")
        meta.record_count = sum(_load_ps_resource(spark, cfg, item, meta)
                                for item in cfg["resources"])
        if meta.record_count == 0:
            meta.skip("moi file dan so da nap dung phien ban", ingested_rows(spark, SOURCE_PS))
        return meta
    except Exception as error:
        meta.fail(error)
        raise
    finally:
        write_or_log(meta, log)
        if owns_session and spark is not None:
            spark.stop()


if __name__ == "__main__":
    ingest_ab()
    sys.exit(0 if ingest_ps().status != "failed" else 1)
