"""Nguon MVP 1: OpenDengue - so ca sot xuat huyet, ca cap quoc gia lan cap tinh.

Pipeline 2 buoc:
    Buoc 1 (Python thuan): hoi GitHub API ban phat hanh moi nhat -> tai zip ->
                           giai nen CSV -> data/landing/opendengue/<release>/
    Buoc 2 (PySpark):      doc CSV -> loc pham vi SEA -> ghi Delta vao data/bronze/

Vi sao tach 2 buoc: Spark KHONG goi duoc API hay tai duoc file tu Internet,
no chi doc duoc file co san tren dia. Nen phai dung requests tai ve truoc.

Idempotency THEO BAN PHAT HANH, khong theo ngay chay. OpenDengue khong phai live
data: ho phat hanh theo version (V1.2, V1.3...), vai thang mot lan. Truoc day moi
ngay tai lai file ~55MB va ghi them mot ban sao 70.557 dong giong het hom truoc
(~557MB landing/ngay). Nay moi lan chay:
    - hoi GitHub API thu muc data/releases -> chon version lon nhat
    - release + sha file giong lan da nap -> BO QUA, khong tai, khong ghi
    - release moi (vd V1.4)               -> them partition moi, V1.3 giu nguyen
    - cung release nhung sha khac         -> OpenDengue sua file -> nap lai partition do
"Lan truoc da nap gi" doc nguoc tu chinh bang Delta (cot _file_sha), khong luu
file trang thai rieng: du lieu va trang thai nam cung mot commit, khong the lech.

Dung SPATIAL extract (khong phai National): National chi co cap quoc gia. Loc
theo pham vi du an (11 nuoc SEA) NGAY O DAY - ngoai le co chu dich cua nguyen tac
"Bronze khong loc", xem configs/sources.yaml (filter_countries). Landing van giu
100% file goc chua loc.
"""

import hashlib
import re
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import functions as F

from ingestion.common import http
from ingestion.common.bronze import (
    add_bronze_columns,
    check_existing_table,
    ingested_rows,
    read_csv_strings,
    write_bronze,
)
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
)

SOURCE = "opendengue"
log = get_logger(SOURCE)

RELEASE_COLUMN = "release"
SHA_COLUMN = "_file_sha"
GITHUB_HEADERS = {"Accept": "application/vnd.github+json"}
# Ten thu muc release hop le: V1, V1.3, V1.2.2, V1.10 (bo qua .DS_Store...).
RELEASE_RE = re.compile(r"^V(\d+(?:\.\d+)*)$")


def parse_release(name: str) -> tuple[int, ...] | None:
    """Doi ten release thanh tuple so de so sanh dung thu tu.

    So sanh chuoi se sai: "V1.10" < "V1.2" theo thu tu chu cai.

    Args:
        name: Ten thu muc, vd "V1.2.2".

    Returns:
        (1, 2, 2), hoac None neu khong phai ten release.
    """
    match = RELEASE_RE.match(name)
    return tuple(int(p) for p in match.group(1).split(".")) if match else None


def pick_latest_release(entries: list[dict[str, Any]]) -> str:
    """Chon release moi nhat tu noi dung thu muc data/releases (GitHub API).

    Args:
        entries: Danh sach muc cua `GET .../contents/data/releases`.

    Returns:
        Ten release, vd "V1.3".

    Raises:
        IngestionValidationError: Neu khong co thu muc release nao.
    """
    releases = [
        e["name"] for e in entries
        if e.get("type") == "dir" and parse_release(e.get("name", "")) is not None
    ]
    if not releases:
        raise IngestionValidationError(f"[{SOURCE}] khong tim thay release nao")
    return max(releases, key=parse_release)


def find_extract(entries: list[dict[str, Any]], extract: str) -> dict[str, Any]:
    """Tim file zip cua mot extract trong thu muc release.

    Args:
        entries: Noi dung thu muc release (GitHub API).
        extract: Ten extract, vd "Spatial" (file "Spatial_extract_V1_3.zip").

    Returns:
        Muc GitHub cua file (name, sha, size, download_url).

    Raises:
        IngestionValidationError: Neu release khong co file cua extract nay.
    """
    prefix = f"{extract.lower()}_extract"
    for entry in entries:
        name = entry.get("name", "")
        if name.lower().startswith(prefix) and name.lower().endswith(".zip"):
            return entry
    raise IngestionValidationError(
        f"[{SOURCE}] release khong co file {extract}_extract*.zip: "
        f"{[e.get('name') for e in entries]}"
    )


def git_blob_sha(path: Path) -> str:
    """sha kieu git ("blob <size>\\0" + noi dung) - cung gia tri GitHub API tra ve.

    Dung de xac nhan file da tai (hoac file cu trong landing) dung la file ma
    GitHub mo ta: file tai do dang hay bi hong se lech sha.

    Args:
        path: File can bam.

    Returns:
        Chuoi hex 40 ky tu.
    """
    digest = hashlib.sha1(f"blob {path.stat().st_size}\0".encode())
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_release(cfg: dict) -> tuple[str, dict[str, Any]]:
    """Hoi GitHub API: release moi nhat va file extract can nap.

    Args:
        cfg: Config nguon.

    Returns:
        Bo (ten release, muc GitHub cua file zip).
    """
    releases = http.get(cfg["releases_api"], cfg["retries"], cfg["timeout_seconds"],
                        headers=GITHUB_HEADERS).json()
    release = pick_latest_release(releases)
    entries = http.get(f"{cfg['releases_api']}/{release}", cfg["retries"],
                       cfg["timeout_seconds"], headers=GITHUB_HEADERS).json()
    return release, find_extract(entries, cfg["extract"])


def fetch_raw(cfg: dict, release: str, entry: dict[str, Any], meta: IngestionMetadata) -> Path:
    """Buoc 1: tai zip cua release ve landing (neu chua co dung file) va giai nen CSV.

    Args:
        cfg: Config nguon.
        release: Ten release.
        entry: Muc GitHub cua file zip.
        meta: Metadata lan chay.

    Returns:
        Duong dan file CSV da giai nen.

    Raises:
        IngestionValidationError: Neu file tai ve lech sha hoac rong.
    """
    dest = landing_dir(SOURCE, release)
    zip_path = dest / entry["name"]

    if zip_path.exists() and git_blob_sha(zip_path) == entry["sha"]:
        log.info("Da co %s dung sha trong landing, dung lai", zip_path.name)
    else:
        log.info("Dang tai %s (%s bytes)", entry["name"], f"{entry.get('size', 0):,}")
        http.download(entry["download_url"], zip_path, cfg["retries"], cfg["timeout_seconds"])
        if git_blob_sha(zip_path) != entry["sha"]:
            raise IngestionValidationError(
                f"[{SOURCE}] {zip_path.name} tai ve lech sha voi GitHub - file hong/do dang"
            )
    check_raw_file(zip_path, SOURCE)
    meta.add_raw_file(zip_path)

    with zipfile.ZipFile(zip_path) as archive:
        names = [n for n in archive.namelist() if n.lower().endswith(".csv")]
        if not names:
            raise IngestionValidationError(f"[{SOURCE}] khong co CSV trong {zip_path.name}")
        archive.extract(names[0], dest)

    csv_path = dest / names[0]
    check_raw_file(csv_path, SOURCE)
    log.info("Da giai nen ra %s", csv_path.name)
    return csv_path


def load_to_bronze(spark, cfg: dict, release: str, sha: str, zip_path: Path,
                   ingestion_date: str) -> int:
    """Buoc 2: Spark doc CSV cua release, loc pham vi SEA, ghi partition cua release.

    Args:
        spark: SparkSession.
        cfg: Config nguon.
        release: Ten release (gia tri partition).
        sha: git blob sha cua file zip.
        zip_path: File zip da tai (lay thoi diem tai lam _fetched_at).
        ingestion_date: Ngay chay, giu thanh cot thuong (khong con la partition).

    Returns:
        So dong da ghi (sau khi loc).
    """
    landing = landing_dir(SOURCE, release)
    check_landing_not_empty(landing, "*.csv", SOURCE)

    # Moi cot la string (Bronze khong ep kieu); multiLine vi co o chua xuong dong.
    frame = read_csv_strings(spark, landing / "*.csv")
    raw_count = check_dataframe_readable(frame, SOURCE)

    countries = cfg.get("filter_countries")
    if countries:
        frame = frame.filter(F.col("adm_0_name").isin(countries))
        count = check_dataframe_readable(frame, SOURCE)
        log.info("Da loc theo pham vi SEA (%d nuoc): %s -> %s dong",
                 len(countries), f"{raw_count:,}", f"{count:,}")
    else:
        count = raw_count

    fetched_at = datetime.fromtimestamp(zip_path.stat().st_mtime, timezone.utc).isoformat()
    enriched = (
        add_bronze_columns(frame, SOURCE, ingestion_date, utc_now())
        .withColumn(SHA_COLUMN, F.lit(sha))
        .withColumn("_fetched_at", F.lit(fetched_at))
        .withColumn(RELEASE_COLUMN, F.lit(release))
    )
    target = write_bronze(enriched, SOURCE, release, partition_column=RELEASE_COLUMN)
    log.info("Da ghi %s dong vao %s (partition %s=%s)", f"{count:,}", target,
             RELEASE_COLUMN, release)
    return count


def ingest(spark=None) -> IngestionMetadata:
    """Chay mot lan ingestion cho OpenDengue.

    Args:
        spark: SparkSession de dung lai. Neu khong truyen, ham tu tao va tu dong lai.

    Returns:
        Metadata lan chay (status skipped neu release + sha da nap).
    """
    cfg = source_config(SOURCE)
    ingestion_date = today_str()
    meta = new_metadata(cfg, ingestion_date, run_id(), cfg["releases_api"])

    log.info("=== Bat dau ingestion | ingestion_date=%s ===", ingestion_date)
    owns_session = spark is None
    try:
        release, entry = resolve_release(cfg)
        meta.source_version = release
        meta.source_url = entry["download_url"]
        log.info("Release moi nhat: %s, file %s, sha %s", release, entry["name"], entry["sha"][:10])

        spark = spark or build_spark_session(f"{SOURCE}_ingest")
        # Bang cu (phan vung theo ngay) -> bao loi ngay, truoc khi tai 55MB.
        check_existing_table(spark, SOURCE, RELEASE_COLUMN)
        existing = ingested_rows(spark, SOURCE, **{RELEASE_COLUMN: release, SHA_COLUMN: entry["sha"]})
        if existing:
            meta.skip(f"{release} (sha {entry['sha'][:10]}) da nap {existing:,} dong - bo qua",
                      existing)
            log.info("=== Bo qua: %s ===", meta.warnings[-1])
            return meta

        zip_path = landing_dir(SOURCE, release) / entry["name"]
        fetch_raw(cfg, release, entry, meta)
        meta.record_count = load_to_bronze(spark, cfg, release, entry["sha"], zip_path,
                                           ingestion_date)
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
