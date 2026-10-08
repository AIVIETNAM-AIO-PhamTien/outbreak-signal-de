"""Bang noi 64 don vi cap tinh CU cua Viet Nam -> 34 don vi MOI.

Vi sao can: tu 01/07/2025 Viet Nam sap nhap 63 tinh con 34 (NQ 202/2025/QH15).
Ba bang Bronze hien noi theo hai ky khac nhau:

    opendengue    so ca     63 tinh cu   ten chu, khong co P-code
    hdx_cod_ps    dan so    63 tinh cu   P-code 5 ky tu (VN101, VN605...)
    hdx_cod_ab    ranh gioi 34 tinh moi  P-code 4 ky tu (VN01, VN66...)

Hai he P-code GIAO NHAU BANG 0 - khong join truc tiep duoc. Nguy hiem hon:
ten trung nhau van la hai thu khac nhau ("Dak Lak" cu 1,93 trieu dan; "Dak Lak"
moi = Dak Lak + Phu Yen = 2,80 trieu dan), nen join theo ten chay duoc ma ra so
sai khoang 45% chu khong bao loi.

HUONG QUY DOI: luon CU -> MOI, khong bao gio nguoc lai. Sap nhap la phep gop
nhieu-thanh-mot: cong lai thi duoc, tach ra thi mat thong tin vinh vien.

Vi sao la seed file chu khong phai tai tu mang: day la du lieu PHAP LY co dinh
(nghi quyet Quoc hoi), khong co API nao phat hanh. HDX chi phat hanh ban hien
hanh - cod-ab-vnm co dung 34 dong, moi `valid_to` deu rong, khong luu lich su.

64 chu khong phai 63: OpenDengue con du lieu lich su cua Ha Tay (nhap vao Ha Noi
tu 2008 theo NQ 15/2008/QH12), nen bang nay phu ca hai dot sap nhap.

Bang nay duoc KIEM CHUNG NGUOC voi hdx_cod_ab moi lan chay (check_against_cod_ab):
seed sai hoac COD-AB doi phien ban deu lam job FAILED, khong am tham ghi bang lech.
"""

import shutil
import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ingestion.common.bronze import (
    add_bronze_columns,
    ingested_rows,
    read_csv_strings,
    write_bronze,
)
from ingestion.common.config import source_config
from ingestion.common.logging import get_logger
from ingestion.common.metadata import IngestionMetadata, new_metadata, sha256_of, write_or_log
from ingestion.common.paths import PROJECT_ROOT, bronze_path, landing_dir, run_id, today_str, utc_now
from ingestion.common.validation import (
    IngestionValidationError,
    check_dataframe_readable,
    check_raw_file,
)

SOURCE = "vn_province_crosswalk"
COD_AB_SOURCE = "hdx_cod_ab"
VERSION_COLUMN = "_version"
REQUIRED_COLUMNS = ("old_name", "old_pcode_ps", "new_name", "new_pcode",
                    "effective_from", "legal_basis", "source_aliases")
EXPECTED_NEW_UNITS = 34
log = get_logger(SOURCE)


def seed_path(cfg: dict) -> Path:
    """Duong dan tuyet doi cua seed CSV khai bao trong config.

    Args:
        cfg: Config nguon, khoa `seed_file` la duong dan tuong doi tu goc repo.

    Returns:
        Duong dan file seed.

    Raises:
        IngestionValidationError: Neu file khong ton tai.
    """
    path = PROJECT_ROOT / cfg["seed_file"]
    if not path.exists():
        raise IngestionValidationError(f"[{SOURCE}] khong tim thay seed file: {path}")
    return path


def check_shape(frame: DataFrame) -> None:
    """Kiem tra tinh toan ven cua chinh bang noi, truoc khi doi chieu nguon ngoai.

    Bat ba loi de mac nhat khi sua seed bang tay:
      - thieu cot bat buoc;
      - mot ten cu xuat hien hai lan (join se nhan ban so ca cua tinh do);
      - so don vi moi khong con dung 34.

    Args:
        frame: DataFrame doc tu seed CSV.

    Raises:
        IngestionValidationError: Neu vi pham bat ky dieu nao tren.
    """
    missing = [c for c in REQUIRED_COLUMNS if c not in frame.columns]
    if missing:
        raise IngestionValidationError(f"[{SOURCE}] seed thieu cot {missing}")

    total = frame.count()
    if frame.select("old_name").distinct().count() != total:
        duplicates = [r["old_name"] for r in
                      frame.groupBy("old_name").count().where(F.col("count") > 1).collect()]
        raise IngestionValidationError(
            f"[{SOURCE}] ten tinh cu bi lap: {sorted(duplicates)} - "
            f"join se nhan ban so ca cua cac tinh nay"
        )

    new_units = frame.select("new_pcode").distinct().count()
    if new_units != EXPECTED_NEW_UNITS:
        raise IngestionValidationError(
            f"[{SOURCE}] co {new_units} don vi moi, phai dung {EXPECTED_NEW_UNITS} "
            f"(NQ 202/2025/QH15)"
        )
    log.info("Seed hop le: %d don vi cu -> %d don vi moi", total, new_units)


def check_against_cod_ab(spark, frame: DataFrame, meta: IngestionMetadata) -> None:
    """Doi chieu cot new_pcode / new_name voi bang Bronze hdx_cod_ab that.

    Day la phep kiem tra quan trong nhat cua module: bang noi chi co gia tri khi
    dich den cua no ton tai THAT trong COD-AB. Neu HDX phat hanh phien ban moi
    doi P-code, job phai FAILED ngay thay vi ghi mot bang noi tro vao hu khong.

    Bang hdx_cod_ab chua ca sheet admin1 lan adminpoints va nhieu nuoc, nen chi
    lay dong cua Viet Nam o sheet admin1.

    Args:
        spark: SparkSession.
        frame: DataFrame seed.
        meta: Metadata lan chay, de ghi canh bao khi chua co COD-AB.

    Raises:
        IngestionValidationError: Neu co P-code moi khong ton tai trong COD-AB,
            hoac COD-AB co don vi khong duoc bang noi tro toi.
    """
    from delta.tables import DeltaTable

    target = str(bronze_path(COD_AB_SOURCE))
    if not DeltaTable.isDeltaTable(spark, target):
        meta.warnings.append(
            f"chua co bang {COD_AB_SOURCE} - bo qua buoc doi chieu P-code. "
            f"Chay `python scripts/run_batch.py --source {COD_AB_SOURCE}` roi chay lai."
        )
        log.warning("Chua co bang %s, khong doi chieu duoc P-code", COD_AB_SOURCE)
        return

    rows = (
        spark.read.format("delta").load(target)
        .where((F.col("iso3") == "VNM") & (F.col("_sheet") == "admin1"))
        .select("adm1_pcode", "adm1_name")
        .distinct()
        .collect()
    )
    actual = {r["adm1_pcode"]: r["adm1_name"] for r in rows}
    seed = {r["new_pcode"]: r["new_name"]
            for r in frame.select("new_pcode", "new_name").distinct().collect()}

    unknown = sorted(set(seed) - set(actual))
    if unknown:
        raise IngestionValidationError(
            f"[{SOURCE}] P-code moi khong co trong {COD_AB_SOURCE}: {unknown}. "
            f"COD-AB co the da doi phien ban - kiem tra lai seed."
        )
    uncovered = sorted(set(actual) - set(seed))
    if uncovered:
        raise IngestionValidationError(
            f"[{SOURCE}] {COD_AB_SOURCE} co don vi khong tinh cu nao tro toi: {uncovered}. "
            f"Bang noi phai phu het {EXPECTED_NEW_UNITS} don vi moi."
        )
    # Ten lech khong lam hong join (join bang P-code) nhung bao hieu seed da cu.
    renamed = sorted(f"{p}: seed={n!r}, COD-AB={actual[p]!r}"
                     for p, n in seed.items() if actual[p] != n)
    if renamed:
        meta.warnings.append(f"ten lech so voi {COD_AB_SOURCE}: {renamed}")
    log.info("Doi chieu %s: %d/%d P-code khop", COD_AB_SOURCE, len(seed), len(actual))


def ingest(spark=None) -> IngestionMetadata:
    """Nap bang noi 64 -> 34 vao Bronze, sau khi kiem chung voi hdx_cod_ab.

    Idempotency theo noi dung seed: SHA-256 cua file lam phien ban. Seed khong
    doi -> bo qua, khong ghi lai. Seed sua mot ky tu -> phien ban moi, nap lai.

    Args:
        spark: SparkSession dung lai; khong truyen thi tu tao va tu dong.

    Returns:
        Metadata lan chay.
    """
    from ingestion.common.spark_session import build_spark_session

    cfg = source_config(SOURCE)
    meta = new_metadata(cfg, today_str(), run_id(), cfg["seed_file"])
    owns_session = spark is None
    try:
        spark = spark or build_spark_session(f"{SOURCE}_ingest")
        source_file = seed_path(cfg)
        version = sha256_of(source_file)[:12]
        meta.source_version = version

        if ingested_rows(spark, SOURCE, **{VERSION_COLUMN: version}) > 0:
            meta.skip(f"seed {version} da nap - bo qua", ingested_rows(spark, SOURCE))
            return meta

        # Copy sang landing: landing la ban goc bat bien cua lan chay, ke ca khi
        # seed trong repo bi sua sau do. Cung giu duoc lineage _source_file.
        landing = landing_dir(SOURCE, version)
        raw = landing / source_file.name
        if not raw.exists():
            shutil.copy2(source_file, raw)
        check_raw_file(raw, SOURCE)
        meta.add_raw_file(raw)

        frame = read_csv_strings(spark, raw)
        count = check_dataframe_readable(frame, SOURCE)
        check_shape(frame)
        check_against_cod_ab(spark, frame, meta)

        enriched = (
            add_bronze_columns(frame, SOURCE, meta.ingestion_date, utc_now())
            .withColumn(VERSION_COLUMN, F.lit(version))
            .withColumn("iso3", F.lit("VNM"))
        )
        write_bronze(enriched, SOURCE, version, partition_column=VERSION_COLUMN)
        meta.record_count = count
        log.info("%s: %d dong, phien ban %s", SOURCE, count, version)
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
