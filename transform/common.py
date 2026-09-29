"""Duong dan, doc/ghi bang va bao cao chat luong dung chung cho Silver + Gold.

Bo cuc tren dia (ngang hang voi data/bronze/ cua tang ingestion):

    data/silver/<bang>/          <- bang Delta Silver
    data/gold/<bang>/            <- bang Delta Gold (star schema + mart)
    data/quality/quality_<run_id>.json|.md   <- bao cao chat luong moi lan chay
    data/reference/              <- du lieu tham chieu tai 1 lan (ranh gioi tinh)

Silver/Gold ghi de TOAN BANG moi lan chay: du lieu nho, dung lai toan bo tu
Bronze la cach idempotent don gian nhat - khong can watermark hay merge.
"""

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingestion.common import paths

DATA_ROOT = paths.PROJECT_ROOT / "data"
SILVER_ROOT = DATA_ROOT / "silver"
GOLD_ROOT = DATA_ROOT / "gold"
QUALITY_ROOT = DATA_ROOT / "quality"
REFERENCE_ROOT = DATA_ROOT / "reference"

SEVERITY_ERROR = "ERROR"
SEVERITY_WARN = "WARN"
SEVERITY_INFO = "INFO"
STATUS_PASS = "PASS"


def silver_path(table: str) -> Path:
    """Duong dan bang Delta Silver.

    Args:
        table: Ten bang, vi du "dengue_cases".

    Returns:
        Duong dan thu muc bang.
    """
    return SILVER_ROOT / table


def gold_path(table: str) -> Path:
    """Duong dan bang Delta Gold.

    Args:
        table: Ten bang, vi du "fact_monthly_cases".

    Returns:
        Duong dan thu muc bang.
    """
    return GOLD_ROOT / table


def read_delta(spark: SparkSession, path: Path) -> DataFrame:
    """Doc mot bang Delta.

    Args:
        spark: SparkSession dang chay.
        path: Thu muc bang.

    Returns:
        DataFrame cua bang.
    """
    return spark.read.format("delta").load(str(path))


def read_bronze_all(spark: SparkSession, source: str) -> DataFrame:
    """Doc MOI partition cua mot bang Bronze.

    Args:
        spark: SparkSession dang chay.
        source: Ten nguon (opendengue / news_rss / who_gho).

    Returns:
        DataFrame gom moi partition ingestion_date.
    """
    return read_delta(spark, paths.bronze_path(source))


def latest_partition(frame: DataFrame) -> str:
    """Tra ve gia tri ingestion_date moi nhat trong bang Bronze.

    Args:
        frame: Bang Bronze da doc.

    Returns:
        Chuoi ngay YYYY-MM-DD.

    Raises:
        ValueError: Neu bang rong.
    """
    value = frame.agg(F.max(paths.PARTITION_COLUMN)).first()[0]
    if value is None:
        raise ValueError("bang Bronze rong, khong co partition nao")
    return value


def read_bronze_latest(spark: SparkSession, source: str) -> tuple[DataFrame, str]:
    """Doc partition MOI NHAT cua mot bang Bronze.

    Dung cho nguon ma moi partition la mot snapshot day du (OpenDengue, WHO):
    doc gop moi partition se nhan ban du lieu theo so ngay da chay.

    Args:
        spark: SparkSession dang chay.
        source: Ten nguon.

    Returns:
        Bo (DataFrame chi gom partition moi nhat, ngay cua partition do).
    """
    frame = read_bronze_all(spark, source)
    day = latest_partition(frame)
    return frame.where(F.col(paths.PARTITION_COLUMN) == day), day


def small_frame(spark: SparkSession, rows: list, schema) -> DataFrame:
    """Tao DataFrame nho tu list Python, gom trong MOT partition.

    createDataFrame(list) mac dinh chia theo defaultParallelism (20 tren may
    dev), moi phan can mot Python worker; tren Windows moi lan bat worker ton
    ~0,8 s -> ~16 s cho moi action tren 5 dong (da do bang thi nghiem co lap).
    Mot partition -> ~1 s.

    Args:
        spark: SparkSession.
        rows: Danh sach tuple / Row.
        schema: StructType hoac chuoi DDL.

    Returns:
        DataFrame 1 partition.
    """
    return spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)


def write_table(frame: DataFrame, path: Path) -> int:
    """Ghi de toan bo bang Delta va tra ve so dong da ghi.

    Args:
        frame: DataFrame can ghi.
        path: Thu muc bang dich.

    Returns:
        So dong cua bang sau khi ghi.
    """
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(path))
    )
    return read_delta(frame.sparkSession, path).count()


@dataclass
class Check:
    """Mot kiem tra chat luong.

    Attributes:
        layer: bronze / silver / gold.
        table: Bang duoc kiem tra.
        name: Ten ngan cua kiem tra.
        severity: Muc do NEU that bai (ERROR / WARN / INFO).
        failed: So dong (hoac so doi tuong) vi pham.
        total: Tong so dong duoc xet, None neu khong ap dung.
        detail: Giai thich + vi du cu the.
        status: PASS neu failed == 0, nguoc lai bang severity.
    """

    layer: str
    table: str
    name: str
    severity: str
    failed: int
    total: int | None = None
    detail: str = ""
    status: str = field(init=False)

    def __post_init__(self) -> None:
        """Suy ra status tu so vi pham."""
        self.status = STATUS_PASS if self.failed == 0 else self.severity


class QualityReport:
    """Gom cac kiem tra cua mot lan chay va ghi ra JSON + Markdown."""

    def __init__(self, run_id: str) -> None:
        """Tao bao cao rong.

        Args:
            run_id: Ma lan chay, dung dat ten file.
        """
        self.run_id = run_id
        self.checks: list[Check] = []

    def add(
        self,
        layer: str,
        table: str,
        name: str,
        severity: str,
        failed: int,
        total: int | None = None,
        detail: str = "",
    ) -> Check:
        """Them mot kiem tra.

        Args:
            layer: bronze / silver / gold.
            table: Bang duoc kiem tra.
            name: Ten kiem tra.
            severity: Muc do neu that bai.
            failed: So vi pham.
            total: Tong so dong duoc xet.
            detail: Giai thich.

        Returns:
            Check vua them.
        """
        check = Check(layer, table, name, severity, int(failed), total, detail)
        self.checks.append(check)
        return check

    @property
    def has_errors(self) -> bool:
        """True neu co it nhat mot kiem tra o trang thai ERROR."""
        return any(check.status == SEVERITY_ERROR for check in self.checks)

    def counts(self) -> dict[str, int]:
        """Dem so kiem tra theo trang thai.

        Returns:
            Dict {status: so luong}.
        """
        result: dict[str, int] = {}
        for check in self.checks:
            result[check.status] = result.get(check.status, 0) + 1
        return result

    def write(self, directory: Path | None = None) -> tuple[Path, Path]:
        """Ghi bao cao ra JSON (may doc) va Markdown (nguoi doc).

        Args:
            directory: Thu muc dich, mac dinh data/quality/.

        Returns:
            Bo (duong dan JSON, duong dan Markdown).
        """
        target = directory or QUALITY_ROOT
        target.mkdir(parents=True, exist_ok=True)
        json_path = target / f"quality_{self.run_id}.json"
        md_path = target / f"quality_{self.run_id}.md"

        payload = {
            "run_id": self.run_id,
            "counts": self.counts(),
            "checks": [asdict(check) for check in self.checks],
        }
        json_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
        )

        order = {SEVERITY_ERROR: 0, SEVERITY_WARN: 1, SEVERITY_INFO: 2, STATUS_PASS: 3}
        lines = [
            f"# Bao cao chat luong du lieu - {self.run_id}",
            "",
            "Tong: " + ", ".join(f"{k} {v}" for k, v in sorted(self.counts().items())),
            "",
            "| Trang thai | Tang | Bang | Kiem tra | Vi pham | Tong | Chi tiet |",
            "|---|---|---|---|---|---|---|",
        ]
        for check in sorted(self.checks, key=lambda c: (order[c.status], c.layer, c.table)):
            detail = check.detail.replace("|", "\\|").replace("\n", " ")
            total = "" if check.total is None else f"{check.total:,}"
            lines.append(
                f"| {check.status} | {check.layer} | {check.table} | {check.name} "
                f"| {check.failed:,} | {total} | {detail} |"
            )
        md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return json_path, md_path
