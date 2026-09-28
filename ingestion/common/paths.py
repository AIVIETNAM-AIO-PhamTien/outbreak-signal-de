"""Duong dan va moc thoi gian dung chung cho moi job ingestion.

Gom mot cho de ba nguon khong tu dat duong dan theo kieu rieng - do la ly do
truoc day co nguon ghi vao `ingest_date=...` con nguon khac ghi thang vao
`sg_dengue_clusters/` khong phan vung gi.

Bo cuc tren dia:

    data/landing/<nguon>/<ingestion_date>/<file raw>   <- ban goc y nguyen
    data/bronze/<nguon>/ingestion_date=<date>/         <- bang Delta
    data/metadata/<nguon>/ingestion_date=<date>/*.json <- metadata moi lan chay

`landing` la ban goc (source of truth), khong bao gio bi sua. `bronze` la ban
Spark doc duoc. Tach hai cai vi Bronze phai o dinh dang query duoc, con nguyen
tac "giu nguyen trang nguon" van phai co cho de thoa man.
"""

from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LANDING_ROOT = PROJECT_ROOT / "data" / "landing"
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"
METADATA_ROOT = PROJECT_ROOT / "data" / "metadata"
LOG_ROOT = PROJECT_ROOT / "logs"

# Ten cot phan vung cua moi bang Bronze. Viet day du "ingestion_date" chu
# khong viet tat "ingest_date" - doi ten sau khi Silver da doc la mot lan
# merge conflict khong can thiet.
PARTITION_COLUMN = "ingestion_date"


def utc_now() -> datetime:
    """Thoi diem hien tai theo UTC, co tzinfo."""
    return datetime.now(timezone.utc)


def today_str(moment: datetime | None = None) -> str:
    """Ngay ingestion dang YYYY-MM-DD theo UTC.

    Args:
        moment: Moc thoi gian can quy ve ngay. Mac dinh la bay gio.

    Returns:
        Chuoi ngay, vi du "2026-09-28".
    """
    return (moment or utc_now()).strftime("%Y-%m-%d")


def run_id(moment: datetime | None = None) -> str:
    """Ma dinh danh mot lan chay, dang YYYYMMDDTHHMMSSZ.

    Args:
        moment: Moc bat dau lan chay. Mac dinh la bay gio.

    Returns:
        Chuoi ma lan chay, vi du "20260928T061500Z".
    """
    return (moment or utc_now()).strftime("%Y%m%dT%H%M%SZ")


def landing_dir(source: str, ingestion_date: str) -> Path:
    """Thu muc chua file raw tai ve cua mot nguon trong mot ngay.

    Thu muc duoc tao neu chua co.

    Args:
        source: Ten nguon.
        ingestion_date: Ngay dang YYYY-MM-DD.

    Returns:
        Duong dan thu muc landing.
    """
    path = LANDING_ROOT / source / ingestion_date
    path.mkdir(parents=True, exist_ok=True)
    return path


def bronze_path(source: str) -> Path:
    """Duong dan goc cua bang Delta Bronze cua mot nguon.

    Tra ve goc bang chu khong phai tung phan vung: Delta tu quan ly cac thu
    muc `ingestion_date=...` ben trong.

    Args:
        source: Ten nguon.

    Returns:
        Duong dan bang Delta.
    """
    return BRONZE_ROOT / source


def metadata_dir(source: str, ingestion_date: str) -> Path:
    """Thu muc chua file metadata cua mot nguon trong mot ngay.

    De ngoai bang Delta (cay `data/metadata/` rieng) de khong lan vao file
    cua Delta trong `data/bronze/`.

    Args:
        source: Ten nguon.
        ingestion_date: Ngay dang YYYY-MM-DD.

    Returns:
        Duong dan thu muc metadata, da duoc tao.
    """
    path = METADATA_ROOT / source / f"{PARTITION_COLUMN}={ingestion_date}"
    path.mkdir(parents=True, exist_ok=True)
    return path
