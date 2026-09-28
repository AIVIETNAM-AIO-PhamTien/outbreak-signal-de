"""Ban ghi metadata cho moi lan chay ingestion.

Moi lan chay - thanh cong hay that bai - deu de lai mot file JSON mo ta chinh
LAN CHAY do: lay tu dau, luc nao, duoc bao nhieu dong, mat bao lau, loi gi.
Day la metadata cua thao tac ingestion, khong phai schema nghiep vu cua du lieu.

Cac cot `_source` / `_ingested_at` / `_source_file` nam trong bang Bronze la
lineage o muc TUNG DONG, phuc vu viec truy nguoc mot dong den tu file nao.
Hai thu do khac nhau va can ca hai.
"""

import hashlib
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from ingestion.common.paths import metadata_dir, utc_now

STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"


def sha256_of(path: Path, chunk_size: int = 1 << 20) -> str:
    """Tinh SHA-256 cua mot file, doc theo tung khuc de khong nap het vao RAM.

    Args:
        path: File can bam.
        chunk_size: Kich thuoc moi lan doc, mac dinh 1 MiB.

    Returns:
        Chuoi hex 64 ky tu.
    """
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass
class IngestionMetadata:
    """Mo ta mot lan chay ingestion cua mot nguon.

    Attributes:
        source: Ten nguon.
        source_type: Kieu nguon (file_download / rss / rest_api).
        source_format: Dinh dang du lieu goc (csv / xml / geojson / json).
        source_url: URL da goi.
        ingestion_date: Ngay phan vung, dang YYYY-MM-DD.
        ingestion_timestamp: Thoi diem bat dau chay, ISO-8601 UTC.
        run_id: Ma lan chay.
        ingestion_mode: Luon la "batch" trong pham vi hien tai.
        status: success / failed / skipped.
        record_count: So dong ghi vao Bronze. None neu chua xac dinh duoc.
        raw_files: Cac file raw cua lan chay nay, kem kich thuoc va checksum.
        bytes_downloaded: Tong dung luong tai ve trong lan chay nay.
        source_version: Version cua dataset, neu nguon co danh version.
        duration_seconds: Thoi gian chay, tinh khi goi finish().
        error_message: Noi dung loi neu that bai, None neu thanh cong.
    """

    source: str
    source_type: str
    source_format: str
    source_url: str
    ingestion_date: str
    ingestion_timestamp: str
    run_id: str
    ingestion_mode: str = "batch"
    status: str = STATUS_SUCCESS
    record_count: int | None = None
    raw_files: list[dict[str, Any]] = field(default_factory=list)
    bytes_downloaded: int = 0
    source_version: str | None = None
    duration_seconds: float | None = None
    error_message: str | None = None

    def add_raw_file(self, path: Path) -> None:
        """Ghi nhan mot file raw da luu xuong landing.

        Args:
            path: File raw vua ghi.
        """
        size = path.stat().st_size
        self.raw_files.append(
            {
                "path": str(path),
                "bytes": size,
                "sha256": sha256_of(path),
            }
        )
        self.bytes_downloaded += size

    def fail(self, error: BaseException) -> None:
        """Danh dau lan chay that bai.

        Args:
            error: Ngoai le da bat duoc.
        """
        self.status = STATUS_FAILED
        self.error_message = f"{type(error).__name__}: {error}"

    def write(self) -> Path:
        """Chot duration roi ghi ban ghi ra file JSON.

        Returns:
            Duong dan file metadata da ghi.
        """
        started = _parse_iso(self.ingestion_timestamp)
        self.duration_seconds = round((utc_now() - started).total_seconds(), 2)

        path = metadata_dir(self.source, self.ingestion_date) / f"{self.run_id}.json"
        path.write_text(
            json.dumps(asdict(self), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return path


def _parse_iso(value: str):
    """Parse chuoi ISO-8601 do chinh module nay sinh ra."""
    from datetime import datetime

    return datetime.fromisoformat(value)


def new_metadata(
    source_cfg: dict[str, Any], ingestion_date: str, run_id: str, source_url: str
) -> IngestionMetadata:
    """Tao ban ghi metadata tu config cua nguon.

    Args:
        source_cfg: Config nguon, lay tu ingestion.common.config.source_config().
        ingestion_date: Ngay phan vung dang YYYY-MM-DD.
        run_id: Ma lan chay.
        source_url: URL thuc te da goi (co the da thay tham so vao template).

    Returns:
        Ban ghi metadata o trang thai success, chua co record_count.
    """
    return IngestionMetadata(
        source=source_cfg["name"],
        source_type=source_cfg.get("source_type", "unknown"),
        source_format=source_cfg.get("source_format", "unknown"),
        source_url=source_url,
        ingestion_date=ingestion_date,
        ingestion_timestamp=utc_now().isoformat(),
        run_id=run_id,
        ingestion_mode=source_cfg.get("mode", "batch"),
        source_version=source_cfg.get("version"),
    )
