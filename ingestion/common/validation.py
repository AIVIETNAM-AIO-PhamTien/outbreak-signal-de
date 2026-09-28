"""Kiem tra o muc INGESTION - chi xac nhan viec nap du lieu da chay dung.

Pham vi co y giu hep. O day chi hoi nhung cau kieu "co tai duoc khong",
"file co rong khong", "Spark doc duoc khong". KHONG kiem tra nghiep vu kieu
"so ca phai >= 0" hay "ten nuoc phai hop le" - Bronze giu nguyen trang nguon,
neu nguon bao sai thi Bronze phai ghi lai dung cai sai do. Viec danh gia
chat luong noi dung la cua tang Silver.
"""

from pathlib import Path

from pyspark.sql import DataFrame


class IngestionValidationError(Exception):
    """Mot buoc kiem tra ingestion that bai - dung lai truoc khi ghi Bronze."""


def check_response_ok(response, source: str) -> None:
    """Xac nhan HTTP response dung la thanh cong va co noi dung.

    `requests` khong tu raise voi 4xx/5xx nen phai hoi tuong minh. Ngoai ra
    mot so nguon tra 200 kem body rong khi ho dang loi, nen kiem tra ca do dai.

    Args:
        response: Doi tuong requests.Response.
        source: Ten nguon, de dua vao thong bao loi.

    Raises:
        IngestionValidationError: Neu status khong phai 2xx hoac body rong.
    """
    if not response.ok:
        raise IngestionValidationError(
            f"[{source}] nguon tra HTTP {response.status_code} cho {response.url}"
        )
    if not response.content:
        raise IngestionValidationError(
            f"[{source}] nguon tra HTTP {response.status_code} nhung body rong"
        )


def check_raw_file(path: Path, source: str) -> None:
    """Xac nhan file raw da nam tren dia va khong rong.

    File rong nguy hiem hon file thieu: no chay tiep duoc va ghi vao Bronze
    mot trang thai "khong co du lieu" khong co that.

    Args:
        path: File raw vua ghi xuong landing.
        source: Ten nguon, de dua vao thong bao loi.

    Raises:
        IngestionValidationError: Neu file khong ton tai hoac dung luong bang 0.
    """
    if not path.exists():
        raise IngestionValidationError(f"[{source}] khong thay file raw: {path}")
    if path.stat().st_size == 0:
        raise IngestionValidationError(f"[{source}] file raw rong: {path}")


def check_landing_not_empty(landing: Path, pattern: str, source: str) -> list[Path]:
    """Xac nhan thu muc landing cua ngay co it nhat mot file khop mau.

    Spark doc lai ca thu muc cua ngay chu khong chi file vua tai, nen phai
    biet chac co gi de doc truoc khi goi Spark - Spark bao loi thu muc rong
    bang mot stack trace rat kho doc.

    Args:
        landing: Thu muc landing cua nguon trong ngay.
        pattern: Mau glob, vi du "*.jsonl".
        source: Ten nguon, de dua vao thong bao loi.

    Returns:
        Danh sach file khop, da sap xep.

    Raises:
        IngestionValidationError: Neu khong co file nao khop.
    """
    files = sorted(landing.glob(pattern))
    if not files:
        raise IngestionValidationError(
            f"[{source}] khong co file nao khop {pattern!r} trong {landing}"
        )
    return files


def check_dataframe_readable(frame: DataFrame, source: str) -> int:
    """Xac nhan Spark doc duoc dataset va dem duoc so dong.

    Dem dong la phep kiem tra re nhat ma bat Spark phai thuc su doc het file:
    neu file hong hoac sai dinh dang thi loi no ra o day, truoc khi ghi
    Bronze, chu khong phai luc nguoi khac query ba ngay sau.

    Args:
        frame: DataFrame vua doc tu landing.
        source: Ten nguon, de dua vao thong bao loi.

    Returns:
        So dong doc duoc.

    Raises:
        IngestionValidationError: Neu khong co cot nao, hoac khong co dong nao.
    """
    if not frame.columns:
        raise IngestionValidationError(f"[{source}] doc duoc file nhung khong co cot nao")

    count = frame.count()
    if count == 0:
        raise IngestionValidationError(
            f"[{source}] dataset doc duoc nhung khong co dong nao - "
            f"tu choi ghi mot phan vung rong de len Bronze"
        )
    return count
