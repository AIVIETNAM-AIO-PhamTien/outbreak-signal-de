"""Test sinh duong dan - ba nguon phai dung chung mot bo cuc thu muc."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from ingestion.common import paths
from ingestion.common.paths import (
    PARTITION_COLUMN,
    bronze_path,
    landing_dir,
    metadata_dir,
    run_id,
    today_str,
)

MOMENT = datetime(2026, 9, 28, 6, 15, 0, tzinfo=timezone.utc)


class TestMocThoiGian:
    def test_today_str_dang_iso_theo_utc(self) -> None:
        assert today_str(MOMENT) == "2026-09-28"

    def test_run_id_co_do_phan_giai_den_giay(self) -> None:
        # Phai den giay, vi nguon tin tuc chay nhieu lan trong mot gio va moi
        # lan can mot ten file rieng.
        assert run_id(MOMENT) == "20260928T061500Z"


class TestDuongDan:
    def test_partition_column_viet_day_du(self) -> None:
        # "ingestion_date" chu khong phai "ingest_date" - hai nhanh truoc day
        # viet khac nhau, chot mot cach o day.
        assert PARTITION_COLUMN == "ingestion_date"

    def test_landing_tach_theo_nguon_va_ngay(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setattr(paths, "LANDING_ROOT", tmp_path / "landing")
        path = landing_dir("alpha", "2026-09-28")
        assert path.parts[-2:] == ("alpha", "2026-09-28")

    def test_landing_tu_tao_thu_muc(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setattr(paths, "LANDING_ROOT", tmp_path / "landing")
        assert landing_dir("alpha", "2026-09-28").exists()

    def test_bronze_tra_ve_goc_bang_khong_kem_phan_vung(self) -> None:
        # Delta tu quan ly thu muc ingestion_date=... ben trong bang.
        assert bronze_path("alpha").name == "alpha"

    def test_metadata_phan_vung_cung_kieu_voi_bronze(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        monkeypatch.setattr(paths, "METADATA_ROOT", tmp_path / "metadata")
        path = metadata_dir("alpha", "2026-09-28")
        assert path.name == "ingestion_date=2026-09-28"

    def test_metadata_nam_ngoai_cay_bronze(self) -> None:
        # De trong data/bronze/ se lan vao file cua Delta.
        assert paths.METADATA_ROOT != paths.BRONZE_ROOT
        assert paths.BRONZE_ROOT not in paths.METADATA_ROOT.parents


@pytest.mark.parametrize("source", ["opendengue", "news_rss", "sg_nea"])
def test_ba_nguon_khong_dung_chung_thu_muc(source: str) -> None:
    others = {"opendengue", "news_rss", "sg_nea"} - {source}
    assert all(bronze_path(source) != bronze_path(other) for other in others)
