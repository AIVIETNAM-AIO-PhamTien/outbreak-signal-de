"""Test sinh metadata - moi lan chay phai de lai dau vet, ke ca khi that bai."""

import json
from pathlib import Path

import pytest

from ingestion.common import paths
from ingestion.common.metadata import (
    STATUS_FAILED,
    STATUS_SUCCESS,
    IngestionMetadata,
    new_metadata,
    sha256_of,
)

SOURCE_CFG = {
    "name": "alpha",
    "source_type": "rest_api",
    "source_format": "json",
    "mode": "batch",
    "version": "V1.3",
}


@pytest.fixture(autouse=True)
def metadata_in_tmp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Chuyen cay metadata sang thu muc tam de test khong ghi vao data/ that."""
    monkeypatch.setattr(paths, "METADATA_ROOT", tmp_path / "metadata")
    return tmp_path


def make_metadata() -> IngestionMetadata:
    return new_metadata(SOURCE_CFG, "2026-09-28", "20260928T061500Z", "https://x.invalid")


class TestNewMetadata:
    def test_lay_du_truong_bat_buoc_tu_config(self) -> None:
        meta = make_metadata()
        assert meta.source == "alpha"
        assert meta.source_type == "rest_api"
        assert meta.source_format == "json"
        assert meta.ingestion_mode == "batch"
        assert meta.ingestion_date == "2026-09-28"
        assert meta.source_url == "https://x.invalid"
        assert meta.source_version == "V1.3"

    def test_mac_dinh_la_success_chua_co_so_dong(self) -> None:
        meta = make_metadata()
        assert meta.status == STATUS_SUCCESS
        assert meta.record_count is None
        assert meta.error_message is None


class TestAddRawFile:
    def test_ghi_nhan_kich_thuoc_va_checksum(self, tmp_path: Path) -> None:
        raw = tmp_path / "raw.json"
        raw.write_text('{"a": 1}', encoding="utf-8")

        meta = make_metadata()
        meta.add_raw_file(raw)

        assert len(meta.raw_files) == 1
        assert meta.raw_files[0]["bytes"] == raw.stat().st_size
        assert meta.raw_files[0]["sha256"] == sha256_of(raw)
        assert meta.bytes_downloaded == raw.stat().st_size

    def test_cong_don_khi_mot_lan_chay_tai_nhieu_file(self, tmp_path: Path) -> None:
        meta = make_metadata()
        for index in range(3):
            raw = tmp_path / f"raw{index}.txt"
            raw.write_text("x" * 10, encoding="utf-8")
            meta.add_raw_file(raw)
        assert meta.bytes_downloaded == 30


class TestFail:
    def test_giu_lai_kieu_va_noi_dung_loi(self) -> None:
        meta = make_metadata()
        meta.fail(ValueError("nguon tra ve rong"))
        assert meta.status == STATUS_FAILED
        assert meta.error_message == "ValueError: nguon tra ve rong"


class TestWrite:
    def test_ghi_ra_json_dat_ten_theo_run_id(self) -> None:
        path = make_metadata().write()
        assert path.name == "20260928T061500Z.json"
        assert path.parent.name == "ingestion_date=2026-09-28"

    def test_json_co_du_truong_toi_thieu_theo_yeu_cau(self) -> None:
        meta = make_metadata()
        meta.record_count = 123
        record = json.loads(meta.write().read_text(encoding="utf-8"))

        for key in (
            "source",
            "source_type",
            "ingestion_timestamp",
            "ingestion_date",
            "ingestion_mode",
            "source_format",
            "source_url",
            "status",
            "record_count",
        ):
            assert key in record, f"thieu truong bat buoc: {key}"
        assert record["record_count"] == 123

    def test_tinh_duration_luc_ghi(self) -> None:
        meta = make_metadata()
        meta.write()
        assert meta.duration_seconds is not None
        assert meta.duration_seconds >= 0

    def test_lan_chay_that_bai_van_de_lai_metadata(self) -> None:
        meta = make_metadata()
        meta.fail(RuntimeError("HTTP 500"))
        record = json.loads(meta.write().read_text(encoding="utf-8"))
        assert record["status"] == STATUS_FAILED
        assert "HTTP 500" in record["error_message"]
        assert record["record_count"] is None
