"""Test kiem tra ingestion - phat hien nguon hong TRUOC khi ghi vao Bronze."""

from pathlib import Path

import pytest

from ingestion.common.validation import (
    IngestionValidationError,
    check_landing_not_empty,
    check_raw_file,
    check_response_ok,
)


class FakeResponse:
    """Response gia, du dung cho check_response_ok."""

    def __init__(self, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self.content = content
        self.ok = 200 <= status_code < 300
        self.url = "https://example.invalid/data"


class TestCheckResponseOk:
    def test_chap_nhan_200_co_noi_dung(self) -> None:
        check_response_ok(FakeResponse(200, b'{"ok": true}'), "alpha")

    @pytest.mark.parametrize("status", [404, 429, 500, 503])
    def test_tu_choi_moi_status_loi(self, status: int) -> None:
        with pytest.raises(IngestionValidationError, match=str(status)):
            check_response_ok(FakeResponse(status, b"loi"), "alpha")

    def test_tu_choi_200_nhung_body_rong(self) -> None:
        # Mot so nguon tra 200 kem body rong khi ho dang loi - nhin status
        # thoi thi khong du.
        with pytest.raises(IngestionValidationError, match="body rong"):
            check_response_ok(FakeResponse(200, b""), "alpha")

    def test_thong_bao_loi_co_ten_nguon(self) -> None:
        # Batch chay nhieu nguon, log phai chi ra duoc nguon nao hong.
        with pytest.raises(IngestionValidationError, match=r"\[alpha\]"):
            check_response_ok(FakeResponse(500, b"x"), "alpha")


class TestCheckRawFile:
    def test_chap_nhan_file_co_noi_dung(self, tmp_path: Path) -> None:
        raw = tmp_path / "raw.csv"
        raw.write_text("a,b\n1,2\n", encoding="utf-8")
        check_raw_file(raw, "alpha")

    def test_tu_choi_file_khong_ton_tai(self, tmp_path: Path) -> None:
        with pytest.raises(IngestionValidationError, match="khong thay file raw"):
            check_raw_file(tmp_path / "thieu.csv", "alpha")

    def test_tu_choi_file_rong(self, tmp_path: Path) -> None:
        # File rong nguy hiem hon file thieu: no chay tiep duoc va ghi vao
        # Bronze mot trang thai "khong co du lieu" khong co that.
        raw = tmp_path / "rong.csv"
        raw.touch()
        with pytest.raises(IngestionValidationError, match="file raw rong"):
            check_raw_file(raw, "alpha")


class TestCheckLandingNotEmpty:
    def test_tra_ve_danh_sach_file_da_sap_xep(self, tmp_path: Path) -> None:
        for name in ("c.jsonl", "a.jsonl", "b.jsonl"):
            (tmp_path / name).write_text("{}\n", encoding="utf-8")
        found = check_landing_not_empty(tmp_path, "*.jsonl", "alpha")
        assert [p.name for p in found] == ["a.jsonl", "b.jsonl", "c.jsonl"]

    def test_bo_qua_file_khong_khop_mau(self, tmp_path: Path) -> None:
        # Thu muc landing cua opendengue co ca .zip lan .csv.
        (tmp_path / "data.csv").write_text("a\n", encoding="utf-8")
        (tmp_path / "data.zip").write_bytes(b"PK")
        found = check_landing_not_empty(tmp_path, "*.csv", "alpha")
        assert [p.name for p in found] == ["data.csv"]

    def test_tu_choi_thu_muc_khong_co_file_khop(self, tmp_path: Path) -> None:
        (tmp_path / "data.zip").write_bytes(b"PK")
        with pytest.raises(IngestionValidationError, match=r"\*\.csv"):
            check_landing_not_empty(tmp_path, "*.csv", "alpha")
