"""Test tich hop: nguon -> ingestion -> Bronze -> metadata.

Chay het ca chuoi that: tai file (gia lap o tang HTTP), ghi landing, Spark doc,
ghi bang Delta, sinh metadata. Chi mang la duoc thay the - con lai deu la
duong di that, ke ca Spark va Delta.

Danh dau @pytest.mark.integration vi can JVM + Delta. Bo qua bang:
    pytest -m "not integration"
"""

import io
import json
import zipfile
from pathlib import Path

import pytest

from ingestion.common import paths
from ingestion.common.metadata import STATUS_FAILED, STATUS_SUCCESS
from ingestion.common.validation import IngestionValidationError

pytestmark = pytest.mark.integration

# 3 dong SEA (con lai sau loc) + 1 dong JAPAN (ngoai SEA, phai bi loc mat).
# "VIET NAM" CO dau cach - dung format that cua nguon, dung format trong
# configs/sources.yaml. Vien JAPAN de test filter_countries co thuc su hoat
# dong, khong chi giai dinh no chay ma khong kiem tra.
CSV_BODY = (
    "adm_0_name,calendar_start_date,calendar_end_date,dengue_total\n"
    "VIET NAM,2026-01-01,2026-01-07,120\n"
    "THAILAND,2026-01-01,2026-01-07,300\n"
    "SINGAPORE,2026-01-01,2026-01-07,45\n"
    "JAPAN,2026-01-01,2026-01-07,999\n"
)


class FakeResponse:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.status_code = 200
        self.ok = True
        self.url = "https://example.invalid/Spatial_extract_V1_3.zip"


def make_zip() -> bytes:
    """Dung mot file zip chua dung mot CSV, giong hinh dang cua OpenDengue."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("Spatial_extract_V1_3.csv", CSV_BODY)
    return buffer.getvalue()


@pytest.fixture
def isolated_data_roots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Tro landing / bronze / metadata vao thu muc tam cua test."""
    monkeypatch.setattr(paths, "LANDING_ROOT", tmp_path / "landing")
    monkeypatch.setattr(paths, "BRONZE_ROOT", tmp_path / "bronze")
    monkeypatch.setattr(paths, "METADATA_ROOT", tmp_path / "metadata")
    return tmp_path


@pytest.fixture
def fake_download(monkeypatch: pytest.MonkeyPatch):
    """Thay requests.get trong module opendengue bang mot zip dung san."""
    from ingestion import opendengue

    payload = make_zip()
    monkeypatch.setattr(
        opendengue.requests, "get", lambda *args, **kwargs: FakeResponse(payload)
    )
    return payload


def latest_metadata(source: str) -> dict:
    """Doc ban ghi metadata moi nhat cua mot nguon."""
    files = sorted((paths.METADATA_ROOT / source).rglob("*.json"))
    assert files, "khong co file metadata nao duoc sinh ra"
    return json.loads(files[-1].read_text(encoding="utf-8"))


class TestChuoiDayDu:
    def test_nguon_den_bronze_va_metadata(
        self, spark, isolated_data_roots, fake_download
    ) -> None:
        from ingestion import opendengue

        meta = opendengue.ingest(spark=spark)

        # 1. File raw duoc giu lai o landing, khong chi nam trong bo nho.
        landing = paths.LANDING_ROOT / "opendengue" / meta.ingestion_date
        assert (landing / "Spatial_extract_V1_3.zip").exists()
        assert (landing / "Spatial_extract_V1_3.csv").exists()

        # 2. Bronze co dung so dong SAU KHI LOC pham vi SEA (JAPAN bi loai).
        frame = spark.read.format("delta").load(str(paths.BRONZE_ROOT / "opendengue"))
        assert frame.count() == 3
        assert meta.record_count == 3

        # 3. Cot goc giu nguyen ten, khong bi doi.
        for column in ("adm_0_name", "calendar_start_date", "dengue_total"):
            assert column in frame.columns

        # 4. Cot lineage va cot phan vung da duoc them.
        for column in ("_source", "_ingested_at", "_source_file", "ingestion_date"):
            assert column in frame.columns

        # 5. Metadata da ghi ra dia va khop voi thuc te.
        record = latest_metadata("opendengue")
        assert record["status"] == STATUS_SUCCESS
        assert record["record_count"] == 3
        assert record["source_format"] == "csv"
        assert record["ingestion_mode"] == "batch"
        assert record["raw_files"][0]["sha256"]
        assert record["duration_seconds"] >= 0

    def test_bronze_giu_nguyen_gia_tri_goc_khong_chuan_hoa(
        self, spark, isolated_data_roots, fake_download
    ) -> None:
        from ingestion import opendengue

        opendengue.ingest(spark=spark)
        frame = spark.read.format("delta").load(str(paths.BRONZE_ROOT / "opendengue"))
        rows = {row["adm_0_name"]: row["dengue_total"] for row in frame.collect()}

        # Ten nuoc van VIET HOA nhu nguon, so ca van la string vi inferSchema=False.
        assert rows["VIET NAM"] == "120"
        assert "Vietnam" not in rows
        assert "VIETNAM" not in rows  # dung dung ten UN naming, khong viet lien


class TestLocPhamViSEA:
    """Loc theo SEA la NGOAI LE co chu dich cua nguyen tac Bronze - kiem tra
    rieng de dam bao ranh gioi dung: landing giu 100%, chi Bronze bi thu hep.
    """

    def test_landing_giu_nguyen_dong_ngoai_sea(
        self, spark, isolated_data_roots, fake_download
    ) -> None:
        from ingestion import opendengue

        meta = opendengue.ingest(spark=spark)

        # File CSV trong landing phai con nguyen JAPAN - landing la source of
        # truth, KHONG bi loc, du Bronze co loc hay khong.
        landing = paths.LANDING_ROOT / "opendengue" / meta.ingestion_date
        csv_text = (landing / "Spatial_extract_V1_3.csv").read_text(encoding="utf-8")
        assert "JAPAN" in csv_text

    def test_bronze_khong_con_dong_ngoai_sea(
        self, spark, isolated_data_roots, fake_download
    ) -> None:
        from ingestion import opendengue

        opendengue.ingest(spark=spark)
        frame = spark.read.format("delta").load(str(paths.BRONZE_ROOT / "opendengue"))
        countries = {row["adm_0_name"] for row in frame.collect()}

        assert "JAPAN" not in countries
        assert countries == {"VIET NAM", "THAILAND", "SINGAPORE"}

    def test_metadata_ghi_nhan_dung_so_dong_sau_loc(
        self, spark, isolated_data_roots, fake_download
    ) -> None:
        from ingestion import opendengue

        opendengue.ingest(spark=spark)
        record = latest_metadata("opendengue")

        # record_count trong metadata phai la so SAU loc (3), khong phai
        # tong so dong doc tu file (4) - metadata mo ta ket qua ghi Bronze.
        assert record["record_count"] == 3


class TestIdempotency:
    def test_chay_lai_cung_ngay_khong_nhan_doi_du_lieu(
        self, spark, isolated_data_roots, fake_download
    ) -> None:
        from ingestion import opendengue

        opendengue.ingest(spark=spark)
        opendengue.ingest(spark=spark)
        opendengue.ingest(spark=spark)

        frame = spark.read.format("delta").load(str(paths.BRONZE_ROOT / "opendengue"))
        assert frame.count() == 3, "chay 3 lan phai van la 3 dong, khong phai 9"

    def test_moi_lan_chay_van_de_lai_mot_ban_metadata_rieng(
        self, spark, isolated_data_roots, fake_download
    ) -> None:
        from ingestion import opendengue

        opendengue.ingest(spark=spark)
        opendengue.ingest(spark=spark)

        files = sorted((paths.METADATA_ROOT / "opendengue").rglob("*.json"))
        assert len(files) == 2, "du lieu ghi de, nhung lich su lan chay phai giu du"


class TestThatBai:
    def test_nguon_khong_voi_toi_duoc_van_sinh_metadata_failed(
        self, spark, isolated_data_roots, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from ingestion import opendengue

        monkeypatch.setattr(
            opendengue.requests,
            "get",
            lambda *args, **kwargs: FakeResponse(b"")  # 200 nhung body rong
            ,
        )

        with pytest.raises(IngestionValidationError):
            opendengue.ingest(spark=spark)

        record = latest_metadata("opendengue")
        assert record["status"] == STATUS_FAILED
        assert record["record_count"] is None
        assert "IngestionValidationError" in record["error_message"]

    def test_mot_nguon_chet_khong_xoa_du_lieu_nguon_da_thanh_cong(
        self, spark, isolated_data_roots, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from ingestion import opendengue

        payload = make_zip()
        monkeypatch.setattr(
            opendengue.requests, "get", lambda *a, **k: FakeResponse(payload)
        )
        opendengue.ingest(spark=spark)

        # Lan chay sau that bai - Bronze cua lan truoc phai con nguyen.
        monkeypatch.setattr(
            opendengue.requests, "get", lambda *a, **k: FakeResponse(b"")
        )
        for raw in (paths.LANDING_ROOT / "opendengue").rglob("*"):
            if raw.is_file():
                raw.unlink()

        with pytest.raises(Exception):
            opendengue.ingest(spark=spark)

        frame = spark.read.format("delta").load(str(paths.BRONZE_ROOT / "opendengue"))
        assert frame.count() == 3
