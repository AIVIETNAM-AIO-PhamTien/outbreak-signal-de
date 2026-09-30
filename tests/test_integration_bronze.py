"""Test tich hop OpenDengue: GitHub API -> tai zip -> landing -> Bronze -> metadata.

Chay het ca chuoi that tru mang: GitHub API (tim release) va buoc tai file duoc
gia lap, con lai - giai nen, Spark doc, ghi Delta, sinh metadata - la duong di that.

Danh dau @pytest.mark.integration vi can JVM + Delta. Bo qua bang:
    pytest -m "not integration"
"""

import hashlib
import io
import json
import zipfile
from pathlib import Path

import pytest

from ingestion.common import paths
from ingestion.common.metadata import STATUS_FAILED, STATUS_SKIPPED, STATUS_SUCCESS
from ingestion.common.validation import IngestionValidationError

pytestmark = pytest.mark.integration

# 3 dong SEA (con lai sau loc) + 1 dong JAPAN (ngoai SEA, phai bi loc mat).
# "VIET NAM" CO dau cach - dung format that cua nguon.
CSV_BODY = (
    "adm_0_name,calendar_start_date,calendar_end_date,dengue_total\n"
    "VIET NAM,2026-01-01,2026-01-07,120\n"
    "THAILAND,2026-01-01,2026-01-07,300\n"
    "SINGAPORE,2026-01-01,2026-01-07,45\n"
    "JAPAN,2026-01-01,2026-01-07,999\n"
)
ZIP_NAME = "Spatial_extract_V1_3.zip"


def make_zip(body: str = CSV_BODY) -> bytes:
    """Mot file zip chua dung mot CSV, giong hinh dang cua OpenDengue."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("Spatial_extract_V1_3.csv", body)
    return buffer.getvalue()


def blob_sha(payload: bytes) -> str:
    """git blob sha cua mot noi dung - giong gia tri GitHub API tra ve."""
    return hashlib.sha1(f"blob {len(payload)}\0".encode() + payload).hexdigest()


@pytest.fixture
def isolated_data_roots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Tro landing / bronze / metadata vao thu muc tam cua test."""
    monkeypatch.setattr(paths, "LANDING_ROOT", tmp_path / "landing")
    monkeypatch.setattr(paths, "BRONZE_ROOT", tmp_path / "bronze")
    monkeypatch.setattr(paths, "METADATA_ROOT", tmp_path / "metadata")
    return tmp_path


class FakeGitHub:
    """Gia lap GitHub (release + file) va dem so lan tai file."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, payload: bytes,
                 release: str = "V1.3", advertised_sha: str | None = None) -> None:
        from ingestion import opendengue

        self.payload = payload
        self.downloads = 0
        entry = {"name": ZIP_NAME, "sha": advertised_sha or blob_sha(payload),
                 "size": len(payload), "download_url": f"https://example.invalid/{ZIP_NAME}"}
        monkeypatch.setattr(opendengue, "resolve_release", lambda cfg: (release, entry))
        monkeypatch.setattr(opendengue.http, "download", self._download)

    def _download(self, url: str, dest: Path, retries: int, timeout: float) -> Path:
        self.downloads += 1
        dest.write_bytes(self.payload)
        return dest


def latest_metadata(source: str) -> dict:
    """Doc ban ghi metadata moi nhat cua mot nguon."""
    files = sorted((paths.METADATA_ROOT / source).rglob("*.json"))
    assert files, "khong co file metadata nao duoc sinh ra"
    return json.loads(files[-1].read_text(encoding="utf-8"))


def read_bronze(spark):
    """Doc bang Bronze opendengue."""
    return spark.read.format("delta").load(str(paths.BRONZE_ROOT / "opendengue"))


class TestChuoiDayDu:
    def test_nguon_den_bronze_va_metadata(self, spark, isolated_data_roots, monkeypatch) -> None:
        from ingestion import opendengue

        FakeGitHub(monkeypatch, make_zip())
        meta = opendengue.ingest(spark=spark)

        # Landing theo RELEASE, khong theo ngay chay.
        landing = paths.LANDING_ROOT / "opendengue" / "V1.3"
        assert (landing / ZIP_NAME).exists()
        assert (landing / "Spatial_extract_V1_3.csv").exists()

        frame = read_bronze(spark)
        assert frame.count() == 3 and meta.record_count == 3
        for column in ("adm_0_name", "dengue_total", "_source", "_ingested_at",
                       "_source_file", "_fetched_at", "_file_sha", "release", "ingestion_date"):
            assert column in frame.columns
        assert {r["release"] for r in frame.collect()} == {"V1.3"}
        # _source_file tuong doi tu landing/, khong con duong dan tuyet doi cua may.
        assert frame.first()["_source_file"] == "opendengue/V1.3/Spatial_extract_V1_3.csv"

        record = latest_metadata("opendengue")
        assert record["status"] == STATUS_SUCCESS
        assert record["record_count"] == 3
        assert record["source_version"] == "V1.3"
        assert record["raw_files"][0]["sha256"]

    def test_bronze_giu_nguyen_gia_tri_goc(self, spark, isolated_data_roots, monkeypatch) -> None:
        from ingestion import opendengue

        FakeGitHub(monkeypatch, make_zip())
        opendengue.ingest(spark=spark)
        rows = {row["adm_0_name"]: row["dengue_total"] for row in read_bronze(spark).collect()}
        assert rows["VIET NAM"] == "120"  # chuoi, khong ep kieu
        assert "JAPAN" not in rows  # loc pham vi SEA


class TestIdempotencyTheoRelease:
    def test_cung_release_cung_sha_thi_bo_qua_khong_tai(
        self, spark, isolated_data_roots, monkeypatch
    ) -> None:
        from ingestion import opendengue

        github = FakeGitHub(monkeypatch, make_zip())
        opendengue.ingest(spark=spark)
        meta = opendengue.ingest(spark=spark)

        assert github.downloads == 1, "lan 2 khong duoc tai lai ~55MB"
        assert meta.status == STATUS_SKIPPED
        assert meta.record_count == 3
        assert read_bronze(spark).count() == 3
        assert latest_metadata("opendengue")["status"] == STATUS_SKIPPED

    def test_release_moi_them_partition_giu_release_cu(
        self, spark, isolated_data_roots, monkeypatch
    ) -> None:
        from ingestion import opendengue

        FakeGitHub(monkeypatch, make_zip())
        opendengue.ingest(spark=spark)
        FakeGitHub(monkeypatch, make_zip(CSV_BODY + "MALAYSIA,2026-01-01,2026-01-07,7\n"),
                   release="V1.4")
        opendengue.ingest(spark=spark)

        counts = {r["release"]: r["count"] for r in read_bronze(spark).groupBy("release").count().collect()}
        assert counts == {"V1.3": 3, "V1.4": 4}

    def test_cung_release_sha_doi_thi_nap_lai_thay_the(
        self, spark, isolated_data_roots, monkeypatch
    ) -> None:
        from ingestion import opendengue

        FakeGitHub(monkeypatch, make_zip())
        opendengue.ingest(spark=spark)
        FakeGitHub(monkeypatch, make_zip(CSV_BODY.replace("120", "121")))
        opendengue.ingest(spark=spark)

        frame = read_bronze(spark)
        assert frame.count() == 3, "thay the partition V1.3, khong nhan doi"
        assert frame.where("adm_0_name = 'VIET NAM'").first()["dengue_total"] == "121"


class TestThatBai:
    def test_file_tai_ve_lech_sha_bi_chan(self, spark, isolated_data_roots, monkeypatch) -> None:
        from ingestion import opendengue

        FakeGitHub(monkeypatch, make_zip(), advertised_sha="0" * 40)
        with pytest.raises(IngestionValidationError, match="lech sha"):
            opendengue.ingest(spark=spark)

        record = latest_metadata("opendengue")
        assert record["status"] == STATUS_FAILED
        assert record["record_count"] is None

    def test_lan_sau_that_bai_khong_xoa_du_lieu_cu(
        self, spark, isolated_data_roots, monkeypatch
    ) -> None:
        from ingestion import opendengue

        FakeGitHub(monkeypatch, make_zip())
        opendengue.ingest(spark=spark)
        FakeGitHub(monkeypatch, make_zip(), release="V1.4", advertised_sha="0" * 40)
        with pytest.raises(IngestionValidationError):
            opendengue.ingest(spark=spark)

        assert read_bronze(spark).count() == 3
