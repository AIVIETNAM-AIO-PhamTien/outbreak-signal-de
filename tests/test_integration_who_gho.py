"""Test tich hop: WHO GHO -> ingestion -> Bronze -> metadata.

Chay het ca chuoi that: goi API (gia lap o tang HTTP), ghi landing, Spark
doc, ghi bang Delta, sinh metadata. Chi mang la duoc thay the - con lai deu
la duong di that, ke ca Spark va Delta.

Danh dau @pytest.mark.integration vi can JVM + Delta. Bo qua bang:
    pytest -m "not integration"
"""

import json
from pathlib import Path

import pytest
import requests

from ingestion.common import http, paths
from ingestion.common.metadata import STATUS_FAILED, STATUS_SUCCESS
from ingestion.common.validation import IngestionValidationError

pytestmark = pytest.mark.integration

# 3 ban ghi trong pham vi SEA - dung dinh dang xMart tra ve that (xem
# spikes/test_who_gho.py). Cac truong None mo phong dung thuc te: WHO GHO
# chi dien CASES on dinh, cac truong con lai (CONFIRMED_CASES, SERO_*,
# POPULATION) thuong null cho du lieu SEA gan day.
WHO_VALUE = [
    {
        "COUNTRY": "Viet Nam", "ISO3": "VNM", "WHO_REGION": "WPR",
        "YEAR": 2026, "DATE_NUM": 35, "START_DATE": "2026-08-24",
        "DATE_TYPE": "isoweek", "CASES": 4637, "CONFIRMED_CASES": None,
        "SEVERE_CASES": None, "DEATHS": None, "SERO_1": None,
        "SERO_2": None, "SERO_3": None, "SERO_4": None, "POPULATION": None,
    },
    {
        "COUNTRY": "Thailand", "ISO3": "THA", "WHO_REGION": "SEAR",
        "YEAR": 2026, "DATE_NUM": 30, "START_DATE": "2026-07-27",
        "DATE_TYPE": "isoweek", "CASES": 5921, "CONFIRMED_CASES": None,
        "SEVERE_CASES": None, "DEATHS": None, "SERO_1": None,
        "SERO_2": None, "SERO_3": None, "SERO_4": None, "POPULATION": None,
    },
    {
        "COUNTRY": "Cambodia", "ISO3": "KHM", "WHO_REGION": "WPR",
        "YEAR": 2026, "DATE_NUM": 34, "START_DATE": "2026-08-24",
        "DATE_TYPE": "isoweek", "CASES": 6850, "CONFIRMED_CASES": None,
        "SEVERE_CASES": None, "DEATHS": None, "SERO_1": None,
        "SERO_2": None, "SERO_3": None, "SERO_4": None, "POPULATION": None,
    },
]

WHO_ENVELOPE = {
    "@odata.context": (
        "https://xmart-api-public.who.int/ARBOV/"
        "$metadata#V_DENGUE_GLOBAL_VALIDATED_PUBLIC"
    ),
    "value": WHO_VALUE,
}


class FakeResponse:
    """Response gia du dung cho ca check_response_ok() lan response.json()."""

    def __init__(self, envelope: dict, status_code: int = 200) -> None:
        self._envelope = envelope
        self.content = json.dumps(envelope).encode("utf-8")
        self.status_code = status_code
        self.ok = 200 <= status_code < 300
        self.url = "https://xmart-api-public.who.int/ARBOV/V_DENGUE_GLOBAL_VALIDATED_PUBLIC"

        self.headers: dict = {}

    def json(self) -> dict:
        return self._envelope

    def raise_for_status(self) -> None:
        if not self.ok:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)


@pytest.fixture
def isolated_data_roots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Tro landing / bronze / metadata vao thu muc tam cua test."""
    monkeypatch.setattr(paths, "LANDING_ROOT", tmp_path / "landing")
    monkeypatch.setattr(paths, "BRONZE_ROOT", tmp_path / "bronze")
    monkeypatch.setattr(paths, "METADATA_ROOT", tmp_path / "metadata")
    return tmp_path


@pytest.fixture
def fake_call(monkeypatch: pytest.MonkeyPatch):
    """Thay requests.get trong module who_gho bang envelope dung san."""
    from ingestion import who_gho

    monkeypatch.setattr(
        http.requests, "get", lambda *args, **kwargs: FakeResponse(WHO_ENVELOPE)
    )
    return WHO_ENVELOPE


def latest_metadata(source: str) -> dict:
    """Doc ban ghi metadata moi nhat cua mot nguon."""
    files = sorted((paths.METADATA_ROOT / source).rglob("*.json"))
    assert files, "khong co file metadata nao duoc sinh ra"
    return json.loads(files[-1].read_text(encoding="utf-8"))


class TestChuoiDayDu:
    def test_nguon_den_bronze_va_metadata(
        self, spark, isolated_data_roots, fake_call
    ) -> None:
        from ingestion import who_gho

        meta = who_gho.ingest(spark=spark)

        # 1. Ca ban goc (envelope) lan ban .jsonl deu duoc giu lai o landing.
        landing = paths.LANDING_ROOT / "who_gho" / meta.ingestion_date
        json_files = list(landing.glob("*.json"))
        jsonl_files = list(landing.glob("*.jsonl"))
        assert len(json_files) == 1
        assert len(jsonl_files) == 1

        raw_envelope = json.loads(json_files[0].read_text(encoding="utf-8"))
        assert raw_envelope == WHO_ENVELOPE, "ban goc phai giu nguyen envelope, khong bi sua"

        # 2. Bronze co dung so dong.
        frame = spark.read.format("delta").load(str(paths.BRONZE_ROOT / "who_gho"))
        assert frame.count() == 3
        assert meta.record_count == 3

        # 3. Cot goc giu nguyen ten (chu HOA nhu API tra ve), khong bi doi.
        for column in ("COUNTRY", "ISO3", "CASES", "START_DATE", "DATE_TYPE"):
            assert column in frame.columns

        # 4. Cot lineage va cot phan vung da duoc them.
        for column in ("_source", "_ingested_at", "_source_file", "ingestion_date"):
            assert column in frame.columns

        # 5. Metadata da ghi ra dia va khop voi thuc te.
        record = latest_metadata("who_gho")
        assert record["status"] == STATUS_SUCCESS
        assert record["record_count"] == 3
        assert record["source_format"] == "json"
        assert record["raw_files"][0]["sha256"]

    def test_bronze_giu_nguyen_gia_tri_goc_khong_chuan_hoa(
        self, spark, isolated_data_roots, fake_call
    ) -> None:
        from ingestion import who_gho

        who_gho.ingest(spark=spark)
        frame = spark.read.format("delta").load(str(paths.BRONZE_ROOT / "who_gho"))
        rows = {row["ISO3"]: row["CASES"] for row in frame.collect()}

        # So ca giu nguyen kieu so nhu API tra ve (JSON tu suy ra kieu, khac
        # voi CSV cua opendengue phai ep inferSchema=False).
        assert rows["VNM"] == "4637"
        assert rows["THA"] == "5921"
        assert rows["KHM"] == "6850"


class TestIdempotency:
    def test_chay_lai_cung_ngay_khong_nhan_doi_du_lieu(
        self, spark, isolated_data_roots, fake_call
    ) -> None:
        from ingestion import who_gho

        who_gho.ingest(spark=spark)
        who_gho.ingest(spark=spark)
        who_gho.ingest(spark=spark)

        frame = spark.read.format("delta").load(str(paths.BRONZE_ROOT / "who_gho"))
        assert frame.count() == 3, "chay 3 lan phai van la 3 dong, khong phai 9"

    def test_moi_lan_chay_van_de_lai_mot_ban_metadata_rieng(
        self, spark, isolated_data_roots, fake_call
    ) -> None:
        from ingestion import who_gho

        who_gho.ingest(spark=spark)
        who_gho.ingest(spark=spark)

        files = sorted((paths.METADATA_ROOT / "who_gho").rglob("*.json"))
        assert len(files) == 2, "du lieu ghi de, nhung lich su lan chay phai giu du"


class TestThatBai:
    def test_nguon_khong_voi_toi_duoc_van_sinh_metadata_failed(
        self, spark, isolated_data_roots, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from ingestion import who_gho

        calls = []

        def always_500(*args, **kwargs):
            calls.append(1)
            return FakeResponse({}, status_code=500)

        monkeypatch.setattr(http.requests, "get", always_500)

        # 5xx la loi tam thoi: thu lai du `retries` lan roi moi nem loi goc.
        with pytest.raises(requests.HTTPError):
            who_gho.ingest(spark=spark)
        assert len(calls) == 4  # retries: 3 trong config -> 1 + 3 lan

        record = latest_metadata("who_gho")
        assert record["status"] == STATUS_FAILED
        assert record["record_count"] is None

    def test_envelope_rong_van_bi_chan_truoc_khi_ghi_bronze(
        self, spark, isolated_data_roots, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from ingestion import who_gho

        # HTTP 200 hop le nhung "value" rong - tinh huong that co the xay ra
        # neu $filter khong khop nuoc nao (vi du go sai ma ISO3).
        monkeypatch.setattr(
            http.requests,
            "get",
            lambda *a, **k: FakeResponse({"@odata.context": "x", "value": []}),
        )

        with pytest.raises(IngestionValidationError):
            who_gho.ingest(spark=spark)

        record = latest_metadata("who_gho")
        assert record["status"] == STATUS_FAILED
