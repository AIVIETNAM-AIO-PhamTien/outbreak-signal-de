"""Test tich hop: chay lai sau loi giua chung, bang cu, CSV nhieu dong, snapshot 0 cum.

Moi test tai hien mot loi da xac nhan khi review (30/9/2026) - mang duoc gia lap,
con Spark + Delta la that.
"""

import hashlib
import io
import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from ingestion.common import paths
from ingestion.common.bronze import check_existing_table, read_csv_strings, write_bronze
from ingestion.common.config import source_config
from ingestion.common.metadata import STATUS_SKIPPED, STATUS_SUCCESS, new_metadata
from ingestion.common.validation import IngestionValidationError

pytestmark = pytest.mark.integration


@pytest.fixture
def roots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Tro landing / bronze / metadata vao thu muc tam."""
    monkeypatch.setattr(paths, "LANDING_ROOT", tmp_path / "landing")
    monkeypatch.setattr(paths, "BRONZE_ROOT", tmp_path / "bronze")
    monkeypatch.setattr(paths, "METADATA_ROOT", tmp_path / "metadata")
    return tmp_path


def bronze_count(spark, table: str) -> int:
    """So dong cua mot bang Bronze trong thu muc tam."""
    return spark.read.format("delta").load(str(paths.BRONZE_ROOT / table)).count()


class Resp:
    """Response gia: du thuoc tinh ma http/validation dung toi."""

    ok, status_code, url, headers = True, 200, "https://example.invalid", {}

    def __init__(self, payload: dict | None = None, content: bytes | None = None) -> None:
        self._payload = payload
        self.content = content if content is not None else json.dumps(payload).encode()

    def json(self) -> dict:
        return self._payload if self._payload is not None else json.loads(self.content)


class TestDocCsv:
    def test_o_co_xuong_dong_khong_bi_cat_thanh_2_dong(self, spark, tmp_path: Path) -> None:
        path = tmp_path / "x.csv"
        path.write_text('adm_0_name,adm_1_name,dengue_total\n'
                        'INDIA,"ARUNACHAL\r\nPRADESH",5\n'
                        'VIET NAM,"HA ""NOI""",7\n', encoding="utf-8")
        rows = read_csv_strings(spark, path).collect()
        assert len(rows) == 2
        assert rows[0]["adm_1_name"] == "ARUNACHAL\r\nPRADESH" and rows[0]["dengue_total"] == "5"
        assert rows[1]["adm_1_name"] == 'HA "NOI"'
        assert all(f.dataType.simpleString() == "string"
                   for f in read_csv_strings(spark, path).schema.fields)


class TestBangCu:
    def test_phan_vung_khac_hoac_cot_khong_phai_string_thi_bao_ro(self, spark, roots) -> None:
        old = spark.createDataFrame([("V1.3", "2026-09-28")], "release string, ingestion_date string")
        old.write.format("delta").partitionBy("ingestion_date").save(str(paths.BRONZE_ROOT / "od"))
        with pytest.raises(IngestionValidationError, match="phan vung"):
            check_existing_table(spark, "od", "release")

        typed = spark.createDataFrame([(1, "2026-09-28")], "CASES long, ingestion_date string")
        typed.write.format("delta").partitionBy("ingestion_date").save(str(paths.BRONZE_ROOT / "who"))
        with pytest.raises(IngestionValidationError, match="CASES"):
            check_existing_table(spark, "who", "ingestion_date")

        check_existing_table(spark, "chua_co", "release")  # bang chua co: khong loi

    def test_opendengue_bang_cu_bao_loi_truoc_khi_tai(self, spark, roots, monkeypatch) -> None:
        from ingestion import opendengue

        old = spark.createDataFrame([("VIET NAM", "2026-09-28")], "adm_0_name string, ingestion_date string")
        old.write.format("delta").partitionBy("ingestion_date").save(str(paths.BRONZE_ROOT / "opendengue"))
        entry = {"name": "Spatial_extract_V1_3.zip", "sha": "abc", "download_url": "https://x"}
        monkeypatch.setattr(opendengue, "resolve_release", lambda cfg: ("V1.3", entry))
        downloads = []
        monkeypatch.setattr(opendengue.http, "download", lambda *a, **k: downloads.append(1))

        with pytest.raises(IngestionValidationError, match="khong tuong thich"):
            opendengue.ingest(spark=spark)
        assert downloads == []  # khong ton 55MB cho mot lan chay chac chan loi


class TestTrendsNapLaiBangThieu:
    def test_bang_province_mat_thi_lan_sau_nap_lai(self, spark, roots, monkeypatch) -> None:
        from ingestion import trends_th

        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer) as writer:
            pd.DataFrame({"P-code": ["TH10"], "Dengue total": [640]}).to_excel(
                writer, sheet_name="weekly data", index=False)
            pd.DataFrame({"P-code": ["TH10"], "Province": ["Bangkok"]}).to_excel(
                writer, sheet_name="province data", index=False)
        xlsx = buffer.getvalue()
        record = {"id": 7, "files": [{"key": "TRENDS.xlsx", "links": {"self": "https://x"},
                                      "checksum": "md5:" + hashlib.md5(xlsx).hexdigest()}]}
        monkeypatch.setattr(trends_th.http, "get", lambda *a, **k: Resp(record))
        monkeypatch.setattr(trends_th.http, "download",
                            lambda url, dest, retries, timeout: dest.write_bytes(xlsx))

        assert trends_th.ingest(spark=spark).status == STATUS_SUCCESS
        # Gia lap lan dau ghi province bi loi: bang weekly co, bang province khong.
        shutil.rmtree(paths.BRONZE_ROOT / "trends_th_province")

        meta = trends_th.ingest(spark=spark)
        assert meta.status == STATUS_SUCCESS  # truoc khi sua: SKIPPED mai mai
        assert bronze_count(spark, "trends_th_province") == 1
        assert bronze_count(spark, "trends_th_weekly") == 1  # ghi de theo _release, khong nhan doi
        assert trends_th.ingest(spark=spark).status == STATUS_SKIPPED


class TestHdxNapLaiPolygon:
    VERSION = "20260126T101500"

    @pytest.fixture
    def setup(self, spark, roots, monkeypatch):
        from ingestion import hdx_cod

        monkeypatch.setattr(hdx_cod.hdx, "package_show", lambda name, cfg: {})
        monkeypatch.setattr(hdx_cod.hdx, "find_resource",
                            lambda package, pattern: {"name": "vnm.xlsx",
                                                      "last_modified": "2026-01-26T10:15:00"})

        def no_download(*args, **kwargs):
            raise AssertionError("khong duoc tai lai XLSX khi bang don vi da co")

        monkeypatch.setattr(hdx_cod, "_download_once", no_download)
        geometry_calls = []
        monkeypatch.setattr(hdx_cod, "_load_geometry",
                            lambda *args: geometry_calls.append(args[2]))
        ab = spark.createDataFrame([("VN01", "VNM", self.VERSION)],
                                   "adm1_pcode string, iso3 string, _version string")
        write_bronze(ab, "hdx_cod_ab", "VNM", partition_column="iso3")
        cfg = {**source_config("hdx_cod_ab"), "countries": ["VNM"], "geometry_countries": ["VNM"]}
        return hdx_cod, cfg, geometry_calls

    def test_don_vi_da_co_polygon_chua_co_thi_chi_nap_polygon(self, spark, setup) -> None:
        hdx_cod, cfg, geometry_calls = setup
        meta = new_metadata(cfg, "2026-09-30", "R", cfg["url"])
        assert hdx_cod._load_ab_country(spark, cfg, "VNM", meta) == 0
        assert geometry_calls == ["VNM"]

    def test_du_ca_hai_thi_bo_qua_va_bao_dung_so_dong(self, spark, setup, monkeypatch) -> None:
        hdx_cod, cfg, geometry_calls = setup
        geometry = spark.createDataFrame([("VN01", "{}", "VNM", self.VERSION)],
                                         "adm1_pcode string, geometry string, iso3 string, _version string")
        write_bronze(geometry, "hdx_cod_ab_geometry", "VNM", partition_column="iso3")
        monkeypatch.setattr(hdx_cod, "source_config", lambda name: cfg)

        meta = hdx_cod.ingest_ab(spark=spark)
        assert meta.status == STATUS_SKIPPED and geometry_calls == []
        assert meta.record_count == 1  # truoc khi sua: 0 du bang co du lieu


class TestSgNeaHetDich:
    def test_lan_fetch_0_cum_xoa_snapshot_cu_trong_ngay(self, spark, roots, monkeypatch) -> None:
        from ingestion import sg_nea

        cfg = source_config("sg_nea")
        clusters = {"features": [
            {"type": "Feature", "properties": {"LOCALITY": f"Khu {i}", "CASE_SIZE": i},
             "geometry": {"type": "Polygon", "coordinates": [[[103.7, 1.3]]]}} for i in (1, 2)]}

        def serve(geojson: dict):
            def get(url, retries, timeout, **kwargs):
                if url == cfg["url"]:
                    return Resp({"code": 0, "data": {"url": "https://s3.invalid/x"}})
                return Resp(content=json.dumps(geojson).encode())
            monkeypatch.setattr(sg_nea.http, "get", get)

        serve(clusters)
        assert sg_nea.ingest(spark=spark).record_count == 2
        assert bronze_count(spark, "sg_nea") == 2

        serve({"features": []})
        meta = sg_nea.ingest(spark=spark)
        assert meta.status == STATUS_SUCCESS and meta.record_count == 0
        assert bronze_count(spark, "sg_nea") == 0  # truoc khi sua: van 2 cum da het
        assert not list((paths.LANDING_ROOT / "sg_nea").rglob("*.jsonl"))
