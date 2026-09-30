"""Test don vi cho cac ham thuan cua ingestion - khong can JVM, khong goi mang."""

import hashlib
import subprocess
from pathlib import Path

import pytest
import requests

from ingestion import news_rss, opendengue
from ingestion.common import http
from ingestion.common.validation import IngestionValidationError

RSS = b"""<?xml version="1.0"?><rss><channel>
<item><title>Sot xuat huyet tang o Ha Noi</title><link>https://n/1</link>
<guid isPermaLink="false">CBMi-1</guid><pubDate>Mon, 28 Sep 2026 07:00:00 GMT</pubDate>
<description>&lt;a href="x"&gt;tom tat&lt;/a&gt;</description>
<source url="https://vnexpress.net">VnExpress</source></item>
<item><title>Khong co source</title><link>https://n/2</link></item>
</channel></rss>"""
FEED = {"country": "VNM", "gl": "VN", "hl": "vi", "q": "sốt xuất huyết"}


class TestNewsRss:
    def test_query_gan_bo_loc_thoi_gian(self) -> None:
        params = news_rss.feed_params(FEED, "when:7d")
        assert params == {"q": "sốt xuất huyết when:7d", "hl": "vi", "gl": "VN", "ceid": "VN:vi"}

    def test_giu_guid_source_url_raw_payload_va_feed(self) -> None:
        first, second = news_rss.parse_items(RSS, FEED, "q", "2026-09-29T00:00:00+00:00")
        assert first["guid"] == "CBMi-1"
        assert first["source"] == "VnExpress"
        assert first["source_url"] == "https://vnexpress.net"
        assert first["feed_country"] == "VNM"
        assert first["_fetched_at"] == "2026-09-29T00:00:00+00:00"
        assert "<guid" in first["raw_payload"] and "vnexpress.net" in first["raw_payload"]
        # the thieu -> chuoi rong, khong loi
        assert second["guid"] == "" and second["source_url"] == ""

    def test_xml_khong_phai_rss_thi_bao_loi_khong_im_lang(self) -> None:
        with pytest.raises(IngestionValidationError):
            news_rss.parse_items(b"<html><body>captcha</body></html>", FEED, "q", "t")

    def test_moi_gia_tri_la_chuoi(self) -> None:
        for record in news_rss.parse_items(RSS, FEED, "q", "t"):
            assert all(isinstance(v, str) for v in record.values())


class TestOpenDengueRelease:
    def test_so_sanh_version_theo_so_khong_theo_chu(self) -> None:
        entries = [{"name": n, "type": "dir"} for n in ("V1.2", "V1.10", "V1.3")]
        entries.append({"name": ".DS_Store", "type": "file"})
        assert opendengue.pick_latest_release(entries) == "V1.10"

    def test_khong_co_release_thi_bao_loi(self) -> None:
        with pytest.raises(IngestionValidationError):
            opendengue.pick_latest_release([{"name": "README.md", "type": "file"}])

    def test_tim_dung_file_extract(self) -> None:
        entries = [{"name": "National_extract_V1_3.zip"}, {"name": "Spatial_extract_V1_3.zip"}]
        assert opendengue.find_extract(entries, "Spatial")["name"] == "Spatial_extract_V1_3.zip"
        with pytest.raises(IngestionValidationError):
            opendengue.find_extract(entries, "Temporal")

    def test_git_blob_sha_khop_git(self, tmp_path: Path) -> None:
        path = tmp_path / "f.bin"
        path.write_bytes(b"hello\n")
        expected = hashlib.sha1(b"blob 6\0hello\n").hexdigest()
        assert opendengue.git_blob_sha(path) == expected
        try:  # doi chieu voi chinh git neu may co git
            out = subprocess.run(["git", "hash-object", str(path)], capture_output=True,
                                 text=True, check=True).stdout.strip()
            assert out == expected
        except (FileNotFoundError, subprocess.CalledProcessError):
            pass


class _Resp:
    def __init__(self, status: int) -> None:
        self.status_code = status


class TestRetry:
    @pytest.mark.parametrize(
        ("error", "expected"),
        [
            (requests.ConnectionError(), True),
            (requests.Timeout(), True),
            # dut ket noi giua luc tai (IncompleteRead) - khong phai ConnectionError
            (requests.exceptions.ChunkedEncodingError(), True),
            (requests.exceptions.ContentDecodingError(), True),
            (requests.HTTPError(response=_Resp(503)), True),
            (requests.HTTPError(response=_Resp(429)), True),
            (requests.HTTPError(response=_Resp(404)), False),
            (requests.HTTPError(response=_Resp(403)), False),
            (ValueError(), False),
        ],
    )
    def test_chi_thu_lai_loi_tam_thoi(self, error: BaseException, expected: bool) -> None:
        assert http.is_retryable(error) is expected

    def test_thu_lai_dung_so_lan_config_roi_nem_loi_goc(self, monkeypatch) -> None:
        calls = []

        def flaky(*args, **kwargs):
            calls.append(1)
            raise requests.ConnectionError("mat mang")

        monkeypatch.setattr(http.requests, "get", flaky)
        with pytest.raises(requests.ConnectionError):
            http.get("https://example.invalid", retries=3, timeout=1)
        assert len(calls) == 4  # 1 lan dau + 3 lan thu lai

    def test_loi_4xx_khong_thu_lai(self, monkeypatch) -> None:
        calls = []

        class NotFound:
            status_code = 404
            headers: dict = {}

            def raise_for_status(self) -> None:
                raise requests.HTTPError(response=self)

        def get(*args, **kwargs):
            calls.append(1)
            return NotFound()

        monkeypatch.setattr(http.requests, "get", get)
        with pytest.raises(requests.HTTPError):
            http.get("https://example.invalid", retries=3, timeout=1)
        assert len(calls) == 1


    def test_tai_file_dut_giua_chung_thi_tai_lai_tron_ven(self, monkeypatch, tmp_path) -> None:
        calls = []

        class Stream:
            status_code = 200
            headers: dict = {}

            def raise_for_status(self) -> None:
                return None

            def __enter__(self):
                return self

            def __exit__(self, *exc) -> None:
                return None

            def iter_content(self, size: int):
                yield b"nua "
                if len(calls) == 1:
                    raise requests.exceptions.ChunkedEncodingError("IncompleteRead")
                yield b"file"

        def get(*args, **kwargs):
            calls.append(1)
            return Stream()

        monkeypatch.setattr(http.requests, "get", get)
        dest = http.download("https://example.invalid/x.zip", tmp_path / "x.zip", retries=3, timeout=1)
        assert len(calls) == 2
        assert dest.read_bytes() == b"nua file"  # khong dinh phan do dang cua lan dau
        assert not (tmp_path / "x.zip.part").exists()


class TestNewsLuuBanGoc:
    def test_feed_tra_html_van_luu_ban_goc_de_kiem_tra(self, monkeypatch, tmp_path) -> None:
        from ingestion.common import paths
        from ingestion.common.config import source_config
        from ingestion.common.metadata import new_metadata

        monkeypatch.setattr(paths, "LANDING_ROOT", tmp_path)
        captcha = b"<html><body>Our systems have detected unusual traffic</body></html>"

        class Resp:
            ok, status_code, url = True, 200, "https://news.google.com/rss/search"

            def __init__(self, content: bytes) -> None:
                self.content = content

        def get(url, retries, timeout, params=None, headers=None):
            return Resp(captcha if params["gl"] == "VN" else RSS)

        monkeypatch.setattr(news_rss.http, "get", get)
        cfg = {**source_config("news_rss"),
               "feeds": [FEED, {"country": "THA", "gl": "TH", "hl": "th", "q": "x"}]}
        meta = new_metadata(cfg, "2026-09-29", "R1", cfg["url"])
        jsonl = news_rss.fetch_raw(cfg, "2026-09-29", "R1", meta)

        saved = tmp_path / "news_rss" / "2026-09-29" / "google_news_R1_VNM.xml"
        assert saved.read_bytes() == captcha
        assert any("feed VNM loi" in w for w in meta.warnings)
        assert len(jsonl.read_text(encoding="utf-8").splitlines()) == 2  # 2 bai cua THA


class TestNguonCapTinh:
    def test_sanitize_column_cho_delta(self) -> None:
        from ingestion.common.excel import sanitize_column

        assert sanitize_column("Dengue fever (DF)") == "dengue_fever_df"
        assert sanitize_column("  P-code ") == "p_code"
        assert sanitize_column("()") == "col"

    def test_hdx_version_va_tim_resource(self) -> None:
        from ingestion.common import hdx

        package = {"name": "cod-ps-phl", "resources": [
            {"name": "phl_admpop_adm2_2022_v2.csv", "last_modified": "2026-01-26T10:15:00.123"},
            {"name": "phl_admpop_adm2_2025.csv", "last_modified": "2026-05-28T08:00:00"},
            {"name": "phl_admpop_adm1_2025.csv"},
        ]}
        found = hdx.find_resources(package, r"^phl_admpop_adm2_\d{4}.*\.csv$")
        assert [r["name"] for r in found] == ["phl_admpop_adm2_2022_v2.csv", "phl_admpop_adm2_2025.csv"]
        assert max(found, key=lambda r: r["name"])["name"] == "phl_admpop_adm2_2025.csv"
        assert hdx.version_of(found[0]) == "20260126T101500"
        with pytest.raises(LookupError):
            hdx.find_resources(package, r"\.geojson$")

    def test_zenodo_lay_file_xlsx(self) -> None:
        from ingestion import trends_th

        record = {"id": 20269896, "files": [{"key": "README.md"}, {"key": "TRENDS.xlsx"}]}
        assert trends_th.latest_file(record) == ("20269896", {"key": "TRENDS.xlsx"})
        with pytest.raises(IngestionValidationError):
            trends_th.latest_file({"id": 1, "files": [{"key": "a.csv"}]})

    def test_sg_nea_lam_phang_feature(self) -> None:
        from ingestion import sg_nea

        feature = {"type": "Feature", "properties": {"LOCALITY": "Ho Ching Rd", "CASE_SIZE": 80,
                                                     "HOMES": None},
                   "geometry": {"type": "Polygon", "coordinates": [[[103.7, 1.3]]]}}
        record = sg_nea.feature_to_record(feature, "2026-09-29T00:00:00+00:00")
        assert record["CASE_SIZE"] == "80" and record["HOMES"] == ""
        assert '"Polygon"' in record["geometry"]
        assert record["_fetched_at"] == "2026-09-29T00:00:00+00:00"
