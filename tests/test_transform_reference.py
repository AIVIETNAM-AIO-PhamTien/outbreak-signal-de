"""Test bang tham chieu quoc gia, ranh gioi va chuan hoa ten - khong can JVM."""

import json

import pytest

from ingestion.common.config import source_config
from transform import boundaries
from transform.reference import BY_ISO3, COUNTRIES, match_countries


class TestBangThamChieu:
    def test_du_11_nuoc_khong_trung_ma(self) -> None:
        assert len(COUNTRIES) == 11
        assert len(BY_ISO3) == 11

    def test_ten_opendengue_khop_filter_trong_config(self) -> None:
        expected = set(source_config("opendengue")["filter_countries"])
        assert {c.opendengue_name for c in COUNTRIES} == expected

    def test_iso3_khop_filter_who_trong_config(self) -> None:
        assert set(BY_ISO3) == set(source_config("who_gho")["filter_iso3"])

    def test_who_khong_co_philippines_va_brunei(self) -> None:
        assert BY_ISO3["PHL"].who_name is None
        assert BY_ISO3["BRN"].who_name is None


class TestGanNuocChoTinTuc:
    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("Dengue surges in Vietnam and Thailand", ["THA", "VNM"]),
            ("Thai health officials warn", ["THA"]),
            ("DENGUE IN MALAYSIAN STATES", ["MYS"]),
            ("Outbreak in Phnom Penh", ["KHM"]),
            ("Burmese refugees", ["MMR"]),
            ("Dengue in Southeast Asia", []),
            ("Thailander", []),  # bien tu: khong khop mot phan cua tu
            ("", []),
            (None, []),
        ],
    )
    def test_match_countries(self, text: str | None, expected: list[str]) -> None:
        assert match_countries(text) == expected


def _feature(iso3: str, code: str, coords: list) -> dict:
    """Mot feature Natural Earth toi gian."""
    return {
        "type": "Feature",
        "properties": {"adm0_a3": iso3, "iso_3166_2": code, "name": code, "type_en": "Province",
                       "extra": "bo di"},
        "geometry": {"type": "Polygon", "coordinates": coords},
    }


class TestRanhGioi:
    def test_loc_chi_giu_sea_va_lam_tron(self) -> None:
        collection = {
            "type": "FeatureCollection",
            "features": [
                _feature("VNM", "VN-01", [[[105.123456, 21.987654], [106.0, 22.0]]]),
                _feature("JPN", "JP-13", [[[139.0, 35.0], [140.0, 36.0]]]),
            ],
        }
        result = boundaries.filter_sea(collection)
        assert [f["properties"]["iso_3166_2"] for f in result["features"]] == ["VN-01"]
        assert result["features"][0]["geometry"]["coordinates"][0][0] == [105.123, 21.988]
        assert "extra" not in result["features"][0]["properties"]

    def test_da_co_file_thi_khong_tai_lai(self, tmp_path, monkeypatch) -> None:
        (tmp_path / boundaries.SEA_NAME).write_text(json.dumps({"features": []}))

        def fail(*args, **kwargs):
            raise AssertionError("khong duoc goi mang khi file da co")

        monkeypatch.setattr(boundaries.requests, "get", fail)
        assert boundaries.ensure_boundaries(tmp_path) == tmp_path / boundaries.SEA_NAME
