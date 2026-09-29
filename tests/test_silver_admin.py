"""Test Silver admin_units (SCD2) va admin_crosswalk tren du lieu nho tu dung."""

import json
from datetime import date

import pytest

from transform.common import small_frame
from transform.silver import admin

pytestmark = pytest.mark.integration

AB_SCHEMA = (
    "iso3 string, _sheet string, admin_level string, adm1_pcode string, adm1_name string, "
    "adm1_name1 string, adm2_pcode string, adm2_name string, adm2_name1 string, lang1 string, "
    "center_lon string, center_lat string, x_coord string, y_coord string, _version string"
)


def square(x0: float, y0: float, size: float = 1.0) -> str:
    """GeoJSON polygon vuong."""
    return json.dumps({"type": "Polygon", "coordinates": [[
        [x0, y0], [x0 + size, y0], [x0 + size, y0 + size], [x0, y0 + size], [x0, y0]]]})


@pytest.fixture
def bronze_ab(spark):
    rows = [
        # VNM: admin1 khong co toa do -> lay tu adminpoints
        ("VNM", "admin1", None, "VN01", "Ha Noi", "Hà Nội", None, None, None, "vi",
         None, None, None, None, "v2"),
        ("VNM", "adminpoints", "1", "VN01", None, None, None, None, None, None,
         None, None, "105.5", "21.5", "v2"),
        # PHL: tinh la cap 2
        ("PHL", "admin1", None, "PH01", "Ilocos", None, None, None, None, None,
         "120.5", "16.5", None, None, "v1"),
        ("PHL", "admin2", None, "PH01", "Ilocos", None, "PH0128", "Ilocos Norte", None, None,
         "120.6", "18.1", None, None, "v1"),
    ]
    return small_frame(spark, rows, AB_SCHEMA)


@pytest.fixture
def ne_features(spark):
    rows = [
        # 2 tinh cu cua VN, tam deu nam trong o vuong cua VN01 moi
        ("VNM", "VN-HN", "Ha Noi", "Hà Nội", 105.2, 21.2, square(105.0, 21.0, 0.5)),
        ("VNM", "VN-HT", "Ha Tay", "Hà Tây", 105.7, 21.7, square(105.5, 21.5, 0.5)),
        ("PHL", "PH-ILN", "Ilocos Norte", None, 120.6, 18.1, square(120.0, 18.0)),
    ]
    return small_frame(spark, rows, "iso3 string, iso_3166_2 string, name string, "
                                    "name_vi string, longitude double, latitude double, "
                                    "geometry string")


class TestAdminUnits:
    def test_toa_do_lay_tu_adminpoints_khi_trong(self, bronze_ab) -> None:
        row = admin.cod_units(bronze_ab).where("iso3 = 'VNM'").first()
        assert (row["x"], row["y"]) == (105.5, 21.5)
        assert row["local_name"] == "Hà Nội"

    def test_philippines_lay_cap_2(self, bronze_ab) -> None:
        rows = admin.cod_units(bronze_ab).where("iso3 = 'PHL'").collect()
        assert [(r["unit_id"], r["admin_level"], r["region_name"]) for r in rows] == [
            ("PH0128", 2, "Ilocos")]

    def test_hieu_luc_viet_nam(self, bronze_ab, ne_features) -> None:
        units = admin.build_admin_units(bronze_ab, ne_features).where("iso3 = 'VNM'")
        new = units.where("unit_system = 'pcode'").first()
        old = units.where("unit_id = 'VN-HT'").first()
        assert new["valid_from"] == date(2025, 7, 1) and new["valid_to"] is None
        assert old["valid_to"] == date(2025, 6, 30) and old["local_name"] == "Hà Tây"


class TestCrosswalk:
    def test_noi_tinh_cu_vao_tinh_moi_va_iso_sang_pcode(self, spark, bronze_ab, ne_features) -> None:
        units = admin.build_admin_units(bronze_ab, ne_features)
        geometry = small_frame(spark, [("VNM", "VN01", square(105.0, 21.0, 1.0))],
                               "iso3 string, adm1_pcode string, geometry string")
        pairs = {(r["iso3"], r["iso_3166_2"], r["pcode"], r["method"])
                 for r in admin.build_crosswalk(units, ne_features, geometry).collect()}
        assert pairs == {
            ("VNM", "VN-HN", "VN01", "ne_centroid_in_cod"),
            ("VNM", "VN-HT", "VN01", "ne_centroid_in_cod"),   # 2 tinh cu -> 1 tinh moi
            ("PHL", "PH-ILN", "PH0128", "cod_centroid_in_ne"),
        }


def test_sua_ten_vung_natural_earth_ghi_nham_cho_tinh(spark) -> None:
    ne = small_frame(spark, [("VNM", "VN-39", "Đông Nam Bộ", "Đông Nam Bộ", 107.2, 11.1, square(107.0, 11.0))],
                     "iso3 string, iso_3166_2 string, name string, name_vi string, "
                     "longitude double, latitude double, geometry string")
    row = admin.vn_old_units(ne).first()
    assert (row["unit_name"], row["local_name"]) == ("Dong Nai", "Đồng Nai")
