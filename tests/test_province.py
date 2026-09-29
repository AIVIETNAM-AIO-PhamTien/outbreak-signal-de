"""Test Silver/Gold cap tinh: noi so ca vao don vi, khong cong trung, gazetteer tin tuc.

Du lieu nho tu dung, mo phong cac truong hop THAT da gap 29/9/2026.
"""

from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from transform.common import small_frame
from transform.gold import units as gold_units
from transform.silver import province
from transform.silver.admin import UNIT_COLUMNS
from transform.silver.cases import COLUMNS as CASE_COLUMNS

pytestmark = pytest.mark.integration

UNIT_SCHEMA = (
    "iso3 string, unit_id string, unit_system string, admin_level int, unit_name string, "
    "local_name string, local_lang string, region_name string, x double, y double, "
    "valid_from date, valid_to date, source_version string"
)
REFORM = date(2025, 7, 1)
OLD_END = date(2025, 6, 30)


@pytest.fixture
def admin_units(spark):
    def u(iso3, uid, system, name, local=None, lang=None, vf=None, vt=None, level=1):
        return (iso3, uid, system, level, name, local, lang, None, 0.0, 0.0, vf, vt, "t")
    rows = [
        u("PHL", "PH05005", "pcode", "Albay", level=2),
        u("PHL", "PH05016", "pcode", "Camarines Norte", level=2),
        u("THA", "TH10", "pcode", "Bangkok", "กรุงเทพมหานคร", "th"),
        u("VNM", "VN-DN", "iso_3166_2", "Dong Nai", "Đồng Nai", "vi", vt=OLD_END),
        u("VNM", "VN-BP", "iso_3166_2", "Binh Phuoc", "Bình Phước", "vi", vt=OLD_END),
        u("VNM", "VN75", "pcode", "Dong Nai", "Đồng Nai", "vi", vf=REFORM),
    ]
    assert len(rows[0]) == len(UNIT_COLUMNS)
    return small_frame(spark, rows, UNIT_SCHEMA)


@pytest.fixture
def crosswalk(spark):
    rows = [
        # Ma sai that o PH: RNE "PH-CAN" (Camarines Norte) duoc gan cho ca tinh Albay.
        ("PHL", "PH-CAN", "PH05016", "cod_centroid_in_ne"),
        ("VNM", "VN-DN", "VN75", "ne_centroid_in_cod"),
        ("VNM", "VN-BP", "VN75", "ne_centroid_in_cod"),  # Binh Phuoc gop vao Dong Nai moi
    ]
    return small_frame(spark, rows, "iso3 string, iso_3166_2 string, pcode string, method string")


def od_row(iso3, province_name, rne, start, cases, level=1):
    """Mot dong Silver dengue_cases cua OpenDengue cap tinh."""
    row = {c: None for c in CASE_COLUMNS}
    row.update(source="opendengue", country_iso3=iso3, adm_level=level, province_name=province_name,
               location_name=f"{iso3}, {province_name}", rne_iso_code=rne, period_type="month",
               period_start=start, period_end=start.replace(day=28), year=start.year, cases=cases,
               case_definition="Total", dq_flags=[], bronze_ingestion_date="2026-09-29")
    return tuple(row[c] for c in CASE_COLUMNS)


CASE_SCHEMA = (
    "source string, country_iso3 string, adm_level int, region_name string, "
    "province_name string, location_name string, rne_iso_code string, period_type string, "
    "week_system string, period_start date, period_end date, year int, cases bigint, "
    "deaths bigint, confirmed_cases bigint, case_definition string, source_ref string, "
    "bronze_ingestion_date string, dq_flags array<string>"
)


@pytest.fixture
def province_cases(spark, admin_units, crosswalk):
    jan = date(2010, 1, 1)
    od = small_frame(spark, [
        od_row("PHL", "PROVINCE OF ALBAY", "PH-CAN", jan, 10, level=2),        # ma sai
        od_row("PHL", "PROVINCE OF CAMARINES NORTE", "PH-CAN", jan, 20, level=2),
        od_row("VNM", "DONG NAI", "VN-DN", jan, 100),
        od_row("VNM", "DONG NAI PROVINCE", "VN-DN", jan, 100),                  # bien the ten
        od_row("VNM", "BINH PHUOC", "VN-BP", jan, 30),
    ], CASE_SCHEMA)
    trends = small_frame(spark, [("2016", "2016-01-03 00:00:00", "2016-01-09 00:00:00", "TH10",
                                  "Bangkok", "640")],
                         "year string, week_start string, week_end string, p_code string, "
                         "province string, dengue_total string")
    doh = small_frame(spark, [("ALBAY", "15", "0", "1/10/2016", "V"),
                              ("ANGELES CITY", "3", "0", "1/10/2016", "III")],
                      "loc string, cases string, deaths string, date string, Region string")
    return province.build_province_cases(od, trends, doh, admin_units, crosswalk).cache()


class TestNoiDonVi:
    def test_ma_sai_thi_noi_theo_ten(self, province_cases) -> None:
        row = province_cases.where("source_location = 'PROVINCE OF ALBAY'").first()
        assert row["unit_id"] == "PH05005" and row["match_method"] == "name"
        ok = province_cases.where("source_location = 'PROVINCE OF CAMARINES NORTE'").first()
        assert ok["unit_id"] == "PH05016" and ok["match_method"] == "code+name"

    def test_tinh_cu_vn_quy_ve_tinh_moi(self, province_cases) -> None:
        row = province_cases.where("source_location = 'BINH PHUOC'").first()
        assert row["unit_id"] == "VN-BP" and row["current_unit_id"] == "VN75"

    def test_trends_noi_thang_pcode(self, province_cases) -> None:
        row = province_cases.where("source = 'trends_th'").first()
        assert (row["unit_id"], row["cases"], row["period_end"]) == ("TH10", 640, date(2016, 1, 9))

    def test_thanh_pho_ph_khong_phai_tinh_thi_khong_noi(self, province_cases) -> None:
        doh = {r["source_location"]: r["unit_id"] for r in
               province_cases.where("source = 'ph_doh'").collect()}
        assert doh == {"ALBAY": "PH05005", "ANGELES CITY": None}


class TestKhongCongTrung:
    def test_bien_the_ten_lay_max_tinh_cu_khac_nhau_thi_cong(self, spark, province_cases,
                                                            admin_units, crosswalk) -> None:
        current = province.current_units(admin_units, crosswalk)
        population = small_frame(spark, [], "iso3 string, unit_id string, population long, "
                                            "reference_year int, match_method string, match_score double")
        dim_country = small_frame(spark, [(1, "VNM"), (2, "PHL"), (3, "THA")],
                                  "country_key int, iso3 string")
        dim_unit = gold_units.build_dim_admin_unit(admin_units, population, current, dim_country)
        fact = gold_units.build_fact_unit_monthly_cases(province_cases, dim_unit)
        key = dim_unit.where("unit_id = 'VN75'").first()["unit_key"]
        row = fact.where(F.col("unit_key") == key).first()
        # Dong Nai: 2 bien the ten cung 100 -> max 100 (khong phai 200); + Binh Phuoc 30.
        assert row["cases"] == 130


class TestTinTucCapTinh:
    @pytest.fixture
    def mentions(self, spark, admin_units, crosswalk):
        news = small_frame(spark, [
            ("a1", "Sốt xuất huyết tăng ở Bình Phước", "", ["VNM"], datetime(2026, 9, 20)),
            ("a2", "ไข้เลือดออกระบาดในกรุงเทพมหานคร", "", ["THA"], datetime(2026, 9, 21)),
            ("a3", "Dengue in Albayrak street", "", ["PHL"], datetime(2026, 9, 22)),
        ], "article_id string, title string, description_text string, countries array<string>, "
           "published_at timestamp")
        return {(r["article_id"], r["current_unit_id"]) for r in
                province.build_news_unit_mentions(news, admin_units, crosswalk).collect()}

    def test_ten_tinh_cu_sau_sap_nhap_quy_ve_tinh_moi(self, mentions) -> None:
        assert ("a1", "VN75") in mentions

    def test_chu_thai_khop_chuoi_con(self, mentions) -> None:
        assert ("a2", "TH10") in mentions

    def test_bien_tu_khong_khop_giua_tu(self, mentions) -> None:
        assert not any(article == "a3" for article, _ in mentions)


class TestUnitRisk:
    def test_so_ca_cu_khong_xep_muc_nguy_co(self, spark) -> None:
        from transform.gold import dims

        dim_date = dims.build_dim_date(spark, date(2004, 1, 1), date(2026, 12, 31))
        dim_unit = small_frame(spark, [(1, 1, "VNM", "VN-NB", "Ninh Binh", None, 1000),
                                       (2, 2, "THA", "TH10", "Bangkok", None, 1000)],
                               "unit_key int, country_key int, iso3 string, unit_id string, "
                               "unit_name string, local_name string, population long")
        # Ca hai deu co baseline = 0 o 5 nam truoc; khac nhau o do moi cua so ca.
        rows = [(1, y * 10000 + 1201, "opendengue", 0) for y in range(2005, 2010)]
        rows += [(1, 20101201, "opendengue", 4)]
        rows += [(2, y * 10000 + 801, "trends_th", 0) for y in range(2021, 2026)]
        rows += [(2, 20260801, "trends_th", 4)]
        fact = small_frame(spark, [(*r, None, "month/-/Total", 1, True) for r in rows],
                           "unit_key int, month_date_key int, source string, cases long, "
                           "deaths long, series_used string, periods_in_month long, "
                           "is_complete boolean")
        news = small_frame(spark, [], "unit_key int, date_key int, article_count long")
        risk = gold_units.build_unit_risk(fact, news, dim_unit, dim_date, date(2026, 9, 29))
        got = {r["unit_id"]: (r["case_level"], r["signal"]) for r in risk.collect()}
        assert got == {"VN-NB": ("số liệu cũ", "tin tức"), "TH10": ("cao", "ca bệnh")}
