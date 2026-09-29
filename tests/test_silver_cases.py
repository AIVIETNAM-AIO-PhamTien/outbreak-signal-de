"""Test Silver dengue_cases tren DataFrame nho dung theo dung schema Bronze."""

from datetime import date

import pytest

from transform.common import small_frame
from transform.silver import cases as sc

pytestmark = pytest.mark.integration

TODAY = date(2026, 9, 29)

OD_COLUMNS = (
    "adm_0_name", "adm_1_name", "adm_2_name", "full_name", "ISO_A0", "FAO_GAUL_code",
    "RNE_iso_code", "IBGE_code", "calendar_start_date", "calendar_end_date", "Year",
    "dengue_total", "case_definition_standardised", "S_res", "T_res", "UUID", "ingestion_date",
)


def od_row(**overrides: str) -> tuple:
    """Mot dong Bronze OpenDengue (moi gia tri la chuoi, nhu Bronze that)."""
    row = {
        "adm_0_name": "VIET NAM", "adm_1_name": "NA", "adm_2_name": "NA",
        "full_name": "VIET NAM", "ISO_A0": "VNM", "FAO_GAUL_code": "264",
        "RNE_iso_code": "VNM", "IBGE_code": "NA", "calendar_start_date": "2020-01-01",
        "calendar_end_date": "2020-01-31", "Year": "2020", "dengue_total": "120",
        "case_definition_standardised": "Total", "S_res": "Admin0", "T_res": "Month",
        "UUID": "MOH-VNM-2020-M01-01", "ingestion_date": "2026-09-29",
    }
    row.update(overrides)
    return tuple(row[c] for c in OD_COLUMNS)


WHO_SCHEMA = (
    # Bronze WHO doc moi cot la chuoi (primitivesAsString) - giong het o day.
    "COUNTRY string, ISO3 string, YEAR string, DATE_TYPE string, DATE_NUM string, "
    "START_DATE string, CASES string, CONFIRMED_CASES string, DEATHS string, "
    "ingestion_date string"
)


@pytest.fixture
def od_frame(spark):
    rows = [
        od_row(),
        # Philippines: vung o adm_1, tinh o adm_2
        od_row(adm_0_name="PHILIPPINES", ISO_A0="PHL", adm_1_name="ILOCOS (REGION I)",
               adm_2_name="PROVINCE OF PANGASINAN", full_name="PHILIPPINES, ILOCOS, PANGASINAN",
               RNE_iso_code="PH-PAN", S_res="Admin2"),
        # Thai Lan: tinh o adm_1
        od_row(adm_0_name="THAILAND", ISO_A0="THA", adm_1_name="BURIRAM",
               full_name="THAILAND, BURIRAM", RNE_iso_code="TH-31", S_res="Admin1"),
        od_row(dengue_total="abc", full_name="VIET NAM X"),
        od_row(calendar_start_date="2020-03-01", calendar_end_date="2020-08-31",
               full_name="VIET NAM Y"),  # ghi Month nhung dai 6 thang
    ]
    return small_frame(spark, rows, list(OD_COLUMNS))


@pytest.fixture
def who_frame(spark):
    rows = [
        ("Indonesia", "IDN", "2008", "month", "3", None, "500", None, "5", "2026-09-29"),
        ("Malaysia", "MYS", "2029", "epiweek", "23", "2029-06-03", "2732", None, None, "2026-09-29"),
        ("Cambodia", "KHM", "2025", "isoweek", "3", "2025-01-13", "180", "3683", "0", "2026-09-29"),
        ("Viet Nam", "VNM", "2026", "month", "7", "2026-07-01", "9000", "100", "3", "2026-09-29"),
    ]
    return small_frame(spark, rows, WHO_SCHEMA)


class TestOpenDengue:
    def test_ep_kieu_va_na_thanh_null(self, od_frame) -> None:
        row = sc.opendengue_to_cases(od_frame, TODAY).where("location_name = 'VIET NAM'").first()
        assert row["cases"] == 120
        assert row["period_start"] == date(2020, 1, 1)
        assert row["province_name"] is None and row["region_name"] is None
        assert row["adm_level"] == 0
        assert row["dq_flags"] == []

    def test_philippines_tinh_nam_o_adm2(self, od_frame) -> None:
        row = sc.opendengue_to_cases(od_frame, TODAY).where("country_iso3 = 'PHL'").first()
        assert row["region_name"] == "ILOCOS (REGION I)"
        assert row["province_name"] == "PROVINCE OF PANGASINAN"
        assert row["adm_level"] == 2

    def test_nuoc_khac_tinh_nam_o_adm1(self, od_frame) -> None:
        row = sc.opendengue_to_cases(od_frame, TODAY).where("country_iso3 = 'THA'").first()
        assert row["province_name"] == "BURIRAM"
        assert row["region_name"] is None

    def test_gan_co_so_ca_khong_phai_so_va_ky_bat_thuong(self, od_frame) -> None:
        frame = sc.opendengue_to_cases(od_frame, TODAY)
        bad_cases = frame.where("location_name = 'VIET NAM X'").first()
        irregular = frame.where("location_name = 'VIET NAM Y'").first()
        assert sc.FLAG_CASES_NOT_NUMERIC in bad_cases["dq_flags"]
        assert bad_cases["cases"] is None
        assert sc.FLAG_IRREGULAR in irregular["dq_flags"]


class TestWho:
    def test_suy_start_date_khi_trong(self, who_frame) -> None:
        row = sc.who_to_cases(who_frame, TODAY).where("country_iso3 = 'IDN'").first()
        assert row["period_start"] == date(2008, 3, 1)
        assert row["period_end"] == date(2008, 3, 31)
        assert sc.FLAG_START_DERIVED in row["dq_flags"]

    def test_gan_co_ky_tuong_lai(self, who_frame) -> None:
        row = sc.who_to_cases(who_frame, TODAY).where("country_iso3 = 'MYS'").first()
        assert sc.FLAG_FUTURE in row["dq_flags"]
        assert row["period_type"] == "week" and row["week_system"] == "epiweek"

    def test_gan_co_xac_nhan_lon_hon_tong(self, who_frame) -> None:
        row = sc.who_to_cases(who_frame, TODAY).where("country_iso3 = 'KHM'").first()
        assert sc.FLAG_CONFIRMED_GT_CASES in row["dq_flags"]
        assert row["period_end"] == date(2025, 1, 19)

    def test_dong_sach_khong_co_co(self, who_frame) -> None:
        row = sc.who_to_cases(who_frame, TODAY).where("country_iso3 = 'VNM'").first()
        assert row["dq_flags"] == []
        assert row["case_definition"] == "Total"


class TestDedup:
    def test_trung_khoa_giu_dong_so_ca_lon_nhat(self, spark, od_frame, who_frame) -> None:
        doubled = od_frame.unionByName(
            od_frame.where("full_name = 'VIET NAM'").withColumn(
                "dengue_total", od_frame["dengue_total"].substr(1, 1)))  # "1" < "120"
        kept, dropped = sc.build_dengue_cases(doubled, who_frame, TODAY)
        rows = kept.where("location_name = 'VIET NAM'").collect()
        assert len(rows) == 1 and rows[0]["cases"] == 120
        assert dropped.count() == 1
