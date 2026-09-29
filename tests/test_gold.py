"""Test Gold: chon chuoi khong cong trung, khoa dim, z-score nguy co.

Du lieu o day la chuoi nho tu dung trong test (co y don gian de tinh tay),
khong phai du lieu that.
"""

from datetime import date

import pytest
from pyspark.sql import functions as F

from transform.common import small_frame
from transform import names
from transform.gold import dims, facts, risk
from transform.silver.cases import COLUMNS, FLAG_FUTURE

pytestmark = pytest.mark.integration

TODAY = date(2026, 9, 29)


def case_row(**values) -> dict:
    """Mot dong Silver dengue_cases voi gia tri mac dinh."""
    row = {c: None for c in COLUMNS}
    row.update(
        source="who_gho", country_iso3="VNM", adm_level=0, location_name="Viet Nam",
        period_type="month", case_definition="Total", dq_flags=[],
        bronze_ingestion_date="2026-09-29",
    )
    row.update(values)
    return row


SILVER_SCHEMA = (
    "source string, country_iso3 string, adm_level int, region_name string, "
    "province_name string, location_name string, rne_iso_code string, period_type string, "
    "week_system string, period_start date, period_end date, year int, cases bigint, "
    "deaths bigint, confirmed_cases bigint, case_definition string, source_ref string, "
    "bronze_ingestion_date string, dq_flags array<string>"
)


def silver(spark, rows: list[dict]):
    """Dung DataFrame Silver tu danh sach dong."""
    return small_frame(spark, [tuple(r[c] for c in COLUMNS) for r in rows], SILVER_SCHEMA)


def week(start: date, cases: int, system: str = "isoweek") -> dict:
    """Mot dong tuan."""
    return case_row(period_type="week", week_system=system, period_start=start, cases=cases,
                    year=start.year)


class TestChonChuoi:
    def test_thang_uu_tien_hon_tuan_khong_cong_trung(self, spark) -> None:
        rows = [
            case_row(period_start=date(2026, 7, 1), cases=1000, year=2026),
            week(date(2026, 7, 6), 250), week(date(2026, 7, 13), 250),
        ]
        dim = dims.build_dim_country(spark, silver(spark, rows))
        fact = facts.build_fact_monthly_cases(silver(spark, rows), dim).first()
        assert fact["cases"] == 1000
        assert fact["series_used"] == "month/-/Total"

    def test_chi_co_tuan_thi_cong_isoweek_bo_epiweek(self, spark) -> None:
        rows = [week(date(2026, 7, d), 100) for d in (6, 13, 20, 27)] + [
            week(date(2026, 7, d), 999, "epiweek") for d in (5, 12, 19, 26)
        ]
        dim = dims.build_dim_country(spark, silver(spark, rows))
        fact = facts.build_fact_monthly_cases(silver(spark, rows), dim).first()
        assert fact["cases"] == 400
        assert fact["periods_in_month"] == 4 and fact["is_complete"]

    def test_dong_tuong_lai_bi_loai(self, spark) -> None:
        rows = [case_row(period_start=date(2029, 6, 1), cases=5, year=2029,
                         dq_flags=[FLAG_FUTURE])]
        dim = dims.build_dim_country(spark, silver(spark, rows))
        assert facts.build_fact_monthly_cases(silver(spark, rows), dim).count() == 0


class TestDimVaKhoa:
    def test_dim_country_co_unk_va_11_nuoc(self, spark) -> None:
        dim = dims.build_dim_country(spark, silver(spark, [case_row(period_start=date(2026, 1, 1))]))
        assert dim.count() == 12
        assert dim.where("iso3 = 'UNK'").first()["country_key"] == dims.UNKNOWN_COUNTRY_KEY
        assert dim.where("iso3 = 'VNM'").first()["has_who_data"] is True
        assert dim.where("iso3 = 'PHL'").first()["has_who_data"] is False

    def test_tin_khong_gan_nuoc_vao_unk(self, spark) -> None:
        news = small_frame(spark, 
            [("2026-09-20 00:00:00", [], True), ("2026-09-20 00:00:00", ["VNM", "THA"], True)],
            "published_at string, countries array<string>, is_recent boolean",
        ).withColumn("published_at", F.to_timestamp("published_at"))
        dim = dims.build_dim_country(spark, silver(spark, [case_row(period_start=date(2026, 1, 1))]))
        fact = facts.build_fact_daily_news(news, dim)
        keys = {r["country_key"]: r["article_count"] for r in fact.collect()}
        assert keys[dims.UNKNOWN_COUNTRY_KEY] == 1
        assert len(keys) == 3


class TestNguyCo:
    def _risk(self, spark, history: list[int], current: int):
        rows = [
            case_row(period_start=date(2021 + i, 7, 1), cases=c, year=2021 + i)
            for i, c in enumerate(history)
        ] + [case_row(period_start=date(2021 + len(history), 7, 1), cases=current,
                      year=2021 + len(history))]
        cases = silver(spark, rows)
        dim_country = dims.build_dim_country(spark, cases)
        dim_date = dims.build_dim_date(spark, date(2021, 1, 1), TODAY)
        monthly = facts.build_fact_monthly_cases(cases, dim_country)
        news = small_frame(spark, [], "country_key int, date_key int, article_count long, "
                                         "recent_article_count long")
        result = risk.build_country_risk(monthly, news, dim_country, dim_date, TODAY)
        return result.where("iso3 = 'VNM'").first()

    def test_vuot_2_do_lech_chuan_la_cao(self, spark) -> None:
        row = self._risk(spark, [100, 110, 90, 105, 95], 300)
        assert row["n_baseline_years"] == 5
        assert row["baseline_mean"] == 100.0
        assert row["risk_level"] == risk.LEVEL_HIGH

    def test_binh_thuong_la_thap(self, spark) -> None:
        row = self._risk(spark, [100, 110, 90, 105, 95], 101)
        assert row["risk_level"] == risk.LEVEL_LOW

    def test_it_hon_3_nam_la_khong_du_du_lieu(self, spark) -> None:
        row = self._risk(spark, [100, 110], 300)
        assert row["risk_level"] == risk.LEVEL_NOT_ENOUGH

    def test_nuoc_khong_co_so_lieu(self, spark) -> None:
        cases = silver(spark, [case_row(period_start=date(2026, 7, 1), cases=1, year=2026)])
        dim_country = dims.build_dim_country(spark, cases)
        dim_date = dims.build_dim_date(spark, date(2026, 1, 1), TODAY)
        monthly = facts.build_fact_monthly_cases(cases, dim_country)
        news = small_frame(spark, [], "country_key int, date_key int, article_count long, "
                                         "recent_article_count long")
        result = risk.build_country_risk(monthly, news, dim_country, dim_date, TODAY)
        assert result.count() == 11
        assert result.where("iso3 = 'BRN'").first()["risk_level"] == risk.LEVEL_NO_DATA


def _country_risk(spark, rows: list[dict]):
    """Chay chuoi Silver -> fact -> country_risk tren cac dong cho truoc, tra dong VNM."""
    cases = silver(spark, rows)
    dim_country = dims.build_dim_country(spark, cases)
    dim_date = dims.build_dim_date(spark, date(2020, 1, 1), TODAY)
    monthly = facts.build_fact_monthly_cases(cases, dim_country)
    news = small_frame(spark, [], "country_key int, date_key int, article_count long, "
                                  "recent_article_count long")
    return risk.build_country_risk(monthly, news, dim_country, dim_date, TODAY).where(
        "iso3 = 'VNM'").first()


class TestBaselineHopNhat:
    def test_who_ngan_thi_bu_lich_su_opendengue(self, spark) -> None:
        # WHO chi co 2024-2026 (nhu 6/9 nuoc that); OpenDengue co 2021-2024.
        who = [case_row(period_start=date(y, 7, 1), cases=c, year=y)
               for y, c in ((2024, 100), (2025, 100), (2026, 400))]
        od = [case_row(source="opendengue", location_name="VIET NAM",
                       period_start=date(y, 7, 1), cases=c, year=y)
              for y, c in ((2021, 90), (2022, 110), (2023, 100), (2024, 999))]
        row = _country_risk(spark, who + od)
        assert row["source"] == "who_gho"
        assert row["cases"] == 400
        # 2024 co ca hai nguon -> lay WHO (100), khong lay OpenDengue (999)
        assert row["n_baseline_years"] == 5
        assert row["baseline_mean"] == 100.0
        assert row["baseline_sources"] == "opendengue+who_gho"
        assert row["risk_level"] == risk.LEVEL_HIGH


class TestDoTreBaoCao:
    def test_thang_moi_nhat_tut_manh_thi_lui_ve_thang_on_dinh(self, spark) -> None:
        # Nhu Indonesia that: thang 6 du, thang 7 va 8 chua bao cao du.
        history = [case_row(period_start=date(y, 6, 1), cases=100, year=y) for y in range(2021, 2026)]
        recent = [case_row(period_start=date(2026, m, 1), cases=c, year=2026)
                  for m, c in ((5, 110), (6, 300), (7, 120), (8, 5))]
        row = _country_risk(spark, history + recent)
        assert str(row["data_as_of"]) == "2026-06-01"
        assert str(row["latest_reported_month"]) == "2026-08-01"
        assert row["skipped_recent_months"] == 2
        # baseline cac nam deu 100 (do lech chuan 0) -> so truc tiep: 300 > 100 la cao
        assert row["risk_level"] == risk.LEVEL_HIGH


class TestSoKhopTenTinh:
    """So khop ten tinh bang PySpark (levenshtein) - cap ten lay tu du lieu that 29/9/2026."""

    @pytest.mark.parametrize(
        ("a", "b", "expected"),
        [
            ("BELAIT DISTRICT", "Belait", True),
            ("BANGKOK", "Bangkok Metropolis", True),
            ("SAMUT PRAKARN", "Samut Prakan", True),
            ("BATTAMBANG", "Batdâmbâng", True),
            ("W.P. LABUAN", "Labuan", True),
            ("DKI JAKARTA", "Jakarta Raya", True),
            ("D.I YOGYA", "Yogyakarta", True),
            ("Bà Rịa - Vũng Tàu", "BA RIA VUNG TAU", True),
            ("PROVINCE OF ALBAY", "Camarines Norte", False),   # ma sai that o PH
            ("PROVINCE OF CAMARINES SUR", "Camarines Norte", False),
            ("KALIMANTAN UTARA", "Kalimantan Timur", False),   # tinh moi tach 2012
            ("NAY PYI TAW", "Mandalay", False),
            ("MONGAR", "Mon", False),
        ],
    )
    def test_name_similarity(self, spark, a: str, b: str, expected: bool) -> None:
        frame = small_frame(spark, [(a, b)], "a string, b string")
        score = frame.select(names.name_similarity(F.col("a"), F.col("b"))).first()[0]
        assert (score >= names.MIN_SIMILARITY) is expected, f"{a} ~ {b} = {score:.2f}"

    def test_eth_cua_natural_earth_bo_dau_nhu_d_gach(self, spark) -> None:
        frame = small_frame(spark, [("Ðong Tháp",), ("Đồng Tháp",)], "a string")
        assert {r[0] for r in frame.select(names.normalized_name(F.col("a"))).collect()} == {"DONGTHAP"}
