"""Test app HealthMap: chay that app Streamlit tren mot Gold nho dung trong tmp_path.

Gold nho duoc ghi bang `deltalake` (khong can JVM), dung cung dinh dang Delta
ma Spark ghi, nen app doc duoc y nhu voi Gold that.
"""

import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest
from deltalake import write_deltalake

APP = str(Path(__file__).resolve().parents[1] / "app" / "healthmap.py")


def _gold_tables() -> dict[str, pd.DataFrame]:
    """Cac bang Gold toi thieu ma app doc."""
    return {
        "dim_country": pd.DataFrame({
            "country_key": [0, 1, 2], "iso3": ["UNK", "THA", "VNM"],
            "country_name": ["Unknown", "Thailand", "Viet Nam"],
            "opendengue_name": [None, "THAILAND", "VIET NAM"],
            "who_name": [None, "Thailand", "Viet Nam"], "has_who_data": [False, True, True],
        }),
        "country_risk": pd.DataFrame({
            "country_key": [1, 2], "iso3": ["THA", "VNM"],
            "country_name": ["Thailand", "Viet Nam"], "source": ["who_gho", "who_gho"],
            "data_as_of": [date(2026, 8, 1), date(2026, 8, 1)],
            "latest_reported_month": [date(2026, 8, 1), date(2026, 8, 1)],
            "skipped_recent_months": [0, 0], "cases": [900, 3000],
            "baseline_mean": [1000.0, 1000.0], "baseline_std": [100.0, 200.0],
            "n_baseline_years": [5, 5], "z_score": [-1.0, 10.0],
            "ratio_to_baseline": [0.9, 3.0], "risk_level": ["thấp", "cao"],
            "news_30d": [2, 5], "news_prev_30d": [1, 1],
        }),
        "fact_monthly_cases": pd.DataFrame({
            "country_key": [2, 2], "month_date_key": [20260701, 20260801],
            "source": ["who_gho", "who_gho"], "cases": [2500, 3000], "deaths": [1, 2],
            "series_used": ["month/-/Total"] * 2, "periods_in_month": [1, 1],
            "is_complete": [True, True],
        }),
        "unit_risk": pd.DataFrame({
            "unit_key": [1], "country_key": [1], "iso3": ["THA"], "unit_id": ["TH31"],
            "unit_name": ["Buri Ram"], "local_name": ["บุรีรัมย์"], "population": [1_600_000],
            "source": ["trends_th"], "data_as_of": [date(2025, 12, 1)], "cases": [120],
            "cases_per_100k": [7.5], "baseline_mean": [100.0], "n_baseline_years": [5],
            "z_score": [0.4], "case_level": ["thấp"], "news_30d": [3], "news_prev_30d": [1],
            "signal": ["ca bệnh"],
        }),
        "bridge_map_polygon": pd.DataFrame({"iso_3166_2": ["TH-31"], "unit_key": [1]}),
        "news_feed": pd.DataFrame({
            "article_id": ["a1"], "title": ["Dengue rises in Vietnam"], "link": ["https://n/a1"],
            "publisher": ["VnExpress"], "published_at": [datetime(2026, 9, 28, 7)],
            "first_seen_at": [datetime(2026, 9, 29, 6)], "is_recent": [True],
            "countries": [["VNM"]],
        }),
    }


@pytest.fixture
def small_gold(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Ghi Gold nho + bao cao chat luong + ranh gioi, tro app vao do."""
    gold = tmp_path / "gold"
    for name, frame in _gold_tables().items():
        write_deltalake(str(gold / name), frame)

    quality = tmp_path / "quality"
    quality.mkdir()
    (quality / "quality_20260929T000000Z.json").write_text(json.dumps({
        "run_id": "20260929T000000Z", "counts": {"PASS": 1, "WARN": 1},
        "checks": [
            {"layer": "bronze", "table": "news_rss", "name": "guid_not_kept", "severity": "WARN",
             "failed": 1, "total": 1, "detail": "...", "status": "WARN"},
            {"layer": "gold", "table": "fact_monthly_cases", "name": "fk_country_key",
             "severity": "ERROR", "failed": 0, "total": 2, "detail": "", "status": "PASS"},
        ],
    }), encoding="utf-8")

    boundary = tmp_path / "sea_admin1.geojson"
    boundary.write_text(json.dumps({"type": "FeatureCollection", "features": [{
        "type": "Feature", "properties": {"iso_3166_2": "TH-31", "name": "Buri Ram"},
        "geometry": {"type": "Polygon",
                     "coordinates": [[[102.5, 14.5], [103.5, 14.5], [103.5, 15.5], [102.5, 14.5]]]},
    }]}), encoding="utf-8")

    monkeypatch.setenv("OUTBREAK_GOLD_ROOT", str(gold))
    monkeypatch.setenv("OUTBREAK_QUALITY_ROOT", str(quality))
    monkeypatch.setenv("OUTBREAK_BOUNDARY_PATH", str(boundary))
    return gold


def test_app_chay_khong_loi_va_du_4_tab(small_gold: Path) -> None:
    from streamlit.testing.v1 import AppTest

    app = AppTest.from_file(APP, default_timeout=60).run()
    assert not app.exception
    assert [tab.label for tab in app.tabs] == [
        "Bản đồ nguy cơ", "Tin tức", "Chuỗi thời gian", "Chất lượng dữ liệu"
    ]
    assert app.error == []


def test_bat_tang_cap_tinh_khong_loi(small_gold: Path) -> None:
    from streamlit.testing.v1 import AppTest

    app = AppTest.from_file(APP, default_timeout=60).run()
    app.toggle[0].set_value(True).run()
    assert not app.exception


def test_chua_co_gold_thi_bao_loi_ro_rang(tmp_path: Path, monkeypatch) -> None:
    from streamlit.testing.v1 import AppTest

    monkeypatch.setenv("OUTBREAK_GOLD_ROOT", str(tmp_path / "trong"))
    app = AppTest.from_file(APP, default_timeout=60).run()
    assert not app.exception
    assert "run_transform.py" in app.error[0].value
