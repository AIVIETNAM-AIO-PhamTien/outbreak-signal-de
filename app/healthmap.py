"""OutbreakSignal HealthMap - ban do nguy co sot xuat huyet 11 nuoc Dong Nam A.

Chay:  streamlit run app/healthmap.py

Ket hop hai loai tin hieu giong HealthMap:
  - so ca chinh thuc (WHO GHO / OpenDengue) so voi baseline mua vu -> mau nuoc
  - tin tuc (Google News RSS) 30 ngay gan nhat -> cham tron tren ban do
Chi doc Gold (khong can Spark). Du lieu Gold dung bang scripts/run_transform.py.
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))

import gold_reader as gr  # noqa: E402

LEVEL_COLORS = {
    "cao": "#d7301f",
    "trung bình": "#fc8d59",
    "thấp": "#91cf60",
    "không đủ dữ liệu": "#bdbdbd",
    "số liệu cũ": "#d9d9d9",
    "không có dữ liệu": "#eeeeee",
}
LEVEL_ORDER = list(LEVEL_COLORS)
SOURCE_LABEL = {"who_gho": "WHO GHO", "opendengue": "OpenDengue"}


@st.cache_data(show_spinner=False)
def load_all(gold_dir: str) -> dict[str, pd.DataFrame]:
    """Doc moi bang Gold can cho app (co cache giua cac lan tai trang).

    Args:
        gold_dir: Thu muc Gold - nam trong khoa cache, doi thu muc la doc lai.

    Returns:
        Dict ten bang -> DataFrame.
    """
    names = (
        "country_risk", "dim_country", "fact_monthly_cases", "news_feed",
        "unit_risk", "bridge_map_polygon",
    )
    return {name: gr.load_table(name) for name in names}


@st.cache_data(show_spinner=False)
def load_geo(boundary_file: str) -> dict | None:
    """Doc ranh gioi tinh (co cache).

    Args:
        boundary_file: Duong dan file - nam trong khoa cache.

    Returns:
        GeoJSON hoac None.
    """
    return gr.load_boundaries()


def risk_map(risk: pd.DataFrame) -> go.Figure:
    """Ban do cap quoc gia: mau = muc nguy co, cham tron = so tin 30 ngay.

    Args:
        risk: Bang country_risk.

    Returns:
        Figure plotly.
    """
    frame = risk.assign(
        source_label=risk["source"].map(SOURCE_LABEL).fillna("-"),
        as_of=pd.to_datetime(risk["data_as_of"]).dt.strftime("%m/%Y").fillna("-"),
    )
    fig = px.choropleth(
        frame,
        locations="iso3",
        color="risk_level",
        color_discrete_map=LEVEL_COLORS,
        category_orders={"risk_level": LEVEL_ORDER},
        hover_name="country_name",
        hover_data={
            "iso3": False, "risk_level": True, "cases": ":,", "baseline_mean": ":,",
            "z_score": True, "source_label": True, "as_of": True, "news_30d": True,
        },
        labels={
            "risk_level": "Mức nguy cơ", "cases": "Số ca tháng mới nhất",
            "baseline_mean": "Baseline cùng tháng", "z_score": "z-score",
            "source_label": "Nguồn", "as_of": "Dữ liệu tới", "news_30d": "Tin 30 ngày",
        },
    )
    news = frame[frame["news_30d"] > 0]
    if not news.empty:
        fig.add_trace(
            go.Scattergeo(
                locations=news["iso3"],
                locationmode="ISO-3",
                marker={
                    "size": 8 + 4 * news["news_30d"].clip(upper=10),
                    "color": "rgba(33, 102, 172, 0.55)",
                    "line": {"width": 1, "color": "white"},
                },
                text=news["country_name"] + ": " + news["news_30d"].astype(str) + " tin",
                hoverinfo="text",
                name="Tin tức 30 ngày",
            )
        )
    fig.update_geos(fitbounds="locations", visible=False, showcountries=True,
                    showland=True, landcolor="#f7f7f7")
    fig.update_layout(margin={"l": 0, "r": 0, "t": 0, "b": 0}, height=520,
                      legend={"orientation": "h", "y": -0.05})
    return fig


PROVINCE_METRICS = {
    "Tin tức 30 ngày (hiện tại)": ("news_30d", "Số bài 30 ngày"),
    "Số ca / 100.000 dân (tháng có số liệu mới nhất)": ("cases_per_100k", "Ca / 100.000 dân"),
    "So với cùng tháng các năm trước": ("case_level", "Mức"),
}


def province_frame(unit_risk: pd.DataFrame, bridge: pd.DataFrame) -> pd.DataFrame:
    """Gan chi so cua don vi hien hanh cho tung polygon ban do.

    Viet Nam: 63 polygon tinh cu cung to theo tinh moi chua no (bridge_map_polygon).

    Args:
        unit_risk: Bang unit_risk.
        bridge: Bang bridge_map_polygon (iso_3166_2 -> unit_key).

    Returns:
        DataFrame moi polygon mot dong.
    """
    frame = bridge.merge(unit_risk, on="unit_key", how="inner")
    frame["as_of"] = pd.to_datetime(frame["data_as_of"]).dt.strftime("%m/%Y").fillna("-")
    return frame


def province_map(frame: pd.DataFrame, geo: dict, metric: str) -> go.Figure:
    """Ban do cap tinh theo mot chi so.

    Args:
        frame: Ket qua province_frame().
        geo: GeoJSON ranh gioi (Natural Earth, loc 11 nuoc).
        metric: Mot khoa cua PROVINCE_METRICS.

    Returns:
        Figure plotly.
    """
    column, label = PROVINCE_METRICS[metric]
    common = dict(
        geojson=geo, locations="iso_3166_2", featureidkey="properties.iso_3166_2",
        hover_name="unit_name",
        hover_data={"iso_3166_2": False, "local_name": True, "news_30d": True,
                    "cases": ":,", "cases_per_100k": True, "as_of": True, "signal": True},
        labels={column: label, "local_name": "Tên bản xứ", "news_30d": "Tin 30 ngày",
                "cases": "Số ca", "cases_per_100k": "Ca/100k", "as_of": "Số ca tới",
                "signal": "Tín hiệu"},
    )
    if column == "case_level":
        fig = px.choropleth(frame, color=column, color_discrete_map=LEVEL_COLORS,
                            category_orders={column: LEVEL_ORDER}, **common)
    else:
        fig = px.choropleth(frame, color=column, color_continuous_scale="OrRd", **common)
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(margin={"l": 0, "r": 0, "t": 0, "b": 0}, height=560)
    return fig


def monthly_chart(monthly: pd.DataFrame, risk_row: pd.Series | None) -> go.Figure:
    """Chuoi so ca theo thang cua mot nuoc, hai nguon chong nhau.

    Args:
        monthly: fact_monthly_cases cua mot nuoc (da co cot month).
        risk_row: Dong country_risk cua nuoc do (de ve baseline thang moi nhat).

    Returns:
        Figure plotly.
    """
    frame = monthly.assign(source_label=monthly["source"].map(SOURCE_LABEL))
    fig = px.line(frame.sort_values("month"), x="month", y="cases", color="source_label",
                  labels={"month": "Tháng", "cases": "Số ca", "source_label": "Nguồn"})
    if risk_row is not None and pd.notna(risk_row["baseline_mean"]):
        fig.add_trace(go.Scatter(
            x=[pd.to_datetime(risk_row["data_as_of"])],
            y=[risk_row["baseline_mean"]],
            error_y={"type": "data", "array": [risk_row["baseline_std"] or 0]},
            mode="markers", marker={"symbol": "diamond", "size": 10, "color": "black"},
            name="Baseline tháng mới nhất (±1 SD)",
        ))
    fig.update_layout(height=420, margin={"l": 0, "r": 0, "t": 10, "b": 0})
    return fig


def main() -> None:
    """Dung giao dien app."""
    st.set_page_config(page_title="OutbreakSignal HealthMap", page_icon="🦟", layout="wide")
    st.title("OutbreakSignal HealthMap — Sốt xuất huyết Đông Nam Á")

    try:
        data = load_all(str(gr.gold_root()))
    except FileNotFoundError as error:
        st.error(str(error))
        st.stop()

    risk = data["country_risk"]
    as_of = pd.to_datetime(risk["data_as_of"]).max()
    st.caption(
        "Màu nước = số ca tháng mới nhất so với cùng tháng các năm trước (z-score, ≥2 cao, "
        "1–2 trung bình). Chấm tròn = số tin tức 30 ngày gần nhất. "
        f"Số ca mới nhất: {as_of:%m/%Y}. Đây là thống kê mô tả, không phải dự báo."
        if pd.notna(as_of) else "Chưa có số ca."
    )

    countries = data["dim_country"].query("iso3 != 'UNK'").sort_values("country_name")
    names = dict(zip(countries["iso3"], countries["country_name"]))

    tab_map, tab_news, tab_series, tab_quality = st.tabs(
        ["Bản đồ nguy cơ", "Tin tức", "Chuỗi thời gian", "Chất lượng dữ liệu"]
    )

    with tab_map:
        st.plotly_chart(risk_map(risk), width="stretch")
        show = risk[["country_name", "risk_level", "source", "data_as_of",
                     "skipped_recent_months", "cases", "baseline_mean", "z_score",
                     "n_baseline_years", "news_30d"]]
        lagged = risk[risk["skipped_recent_months"] > 0]
        if not lagged.empty:
            st.caption("Bỏ qua tháng mới nhất do nghi chưa báo cáo đủ (giảm >50% so với tháng "
                       "trước): " + ", ".join(lagged["country_name"]))
        st.dataframe(show.sort_values("z_score", ascending=False, na_position="last"),
                     hide_index=True, width="stretch")

        st.subheader("Tầng cấp tỉnh")
        geo = load_geo(str(gr.boundary_path()))
        unit_risk = data["unit_risk"]
        if geo is None or unit_risk.empty:
            st.info("Chưa có ranh giới tỉnh hoặc dữ liệu tỉnh.")
        elif st.toggle("Hiện bản đồ cấp tỉnh", value=False):
            metric = st.radio("Chỉ số", list(PROVINCE_METRICS), horizontal=True)
            latest = (unit_risk.dropna(subset=["data_as_of"]).groupby("iso3")["data_as_of"].max()
                      .map(lambda d: f"{pd.to_datetime(d):%m/%Y}"))
            st.caption("Số ca cấp tỉnh mới nhất theo nước: "
                       + ", ".join(f"{k} {v}" for k, v in latest.items())
                       + ". Nước chỉ có số ca cũ (VN/KHM/LAO tới 2010) dùng tín hiệu tin tức.")
            frame = province_frame(unit_risk, data["bridge_map_polygon"])
            st.plotly_chart(province_map(frame, geo, metric), width="stretch")
            top = unit_risk.sort_values("news_30d", ascending=False).head(15)
            st.dataframe(top[["iso3", "unit_name", "local_name", "news_30d", "signal",
                              "data_as_of", "cases", "cases_per_100k", "case_level"]],
                         hide_index=True, width="stretch")

    with tab_news:
        feed = data["news_feed"]
        picked = st.multiselect("Lọc theo nước", options=list(names), format_func=names.get)
        only_recent = st.checkbox("Chỉ tin mới (đăng ≤ 7 ngày trước khi thu thập)", value=False)
        view = feed
        if picked:
            view = view[view["countries"].apply(lambda cs: any(c in picked for c in cs))]
        if only_recent:
            view = view[view["is_recent"]]
        latest = pd.to_datetime(feed["published_at"]).max()
        st.caption(f"{len(view):,} / {len(feed):,} bài · bài mới nhất đăng ngày "
                   + (f"{latest:%d/%m/%Y}" if pd.notna(latest) else "?"))
        if feed["is_recent"].sum() == 0:
            st.warning("Không có bài nào đăng trong 7 ngày trước lần thu thập — tín hiệu tin tức "
                       "hiện chưa 'sớm' (truy vấn RSS chưa giới hạn thời gian).")
        for _, row in view.head(50).iterrows():
            tags = ", ".join(names.get(c, c) for c in row["countries"]) or "chưa gắn nước"
            published = pd.to_datetime(row["published_at"])
            when = f"{published:%d/%m/%Y}" if pd.notna(published) else "?"
            st.markdown(f"- [{row['title']}]({row['link']}) — *{row['publisher']}*, "
                        f"{when} · `{tags}`")

    with tab_series:
        iso3 = st.selectbox("Nước", options=list(names), format_func=names.get,
                            index=list(names).index("VNM") if "VNM" in names else 0)
        key_by_iso3 = dict(zip(data["dim_country"]["iso3"], data["dim_country"]["country_key"]))
        monthly = data["fact_monthly_cases"]
        monthly = monthly[(monthly["country_key"] == key_by_iso3[iso3]) & monthly["is_complete"]]
        monthly = monthly.assign(month=gr.key_to_date(monthly["month_date_key"]))
        row = risk[risk["iso3"] == iso3]
        if monthly.empty:
            st.info("Nước này chưa có số ca theo tháng.")
        else:
            st.plotly_chart(monthly_chart(monthly, row.iloc[0] if not row.empty else None),
                            width="stretch")

    with tab_quality:
        report = gr.latest_quality_report()
        if report is None:
            st.info("Chưa có báo cáo chất lượng.")
        else:
            st.caption(f"Lần chạy {report['run_id']} — {report['counts']}")
            checks = pd.DataFrame(report["checks"])
            statuses = st.multiselect("Trạng thái", ["ERROR", "WARN", "INFO", "PASS"],
                                      default=["ERROR", "WARN", "INFO"])
            st.dataframe(checks[checks["status"].isin(statuses)][
                ["status", "layer", "table", "name", "failed", "total", "detail"]],
                hide_index=True, width="stretch")


main()
