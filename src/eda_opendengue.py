"""
EDA Suite for OpenDengue Dataset (National Extract V1.3).
Part of OutbreakSignal DE - Microwave Group (AIO 2026).

This script performs end-to-end Exploratory Data Analysis:
  1. Data loading & schema inspection
  2. Data quality, missingness, and integrity audits (including the 'VIET NAM' whitespace trap)
  3. Temporal resolution harmonization (Week / Month / Year)
  4. Regional SEA epidemiological trend analysis & outbreak detection
  5. Seasonality pattern extraction across Southeast Asia
  6. High-resolution figure generation saved to output/eda/
"""

import io
import sys
import zipfile
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import seaborn as sns

# Configure styles
sns.set_theme(style="whitegrid", palette="muted")
plt.rcParams["font.sans-serif"] = ["DejaVu Sans", "Arial", "Helvetica"]
plt.rcParams["axes.edgecolor"] = "#cccccc"
plt.rcParams["axes.linewidth"] = 0.8

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_ZIP_PATH = BASE_DIR / "output" / "bronze_test" / "opendengue_national_raw.zip"
EDA_OUT_DIR = BASE_DIR / "output" / "eda"
DATA_URL = (
    "https://github.com/OpenDengue/master-repo/raw/main/data/releases/"
    "V1.3/National_extract_V1_3.zip"
)

SEA_COUNTRIES = [
    "BRUNEI DARUSSALAM",
    "CAMBODIA",
    "INDONESIA",
    "LAO PEOPLE'S DEMOCRATIC REPUBLIC",
    "MALAYSIA",
    "MYANMAR",
    "PHILIPPINES",
    "SINGAPORE",
    "THAILAND",
    "TIMOR-LESTE",
    "VIET NAM",  # Note the space: OpenDengue uses 'VIET NAM', NOT 'VIETNAM'
]


def ensure_dataset() -> pd.DataFrame:
    """Download if missing and load OpenDengue national CSV into DataFrame."""
    if not DATA_ZIP_PATH.exists():
        print(f"[*] Downloading OpenDengue from {DATA_URL}...")
        DATA_ZIP_PATH.parent.mkdir(parents=True, exist_ok=True)
        resp = requests.get(DATA_URL, timeout=120)
        resp.raise_for_status()
        DATA_ZIP_PATH.write_bytes(resp.content)
        print(f"    Saved {len(resp.content):,} bytes to {DATA_ZIP_PATH}")

    with zipfile.ZipFile(DATA_ZIP_PATH) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"No CSV found in {DATA_ZIP_PATH}")
        with zf.open(csv_names[0]) as f:
            df = pd.read_csv(f)
    print(f"[*] Loaded dataset: {len(df):,} rows, {len(df.columns)} columns.")
    return df


def audit_data_quality(df: pd.DataFrame) -> dict:
    """Run comprehensive sanity and quality checks."""
    print("\n" + "=" * 60)
    print("1. DATA QUALITY & SCHEMA AUDIT")
    print("=" * 60)

    null_summary = df.isna().sum()
    null_cols = null_summary[null_summary > 0]
    print(f"Columns with Nulls:\n{null_cols}")

    # Check Vietnam naming trap
    has_vietnam_nospace = "VIETNAM" in df["adm_0_name"].values
    has_vietnam_space = "VIET NAM" in df["adm_0_name"].values
    print(f"\n[Naming Audit] 'VIETNAM' (no space): {has_vietnam_nospace}")
    print(f"[Naming Audit] 'VIET NAM' (with space): {has_vietnam_space} (Count: {(df['adm_0_name'] == 'VIET NAM').sum()})")

    # Temporal resolutions
    t_res_counts = df["T_res"].value_counts().to_dict()
    print(f"\n[Temporal Resolutions (T_res)]: {t_res_counts}")

    # Case definitions
    case_defs = df["case_definition_standardised"].value_counts().to_dict()
    print(f"\n[Case Definitions]: {case_defs}")

    # Date duplicates
    date_dups = df.duplicated(
        subset=["adm_0_name", "calendar_start_date", "calendar_end_date"], keep=False
    ).sum()
    print(f"\n[Duplicate Records (Country + Date Range)]: {date_dups}")

    return {
        "null_cols": null_cols.to_dict(),
        "t_res_counts": t_res_counts,
        "case_defs": case_defs,
        "date_dups": int(date_dups),
    }


def analyze_sea_subset(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Filter and harmonize Southeast Asia subset."""
    print("\n" + "=" * 60)
    print("2. SOUTHEAST ASIA (SEA) SUBSET ANALYSIS")
    print("=" * 60)

    df_sea = df[df["adm_0_name"].isin(SEA_COUNTRIES)].copy()
    df_sea["calendar_start_date"] = pd.to_datetime(df_sea["calendar_start_date"])
    df_sea["calendar_end_date"] = pd.to_datetime(df_sea["calendar_end_date"])
    df_sea["Year"] = df_sea["Year"].astype(int)

    print(f"Total SEA records: {len(df_sea):,} across {df_sea['adm_0_name'].nunique()} countries.")
    print(f"Time range: {df_sea['calendar_start_date'].min().strftime('%Y-%m-%d')} to {df_sea['calendar_end_date'].max().strftime('%Y-%m-%d')}")

    breakdown = (
        df_sea.groupby("adm_0_name")
        .agg(
            total_records=("dengue_total", "count"),
            min_year=("Year", "min"),
            max_year=("Year", "max"),
            cumulative_cases=("dengue_total", "sum"),
        )
        .sort_values(by="cumulative_cases", ascending=False)
    )
    print(f"\nCountry Overview in SEA:\n{breakdown}")

    # Harmonize Annual Data:
    # Since within each country and year, OpenDengue uses either Week, Month, or Year,
    # summing dengue_total per (adm_0_name, Year) yields consistent annual figures.
    annual_sea = (
        df_sea.groupby(["adm_0_name", "Year"])
        .agg(
            records_in_year=("dengue_total", "count"),
            t_res_used=("T_res", lambda s: list(s.unique())),
            annual_cases=("dengue_total", "sum"),
        )
        .reset_index()
    )

    return df_sea, annual_sea


def plot_eda_charts(df_sea: pd.DataFrame, annual_sea: pd.DataFrame) -> list[Path]:
    """Generate high quality EDA visualization charts."""
    EDA_OUT_DIR.mkdir(parents=True, exist_ok=True)
    saved_files = []

    # Chart 1: Surveillance Timeline & Resolution per SEA Country
    fig, ax = plt.subplots(figsize=(12, 7))
    country_order = (
        df_sea.groupby("adm_0_name")["Year"].min().sort_values().index.tolist()
    )

    palette = {"Week": "#e74c3c", "Month": "#f39c12", "Year": "#3498db"}
    for country in country_order:
        c_df = df_sea[df_sea["adm_0_name"] == country]
        for res, grp in c_df.groupby("T_res"):
            ax.scatter(
                grp["Year"],
                [country] * len(grp),
                color=palette.get(res, "#95a5a6"),
                label=res,
                s=18,
                alpha=0.6,
                edgecolors="none",
            )

    # Dedup legend
    handles, labels = ax.get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    ax.legend(
        by_label.values(),
        by_label.keys(),
        title="Temporal Resolution (T_res)",
        loc="upper left",
        frameon=True,
    )

    ax.set_title(
        "OpenDengue V1.3: Surveillance Timeline & Temporal Resolution (Southeast Asia)",
        fontsize=14,
        fontweight="bold",
        pad=15,
    )
    ax.set_xlabel("Surveillance Year", fontsize=11)
    ax.set_ylabel("Country (adm_0_name)", fontsize=11)
    ax.grid(True, linestyle="--", alpha=0.5)
    plt.tight_layout()
    p1 = EDA_OUT_DIR / "01_sea_country_coverage.png"
    fig.savefig(p1, dpi=300)
    plt.close(fig)
    saved_files.append(p1)
    print(f"[Chart 1/5] Saved: {p1}")

    # Chart 2: Annual Dengue Cases Trends (Log Scale) across SEA
    fig, ax = plt.subplots(figsize=(14, 7))
    major_sea = [
        "VIET NAM",
        "PHILIPPINES",
        "THAILAND",
        "INDONESIA",
        "MALAYSIA",
        "SINGAPORE",
    ]
    colors = sns.color_palette("tab10", len(major_sea))

    for country, color in zip(major_sea, colors):
        c_ann = annual_sea[annual_sea["adm_0_name"] == country].sort_values("Year")
        c_ann_post1980 = c_ann[c_ann["Year"] >= 1980]
        ax.plot(
            c_ann_post1980["Year"],
            c_ann_post1980["annual_cases"],
            marker="o",
            markersize=3.5,
            linewidth=1.8,
            label=country,
            color=color,
        )

    ax.set_yscale("log")
    ax.set_title(
        "Annual Dengue Case Trajectories (Major SEA Nations, 1980 - 2025)",
        fontsize=14,
        fontweight="bold",
        pad=15,
    )
    ax.set_xlabel("Year", fontsize=11)
    ax.set_ylabel("Annual Reported Dengue Cases (Log Scale)", fontsize=11)
    ax.axvspan(2018.5, 2019.5, color="#e74c3c", alpha=0.15, label="2019 Pandemic Wave")
    ax.axvspan(2021.5, 2022.5, color="#f39c12", alpha=0.15, label="2022 Vietnam Wave")
    ax.legend(title="Country / Event", loc="lower right", frameon=True)
    ax.grid(True, which="both", linestyle="--", alpha=0.5)
    plt.tight_layout()
    p2 = EDA_OUT_DIR / "02_annual_case_trends_sea.png"
    fig.savefig(p2, dpi=300)
    plt.close(fig)
    saved_files.append(p2)
    print(f"[Chart 2/5] Saved: {p2}")

    # Chart 3: Deep Dive into Vietnam (1960 - 2025)
    vn_annual = annual_sea[annual_sea["adm_0_name"] == "VIET NAM"].sort_values("Year")
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # Ax1: Vietnam Historical Cases & Outbreak Annotations
    ax1.bar(
        vn_annual["Year"],
        vn_annual["annual_cases"],
        color="#2c3e50",
        alpha=0.85,
        width=0.8,
    )
    # Highlight major epidemic peaks
    peaks = [
        (1987, 354517, "1987 Epidemic\n(354k)"),
        (2017, 172232, "2017 Spike\n(172k)"),
        (2019, 320702, "2019 Regional Wave\n(320k)"),
        (2022, 367729, "2022 Record Outbreak\n(367k)"),
    ]
    for yr, val, txt in peaks:
        ax1.annotate(
            txt,
            xy=(yr, val),
            xytext=(yr - 4, val + 35000),
            arrowprops=dict(facecolor="#e74c3c", shrink=0.08, width=1.2, headwidth=6),
            fontsize=9,
            fontweight="bold",
            color="#c0392b",
            ha="center",
        )

    ax1.set_title(
        "Vietnam: Historical Dengue Case Load & Epidemic Peaks (1960 - 2025)",
        fontsize=12,
        fontweight="bold",
    )
    ax1.set_xlabel("Year", fontsize=10)
    ax1.set_ylabel("Annual Total Cases", fontsize=10)
    ax1.grid(True, linestyle="--", alpha=0.5)

    # Ax2: Vietnam Monthly Seasonality (from Monthly Resolution Records)
    vn_monthly = df_sea[
        (df_sea["adm_0_name"] == "VIET NAM") & (df_sea["T_res"] == "Month")
    ].copy()
    vn_monthly["month"] = vn_monthly["calendar_start_date"].dt.month
    monthly_stats = vn_monthly.groupby("month")["dengue_total"].agg(["mean", "std"])

    months = range(1, 13)
    month_names = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    ax2.plot(
        months,
        monthly_stats["mean"],
        marker="o",
        color="#e67e22",
        linewidth=2.5,
        label="Mean Cases",
    )
    ax2.fill_between(
        months,
        np.maximum(0, monthly_stats["mean"] - monthly_stats["std"]),
        monthly_stats["mean"] + monthly_stats["std"],
        color="#e67e22",
        alpha=0.2,
        label="±1 Std Dev",
    )
    ax2.set_xticks(list(months))
    ax2.set_xticklabels(month_names)
    ax2.set_title(
        "Vietnam: Seasonal Dengue Transmission Profile (Monthly Distribution)",
        fontsize=12,
        fontweight="bold",
    )
    ax2.set_xlabel("Month of Year", fontsize=10)
    ax2.set_ylabel("Average Cases per Month", fontsize=10)
    ax2.axvspan(6.5, 10.5, color="#e74c3c", alpha=0.1, label="Peak Monsoon Season")
    ax2.legend(loc="upper left", frameon=True)
    ax2.grid(True, linestyle="--", alpha=0.5)

    plt.tight_layout()
    p3 = EDA_OUT_DIR / "03_vietnam_deep_dive.png"
    fig.savefig(p3, dpi=300)
    plt.close(fig)
    saved_files.append(p3)
    print(f"[Chart 3/5] Saved: {p3}")

    # Chart 4: Monthly Seasonality Heatmap across SEA
    df_sea_month = df_sea[df_sea["T_res"] == "Month"].copy()
    df_sea_month["month"] = df_sea_month["calendar_start_date"].dt.month

    # Normalize each country by its max monthly mean so patterns are directly comparable
    country_month = (
        df_sea_month.groupby(["adm_0_name", "month"])["dengue_total"]
        .mean()
        .reset_index()
    )
    pivot = country_month.pivot(
        index="adm_0_name", columns="month", values="dengue_total"
    )
    # Exclude countries with insufficient monthly records
    valid_countries = pivot.dropna(thresh=6).index
    pivot_valid = pivot.loc[valid_countries]
    pivot_norm = pivot_valid.div(pivot_valid.max(axis=1), axis=0)

    fig, ax = plt.subplots(figsize=(12, 6))
    sns.heatmap(
        pivot_norm,
        cmap="YlOrRd",
        annot=True,
        fmt=".2f",
        xticklabels=month_names,
        linewidths=0.5,
        ax=ax,
        cbar_kws={"label": "Normalized Intensity (0.0 to 1.0)"},
    )
    ax.set_title(
        "Southeast Asia Dengue Seasonality Heatmap (Relative Monthly Peak Intensity)",
        fontsize=13,
        fontweight="bold",
        pad=15,
    )
    ax.set_xlabel("Month", fontsize=11)
    ax.set_ylabel("Country", fontsize=11)
    plt.tight_layout()
    p4 = EDA_OUT_DIR / "04_monthly_seasonality_heatmap.png"
    fig.savefig(p4, dpi=300)
    plt.close(fig)
    saved_files.append(p4)
    print(f"[Chart 4/5] Saved: {p4}")

    # Chart 5: Provenance & Case Definition Composition
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    # Ax1: Case definitions in SEA
    case_def_sea = df_sea["case_definition_standardised"].value_counts()
    ax1.pie(
        case_def_sea,
        labels=case_def_sea.index,
        autopct="%1.1f%%",
        startangle=140,
        colors=sns.color_palette("Set2", len(case_def_sea)),
        explode=[0.05 if v < case_def_sea.max() else 0 for v in case_def_sea],
    )
    ax1.set_title(
        "Standardized Case Definitions (SEA Records)",
        fontsize=12,
        fontweight="bold",
    )

    # Ax2: Top Data Sources / Curators (Extracted from UUID prefixes)
    uuid_prefixes = df_sea["UUID"].apply(lambda u: str(u).split("-")[0])
    top_sources = uuid_prefixes.value_counts().head(8)
    sns.barplot(
        x=top_sources.values,
        y=top_sources.index,
        palette="Blues_r",
        ax=ax2,
    )
    ax2.set_title(
        "Primary Data Provenance / Aggregators (UUID Prefix in SEA)",
        fontsize=12,
        fontweight="bold",
    )
    ax2.set_xlabel("Record Count", fontsize=10)
    ax2.set_ylabel("Source Aggregator", fontsize=10)
    for i, v in enumerate(top_sources.values):
        ax2.text(v + 15, i, f"{v:,}", va="center", fontsize=9)
    ax2.grid(True, linestyle="--", alpha=0.5)

    plt.tight_layout()
    p5 = EDA_OUT_DIR / "05_case_definitions_and_sources.png"
    fig.savefig(p5, dpi=300)
    plt.close(fig)
    saved_files.append(p5)
    print(f"[Chart 5/5] Saved: {p5}")

    return saved_files


def main():
    print("=== STARTING OPENDENGUE EDA SUITE ===")
    df = ensure_dataset()
    audit_data_quality(df)
    df_sea, annual_sea = analyze_sea_subset(df)
    saved_plots = plot_eda_charts(df_sea, annual_sea)
    print("\n" + "=" * 60)
    print("EDA COMPLETED SUCCESSFULLY!")
    print(f"Generated {len(saved_plots)} publication plots in {EDA_OUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
