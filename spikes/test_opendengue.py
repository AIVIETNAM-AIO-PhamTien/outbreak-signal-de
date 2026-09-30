"""
Spike test: OpenDengue national dataset.

Muc dich: xac nhan (1) tai duoc file, (2) schema co du field thoi gian /
dia diem / so ca, (3) loc duoc data Dong Nam A. KHONG dung PySpark o day -
day chi la buoc kiem tra nguon truoc khi build Bronze ingestion that.

Nguon: https://opendengue.org/data.html
Ban quyen/dieu khoan: xem trang OpenDengue truoc khi dung cho MVP chinh thuc.
"""

import csv
import io
import sys
import zipfile
from pathlib import Path

import requests

DATA_URL = (
    "https://github.com/OpenDengue/master-repo/raw/main/data/releases/"
    "V1.3/National_extract_V1_3.zip"
)

OUT_DIR = Path(__file__).resolve().parent.parent / "output" / "bronze_test"
RAW_ZIP_PATH = OUT_DIR / "opendengue_national_raw.zip"
SAMPLE_CSV_PATH = OUT_DIR / "opendengue_sea_sample.csv"

# Ten nuoc Dong Nam A theo adm_0_name trong dataset OpenDengue.
#
# BUG DA SUA: "VIET NAM" phai CO dau cach - nguon ghi dung format UN naming.
# Ban cu viet lien "VIETNAM" nen 317 dong cua Viet Nam bi loc mat am tham
# (0 dong con lai khi giao voi SEA_COUNTRIES). Phat hien qua EDA, xem
# notebooks/eda_colab_bronze.ipynb muc 2.3.
SEA_COUNTRIES = {
    "VIET NAM",
    "THAILAND",
    "INDONESIA",
    "PHILIPPINES",
    "MALAYSIA",
    "MYANMAR",
    "CAMBODIA",
    "LAO PEOPLE'S DEMOCRATIC REPUBLIC",
    "LAOS",
    "SINGAPORE",
    "BRUNEI DARUSSALAM",
    "TIMOR-LESTE",
}


def download(url: str, dest: Path) -> None:
    print(f"[1/4] Downloading {url}")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(resp.content)
    print(f"      -> saved {len(resp.content):,} bytes to {dest}")


def load_csv_from_zip(zip_path: Path) -> list[dict]:
    print("[2/4] Extracting CSV from zip")
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"No CSV found inside zip. Contents: {zf.namelist()}")
        csv_name = csv_names[0]
        print(f"      -> found member: {csv_name}")
        with zf.open(csv_name) as f:
            text = io.TextIOWrapper(f, encoding="utf-8")
            reader = csv.DictReader(text)
            rows = list(reader)
    print(f"      -> total rows in file: {len(rows):,}")
    return rows


def filter_sea(rows: list[dict]) -> list[dict]:
    print("[3/4] Filtering rows for Southeast Asia")
    filtered = [
        r for r in rows if r.get("adm_0_name", "").strip().upper() in SEA_COUNTRIES
    ]
    countries_found = sorted({r.get("adm_0_name", "").strip() for r in filtered})
    print(f"      -> {len(filtered):,} rows match. Countries found: {countries_found}")
    return filtered


def report(rows: list[dict], filtered: list[dict]) -> None:
    print("[4/4] Schema / sample check")
    if not rows:
        print("      -> EMPTY dataset, cannot inspect schema.")
        return

    columns = list(rows[0].keys())
    print(f"      -> columns: {columns}")

    required_like = {
        "time": [c for c in columns if "date" in c.lower()],
        "location": [c for c in columns if "adm_" in c.lower() or "name" in c.lower()],
        "case_count": [c for c in columns if "dengue" in c.lower() or "case" in c.lower()],
    }
    print(f"      -> candidate columns per required field: {required_like}")

    print("\n      Sample rows (Southeast Asia, first 5):")
    for r in filtered[:5]:
        print(f"      {r}")

    if filtered:
        dates = sorted(r.get("calendar_start_date", "") for r in filtered if r.get("calendar_start_date"))
        if dates:
            print(f"\n      -> date range in SEA subset: {dates[0]} .. {dates[-1]}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(SAMPLE_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(filtered[:200])
    print(f"\n      -> saved sample ({min(200, len(filtered))} rows) to {SAMPLE_CSV_PATH}")


def main() -> int:
    try:
        if not RAW_ZIP_PATH.exists():
            download(DATA_URL, RAW_ZIP_PATH)
        else:
            print(f"[1/4] Using cached zip at {RAW_ZIP_PATH}")

        rows = load_csv_from_zip(RAW_ZIP_PATH)
        filtered = filter_sea(rows)
        report(rows, filtered)

        print("\nRESULT: OpenDengue source is USABLE for MVP." if filtered else "\nRESULT: NO Southeast Asia rows found - investigate column names / country spelling.")
        return 0
    except requests.RequestException as e:
        print(f"\nRESULT: FAILED - network/API error: {e}")
        return 1
    except Exception as e:
        print(f"\nRESULT: FAILED - unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
