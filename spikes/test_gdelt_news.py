"""
Spike test: GDELT DOC 2.0 API as the "news" source for dengue outbreak signals.

Muc dich: xac nhan (1) API goi duoc khong can key, (2) response co du field
thoi gian / nguon / quoc gia de suy ra dia diem, (3) loc duoc bai viet lien
quan Dong Nam A. KHONG dung PySpark o day - chi la buoc kiem tra nguon.

API docs (khong chinh thuc, suy ra tu cong dong + endpoint thuc te):
https://api.gdeltproject.org/api/v2/doc/doc
"""

import json
import sys
import time
from pathlib import Path

import requests

API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
HEADERS = {"User-Agent": "OutbreakSignalDE-spike/0.1 (student project, ingestion test)"}

OUT_DIR = Path(__file__).resolve().parent.parent / "output" / "bronze_test"
RAW_JSON_PATH = OUT_DIR / "gdelt_dengue_raw.json"
SAMPLE_JSON_PATH = OUT_DIR / "gdelt_dengue_sea_sample.json"

# Khop mem (substring, khong phan biet hoa/thuong) voi ten nuoc GDELT tra ve
# trong field "sourcecountry".
SEA_COUNTRY_HINTS = [
    "vietnam",
    "thailand",
    "indonesia",
    "philippines",
    "malaysia",
    "myanmar",
    "burma",
    "cambodia",
    "laos",
    "singapore",
    "brunei",
    "timor-leste",
    "timor leste",
]


def call_gdelt(query: str, maxrecords: int = 50, timespan: str = "7d") -> dict:
    params = {
        "query": query,
        "mode": "artlist",
        "maxrecords": maxrecords,
        "timespan": timespan,
        "sort": "datedesc",
        "format": "json",
    }
    print(f"[1/3] Calling GDELT DOC API with query={query!r} timespan={timespan}")
    resp = requests.get(API_URL, params=params, headers=HEADERS, timeout=30)
    print(f"      -> HTTP {resp.status_code}, {len(resp.content):,} bytes")
    resp.raise_for_status()
    try:
        return resp.json()
    except json.JSONDecodeError:
        raise RuntimeError(
            f"Response is not valid JSON (first 300 chars): {resp.text[:300]!r}"
        )


def filter_sea(articles: list[dict]) -> list[dict]:
    print("[2/3] Filtering articles for Southeast Asia (by sourcecountry)")
    filtered = []
    countries_seen = set()
    for a in articles:
        country = (a.get("sourcecountry") or "").strip()
        countries_seen.add(country)
        lc = country.lower()
        if any(hint in lc for hint in SEA_COUNTRY_HINTS):
            filtered.append(a)
    print(f"      -> all sourcecountry values seen: {sorted(countries_seen)}")
    print(f"      -> {len(filtered)} / {len(articles)} articles match Southeast Asia")
    return filtered


def report(articles: list[dict], filtered: list[dict]) -> None:
    print("[3/3] Schema / sample check")
    if not articles:
        print("      -> EMPTY result set, cannot inspect schema.")
        return

    columns = sorted(articles[0].keys())
    print(f"      -> fields per article: {columns}")

    required_like = {
        "time": [c for c in columns if "date" in c.lower()],
        "location": [c for c in columns if "country" in c.lower() or "location" in c.lower()],
        "case_count": "KHONG CO - day la nguon tin tuc, khong co so ca, chi co bai bao noi den dich",
    }
    print(f"      -> candidate columns per required field: {required_like}")

    print("\n      Sample articles (Southeast Asia, first 5):")
    for a in filtered[:5]:
        print(f"      - [{a.get('seendate')}] ({a.get('sourcecountry')}) {a.get('title')} -> {a.get('url')}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(SAMPLE_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(filtered, f, ensure_ascii=False, indent=2)
    print(f"\n      -> saved sample ({len(filtered)} articles) to {SAMPLE_JSON_PATH}")


def main() -> int:
    try:
        data = call_gdelt(query="dengue", maxrecords=75, timespan="14d")

        OUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(RAW_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        articles = data.get("articles", [])
        if not articles:
            print(f"      -> API returned no 'articles' key or empty list. Raw keys: {list(data.keys())}")

        filtered = filter_sea(articles)
        report(articles, filtered)

        if filtered:
            print("\nRESULT: GDELT news source is USABLE for MVP (note: khong co so ca benh, chi la tin tuc - can NLP/manual de suy ra so ca neu can).")
        else:
            print("\nRESULT: API goi duoc nhung KHONG tim thay bai viet Dong Nam A trong query nay - thu query/timespan khac.")
        return 0
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        print(f"\nRESULT: FAILED - HTTP error {status}: {e}")
        if status == 429:
            print("      -> Bi rate-limit. Thu lai sau vai phut hoac giam maxrecords/tan suat goi.")
        return 1
    except requests.RequestException as e:
        print(f"\nRESULT: FAILED - network error: {e}")
        return 1
    except Exception as e:
        print(f"\nRESULT: FAILED - unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
