"""
Spike test: Google News RSS lam nguon tin tuc thay the / bo sung cho GDELT.

Ly do them script nay: GDELT DOC API bi 429 (rate-limit) tu moi trong sandbox
hien tai ngay ca khi goi 1 request duy nhat sau khi cho - can nguon du phong
de co it nhat 1 "news source" chay duoc that su trong toi nay.

KHONG dung PySpark o day - chi la buoc kiem tra nguon.
"""

import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

RSS_URL = "https://news.google.com/rss/search"
HEADERS = {"User-Agent": "OutbreakSignalDE-spike/0.1 (student project, ingestion test)"}

OUT_DIR = Path(__file__).resolve().parent.parent / "output" / "bronze_test"
SAMPLE_JSON_PATH = OUT_DIR / "google_news_rss_sea_sample.json"

SEA_COUNTRIES = [
    "vietnam", "thailand", "indonesia", "philippines", "malaysia",
    "myanmar", "cambodia", "laos", "singapore", "brunei", "timor-leste",
]


def fetch_rss(query: str, hl: str = "en", gl: str = "US") -> list[dict]:
    params = {"q": query, "hl": hl, "gl": gl, "ceid": f"{gl}:{hl}"}
    print(f"[1/2] Fetching Google News RSS with q={query!r}")
    resp = requests.get(RSS_URL, params=params, headers=HEADERS, timeout=30)
    print(f"      -> HTTP {resp.status_code}, {len(resp.content):,} bytes")
    resp.raise_for_status()

    root = ET.fromstring(resp.content)
    items = []
    for item in root.findall(".//item"):
        items.append({
            "title": (item.findtext("title") or "").strip(),
            "link": (item.findtext("link") or "").strip(),
            "pubDate": (item.findtext("pubDate") or "").strip(),
            "source": (item.findtext("source") or "").strip(),
        })
    print(f"      -> parsed {len(items)} items")
    return items


def report(items: list[dict]) -> None:
    print("[2/2] Schema / sample check")
    if not items:
        print("      -> EMPTY result, cannot inspect schema.")
        return

    print(f"      -> fields per item: {list(items[0].keys())}")
    print("      -> NOTE: khong co field quoc gia/dia diem tuong minh - phai suy ra")
    print("         tu title/source bang keyword matching hoac NLP sau nay (Silver layer).")

    print("\n      Sample items (first 5):")
    for it in items[:5]:
        print(f"      - [{it['pubDate']}] ({it['source']}) {it['title']}")

    matched = [
        it for it in items
        if any(c in it["title"].lower() for c in SEA_COUNTRIES)
    ]
    print(f"\n      -> {len(matched)}/{len(items)} items mention a Southeast Asia country name in title")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(SAMPLE_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"      -> saved sample ({len(items)} items) to {SAMPLE_JSON_PATH}")


def main() -> int:
    try:
        items = fetch_rss(query="dengue Southeast Asia")
        report(items)
        print("\nRESULT: Google News RSS is USABLE for MVP (khong co so ca, chi tin tuc; can them buoc suy ra quoc gia/dia diem tu title o Silver).")
        return 0
    except requests.HTTPError as e:
        print(f"\nRESULT: FAILED - HTTP error: {e}")
        return 1
    except ET.ParseError as e:
        print(f"\nRESULT: FAILED - response is not valid XML/RSS: {e}")
        return 1
    except Exception as e:
        print(f"\nRESULT: FAILED - unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
