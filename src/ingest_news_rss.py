"""
Ingest nguon 2: Google News RSS (tin tuc ve sot xuat huyet).

Pipeline 2 buoc, giong nguon OpenDengue:
  Buoc 1 (Python thuan): goi RSS -> luu file .xml goc + file .jsonl vao landing/
  Buoc 2 (PySpark):      doc cac file .jsonl -> ghi Parquet vao bronze/

Vi sao luu ca .xml lan .jsonl:
  - .xml la du lieu GOC y nguyen nhu server tra ve (dung tinh than Bronze).
  - .jsonl la ban chuyen doi de Spark doc duoc (Spark khong doc duoc XML neu
    khong cai them thu vien spark-xml). Khong sua noi dung, chi doi dinh dang.

Luu y ve lich chay: day la nguon tin tuc, bai moi xuat hien lien tuc nen chay
moi 15-60 phut la hop ly. Moi lan chay tao them 1 file trong landing cua ngay
hom do, va Bronze cua ngay do se duoc dung lai tu TOAN BO file trong ngay
(nen chay lai nhieu lan khong bi nhan doi du lieu).
"""

import json
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import requests

from spark_utils import get_spark, landing_dir, today_str, write_bronze

SOURCE = "news_rss"
RSS_URL = "https://news.google.com/rss/search"
QUERY = "dengue Southeast Asia"
HEADERS = {"User-Agent": "OutbreakSignalDE/0.1 (AIO student project)"}


def fetch_raw(ingest_date: str) -> Path:
    """Buoc 1: goi RSS, luu file .xml goc va file .jsonl. Tra ve duong dan .jsonl."""
    dest_dir = landing_dir(SOURCE, ingest_date)
    stamp = datetime.now(timezone.utc).strftime("%H%M%S")

    print(f"[1/2] Goi Google News RSS, query={QUERY!r}")
    params = {"q": QUERY, "hl": "en", "gl": "US", "ceid": "US:en"}
    resp = requests.get(RSS_URL, params=params, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    print(f"      -> HTTP {resp.status_code}, {len(resp.content):,} bytes")

    xml_path = dest_dir / f"google_news_{stamp}.xml"
    xml_path.write_bytes(resp.content)

    root = ET.fromstring(resp.content)
    items = [
        {
            "title": (it.findtext("title") or "").strip(),
            "link": (it.findtext("link") or "").strip(),
            "pubDate": (it.findtext("pubDate") or "").strip(),
            "source": (it.findtext("source") or "").strip(),
            "description": (it.findtext("description") or "").strip(),
        }
        for it in root.findall(".//item")
    ]

    jsonl_path = dest_dir / f"google_news_{stamp}.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")

    print(f"      -> {len(items)} bai viet, luu vao {jsonl_path.name} (va ban .xml goc)")
    return jsonl_path


def load_to_bronze(ingest_date: str) -> int:
    """Buoc 2: Spark doc TAT CA file .jsonl cua ngay hom nay va ghi vao Bronze."""
    print("[2/2] Spark doc cac file JSONL va ghi vao Bronze")
    spark = get_spark(f"ingest_{SOURCE}")

    # Doc ca thu muc cua ngay, khong chi file vua tai. Ly do: mot ngay chay
    # nhieu lan (moi 15-60 phut), nen Bronze cua ngay do phai gom tat ca cac
    # lan chay trong ngay. Cach nay cung giup chay lai khong bi nhan doi.
    day_glob = str(landing_dir(SOURCE, ingest_date) / "*.jsonl")
    df = spark.read.json(day_glob)

    # KHONG loc theo quoc gia o day. Viec suy ra quoc gia tu title
    # la nhiem vu cua Silver layer.
    return write_bronze(df, SOURCE, ingest_date)


def main() -> int:
    ingest_date = today_str()
    print(f"=== Ingest {SOURCE} | ingest_date={ingest_date} ===")
    try:
        fetch_raw(ingest_date)
        count = load_to_bronze(ingest_date)
        print(f"=== XONG: {count:,} dong vao Bronze ===")
        return 0
    except requests.RequestException as e:
        print(f"=== LOI mang khi goi RSS: {e} ===", file=sys.stderr)
        return 1
    except ET.ParseError as e:
        print(f"=== LOI: response khong phai XML hop le: {e} ===", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
