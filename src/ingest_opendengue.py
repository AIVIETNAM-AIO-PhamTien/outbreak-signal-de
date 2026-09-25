"""
Ingest nguon 1: OpenDengue (so ca benh sot xuat huyet).

Pipeline 2 buoc:
  Buoc 1 (Python thuan): tai file zip -> giai nen CSV -> luu vao data/landing/
  Buoc 2 (PySpark):      doc CSV do -> ghi Parquet vao data/bronze/

Vi sao tach 2 buoc: Spark KHONG goi duoc API/tai duoc file tu Internet.
Spark chi biet doc file co san tren dia. Nen phai dung Python tai ve truoc.

Luu y ve lich chay: OpenDengue KHONG phai live data - ho phat hanh theo
version (V1.3, V1.2...), vai thang moi co ban moi. Nen chay 1 lan/ngay la du,
khong can chay moi 15-60 phut nhu nguon tin tuc.
"""

import sys
import zipfile
from pathlib import Path

import requests

from spark_utils import get_spark, landing_dir, today_str, write_bronze

SOURCE = "opendengue"
DATA_URL = (
    "https://github.com/OpenDengue/master-repo/raw/main/data/releases/"
    "V1.3/National_extract_V1_3.zip"
)


def fetch_raw(ingest_date: str) -> Path:
    """Buoc 1: tai zip ve va giai nen lay file CSV. Tra ve duong dan CSV."""
    dest_dir = landing_dir(SOURCE, ingest_date)
    zip_path = dest_dir / "National_extract_V1_3.zip"

    if zip_path.exists():
        print(f"[1/2] Da co file tai truoc do: {zip_path}")
    else:
        print(f"[1/2] Dang tai {DATA_URL}")
        resp = requests.get(DATA_URL, timeout=120)
        resp.raise_for_status()
        zip_path.write_bytes(resp.content)
        print(f"      -> luu {len(resp.content):,} bytes vao {zip_path}")

    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"Khong tim thay file CSV trong zip: {zf.namelist()}")
        zf.extract(csv_names[0], dest_dir)

    csv_path = dest_dir / csv_names[0]
    print(f"      -> giai nen ra {csv_path}")
    return csv_path


def load_to_bronze(csv_path: Path, ingest_date: str) -> int:
    """Buoc 2: Spark doc CSV va ghi vao Bronze."""
    print("[2/2] Spark doc CSV va ghi vao Bronze")
    spark = get_spark(f"ingest_{SOURCE}")

    # inferSchema=False -> doc tat ca thanh kieu string.
    # Day la CO Y: Bronze giu nguyen trang, khong ep kieu du lieu.
    # Viec doi sang kieu so/ngay thang de Silver layer lam.
    df = spark.read.csv(str(csv_path), header=True, inferSchema=False)

    # KHONG loc rieng Dong Nam A o day. Bronze luu toan bo du lieu goc;
    # viec loc theo khu vuc la nhiem vu cua Silver layer.
    return write_bronze(df, SOURCE, ingest_date)


def main() -> int:
    ingest_date = today_str()
    print(f"=== Ingest {SOURCE} | ingest_date={ingest_date} ===")
    try:
        csv_path = fetch_raw(ingest_date)
        count = load_to_bronze(csv_path, ingest_date)
        print(f"=== XONG: {count:,} dong vao Bronze ===")
        return 0
    except requests.RequestException as e:
        print(f"=== LOI mang khi tai du lieu: {e} ===", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
