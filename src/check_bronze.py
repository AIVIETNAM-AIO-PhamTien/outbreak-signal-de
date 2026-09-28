"""
Doc lai Bronze layer de kiem tra du lieu da nap dung chua.

Dung de demo / chup man hinh cho bao cao: cho thay du lieu that su nam trong
Bronze duoi dang Parquet va doc lai duoc bang Spark.
"""

import sys

from spark_utils import BRONZE_ROOT, get_spark

SOURCES = ["opendengue", "news_rss"]


def main() -> int:
    spark = get_spark("check_bronze")

    for source in SOURCES:
        path = BRONZE_ROOT / source
        print(f"\n{'=' * 60}\nNguon: {source}\n{'=' * 60}")

        if not path.exists():
            print("  (chua co du lieu - hay chay run_batch.py truoc)")
            continue

        # Spark tu dong nhan ra cac thu muc dang ingest_date=... la partition
        # va them cot 'ingest_date' vao DataFrame.
        df = spark.read.parquet(str(path))

        print(f"  So dong: {df.count():,}")
        print(f"  So cot : {len(df.columns)}")
        print(f"  Cac cot: {df.columns}")

        print("\n  So dong theo ngay nap:")
        df.groupBy("ingest_date").count().orderBy("ingest_date").show(truncate=False)

        print("  5 dong dau:")
        df.show(5, truncate=60)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
