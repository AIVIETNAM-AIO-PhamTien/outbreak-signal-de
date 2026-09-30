"""Doc lai tang Bronze de kiem tra du lieu da nap dung chua.

Dung de kiem tra nhanh sau khi chay batch, va de chup man hinh cho bao cao:
cho thay du lieu that su nam trong Bronze duoi dang bang Delta, doc lai duoc
bang Spark, va co day du cot lineage.

Chay:
    python scripts/check_bronze.py
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.common.config import all_sources  # noqa: E402
from ingestion.common.paths import (  # noqa: E402
    METADATA_ROOT,
    PARTITION_COLUMN,
    bronze_path,
)
from ingestion.common.spark_session import build_spark_session  # noqa: E402


def show_bronze(spark, source: str) -> None:
    """In schema, so dong theo ngay va vai dong mau cua mot bang Bronze.

    Args:
        spark: SparkSession dang hoat dong.
        source: Ten nguon.
    """
    path = bronze_path(source)
    print(f"\n{'=' * 70}\nBRONZE: {source}\n{'=' * 70}")

    if not path.exists():
        print("  (chua co du lieu - chay scripts/run_batch.py truoc)")
        return

    frame = spark.read.format("delta").load(str(path))
    print(f"  So dong : {frame.count():,}")
    print(f"  So cot  : {len(frame.columns)}")
    print(f"  Cac cot : {frame.columns}")

    print(f"\n  So dong theo {PARTITION_COLUMN}:")
    frame.groupBy(PARTITION_COLUMN).count().orderBy(PARTITION_COLUMN).show(
        truncate=False
    )

    print("  5 dong dau:")
    frame.show(5, truncate=50)


def show_metadata(source: str) -> None:
    """In ban ghi metadata moi nhat cua mot nguon.

    Args:
        source: Ten nguon.
    """
    root = METADATA_ROOT / source
    if not root.exists():
        return

    files = sorted(root.rglob("*.json"))
    if not files:
        return

    latest = files[-1]
    record = json.loads(latest.read_text(encoding="utf-8"))
    print(f"\n  Metadata lan chay gan nhat ({latest.name}):")
    for key in (
        "status",
        "record_count",
        "source_format",
        "source_url",
        "bytes_downloaded",
        "duration_seconds",
        "error_message",
    ):
        print(f"    {key:<18} {record.get(key)}")


def main() -> int:
    """Diem vao CLI.

    Returns:
        Luon 0 - day la script kiem tra, khong phai kiem thu.
    """
    spark = build_spark_session("check_bronze")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        for source in all_sources():
            show_bronze(spark, source)
            show_metadata(source)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
