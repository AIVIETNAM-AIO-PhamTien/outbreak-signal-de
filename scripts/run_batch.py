"""Chay batch ingestion cho cac nguon dang bat trong configs/sources.yaml.

Dung cho scheduler (Windows Task Scheduler / cron). Vi du:

    python scripts/run_batch.py                      # moi nguon dang bat
    python scripts/run_batch.py --source opendengue  # rieng mot nguon
    python scripts/run_batch.py --source news_rss --source sg_nea

CACH LY LOI: moi nguon chay trong try/except rieng. Mot nguon chet khong keo
theo nguon khac, va du lieu nguon da nap thanh cong KHONG bi xoa. Bang tong
ket luon duoc in ra, ke ca khi co nguon that bai.

Exit code: 0 neu khong nguon nao FAILED (SKIPPED khong tinh la loi), 1 neu co.
"""

import argparse
import sys
from pathlib import Path

# Cho phep chay truc tiep `python scripts/run_batch.py` ma khong can set
# PYTHONPATH: them goc repo vao sys.path truoc khi import package ingestion.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion import news_rss, opendengue, sg_nea_dengue, who_gho  # noqa: E402
from ingestion.common.config import all_sources, source_config  # noqa: E402
from ingestion.common.logging import get_logger  # noqa: E402
from ingestion.common.metadata import (  # noqa: E402
    STATUS_FAILED,
    STATUS_SKIPPED,
    STATUS_SUCCESS,
)
from ingestion.common.spark_session import build_spark_session  # noqa: E402

log = get_logger("run_batch")

# Nguon nao da co module ingestion. Nguon khai bao trong YAML nhung chua co
# module (gdelt) se bao SKIPPED kem ly do, chu khong lam vo batch.
JOBS = {
    "opendengue": opendengue.ingest,
    "news_rss": news_rss.ingest,
    "sg_nea": sg_nea_dengue.ingest,
    "who_gho": who_gho.ingest,
}


def run_source(name: str, spark) -> tuple[str, int | None, str | None]:
    """Chay mot nguon va bat moi loi cua rieng no.

    Args:
        name: Ten nguon trong configs/sources.yaml.
        spark: SparkSession dung chung cho ca batch.

    Returns:
        Bo ba (status, record_count, reason). `reason` chi khac None khi
        nguon bi bo qua hoac that bai.
    """
    try:
        cfg = source_config(name)
    except Exception as error:
        return STATUS_FAILED, None, f"{type(error).__name__}: {error}"

    if not cfg.get("enabled", False):
        return STATUS_SKIPPED, None, cfg.get("note", "enabled: false trong config")

    if name not in JOBS:
        return STATUS_SKIPPED, None, "chua co module ingestion cho nguon nay"

    try:
        meta = JOBS[name](spark=spark)
        return STATUS_SUCCESS, meta.record_count, None
    except Exception as error:
        # Bat Exception rong la CO Y o day: moi nguon phai that bai doc lap.
        # Loi da duoc log day du va ghi vao metadata ben trong ingest().
        log.exception("Nguon %s that bai", name)
        return STATUS_FAILED, None, f"{type(error).__name__}: {error}"


def print_report(results: dict[str, tuple[str, int | None, str | None]]) -> None:
    """In bang tong ket cuoi batch.

    Args:
        results: Ket qua tung nguon, theo thu tu da chay.
    """
    width = max(len(name) for name in results) + 1
    print("\n" + "=" * 60)
    print("TONG KET INGESTION")
    print("=" * 60)
    for name, (status, count, reason) in results.items():
        detail = f"{count:,} dong" if count is not None else (reason or "")
        print(f"  {name:<{width}} {status.upper():<8} {detail}")
    print("=" * 60)


def main() -> int:
    """Diem vao CLI.

    Returns:
        0 neu khong nguon nao that bai, 1 neu co.
    """
    parser = argparse.ArgumentParser(
        description="Batch ingestion cho OutbreakSignal DE"
    )
    parser.add_argument(
        "--source",
        action="append",
        choices=all_sources(),
        help="Nguon can chay. Lap lai duoc. Mac dinh: moi nguon dang bat.",
    )
    args = parser.parse_args()

    names = args.source or all_sources()
    log.info("Batch bat dau, %d nguon: %s", len(names), ", ".join(names))

    spark = build_spark_session("outbreak_signal_batch")
    try:
        results = {name: run_source(name, spark) for name in names}
    finally:
        spark.stop()

    print_report(results)
    failed = [n for n, (status, _, _) in results.items() if status == STATUS_FAILED]
    if failed:
        log.error("Batch ket thuc, %d nguon that bai: %s", len(failed), ", ".join(failed))
        return 1

    log.info("Batch ket thuc, khong co nguon nao that bai")
    return 0


if __name__ == "__main__":
    sys.exit(main())
