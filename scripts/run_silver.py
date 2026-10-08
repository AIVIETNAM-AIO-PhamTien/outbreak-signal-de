"""Build Silver tables from Bronze; news is independent of dengue tables.

Usage: python scripts/run_silver.py [--table news]
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.common.paths import silver_path  # noqa: E402
from ingestion.common.spark_session import build_spark_session  # noqa: E402
from ingestion.silver import administrative_boundaries, dengue_history, news  # noqa: E402

JOBS = {
    "administrative_boundaries": administrative_boundaries.ingest,
    "dengue_history": dengue_history.ingest,
    "news": news.ingest,
}


def ordered_jobs(requested: list[str] | None) -> list[str]:
    """Always refresh the COD lookup before building dengue history."""
    selected = set(requested or JOBS)
    if "dengue_history" in selected:
        selected.add("administrative_boundaries")
    return [table for table in JOBS if table in selected]


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Silver tables directly from Bronze")
    parser.add_argument("--table", action="append", choices=tuple(JOBS),
                        help="Run only this Silver table; may be repeated")
    args = parser.parse_args()
    spark = build_spark_session("silver_ingestion")
    try:
        for table in ordered_jobs(args.table):
            print(f"Silver {table}: {silver_path(table)}")
            for name, value in JOBS[table](spark).items():
                print(f"    {name}: {value}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
