"""
Chay batch ingestion cho cac nguon.

Dung cho scheduler (Windows Task Scheduler / cron). Vi du:
  python src/run_batch.py --source news          # moi 15-60 phut
  python src/run_batch.py --source opendengue    # 1 lan/ngay
  python src/run_batch.py --source all           # chay ca hai
"""

import argparse
import sys

import ingest_news_rss
import ingest_opendengue

JOBS = {
    "opendengue": ingest_opendengue.main,
    "news": ingest_news_rss.main,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Batch ingestion cho OutbreakSignal DE")
    parser.add_argument(
        "--source",
        choices=[*JOBS.keys(), "all"],
        default="all",
        help="Nguon can chay (mac dinh: all)",
    )
    args = parser.parse_args()

    names = list(JOBS) if args.source == "all" else [args.source]

    results = {}
    for name in names:
        results[name] = JOBS[name]()

    print("\n=== TONG KET ===")
    for name, code in results.items():
        print(f"  {name}: {'OK' if code == 0 else 'THAT BAI'}")

    return 0 if all(code == 0 for code in results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
