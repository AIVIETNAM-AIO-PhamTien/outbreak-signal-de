"""Phase 0 smoke test: confirm PySpark + Delta Lake + Java work together.

Writes a trivial Delta table to ./data/_smoke_test/, then reads it back and
prints the result. Run with:

    python scripts/smoke_test.py

Success looks like: a printed DataFrame with 3 rows, and a
data/_smoke_test/_delta_log/ directory containing committed JSON files.
"""

from pathlib import Path

from ingestion.common.spark_session import build_spark_session

SMOKE_TEST_PATH = str(Path(__file__).resolve().parent.parent / "data" / "_smoke_test")


def run_smoke_test() -> None:
    """Write and read back a trivial Delta table, printing the result."""
    spark = build_spark_session("phase0_smoke_test")
    try:
        df = spark.createDataFrame(
            [("ok", 1), ("ok", 2), ("ok", 3)], schema=["status", "value"]
        )
        df.write.format("delta").mode("overwrite").save(SMOKE_TEST_PATH)

        readback = spark.read.format("delta").load(SMOKE_TEST_PATH)
        readback.show()
        print(f"Smoke test OK — Delta table read back from {SMOKE_TEST_PATH}")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_smoke_test()
