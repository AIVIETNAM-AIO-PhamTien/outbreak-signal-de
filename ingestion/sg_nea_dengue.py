"""Ingest Singapore NEA dengue clusters into the bronze layer.

Data source: data.gov.sg dataset `d_dbfabf16158d1b0e1c420627c0819168`,
published by the National Environment Agency (NEA). It is the only source in
this project carrying both real case counts and real coordinates.

Access is two-step: a `poll-download` endpoint returns a short-lived presigned
S3 URL, which then serves the actual GeoJSON.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.common.spark_session import build_spark_session

SOURCE_NAME = "sg_nea"
BRONZE_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "bronze" / "sg_dengue_clusters"
)
DATASET_ID = "d_dbfabf16158d1b0e1c420627c0819168"
POLL_DOWNLOAD_URL = (
    f"https://api-open.data.gov.sg/v1/public/api/datasets/{DATASET_ID}/poll-download"
)
REQUEST_TIMEOUT_SECONDS = 30

# How wide one polling slot is. Every fetch is stamped with the slot it belongs
# to rather than its wall-clock time, so re-running a slot overwrites it instead
# of appending a second copy. Evidence from data/_probe/ shows the source only
# changes every few days, so a wide slot costs nothing in freshness.
POLL_SLOT_MINUTES = 60

BRONZE_SCHEMA = StructType(
    [
        # Identity of the fetch, not of the cluster — see build_rows().
        StructField("batch_id", StringType(), nullable=False),
        StructField("fetched_at", TimestampType(), nullable=False),
        # NEA fields, kept close to the source. OBJECTID is deliberately NOT
        # treated as a key: it is renumbered on every publish (observed
        # 527703 -> 528001 for a cluster whose content did not change at all).
        StructField("object_id", StringType(), nullable=True),
        StructField("locality", StringType(), nullable=True),
        StructField("case_count", IntegerType(), nullable=True),
        StructField("cluster_updated_at_raw", StringType(), nullable=True),
        StructField("inc_crc", StringType(), nullable=True),
        StructField("polygon_geojson", StringType(), nullable=True),
        # Full original feature, so nothing dropped here is lost for good.
        StructField("raw_payload", StringType(), nullable=False),
        StructField("source", StringType(), nullable=False),
    ]
)


def poll_slot_id(moment: datetime, slot_minutes: int = POLL_SLOT_MINUTES) -> str:
    """Return the identifier of the polling slot a moment falls into.

    Truncating to a slot is what makes the job idempotent: two runs triggered
    for the same slot produce the same `batch_id` and therefore replace each
    other, while a genuinely later poll gets a new one even if the payload is
    byte-identical.

    Args:
        moment: The time to classify, expected in UTC.
        slot_minutes: Width of a slot in minutes.

    Returns:
        A slot id such as `"20260925T0600Z"`.
    """
    minutes_into_day = moment.hour * 60 + moment.minute
    slot_start = minutes_into_day - (minutes_into_day % slot_minutes)
    return f"{moment:%Y%m%d}T{slot_start // 60:02d}{slot_start % 60:02d}Z"


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
def _get_json(url: str) -> dict[str, Any]:
    """GET a URL and parse the response as JSON, retrying transient failures.

    Retry wraps this low-level call rather than `fetch_clusters`, so a failure
    on the second request does not force the first one to be repeated.

    Args:
        url: Fully-formed URL to fetch.

    Returns:
        The parsed JSON body.

    Raises:
        requests.HTTPError: If the server returned 4xx/5xx. `requests` does not
            raise on those by default, hence the explicit `raise_for_status()`.
    """
    response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def fetch_clusters() -> dict[str, Any]:
    """Fetch the current dengue-cluster GeoJSON from data.gov.sg.

    Returns:
        A GeoJSON FeatureCollection dict.

    Raises:
        RuntimeError: If the poll-download envelope reports a non-zero `code`.
            data.gov.sg signals application-level errors that way even when the
            HTTP status is 200, so status alone is not enough to trust it.
    """
    envelope = _get_json(POLL_DOWNLOAD_URL)
    if envelope.get("code") != 0:
        raise RuntimeError(
            f"data.gov.sg poll-download failed: code={envelope.get('code')!r} "
            f"errorMsg={envelope.get('errorMsg')!r}"
        )

    return _get_json(envelope["data"]["url"])


def feature_to_row(
    feature: dict[str, Any], batch_id: str, fetched_at: datetime
) -> dict[str, Any]:
    """Flatten one GeoJSON feature into a bronze row.

    Args:
        feature: One entry from the FeatureCollection's `features` list.
        batch_id: Slot id shared by every row of this fetch.
        fetched_at: Wall-clock time of the fetch, kept alongside `batch_id` so
            the actual execution time is still recoverable.

    Returns:
        A dict matching BRONZE_SCHEMA.
    """
    properties = feature.get("properties", {})
    geometry = feature.get("geometry")
    locality = properties.get("LOCALITY")

    return {
        "batch_id": batch_id,
        "fetched_at": fetched_at,
        "object_id": str(properties["OBJECTID"])
        if properties.get("OBJECTID") is not None
        else None,
        # Source pads localities with trailing spaces inconsistently; stripping
        # whitespace is safe enough to do here, unlike reshaping the value.
        "locality": locality.strip() if locality else None,
        "case_count": properties.get("CASE_SIZE"),
        "cluster_updated_at_raw": properties.get("FMEL_UPD_D"),
        "inc_crc": properties.get("INC_CRC"),
        "polygon_geojson": json.dumps(geometry, sort_keys=True) if geometry else None,
        "raw_payload": json.dumps(feature, sort_keys=True, ensure_ascii=False),
        "source": SOURCE_NAME,
    }


def build_rows(
    geojson: dict[str, Any], fetched_at: datetime | None = None
) -> list[dict[str, Any]]:
    """Convert a fetched FeatureCollection into bronze rows.

    Args:
        geojson: The FeatureCollection returned by `fetch_clusters`.
        fetched_at: Override for the fetch time; defaults to now (UTC). Mainly
            an injection point for tests.

    Returns:
        One row dict per feature, all sharing the same `batch_id`.
    """
    fetched_at = fetched_at or datetime.now(timezone.utc).replace(tzinfo=None)
    batch_id = poll_slot_id(fetched_at)
    return [
        feature_to_row(feature, batch_id, fetched_at)
        for feature in geojson.get("features", [])
    ]


def validate_snapshot(geojson: dict[str, Any]) -> None:
    """Reject a payload that is structurally wrong before anything is written.

    Only structural problems raise here — a payload that is not a
    FeatureCollection, or one with no features at all, means the fetch went
    wrong (expired URL, error page, upstream outage) and writing it would
    record a fake "no clusters in Singapore" state.

    Oddities *within* a feature are deliberately not rejected: bronze's job is
    to capture what the source said, and `raw_payload` keeps the original for
    inspection. Cleaning those belongs in silver.

    Args:
        geojson: Payload returned by `fetch_clusters`.

    Raises:
        ValueError: If the payload is not a non-empty FeatureCollection.
    """
    if geojson.get("type") != "FeatureCollection":
        raise ValueError(f"expected a FeatureCollection, got {geojson.get('type')!r}")

    if not geojson.get("features"):
        raise ValueError("FeatureCollection has no features — refusing to write")


def write_bronze(spark: SparkSession, rows: list[dict[str, Any]]) -> None:
    """Write one batch of rows into the bronze Delta table.

    Uses `replaceWhere` scoped to this batch's `batch_id` instead of a plain
    append: re-running the same polling slot overwrites that slot's rows rather
    than adding a second copy, leaving every other slot untouched. That gives
    idempotency without reading the table first.

    Args:
        spark: Active SparkSession.
        rows: Rows to write; all must share one `batch_id`.
    """
    batch_id = rows[0]["batch_id"]
    frame = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"batch_id = '{batch_id}'")
        .save(str(BRONZE_PATH))
    )


def ingest(spark: SparkSession | None = None) -> int:
    """Run one ingest cycle: fetch the snapshot and write it to bronze.

    Args:
        spark: Existing SparkSession to reuse. When omitted, one is created and
            stopped inside this call — how the scheduler will invoke it.

    Returns:
        Number of rows written.
    """
    owns_session = spark is None
    spark = spark or build_spark_session("sg_nea_dengue_ingest")
    try:
        geojson = fetch_clusters()
        validate_snapshot(geojson)
        rows = build_rows(geojson)
        write_bronze(spark, rows)
        return len(rows)
    finally:
        if owns_session:
            spark.stop()


if __name__ == "__main__":
    written = ingest()
    print(f"Wrote {written} row(s) to {BRONZE_PATH}")
