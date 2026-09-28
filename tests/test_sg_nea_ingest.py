"""Unit tests for the Singapore NEA dengue-cluster ingest job.

These cover the pure functions only — no network, no Spark. The parts that do
touch those are verified by running the job and inspecting the table, since
mocking them would mostly test the mocks.
"""

from datetime import datetime

import pytest

from ingestion.common.validation import IngestionValidationError
from ingestion.sg_nea_dengue import (
    build_rows,
    feature_to_row,
    poll_slot_id,
    validate_snapshot,
)

# Shaped after a real feature observed on 2026-09-25, trimmed to two polygon
# points so the fixture stays readable.
SAMPLE_FEATURE = {
    "type": "Feature",
    "geometry": {
        "type": "Polygon",
        "coordinates": [[[103.748, 1.348], [103.749, 1.349]]],
    },
    "properties": {
        "OBJECTID": 527711,
        "LOCALITY": "Bt Batok St 21 (Blk 207, 209, 210)  ",
        "CASE_SIZE": 3,
        "FMEL_UPD_D": "20260922152547",
        "INC_CRC": "639FF7772D47C8E0",
        "HOMES": None,
    },
}


class TestPollSlotId:
    """The slot id is what makes re-running a scheduled poll idempotent."""

    def test_moments_within_one_hour_share_a_slot(self) -> None:
        start = poll_slot_id(datetime(2026, 9, 25, 6, 0, 0))
        middle = poll_slot_id(datetime(2026, 9, 25, 6, 4, 12))
        end = poll_slot_id(datetime(2026, 9, 25, 6, 59, 59))
        assert start == middle == end == "20260925T0600Z"

    def test_next_hour_starts_a_new_slot(self) -> None:
        assert poll_slot_id(datetime(2026, 9, 25, 7, 0, 0)) == "20260925T0700Z"

    def test_midnight_is_slot_zero(self) -> None:
        assert poll_slot_id(datetime(2026, 9, 25, 0, 3, 0)) == "20260925T0000Z"

    def test_slot_width_is_configurable(self) -> None:
        assert (
            poll_slot_id(datetime(2026, 9, 25, 6, 44, 0), slot_minutes=30)
            == "20260925T0630Z"
        )


class TestFeatureToRow:
    """Flattening must preserve the source faithfully — bronze keeps raw data."""

    def _row(self) -> dict:
        return feature_to_row(SAMPLE_FEATURE, "20260925T0600Z", datetime(2026, 9, 25))

    def test_maps_the_documented_fields(self) -> None:
        row = self._row()
        assert row["object_id"] == "527711"
        assert row["case_count"] == 3
        assert row["cluster_updated_at_raw"] == "20260922152547"
        assert row["inc_crc"] == "639FF7772D47C8E0"

    def test_records_which_landing_file_the_row_came_from(self) -> None:
        # Spark cannot derive this itself here: the frame is built with
        # createDataFrame(), so input_file_name() would come back empty.
        row = feature_to_row(
            SAMPLE_FEATURE,
            "20260925T0600Z",
            datetime(2026, 9, 25),
            source_file="clusters_20260925T060000Z.json",
        )
        assert row["_source_file"] == "clusters_20260925T060000Z.json"

    def test_strips_localities_padded_by_the_source(self) -> None:
        assert self._row()["locality"] == "Bt Batok St 21 (Blk 207, 209, 210)"

    def test_keeps_the_whole_feature_in_raw_payload(self) -> None:
        # Fields not promoted to columns (HOMES here) must still be recoverable.
        assert "HOMES" in self._row()["raw_payload"]

    def test_serialises_geometry_rather_than_dropping_it(self) -> None:
        assert "103.748" in self._row()["polygon_geojson"]

    def test_object_id_is_a_string_not_an_identity(self) -> None:
        # OBJECTID is renumbered by the source on every publish, so it is kept
        # as an opaque string and never used as a key.
        assert isinstance(self._row()["object_id"], str)


class TestBuildRows:
    def test_every_row_of_one_fetch_shares_a_batch_id(self) -> None:
        collection = {
            "type": "FeatureCollection",
            "features": [SAMPLE_FEATURE, SAMPLE_FEATURE],
        }
        rows = build_rows(collection, fetched_at=datetime(2026, 9, 25, 6, 30))
        assert [row["batch_id"] for row in rows] == ["20260925T0600Z"] * 2

    def test_empty_collection_produces_no_rows(self) -> None:
        assert build_rows({"type": "FeatureCollection", "features": []}) == []


class TestValidateSnapshot:
    """Structural problems must stop the write; they mean the fetch went wrong."""

    def test_accepts_a_populated_collection(self) -> None:
        validate_snapshot({"type": "FeatureCollection", "features": [SAMPLE_FEATURE]})

    def test_rejects_a_collection_with_no_features(self) -> None:
        # Writing this would record a false "no dengue clusters" state.
        with pytest.raises(IngestionValidationError, match="khong co feature"):
            validate_snapshot({"type": "FeatureCollection", "features": []})

    def test_rejects_a_payload_that_is_not_a_feature_collection(self) -> None:
        with pytest.raises(IngestionValidationError, match="FeatureCollection"):
            validate_snapshot({"type": "Feature", "features": [SAMPLE_FEATURE]})
