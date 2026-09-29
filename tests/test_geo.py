"""Test phep diem-trong-polygon bang PySpark (transform.geo)."""

import json

import pytest

from transform.common import small_frame
from transform.geo import points_in_polygons

pytestmark = pytest.mark.integration

SQUARE = [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]
HOLE = [[4, 4], [6, 4], [6, 6], [4, 6], [4, 4]]


def geometry(kind: str, coordinates) -> str:
    """Chuoi GeoJSON geometry."""
    return json.dumps({"type": kind, "coordinates": coordinates})


@pytest.fixture
def polygons(spark):
    rows = [
        ("A", "X", geometry("Polygon", [SQUARE, HOLE])),                   # vuong co lo
        ("B", "X", geometry("MultiPolygon", [[[[20, 0], [30, 0], [30, 10], [20, 0]]],
                                             [[[40, 0], [50, 0], [50, 10], [40, 10], [40, 0]]]])),
        ("C", "Y", geometry("Polygon", [SQUARE])),                         # nuoc khac, cung toa do
    ]
    return small_frame(spark, rows, "poly string, iso3 string, geometry string")


def pairs(spark, polygons, points, same=None):
    """Tap (diem, polygon) tim duoc."""
    frame = small_frame(spark, points, "pid string, iso3 string, x double, y double")
    result = points_in_polygons(frame, ["pid"], polygons, ["poly"], same=same)
    return {(r["pid"], r["poly"]) for r in result.collect()}


def test_trong_ngoai_va_lo_thung(spark, polygons) -> None:
    found = pairs(spark, polygons, [
        ("in", "X", 2.0, 2.0),     # trong A
        ("hole", "X", 5.0, 5.0),   # trong lo cua A -> khong thuoc A
        ("out", "X", 15.0, 5.0),   # ngoai moi polygon
        ("multi", "X", 45.0, 5.0),  # phan thu 2 cua MultiPolygon B
    ], same=["iso3"])
    assert found == {("in", "A"), ("multi", "B")}


def test_rang_buoc_cung_nuoc(spark, polygons) -> None:
    # Khong rang buoc: diem (2,2) nam trong ca A (X) va C (Y).
    assert pairs(spark, polygons, [("p", "X", 2.0, 2.0)]) == {("p", "A"), ("p", "C")}
    assert pairs(spark, polygons, [("p", "Y", 2.0, 2.0)], same=["iso3"]) == {("p", "C")}
