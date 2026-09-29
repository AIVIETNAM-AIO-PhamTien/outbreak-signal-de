"""Phep "diem nam trong polygon" bang PySpark thuan (khong Sedona, khong UDF).

Dung de lam bang noi (crosswalk) giua cac he ma dia ly:
    - tam don vi COD (P-code) nam trong polygon Natural Earth nao -> ma ISO 3166-2
    - tam 63 tinh cu cua Viet Nam nam trong polygon 34 tinh moi nao -> bang noi cu/moi

Thuat toan ray casting: ban mot tia ngang tu diem sang phai, dem so canh polygon
bi cat; so le = diem nam trong. Dem tren MOI vong (ke ca lo thung) cho ket qua
dung voi polygon co lo. Loc truoc bang khung bao (bbox) de khong phai so diem
voi moi canh cua moi polygon.

Geometry vao dang chuoi GeoJSON (Polygon hoac MultiPolygon), toa do [lon, lat].
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

POLYGON_SCHEMA = "array<array<array<double>>>"
MULTIPOLYGON_SCHEMA = "array<array<array<array<double>>>>"


def polygon_edges(frame: DataFrame, geometry: str, keys: list[str]) -> DataFrame:
    """Tach geometry GeoJSON thanh cac canh (x1, y1, x2, y2).

    Args:
        frame: DataFrame co cot geometry (chuoi GeoJSON) va cot khoa.
        geometry: Ten cot geometry.
        keys: Cot dinh danh polygon (giu lai tren moi canh).

    Returns:
        DataFrame keys + x1, y1, x2, y2 - moi dong mot canh.
    """
    gtype = F.get_json_object(geometry, "$.type")
    coords = F.get_json_object(geometry, "$.coordinates")
    rings = F.when(gtype == "Polygon", F.from_json(coords, POLYGON_SCHEMA)).when(
        gtype == "MultiPolygon", F.flatten(F.from_json(coords, MULTIPOLYGON_SCHEMA)))
    with_rings = frame.select(*keys, F.explode(rings).alias("ring"))
    edges = F.transform(
        F.sequence(F.lit(0), F.size("ring") - 2),
        lambda i: F.struct(
            F.col("ring")[i][0].alias("x1"), F.col("ring")[i][1].alias("y1"),
            F.col("ring")[i + 1][0].alias("x2"), F.col("ring")[i + 1][1].alias("y2"),
        ),
    )
    return (
        with_rings.where(F.size("ring") >= 4)  # vong hop le co it nhat 4 diem
        .select(*keys, F.explode(edges).alias("e"))
        .select(*keys, "e.x1", "e.y1", "e.x2", "e.y2")
    )


def points_in_polygons(
    points: DataFrame,
    point_keys: list[str],
    polygons: DataFrame,
    polygon_keys: list[str],
    geometry: str = "geometry",
    same: list[str] | None = None,
) -> DataFrame:
    """Voi moi diem, tim polygon chua no.

    Args:
        points: DataFrame co point_keys + x (kinh do), y (vi do).
        point_keys: Cot dinh danh diem.
        polygons: DataFrame co polygon_keys + cot geometry (chuoi GeoJSON).
        polygon_keys: Cot dinh danh polygon.
        geometry: Ten cot geometry.
        same: Cot phai bang nhau giua diem va polygon (vd ["iso3"]) - thu hep
            phep noi, tranh so diem Viet Nam voi polygon Thai Lan.

    Returns:
        DataFrame point_keys + polygon_keys: moi cap (diem, polygon chua diem).
        Diem khong nam trong polygon nao (vd tam dao ngoai bo bien do phan giai)
        khong co dong - nguoi goi tu kiem tra do phu.
    """
    same = same or []
    edges = polygon_edges(polygons, geometry, polygon_keys + same).cache()
    boxes = edges.groupBy(*polygon_keys, *same).agg(
        F.least(F.min("x1"), F.min("x2")).alias("minx"),
        F.greatest(F.max("x1"), F.max("x2")).alias("maxx"),
        F.least(F.min("y1"), F.min("y2")).alias("miny"),
        F.greatest(F.max("y1"), F.max("y2")).alias("maxy"),
    )
    p = points.select(*point_keys, *same, "x", "y").alias("p")
    b = boxes.alias("b")
    condition = (F.col("p.x").between(F.col("b.minx"), F.col("b.maxx"))
                 & F.col("p.y").between(F.col("b.miny"), F.col("b.maxy")))
    for column in same:
        condition = condition & (F.col(f"p.{column}") == F.col(f"b.{column}"))
    candidates = p.join(b, condition).select(
        *[F.col(f"p.{c}").alias(c) for c in point_keys],
        *[F.col(f"b.{c}").alias(c) for c in polygon_keys + same],
        "p.x", "p.y",
    )

    joined = candidates.join(edges, polygon_keys + same)
    crosses = (
        ((F.col("y1") > F.col("y")) != (F.col("y2") > F.col("y")))
        & (F.col("x") < (F.col("x2") - F.col("x1")) * (F.col("y") - F.col("y1"))
           / (F.col("y2") - F.col("y1")) + F.col("x1"))
    )
    return (
        joined.where(crosses)
        .groupBy(*point_keys, *polygon_keys)
        .count()
        .where(F.col("count") % 2 == 1)
        .drop("count")
    )
