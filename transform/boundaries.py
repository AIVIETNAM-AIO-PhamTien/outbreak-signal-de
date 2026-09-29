"""Ranh gioi cap tinh cho ban do HealthMap (Natural Earth admin-1, 1:10m).

Tai file goc MOT LAN ve data/reference/ (giu nguyen ban goc), roi loc ra 11 nuoc
SEA va lam tron toa do de file nhe, du cho trinh duyet ve ban do.

Khoa noi voi OpenDengue: truong `iso_3166_2` cua Natural Earth <-> cot
`RNE_iso_code` cua OpenDengue. Bai bao goc cua OpenDengue (Sci Data 2024) dinh
nghia RNE_iso_code la "RnaturalEarth ISO code" - tuc chinh ma nay.
"""

import json
from pathlib import Path
from typing import Any

import requests

from transform import common
from transform.reference import COUNTRIES

SOURCE_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/"
    "geojson/ne_10m_admin_1_states_provinces.geojson"
)
RAW_NAME = "ne_10m_admin_1_states_provinces.geojson"
SEA_NAME = "sea_admin1.geojson"

# Truong giu lai trong file da loc - du de noi khoa, hien ten tren ban do, va
# toa do tam (latitude/longitude) de noi tinh cu -> moi bang diem-trong-polygon.
# name_vi: ten tieng Viet cua 63 tinh CU - bao chi van nhac ten cu sau sap nhap 7/2025.
KEEP_PROPERTIES = ("iso_3166_2", "name", "adm0_a3", "type_en", "latitude", "longitude",
                   "name_vi")


def _round_coords(coords: Any, digits: int) -> Any:
    """Lam tron de quy moi toa do trong mot geometry GeoJSON.

    Args:
        coords: Mang toa do long nhau (Polygon / MultiPolygon).
        digits: So chu so thap phan giu lai.

    Returns:
        Mang cung cau truc, toa do da lam tron.
    """
    if isinstance(coords, (int, float)):
        return round(coords, digits)
    return [_round_coords(item, digits) for item in coords]


def filter_sea(collection: dict[str, Any], digits: int = 3) -> dict[str, Any]:
    """Loc FeatureCollection con 11 nuoc SEA, bot thuoc tinh, lam tron toa do.

    3 chu so thap phan ~ 100 m - thua du cho ban do cap tinh.

    Args:
        collection: GeoJSON FeatureCollection goc cua Natural Earth.
        digits: So chu so thap phan cua toa do.

    Returns:
        FeatureCollection moi, chi gom tinh cua 11 nuoc.

    Raises:
        KeyError: Neu feature khong co truong adm0_a3 (file sai dinh dang).
    """
    iso3 = {country.iso3 for country in COUNTRIES}
    features = []
    for feature in collection["features"]:
        props = feature["properties"]
        if props["adm0_a3"] not in iso3:
            continue
        geometry = dict(feature["geometry"])
        geometry["coordinates"] = _round_coords(geometry["coordinates"], digits)
        features.append(
            {
                "type": "Feature",
                "properties": {key: props.get(key) for key in KEEP_PROPERTIES},
                "geometry": geometry,
            }
        )
    return {"type": "FeatureCollection", "features": features}


def ensure_boundaries(directory: Path | None = None, timeout: int = 300) -> Path:
    """Dam bao co file ranh gioi SEA; tai va loc neu chua co.

    Args:
        directory: Thu muc luu, mac dinh data/reference/.
        timeout: Timeout tai file goc (giay) - file ~40 MB.

    Returns:
        Duong dan file sea_admin1.geojson.

    Raises:
        requests.HTTPError: Neu tai file goc that bai.
    """
    target = directory or common.REFERENCE_ROOT
    target.mkdir(parents=True, exist_ok=True)
    sea_path = target / SEA_NAME
    if sea_path.exists():
        return sea_path

    raw_path = target / RAW_NAME
    if not raw_path.exists():
        response = requests.get(SOURCE_URL, timeout=timeout)
        response.raise_for_status()
        raw_path.write_bytes(response.content)

    collection = json.loads(raw_path.read_text(encoding="utf-8"))
    sea_path.write_text(json.dumps(filter_sea(collection)), encoding="utf-8")
    return sea_path


def load_boundaries(path: Path) -> dict[str, Any]:
    """Doc file ranh gioi da loc.

    Args:
        path: Duong dan sea_admin1.geojson.

    Returns:
        FeatureCollection.
    """
    return json.loads(path.read_text(encoding="utf-8"))


FEATURE_SCHEMA = (
    "iso3 string, iso_3166_2 string, name string, name_vi string, "
    "longitude double, latitude double, geometry string"
)


def feature_rows(collection: dict[str, Any]) -> list[tuple]:
    """Feature ranh gioi thanh cac dong cho Spark (theo FEATURE_SCHEMA).

    Args:
        collection: FeatureCollection da loc.

    Returns:
        Danh sach tuple (iso3, iso_3166_2, name, name_vi, longitude, latitude,
        geometry GeoJSON).
    """
    rows = []
    for feature in collection["features"]:
        p = feature["properties"]
        rows.append((
            p.get("adm0_a3"), p.get("iso_3166_2"), p.get("name"), p.get("name_vi"),
            float(p["longitude"]) if p.get("longitude") is not None else None,
            float(p["latitude"]) if p.get("latitude") is not None else None,
            json.dumps(feature["geometry"]),
        ))
    return rows
