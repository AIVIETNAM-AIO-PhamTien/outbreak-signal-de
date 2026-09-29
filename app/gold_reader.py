"""Doc Gold cho app HealthMap - KHONG dung Spark/JVM.

App chi doc Gold (va bao cao chat luong), bang thu vien `deltalake` (delta-rs):
mo app nhanh, khong phai khoi dong JVM moi lan tai trang.

Duong dan lay tu bien moi truong de test tro sang du lieu nho:
    OUTBREAK_GOLD_ROOT       mac dinh data/gold
    OUTBREAK_QUALITY_ROOT    mac dinh data/quality
    OUTBREAK_BOUNDARY_PATH   mac dinh data/reference/sea_admin1.geojson
"""

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
from deltalake import DeltaTable

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _env_path(name: str, default: Path) -> Path:
    """Doc duong dan tu bien moi truong, co gia tri mac dinh.

    Args:
        name: Ten bien moi truong.
        default: Duong dan mac dinh.

    Returns:
        Duong dan.
    """
    value = os.environ.get(name)
    return Path(value) if value else default


def gold_root() -> Path:
    """Thu muc Gold."""
    return _env_path("OUTBREAK_GOLD_ROOT", PROJECT_ROOT / "data" / "gold")


def quality_root() -> Path:
    """Thu muc bao cao chat luong."""
    return _env_path("OUTBREAK_QUALITY_ROOT", PROJECT_ROOT / "data" / "quality")


def boundary_path() -> Path:
    """File ranh gioi tinh da loc."""
    return _env_path(
        "OUTBREAK_BOUNDARY_PATH", PROJECT_ROOT / "data" / "reference" / "sea_admin1.geojson"
    )


def load_table(name: str) -> pd.DataFrame:
    """Doc mot bang Gold thanh pandas DataFrame.

    Args:
        name: Ten bang Gold.

    Returns:
        DataFrame.

    Raises:
        FileNotFoundError: Neu bang chua duoc dung (chua chay run_transform.py).
    """
    path = gold_root() / name
    if not (path / "_delta_log").exists():
        raise FileNotFoundError(
            f"chua co bang Gold {name!r} o {path} - chay scripts/run_transform.py truoc"
        )
    return DeltaTable(str(path)).to_pandas()


def key_to_date(series: pd.Series) -> pd.Series:
    """Doi khoa ngay yyyymmdd (int) thanh datetime.

    Args:
        series: Cot khoa ngay.

    Returns:
        Cot datetime64.
    """
    return pd.to_datetime(series.astype(str), format="%Y%m%d")


def load_boundaries() -> dict[str, Any] | None:
    """Doc GeoJSON ranh gioi tinh, None neu chua tai.

    Returns:
        FeatureCollection hoac None.
    """
    path = boundary_path()
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def latest_quality_report() -> dict[str, Any] | None:
    """Doc bao cao chat luong moi nhat, None neu chua co.

    Returns:
        Noi dung JSON cua bao cao moi nhat.
    """
    files = sorted(quality_root().glob("quality_*.json"))
    if not files:
        return None
    return json.loads(files[-1].read_text(encoding="utf-8"))
