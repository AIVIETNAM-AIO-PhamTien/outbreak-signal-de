"""Doc dinh nghia nguon du lieu tu configs/sources.yaml.

Moi endpoint, tham so va lich chay deu nam trong YAML chu khong rai rac trong
code, de doi mot nguon khong phai sua nhieu file. Gia tri trong `defaults`
duoc ap cho moi nguon, va tung nguon co the ghi de tung khoa mot.
"""

from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "configs" / "sources.yaml"


class ConfigError(Exception):
    """File config thieu, sai dinh dang, hoac hoi mot nguon khong ton tai."""


def load_config(config_path: Path | None = None) -> dict[str, Any]:
    """Doc va parse file YAML dinh nghia nguon.

    Args:
        config_path: Duong dan file config. Mac dinh la configs/sources.yaml
            o goc repo. Tham so nay chu yeu de test truyen file gia vao.

    Returns:
        Dict da parse, luon co khoa "defaults" va "sources".

    Raises:
        ConfigError: Neu file khong ton tai, khong parse duoc, hoac thieu
            khoa "sources".
    """
    path = config_path or CONFIG_PATH
    if not path.exists():
        raise ConfigError(f"khong tim thay file config: {path}")

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"file config sai cu phap YAML: {path}") from exc

    if not isinstance(raw, dict) or "sources" not in raw:
        raise ConfigError(f"file config phai co khoa 'sources' o cap ngoai cung: {path}")

    raw.setdefault("defaults", {})
    return raw


def source_config(name: str, config_path: Path | None = None) -> dict[str, Any]:
    """Tra ve config cua mot nguon, da tron voi `defaults`.

    Args:
        name: Ten nguon, dung nhu khoa trong sources.yaml (vi du "opendengue").
        config_path: Xem load_config().

    Returns:
        Dict config cua nguon, co them khoa "name". Gia tri rieng cua nguon
        de len tren gia tri trong defaults.

    Raises:
        ConfigError: Neu khong co nguon nao ten nhu vay.
    """
    config = load_config(config_path)
    sources = config["sources"]

    if name not in sources:
        raise ConfigError(
            f"khong co nguon ten {name!r} trong config. "
            f"Cac nguon dang khai bao: {sorted(sources)}"
        )

    merged = {**config["defaults"], **(sources[name] or {})}
    merged["name"] = name
    return merged


def enabled_sources(config_path: Path | None = None) -> list[str]:
    """Liet ke ten cac nguon dang bat, theo dung thu tu khai bao trong YAML.

    Args:
        config_path: Xem load_config().

    Returns:
        Danh sach ten nguon co enabled: true.
    """
    config = load_config(config_path)
    return [
        name
        for name, spec in config["sources"].items()
        if (spec or {}).get("enabled", False)
    ]


def all_sources(config_path: Path | None = None) -> list[str]:
    """Liet ke moi nguon khai bao trong config, ke ca nguon dang tat.

    Args:
        config_path: Xem load_config().

    Returns:
        Danh sach ten nguon.
    """
    return list(load_config(config_path)["sources"])
