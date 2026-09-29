"""Truy cap Humanitarian Data Exchange (HDX, UN OCHA) qua CKAN API.

HDX khong co hash cho file, nhung moi resource co `last_modified`. Dung gia tri
nay lam phien ban: chua doi thi bo qua, khong tai lai.
"""

import re
from typing import Any

from ingestion.common import http

PACKAGE_SHOW = "https://data.humdata.org/api/3/action/package_show"


def package_show(package: str, cfg: dict) -> dict[str, Any]:
    """Mo ta day du mot dataset HDX.

    Args:
        package: Ten dataset, vd "cod-ab-vnm".
        cfg: Config nguon (retries, timeout_seconds).

    Returns:
        Truong `result` cua CKAN.

    Raises:
        RuntimeError: Neu CKAN tra success = false.
    """
    body = http.get(PACKAGE_SHOW, cfg["retries"], cfg["timeout_seconds"],
                    params={"id": package}).json()
    if not body.get("success"):
        raise RuntimeError(f"HDX package_show {package!r} that bai: {body.get('error')}")
    return body["result"]


def find_resources(package: dict[str, Any], pattern: str) -> list[dict[str, Any]]:
    """Moi resource co ten khop regex (khong phan biet hoa thuong).

    Args:
        package: Ket qua package_show.
        pattern: Regex ap len ten resource.

    Returns:
        Danh sach resource, theo thu tu HDX tra ve.

    Raises:
        LookupError: Neu khong co resource nao khop.
    """
    regex = re.compile(pattern, re.IGNORECASE)
    matched = [r for r in package["resources"] if regex.search(r.get("name", ""))]
    if not matched:
        raise LookupError(
            f"{package.get('name')}: khong co resource khop {pattern!r} trong "
            f"{[r.get('name') for r in package['resources']]}"
        )
    return matched


def find_resource(package: dict[str, Any], pattern: str) -> dict[str, Any]:
    """Resource dau tien co ten khop regex.

    Args:
        package: Ket qua package_show.
        pattern: Regex ap len ten resource.

    Returns:
        Resource (name, url, last_modified, format...).
    """
    return find_resources(package, pattern)[0]


def version_of(resource: dict[str, Any]) -> str:
    """Phien ban cua resource = last_modified, dang an toan lam ten thu muc.

    Args:
        resource: Resource HDX.

    Returns:
        Vd "20260126T101500".
    """
    stamp = resource.get("last_modified") or resource.get("created") or "unknown"
    return re.sub(r"[^0-9T]", "", stamp.split(".")[0])
