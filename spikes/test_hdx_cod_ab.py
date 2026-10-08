"""
Spike test: HDX COD-AB (Common Operational Dataset - Administrative
Boundaries) lam nguon REFERENCE dia ly, de chuan hoa cot adm_1_name cua
OpenDengue / WHO GHO.

Khac han 4 nguon hien tai: day KHONG phai nguon tin hieu dich te (khong co
so ca benh), ma la dimension/lookup - dung de tra loi "ten tinh nay thuc su
la don vi hanh chinh nao, p-code gi, ranh gioi o dau".

Muc dich spike: tra loi 3 cau truoc khi quyet dinh co dua vao Bronze khong:
  (1) HDX co du COD-AB cho ca 11 nuoc Dong Nam A khong?
  (2) Cap Admin1 cua HDX co khop voi adm_1_name cua OpenDengue khong - tuc
      la co join duoc theo TEN khong, hay bat buoc phai xay crosswalk?
  (3) Du lieu co con moi khong (sau dot sap nhap tinh cua VN 01/07/2025)?

API: https://data.humdata.org/api/3/action/package_show?id=cod-ab-{iso3}
Moi nuoc la MOT dataset rieng, khong co endpoint gop. License CC-BY-IGO.
KHONG dung PySpark o day - chi la buoc kiem tra nguon truoc khi build that.
"""

import json
import re
import sys
from pathlib import Path

import pandas as pd
import requests

API_URL = "https://data.humdata.org/api/3/action/package_show"
HEADERS = {"User-Agent": "OutbreakSignalDE-spike/0.1 (student project, ingestion test)"}

ROOT = Path(__file__).resolve().parent.parent
OPENDENGUE_CSV = ROOT / "data" / "landing" / "opendengue" / "2026-09-29" / "Spatial_extract_V1_3.csv"
OUT_DIR = ROOT / "output" / "bronze_test"
REPORT_PATH = OUT_DIR / "hdx_cod_ab_adm1_match.json"

# Map ma ISO3 (HDX dung) -> ten nuoc theo chuan UN (OpenDengue dung).
# Luu y "VIET NAM" CO dau cach - cung bug da gap o spike OpenDengue.
SEA = {
    "vnm": "VIET NAM",
    "tha": "THAILAND",
    "idn": "INDONESIA",
    "phl": "PHILIPPINES",
    "mys": "MALAYSIA",
    "mmr": "MYANMAR",
    "khm": "CAMBODIA",
    "lao": "LAO PEOPLE'S DEMOCRATIC REPUBLIC",
    "sgp": "SINGAPORE",
    "brn": "BRUNEI DARUSSALAM",
    "tls": "TIMOR-LESTE",
}


def normalise(name: str) -> str:
    """Chuan hoa ten tinh de so khop 'de dai nhat co the'.

    Co tinh lam RONG RAI (bo hau to hanh chinh, bo het ky tu khong phai chu/so)
    de ty le khop thu duoc la can TREN lac quan nhat. Neu ngay ca cach nay ma
    ty le van thap thi ket luan "khong join duoc theo ten" la chac chan.
    """
    s = str(name).upper().strip()
    s = re.sub(r"\b(CITY|PROVINCE|PROVINCIA|KOTA|DKI|DI)\b", "", s)
    return re.sub(r"[^A-Z0-9]", "", s)


def fetch_metadata(iso3: str) -> dict | None:
    """Lay metadata dataset cod-ab-{iso3}. Tra None neu nuoc do khong co COD-AB."""
    resp = requests.get(API_URL, params={"id": f"cod-ab-{iso3}"}, headers=HEADERS, timeout=30)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()["result"]


def fetch_admin1_names(meta: dict, iso3: str) -> set[str]:
    """Tai resource XLSX va doc sheet {iso3}_admin1.

    Chon XLSX chu khong phai SHP/GeoJSON: ban XLSX chi vai chuc KB -> vai MB,
    trong khi bo SHP day du cua PHL/IDN len toi 1-2 GB. Spike chi can TEN va
    P-CODE de kiem tra kha nang join, chua can hinh hoc ranh gioi.
    """
    xlsx = [r for r in meta["resources"] if r["format"] == "XLSX"]
    if not xlsx:
        return set()
    content = requests.get(xlsx[0]["url"], headers=HEADERS, timeout=180).content
    tmp = OUT_DIR / f"hdx_{iso3}.xlsx"
    tmp.write_bytes(content)
    df = pd.read_excel(tmp, sheet_name=f"{iso3}_admin1")
    return set(df["adm1_name"].dropna())


def load_opendengue_adm1() -> dict[str, set[str]]:
    """Doc adm_1_name cua 11 nuoc SEA tu file landing OpenDengue (doc theo chunk)."""
    print(f"[1/4] Doc adm_1_name tu OpenDengue landing: {OPENDENGUE_CSV.name}")
    if not OPENDENGUE_CSV.exists():
        print(f"      -> KHONG TIM THAY {OPENDENGUE_CSV}, bo qua phan doi chieu.")
        return {}
    found: dict[str, set[str]] = {c: set() for c in SEA.values()}
    for chunk in pd.read_csv(
        OPENDENGUE_CSV, chunksize=500_000, low_memory=False,
        usecols=["adm_0_name", "adm_1_name"],
    ):
        chunk = chunk[chunk["adm_0_name"].isin(found)]
        for country, grp in chunk.groupby("adm_0_name"):
            found[country].update(grp["adm_1_name"].dropna().unique())
    print(f"      -> {sum(len(v) for v in found.values())} ten tinh thuoc {len(found)} nuoc")
    return found


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    od = load_opendengue_adm1()

    print("\n[2/4] Kiem tra do phu COD-AB tren 11 nuoc SEA")
    meta_by_iso: dict[str, dict] = {}
    missing_country: list[str] = []
    for iso3 in SEA:
        meta = fetch_metadata(iso3)
        if meta is None:
            missing_country.append(iso3.upper())
            print(f"      {iso3.upper()}: KHONG CO COD-AB tren HDX (HTTP 404)")
            continue
        meta_by_iso[iso3] = meta
        asof = (meta.get("dataset_date") or "")[1:11]
        print(f"      {iso3.upper()}: co, valid_on={asof}, cap nhat moi {meta.get('data_update_frequency')} ngay")

    print("\n[3/4] Doi chieu Admin1 cua HDX voi adm_1_name cua OpenDengue")
    print(f"      {'ISO':<5}{'HDX':>5}{'OD':>5}{'khop':>6}{'ty le':>8}   ghi chu")
    report: dict[str, dict] = {}
    for iso3, meta in meta_by_iso.items():
        hdx_names = fetch_admin1_names(meta, iso3)
        od_names = od.get(SEA[iso3], set())
        hdx_norm = {normalise(n) for n in hdx_names}
        matched = {n for n in od_names if normalise(n) in hdx_norm}
        unmatched = sorted(od_names - matched)
        rate = len(matched) / len(od_names) * 100 if od_names else 0.0
        print(f"      {iso3.upper():<5}{len(hdx_names):>5}{len(od_names):>5}{len(matched):>6}{rate:>7.0f}%   "
              f"{len(unmatched)} ten OD khong map duoc")
        report[iso3] = {
            "hdx_admin1_count": len(hdx_names),
            "opendengue_adm1_count": len(od_names),
            "matched": len(matched),
            "match_rate_pct": round(rate, 1),
            "unmatched_opendengue_names": unmatched,
        }

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\n      -> saved report to {REPORT_PATH}")

    print("\n[4/4] Ket luan")
    rates = [r["match_rate_pct"] for r in report.values()]
    avg = sum(rates) / len(rates) if rates else 0.0
    print(f"      Do phu: {len(meta_by_iso)}/11 nuoc SEA (thieu {missing_country})")
    print(f"      Ty le khop ten trung binh: {avg:.0f}% (da chuan hoa de dai nhat co the)")
    print(
        "\nRESULT: HDX COD-AB DUNG DUOC lam reference, NHUNG KHONG join duoc theo ten.\n"
        "        Bat buoc phai xay crosswalk thu cong (ten OpenDengue -> p-code HDX)\n"
        "        truoc khi dung o Silver. Xem chi tiet tung nuoc trong file report."
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except requests.HTTPError as e:
        print(f"\nRESULT: FAILED - HTTP error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nRESULT: FAILED - unexpected error: {e}")
        sys.exit(1)
