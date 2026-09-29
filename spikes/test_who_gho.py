"""
Spike test: WHO GHO xMart (V_DENGUE_GLOBAL_VALIDATED_PUBLIC) lam nguon
ground-truth thu hai, bo sung cho OpenDengue.

Muc dich: xac nhan (1) API OData goi duoc khong can key, (2) response co du
truong can thiet, (3) loc duoc theo 11 nuoc Dong Nam A bang $filter server-
side, (4) khong bi phan trang am tham (@odata.nextLink). KHONG dung PySpark
o day - chi la buoc kiem tra nguon truoc khi build ingestion that.

API: https://xmart-api-public.who.int/ARBOV/V_DENGUE_GLOBAL_VALIDATED_PUBLIC
Day la dataset OData cong khai cua WHO Global Health Observatory.
"""

import json
import sys
from pathlib import Path

import requests

API_URL = "https://xmart-api-public.who.int/ARBOV/V_DENGUE_GLOBAL_VALIDATED_PUBLIC"
HEADERS = {"User-Agent": "OutbreakSignalDE-spike/0.1 (student project, ingestion test)"}

OUT_DIR = Path(__file__).resolve().parent.parent / "output" / "bronze_test"
SAMPLE_JSON_PATH = OUT_DIR / "who_gho_sea_sample.json"

# Ma ISO3 cua 11 nuoc Dong Nam A - dung de loc NGAY O REQUEST (OData $filter),
# khac voi OpenDengue phai tai toan cau roi moi loc duoc (file zip tinh,
# khong co tham so loc o URL).
SEA_ISO3 = [
    "VNM", "THA", "IDN", "PHL", "MYS",
    "MMR", "KHM", "LAO", "SGP", "BRN", "TLS",
]


def call_who_gho(iso3_codes: list[str], top: int = 10_000) -> dict:
    """Goi OData API, loc theo danh sach ma ISO3."""
    quoted = ",".join(f"'{code}'" for code in iso3_codes)
    params = {"$filter": f"ISO3 in ({quoted})", "$top": top}

    print(f"[1/3] Calling WHO GHO xMart, loc {len(iso3_codes)} ma ISO3, top={top}")
    resp = requests.get(API_URL, params=params, headers=HEADERS, timeout=30)
    print(f"      -> HTTP {resp.status_code}, {len(resp.content):,} bytes")
    resp.raise_for_status()
    return resp.json()


def check_pagination(envelope: dict, top: int, count: int) -> None:
    print("[2/3] Kiem tra phan trang")
    if "@odata.nextLink" in envelope:
        print(f"      -> CANH BAO: co @odata.nextLink, con trang tiep theo chua lay!")
    else:
        print("      -> khong co @odata.nextLink, da lay het trong 1 request")
    if count >= top:
        print(f"      -> CANH BAO: so dong ({count}) >= top ({top}), co the bi cat boi server")
    else:
        print(f"      -> so dong ({count}) < top ({top}), an toan khong bi cat")


def report(records: list[dict]) -> None:
    print("[3/3] Schema / sample / do phu")
    if not records:
        print("      -> EMPTY result, khong kiem tra duoc schema.")
        return

    print(f"      -> truong moi ban ghi: {list(records[0].keys())}")

    by_iso3: dict[str, list[dict]] = {}
    for r in records:
        by_iso3.setdefault(r["ISO3"], []).append(r)

    print(f"\n      -> Do phu {len(by_iso3)}/{len(SEA_ISO3)} nuoc SEA:")
    for code in SEA_ISO3:
        recs = by_iso3.get(code)
        if not recs:
            print(f"         {code}: KHONG CO DU LIEU")
            continue
        dated = sorted((r["START_DATE"] for r in recs if r.get("START_DATE")))
        span = f"{dated[0]} -> {dated[-1]}" if dated else "(khong co ngay)"
        print(f"         {code}: {len(recs):>5} dong   {span}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(SAMPLE_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"\n      -> saved sample ({len(records)} ban ghi) to {SAMPLE_JSON_PATH}")


def main() -> int:
    try:
        top = 10_000
        envelope = call_who_gho(SEA_ISO3, top=top)
        records = envelope.get("value", [])
        check_pagination(envelope, top, len(records))
        report(records)
        missing = {c for c in SEA_ISO3 if c not in {r["ISO3"] for r in records}}
        print(
            f"\nRESULT: WHO GHO DUNG DUOC cho MVP. "
            f"Thieu {sorted(missing)} trong 11 nuoc SEA (da xac nhan khong phai loi query - "
            f"nguon thuc su khong co du lieu cho hai nuoc nay)."
        )
        return 0
    except requests.HTTPError as e:
        print(f"\nRESULT: FAILED - HTTP error: {e}")
        return 1
    except Exception as e:
        print(f"\nRESULT: FAILED - unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
