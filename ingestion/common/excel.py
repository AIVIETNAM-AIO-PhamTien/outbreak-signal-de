"""Chuyen sheet Excel sang CSV de Spark doc (Spark khong doc duoc .xlsx).

Chi doi DINH DANG chua, khong doi noi dung: moi o doc thanh chuoi (dtype=str),
o trong giu la chuoi rong. Ngoai le duy nhat: ten cot. Delta khong cho ten cot
co dau cach / ngoac / dau phay ("Dengue fever (DF)"), nen ten cot duoc doi bang
`sanitize_column` (co dinh, tat dinh) va bang doi chieu ten goc duoc ghi canh
file CSV trong landing - khong mat thong tin.
"""

import json
import re
from pathlib import Path

import pandas as pd


def sanitize_column(name: str) -> str:
    """Ten cot hop le cho Delta: chu thuong, chi chu/so/gach duoi.

    Args:
        name: Ten cot goc, vd "Dengue fever (DF)".

    Returns:
        Ten da doi, vd "dengue_fever_df".
    """
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", str(name)).strip("_").lower()
    return cleaned or "col"


def sheet_to_csv(xlsx_path: Path, sheet: str, csv_path: Path) -> dict[str, str]:
    """Ghi mot sheet ra CSV (moi o la chuoi) va file doi chieu ten cot.

    Args:
        xlsx_path: File Excel goc trong landing.
        sheet: Ten sheet.
        csv_path: File CSV dich.

    Returns:
        Dict ten cot moi -> ten goc.

    Raises:
        ValueError: Neu hai cot goc doi ra trung ten.
    """
    frame = pd.read_excel(xlsx_path, sheet_name=sheet, dtype=str, keep_default_na=False)
    mapping = {sanitize_column(c): str(c) for c in frame.columns}
    if len(mapping) != len(frame.columns):
        raise ValueError(f"{xlsx_path.name}/{sheet}: hai cot doi ra trung ten - {list(frame.columns)}")
    frame.columns = list(mapping)
    frame.to_csv(csv_path, index=False, encoding="utf-8")
    csv_path.with_suffix(".columns.json").write_text(
        json.dumps(mapping, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return mapping
