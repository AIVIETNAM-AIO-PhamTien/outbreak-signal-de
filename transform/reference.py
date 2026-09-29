"""Bang tham chieu 11 nuoc Dong Nam A - nguon DUY NHAT cho moi mapping quoc gia.

Ba nguon Bronze goi ten nuoc theo ba kieu khac nhau:
    OpenDengue: ten UN viet hoa ("VIET NAM", "LAO PEOPLE'S DEMOCRATIC REPUBLIC")
    WHO GHO:    ma ISO3 + ten WHO ("Viet Nam")
    News RSS:   khong co truong quoc gia - chi co chu trong tieu de / tom tat

Silver quy het ve ISO3. Ten OpenDengue va ma ISO3 o day phai khop voi
filter_countries / filter_iso3 trong configs/sources.yaml (co test kiem tra).

Gan nuoc cho tin tuc bang tu khoa (ten nuoc, tinh tu, thu do / thanh pho lon)
- rule-based co chu dich, khong dung NER/ML. Do phu thap la gioi han da biet.
"""

import re
from dataclasses import dataclass

UNKNOWN_ISO3 = "UNK"


@dataclass(frozen=True)
class Country:
    """Mot nuoc trong pham vi du an.

    Attributes:
        iso3: Ma ISO 3166-1 alpha-3.
        name: Ten hien thi (tieng Anh).
        opendengue_name: Gia tri cot adm_0_name cua OpenDengue.
        who_name: Gia tri cot COUNTRY cua WHO GHO, None neu WHO khong co du lieu.
        keywords: Tu khoa nhan dien nuoc trong tin tuc (khong phan biet hoa thuong).
    """

    iso3: str
    name: str
    opendengue_name: str
    who_name: str | None
    keywords: tuple[str, ...]


COUNTRIES: tuple[Country, ...] = (
    Country("BRN", "Brunei", "BRUNEI DARUSSALAM", None,
            ("Brunei", "Bandar Seri Begawan")),
    Country("IDN", "Indonesia", "INDONESIA", "Indonesia",
            ("Indonesia", "Indonesian", "Jakarta", "Bali", "Java", "Sumatra")),
    Country("KHM", "Cambodia", "CAMBODIA", "Cambodia",
            ("Cambodia", "Cambodian", "Phnom Penh")),
    Country("LAO", "Laos", "LAO PEOPLE'S DEMOCRATIC REPUBLIC",
            "Lao People's Democratic Republic", ("Laos", "Lao", "Vientiane")),
    Country("MMR", "Myanmar", "MYANMAR", "Myanmar",
            ("Myanmar", "Burma", "Burmese", "Yangon")),
    Country("MYS", "Malaysia", "MALAYSIA", "Malaysia",
            ("Malaysia", "Malaysian", "Kuala Lumpur", "Selangor", "Sabah", "Sarawak")),
    Country("PHL", "Philippines", "PHILIPPINES", None,
            ("Philippines", "Philippine", "Filipino", "Manila", "Mindanao", "Luzon")),
    Country("SGP", "Singapore", "SINGAPORE", "Singapore",
            ("Singapore", "Singaporean")),
    Country("THA", "Thailand", "THAILAND", "Thailand",
            ("Thailand", "Thai", "Bangkok")),
    Country("TLS", "Timor-Leste", "TIMOR-LESTE", "Timor-Leste",
            ("Timor-Leste", "East Timor", "Timorese", "Dili")),
    Country("VNM", "Viet Nam", "VIET NAM", "Viet Nam",
            ("Vietnam", "Viet Nam", "Vietnamese", "Hanoi", "Ha Noi", "Ho Chi Minh", "Saigon")),
)

BY_ISO3: dict[str, Country] = {country.iso3: country for country in COUNTRIES}
BY_OPENDENGUE_NAME: dict[str, Country] = {c.opendengue_name: c for c in COUNTRIES}


def country_pattern(country: Country) -> str:
    """Bieu thuc chinh quy nhan dien mot nuoc trong van ban.

    Dung chung cho Python (re) va Spark (rlike, cu phap Java): chi dung nhom,
    `|` va bien tu `\\b`, nen hai engine hieu giong nhau.

    Args:
        country: Nuoc can nhan dien.

    Returns:
        Chuoi regex khong phan biet hoa thuong, vi du "(?i)\\b(Thailand|Thai)\\b".
    """
    alternatives = "|".join(re.escape(keyword) for keyword in country.keywords)
    return rf"(?i)\b({alternatives})\b"


def match_countries(text: str | None) -> list[str]:
    """Tim cac nuoc SEA duoc nhac toi trong mot doan van ban.

    Args:
        text: Tieu de / tom tat bai bao. None duoc coi nhu chuoi rong.

    Returns:
        Danh sach ma ISO3 (sap xep), rong neu khong khop nuoc nao.
    """
    if not text:
        return []
    return sorted(
        country.iso3 for country in COUNTRIES if re.search(country_pattern(country), text)
    )
