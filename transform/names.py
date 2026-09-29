"""So khop ten dia danh giua cac nguon - PySpark thuan (translate + levenshtein).

Dung chung cho Silver (noi ten tinh PH DOH / dan so VN, PH vao P-code) va Gold
(kiem tra RNE_iso_code cua OpenDengue voi ten ranh gioi).
"""

import unicodedata

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Tu chi LOAI don vi hanh chinh, bo di truoc khi so sanh ("BELAIT DISTRICT" va
# "Belait" la mot noi; "DKI JAKARTA" va "Jakarta Raya" cung vay). KHONG bo tu chi
# huong (UTARA, SUR, NORTE...) - chung phan biet cac tinh khac nhau.
_GENERIC_WORDS = (
    "PROVINCE", "CITY", "MUNICIPALITY", "DISTRICT", "PREFECTURE", "CAPITAL",
    "METROPOLIS", "REGION", "STATE", "SPECIAL", "OF", "AND",
    "DKI", "DAERAH", "ISTIMEWA", "KHUSUS", "RAYA", "WP",
)
# Nguong do giong (0..1). Chon 0.75 tren cap ten THAT (29/9/2026): bien the
# phien am dat 0.82-0.94 (SAMUT PRAKARN/Prakan, AYAYARWADDY/Ayeyarwady), con ma
# sai that nam duoi (CAMARINES SUR/Camarines Norte 0.71, KALIMANTAN UTARA/Timur 0.67).
MIN_SIMILARITY = 0.75
MIN_CONTAIN_LENGTH = 4


def _accent_table() -> tuple[str, str]:
    """Bang doi chu co dau -> chu khong dau cho F.translate (Latin + tieng Viet).

    Tao san bang tu Unicode o Python (hang so); phep bien doi tren du lieu do
    Spark lam (F.translate), khong dung Python UDF.

    Returns:
        Bo (chuoi ky tu nguon, chuoi ky tu dich) cung do dai.
    """
    source, target = [], []
    for code in list(range(0xC0, 0x250)) + list(range(0x1E00, 0x1F00)):
        char = chr(code)
        base = unicodedata.normalize("NFKD", char).encode("ascii", "ignore").decode("ascii")
        if len(base) == 1 and base.isalpha():
            source.append(char)
            target.append(base)
    # NFKD khong tach duoc D gach. Natural Earth con dung ca Ð (U+00D0, Eth) - vd "Ðong Tháp".
    source += ["Đ", "đ", "Ð", "ð"]
    target += ["D", "d", "D", "d"]
    return "".join(source), "".join(target)


_ACCENT_SOURCE, _ACCENT_TARGET = _accent_table()


def name_words(column: F.Column) -> F.Column:
    """Tach ten dia danh thanh mang tu da chuan hoa (khong dau, viet hoa, bo tu chung).

    Args:
        column: Cot ten.

    Returns:
        Cot array<string>.
    """
    ascii_name = F.translate(column, _ACCENT_SOURCE, _ACCENT_TARGET)
    cleaned = F.trim(F.regexp_replace(F.upper(ascii_name), r"[^A-Z0-9]+", " "))
    words = F.split(cleaned, " ")
    return F.filter(words, lambda w: (F.length(w) > 1) & ~w.isin(*_GENERIC_WORDS))


def normalized_name(column: F.Column) -> F.Column:
    """Ten da chuan hoa, ghep lien (vd "PROVINCE OF TARLAC" -> "TARLAC").

    Args:
        column: Cot ten.

    Returns:
        Cot chuoi (rong neu ten null).
    """
    return F.coalesce(F.array_join(name_words(column), ""), F.lit(""))


def name_similarity(a: F.Column, b: F.Column) -> F.Column:
    """Do giong nhau cua hai ten dia danh (0..1), chiu duoc khac phien am.

    Ten nay nam tron trong ten kia (du dai) -> 1.0 ("BANGKOK" / "Bangkok
    Metropolis"). Con lai: 1 - levenshtein / do dai ten dai hon.

    Args:
        a: Cot ten thu nhat.
        b: Cot ten thu hai.

    Returns:
        Cot double; 0.0 neu mot ben rong.
    """
    na, nb = normalized_name(a), normalized_name(b)
    shortest = F.least(F.length(na), F.length(nb))
    contained = (shortest >= MIN_CONTAIN_LENGTH) & (na.contains(nb) | nb.contains(na))
    ratio = 1 - F.levenshtein(na, nb) / F.greatest(F.length(na), F.length(nb))
    return (
        F.when(shortest == 0, F.lit(0.0))
        .when(contained, F.lit(1.0))
        .otherwise(ratio.cast("double"))
    )


def best_name_match(left: DataFrame, left_name: str, right: DataFrame, right_name: str,
                    same: list[str]) -> DataFrame:
    """Voi moi dong ben trai, chon dong ben phai co ten giong nhat (cung `same`).

    Tra ve ca diem giong de nguoi goi tu quyet dinh nguong; dong khong co ung
    vien nao (khac nuoc) van giu lai voi diem null.

    Args:
        left: DataFrame can noi (vd ten tinh trong nguon so ca).
        left_name: Cot ten ben trai.
        right: DataFrame dich (vd don vi hanh chinh).
        right_name: Cot ten ben phai.
        same: Cot phai bang nhau (vd ["iso3"]).

    Returns:
        left + moi cot cua right (tru `same`) + match_score.
    """
    l, r = left.alias("l"), right.alias("r")
    condition = F.lit(True)
    for column in same:
        condition = condition & (F.col(f"l.{column}") == F.col(f"r.{column}"))
    scored = l.join(r, condition, "left").withColumn(
        "match_score", F.round(name_similarity(F.col(f"l.{left_name}"), F.col(f"r.{right_name}")), 3))
    right_cols = [F.col(f"r.{c}").alias(c) for c in right.columns if c not in same]
    window = Window.partitionBy(*[F.col(f"l.{c}") for c in left.columns]).orderBy(
        F.col("match_score").desc_nulls_last())
    return (
        scored.withColumn("_rank", F.row_number().over(window))
        .where("_rank = 1")
        .select(*[F.col(f"l.{c}").alias(c) for c in left.columns], *right_cols, "match_score")
    )
