"""Silver: don vi hanh chinh cap tinh (co thoi gian hieu luc) + bang noi ma.

    admin_units      moi don vi cap tinh mot dong: unit_id, ten, ten ban xu, toa do
                     tam, valid_from / valid_to (SCD type 2)
    admin_crosswalk  noi ma ISO 3166-2 (Natural Earth, OpenDengue RNE_iso_code) voi
                     P-code (COD), va 63 tinh cu -> 34 tinh moi cua Viet Nam

Dinh nghia "cap tinh" theo tung nuoc: PHL la cap 2 (cap 1 la vung), con lai cap 1.

Viet Nam sap nhap 63 -> 34 tinh tu 1/7/2025. COD-AB hien chi con 34 tinh moi (P-code),
nen 63 tinh cu lay tu Natural Earth (ma ISO 3166-2, ten tieng Viet `name_vi`):
    tinh cu: valid_to   = 30/6/2025, unit_system = "iso_3166_2"
    tinh moi: valid_from = 1/7/2025, unit_system = "pcode"
Moi ban ghi (so ca, bai bao) duoc gan vao don vi CO HIEU LUC vao ngay cua no.
Cac nuoc khac: COD-AB la phien ban hien hanh, khong co moc doi don vi -> valid_from
va valid_to de trong (hieu luc mo). Indonesia: COD-AB 2020 van la 34 tinh (chua co
4 tinh Papua tach 2022) - so lieu cac tinh moi do se gop ve tinh cu chua no.

Bang noi tao bang phep diem-trong-polygon (transform.geo, PySpark thuan):
    - tam don vi COD nam trong polygon Natural Earth nao -> (P-code, ma ISO)
    - VN: tam tinh cu (Natural Earth) nam trong polygon tinh moi (COD) -> cu -> moi
"""

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transform.geo import points_in_polygons

VN_REFORM_DATE = date(2025, 7, 1)
VN_OLD_VALID_TO = date(2025, 6, 30)
PROVINCE_LEVEL = {"PHL": 2}  # con lai: cap 1

# Natural Earth 10m ghi nham ten VUNG cho 3 tinh (ma ISO va toa do tam van dung; truong
# gn_name cua chinh file do ghi dung: "Tinh Dong Nai", "Tinh Bac Kan", "Tinh Hung Yen").
# Khong sua thi OpenDengue khong noi duoc 3 tinh nay, va tin nhac "Dong Nam Bo" (ca vung)
# bi gazetteer gan nham vao Dong Nai. Kiem tra 29/9/2026.
NE_VN_NAME_FIXES = {
    "VN-39": ("Dong Nai", "Đồng Nai"),
    "VN-53": ("Bac Kan", "Bắc Kạn"),
    "VN-66": ("Hung Yen", "Hưng Yên"),
}

UNIT_COLUMNS = (
    "iso3", "unit_id", "unit_system", "admin_level", "unit_name", "local_name",
    "local_lang", "region_name", "x", "y", "valid_from", "valid_to", "source_version",
)


def _province_level() -> F.Column:
    """Cap hanh chinh duoc coi la "tinh" cua moi nuoc (theo cot iso3)."""
    level = F.lit(1)
    for iso3, value in PROVINCE_LEVEL.items():
        level = F.when(F.col("iso3") == iso3, F.lit(value)).otherwise(level)
    return level


def _by_level(column_template: str) -> F.Column:
    """Chon cot adm1_* hoac adm2_* theo cap tinh cua dong.

    Args:
        column_template: Mau ten cot co {n}, vd "adm{n}_pcode".

    Returns:
        Cot gia tri tuong ung.
    """
    return F.when(F.col("_level") == 2, F.col(column_template.format(n=2))).otherwise(
        F.col(column_template.format(n=1)))


def cod_units(bronze_ab: DataFrame) -> DataFrame:
    """Don vi cap tinh tu COD-AB (P-code), kem toa do tam.

    Toa do: center_lon/center_lat cua sheet admin; trong (vd VNM) thi lay tu
    sheet adminpoints (x_coord/y_coord).

    Args:
        bronze_ab: Bang Bronze hdx_cod_ab.

    Returns:
        DataFrame theo UNIT_COLUMNS.
    """
    frame = bronze_ab.withColumn("_level", _province_level())
    admin = frame.where(F.col("_sheet") == F.concat(F.lit("admin"), F.col("_level")))
    points = frame.where(
        (F.col("_sheet") == "adminpoints") & (F.col("admin_level") == F.col("_level").cast("string"))
    ).select("iso3", _by_level("adm{n}_pcode").alias("unit_id"),
             F.col("x_coord").cast("double").alias("px"),
             F.col("y_coord").cast("double").alias("py"))
    units = admin.select(
        "iso3",
        _by_level("adm{n}_pcode").alias("unit_id"),
        F.lit("pcode").alias("unit_system"),
        F.col("_level").alias("admin_level"),
        _by_level("adm{n}_name").alias("unit_name"),
        _by_level("adm{n}_name1").alias("local_name"),
        F.col("lang1").alias("local_lang"),
        F.when(F.col("_level") == 2, F.col("adm1_name")).alias("region_name"),
        F.col("center_lon").cast("double").alias("cx"),
        F.col("center_lat").cast("double").alias("cy"),
        F.when(F.col("iso3") == "VNM", F.lit(VN_REFORM_DATE)).cast("date").alias("valid_from"),
        F.lit(None).cast("date").alias("valid_to"),
        F.col("_version").alias("source_version"),
    )
    return units.join(points, ["iso3", "unit_id"], "left").select(
        *[c for c in UNIT_COLUMNS if c not in ("x", "y")],
        F.coalesce("cx", "px").alias("x"), F.coalesce("cy", "py").alias("y"),
    ).select(*UNIT_COLUMNS)


def vn_old_units(ne_features: DataFrame) -> DataFrame:
    """63 tinh cu cua Viet Nam (truoc 1/7/2025) tu Natural Earth (da sua NE_VN_NAME_FIXES).

    Args:
        ne_features: Feature Natural Earth (iso3, iso_3166_2, name, name_vi,
            latitude, longitude, geometry).

    Returns:
        DataFrame theo UNIT_COLUMNS.
    """
    name, local = F.col("name"), F.col("name_vi")
    for code, (fixed_name, fixed_local) in NE_VN_NAME_FIXES.items():
        is_code = F.col("iso_3166_2") == code
        name = F.when(is_code, F.lit(fixed_name)).otherwise(name)
        local = F.when(is_code, F.lit(fixed_local)).otherwise(local)
    return ne_features.where(F.col("iso3") == "VNM").select(
        "iso3",
        F.col("iso_3166_2").alias("unit_id"),
        F.lit("iso_3166_2").alias("unit_system"),
        F.lit(1).alias("admin_level"),
        name.alias("unit_name"),
        local.alias("local_name"),
        F.lit("vi").alias("local_lang"),
        F.lit(None).cast("string").alias("region_name"),
        F.col("longitude").alias("x"),
        F.col("latitude").alias("y"),
        F.lit(None).cast("date").alias("valid_from"),
        F.lit(VN_OLD_VALID_TO).cast("date").alias("valid_to"),
        F.lit("natural_earth_10m").alias("source_version"),
    )


def build_admin_units(bronze_ab: DataFrame, ne_features: DataFrame) -> DataFrame:
    """Bang admin_units: don vi COD hien hanh + 63 tinh cu cua Viet Nam.

    Args:
        bronze_ab: Bronze hdx_cod_ab.
        ne_features: Feature Natural Earth.

    Returns:
        DataFrame admin_units.
    """
    return cod_units(bronze_ab).unionByName(vn_old_units(ne_features))


def build_crosswalk(admin_units: DataFrame, ne_features: DataFrame,
                    cod_geometry: DataFrame) -> DataFrame:
    """Bang noi ma: moi dong (iso3, iso_3166_2, pcode, method).

    - method "cod_centroid_in_ne": tam don vi COD hien hanh nam trong polygon NE
      (cac nuoc tru VNM) -> cho phep dich RNE_iso_code cua OpenDengue sang P-code.
    - method "ne_centroid_in_cod": tam tinh cu (NE) nam trong polygon tinh moi (COD)
      -> bang noi 63 tinh cu -> 34 tinh moi cua Viet Nam.

    Args:
        admin_units: Silver admin_units.
        ne_features: Feature Natural Earth (co geometry).
        cod_geometry: Bronze hdx_cod_ab_geometry (polygon COD, hien chi VNM).

    Returns:
        DataFrame admin_crosswalk.
    """
    current = admin_units.where(
        (F.col("unit_system") == "pcode") & (F.col("iso3") != "VNM") & F.col("x").isNotNull()
    ).select("iso3", F.col("unit_id").alias("pcode"), "x", "y")
    cod_in_ne = points_in_polygons(
        current, ["pcode"], ne_features.select("iso3", "iso_3166_2", "geometry"),
        ["iso_3166_2"], same=["iso3"],
    ).withColumn("method", F.lit("cod_centroid_in_ne"))

    old_vn = admin_units.where(F.col("unit_system") == "iso_3166_2").select(
        "iso3", F.col("unit_id").alias("iso_3166_2"), "x", "y")
    new_vn = cod_geometry.where(F.col("iso3") == "VNM").select(
        "iso3", F.col("adm1_pcode").alias("pcode"), "geometry")
    ne_in_cod = points_in_polygons(
        old_vn, ["iso_3166_2"], new_vn, ["pcode"], same=["iso3"],
    ).withColumn("method", F.lit("ne_centroid_in_cod"))

    iso3_of = admin_units.select(
        F.col("unit_id").alias("pcode"), F.col("iso3")).distinct()
    return (
        cod_in_ne.join(iso3_of, "pcode")
        .select("iso3", "iso_3166_2", "pcode", "method")
        .unionByName(ne_in_cod.withColumn("iso3", F.lit("VNM"))
                     .select("iso3", "iso_3166_2", "pcode", "method"))
    )
