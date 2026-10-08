"""Test cho bang noi 64 -> 34 tinh va nguon ranh gioi va lap geoBoundaries.

Phan lon la test thuan: doc seed CSV bang csv chuan, khong can JVM, khong goi mang.
Cac test can Spark duoc danh dau `integration`.
"""

import csv
import json
from pathlib import Path

import pytest

from ingestion import geoboundaries, vn_province_crosswalk
from ingestion.common.config import source_config
from ingestion.common.paths import PROJECT_ROOT
from ingestion.common.validation import IngestionValidationError

SEED = PROJECT_ROOT / "configs" / "reference" / "vn_province_merge_2025.csv"

# Nghi quyet 202/2025/QH15: 63 tinh -> 34, trong do 11 don vi giu nguyen.
# Cong them Ha Tay (NQ 15/2008/QH12) vi OpenDengue con du lieu lich su.
EXPECTED_ROWS = 64
EXPECTED_NEW_UNITS = 34


@pytest.fixture(scope="module")
def rows() -> list[dict[str, str]]:
    """Noi dung seed CSV."""
    with open(SEED, encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


class TestSeedData:
    def test_du_64_dong_va_34_don_vi_moi(self, rows) -> None:
        assert len(rows) == EXPECTED_ROWS
        assert len({r["new_pcode"] for r in rows}) == EXPECTED_NEW_UNITS

    def test_khong_co_ten_tinh_cu_bi_lap(self, rows) -> None:
        # Mot ten cu xuat hien hai lan se nhan ban so ca cua tinh do khi join.
        names = [r["old_name"] for r in rows]
        assert len(set(names)) == len(names)

    def test_moi_dong_du_cot_bat_buoc_va_khong_rong(self, rows) -> None:
        for row in rows:
            assert set(vn_province_crosswalk.REQUIRED_COLUMNS) <= set(row)
            for column in ("old_name", "new_name", "new_pcode", "effective_from", "legal_basis"):
                assert row[column].strip(), f"{row['old_name']}: cot {column} rong"

    def test_pcode_moi_dung_dang_cua_cod_ab(self, rows) -> None:
        # COD-AB dung VN + 2 chu so (VN01..VN96); COD-PS dung VN + 3 chu so.
        # Nham hai he nay lai chinh la goc cua ca van de - khoa chat o day.
        for row in rows:
            assert row["new_pcode"].startswith("VN") and len(row["new_pcode"]) == 4, row

    def test_tinh_cu_tro_dung_mot_tinh_moi(self, rows) -> None:
        # Sap nhap 2025 chi GOP, khong tach tinh nao - nen anh xa phai la nhieu-mot.
        by_old = {}
        for row in rows:
            by_old.setdefault(row["old_name"], set()).add(row["new_pcode"])
        multi = {k: v for k, v in by_old.items() if len(v) > 1}
        assert not multi, f"tinh cu tro toi nhieu tinh moi: {multi}"

    def test_ha_tay_map_ve_ha_noi_theo_nghi_quyet_2008(self, rows) -> None:
        ha_tay = next(r for r in rows if r["old_name"] == "Ha Tay")
        assert ha_tay["new_name"] == "Ha Noi"
        assert ha_tay["effective_from"] == "2008-08-01"
        # Ha Tay bi xoa truoc khi COD-PS duoc lap nen khong co P-code dan so.
        assert ha_tay["old_pcode_ps"] == ""

    def test_hue_doi_ten_nen_ten_cu_khac_ten_moi(self, rows) -> None:
        hue = next(r for r in rows if r["new_pcode"] == "VN46")
        assert hue["old_name"] == "Thua Thien Hue"
        assert hue["new_name"] == "Hue"

    def test_vi_du_gop_dak_lak_phu_yen(self, rows) -> None:
        merged = {r["old_name"]: r["new_pcode"] for r in rows}
        assert merged["Dak Lak"] == merged["Phu Yen"] == "VN66"

    def test_moi_ten_opendengue_deu_co_alias(self, rows) -> None:
        # source_aliases la cau noi sang adm_1_name cua OpenDengue (VIET HOA, dau
        # cach khac). Thieu alias nghia la dong OpenDengue do se rot khi join.
        aliases = {a for r in rows for a in r["source_aliases"].split("|") if a}
        assert len(aliases) == EXPECTED_ROWS
        assert "THUA THIEN - HUE" in aliases
        assert "BA RIA-VUNG TAU" in aliases


class TestSeedPath:
    def test_bao_loi_ro_khi_thieu_seed(self, tmp_path: Path) -> None:
        with pytest.raises(IngestionValidationError, match="khong tim thay seed file"):
            vn_province_crosswalk.seed_path({"seed_file": "configs/reference/khong-co.csv"})

    def test_config_tro_dung_file_dang_co(self) -> None:
        cfg = source_config(vn_province_crosswalk.SOURCE)
        assert vn_province_crosswalk.seed_path(cfg) == SEED


class TestGeoBoundaries:
    def test_url_dien_dung_tham_so(self) -> None:
        cfg = source_config(geoboundaries.SOURCE)
        url = geoboundaries.metadata_url(cfg, "brn", "adm1")
        assert url == "https://www.geoboundaries.org/api/current/gbOpen/BRN/ADM1/"

    def test_config_chi_va_lap_nuoc_hdx_khong_co(self) -> None:
        cfg = source_config(geoboundaries.SOURCE)
        hdx_countries = set(source_config("hdx_cod_ab")["countries"])
        assert set(cfg["countries"]).isdisjoint(hdx_countries)
        assert "BRN" in cfg["countries"]

    def test_version_lay_tu_boundary_id(self) -> None:
        assert geoboundaries.version_of({"boundaryID": "BRN-ADM1-89281809"}) == "BRN-ADM1-89281809"

    def test_thieu_boundary_id_thi_bao_loi(self) -> None:
        with pytest.raises(IngestionValidationError, match="boundaryID"):
            geoboundaries.version_of({"boundaryName": "Brunei Darussalam"})

    def test_feature_collection_rong_thi_bao_loi(self, tmp_path: Path) -> None:
        # Nguy hiem hon loi mang: ghi mot phan vung rong de len Bronze.
        with pytest.raises(IngestionValidationError, match="rong"):
            geoboundaries.features_to_jsonl({"features": []}, "BRN", "ADM1", {},
                                            tmp_path / "x.jsonl")

    def test_giu_nguyen_properties_goc_trong_raw_payload(self, tmp_path: Path) -> None:
        collection = {"features": [{
            "properties": {"shapeName": "Belait", "shapeISO": "BN-BE",
                           "shapeID": "89281809B69825435871977", "shapeGroup": "BRN",
                           "shapeType": "ADM1"},
            "geometry": {"type": "Polygon", "coordinates": [[[114.0, 4.0]]]},
        }]}
        path = tmp_path / "brn.jsonl"
        assert geoboundaries.features_to_jsonl(
            collection, "BRN", "ADM1",
            {"boundaryCanonical": "Districts", "boundaryLicense": "Public Domain"}, path) == 1

        record = json.loads(path.read_text(encoding="utf-8").strip())
        assert record["shape_name"] == "Belait"
        assert record["boundary_license"] == "Public Domain"
        # Bronze giu nguyen trang nguon: properties goc con nguyen ven.
        assert json.loads(record["raw_payload"]) == collection["features"][0]["properties"]
        assert json.loads(record["geometry"])["type"] == "Polygon"
        assert all(isinstance(v, str) for v in record.values())


@pytest.mark.integration
class TestCrosswalkChecksWithSpark:
    def _frame(self, spark, rows: list[dict[str, str]]):
        return spark.createDataFrame(rows)

    def test_seed_that_qua_duoc_check_shape(self, spark, rows) -> None:
        vn_province_crosswalk.check_shape(self._frame(spark, rows))

    def test_thieu_cot_thi_bao_loi(self, spark, rows) -> None:
        trimmed = [{k: v for k, v in r.items() if k != "legal_basis"} for r in rows]
        with pytest.raises(IngestionValidationError, match="thieu cot"):
            vn_province_crosswalk.check_shape(self._frame(spark, trimmed))

    def test_ten_tinh_cu_bi_lap_thi_bao_loi(self, spark, rows) -> None:
        with pytest.raises(IngestionValidationError, match="bi lap"):
            vn_province_crosswalk.check_shape(self._frame(spark, rows + [dict(rows[0])]))

    def test_thieu_mot_don_vi_moi_thi_bao_loi(self, spark, rows) -> None:
        # Bo het cac dong tro ve VN66 -> con 33 don vi moi.
        kept = [r for r in rows if r["new_pcode"] != "VN66"]
        with pytest.raises(IngestionValidationError, match="phai dung 34"):
            vn_province_crosswalk.check_shape(self._frame(spark, kept))
