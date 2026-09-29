"""Unit test cho phan logic thuan cua nguon WHO GHO - khong mang, khong Spark."""

from ingestion.who_gho import build_odata_filter


class TestBuildODataFilter:
    """Cau truc OData $filter phai dung cu phap va dat dung ten cot."""

    def test_mot_ma_don(self) -> None:
        assert build_odata_filter(["VNM"]) == "ISO3 in ('VNM')"

    def test_nhieu_ma_cach_nhau_bang_dau_phay_khong_khoang_trang(self) -> None:
        assert build_odata_filter(["VNM", "THA"]) == "ISO3 in ('VNM','THA')"

    def test_giu_dung_thu_tu_danh_sach_dau_vao(self) -> None:
        result = build_odata_filter(["SGP", "BRN", "TLS"])
        assert result == "ISO3 in ('SGP','BRN','TLS')"

    def test_moi_ma_duoc_boc_trong_dau_nhay_don(self) -> None:
        result = build_odata_filter(["VNM"])
        assert "'VNM'" in result
        assert '"VNM"' not in result

    def test_danh_sach_rong_van_tao_cu_phap_hop_le(self) -> None:
        # OData chap nhan "IN ()" ve mat cu phap, du server co the tra loi
        # khac nhau - ham nay chi chiu trach nhiem dung cu phap, khong chiu
        # trach nhiem danh sach dau vao co hop ly hay khong.
        assert build_odata_filter([]) == "ISO3 in ()"
