"""Test lop cau hinh - nguon phai khai bao duoc tu YAML, khong hardcode."""

from pathlib import Path

import pytest

from ingestion.common.config import (
    ConfigError,
    all_sources,
    enabled_sources,
    load_config,
    source_config,
)


class TestLoadConfig:
    def test_doc_duoc_file_hop_le(self, sample_config: Path) -> None:
        config = load_config(sample_config)
        assert set(config["sources"]) == {"alpha", "beta"}

    def test_bao_loi_ro_rang_khi_thieu_file(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="khong tim thay"):
            load_config(tmp_path / "khong-ton-tai.yaml")

    def test_bao_loi_khi_yaml_sai_cu_phap(self, tmp_path: Path) -> None:
        broken = tmp_path / "broken.yaml"
        broken.write_text("sources:\n  alpha:\n   - [unclosed", encoding="utf-8")
        with pytest.raises(ConfigError, match="YAML"):
            load_config(broken)

    def test_bao_loi_khi_thieu_khoa_sources(self, tmp_path: Path) -> None:
        path = tmp_path / "no-sources.yaml"
        path.write_text("defaults:\n  mode: batch\n", encoding="utf-8")
        with pytest.raises(ConfigError, match="sources"):
            load_config(path)


class TestSourceConfig:
    def test_tron_defaults_vao_tung_nguon(self, sample_config: Path) -> None:
        cfg = source_config("alpha", sample_config)
        assert cfg["mode"] == "batch"
        assert cfg["timeout_seconds"] == 60

    def test_gia_tri_rieng_cua_nguon_de_len_defaults(self, sample_config: Path) -> None:
        assert source_config("beta", sample_config)["timeout_seconds"] == 5

    def test_them_ten_nguon_vao_config(self, sample_config: Path) -> None:
        assert source_config("alpha", sample_config)["name"] == "alpha"

    def test_bao_loi_kem_danh_sach_nguon_hop_le(self, sample_config: Path) -> None:
        with pytest.raises(ConfigError, match="alpha"):
            source_config("khong-co-nguon-nay", sample_config)


class TestEnabledSources:
    def test_chi_liet_ke_nguon_dang_bat(self, sample_config: Path) -> None:
        assert enabled_sources(sample_config) == ["alpha"]

    def test_all_sources_liet_ke_ca_nguon_dang_tat(self, sample_config: Path) -> None:
        assert all_sources(sample_config) == ["alpha", "beta"]


class TestConfigThat:
    """Config that trong repo phai dung duoc, khong chi file gia trong test."""

    def test_ba_nguon_mvp_deu_bat_va_du_truong_bat_buoc(self) -> None:
        for name in ("opendengue", "news_rss", "sg_nea"):
            cfg = source_config(name)
            assert cfg["enabled"] is True
            assert cfg["url"]
            assert cfg["source_type"]
            assert cfg["source_format"]
            assert cfg["timeout_seconds"] > 0

    def test_nguon_backlog_phai_tat_va_co_ghi_chu_ly_do(self) -> None:
        # who_gho va gdelt da khao sat nhung chua trien khai - phai tat, va
        # phai noi ro ly do, khong de treo im lang.
        for name in ("who_gho", "gdelt"):
            cfg = source_config(name)
            assert cfg["enabled"] is False
            assert cfg.get("note")
