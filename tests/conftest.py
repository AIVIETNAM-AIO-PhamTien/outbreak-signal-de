"""Fixture dung chung cho toan bo test suite."""

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Cho phep chay `pytest` tu goc repo ma khong can cai package hay set PYTHONPATH.
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(scope="session")
def spark():
    """SparkSession dung chung cho cac test can Spark.

    Scope "session" vi khoi dong JVM mat vai giay - tao lai cho tung test se
    lam suite cham gap nhieu lan.

    Yields:
        SparkSession da bat Delta.
    """
    from ingestion.common.spark_session import build_spark_session

    session = build_spark_session("pytest")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sample_config(tmp_path: Path) -> Path:
    """File sources.yaml toi gian de test config khong phu thuoc file that.

    Args:
        tmp_path: Thu muc tam cua pytest.

    Returns:
        Duong dan file YAML vua tao.
    """
    path = tmp_path / "sources.yaml"
    path.write_text(
        """
defaults:
  mode: batch
  timeout_seconds: 60

sources:
  alpha:
    enabled: true
    source_type: rest_api
    source_format: json
    url: https://example.invalid/alpha
  beta:
    enabled: false
    source_type: rss
    source_format: xml
    url: https://example.invalid/beta
    timeout_seconds: 5
""",
        encoding="utf-8",
    )
    return path
