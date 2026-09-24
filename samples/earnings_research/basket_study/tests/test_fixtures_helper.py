"""tests/_fixtures.require(): fixture-backed tests skip, not fail, when the
vendor-data parquet is absent (the exported sample ships no fixtures)."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _fixtures  # noqa: E402


def test_require_returns_existing_path(tmp_path):
    p = tmp_path / "panel.parquet"
    p.write_bytes(b"x")
    assert _fixtures.require(p) == p


def test_require_skips_when_missing(tmp_path):
    with pytest.raises(pytest.skip.Exception, match="fixture not present"):
        _fixtures.require(tmp_path / "gone.parquet")


def test_conftest_installs_fake_config():
    """Unit tests never see the real account: conftest points settings at a
    fake config with test values."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    import settings
    assert settings.get("s3", "data_bucket") == "test-data-bucket"
    assert settings.get("aws", "profile") is None
