"""settings.py: the ONE place account-specific values come from.

A byte-identical copy of settings.py lives in every exported package. It reads
`config.json` (sample layout) or `earnings_research.config.json` (internal
layout) found by walking up from the package directory, never at import time,
and refuses to hand out placeholder values (`<like-this>`).
"""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import settings


def _write(path: Path, data: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))
    return path


@pytest.fixture(autouse=True)
def _isolate():
    settings.reset()
    yield
    settings.reset()


def test_get_returns_nested_value(tmp_path):
    settings.use(_write(tmp_path / "config.json", {"s3": {"data_bucket": "bkt"}}))
    assert settings.get("s3", "data_bucket") == "bkt"


def test_get_missing_key_names_the_key(tmp_path):
    settings.use(_write(tmp_path / "config.json", {"s3": {}}))
    with pytest.raises(settings.SettingsError, match="s3.data_bucket"):
        settings.get("s3", "data_bucket")


def test_get_placeholder_is_refused(tmp_path):
    settings.use(_write(tmp_path / "config.json", {"s3": {"data_bucket": "<your-bucket>"}}))
    with pytest.raises(settings.SettingsError, match="placeholder"):
        settings.get("s3", "data_bucket")


def test_get_null_is_a_legal_value(tmp_path):
    settings.use(_write(tmp_path / "config.json", {"aws": {"profile": None}}))
    assert settings.get("aws", "profile") is None


def test_import_does_not_read_any_file():
    # reset() clears the cache; nothing should have been loaded by importing.
    assert settings.loaded_path() is None


def test_resolution_walks_up_and_prefers_sample_layout(tmp_path):
    # <root>/earnings_research.config.json (internal) AND <root>/config.json (sample):
    # sample name wins; discovered from a package TWO levels down (tools/results_browser).
    _write(tmp_path / "earnings_research.config.json", {"s3": {"data_bucket": "internal"}})
    _write(tmp_path / "config.json", {"s3": {"data_bucket": "sample"}})
    pkg = tmp_path / "tools" / "results_browser"
    pkg.mkdir(parents=True)
    settings.reset(anchor=pkg / "settings.py")
    assert settings.get("s3", "data_bucket") == "sample"
    assert settings.root() == tmp_path


def test_resolution_finds_internal_config_one_level_up(tmp_path):
    _write(tmp_path / "earnings_research.config.json", {"s3": {"data_bucket": "internal"}})
    pkg = tmp_path / "earnings_basket_study"
    pkg.mkdir()
    settings.reset(anchor=pkg / "settings.py")
    assert settings.get("s3", "data_bucket") == "internal"


def test_no_config_anywhere_is_an_error_only_on_use(tmp_path):
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    settings.reset(anchor=pkg / "settings.py")  # must not raise
    with pytest.raises(settings.SettingsError, match="config.json"):
        settings.get("s3", "data_bucket")


def test_sibling_maps_logical_names_in_internal_layout(tmp_path):
    _write(tmp_path / "earnings_research.config.json", {})
    settings.reset(anchor=tmp_path / "earnings_basket_study" / "settings.py")
    assert settings.sibling("market_data") == tmp_path / "earnings_market_data"
    assert settings.sibling("content_pipeline") == tmp_path / "earnings_content_pipeline"
    assert settings.sibling("viewer") == tmp_path / "tools" / "results_browser"
    assert settings.sibling("index_constituents") == tmp_path / "scripts"


def test_sibling_is_identity_in_sample_layout(tmp_path):
    _write(tmp_path / "config.json", {})
    settings.reset(anchor=tmp_path / "basket_study" / "settings.py")
    assert settings.sibling("market_data") == tmp_path / "market_data"
    assert settings.sibling("viewer") == tmp_path / "viewer"


def test_sibling_unknown_name_is_an_error(tmp_path):
    _write(tmp_path / "config.json", {})
    settings.reset(anchor=tmp_path / "basket_study" / "settings.py")
    with pytest.raises(settings.SettingsError, match="unknown package"):
        settings.sibling("nope")


def test_job_overrides_prefixes_package_and_sizes_by_command(tmp_path):
    settings.use(_write(tmp_path / "config.json", {"batch": {"resources": {
        "basket_study": {"vcpus": 1, "memory_mib": 4096},
        "basket_study:aggregate": {"vcpus": 2, "memory_mib": 16384}}}}))
    assert settings.job_overrides("basket_study", ["aggregate", "--finalize"]) == {
        "command": ["basket_study", "aggregate", "--finalize"],
        "resourceRequirements": [{"type": "VCPU", "value": "2"},
                                 {"type": "MEMORY", "value": "16384"}]}
    assert settings.job_overrides("basket_study", ["eval-event"])["resourceRequirements"] == [
        {"type": "VCPU", "value": "1"}, {"type": "MEMORY", "value": "4096"}]


def test_job_overrides_without_resources_keeps_job_definition_defaults(tmp_path):
    settings.use(_write(tmp_path / "config.json", {"batch": {}}))
    assert settings.job_overrides("market_data", ["plan"]) == {
        "command": ["market_data", "plan"]}
