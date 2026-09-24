"""load_config(): account-specific values left null in a pipeline config JSON
are filled from settings (the one root config); Config.from_dict stays pure so
unit tests that build configs by hand never touch settings."""
import json

import pytest

import settings
from config import Config, load_config


def _cfg_file(tmp_path, data):
    p = tmp_path / "cfg.json"
    p.write_text(json.dumps(data))
    return str(p)


def test_null_bucket_workgroup_secret_filled_from_settings(tmp_path):
    cfg = load_config(_cfg_file(tmp_path, {
        "s3": {"bucket": None, "prefix": "earnings-content"},
        "redshift": {"workgroup": None, "secret_arn": None}}))
    assert cfg.s3.bucket == settings.get("s3", "data_bucket")
    assert cfg.redshift.workgroup == settings.get("redshift", "workgroup")
    assert cfg.redshift.database == settings.get("redshift", "database")
    assert cfg.redshift.secret_arn == settings.get("redshift", "secret_arn")


def test_absent_sections_are_filled_too(tmp_path):
    cfg = load_config(_cfg_file(tmp_path, {"job_name": "x"}))
    assert cfg.s3.bucket == settings.get("s3", "data_bucket")
    assert cfg.redshift.secret_arn == settings.get("redshift", "secret_arn")
    assert cfg.aws.region == settings.get("aws", "region")


def test_explicit_values_win_over_settings(tmp_path):
    cfg = load_config(_cfg_file(tmp_path, {
        "s3": {"bucket": "my-own-bucket"}, "redshift": {"workgroup": "my-wg", "secret_arn": "arn:x"}}))
    assert cfg.s3.bucket == "my-own-bucket"
    assert cfg.redshift.workgroup == "my-wg"
    assert cfg.redshift.secret_arn == "arn:x"


def test_null_user_agent_gets_contact_from_settings(tmp_path):
    cfg = load_config(_cfg_file(tmp_path, {"fetch": {"user_agent": None}}))
    assert cfg.fetch.user_agent == (
        f"quant-research-earnings-pipeline/1.0 (contact: {settings.get('contact_email')})")


def test_default_user_agent_stays_contactless(tmp_path):
    cfg = load_config(_cfg_file(tmp_path, {}))
    assert cfg.fetch.user_agent == "quant-research-earnings-pipeline/1.0"


def test_profile_is_never_filled_from_settings(tmp_path):
    # in-container runs must use the task role; local runs set AWS_PROFILE
    cfg = load_config(_cfg_file(tmp_path, {"aws": {"profile": None}}))
    assert cfg.aws.profile is None


def test_from_dict_is_pure():
    cfg = Config.from_dict({"s3": {"bucket": None}, "redshift": {"workgroup": None}})
    assert cfg.s3.bucket is None
    assert cfg.redshift.workgroup is None
