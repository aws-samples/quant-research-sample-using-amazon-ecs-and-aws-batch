"""Point settings at a FAKE config for the whole test session (byte-identical
copy in every package's tests directory).

Unit tests must never depend on the real account values in
earnings_research.config.json, and must run unchanged in the exported sample
where config.json holds placeholders. Live tests that are opt-in via an
environment flag call `settings.reset()` themselves to pick up the real file.
"""
import json
import sys
from pathlib import Path

import pytest

_PKG = Path(__file__).resolve().parent
if _PKG.name == "tests":
    _PKG = _PKG.parent
sys.path.insert(0, str(_PKG))

FAKE_CONFIG = {
    "aws": {"region": "us-east-1", "profile": None, "account_id": "123456789012"},
    "s3": {"data_bucket": "test-data-bucket", "diagnostics_bucket": "test-diagnostics-bucket"},
    "redshift": {"workgroup": "test-workgroup", "database": "dev",
                 "secret_arn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:test-secret"},
    "batch": {"job_queue": "test-job-queue", "alpaca_job_queue": "test-capped-queue",
              "compute_environment": "test-compute-env",
              "job_definitions": {"basket_study": "test-basket-eval", "market_data": "test-events",
                                  "sentiment_analysis": "test-sentiment-eval",
                                  "content_pipeline": "test-content-backfill"}},
    "codebuild": {"image_build_projects": {"basket_study": "test-image-build",
                                           "sentiment_analysis": "test-sentiment-image-build"}},
    "secrets": {"alpaca": "test-alpaca-secret", "databento": "test-databento-secret"},
    "contact_email": "test@example.com",
    "paths": {"constituent_cache_dir": "constituent_cache"},
}


@pytest.fixture(scope="session", autouse=True)
def _fake_settings(tmp_path_factory):
    import settings
    path = tmp_path_factory.mktemp("cfg") / "config.json"
    path.write_text(json.dumps(FAKE_CONFIG))
    settings.use(path)
    yield
    settings.reset()
