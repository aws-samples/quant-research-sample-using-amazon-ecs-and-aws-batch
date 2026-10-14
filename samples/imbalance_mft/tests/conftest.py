"""Point settings at a FAKE config for the whole test session: unit tests never depend on real account values and run unchanged against the
placeholder config.json of the sample."""
import json, sys
from pathlib import Path
import pytest

_PKG = Path(__file__).resolve().parent
if _PKG.name == "tests": _PKG = _PKG.parent
sys.path.insert(0, str(_PKG))

FAKE_CONFIG = {"aws": {"region": "us-east-1", "profile": None},
               "s3": {"data_bucket": "test-data-bucket", "dataset_prefix": "derived/test", "dataset_run": "test-dataset-run"},
               "batch": {"job_queue": "test-cpu-queue", "big_queue": "test-big-queue", "job_definition": "test-job-definition", "mem_cap_mib": 30000, "big_cap_mib": 360000,
                         "resources": {"controller": [2, 4000, 40], "sig": [8, 32000, 3], "bask": [8, 64000, 3], "pnl": [32, 250000, 10]}},
               "paths": {"chrome": "/usr/bin/chromium", "viewer_out": "/tmp/imbalance_mft_reports"}, "runs": {"default": "test-run"}}


@pytest.fixture(scope="session", autouse=True)
def _fake_settings(tmp_path_factory):
    import settings
    path = tmp_path_factory.mktemp("cfg") / "config.json"; path.write_text(json.dumps(FAKE_CONFIG)); settings.use(path)
