import json
import sys
from pathlib import Path

import pytest

_MCP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_MCP_DIR))

import server
import settings

PARAMETERS = _MCP_DIR.parents[2] / "infrastructure" / "config" / "parameters.json"


@pytest.fixture(autouse=True)
def _config(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({
        "aws": {"region": "us-east-1"},
        "batch": {"job_queue": "ns-cpu", "alpaca_job_queue": "ns-capped"}}))
    settings.use(path)
    yield
    settings.reset()


class _Batch:
    def __init__(self, queues):
        self.queues = queues

    def describe_job_queues(self, jobQueues):
        return {"jobQueues": [q for q in self.queues if q["jobQueueName"] in jobQueues]}

    def describe_compute_environments(self, computeEnvironments):
        return {"computeEnvironments": [
            {"computeEnvironmentArn": arn, "computeEnvironmentName": arn.split("/")[-1],
             "state": "ENABLED", "status": "VALID",
             "computeResources": {"type": "SPOT", "maxvCpus": 24 if "capped" in arn else 256,
                                  "desiredvCpus": 0}}
            for arn in computeEnvironments]}


def _queue(name):
    return {"jobQueueName": name, "state": "ENABLED", "status": "VALID",
            "computeEnvironmentOrder": [{"order": 1, "computeEnvironment":
                                         f"arn:aws:batch:us-east-1:123456789012:compute-environment/{name}"}]}


def test_every_configured_queue_has_a_registry_entry():
    params = json.loads(PARAMETERS.read_text())
    assert {q["id"] for q in params["batch"]["queues"]} == set(server.QUEUE_REGISTRY)


def test_describe_joins_purpose_with_live_state():
    rows = {r["id"]: r for r in server.describe_queues(_Batch([_queue("ns-cpu"), _queue("ns-capped")]))}
    assert rows["capped"]["job_queue"] == "ns-capped"
    assert rows["capped"]["why_capped"]
    assert rows["capped"]["live"]["compute_environments"][0]["maxv_cpus"] == 24
    assert rows["cpu"]["live"]["state"] == "ENABLED"


def test_missing_queue_is_reported_not_raised():
    rows = {r["id"]: r for r in server.describe_queues(_Batch([_queue("ns-cpu")]))}
    assert "not found" in rows["capped"]["live"]["error"]
    assert rows["cpu"]["live"]["status"] == "VALID"


def test_unset_config_key_is_reported(tmp_path):
    path = tmp_path / "partial.json"
    path.write_text(json.dumps({"batch": {"job_queue": "ns-cpu", "alpaca_job_queue": "<placeholder>"}}))
    settings.use(path)
    rows = {r["id"]: r for r in server.describe_queues(_Batch([_queue("ns-cpu")]))}
    assert "not set" in rows["capped"]["live"]["error"]
