# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import importlib.util
import pathlib

from infrastructure.gpu_fleet import catalogue as cat

_PATH = pathlib.Path(__file__).resolve().parents[2] / "gpu_fleet" / "mcp" / "server.py"
_spec = importlib.util.spec_from_file_location("gpu_fleet_mcp_server", _PATH)
server = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(server)


class _Batch:
    def __init__(self, queues):
        self.queues = queues

    def describe_job_queues(self, jobQueues):
        assert len(jobQueues) <= 100
        return {"jobQueues": [{"jobQueueName": n, "state": "ENABLED", "status": "VALID",
                               "computeEnvironmentOrder": [{"order": 1, "computeEnvironment":
                                   f"arn:aws:batch:us-east-1:123456789012:compute-environment/{n[:-6]}-ce"}]}
                              for n in jobQueues if n in self.queues]}

    def describe_compute_environments(self, computeEnvironments):
        return {"computeEnvironments": [
            {"computeEnvironmentArn": a, "computeEnvironmentName": a.split("/")[-1],
             "state": "ENABLED", "status": "VALID",
             "computeResources": {"type": "SPOT", "maxvCpus": 192, "desiredvCpus": 0}}
            for a in computeEnvironments]}


def test_every_preset_family_has_a_reason():
    p = server._fleet_params()
    for name, preset in p["presets"].items():
        assert name in server.PRESET_REASONS
        assert set(preset["families"]) <= set(server.FAMILY_REASONS)
    assert set(server.FAMILY_REASONS) == set(cat.FAMILIES)


def test_describe_preset_counts_every_region():
    d = server.describe_preset()
    assert d["active"] and d["preset"] == "open-weight-fine-tuning"
    assert tuple(d["regions"]) == cat.RECOMMENDED_REGIONS
    n = len(d["instance_types"])
    assert all(sum(c.values()) == n for c in d["regions"].values())
    assert d["families"]["b200"]["arch"] == "blackwell" and d["families"]["l4"]["spot_quota"] == "G"


def test_describe_shapes_names_match_the_catalogue():
    rows = server.describe_shapes("ap-northeast-2", family="b300")
    names = {r["launch_template"] for r in rows}
    assert names == {"rt-b300-p6-48xl-8g-1c", "rt-b300-p6-48xl-8g-16c-noefa"}
    for r in server.describe_shapes("us-east-1"):
        assert r["state"] != cat.ABSENT
        if r["state"] == cat.TEMPLATE_ONLY:
            assert r["job_queue"] is None and r["bench_job_definition"] is None
        else:
            assert r["job_queue"] == r["launch_template"] + "-queue"


def test_describe_queues_joins_live_and_reports_missing():
    rows = server.describe_queues("us-east-1", _Batch(set()))
    assert rows and all("not found" in r["live"]["error"] for r in rows)
    first = rows[0]["job_queue"]
    rows = {r["job_queue"]: r for r in server.describe_queues("us-east-1", _Batch({first}))}
    assert rows[first]["live"]["compute_environments"][0]["type"] == "SPOT"
