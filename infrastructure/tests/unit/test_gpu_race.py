# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import itertools
import threading

import pytest

from infrastructure.gpu_fleet.race import common, hunt, racer, runner, server

OFFERINGS = {
    "us-east-1": {"az_ids": ["use1-az1", "use1-az2"], "types": {"p5.48xlarge": ["use1-az1", "use1-az2"],
                                                               "g6.xlarge": ["use1-az1"]},
                  "batch": ["p5.48xlarge", "g6.xlarge"]},
    "us-west-2": {"az_ids": ["usw2-az1"], "types": {"p5.48xlarge": ["usw2-az1"]},
                  "batch": ["p5.48xlarge"]},
    # offered, but Batch does not accept it: raw EC2 only
    "eu-north-1": {"az_ids": ["eun1-az1"], "types": {"p5.48xlarge": ["eun1-az1"]}, "batch": []},
}


@pytest.fixture(autouse=True)
def state(tmp_path, monkeypatch):
    monkeypatch.setenv("GPU_RACE_STATE_DIR", str(tmp_path))


def fleet():
    return common.Fleet(namespace="test", prefix="rt", home_region="us-east-1",
                        regions=("us-east-1", "us-west-2", "eu-north-1"),
                        runtime_repositories={"hopper": "test-gpu-fleet-hopper"},
                        weight_bucket="amzn-s3-demo-bucket-weights", max_node_hours=24,
                        offerings=OFFERINGS)


class Clients:
    def __init__(self, **by_service):
        self.by = by_service

    def __call__(self, service, region):
        return self.by[service][region] if isinstance(self.by[service], dict) else self.by[service]


# --------------------------------------------------------------------------------- batch
class Batch:
    ids = itertools.count()

    def __init__(self, region):
        self.region, self.jobs, self.submitted, self.terminated = region, {}, [], []

    def submit_job(self, **kw):
        jid = f"{self.region}-{next(self.ids)}"
        self.submitted.append(kw)
        self.jobs[jid] = {"jobId": jid, "status": "RUNNABLE"}
        return {"jobId": jid}

    def describe_jobs(self, jobs):
        return {"jobs": [self.jobs[j] for j in jobs]}

    def terminate_job(self, jobId, reason):
        self.terminated.append(jobId)
        self.jobs[jobId]["status"] = "FAILED"


def batch_clients():
    b = {r: Batch(r) for r in OFFERINGS}
    return b, Clients(batch=b)


def test_race_submits_only_where_the_queue_is_enabled():
    b, clients = batch_clients()
    rec = racer.start(fleet(), clients, "p5.48xlarge", command=["bench_gpu", "--skip", "download"])
    assert sorted(e["region"] for e in rec["entrants"]) == ["us-east-1", "us-west-2"]
    kw = b["us-east-1"].submitted[0]
    assert kw["jobQueue"] == "rt-h100-p5-48xl-8g-32c-queue"
    assert kw["jobDefinition"] == "bench-gpu-h100-p5-48xl-8g-32c-hopper-jd"
    assert kw["containerOverrides"]["command"] == ["bench_gpu", "--skip", "download"]
    assert kw["tags"] == {common.RACE_TAG_KEY: rec["id"]} and kw["propagateTags"] is True
    assert not b["eu-north-1"].submitted
    with pytest.raises(ValueError):
        racer.start(fleet(), clients, "p6-b200.48xlarge")


def test_first_placed_wins_and_losers_are_withdrawn():
    b, clients = batch_clients()
    rec = racer.start(fleet(), clients, "p5.48xlarge", job_definition="sft-8gpu-hopper-jd")
    assert racer.poll(rec, clients)["state"] == "racing"
    west = next(e for e in rec["entrants"] if e["region"] == "us-west-2")
    b["us-west-2"].jobs[west["job_id"]].update(status="STARTING")
    rec = racer.poll(rec, clients)
    assert rec["winner"]["region"] == "us-west-2" and rec["state"] == "won"
    east = next(e for e in rec["entrants"] if e["region"] == "us-east-1")
    assert b["us-east-1"].terminated == [east["job_id"]]
    # sticky: a later placement elsewhere never replaces the winner
    b["us-east-1"].jobs[east["job_id"]].update(status="RUNNING", startedAt=1)
    assert racer.poll(rec, clients)["winner"]["region"] == "us-west-2"
    # and the saved record resumes the same race
    assert common.load(rec["id"])["winner"]["job_id"] == west["job_id"]


def test_photo_finish_goes_to_the_earliest_start():
    b, clients = batch_clients()
    rec = racer.start(fleet(), clients, "p5.48xlarge")
    for e, t in zip(rec["entrants"], (200, 100)):
        b[e["region"]].jobs[e["job_id"]].update(status="RUNNING", startedAt=t)
    rec = racer.poll(rec, clients)
    assert rec["winner"]["region"] == "us-west-2"
    assert b["us-east-1"].terminated and not b["us-west-2"].terminated


def test_a_job_that_failed_after_starting_was_placed():
    b, clients = batch_clients()
    rec = racer.start(fleet(), clients, "p5.48xlarge")
    e = rec["entrants"][0]
    b[e["region"]].jobs[e["job_id"]].update(status="FAILED", startedAt=5)
    rec = racer.poll(rec, clients)
    assert rec["winner"]["job_id"] == e["job_id"] and rec["state"] == "finished"


def test_deadline_and_cancel_withdraw_everything():
    b, clients = batch_clients()
    rec = racer.start(fleet(), clients, "p5.48xlarge", deadline_minutes=1)
    rec = racer.poll(rec, clients, now=rec["created_epoch"] + 120)
    assert rec["state"] == "cancelled"
    assert len(b["us-east-1"].terminated) == len(b["us-west-2"].terminated) == 1
    assert racer.poll(rec, clients)["state"] == "cancelled"


def test_a_region_that_refuses_the_submit_does_not_stop_the_race():
    b, clients = batch_clients()
    b["us-west-2"].submit_job = lambda **kw: (_ for _ in ()).throw(RuntimeError("no such jd"))
    rec = racer.start(fleet(), clients, "p5.48xlarge")
    west = next(e for e in rec["entrants"] if e["region"] == "us-west-2")
    assert west["status"] == "FAILED" and "no such jd" in west["reason"]
    assert racer.poll(rec, clients)["state"] == "racing"


def test_race_status_waits_while_racing():
    b, clients = batch_clients()
    rid = server.race_job("p5.48xlarge", clients=clients)["id"]
    sleeps = []

    def sleep(s):
        sleeps.append(s)
        job = next(iter(b["us-east-1"].jobs.values()))
        job.update(status="RUNNING", startedAt=1)

    out = server.race_status(rid, wait_seconds=600, clients=clients, sleep=sleep)
    assert out["winner"]["region"] == "us-east-1" and sleeps == [server.POLL_S]


# ----------------------------------------------------------------------------------- ec2
class Ec2:
    ids = itertools.count()

    def __init__(self, region, capacity):
        self.region, self.capacity = region, capacity   # az id -> nodes it will give
        self.fleets, self.terminated, self.pgs = [], [], []

    def describe_launch_templates(self, LaunchTemplateNames):
        return {"LaunchTemplates": [{"LaunchTemplateName": n} for n in LaunchTemplateNames]}

    def describe_subnets(self, Filters):
        names = Filters[0]["Values"]
        return {"Subnets": [{"SubnetId": f"subnet-{n}", "AvailabilityZoneId": n.rsplit("-", 2)[-2]
                             + "-" + n.rsplit("-", 1)[-1]} for n in names]}

    def create_placement_group(self, GroupName, Strategy):
        self.pgs.append(GroupName)

    def create_fleet(self, **kw):
        self.fleets.append(kw)
        o = kw["LaunchTemplateConfigs"][0]["Overrides"][0]
        az = o["SubnetId"].rsplit("-", 2)[-2] + "-" + o["SubnetId"].rsplit("-", 1)[-1]
        n = min(self.capacity.get(az, 0), kw["TargetCapacitySpecification"]["TotalTargetCapacity"])
        ids = [f"i-{self.region}-{next(self.ids)}" for _ in range(n)]
        return {"Instances": [{"InstanceIds": ids}] if ids else [],
                "Errors": [] if ids else [{"ErrorCode": "InsufficientInstanceCapacity"}]}

    def terminate_instances(self, InstanceIds):
        self.terminated.extend(InstanceIds)


class Ssm:
    def __init__(self):
        self.sent = []

    def get_parameter(self, Name):
        assert Name == common.cat.AMI_SSM_PARAMETER
        return {"Parameter": {"Value": "ami-0123456789abcdef0"}}

    def send_command(self, **kw):
        self.sent.append(kw)
        return {"Command": {"CommandId": "cmd-1"}}

    def get_command_invocation(self, CommandId, InstanceId):
        return {"Status": "Success", "StandardOutputContent": f"started on {InstanceId}"}


class Sts:
    def get_caller_identity(self):
        return {"Account": "123456789012"}


def ec2_clients(capacity):
    e = {r: Ec2(r, capacity.get(r, {})) for r in OFFERINGS}
    s = Ssm()
    return e, s, Clients(ec2=e, ssm=s, sts=Sts())


def test_hunt_fills_in_one_region_and_the_template_is_the_fleets():
    e, _, clients = ec2_clients({"eu-north-1": {"eun1-az1": 2}})
    rec = hunt.hunt(fleet(), clients, "p5.48xlarge", count=2, ttl_hours=6)
    w = rec["winner"]
    assert rec["state"] == "filled" and w["region"] == "eu-north-1" and len(w["instance_ids"]) == 2
    req = e["eu-north-1"].fleets[0]
    spec = req["LaunchTemplateConfigs"][0]
    assert spec["LaunchTemplateSpecification"] == {"LaunchTemplateName": "rt-h100-p5-48xl-8g-32c",
                                                   "Version": "$Latest"}
    # multi-card: private subnet; two nodes: a cluster placement group
    assert spec["Overrides"][0]["SubnetId"] == "subnet-test-gpu-private-eun1-az1"
    assert spec["Overrides"][0]["Placement"]["GroupName"] == "rt-h100-p5-48xl-8g-32c-eun1-az1"
    assert req["SpotOptions"]["MinTargetCapacity"] == 2 and req["SpotOptions"]["SingleAvailabilityZone"]
    tags = {t["Key"]: t["Value"] for t in req["TagSpecifications"][0]["Tags"]}
    assert tags[common.cat.HUNT_TAG_KEY] == common.cat.HUNT_TAG_VALUE
    assert tags[common.cat.TTL_TAG_KEY] == "6" and tags[common.RACE_TAG_KEY] == rec["id"]


def test_partial_fill_is_given_back_and_the_next_az_is_tried():
    e, _, clients = ec2_clients({"us-east-1": {"use1-az1": 1, "use1-az2": 2}})
    rec = hunt.hunt(fleet(), clients, "p5.48xlarge", count=2, regions=["us-east-1"])
    assert rec["winner"]["az_id"] == "use1-az2"
    assert len(e["us-east-1"].terminated) == 1      # the partial az1 node


def test_no_capacity_anywhere_and_dry_run_launch_nothing():
    e, _, clients = ec2_clients({})
    assert hunt.hunt(fleet(), clients, "p5.48xlarge")["state"] == "no-capacity"
    e, _, clients = ec2_clients({"us-east-1": {"use1-az1": 1}})
    rec = hunt.hunt(fleet(), clients, "p5.48xlarge", dry_run=True)
    assert rec["state"] == "dry-run" and not any(x.fleets for x in e.values())


def test_single_card_shape_uses_the_public_subnet():
    e, _, clients = ec2_clients({"us-east-1": {"use1-az1": 1}})
    rec = hunt.hunt(fleet(), clients, "g6.xlarge")
    o = e["us-east-1"].fleets[0]["LaunchTemplateConfigs"][0]["Overrides"][0]
    assert o["SubnetId"] == "subnet-test-gpu-public-use1-az1" and "Placement" not in o
    assert rec["winner"]["arch"] == "hopper"


def test_run_and_release():
    e, s, clients = ec2_clients({"us-west-2": {"usw2-az1": 1}})
    rec = hunt.hunt(fleet(), clients, "p5.48xlarge", regions=["us-west-2"])
    out = runner.run(fleet(), clients, rec, ["bench_gpu", "--cohort", "c1"], name="bench")
    script = s.sent[0]["Parameters"]["commands"][0]
    assert "123456789012.dkr.ecr.us-west-2.amazonaws.com/test-gpu-fleet-hopper:latest" in script
    assert "-e GPU_FLEET_HOME_REGION=us-east-1" in script and "TRAIN_REQUIRE_NVME=1" in script
    assert "awslogs-group=/test/gpu-fleet/nodes" in script and "bench_gpu --cohort c1" in script
    assert out["log_streams"] == [f"bench/{i}" for i in rec["winner"]["instance_ids"]]
    st = runner.status(common.load(rec["id"]), clients)
    assert all(n["status"] == "Success" for n in st["nodes"].values())
    rel = hunt.release(common.load(rec["id"]), clients)
    assert rel["state"] == "released" and e["us-west-2"].terminated == rec["winner"]["instance_ids"]
    with pytest.raises(ValueError):
        runner.run(fleet(), clients, rel, ["bench_gpu"])


def test_fleet_load_reads_the_committed_parameters(monkeypatch):
    monkeypatch.setenv("NAMESPACE", "test")
    f = common.Fleet.load(home_region="us-east-1", offerings=OFFERINGS)
    assert f.prefix == "rt" and f.runtime_repositories["blackwell"] == "test-gpu-fleet-blackwell"
    assert f.regions == common.cat.RECOMMENDED_REGIONS
    assert f.base_env()["GPU_FLEET_WEIGHT_BUCKET"] == f.weight_bucket


def test_photo_finish_loser_gives_back_exactly_its_own_nodes():
    e, _, clients = ec2_clients({"us-east-1": {"use1-az1": 1}, "us-west-2": {"usw2-az1": 1}})
    both_launched = threading.Barrier(2, timeout=5)
    for x in (e["us-east-1"], e["us-west-2"]):
        x.create_fleet = (lambda f: lambda **kw: (f(**kw), both_launched.wait())[0])(x.create_fleet)
    real = hunt._Hunt.claim
    hunt._Hunt.claim = lambda self, got: real(self, got) if got["region"] == "us-east-1" else False
    try:
        rec = hunt.hunt(fleet(), clients, "p5.48xlarge", regions=["us-east-1", "us-west-2"])
    finally:
        hunt._Hunt.claim = real
    assert rec["winner"]["region"] == "us-east-1" and not e["us-east-1"].terminated
    assert len(e["us-west-2"].terminated) == 1
    assert any(a["outcome"] == "lost-photo-finish" for a in rec["attempts"])
