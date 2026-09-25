# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Race one Batch job across every fleet region that sells its instance type.

A Batch job goes to one queue in one region, and spot capacity for an 8-GPU node moves
between regions by the hour. So the same job definition is submitted to the shape's queue
in EVERY region where the type is enabled; the first copy Batch PLACES wins and every other
copy is withdrawn.

  placed    STARTING, RUNNING or SUCCEEDED, or FAILED after its container started: Batch
            gave it a node. A copy that ran and died WAS placed; racing on would only run
            the same broken job somewhere else.
  winner    the placed copy that started first (ties: region name), fixed once chosen
  losers    every other copy, withdrawn with TerminateJob (which cancels a queued job and
            stops a running one, so a photo finish never leaves two copies burning)

The race is poll-driven and resumable: its record lives in the state directory, and every
`poll` reads Batch, updates the record and yields if there is a winner. Nothing here waits
in the background, so a closed session leaves at worst queued copies, which `cancel`
withdraws and a deadline withdraws on the next poll.

The job definition must exist in every raced region. The fleet's bench and sft job
definitions do (the CDK declares them per region); a job definition of your own must be
deployed the same way — never registered from here.
"""
import time
from typing import Dict, List, Optional

from . import common
from .common import RACE_TAG_KEY, cat

PLACED = {"STARTING", "RUNNING", "SUCCEEDED"}
TERMINAL = {"SUCCEEDED", "FAILED"}


def race_regions(fleet: common.Fleet, itype: str, regions=None) -> List[dict]:
    """(region, queue, shape) for every region whose queue for the type is ENABLED."""
    t = fleet.instance_type(itype)
    out = []
    for region in (regions or fleet.regions):
        state, azs = cat.region_state(t, region, fleet.offerings)
        if state != cat.ENABLED:
            continue
        shape = cat.shapes_in(t, region)[0]
        out.append({"region": region, "queue": shape.queue_name(fleet.prefix),
                    "shape": shape.name(fleet.prefix), "arch": shape.arch,
                    "default_job_definition": cat.bench_job_definition_name(shape, fleet.prefix),
                    "az_ids": list(azs)})
    return out


def start(fleet: common.Fleet, clients: common.ClientFactory, instance_type: str, *,
          command: Optional[List[str]] = None, job_definition: Optional[str] = None,
          environment: Optional[Dict[str, str]] = None, regions=None, job_name: str = None,
          deadline_minutes: Optional[float] = None) -> dict:
    """Submit one copy per region and record the race. `job_definition` defaults to the
    shape's bench job definition; `command` overrides the job definition's."""
    targets = race_regions(fleet, instance_type, regions)
    if not targets:
        raise ValueError(f"no fleet region has an ENABLED queue for {instance_type}")
    rid = common.new_id("race")
    name = job_name or f"race-{instance_type.replace('.', '-')}"
    overrides = {}
    if command:
        overrides["command"] = list(command)
    if environment:
        overrides["environment"] = [{"name": k, "value": str(v)} for k, v in sorted(environment.items())]
    entrants = []
    for t in targets:
        jd = job_definition or t["default_job_definition"]
        e = {"region": t["region"], "queue": t["queue"], "job_definition": jd,
             "job_id": None, "status": "NOT_SUBMITTED", "reason": "", "started_at": None}
        try:
            r = clients("batch", t["region"]).submit_job(
                jobName=name, jobQueue=t["queue"], jobDefinition=jd,
                containerOverrides=overrides, tags={RACE_TAG_KEY: rid}, propagateTags=True)
            e.update(job_id=r["jobId"], status="SUBMITTED")
        except Exception as ex:  # a region that refuses must not stop the others
            e.update(status="FAILED", reason=f"submit failed: {type(ex).__name__}: {ex}")
        entrants.append(e)
    record = {"id": rid, "kind": "batch-race", "instance_type": instance_type, "job_name": name,
              "command": command, "created_utc": common.utc_now(), "created_epoch": time.time(),
              "deadline_epoch": (time.time() + deadline_minutes * 60) if deadline_minutes else None,
              "state": "racing", "winner": None, "entrants": entrants}
    return common.save(record)


def _describe(clients, region: str, job_ids: List[str]) -> Dict[str, dict]:
    out = {}
    for i in range(0, len(job_ids), 100):
        for j in clients("batch", region).describe_jobs(jobs=job_ids[i:i + 100]).get("jobs", []):
            out[j["jobId"]] = j
    return out


def _placed(e: dict) -> bool:
    return e["status"] in PLACED or (e["status"] == "FAILED" and e.get("started_at") is not None)


def _withdraw(clients, e: dict, reason: str) -> None:
    try:
        clients("batch", e["region"]).terminate_job(jobId=e["job_id"], reason=reason[:250])
        e["withdrawn"] = True
    except Exception as ex:
        e["withdraw_error"] = f"{type(ex).__name__}: {ex}"


def poll(record: dict, clients: common.ClientFactory, now: Optional[float] = None) -> dict:
    """Read every copy's state, pick the winner once, withdraw the losers. Idempotent."""
    now = time.time() if now is None else now
    by_region: Dict[str, List[dict]] = {}
    for e in record["entrants"]:
        if e["job_id"] and e["status"] not in TERMINAL:
            by_region.setdefault(e["region"], []).append(e)
    for region, es in by_region.items():
        try:
            jobs = _describe(clients, region, [e["job_id"] for e in es])
        except Exception as ex:   # one region's API outage must not stall the race
            for e in es:
                e["poll_error"] = f"{type(ex).__name__}: {ex}"
            continue
        for e in es:
            j = jobs.get(e["job_id"])
            if j is None:
                continue
            e.update(status=j["status"], reason=j.get("statusReason", ""),
                     started_at=j.get("startedAt"), poll_error=None)

    if record["state"] == "cancelled":
        return common.save(record)
    if record["winner"] is None:
        placed = [e for e in record["entrants"] if _placed(e)]
        if placed:
            w = min(placed, key=lambda e: (e.get("started_at") or float("inf"), e["region"]))
            record["winner"] = {"region": w["region"], "queue": w["queue"], "job_id": w["job_id"],
                                "placed_after_s": round(now - record["created_epoch"], 1)}
    if record["winner"]:
        for e in record["entrants"]:
            if e["job_id"] and e["job_id"] != record["winner"]["job_id"] \
                    and e["status"] not in TERMINAL and not e.get("withdrawn"):
                _withdraw(clients, e, f"lost the race to {record['winner']['region']}")
        w = next(e for e in record["entrants"] if e["job_id"] == record["winner"]["job_id"])
        record["state"] = "finished" if w["status"] in TERMINAL else "won"
    elif all(e["status"] in TERMINAL for e in record["entrants"]):
        record["state"] = "all-failed"
    elif record.get("deadline_epoch") and now > record["deadline_epoch"]:
        cancel(record, clients, "race deadline passed without a placement")
    return common.save(record)


def cancel(record: dict, clients: common.ClientFactory, reason: str = "race cancelled") -> dict:
    """Withdraw every copy that is not terminal, the winner included."""
    for e in record["entrants"]:
        if e["job_id"] and e["status"] not in TERMINAL and not e.get("withdrawn"):
            _withdraw(clients, e, reason)
    record["state"] = "cancelled"
    return common.save(record)


def summary(record: dict) -> dict:
    return {"id": record["id"], "state": record["state"], "instance_type": record["instance_type"],
            "winner": record["winner"],
            "entrants": [{k: e.get(k) for k in ("region", "queue", "job_id", "status", "reason",
                                                 "withdrawn", "withdraw_error", "poll_error")
                          if e.get(k) not in (None, "")}
                         for e in record["entrants"]]}
