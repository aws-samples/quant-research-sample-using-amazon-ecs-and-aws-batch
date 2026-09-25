# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Hunt raw EC2 spot nodes in every fleet region at once; keep the first region that fills.

For what Batch cannot do: several nodes placed together, a node held across jobs, or a type
no Batch catalogue accepts (the fleet's template-only shapes). The nodes are launched from
the SAME launch template the region's compute environment uses, so a hunted node boots,
mounts its NVMe pool and data volume, and runs the fleet image exactly like a Batch node.

  per region  one thread; its AZs are tried in turn, each an all-or-nothing `instant` spot
              fleet (MinTargetCapacity = count, one AZ) — a partial fill is given back at once
              so the quota stays free for the next AZ
  yield       every thread re-checks the shared win before each AZ, so a region stops buying
              the moment another has filled
  photo finish two regions that fill in the same instant: the first to claim keeps its nodes,
              the other terminates its own

Every node carries the hunt tag (the only thing the hunter policy may terminate), a ttl tag
(the region's age terminator enforces it) and the race tag. Subnets are found by the Name
tag the fleet network gives them; a multi-card shape goes into the private subnet, since
EC2 gives no public IP to an instance with several network interfaces.
"""
import threading
from typing import Dict, List, Optional

from . import common
from .common import RACE_TAG_KEY, cat


class _Hunt:
    def __init__(self, count: int):
        self.count = count
        self.won = threading.Event()
        self.lock = threading.Lock()
        self.winner: Optional[dict] = None
        self.attempts: List[dict] = []
        #: instances THIS hunt launched, per region: the only ones it may terminate
        self.launched: Dict[str, List[str]] = {}

    def claim(self, result: dict) -> bool:
        with self.lock:
            if self.winner is None:
                self.winner = result
                self.won.set()
                return True
            return False

    def give_back(self, ec2, region: str, ids: List[str]) -> None:
        """Terminate nodes this hunt will not keep and take them off the surplus ledger."""
        ec2.terminate_instances(InstanceIds=ids)
        with self.lock:
            self.launched[region] = [i for i in self.launched.get(region, []) if i not in ids]

    def note(self, **attempt) -> None:
        with self.lock:
            self.attempts.append(attempt)


def _subnets(ec2, fleet: common.Fleet, az_ids, private: bool) -> Dict[str, str]:
    kind = "private" if private else "public"
    names = [f"{fleet.namespace}-gpu-{kind}-{a}" for a in az_ids]
    resp = ec2.describe_subnets(Filters=[{"Name": "tag:Name", "Values": names}])
    return {s["AvailabilityZoneId"]: s["SubnetId"] for s in resp.get("Subnets", [])}


def _tags(fleet, rid, shape_name, ttl_hours) -> List[dict]:
    return [{"Key": cat.HUNT_TAG_KEY, "Value": cat.HUNT_TAG_VALUE},
            {"Key": cat.TTL_TAG_KEY, "Value": str(ttl_hours)},
            {"Key": RACE_TAG_KEY, "Value": rid},
            {"Key": "Name", "Value": f"{shape_name}-hunt"}]


def _hunt_region(h: _Hunt, fleet, clients, rid, region, t, ttl_hours, dry_run) -> None:
    state, azs = cat.region_state(t, region, fleet.offerings)
    if not azs:
        h.note(region=region, outcome="skip", detail=f"{t.itype} not offered ({state})")
        return
    if h.won.is_set():
        h.note(region=region, outcome="yield", detail="another region already filled")
        return
    shape = cat.shapes_in(t, region)[0]
    lt = shape.name(fleet.prefix)
    ec2 = clients("ec2", region)
    try:
        ec2.describe_launch_templates(LaunchTemplateNames=[lt])
        subnets = _subnets(ec2, fleet, azs, private=shape.cards > 1)
        ami = clients("ssm", region).get_parameter(Name=cat.AMI_SSM_PARAMETER)["Parameter"]["Value"]
    except Exception as ex:
        h.note(region=region, outcome="skip", detail=f"{type(ex).__name__}: {ex}")
        return
    if dry_run:
        h.note(region=region, outcome="dry-run", launch_template=lt, az_ids=sorted(subnets))
        return
    for az in azs:
        if h.won.is_set():
            h.note(region=region, outcome="yield", detail="another region filled")
            return
        subnet = subnets.get(az)
        if subnet is None:
            h.note(region=region, az_id=az, outcome="skip", detail="no fleet subnet in this AZ")
            continue
        override = {"InstanceType": t.itype, "SubnetId": subnet, "ImageId": ami}
        if h.count > 1:
            pg = f"{lt}-{az}"
            try:
                ec2.create_placement_group(GroupName=pg, Strategy="cluster")
            except Exception:
                pass   # already exists
            override["Placement"] = {"GroupName": pg}
        try:
            resp = ec2.create_fleet(
                Type="instant",
                TargetCapacitySpecification={"TotalTargetCapacity": h.count,
                                             "DefaultTargetCapacityType": "spot"},
                SpotOptions={"AllocationStrategy": "price-capacity-optimized",
                             "SingleAvailabilityZone": True, "MinTargetCapacity": h.count},
                LaunchTemplateConfigs=[{
                    # $Latest: CloudFormation adds a version on every update without moving
                    # the default one
                    "LaunchTemplateSpecification": {"LaunchTemplateName": lt, "Version": "$Latest"},
                    "Overrides": [override]}],
                TagSpecifications=[{"ResourceType": "instance",
                                    "Tags": _tags(fleet, rid, lt, ttl_hours)}])
        except Exception as ex:
            h.note(region=region, az_id=az, outcome="error", detail=f"{type(ex).__name__}: {ex}")
            continue
        ids = [i for f in resp.get("Instances", []) for i in f.get("InstanceIds", [])]
        with h.lock:
            h.launched.setdefault(region, []).extend(ids)
        if len(ids) >= h.count:
            got = {"region": region, "az_id": az, "subnet": subnet, "launch_template": lt,
                   "arch": shape.arch, "instance_ids": ids[:h.count]}
            if h.claim(got):
                h.note(region=region, az_id=az, outcome="filled", instance_ids=ids)
                return
            h.note(region=region, az_id=az, outcome="lost-photo-finish", instance_ids=ids)
            h.give_back(ec2, region, ids)
            return
        codes = sorted({e.get("ErrorCode", "") for e in resp.get("Errors", [])})
        if ids:   # partial: give it back so the quota is free for the next AZ
            h.give_back(ec2, region, ids)
        h.note(region=region, az_id=az, outcome="no-capacity", partial=len(ids), errors=codes)


def hunt(fleet: common.Fleet, clients: common.ClientFactory, instance_type: str, *,
         count: int = 1, regions=None, ttl_hours: Optional[float] = None,
         dry_run: bool = False) -> dict:
    """Race every region for `count` nodes in one AZ; returns (and saves) the hunt record."""
    t = fleet.instance_type(instance_type)
    ttl = ttl_hours if ttl_hours is not None else fleet.max_node_hours
    rid = common.new_id("hunt")
    h = _Hunt(count)
    threads = [threading.Thread(target=_hunt_region, daemon=True,
                                args=(h, fleet, clients, rid, r, t, ttl, dry_run))
               for r in (regions or fleet.regions)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    keep = set(h.winner["instance_ids"]) if h.winner else set()
    for region, ids in h.launched.items():   # a surplus that slipped past the yield
        extra = [i for i in ids if i not in keep]
        if extra:
            try:
                clients("ec2", region).terminate_instances(InstanceIds=extra)
            except Exception:
                pass   # the age terminator still bounds them
    record = {"id": rid, "kind": "ec2-hunt", "instance_type": instance_type, "count": count,
              "ttl_hours": ttl, "dry_run": dry_run, "created_utc": common.utc_now(),
              "state": "filled" if h.winner else ("dry-run" if dry_run else "no-capacity"),
              "winner": h.winner, "attempts": sorted(h.attempts, key=lambda a: a["region"])}
    return common.save(record)


def release(record: dict, clients: common.ClientFactory) -> dict:
    """Terminate the hunt's nodes. The hunter policy only allows hunt-tagged instances."""
    w = record.get("winner")
    if w and record["state"] != "released":
        clients("ec2", w["region"]).terminate_instances(InstanceIds=w["instance_ids"])
        record["state"] = "released"
        record["released_utc"] = common.utc_now()
    return common.save(record)
