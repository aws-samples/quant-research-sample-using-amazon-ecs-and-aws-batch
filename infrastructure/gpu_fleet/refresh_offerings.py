# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Rewrite offerings.json: where each catalogue instance type can be built, per region.

    python gpu_fleet/refresh_offerings.py                    every region of the preset
    python gpu_fleet/refresh_offerings.py --regions us-east-1,us-west-2
    python gpu_fleet/refresh_offerings.py --probe-batch      also re-read Batch's catalogue

Run from infrastructure/ with credentials for any account. Commit the result: synth reads
the snapshot and never calls AWS, so a change in offerings is a reviewable diff.

AZ IDs (use1-az4), never AZ names (us-east-1a). The name -> physical zone mapping is
shuffled per account, so a snapshot keyed by names would point another account's subnets
at the wrong hardware. IDs name the physical zone in every account.

Per region the snapshot records:
  az_ids            every available AZ ID (the VPC gets one public + one private subnet each)
  types             instance type -> AZ IDs offering it
  batch             catalogue types AWS Batch accepts in a compute environment here
  endpoint_az_ids   AZ IDs hosting every interface endpoint service the fleet uses

`batch` has no describe API. --probe-batch asks create_compute_environment for a bogus
instance type in the default VPC; the call is rejected and the error lists the accepted
types (nothing is created). Without --probe-batch the previous `batch` list is kept.
"""
import argparse
import datetime
import json
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from gpu_fleet import catalogue  # noqa: E402

SNAPSHOT = HERE / "offerings.json"


def _az_ids(ec2):
    zones = ec2.describe_availability_zones(
        Filters=[{"Name": "zone-type", "Values": ["availability-zone"]},
                 {"Name": "state", "Values": ["available"]}])["AvailabilityZones"]
    return {z["ZoneName"]: z["ZoneId"] for z in zones}


def _type_offerings(ec2, types):
    out = {}
    pages = ec2.get_paginator("describe_instance_type_offerings").paginate(
        LocationType="availability-zone-id",
        Filters=[{"Name": "instance-type", "Values": sorted(types)}])
    for page in pages:
        for o in page["InstanceTypeOfferings"]:
            out.setdefault(o["InstanceType"], set()).add(o["Location"])
    return {t: sorted(v) for t, v in sorted(out.items())}


def _endpoint_az_ids(ec2, region, name_to_id):
    names = [f"com.amazonaws.{region}.{s}" for s in catalogue.INTERFACE_ENDPOINTS]
    common = None
    for d in ec2.describe_vpc_endpoint_services(ServiceNames=names)["ServiceDetails"]:
        ids = {name_to_id[z] for z in d.get("AvailabilityZones", []) if z in name_to_id}
        common = ids if common is None else common & ids
    return sorted(common or [])


def _batch_catalogue(session, region):
    from botocore.exceptions import ClientError

    ec2, b = session.client("ec2", region_name=region), session.client("batch", region_name=region)
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])["Vpcs"]
    if not vpcs:
        raise SystemExit(f"{region}: --probe-batch needs a default VPC")
    vpc = vpcs[0]["VpcId"]
    subnets = [s["SubnetId"] for s in ec2.describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc]}])["Subnets"]][:1]
    groups = [g["GroupId"] for g in ec2.describe_security_groups(
        Filters=[{"Name": "vpc-id", "Values": [vpc]},
                 {"Name": "group-name", "Values": ["default"]}])["SecurityGroups"]][:1]
    try:
        b.create_compute_environment(
            computeEnvironmentName=f"catalogue-probe-{region}", type="MANAGED",
            computeResources={"type": "SPOT", "minvCpus": 0, "maxvCpus": 0,
                              "allocationStrategy": "SPOT_PRICE_CAPACITY_OPTIMIZED",
                              "instanceTypes": ["zz9.bogus"], "subnets": subnets,
                              "securityGroupIds": groups,
                              "instanceRole": "ecsInstanceRole",
                              "ec2Configuration": [{"imageType": catalogue.IMAGE_TYPE}]})
    except ClientError as e:
        m = re.search(r"\[(.*)\]", e.response["Error"]["Message"], re.S)
        if not m:
            raise
        return sorted(x.strip() for x in m.group(1).split(",") if x.strip())
    raise SystemExit(f"{region}: the bogus type was ACCEPTED; delete "
                     f"catalogue-probe-{region} by hand")


def refresh(session, regions, probe_batch=False):
    old = json.loads(SNAPSHOT.read_text()) if SNAPSHOT.exists() else {"regions": {}}
    types = sorted(catalogue.INSTANCE_TYPES)
    out = dict(old.get("regions", {}))
    for region in regions:
        ec2 = session.client("ec2", region_name=region)
        name_to_id = _az_ids(ec2)
        batch = (_batch_catalogue(session, region) if probe_batch
                 else old.get("regions", {}).get(region, {}).get("batch", []))
        out[region] = {
            "az_ids": sorted(name_to_id.values()),
            "types": _type_offerings(ec2, types),
            "batch": sorted(t for t in batch if t in catalogue.INSTANCE_TYPES),
            "endpoint_az_ids": _endpoint_az_ids(ec2, region, name_to_id),
        }
        print(f"{region}: {len(out[region]['az_ids'])} AZs, {len(out[region]['types'])} "
              f"types offered, {len(out[region]['batch'])} in the Batch catalogue")
    snapshot = {"_doc": __doc__.strip().splitlines()[0],
                "generated": datetime.date.today().isoformat(),
                "regions": dict(sorted(out.items()))}
    SNAPSHOT.write_text(json.dumps(snapshot, indent=1) + "\n")
    return snapshot


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--regions", default="", help="comma-separated; default the preset's")
    ap.add_argument("--profile", default=None)
    ap.add_argument("--probe-batch", action="store_true")
    a = ap.parse_args()
    import boto3

    regions = [r for r in a.regions.split(",") if r] or list(catalogue.RECOMMENDED_REGIONS)
    session = boto3.Session(profile_name=a.profile) if a.profile else boto3.Session()
    refresh(session, regions, probe_batch=a.probe_batch)


if __name__ == "__main__":
    main()
