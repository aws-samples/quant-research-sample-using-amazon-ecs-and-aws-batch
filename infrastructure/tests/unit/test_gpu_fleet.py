# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import base64
import json
import pathlib

import aws_cdk as core
import pytest
from aws_cdk.assertions import Match, Template

from infrastructure.common.pipeline import ImageBuildSpec
from infrastructure.gpu_fleet import catalogue as cat
from infrastructure.gpu_fleet.global_stack import GpuFleetGlobalConfig, GpuFleetGlobalStack
from infrastructure.gpu_fleet.region_stack import GpuFleetRegionStack, GpuRegionConfig
from infrastructure.gpu_fleet.weights import (GpuFleetWeightsConfig, GpuFleetWeightsStack,
                                              replica_bucket_name)

ACCOUNT = "012345678901"
AZS = ["use1-az1", "use1-az2", "use1-az4"]
OFFERINGS = {
    "us-east-1": {
        "az_ids": AZS,
        "endpoint_az_ids": ["use1-az1", "use1-az2"],
        "types": {"p5.48xlarge": ["use1-az2", "use1-az4"], "g6.xlarge": AZS,
                  "p6-b300.48xlarge": ["use1-az1"], "p4d.24xlarge": ["use1-az1"]},
        # p6-b200 in the catalogue but offered nowhere -> DISABLED; p4d offered, not accepted
        "batch": ["p5.48xlarge", "g6.xlarge", "p6-b300.48xlarge", "p6-b200.48xlarge"],
    },
    "ap-northeast-2": {
        "az_ids": ["apne2-az1", "apne2-az2"], "endpoint_az_ids": ["apne2-az1", "apne2-az2"],
        "types": {"p6-b300.48xlarge": ["apne2-az1"]}, "batch": ["p6-b300.48xlarge"],
    },
}
TYPES = ["p5.48xlarge", "g6.xlarge", "p6-b300.48xlarge", "p6-b200.48xlarge", "p4d.24xlarge"]


def region_template(region="us-east-1", **overrides) -> Template:
    kw = dict(namespace="test", home_region="us-east-1", prefix="rt", instance_types=TYPES,
              offerings=OFFERINGS, vpc_cidr="10.64.0.0/16",
              repositories={"hopper": "test-gpu-train-hopper",
                            "blackwell": "test-gpu-train-blackwell"},
              runtime_repositories={"hopper": "test-gpu-fleet-hopper",
                                    "blackwell": "test-gpu-fleet-blackwell"},
              instance_profile_name="test-gpu-node", job_role_name="test-gpu-job",
              execution_role_name="test-gpu-execution")
    kw.update(overrides)
    app = core.App()
    stack = GpuFleetRegionStack(app, f"gpu-{region}", config=GpuRegionConfig(**kw),
                                env=core.Environment(account=ACCOUNT, region=region))
    return Template.from_stack(stack)


def by_name(template: Template, typ: str, name_key: str) -> dict:
    return {r["Properties"][name_key]: r["Properties"]
            for r in template.find_resources(typ).values()}


# ---------------------------------------------------------------------------- catalogue
def test_catalogue_names_and_cards():
    t = cat.INSTANCE_TYPES
    assert cat.shapes_in(t["p5.48xlarge"], "us-east-1")[0].name("rt") == "rt-h100-p5-48xl-8g-32c"
    assert cat.shapes_in(t["p6-b200.48xlarge"], "us-east-1")[0].name("rt") == "rt-b200-p6-48xl-8g-8c"
    assert cat.shapes_in(t["g6e.24xlarge"], "us-east-1")[0].name("rt") == "rt-l40s-g6e-24xl-4g-1c"
    assert cat.shapes_in(t["g6.xlarge"], "us-east-1")[0].name("rt") == "rt-l4-g6-1xl-1g-1c"
    b300 = [s.name("rt") for s in cat.shapes_in(t["p6-b300.48xlarge"], "us-east-1")]
    assert b300 == ["rt-b300-p6-48xl-8g-16c", "rt-b300-p6-48xl-8g-16c-noefa"]
    # regions that refuse the EFA interface get a one-card template
    b300_kr = [s.name("rt") for s in cat.shapes_in(t["p6-b300.48xlarge"], "ap-northeast-2")]
    assert b300_kr == ["rt-b300-p6-48xl-8g-1c", "rt-b300-p6-48xl-8g-16c-noefa"]


def test_catalogue_families_and_disks():
    assert len(cat.INSTANCE_TYPES) == 26
    assert {t.chip for t in cat.INSTANCE_TYPES.values()} == set(cat.FAMILIES)
    for slug in ("h100", "h200", "b200", "b300", "a100"):
        assert cat.FAMILIES[slug].data_gib == 4096 and cat.FAMILIES[slug].p_family
    for slug in ("l4", "l40s"):
        assert cat.FAMILIES[slug].data_gib == 3072 and not cat.FAMILIES[slug].p_family
    assert {s for s, f in cat.FAMILIES.items() if f.arch == "blackwell"} == {"b200", "b300"}


def test_user_data_is_mime_multipart_with_pool_setup():
    ud = base64.b64decode(cat.FAMILIES["h100"].user_data(efa=True)).decode()
    assert ud.startswith('Content-Type: multipart/mixed; boundary="==BOUNDARY=="')
    assert ud.rstrip().endswith("--==BOUNDARY==--")
    assert "efa_installer.sh" in ud and "/mnt/nvme/ebs-data" in ud and "data-root" in ud
    # data volume first, so the pool's chmod covers it
    assert ud.index("ebs-data") < ud.index("Amazon EC2 NVMe Instance Storage")
    assert "efa_installer" not in base64.b64decode(cat.FAMILIES["l4"].user_data(efa=False)).decode()


def test_subnet_cidrs_are_stable_and_disjoint():
    pairs = [cat.subnet_cidrs("10.64.0.0/16", i) for i in range(8)]
    flat = [c for p in pairs for c in p]
    assert len(set(flat)) == 16
    assert pairs[0] == ("10.64.0.0/20", "10.64.128.0/20")
    with pytest.raises(ValueError):
        cat.subnet_cidrs("10.64.0.0/16", 8)


def test_select_families_and_unknowns():
    assert cat.select(families=["b200"]) == ["p6-b200.48xlarge"]
    assert set(cat.select(families=["l4"], instance_types=["p5.48xlarge"])) >= {"g6.xlarge", "p5.48xlarge"}
    with pytest.raises(ValueError):
        cat.select(families=["v100"])


def test_region_states():
    t = cat.INSTANCE_TYPES
    assert cat.region_state(t["p5.48xlarge"], "us-east-1", OFFERINGS)[0] == cat.ENABLED
    assert cat.region_state(t["p6-b200.48xlarge"], "us-east-1", OFFERINGS)[0] == cat.DISABLED
    assert cat.region_state(t["p4d.24xlarge"], "us-east-1", OFFERINGS)[0] == cat.TEMPLATE_ONLY
    assert cat.region_state(t["g6e.xlarge"], "us-east-1", OFFERINGS)[0] == cat.ABSENT
    # an excluded AZ narrows the placement set
    assert cat.region_state(t["p5.48xlarge"], "us-east-1", OFFERINGS,
                            ("use1-az2",))[1] == ("use1-az4",)


def test_offerings_snapshot_covers_the_preset():
    snap = cat.load_offerings()
    for region in cat.RECOMMENDED_REGIONS:
        assert snap[region]["az_ids"], region
        assert snap[region]["endpoint_az_ids"], region
        # AZ IDs only: names are shuffled per account
        assert all("-az" in a for a in snap[region]["az_ids"]), region
        assert set(snap[region]["batch"]) <= set(cat.INSTANCE_TYPES), region


def test_preset_matches_recommended_regions():
    p = json.loads((pathlib.Path(__file__).resolve().parents[2] / "config" /
                    "parameters.json").read_text())
    preset = p["gpu_fleet"]["presets"][p["gpu_fleet"]["preset"]]
    assert tuple(preset["regions"]) == cat.RECOMMENDED_REGIONS
    assert p["app_with_gpu_fleet"] is False


# ------------------------------------------------------------------------------ network
def test_network_subnets_nat_and_endpoints():
    t = region_template()
    t.resource_count_is("AWS::EC2::VPC", 1)
    t.resource_count_is("AWS::EC2::Subnet", 6)
    t.resource_count_is("AWS::EC2::NatGateway", 1)
    t.has_resource_properties("AWS::EC2::Subnet", {"AvailabilityZoneId": "use1-az4",
                                                   "MapPublicIpOnLaunch": False})
    gw = [r for r in t.find_resources("AWS::EC2::VPCEndpoint").values()
          if r["Properties"]["VpcEndpointType"] == "Gateway"][0]
    assert len(gw["Properties"]["RouteTableIds"]) == 2
    interface = [r for r in t.find_resources("AWS::EC2::VPCEndpoint").values()
                 if r["Properties"]["VpcEndpointType"] == "Interface"]
    assert len(interface) == len(cat.INTERFACE_ENDPOINTS)
    # only AZs that host every endpoint service
    assert all(len(r["Properties"]["SubnetIds"]) == 2 for r in interface)


def test_network_nat_az_override_and_opt_out():
    t = region_template(nat_az_id="use1-az4")
    nat = list(t.find_resources("AWS::EC2::NatGateway").values())[0]
    assert nat["Properties"]["SubnetId"]["Ref"].startswith("NetworkPublicUse1Az4")
    t = region_template(egress=False)
    t.resource_count_is("AWS::EC2::NatGateway", 0)
    t.resource_count_is("AWS::EC2::EIP", 0)
    with pytest.raises(ValueError):
        region_template(nat_az_id="use1-az9")


# -------------------------------------------------------------------------------- fleet
def test_fleet_states():
    t = region_template()
    lts = by_name(t, "AWS::EC2::LaunchTemplate", "LaunchTemplateName")
    queues = by_name(t, "AWS::Batch::JobQueue", "JobQueueName")
    assert set(lts) == {"rt-h100-p5-48xl-8g-32c", "rt-l4-g6-1xl-1g-1c", "rt-b300-p6-48xl-8g-16c",
                        "rt-b300-p6-48xl-8g-16c-noefa", "rt-b200-p6-48xl-8g-8c",
                        "rt-a100-p4d-24xl-8g-4c"}
    assert "rt-a100-p4d-24xl-8g-4c-queue" not in queues    # template-only
    assert queues["rt-b200-p6-48xl-8g-8c-queue"]["State"] == "DISABLED"
    assert queues["rt-h100-p5-48xl-8g-32c-queue"]["State"] == "ENABLED"
    t = region_template(raw_ec2=False)
    assert "rt-a100-p4d-24xl-8g-4c" not in by_name(t, "AWS::EC2::LaunchTemplate", "LaunchTemplateName")


def test_launch_templates_cards_and_public_ip():
    lts = by_name(region_template(), "AWS::EC2::LaunchTemplate", "LaunchTemplateName")
    p5 = lts["rt-h100-p5-48xl-8g-32c"]["LaunchTemplateData"]
    assert len(p5["NetworkInterfaces"]) == 32
    assert all(n["InterfaceType"] == "efa" for n in p5["NetworkInterfaces"])
    assert not any("AssociatePublicIpAddress" in n for n in p5["NetworkInterfaces"])
    assert "SubnetId" not in json.dumps(p5)
    assert [b["Ebs"]["VolumeSize"] for b in p5["BlockDeviceMappings"]] == [500, 4096]
    g6 = lts["rt-l4-g6-1xl-1g-1c"]["LaunchTemplateData"]
    assert g6["NetworkInterfaces"][0]["AssociatePublicIpAddress"] is True
    assert [b["Ebs"]["VolumeSize"] for b in g6["BlockDeviceMappings"]] == [500, 3072]
    noefa = lts["rt-b300-p6-48xl-8g-16c-noefa"]["LaunchTemplateData"]["NetworkInterfaces"]
    assert len(noefa) == 16 and not any("InterfaceType" in n for n in noefa)


def test_compute_environments_spot_and_subnets():
    t = region_template()
    ces = by_name(t, "AWS::Batch::ComputeEnvironment", "ComputeEnvironmentName")
    private = {k for k in t.find_resources("AWS::EC2::Subnet") if "Private" in k}
    for ce in ces.values():
        cr = ce["ComputeResources"]
        assert cr["Type"] == "SPOT" and cr["AllocationStrategy"] == "SPOT_PRICE_CAPACITY_OPTIMIZED"
        assert cr["MinvCpus"] == 0 and "SecurityGroupIds" not in cr
    p5 = ces["rt-h100-p5-48xl-8g-32c-ce"]["ComputeResources"]
    assert p5["MaxvCpus"] == 192 and p5["InstanceTypes"] == ["p5.48xlarge"]
    assert sorted(s["Ref"] for s in p5["Subnets"]) == sorted(
        k for k in private if "Az2" in k or "Az4" in k)
    g6 = ces["rt-l4-g6-1xl-1g-1c-ce"]["ComputeResources"]
    assert not any(s["Ref"] in private for s in g6["Subnets"]) and len(g6["Subnets"]) == 3
    # without egress a multi-card node keeps the public subnets
    t = region_template(egress=False)
    p5 = by_name(t, "AWS::Batch::ComputeEnvironment",
                 "ComputeEnvironmentName")["rt-h100-p5-48xl-8g-32c-ce"]["ComputeResources"]
    assert not any("Private" in s["Ref"] for s in p5["Subnets"])


def test_efa_refused_region():
    t = region_template(region="ap-northeast-2")
    lts = by_name(t, "AWS::EC2::LaunchTemplate", "LaunchTemplateName")
    assert set(lts) == {"rt-b300-p6-48xl-8g-1c", "rt-b300-p6-48xl-8g-16c-noefa"}


# --------------------------------------------------------------------------------- jobs
def test_job_definitions():
    t = region_template()
    jds = by_name(t, "AWS::Batch::JobDefinition", "JobDefinitionName")
    bench = {n for n in jds if n.startswith("bench-gpu-")}
    assert bench == {"bench-gpu-h100-p5-48xl-8g-32c-hopper-jd", "bench-gpu-l4-g6-1xl-1g-1c-hopper-jd",
                     "bench-gpu-b300-p6-48xl-8g-16c-blackwell-jd",
                     "bench-gpu-b300-p6-48xl-8g-16c-noefa-blackwell-jd",
                     "bench-gpu-b200-p6-48xl-8g-8c-blackwell-jd"}
    assert {n for n in jds if n.startswith("sft-")} == {
        "sft-1gpu-hopper-jd", "sft-1gpu-blackwell-jd", "sft-8gpu-hopper-jd",
        "sft-8gpu-blackwell-jd", "sft-1gpu-g-hopper-jd", "sft-2gpu-g-hopper-jd"}
    b = jds["bench-gpu-h100-p5-48xl-8g-32c-hopper-jd"]["ContainerProperties"]
    assert [r["Value"] for r in b["ResourceRequirements"]] == ["8", "180", str(int(2048 * 1024 * 0.9))]
    # bench runs the fleet runtime image; sft runs the training image
    assert "gpu-fleet-hopper:latest" in json.dumps(b["Image"])
    env = {e["Name"]: e["Value"] for e in b["Environment"]}
    assert env["TRAIN_REQUIRE_NVME"] == "1" and env["BENCH_SHAPE"] == "h100-p5-48xl-8g-32c"
    assert env["GPU_FLEET_HOME_REGION"] == "us-east-1" and "GPU_FLEET_WEIGHT_BUCKET" not in env
    assert b["MountPoints"][0]["ContainerPath"] == "/mnt/nvme"
    s = jds["sft-8gpu-blackwell-jd"]
    assert "gpu-train-blackwell:latest" in json.dumps(s["ContainerProperties"]["Image"])
    assert s["ContainerProperties"]["LinuxParameters"]["SharedMemorySize"] == 65536
    assert s["Timeout"]["AttemptDurationSeconds"] == 86400 and s["RetryStrategy"]["Attempts"] == 1
    assert "LinuxParameters" not in jds["sft-1gpu-hopper-jd"]["ContainerProperties"]
    t = region_template(bench_job_definitions=False, training_job_definitions=False)
    t.resource_count_is("AWS::Batch::JobDefinition", 0)


def test_self_managed_terminator():
    t = region_template()
    t.has_resource_properties("AWS::Lambda::Function", {
        "Environment": {"Variables": Match.object_like({"TAG_KEY": cat.HUNT_TAG_KEY,
                                                        "MAX_NODE_HOURS": "24"})}})
    t.resource_count_is("AWS::Events::Rule", 1)
    t = region_template(raw_ec2=False)
    t.resource_count_is("AWS::Lambda::Function", 0)


# ------------------------------------------------------------------------------ weights
WEIGHTS = "amzn-s3-demo-bucket-weights"


def test_weight_replica_only_outside_the_home_region():
    region_template(weight_bucket=WEIGHTS).resource_count_is("AWS::S3::Bucket", 0)
    t = region_template("ap-northeast-2", weight_bucket=WEIGHTS)
    t.has_resource("AWS::S3::Bucket", {
        "Properties": {"BucketName": f"{WEIGHTS}-ap-northeast-2",
                       "VersioningConfiguration": {"Status": "Enabled"}},
        "DeletionPolicy": "Retain"})
    region_template("ap-northeast-2").resource_count_is("AWS::S3::Bucket", 0)
    jds = by_name(t, "AWS::Batch::JobDefinition", "JobDefinitionName")
    for jd in jds.values():
        env = {e["Name"]: e["Value"] for e in jd["ContainerProperties"]["Environment"]}
        assert env["GPU_FLEET_WEIGHT_BUCKET"] == WEIGHTS


def test_weight_source_replicates_to_every_other_region():
    stack = GpuFleetWeightsStack(
        core.App(), "gpu-weights", env=core.Environment(account=ACCOUNT, region="us-east-1"),
        config=GpuFleetWeightsConfig(source_bucket=WEIGHTS, home_region="us-east-1",
                                     regions=["us-east-1", "us-west-2", "ap-northeast-2"]))
    t = Template.from_stack(stack)
    src = next(iter(t.find_resources("AWS::S3::Bucket").values()))["Properties"]
    assert src["BucketName"] == WEIGHTS
    rules = src["ReplicationConfiguration"]["Rules"]
    assert [(r["Id"], r["Priority"]) for r in rules] == [("to-us-west-2", 1),
                                                         ("to-ap-northeast-2", 2)]
    assert f":s3:::{WEIGHTS}-us-west-2" in json.dumps(rules[0]["Destination"])
    assert all(r["DeleteMarkerReplication"] == {"Status": "Disabled"} for r in rules)
    with pytest.raises(ValueError, match="63"):
        replica_bucket_name("x" * 50, "ap-southeast-3")


# ------------------------------------------------------------------------------- global
def global_template(**overrides) -> Template:
    kw = dict(namespace="test", home_region="us-east-1", regions=["us-east-1", "us-west-2"],
              bucket_arns=["arn:aws:s3:::amzn-s3-demo-bucket/"],
              weight_replica_bucket_prefix="amzn-s3-demo-bucket-weights",
              replication_prefix="test-gpu-train-")
    kw.update(overrides)
    stack = GpuFleetGlobalStack(core.App(), "gpu-global", config=GpuFleetGlobalConfig(**kw),
                                env=core.Environment(account=ACCOUNT, region="us-east-1"))
    return Template.from_stack(stack)


def _statements(t: Template, role_name: str) -> dict:
    roles = {k for k, r in t.find_resources("AWS::IAM::Role").items()
             if r["Properties"].get("RoleName") == role_name}
    out = {}
    for p in t.find_resources("AWS::IAM::Policy").values():
        if any(r["Ref"] in roles for r in p["Properties"]["Roles"]):
            for s in p["Properties"]["PolicyDocument"]["Statement"]:
                out[s.get("Sid")] = s
    return out


def test_job_role_grants():
    st = _statements(global_template(), "test-gpu-job")
    assert {"Data", "WeightReplicas", "WeightStage", "BatchChain", "BatchRead", "Ec2Read", "Ec2TerminateHuntNodes",
            "SsmRead", "SsmSendCommandHuntNodes", "BedrockInvoke", "BedrockCustomize",
            "BedrockPassCustomizationRole", "SpotQuota", "Logs"} <= set(st)
    assert "s3:DeleteObject" in st["Data"]["Action"]
    assert st["Data"]["Resource"] == ["arn:aws:s3:::amzn-s3-demo-bucket",
                                      "arn:aws:s3:::amzn-s3-demo-bucket/*"]
    assert st["Ec2TerminateHuntNodes"]["Condition"] == {
        "StringEquals": {f"ec2:ResourceTag/{cat.HUNT_TAG_KEY}": cat.HUNT_TAG_VALUE}}
    assert ":s3:::amzn-s3-demo-bucket-weights/*" in json.dumps(st["WeightStage"]["Resource"])
    assert ":s3:::amzn-s3-demo-bucket-weights\"" in json.dumps(st["WeightReplicas"]["Resource"])
    actions = json.dumps([s["Action"] for s in st.values()])
    for forbidden in ("ec2:RunInstances", "ec2:CreateFleet", "s3tables"):
        assert forbidden not in actions


def test_hunter_policy_and_replication():
    t = global_template()
    t.has_resource_properties("AWS::IAM::ManagedPolicy", {"ManagedPolicyName": "test-gpu-hunter"})
    t.has_resource_properties("AWS::IAM::InstanceProfile", {"InstanceProfileName": "test-gpu-node"})
    t.has_resource_properties("AWS::ECR::ReplicationConfiguration", {
        "ReplicationConfiguration": {"Rules": [{
            "Destinations": [{"Region": "us-west-2", "RegistryId": ACCOUNT}],
            "RepositoryFilters": [{"Filter": "test-gpu-train-", "FilterType": "PREFIX_MATCH"}]}]}})
    t = global_template(ecr_replication=False, raw_ec2=False)
    t.resource_count_is("AWS::ECR::ReplicationConfiguration", 0)
    t.resource_count_is("AWS::IAM::ManagedPolicy", 0)


# ----------------------------------------------------------------------------- pipeline
def test_image_build_spec_defaults_and_build_args():
    plain = ImageBuildSpec(id="x", repository="x", dockerfile="D", context=".", branch="main")
    assert plain.docker_build_command() == (
        "docker build -f $DOCKERFILE --build-arg CODE_VERSION=$CODE_VERSION "
        "-t $ECR_REPOSITORY_URI:$CODE_VERSION $BUILD_CONTEXT")
    assert (plain.compute_type, plain.timeout_minutes) == ("MEDIUM", 30)
    bw = ImageBuildSpec.from_params({"id": "x", "repository": "x", "dockerfile": "D",
                                     "context": ".", "branch": "main", "arch": "blackwell",
                                     "build_args": ["CUDA_ARCHS=9.0;10.0"]})
    assert "--build-arg 'CUDA_ARCHS=9.0;10.0'" in bw.docker_build_command()
