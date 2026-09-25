# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Batch job definitions for the fleet, declared here and nowhere else.

Submitters name a job definition and override the command; they never register one.
A document registered by a script is a revision nobody can attribute to a deploy, and
two scripts registering the "same" document drift apart one field at a time.

bench-gpu-<shape>-<arch>-jd   one per template that has a queue in the region: the whole
                              node less the ECS agent's slice, `bench_gpu` from the fleet
                              runtime image (gpu_fleet/runtime)
sft-<size>-<arch>-jd          fine-tuning / scoring: 1gpu and 8gpu on hopper + blackwell,
                              1gpu-g and 2gpu-g (L4 / L40S nodes) on hopper

The arch is the chip's, never a choice. A Hopper image (sm_80/sm_90) on a Blackwell node
reports zero devices while the driver sees eight and nothing raises, so that pairing is
never declared.

Bench documents run the fleet runtime image; sft documents run the training image built FROM
it. Images are `:latest` of the region-local replica of the arch's repository. The ECS agent
pulls on every job start, so a new build reaches the next job without a deploy.

Every document mounts the NVMe pool the launch template builds at /mnt/nvme at the same
path and sets TRAIN_REQUIRE_NVME=1: a node without the pool fails in its first second
instead of slowly filling its root volume.
"""
from typing import Dict, List

from aws_cdk import CfnOutput, Stack, aws_batch as batch
from constructs import Construct

from . import catalogue as cat
from .fleet import GpuFleet

P = batch.CfnJobDefinition


def _pool_mount():
    return dict(
        volumes=[P.VolumesProperty(name="nvmepool",
                                   host=P.VolumesHostProperty(source_path=cat.NVME_POOL))],
        mount_points=[P.MountPointsProperty(source_volume="nvmepool",
                                            container_path=cat.NVME_POOL, read_only=False)])


def _env(pairs: Dict[str, str]) -> List[P.EnvironmentProperty]:
    return [P.EnvironmentProperty(name=k, value=v) for k, v in pairs.items()]


def _resources(gpus: int, vcpu: int, memory_mib: int) -> List[P.ResourceRequirementProperty]:
    return [P.ResourceRequirementProperty(type="GPU", value=str(gpus)),
            P.ResourceRequirementProperty(type="VCPU", value=str(vcpu)),
            P.ResourceRequirementProperty(type="MEMORY", value=str(memory_mib))]


class FleetJobDefinitions(Construct):
    def __init__(self, scope: Construct, construct_id: str, *, fleet: GpuFleet, prefix: str,
                 images: Dict[str, str], job_role_arn: str, execution_role_arn: str,
                 home_region: str, bench_images: Dict[str, str] = None,
                 weight_bucket: str = None, bench: bool = True, training: bool = True,
                 tags: Dict[str, str] = None) -> None:
        """`images` maps arch (hopper / blackwell) -> training image URI in this region,
        `bench_images` the same for the runtime image (default: `images`)."""
        super().__init__(scope, construct_id)
        tags = dict(tags or {})
        self.job_definitions: Dict[str, P] = {}
        pool = cat.NVME_POOL
        #: data buckets live in the home region; the job reads its placement region from task
        #: metadata, never from this variable
        base_env = {"AWS_DEFAULT_REGION": home_region, "GPU_FLEET_HOME_REGION": home_region}
        if weight_bucket:
            #: the SOURCE bucket; the image resolves the region-local replica itself
            base_env["GPU_FLEET_WEIGHT_BUCKET"] = weight_bucket
        bench_images = bench_images or images

        def declare(name: str, container: P.ContainerPropertiesProperty, timeout_s: int):
            self.job_definitions[name] = P(
                self, "Jd" + name.replace("-", ""), job_definition_name=name,
                type="container", platform_capabilities=["EC2"],
                # attempts 1: a retry restarts the work from scratch, and a spot reclaim is
                # better answered by racing another queue than by waiting in this one
                retry_strategy=P.RetryStrategyProperty(attempts=1),
                timeout=P.TimeoutProperty(attempt_duration_seconds=timeout_s),
                container_properties=container, tags=tags)

        if bench:
            for tpl, shape in sorted(fleet.shapes.items()):
                if tpl not in fleet.queues:
                    continue   # template-only: no queue to submit to
                rr = cat.bench_resources(shape.itype)
                declare(cat.bench_job_definition_name(shape, prefix), P.ContainerPropertiesProperty(
                    image=bench_images[shape.arch], job_role_arn=job_role_arn,
                    execution_role_arn=execution_role_arn, command=["bench_gpu"],
                    resource_requirements=_resources(rr["gpus"], rr["vcpu"], rr["memory_mib"]),
                    environment=_env({**base_env, "TRAIN_NVME_POOL": pool, "TRAIN_SCRATCH": pool,
                                      "TRAIN_REQUIRE_NVME": "1", "PYTHONUNBUFFERED": "1",
                                      # recorded beside the job's own measurements
                                      "BENCH_SHAPE": tpl[len(prefix) + 1:]}),
                    # NCCL's shared-memory transport needs more than docker's 64 MiB
                    linux_parameters=P.LinuxParametersProperty(shared_memory_size=cat.BENCH_SHM_MIB),
                    **_pool_mount()), cat.BENCH_TIMEOUT_S)

        if training:
            for size, s in cat.SFT_SHAPES.items():
                for arch in s["arches"]:
                    declare(cat.sft_job_definition_name(size, arch), P.ContainerPropertiesProperty(
                        image=images[arch], job_role_arn=job_role_arn,
                        execution_role_arn=execution_role_arn,
                        resource_requirements=_resources(s["gpu"], s["vcpu"], s["memory"]),
                        environment=_env({**base_env, "HF_HOME": "/tmp/hf",
                                          "TRAIN_BASE_WEIGHT_CACHE": pool, "TRAIN_SCRATCH": pool,
                                          "TRAIN_NVME_POOL": pool, "TRAIN_REQUIRE_NVME": "1",
                                          # liveness is the per-step log line; tqdm buffers it away
                                          "TRAIN_DISABLE_TQDM": "1", "TRAIN_LOGGING_STEPS": "1",
                                          "PYTHONUNBUFFERED": "1"}),
                        linux_parameters=(P.LinuxParametersProperty(shared_memory_size=s["shm_mib"])
                                          if s["shm_mib"] else None),
                        **_pool_mount()), cat.SFT_TIMEOUT_S)

        CfnOutput(self, "JobDefinitionCount", value=str(len(self.job_definitions)))


def image_uri(stack: Stack, repository: str) -> str:
    """The region-local replica of `repository` at :latest (ECR replication keeps the name)."""
    return f"{stack.account}.dkr.ecr.{stack.region}.{stack.url_suffix}/{repository}:latest"
