# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_cdk import App, Environment, aws_ec2 as ec2

from batch.base_construct import S3BucketArnConfig, BatchJobDeploymentType
from batch.cpu_queues import CpuQueueSpec
from batch.earnings_research import EarningsResearchConfig, EarningsResearchStack
from batch.multi_node_with_gpu import (
    BatchJobConfigForMultiNodeWithGPU,
    BatchJobConfigForNodeWithGPU,
    BatchJobStackForMultiNodeWithGPU,
)
from batch.single_node_with_cpu import (
    BatchJobConfigForSingleNodeWithCPU,
    BatchJobStackForSingleNodeWithCPU,
)
from common.fsx import FSxStack
from common.network import NetworkStack
from common.pipeline import ImageBuildSpec, PipelineConfig, DeploymentPipelineStack
from common.s3 import S3Stack
from gpu_fleet import catalogue as gpu_catalogue
from gpu_fleet.global_stack import GpuFleetGlobalConfig, GpuFleetGlobalStack
from gpu_fleet.region_stack import GpuFleetRegionStack, GpuRegionConfig
from utils import get_stack_name, EnvironmentConfig, load_parameters

# Load environment variables
env_config = EnvironmentConfig.from_env()
env = Environment(account=env_config.aws_account_id, region=env_config.aws_region)
namespace = env_config.namespace

# Initialize the app construct which represents an entire CDK app
app = App()

# Load default parameters from file
params = load_parameters(app)
with_earnings_research = getattr(params, "app_with_earnings_research", False)
with_gpu_fleet = getattr(params, "app_with_gpu_fleet", False)

# Build the networking stack
network = NetworkStack(
    app,
    get_stack_name(namespace=namespace, prefix="network-stack-"),
    description=f"Network infrastructure for the {namespace}",
    env=env,
    namespace=namespace,
    availability_zone=params.availability_zone.name,
    with_s3express=params.app_with_s3express,
    # NAT egress is only needed by the earnings research jobs (vendor APIs, web fetches)
    nat_gateways=(
        params.earnings_research.nat_gateways if with_earnings_research else 0
    ),
)

# Build the S3 storage stack
# Check the availability zone ID for the S3 directory bucket
# It may differ based on your target AWS account
s3_storage = S3Stack(
    app,
    get_stack_name(namespace=namespace, prefix="s3-storage-stack-"),
    description=f"S3 storage infrastructure for the {namespace}",
    env=env,
    namespace=namespace,
    availability_zone_id=params.availability_zone.id,
    s3_object_expiration_in_days=params.s3.object_expiration_in_days,
    with_s3express=params.app_with_s3express,
)

s3_storage.add_dependency(network)

# Build the FSx storage stack
fsx_storage = None
if params.app_with_fsx:
    fsx_storage = FSxStack(
        app,
        get_stack_name(namespace=namespace, prefix="fsx-storage-stack-"),
        description=f"FSx storage infrastructure for the {namespace}",
        env=env,
        namespace=namespace,
        subnet=network.vpc.select_subnets(
            subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
        ).subnets[0],
        security_group=network.security_group,
        data_repository_associations={
            "S3Bucket": {
                "data_repository_path": s3_storage.standard_bucket.s3_url_for_object(),
                "file_system_path": "/scratch",
                "auto_export_on": ["NEW", "CHANGED", "DELETED"],
                "auto_import_on": ["NEW", "CHANGED", "DELETED"],
            }
        },
        per_unit_storage_throughput=params.fsx.per_unit_storage_throughput,
        storage_capacity_gib=params.fsx.storage_capacity_gib,
        deployment_type=params.fsx.deployment_type,
    )

# Build the deployment pipeline
image_builds = [
    ImageBuildSpec.from_params(b) for b in getattr(params, "image_builds", [])
]
if with_earnings_research:
    image_builds.append(
        ImageBuildSpec.from_params(params.earnings_research.image_build)
    )
if with_gpu_fleet:
    image_builds += [
        ImageBuildSpec.from_params(b) for b in params.gpu_fleet.image_builds
    ]

pipeline_config = PipelineConfig(
    namespace=namespace,
    github_owner=env_config.github_owner,
    github_repo=env_config.github_repo,
    github_branch=env_config.github_branch,
    github_token_secret_name=env_config.github_token,
    enable_code_pipeline=params.app_with_codepipeline,
    image_builds=image_builds,
    register_github_credentials=getattr(params, "register_github_credentials", True),
)

deployment_pipeline = DeploymentPipelineStack(
    app,
    get_stack_name(namespace=namespace, prefix="deployment-pipeline-stack-"),
    env=env,
    description=f"Deployment pipeline infrastructure for the {namespace}",
    config=pipeline_config,
)
container_image_uri = deployment_pipeline.ecr_repo.repository_uri


def deploy_single_node_with_cpu():
    """
    Deploy a single node AWS Batch compute with CPU
    """
    single_node_config = BatchJobConfigForSingleNodeWithCPU(
        namespace=namespace,
        vpc=network.vpc,
        security_group=network.security_group,
        s3_bucket_config=S3BucketArnConfig(
            s3_standard_bucket_arn=s3_storage.standard_bucket.bucket_arn,
            s3_express_bucket_arn=(
                s3_storage.express_bucket.attr_arn
                if params.app_with_s3express
                else None
            ),
            custom_s3_arns=params.s3.custom_arns,
        ),
        lustre_fs=fsx_storage.lustre_fs if params.app_with_fsx else None,
        maxv_cpus=params.batch.single_node.maxv_cpus,
        minv_cpus=params.batch.single_node.minv_cpus,
        container_image_uri=container_image_uri,
        container_memory=params.batch.single_node.container_memory,
        container_cpu=params.batch.single_node.container_cpu,
        container_command=params.batch.single_node.container_command,
        instance_classes=params.batch.single_node.instance_classes,
        allocation_strategy=params.batch.single_node.allocation_strategy,
        num_queues=params.batch.single_node.num_queues,
        spot=params.batch.single_node.spot,
    )
    BatchJobStackForSingleNodeWithCPU(
        app,
        get_stack_name(
            namespace=namespace, prefix="batch-job-single-node-with-cpu-stack-"
        ),
        env=env,
        description=f"Batch job single node with CPU infrastructure for the {namespace}",
        config=single_node_config,
    )


def deploy_multi_node_with_gpu():
    """
    Deploy a multi node AWS Batch compute with GPU
    """
    multi_node_config = BatchJobConfigForMultiNodeWithGPU(
        namespace=namespace,
        vpc=network.vpc,
        security_group=network.security_group,
        s3_bucket_config=S3BucketArnConfig(
            s3_standard_bucket_arn=s3_storage.standard_bucket.bucket_arn,
            s3_express_bucket_arn=(
                s3_storage.express_bucket.attr_arn
                if params.app_with_s3express
                else None
            ),
            custom_s3_arns=params.s3.custom_arns,
        ),
        lustre_fs=fsx_storage.lustre_fs if params.app_with_fsx else None,
        maxv_cpus=params.batch.multi_node.maxv_cpus,
        minv_cpus=params.batch.multi_node.minv_cpus,
        main_config=BatchJobConfigForNodeWithGPU(
            container_image_uri=container_image_uri,
            container_memory=params.batch.multi_node.main.container_memory,
            container_cpu=params.batch.multi_node.main.container_cpu,
            container_gpu=params.batch.multi_node.main.container_gpu,
            container_command=params.batch.multi_node.main.container_command,
            start_node_index=params.batch.multi_node.main.start_node_index,
            end_node_index=params.batch.multi_node.main.end_node_index,
        ),
        worker_config=BatchJobConfigForNodeWithGPU(
            container_image_uri=container_image_uri,
            container_memory=params.batch.multi_node.worker.container_memory,
            container_cpu=params.batch.multi_node.worker.container_cpu,
            container_gpu=params.batch.multi_node.worker.container_gpu,
            container_command=params.batch.multi_node.worker.container_command,
            start_node_index=params.batch.multi_node.worker.start_node_index,
            end_node_index=params.batch.multi_node.worker.end_node_index,
        ),
        instance_classes=params.batch.multi_node.instance_classes,
        allocation_strategy=params.batch.multi_node.allocation_strategy,
        spot=params.batch.multi_node.spot,
    )
    BatchJobStackForMultiNodeWithGPU(
        app,
        get_stack_name(
            namespace=namespace, prefix="batch-job-multi-node-with-gpu-stack-"
        ),
        env=env,
        description=f"Batch job multi node with GPU infrastructure for the {namespace}",
        config=multi_node_config,
    )


def deploy_earnings_research():
    """
    Deploy the job queues listed in batch.queues and the job definition of
    samples/earnings_research
    """
    earnings_config = EarningsResearchConfig(
        namespace=namespace,
        vpc=network.vpc,
        security_group=network.security_group,
        s3_bucket_config=S3BucketArnConfig(
            s3_standard_bucket_arn=s3_storage.standard_bucket.bucket_arn,
            s3_express_bucket_arn=(
                s3_storage.express_bucket.attr_arn
                if params.app_with_s3express
                else None
            ),
            custom_s3_arns=params.s3.custom_arns,
        ),
        image_repository=deployment_pipeline.image_repos[
            params.earnings_research.image_build.id
        ],
        queues=[CpuQueueSpec.from_params(q) for q in params.batch.queues],
        job_definition_name=params.earnings_research.job_definition_name,
        secret_names=params.earnings_research.secret_names,
        bedrock_model_patterns=params.earnings_research.bedrock_model_patterns,
        container_cpu=params.earnings_research.container_cpu,
        container_memory=params.earnings_research.container_memory,
    )
    EarningsResearchStack(
        app,
        get_stack_name(namespace=namespace, prefix="batch-earnings-research-stack-"),
        env=env,
        description=f"Earnings research job queues and job definition for the {namespace}",
        config=earnings_config,
    )


# Validate and initiate the Batch deployment
batch_deployment_type = params.batch.deployment_type

if batch_deployment_type == BatchJobDeploymentType.SINGLE_NODE:
    deploy_single_node_with_cpu()
elif batch_deployment_type == BatchJobDeploymentType.MULTI_NODE:
    deploy_multi_node_with_gpu()
else:  # ALL
    deploy_single_node_with_cpu()
    deploy_multi_node_with_gpu()

def deploy_gpu_fleet():
    """
    Deploy the GPU fine-tuning fleet: one global stack in the home region and one
    stack per region of the selected preset (gpu_fleet in parameters.json)
    """
    gpu = params.gpu_fleet
    preset = vars(gpu.presets)[gpu.preset]
    regions = list(preset.regions)
    instance_types = gpu_catalogue.select(
        families=preset.families, instance_types=preset.instance_types
    )
    offerings = gpu_catalogue.load_offerings()
    repositories = {
        b.arch: f"{namespace}-{b.repository}" for b in gpu.image_builds
    }

    global_stack = GpuFleetGlobalStack(
        app,
        get_stack_name(namespace=namespace, prefix="gpu-fleet-global-stack-"),
        env=env,
        description=f"GPU fleet IAM and image replication for the {namespace}",
        config=GpuFleetGlobalConfig(
            namespace=namespace,
            home_region=env.region,
            regions=regions,
            bucket_arns=[s3_storage.standard_bucket.bucket_arn, *params.s3.custom_arns],
            weight_replica_bucket_prefix=gpu.weight_replica_bucket_prefix,
            replication_prefix=f"{namespace}-{gpu.repository_prefix}",
            ecr_replication=gpu.ecr_replication,
            raw_ec2=preset.raw_ec2,
        ),
    )

    nat_az_ids = vars(gpu.nat_az_id)
    exclude_az_ids = vars(gpu.exclude_az_ids)
    for region in regions:
        stack = GpuFleetRegionStack(
            app,
            get_stack_name(namespace=namespace, prefix=f"gpu-fleet-{region}-stack-"),
            env=Environment(account=env.account, region=region),
            description=f"GPU fleet in {region} for the {namespace}",
            config=GpuRegionConfig(
                namespace=namespace,
                home_region=env.region,
                prefix=gpu.name_prefix,
                instance_types=instance_types,
                offerings=offerings,
                vpc_cidr=gpu.vpc_cidr,
                repositories=repositories,
                instance_profile_name=global_stack.node_role_name,
                job_role_name=global_stack.job_role_name,
                execution_role_name=global_stack.execution_role_name,
                nat_az_id=nat_az_ids.get(region),
                egress=region not in gpu.no_egress_regions,
                exclude_az_ids=tuple(exclude_az_ids.get(region, [])),
                raw_ec2=preset.raw_ec2,
                bench_job_definitions=preset.bench_job_definitions,
                training_job_definitions=preset.training_job_definitions,
                priority=gpu.queue_priority,
                max_node_hours=gpu.max_node_hours,
            ),
        )
        stack.add_dependency(global_stack)


if with_earnings_research:
    deploy_earnings_research()

if with_gpu_fleet:
    deploy_gpu_fleet()

app.synth()
