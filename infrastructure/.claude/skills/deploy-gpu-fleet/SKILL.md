---
name: deploy-gpu-fleet
description: Deploy, extend or inspect the GPU fine-tuning fleet (infrastructure/gpu_fleet) — the recommended open-weight-fine-tuning preset across 18 regions, spot only, one launch template per GPU type x network-card count, CDK-owned job definitions, the fleet runtime image (weight downloader + bench_gpu), per-region weight buckets and the gpu-race MCP. Use when enabling app_with_gpu_fleet, adding a GPU type or region, refreshing offerings, staging a model, benchmarking a shape, or racing a job / hunting nodes across regions.
---

# Deploy the GPU fine-tuning fleet

The fleet is a fixed catalogue (`gpu_fleet/catalogue.py`: GPU families, instance types,
disks, user data, images) plus a preset in `config/parameters.json` `gpu_fleet` that only
SELECTS families, types and regions. Nothing about a shape is configurable per account.

## 1. Why the preset is what it is — ask the MCP server, do not restate

`python gpu_fleet/mcp/server.py` (stdio; `pip install -r gpu_fleet/mcp/requirements.txt`).

| Tool | Answers |
|---|---|
| `describe_preset` | why spot only, why all 18 regions, why one template per shape, why raw EC2; per family: GPU, image arch, spot quota (P / G), data volume; per region: enabled / disabled / template-only / absent counts |
| `describe_shapes` | every launch template with its state, AZ IDs, GPUs, cards, EFA, arch, job queue and bench job definition (filter by `region`, `family`) |
| `describe_queues` | one region's queues joined with live Batch state and compute environment |

The reasons live only there; keep them there.

## 2. Region states (per instance type, from `gpu_fleet/offerings.json`)

| State | When | Builds |
|---|---|---|
| enabled | Batch accepts the type, >= 1 AZ offers it | template + CE + ENABLED queue |
| disabled | Batch accepts it, no AZ offers it | template + CE + DISABLED queue |
| template-only | offered, Batch does not accept it | template only (raw-EC2 path), only when `raw_ec2` |
| absent | neither | nothing |

Names: `<prefix>-<chip>-<fam>-<size>-<gpus>g-<cards>c[-noefa]`, CE `...-ce`, queue `...-queue`,
bench job definition `bench-gpu-<chip>-...-<arch>-jd`, training `sft-{1gpu,8gpu,1gpu-g,2gpu-g}-<arch>-jd`.

## 3. First deploy (once per account)

1. `config/parameters.json`: `"app_with_gpu_fleet": true`. Put the data buckets the jobs read
   in `s3.custom_arns`. Set `gpu_fleet.weight_replica_bucket_prefix` to a globally unique
   name: with `weight_buckets` on it is the weight SOURCE bucket in the home region, and
   every other region gets `<name>-<region>` (at most 63 characters, so keep the name to 48).
2. Refresh where types are sold, from `infrastructure/`, with read-only credentials:
   `python gpu_fleet/refresh_offerings.py --probe-batch`. Commit `offerings.json`; synth
   never calls AWS, so the diff is the review.
3. Per-region knobs, only where a deploy failed:
   - `nat_az_id {region: az_id}` — the NAT gateway limit is per AZ; name an AZ with room.
   - `no_egress_regions [region]` — Elastic IP quota exhausted; multi-card nodes then use
     public subnets and have no off-region path.
   - `exclude_az_ids {region: [az_id]}` — an AZ that stopped selling a type.
4. `cdk deploy --all` from `infrastructure/`: one `gpu-fleet-global-stack-*` (IAM, ECR
   replication) in the home region, then one `gpu-fleet-<region>-stack-*` per region (VPC,
   templates, queues, job definitions, weight replica), then `gpu-fleet-weights-stack-*`
   (source bucket + one replication rule per region; S3 validates every destination when
   the rule is written, so it deploys last). A region that fails rolls back alone; fix its
   knob and redeploy that stack.
5. Images: the pipeline builds the fleet runtime `gpu-fleet-hopper` / `gpu-fleet-blackwell`
   from `gpu_fleet/runtime/` (section 6); ECR replicates every `<namespace>-gpu-*`
   repository to every other region. The bench job definitions run the runtime image; the
   sft job definitions run `<namespace>-gpu-train-<arch>`, a training image of your own
   built `FROM` the runtime image and pushed to the home region. Job definitions use
   `:latest`, so a new build reaches the next job without a deploy. Start the first build
   by hand.

## 4. Rules that are not negotiable

- P-family is SPOT only. Do not add on-demand compute environments.
- Job definitions ship through CDK (`gpu_fleet/jobs.py`). Submitters override the command;
  they never call `register_job_definition`.
- The arch follows the chip: B200 / B300 run the blackwell image, everything else hopper.
  A hopper image on Blackwell reports zero GPUs without raising.
- Every job writes under `/mnt/nvme` (`TRAIN_REQUIRE_NVME=1`); the data volume is at
  `/mnt/nvme/ebs-data`. Never write to the root volume.
- Multi-card templates have no public IP and no subnet: they launch in the private subnets
  and reach AWS through the endpoints and the NAT.
- Raw-EC2 nodes must carry `rt-gpu:managed-by=hunt` (and optionally `rt-gpu:ttl-hours`);
  the per-region terminator ends them after `max_node_hours`.

## 5. Adding a GPU type or family

Add the row to `INSTANCE_TYPES` (and the family to `FAMILIES`) in `catalogue.py`, a reason
to `FAMILY_REASONS` in the MCP server, run `refresh_offerings.py --probe-batch`, then
`pytest tests/unit/test_gpu_fleet.py tests/unit/test_gpu_fleet_mcp.py`. Flag-off synth must
stay byte-identical to `main`.

## 6. The fleet runtime image (`gpu_fleet/runtime/`)

Model transport and the benchmark are part of the fleet, baked into one image per arch
(`Dockerfile`; build context is the runtime directory). It carries the EFA userspace, an
unversioned `libcudart.so` (without it NCCL's EFA plugin silently falls back to TCP), the
`model_transport` package, `local_disk.py` (the NVMe pool resolver) and `bench_gpu.py`.
The entrypoint `gpu-fleet` routes:

| Command | Does |
|---|---|
| `gpu-fleet stage_model <hf_repo> <model_name> [--bucket] [--prefix base_models]` | copies a Hugging Face repo into the weight source bucket (default `$GPU_FLEET_WEIGHT_BUCKET`); replication fans it out |
| `gpu-fleet fetch_model s3://<source>/base_models/<model> <local_dir> [--limit-gib] [--force-source]` | downloads onto the NVMe pool from the region-local replica when its listing is byte-identical to the source's, otherwise from the source; prints a JSON report with Gbps |
| `gpu-fleet bench_gpu --model-uri ... --report-uri s3://...` | records the node: inventory, pool, bf16 TFLOP/s, HBM, NCCL, and replica vs source download A/B; never exits non-zero on a mismatch |
| anything else | exec'd as is, so images built FROM the runtime run their own commands |

Every job gets `GPU_FLEET_HOME_REGION` and, with weight buckets, `GPU_FLEET_WEIGHT_BUCKET`.

S3 replication copies only objects written AFTER the rule exists. Stage models after the
weights stack is deployed; objects already in the source need an S3 Batch Replication job
(`aws s3control create-job ... --manifest-generator` over the source), or a re-stage.

Tests: `cd gpu_fleet/runtime/model_transport && pip install ".[test,stage]" && pytest`
(uses moto), then `cd gpu_fleet/runtime && pytest tests`. Keep the two runs separate: with
`runtime/` on the path the `model_transport` project directory shadows the package.

## 7. Benchmark a shape

Submit the region's bench job definition to the matching queue (names from `describe_shapes`):

```bash
aws batch submit-job --region <region> --job-name bench \
  --job-queue <prefix>-h100-p5-48xl-8g-32c-queue \
  --job-definition bench-gpu-h100-p5-48xl-8g-32c-hopper-jd \
  --container-overrides '{"command": ["bench_gpu", "--model-uri",
    "s3://<weight-source>/base_models/<model>", "--report-uri", "s3://<bucket>/bench_gpu"]}'
```

The document asks for the whole node less the ECS agent's slice; do not override its
resources. To bench whichever region has capacity first, race it (section 8).

## 8. Race across regions — the `gpu-race` MCP server

Spot capacity for a multi-GPU node moves between regions by the hour, so the fleet gets
capacity by racing every region and yielding the losers. From `infrastructure/`:
`pip install -r gpu_fleet/race/requirements.txt && python -m gpu_fleet.race.server` (stdio).
Set `GPU_FLEET_HOME_REGION` (or `AWS_REGION`) to the deploy region and `NAMESPACE` if not the
default.

| Tool | Does |
|---|---|
| `race_job` | submits the same job to the type's queue in every ENABLED region (default job definition: the shape's bench; pass e.g. `sft-8gpu-hopper-jd` and a `command`) |
| `race_status` | polls; the first copy Batch places (STARTING or later) wins, every other copy is terminated; `wait_seconds` keeps polling |
| `cancel_race` | withdraws every unfinished copy |
| `hunt_nodes` | raw EC2: `count` spot nodes in ONE AZ from the fleet's own launch templates, all regions at once; first fill wins, partial fills and photo-finish losers are terminated at once |
| `run_on_nodes` / `node_status` | runs the runtime image (or `image`) on every hunted node over SSM, logs to `/<namespace>/gpu-fleet/nodes`, one stream per node |
| `release_nodes` | terminates a hunt's nodes (the terminator also ends them after `ttl_hours`) |
| `list_races` | recent races and hunts |

Records live in `~/.gpu-fleet/races` (`GPU_RACE_STATE_DIR`), so a race outlives the
session; nothing waits in the background, so poll. Raw-EC2 tools need the
`<namespace>-gpu-hunter` managed policy; every hunted node carries the hunt, ttl and
`rt-gpu:race` tags. Tests: `pytest tests/unit/test_gpu_race.py`.
