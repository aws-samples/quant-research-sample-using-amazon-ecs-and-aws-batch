---
name: deploy-gpu-fleet
description: Deploy, extend or inspect the GPU fine-tuning fleet (infrastructure/gpu_fleet) — the recommended open-weight-fine-tuning preset across 18 regions, spot only, one launch template per GPU type x network-card count, CDK-owned job definitions. Use when enabling app_with_gpu_fleet, adding a GPU type or region, refreshing offerings, or picking a queue / bench job definition.
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
   in `s3.custom_arns`; set `gpu_fleet.weight_replica_bucket_prefix` if weights are replicated
   per region (`<prefix>-<region>`).
2. Refresh where types are sold, from `infrastructure/`, with read-only credentials:
   `python gpu_fleet/refresh_offerings.py --probe-batch`. Commit `offerings.json`; synth
   never calls AWS, so the diff is the review.
3. Per-region knobs, only where a deploy failed:
   - `nat_az_id {region: az_id}` — the NAT gateway limit is per AZ; name an AZ with room.
   - `no_egress_regions [region]` — Elastic IP quota exhausted; multi-card nodes then use
     public subnets and have no off-region path.
   - `exclude_az_ids {region: [az_id]}` — an AZ that stopped selling a type.
4. `cdk deploy --all` from `infrastructure/`: one `gpu-fleet-global-stack-*` (IAM, ECR
   replication) in the home region, then one `gpu-fleet-<region>-stack-*` per region. A
   region that fails rolls back alone; fix its knob and redeploy that stack.
5. Images: the pipeline builds `gpu-train-hopper` and `gpu-train-blackwell`; ECR replicates
   them to every other region. Job definitions use `:latest`, so a new build reaches the next
   job without a deploy. Start the first build by hand.

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

## 6. Benchmark a shape

Submit the region's bench job definition to the matching queue (names from `describe_shapes`):

```bash
aws batch submit-job --region <region> --job-name bench \
  --job-queue <prefix>-h100-p5-48xl-8g-32c-queue \
  --job-definition bench-gpu-h100-p5-48xl-8g-32c-hopper-jd
```

The document asks for the whole node less the ECS agent's slice; do not override its
resources.
