# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""MCP server: why the GPU fleet preset is what it is, what it builds where, and its live state.

The REASONS behind the `open-weight-fine-tuning` preset live here (and in the deploy-gpu-fleet
skill), never in parameters.json or the CDK: the config only selects, the catalogue only
describes. Two tools answer offline from the catalogue and the committed offerings snapshot;
one reads Batch.

  describe_preset    the preset, why each region and family is in it, per-region state counts
  describe_shapes    every template / queue / bench job definition, filtered by region or family
  describe_queues    one region's fleet queues joined with their live state and compute env

Run (stdio), from infrastructure/:
    pip install -r gpu_fleet/mcp/requirements.txt && python gpu_fleet/mcp/server.py
"""
import json
import pathlib
import sys
from collections import Counter
from typing import Any, Dict, List, Optional

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))

from gpu_fleet import catalogue as cat  # noqa: E402

PARAMETERS = HERE.parents[1] / "config" / "parameters.json"

PRESET_REASONS: Dict[str, Dict[str, Any]] = {
    "open-weight-fine-tuning": {
        "purpose": "LoRA / QLoRA / full fine-tuning and scoring of open-weight models from "
                   "1 GPU up to one whole 8-GPU node, on the cheapest capacity that places.",
        "why_spot_only": "On-demand P-family capacity is rarely available without a capacity "
                         "reservation; spot places, at a fraction of the price. A fine-tuning "
                         "job checkpoints to the data volume and S3, so a reclaim costs the "
                         "steps since the last checkpoint, not the run.",
        "why_all_regions": "Spot capacity for 8-GPU nodes is per AZ and moves between regions by "
                           "the hour. A job raced across every region that sells the type "
                           "places; a job waiting in one region's queue can sit RUNNABLE for "
                           "hours. Breadth IS the capacity strategy, and an idle region costs "
                           "nothing: every compute environment scales to 0 vCPU.",
        "why_one_template_per_shape": "A launch template differs from its siblings only by GPU "
                                      "chip and network-card count; disks, user data, IMDS and "
                                      "tags are fixed in the catalogue. Batch and raw EC2 launch "
                                      "the SAME template, so a node behaves the same either way.",
        "why_raw_ec2": "Batch cannot race regions. The raw-EC2 path launches spot nodes from the "
                       "fleet templates in every region at once, keeps the first and cancels the "
                       "rest; the per-region terminator bounds how long any such node lives.",
    },
}

FAMILY_REASONS: Dict[str, str] = {
    "h100": "The default for 8-GPU fine-tuning: widest spot availability of the 80 GB class.",
    "h200": "141 GB per GPU: models whose optimizer state does not fit 8 x 80 GB.",
    "b200": "Blackwell, ~180 GB per GPU: the largest models on one node; needs the blackwell image.",
    "b300": "Blackwell Ultra, 16 network cards; the regions that refuse its EFA interface get a "
            "one-card template, and every region also gets a 16-card no-EFA template.",
    "a100": "Older and cheaper; enough for 7-30B LoRA and for scoring.",
    "l4": "24 GB G-family: small-GPU scoring and small-model LoRA, drawn on the G spot quota "
          "instead of the P quota.",
    "l40s": "48 GB G-family: 1-8 GPU scoring and mid-size LoRA on the G quota.",
}


def _fleet_params() -> dict:
    return json.loads(PARAMETERS.read_text())["gpu_fleet"]


def _preset(name: Optional[str] = None) -> tuple:
    p = _fleet_params()
    name = name or p["preset"]
    return name, p["presets"][name], p


def _selected(preset: dict) -> List[str]:
    return cat.select(preset.get("families", ()), preset.get("instance_types", ()))


def describe_preset(name: Optional[str] = None, offerings: Optional[dict] = None) -> Dict[str, Any]:
    """The preset, the reasons for it, and what it builds per region (from the snapshot)."""
    name, preset, params = _preset(name)
    offerings = offerings if offerings is not None else cat.load_offerings()
    types = _selected(preset)
    regions = {}
    for region in preset["regions"]:
        excl = tuple(params.get("exclude_az_ids", {}).get(region, ()))
        states = Counter(cat.region_state(cat.INSTANCE_TYPES[t], region, offerings, excl)[0]
                         for t in types)
        regions[region] = dict(sorted(states.items()))
    return {"preset": name, "active": name == params["preset"], "doc": preset.get("_doc"),
            **PRESET_REASONS.get(name, {}),
            "families": {f: {"gpu": cat.FAMILIES[f].gpu_name, "arch": cat.FAMILIES[f].arch,
                             "spot_quota": "P" if cat.FAMILIES[f].p_family else "G",
                             "data_volume_gib": cat.FAMILIES[f].data_gib,
                             "why": FAMILY_REASONS.get(f)}
                         for f in preset.get("families", ())},
            "instance_types": types,
            "regions": regions,
            "raw_ec2": preset.get("raw_ec2", True)}


def describe_shapes(region: Optional[str] = None, family: Optional[str] = None,
                    offerings: Optional[dict] = None) -> List[Dict[str, Any]]:
    """One row per (region, template): names, state, AZ IDs, GPUs, cards, EFA, arch."""
    _, preset, params = _preset()
    offerings = offerings if offerings is not None else cat.load_offerings()
    prefix = params.get("name_prefix", "rt")
    raw_ec2 = preset.get("raw_ec2", True)
    rows = []
    for r in ([region] if region else preset["regions"]):
        excl = tuple(params.get("exclude_az_ids", {}).get(r, ()))
        for itype in _selected(preset):
            t = cat.INSTANCE_TYPES[itype]
            if family and t.chip != family:
                continue
            state, azs = cat.region_state(t, r, offerings, excl)
            if state == cat.ABSENT or (state == cat.TEMPLATE_ONLY and not raw_ec2):
                continue
            for s in cat.shapes_in(t, r):
                queued = state != cat.TEMPLATE_ONLY
                rows.append({
                    "region": r, "instance_type": itype, "launch_template": s.name(prefix),
                    "state": state, "az_ids": list(azs), "gpus": t.gpus,
                    "gpu_mem_gib": t.gpu_mem_gib, "cards": s.cards, "efa": s.efa,
                    "arch": s.arch,
                    "job_queue": s.queue_name(prefix) if queued else None,
                    "bench_job_definition": (cat.bench_job_definition_name(s, prefix)
                                             if queued and preset.get("bench_job_definitions", True)
                                             else None)})
    return rows


def describe_queues(region: str, batch_client=None,
                    offerings: Optional[dict] = None) -> List[Dict[str, Any]]:
    """The region's fleet queues (from describe_shapes) joined with live Batch state."""
    rows = [r for r in describe_shapes(region, offerings=offerings) if r["job_queue"]]
    if batch_client is None:
        import boto3
        batch_client = boto3.client("batch", region_name=region)
    names = [r["job_queue"] for r in rows]
    live = {}
    for i in range(0, len(names), 100):
        for q in batch_client.describe_job_queues(jobQueues=names[i:i + 100]).get("jobQueues", []):
            live[q["jobQueueName"]] = q
    ce_arns = sorted({o["computeEnvironment"] for q in live.values()
                      for o in q.get("computeEnvironmentOrder", [])})
    ces = {}
    for i in range(0, len(ce_arns), 100):
        for ce in batch_client.describe_compute_environments(
                computeEnvironments=ce_arns[i:i + 100]).get("computeEnvironments", []):
            ces[ce["computeEnvironmentArn"]] = ce
    for row in rows:
        q = live.get(row["job_queue"])
        if q is None:
            row["live"] = {"error": f"job queue {row['job_queue']} not found (not deployed?)"}
            continue
        envs = [ces.get(o["computeEnvironment"], {}) for o in q.get("computeEnvironmentOrder", [])]
        row["live"] = {
            "state": q.get("state"), "status": q.get("status"),
            "compute_environments": [
                {"name": ce.get("computeEnvironmentName"), "state": ce.get("state"),
                 "status": ce.get("status"),
                 "type": ce.get("computeResources", {}).get("type"),
                 "maxv_cpus": ce.get("computeResources", {}).get("maxvCpus"),
                 "desiredv_cpus": ce.get("computeResources", {}).get("desiredvCpus")}
                for ce in envs]}
    return rows


def main() -> None:
    from mcp.server.fastmcp import FastMCP

    server = FastMCP("gpu-fleet")

    @server.tool(name="describe_preset")
    def _describe_preset(name: Optional[str] = None) -> Dict[str, Any]:
        """Why the GPU fleet preset is recommended (spot only, all regions, one template per
        shape, raw EC2), its families and the per-region count of enabled / disabled /
        template-only / absent types."""
        return describe_preset(name)

    @server.tool(name="describe_shapes")
    def _describe_shapes(region: Optional[str] = None, family: Optional[str] = None) -> List[Dict[str, Any]]:
        """Every launch template the fleet builds: region, instance type, state, AZ IDs,
        GPUs, network cards, EFA, image arch, job queue and bench job definition names."""
        return describe_shapes(region, family)

    @server.tool(name="describe_queues")
    def _describe_queues(region: str) -> List[Dict[str, Any]]:
        """One region's fleet job queues joined with their live state and compute
        environment (type, vCPU cap, desired vCPUs)."""
        return describe_queues(region)

    server.run()


if __name__ == "__main__":
    main()
