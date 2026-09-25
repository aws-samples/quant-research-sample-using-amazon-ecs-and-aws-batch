"""MCP server: what each earnings research Batch queue is for, joined with its live state.

Queue PURPOSES live here (and in the submit-earnings-job skill), never in config or CDK:
infrastructure/config/parameters.json batch.queues[] only carries deployable shape, keyed by
a fixed `id`. This registry is keyed by the same ids; `config_key` names the setting in the
sample's config.json that holds the deployed queue's name.

Run (stdio):  pip install -r mcp/requirements.txt && python mcp/server.py
"""
from typing import Any, Dict, List, Optional

import settings

QUEUE_REGISTRY: Dict[str, Dict[str, Any]] = {
    "cpu": {
        "config_key": "job_queue",
        "purpose": "Shared CPU queue for every array job of the program.",
        "consumers": [
            "basket_study: plan -> eval-event array, aggregate --mark auto array, aggregate --finalize",
            "basket_study: plan-sentiment -> eval-model array",
            "sentiment_analysis: plan -> eval-child array",
            "market_data: plan -> fetch-event array; refresh_universe.py --apply -> plan",
            "content_pipeline: build-manifest, fetch, backfill-scan, backfill-fetch (submitted by hand)",
        ],
        "why_capped": None,
        "egress": "Market-data vendor APIs and press-release hosts are on the internet; "
                  "instances stay private and reach them through the NAT gateway.",
    },
    "capped": {
        "config_key": "alpaca_job_queue",
        "purpose": "Per-ticker history fetches against the market-data vendor API.",
        "consumers": ["market_data: fetch_xle_history.py (one job per ticker)"],
        "why_capped": "Its own compute environment with a small maxv_cpus: the vCPU cap is a "
                      "hard bound on concurrent jobs, which keeps the vendor's per-key rate "
                      "limit from being exceeded. Never point uncapped work at it.",
        "egress": "Vendor API over the internet through the NAT gateway.",
    },
}


def _queue_name(queue_id: str) -> Optional[str]:
    try:
        return settings.get("batch", QUEUE_REGISTRY[queue_id]["config_key"])
    except settings.SettingsError:
        return None


def describe_queues(batch_client=None) -> List[Dict[str, Any]]:
    """Every registered queue: purpose, consumers, cap and egress, plus live Batch
    state (status, compute environment vCPU cap, pricing) when the queue exists."""
    if batch_client is None:
        import boto3
        batch_client = boto3.client("batch", region_name=settings.get("aws", "region"))

    names = {qid: _queue_name(qid) for qid in QUEUE_REGISTRY}
    live_queues = {}
    wanted = [n for n in names.values() if n]
    if wanted:
        for q in batch_client.describe_job_queues(jobQueues=wanted).get("jobQueues", []):
            live_queues[q["jobQueueName"]] = q

    ce_arns = sorted({ce["computeEnvironment"]
                      for q in live_queues.values()
                      for ce in q.get("computeEnvironmentOrder", [])})
    live_ces = {}
    if ce_arns:
        for ce in batch_client.describe_compute_environments(
                computeEnvironments=ce_arns).get("computeEnvironments", []):
            live_ces[ce["computeEnvironmentArn"]] = ce

    out = []
    for qid, entry in QUEUE_REGISTRY.items():
        name = names[qid]
        row: Dict[str, Any] = {"id": qid, "job_queue": name,
                               **{k: v for k, v in entry.items()}}
        queue = live_queues.get(name) if name else None
        if name is None:
            row["live"] = {"error": f"config.json batch.{entry['config_key']} is not set"}
        elif queue is None:
            row["live"] = {"error": f"job queue {name} not found (not deployed?)"}
        else:
            ces = [live_ces.get(o["computeEnvironment"], {})
                   for o in queue.get("computeEnvironmentOrder", [])]
            row["live"] = {
                "state": queue.get("state"),
                "status": queue.get("status"),
                "compute_environments": [
                    {"name": ce.get("computeEnvironmentName"),
                     "state": ce.get("state"),
                     "status": ce.get("status"),
                     "type": ce.get("computeResources", {}).get("type"),
                     "maxv_cpus": ce.get("computeResources", {}).get("maxvCpus"),
                     "desiredv_cpus": ce.get("computeResources", {}).get("desiredvCpus")}
                    for ce in ces],
            }
        out.append(row)
    return out


def main() -> None:
    from mcp.server.fastmcp import FastMCP

    server = FastMCP("earnings-research")

    @server.tool(name="describe_queues")
    def _describe_queues() -> List[Dict[str, Any]]:
        """What each earnings research Batch job queue is for (purpose, consumers,
        why it is capped, egress) joined with its live state and vCPU cap."""
        return describe_queues()

    server.run()


if __name__ == "__main__":
    main()
