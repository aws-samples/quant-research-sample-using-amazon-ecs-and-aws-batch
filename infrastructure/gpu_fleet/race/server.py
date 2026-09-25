# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""MCP server `gpu-race`: get GPU capacity by racing every fleet region, then yield the losers.

Two paths, both over the deployed fleet (`app_with_gpu_fleet`):

  Batch     race_job -> race_status (poll; withdraws the losers once one copy is placed)
            -> cancel_race
  raw EC2   hunt_nodes -> run_on_nodes -> node_status -> release_nodes

Race and hunt records live in ~/.gpu-fleet/races (GPU_RACE_STATE_DIR overrides), so a race
survives the session that started it. Credentials: the standard chain; raw-EC2 tools need
the `<namespace>-gpu-hunter` managed policy. Set GPU_FLEET_HOME_REGION (or AWS_REGION) to
the region the fleet was deployed from, and NAMESPACE if it is not the default.

Run (stdio), from infrastructure/:
    pip install -r gpu_fleet/race/requirements.txt && python -m gpu_fleet.race.server
"""
import time
from typing import Any, Dict, List, Optional

from . import common, hunt, racer, runner

POLL_S = 30
MAX_WAIT_S = 900


def _fleet():
    return common.Fleet.load()


def race_job(instance_type: str, command: Optional[List[str]] = None,
             job_definition: Optional[str] = None, environment: Optional[Dict[str, str]] = None,
             regions: Optional[List[str]] = None, deadline_minutes: Optional[float] = None,
             clients=None) -> Dict[str, Any]:
    rec = racer.start(_fleet(), clients or common.boto3_clients(), instance_type,
                      command=command, job_definition=job_definition, environment=environment,
                      regions=regions, deadline_minutes=deadline_minutes)
    return racer.summary(rec)


def race_status(race_id: str, wait_seconds: int = 0, clients=None,
                sleep=time.sleep) -> Dict[str, Any]:
    clients = clients or common.boto3_clients()
    rec = racer.poll(common.load(race_id), clients)
    waited = 0
    while rec["state"] == "racing" and waited < min(wait_seconds, MAX_WAIT_S):
        sleep(POLL_S)
        waited += POLL_S
        rec = racer.poll(rec, clients)
    return racer.summary(rec)


def cancel_race(race_id: str, clients=None) -> Dict[str, Any]:
    return racer.summary(racer.cancel(common.load(race_id), clients or common.boto3_clients()))


def hunt_nodes(instance_type: str, count: int = 1, regions: Optional[List[str]] = None,
               ttl_hours: Optional[float] = None, dry_run: bool = False,
               clients=None) -> Dict[str, Any]:
    return hunt.hunt(_fleet(), clients or common.boto3_clients(), instance_type, count=count,
                     regions=regions, ttl_hours=ttl_hours, dry_run=dry_run)


def run_on_nodes(hunt_id: str, command: List[str], name: str = "gpu-fleet",
                 environment: Optional[Dict[str, str]] = None, image: Optional[str] = None,
                 clients=None) -> Dict[str, Any]:
    return runner.run(_fleet(), clients or common.boto3_clients(), common.load(hunt_id), command,
                      name=name, environment=environment, image=image)


def node_status(hunt_id: str, command_id: Optional[str] = None, clients=None) -> Dict[str, Any]:
    return runner.status(common.load(hunt_id), clients or common.boto3_clients(), command_id)


def release_nodes(hunt_id: str, clients=None) -> Dict[str, Any]:
    rec = hunt.release(common.load(hunt_id), clients or common.boto3_clients())
    return {"id": rec["id"], "state": rec["state"], "winner": rec.get("winner")}


def main() -> None:
    from mcp.server.fastmcp import FastMCP

    server = FastMCP("gpu-race")

    @server.tool(name="race_job")
    def _race_job(instance_type: str, command: Optional[List[str]] = None,
                  job_definition: Optional[str] = None,
                  environment: Optional[Dict[str, str]] = None,
                  regions: Optional[List[str]] = None,
                  deadline_minutes: Optional[float] = None) -> Dict[str, Any]:
        """Submit the same Batch job to the instance type's fleet queue in every region where
        it is enabled. job_definition defaults to the shape's bench job definition (e.g. pass
        sft-8gpu-hopper-jd for training); command overrides the job definition's. Returns the
        race id; poll it with race_status, which withdraws the losers once one copy is placed."""
        return race_job(instance_type, command, job_definition, environment, regions,
                        deadline_minutes)

    @server.tool(name="race_status")
    def _race_status(race_id: str, wait_seconds: int = 0) -> Dict[str, Any]:
        """Poll a race: per-region job state, the winner once one copy is placed (STARTING or
        later), and withdraw every other copy. wait_seconds (max 900) keeps polling every 30 s
        while no copy has been placed."""
        return race_status(race_id, wait_seconds)

    @server.tool(name="cancel_race")
    def _cancel_race(race_id: str) -> Dict[str, Any]:
        """Withdraw every copy of a race that has not finished, the winner included."""
        return cancel_race(race_id)

    @server.tool(name="hunt_nodes")
    def _hunt_nodes(instance_type: str, count: int = 1, regions: Optional[List[str]] = None,
                    ttl_hours: Optional[float] = None, dry_run: bool = False) -> Dict[str, Any]:
        """Launch `count` raw EC2 spot nodes of one type in ONE AZ, racing every fleet region
        at once from the fleet's own launch templates; the first region that fills wins and
        every other region yields. Nodes are terminated by the region's age terminator after
        ttl_hours (default: gpu_fleet.max_node_hours). dry_run checks templates, subnets and
        the AMI without launching."""
        return hunt_nodes(instance_type, count, regions, ttl_hours, dry_run)

    @server.tool(name="run_on_nodes")
    def _run_on_nodes(hunt_id: str, command: List[str], name: str = "gpu-fleet",
                      environment: Optional[Dict[str, str]] = None,
                      image: Optional[str] = None) -> Dict[str, Any]:
        """Run the fleet runtime image (or `image`) on every node of a filled hunt over SSM,
        e.g. command ["bench_gpu", "--model-uri", "s3://..."]. Output goes to the fleet node
        log group, one stream per node."""
        return run_on_nodes(hunt_id, command, name, environment, image)

    @server.tool(name="node_status")
    def _node_status(hunt_id: str, command_id: Optional[str] = None) -> Dict[str, Any]:
        """SSM status of the last (or the given) run_on_nodes command on every node."""
        return node_status(hunt_id, command_id)

    @server.tool(name="release_nodes")
    def _release_nodes(hunt_id: str) -> Dict[str, Any]:
        """Terminate every node of a hunt."""
        return release_nodes(hunt_id)

    @server.tool(name="list_races")
    def _list_races(kind: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
        """Recent races and hunts from the state directory, newest first; kind is
        batch-race or ec2-hunt."""
        return common.list_records(kind, limit)

    server.run()


if __name__ == "__main__":
    main()
