# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Run the fleet image on hunted nodes over SSM, the way a Batch job definition would.

The node pulls the image from its OWN region's registry (ECR replication put it there),
mounts the NVMe pool, passes every EFA device through, uses the host network (NCCL needs it)
and ships the container's output to the fleet's node log group, one stream per node.
The container gets the fleet job definitions' environment, so `bench_gpu`, `fetch_model`
and training images built FROM the runtime behave identically on either path.
"""
import shlex
from typing import Dict, List, Optional

from . import common
from .common import cat

SHM_GIB = 64


def image_uri(fleet: common.Fleet, account: str, region: str, arch: str, tag: str = "latest") -> str:
    repo = fleet.runtime_repositories.get(arch)
    if repo is None:
        raise ValueError(f"no fleet runtime image for arch {arch!r}; "
                         f"known: {sorted(fleet.runtime_repositories)}")
    return f"{account}.dkr.ecr.{region}.amazonaws.com/{repo}:{tag}"


def container_script(image: str, command: List[str], env: Dict[str, str], name: str,
                     region: str, log_group: str, pool: str = cat.NVME_POOL,
                     shm_gib: int = SHM_GIB) -> str:
    """The shell AWS-RunShellScript runs on the node."""
    q = shlex.quote
    envflags = " ".join(f"-e {q(f'{k}={v}')}" for k, v in sorted(env.items()))
    registry = image.split("/")[0]
    return "\n".join([
        "set -euo pipefail",
        # SSM injects its own credentials; the node role (via IMDS) is the one with ECR and S3
        "unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN",
        'TOKEN=$(curl -sX PUT http://169.254.169.254/latest/api/token '
        '-H "X-aws-ec2-metadata-token-ttl-seconds: 300")',
        'IID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" '
        'http://169.254.169.254/latest/meta-data/instance-id)',
        f"aws ecr get-login-password --region {q(region)} | "
        f"docker login --username AWS --password-stdin {q(registry)}",
        f"docker pull {q(image)}",
        f"docker rm -f {q(name)} 2>/dev/null || true",
        # every EFA device, if the node has any
        "EFA_DEV=''; for d in /dev/infiniband/uverbs* /dev/infiniband/rdma_cm; do "
        '[ -e "$d" ] && EFA_DEV="$EFA_DEV --device $d"; done',
        f"POOL=''; [ -d {q(pool)} ] && POOL='-v {pool}:{pool}'",
        f"docker run -d --name {q(name)} --gpus all --network host $EFA_DEV $POOL "
        f"--ulimit memlock=-1 --shm-size {shm_gib}g "
        f"--log-driver awslogs --log-opt awslogs-region={q(region)} "
        f"--log-opt awslogs-group={q(log_group)} --log-opt awslogs-stream={q(name)}/$IID "
        f"{envflags} {q(image)} {' '.join(q(c) for c in command)}",
        f"echo started {q(name)} on $IID",
    ])


def run(fleet: common.Fleet, clients: common.ClientFactory, record: dict, command: List[str], *,
        name: str = "gpu-fleet", environment: Optional[Dict[str, str]] = None,
        image: Optional[str] = None) -> dict:
    """Start `command` in the fleet image on every node of a filled hunt."""
    w = record.get("winner")
    if not w or record["state"] != "filled":
        raise ValueError(f"hunt {record['id']} holds no nodes (state {record['state']})")
    region = w["region"]
    if image is None:
        account = clients("sts", region).get_caller_identity()["Account"]
        image = image_uri(fleet, account, region, w["arch"])
    env = {**fleet.base_env(), **(environment or {})}
    script = container_script(image, command, env, name, region, fleet.node_log_group)
    r = clients("ssm", region).send_command(
        InstanceIds=w["instance_ids"], DocumentName="AWS-RunShellScript",
        Parameters={"commands": [script]}, Comment=f"{record['id']} {name}"[:100])
    run_rec = {"command_id": r["Command"]["CommandId"], "name": name, "image": image,
               "command": command, "started_utc": common.utc_now()}
    record.setdefault("runs", []).append(run_rec)
    common.save(record)
    return {**run_rec, "region": region, "log_group": fleet.node_log_group,
            "log_streams": [f"{name}/{i}" for i in w["instance_ids"]]}


def status(record: dict, clients: common.ClientFactory, command_id: Optional[str] = None) -> dict:
    """SSM status of the docker start on every node (the container itself logs to CloudWatch)."""
    w = record["winner"]
    cid = command_id or record["runs"][-1]["command_id"]
    ssm = clients("ssm", w["region"])
    nodes = {}
    for iid in w["instance_ids"]:
        try:
            inv = ssm.get_command_invocation(CommandId=cid, InstanceId=iid)
            nodes[iid] = {"status": inv["Status"],
                          "stdout": inv.get("StandardOutputContent", "")[-2000:],
                          "stderr": inv.get("StandardErrorContent", "")[-2000:]}
        except Exception as ex:
            nodes[iid] = {"status": "unknown", "error": f"{type(ex).__name__}: {ex}"}
    return {"hunt": record["id"], "command_id": cid, "region": w["region"], "nodes": nodes}
