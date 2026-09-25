# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""What the racer needs to know about the deployed fleet, and where it keeps its state.

Names are derived exactly as the CDK derives them (namespace, `gpu_fleet.name_prefix`, the
catalogue), so the racer never looks anything up that the deploy already decided. The
config is read lazily: importing this module never touches the file or AWS.
"""
import json
import os
import pathlib
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Callable, Dict, Optional

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))

from gpu_fleet import catalogue as cat  # noqa: E402

PARAMETERS = HERE.parents[1] / "config" / "parameters.json"
DEFAULT_NAMESPACE = "quant-research-with-aws-batch"
#: every job and instance a race creates carries this tag, valued with the race id
RACE_TAG_KEY = "rt-gpu:race"

#: `clients(service, region)` -> a boto3 client. Injected so tests never reach AWS.
ClientFactory = Callable[[str, str], object]


def boto3_clients() -> ClientFactory:
    import boto3
    import botocore.config
    #: bounded connects: an unreachable regional endpoint must fail in seconds, not minutes
    cfg = botocore.config.Config(connect_timeout=10, retries={"max_attempts": 3, "mode": "standard"})
    session = boto3.session.Session()
    cache: Dict[tuple, object] = {}

    def make(service: str, region: str):
        key = (service, region)
        if key not in cache:
            cache[key] = session.client(service, region_name=region, config=cfg)
        return cache[key]
    return make


@dataclass
class Fleet:
    namespace: str
    prefix: str
    home_region: str
    regions: tuple
    #: arch -> ECR repository name of the fleet runtime image
    runtime_repositories: Dict[str, str]
    weight_bucket: Optional[str]
    max_node_hours: int
    offerings: dict

    @classmethod
    def load(cls, home_region: Optional[str] = None, parameters: pathlib.Path = PARAMETERS,
             offerings: Optional[dict] = None) -> "Fleet":
        p = json.loads(parameters.read_text())["gpu_fleet"]
        ns = os.environ.get("NAMESPACE", DEFAULT_NAMESPACE)
        home = (home_region or os.environ.get("GPU_FLEET_HOME_REGION")
                or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"))
        if not home:
            raise ValueError("set GPU_FLEET_HOME_REGION (or AWS_REGION) to the region the "
                             "fleet was deployed from")
        return cls(namespace=ns, prefix=p.get("name_prefix", "rt"), home_region=home,
                   regions=tuple(p["presets"][p["preset"]]["regions"]),
                   runtime_repositories={b["arch"]: f"{ns}-{b['repository']}"
                                         for b in p.get("image_builds", [])},
                   weight_bucket=(p.get("weight_replica_bucket_prefix")
                                  if p.get("weight_buckets") else None),
                   max_node_hours=int(p.get("max_node_hours", 24)),
                   offerings=offerings if offerings is not None else cat.load_offerings())

    def instance_type(self, itype: str) -> "cat.InstanceType":
        t = cat.INSTANCE_TYPES.get(itype)
        if t is None:
            raise ValueError(f"{itype} is not in the fleet catalogue; one of "
                             f"{sorted(cat.INSTANCE_TYPES)}")
        return t

    def base_env(self) -> Dict[str, str]:
        """What a fleet job definition sets, so a raw node runs the image the same way."""
        pool = cat.NVME_POOL
        env = {"AWS_DEFAULT_REGION": self.home_region, "GPU_FLEET_HOME_REGION": self.home_region,
               "TRAIN_NVME_POOL": pool, "TRAIN_SCRATCH": pool, "TRAIN_REQUIRE_NVME": "1",
               "PYTHONUNBUFFERED": "1"}
        if self.weight_bucket:
            env["GPU_FLEET_WEIGHT_BUCKET"] = self.weight_bucket
        return env

    @property
    def node_log_group(self) -> str:
        return f"/{self.namespace}/gpu-fleet/nodes"


# ------------------------------------------------------------------------------ state
def state_dir() -> pathlib.Path:
    d = pathlib.Path(os.environ.get("GPU_RACE_STATE_DIR", pathlib.Path.home() / ".gpu-fleet" / "races"))
    d.mkdir(parents=True, exist_ok=True)
    return d


def new_id(kind: str) -> str:
    return f"{kind}-{time.strftime('%Y%m%d-%H%M%S', time.gmtime())}-{uuid.uuid4().hex[:6]}"


def save(record: dict) -> dict:
    path = state_dir() / f"{record['id']}.json"
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(record, indent=1, default=str))
    tmp.replace(path)
    return record


def load(record_id: str) -> dict:
    path = state_dir() / f"{record_id}.json"
    if not path.exists():
        raise ValueError(f"no race or hunt named {record_id} in {state_dir()}")
    return json.loads(path.read_text())


def list_records(kind: Optional[str] = None, limit: int = 20) -> list:
    paths = sorted(state_dir().glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    out = []
    for p in paths:
        r = json.loads(p.read_text())
        if kind and r.get("kind") != kind:
            continue
        out.append({k: r.get(k) for k in ("id", "kind", "instance_type", "state", "winner",
                                           "created_utc")})
        if len(out) >= limit:
            break
    return out


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
