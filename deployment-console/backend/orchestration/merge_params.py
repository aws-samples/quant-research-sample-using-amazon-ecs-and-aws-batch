# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Deep-merge an agent-produced override onto the canonical parameters.json defaults,
then write the result to infrastructure/cdk.context.json as {"parameters": {...}}.

WHY THIS EXISTS (empirically verified against the real infrastructure/app.py):
  - `cdk -c parameters='{json}'` passes the value as a STRING, which breaks utils.py.
    The only working injection path is a JSON context FILE: cdk.context.json with
    {"parameters": <object>}, which CDK parses into a real dict.
  - infrastructure/app.py accesses fields like
    params.batch.single_node.container_command UNCONDITIONALLY, so the injected object
    must be COMPLETE. The agent therefore supplies a partial OVERRIDE that we deep-merge
    onto the canonical defaults — never a from-scratch object. This also guarantees the
    locked container_command default is always present and unchanged.

Usage (in CodeBuild, before `cdk deploy`):
    python merge_params.py \
        --defaults infrastructure/config/parameters.json \
        --override /tmp/override.json \
        --out infrastructure/cdk.context.json
"""

from __future__ import annotations

import argparse
import json
from typing import Any


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge `override` into a copy of `base`. Scalars and lists in
    `override` replace the base value; dicts merge key-by-key."""
    result = json.loads(json.dumps(base))  # deep copy via round-trip

    def _merge(a: dict[str, Any], b: dict[str, Any]) -> None:
        for k, v in b.items():
            if isinstance(v, dict) and isinstance(a.get(k), dict):
                _merge(a[k], v)
            else:
                a[k] = v

    _merge(result, override)
    return result


def build_context(defaults_path: str, override_path: str) -> dict[str, Any]:
    with open(defaults_path, "r", encoding="utf-8") as f:
        defaults = json.load(f)
    with open(override_path, "r", encoding="utf-8") as f:
        override = json.load(f)
    merged = deep_merge(defaults, override)
    return {"parameters": merged}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--defaults", required=True, help="canonical parameters.json")
    p.add_argument("--override", required=True, help="agent-produced partial override")
    p.add_argument("--out", required=True, help="output cdk.context.json path")
    args = p.parse_args()

    context = build_context(args.defaults, args.override)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(context, f, indent=2)
    print(f"Wrote {args.out} with deployment_type="
          f"{context['parameters'].get('batch', {}).get('deployment_type')}")


if __name__ == "__main__":
    main()
