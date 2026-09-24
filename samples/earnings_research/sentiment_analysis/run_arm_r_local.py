"""Phase 3 (semis study): build the 64-seed Arm R (null) frozen shards locally.

Bit-identical to the Batch `eval-child --arm R` path, but avoids an image
rebuild: ridge.build sets weights = {reporter:+dir, peer:-dir*v}, so pnl =
direction * hedged_return and the fit is direction-INDEPENDENT (ridge.py:157).
So we run the pipeline's own `evaluate.event_table` ONCE per event at
direction=+1 to get the hedged return + all metadata, then fan out to 64 seeds
by multiplying pnl by the frozen sign (signs.generate_frame, MASTER_SEED
20260821) — exactly what eval-child computes per (event, seed).

Writes one shard per seed to a NEW results prefix (never XLE's):
  s3://<bucket>/earnings-basket-study/results-semis/shards/arm=R/seed=NN.parquet

Usage:
    python run_arm_r_local.py --events ../semis_study/phase1b/semis_event_manifest_post.csv
        [--prefix earnings-basket-study/results-semis] [--workers 16]
"""
import argparse
import csv
import io
import json
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from evaluate import COLUMNS, DTYPES, event_table   # noqa: E402
from rules.direction import Decision                 # noqa: E402
import signs as signs_mod                             # noqa: E402

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
PANEL_PREFIX = "earnings-market-data/ALPACA.SIP/panels/"


def _session():
    return boto3.Session(profile_name=settings.get("aws", "profile"),
                         region_name=settings.get("aws", "region"))


def _code_version():
    root = Path(__file__).resolve().parent
    try:
        return subprocess.run(["git", "-C", str(root), "rev-parse", "--short", "HEAD"],
                              capture_output=True, text=True, check=True).stdout.strip()
    except Exception:
        return "local"


def panel_key_map(s3, event_ids):
    """event_id -> panel S3 key (from the corpus listing)."""
    want = set(event_ids)
    out = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=_bucket(), Prefix=PANEL_PREFIX):
        for o in page.get("Contents", []):
            k = o["Key"]
            if k.endswith(".parquet") and "event_" in k:
                eid = int(k.rsplit("event_", 1)[1].split(".")[0])
                if eid in want:
                    out[eid] = k
    return out


def base_row(s3, key, meta):
    """The direction=+1 event_table row (hedged return + metadata)."""
    panel = pd.read_parquet(io.BytesIO(s3.get_object(Bucket=_bucket(), Key=key)["Body"].read()))
    plus_one = lambda _e: Decision(+1, "random", None)  # noqa: E731
    return event_table(panel, plus_one, meta)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--events", default="../semis_study/phase1b/semis_event_manifest_post.csv")
    ap.add_argument("--prefix", default="earnings-basket-study/results-semis")
    ap.add_argument("--usable-only", action="store_true", default=True)
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--n-seeds", type=int, default=signs_mod.N_SEEDS)
    args = ap.parse_args()

    rows = list(csv.DictReader(open(args.events)))
    event_ids = sorted({int(r["event_id"]) for r in rows if r["class_post"] == "usable"})
    print(f"Arm R: {len(event_ids)} usable events x {args.n_seeds} seeds", file=sys.stderr)

    sess = _session()
    s3 = sess.client("s3")
    keymap = panel_key_map(s3, event_ids)
    missing = [e for e in event_ids if e not in keymap]
    if missing:
        print(f"WARN {len(missing)} events have no panel, skipped: {missing[:10]}", file=sys.stderr)
    event_ids = [e for e in event_ids if e in keymap]

    signs = signs_mod.generate_frame(event_ids, args.n_seeds)   # (event_id, seed, sign)
    sign_by = {(int(r.event_id), int(r.seed)): int(r.sign) for r in signs.itertuples()}

    meta = {"code_version": _code_version(), "run_id": str(uuid.uuid4()),
            "neutral_threshold": None, "seed": None, "sentiment_model": None}

    # 1) compute the +1 base row for every event (parallel S3 reads + ridge fit)
    bases = {}
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(base_row, s3, keymap[e], meta): e for e in event_ids}
        for fut in as_completed(futs):
            e = futs[fut]
            try:
                bases[e] = fut.result()
            except Exception as exc:  # noqa: BLE001
                err = pd.DataFrame([{"event_id": e, "status": "error", "skip_reason": repr(exc)}])
                for c in COLUMNS:
                    if c not in err.columns:
                        err[c] = None
                bases[e] = err[COLUMNS].astype(DTYPES)
            done += 1
            if done % 250 == 0:
                print(f"  {done}/{len(event_ids)} events evaluated", file=sys.stderr)

    ok = sum(1 for e in bases if str(bases[e]["status"].iloc[0]) == "ok")
    print(f"base rows: {ok} ok / {len(bases)} events", file=sys.stderr)

    # 2) fan out to 64 seed shards via pnl = sign * hedged_return
    for seed in range(args.n_seeds):
        parts = []
        for e in event_ids:
            row = bases[e].copy()
            sgn = sign_by[(e, seed)]
            row["seed"] = seed
            if str(row["status"].iloc[0]) == "ok":
                row["direction"] = sgn
                row["direction_source"] = "random"
                row["pnl"] = row["pnl"].astype("Float64") * sgn
            parts.append(row)
        shard = pd.concat(parts, ignore_index=True)[COLUMNS].astype(DTYPES)
        buf = io.BytesIO()
        shard.to_parquet(buf, index=False)
        key = f"{args.prefix}/shards/arm=R/seed={seed:02d}.parquet"
        s3.put_object(Bucket=_bucket(), Key=key, Body=buf.getvalue())
        if seed % 16 == 0 or seed == args.n_seeds - 1:
            print(f"  wrote seed {seed:02d}: {len(shard)} rows -> {key}", file=sys.stderr)

    print(json.dumps({"arm": "R", "events": len(event_ids), "seeds": args.n_seeds,
                      "ok_events": ok, "prefix": args.prefix, "run_id": meta["run_id"],
                      "code_version": meta["code_version"]}))


if __name__ == "__main__":
    sys.exit(main())
