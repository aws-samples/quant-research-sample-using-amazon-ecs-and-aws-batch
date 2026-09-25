"""Per-event evaluation (design spec §3, §6). One basket per event:
ridge levels beta-neutral on the full peer set. Entry 09:30 open,
exit last RTH close, one pnl per row. Batch commands live in this file
too (Task 5) — core evaluation here.
"""
import sys
from pathlib import Path
from typing import Callable, Dict, List, Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from construction.base import BasketContext, admissible_peers
from construction import ridge
from rules.direction import Decision
from rules.sessions import consolidate, entry_price, exit_price, trade_day_for_event

CONSTRUCTION = "ridge_levels_beta_neutral"
UNIVERSE = "all"

COLUMNS = ["event_id", "event_symbol", "event_date", "trade_day",
           "construction", "direction", "direction_source", "seed",
           "sentiment_model", "sentiment_score", "neutral_threshold",
           "universe", "status", "skip_reason", "pnl",
           "n_peers_theo", "n_peers_actual", "n_ffill_entries",
           "hedge_gross", "clamped", "code_version", "run_id"]

DTYPES = {"event_id": "Int64", "event_symbol": "string", "event_date": "string",
          "trade_day": "string", "construction": "string", "direction": "Int64",
          "direction_source": "string", "seed": "Int64",
          "sentiment_model": "string", "sentiment_score": "Float64",
          "neutral_threshold": "Float64", "universe": "string",
          "status": "string", "skip_reason": "string", "pnl": "Float64",
          "n_peers_theo": "Int64", "n_peers_actual": "Int64",
          "n_ffill_entries": "Int64", "hedge_gross": "Float64",
          "clamped": "boolean", "code_version": "string", "run_id": "string"}


def evaluate_event(panel: pd.DataFrame,
                   decision_fn: Callable[[int], Decision],
                   meta: dict) -> List[dict]:
    reporter = panel[panel["reporter_relationship"] == "primary"]["symbol"].iloc[0]
    event_id = int(panel["event_id"].iloc[0])
    event_ts = panel[panel["event_bar"]]["ts"].iloc[0]
    scores = meta.get("sentiment_score_by_event") or {}
    base = {"event_id": event_id, "event_symbol": reporter,
            "event_date": str(event_ts.tz_convert("America/New_York").date()),
            "construction": CONSTRUCTION, "universe": UNIVERSE,
            "seed": meta.get("seed"),
            "sentiment_model": meta.get("sentiment_model"),
            "sentiment_score": scores.get(event_id),
            "neutral_threshold": meta.get("neutral_threshold"),
            "code_version": meta["code_version"], "run_id": meta["run_id"]}

    day = trade_day_for_event(event_ts)
    if day is None:
        return [base | {"status": "skip", "skip_reason": "rth_release"}]
    base["trade_day"] = day

    decision = decision_fn(event_id)
    base["direction_source"] = decision.source
    if decision.direction == 0:
        return [base | {"status": "skip", "skip_reason": decision.skip_reason}]
    base["direction"] = decision.direction

    bars = consolidate(panel)
    rep_entry = entry_price(bars, reporter, day, allow_ffill=False)
    if rep_entry is None:
        return [base | {"status": "skip", "skip_reason": "reporter_no_open_bar"}]
    rep_exit = exit_price(bars, reporter, day)
    if rep_exit is None:
        return [base | {"status": "skip", "skip_reason": "reporter_no_exit"}]

    peers_raw = {rel: sorted(g["symbol"].unique())
                 for rel, g in panel[panel["reporter_relationship"] != "primary"]
                 .groupby("reporter_relationship")}
    peers, _dropped = admissible_peers(bars, peers_raw, day, reporter)
    ctx = BasketContext(bars=bars, reporter=reporter, peers=peers,
                        trade_day=day, direction=decision.direction)
    basket = ridge.build(ctx, UNIVERSE, fit_space="levels", sizing="beta")
    if not basket.weights:
        return [base | {"status": "skip",
                        "skip_reason": f"construction:{basket.diagnostics.get('skip')}"}]

    pnl, n_ffill, peers_actual = 0.0, 0, []
    for sym, w in basket.weights.items():
        if sym == reporter:
            e, x = rep_entry, rep_exit
        else:
            e = entry_price(bars, sym, day, allow_ffill=True)
            x = exit_price(bars, sym, day)
            at_open = bars[(bars["symbol"] == sym) & (bars["et_date"] == day)
                           & (bars["et_min"] == 570)]
            n_ffill += int(at_open.empty)
        if e is None or x is None or e <= 0:
            continue
        pnl += w * (x / e - 1.0)
        if sym != reporter:
            peers_actual.append(sym)

    n_theo = len([s for v in peers.values() for s in v])
    return [base | {"status": "ok", "pnl": pnl,
                    "n_peers_theo": n_theo, "n_peers_actual": len(peers_actual),
                    "n_ffill_entries": n_ffill,
                    "hedge_gross": basket.diagnostics.get("hedge_gross"),
                    "clamped": basket.diagnostics.get("clamped")}]


def event_table(panel, decision_fn, meta) -> pd.DataFrame:
    df = pd.DataFrame(evaluate_event(panel, decision_fn, meta))
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = None
    return df[COLUMNS].astype(DTYPES)


# ============================================================================
# Batch commands (Task 5): plan + eval-child
# ============================================================================

import argparse
import io
import json
import os
import re
import time
import uuid
import study

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
# Batch target: settings batch.job_queue / batch.job_definition (shared by all packages).


def _code_version() -> str:
    p = Path(__file__).resolve().parent / "CODE_VERSION"
    return p.read_text().strip() if p.exists() else "dev"


def _git_code_version() -> str:
    import subprocess
    root = Path(__file__).resolve().parent
    dirty = subprocess.run(["git", "-C", str(root), "status", "--porcelain",
                            "--", str(root)], capture_output=True, text=True).stdout.strip()
    if dirty:
        raise SystemExit("FATAL: uncommitted changes under sentiment_analysis/")
    # first 12 characters of the full SHA, exactly what the image build bakes
    return subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"],
                          capture_output=True, text=True, check=True).stdout.strip()[:12]


def _session(profile):
    import boto3
    return boto3.Session(profile_name=profile) if profile else boto3.Session()


def _list_universe_panels(s3, s3io):
    import study_config
    ids = set(study.universe_event_ids(s3io, study.current()))
    prefix = study_config.load()["evaluation"]["panel_sources"]["alpaca"]
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=_bucket(), Prefix=prefix):
        for o in page.get("Contents", []):
            m = re.search(r"event_(\d+)\.parquet$", o["Key"])
            if m and int(m.group(1)) in ids:
                keys.append(o["Key"])
    return sorted(keys), sorted(ids)


def plan_models(st, extra_arms_only: bool = False, only_model=None) -> list:
    """Arm S model list for a plan: the study CSV plus the registry's extra arms,
    or the extra arms alone; optionally narrowed to --only-model names."""
    from scores import load_model_list
    extras = sorted(st.extra_arms)
    if extra_arms_only:
        models = list(extras)
    else:
        models = load_model_list(str(Path(__file__).parent / "configs" / st.models_csv)) + extras
    if only_model:
        keep = set(only_model)
        models = [m for m in models if m in keep]
        if not models:
            raise SystemExit(f"--only-model matched nothing among {sorted(keep)}")
    return models


def resolve_scores_source(manifest: dict, model: str) -> tuple:
    """Where an Arm S child loads scores from: the manifest's extra_arms entry
    (model + scores_job) if present, else the study scores job under the same name."""
    ea = (manifest.get("extra_arms") or {}).get(model) or {}
    return ea.get("model", model), ea.get("scores_job") or manifest.get("scores_job")


def cmd_plan(args) -> int:
    from s3io import S3IO
    import signs as signs_mod
    session = _session(args.profile)
    s3 = session.client("s3")
    s3io = S3IO(profile=args.profile)
    st = study.current()
    signs_mod.load_signs(s3io)                       # verify OR die (spec §4)
    if args.arm == "R" and args.seeds != signs_mod.N_SEEDS:
        raise SystemExit(f"FATAL: --seeds {args.seeds} != signs table size {signs_mod.N_SEEDS}")
    signs_sha = signs_mod.paths()[2].read_text().strip()
    panels, event_ids = _list_universe_panels(s3, s3io)
    code_version = _git_code_version()
    run_id = str(uuid.uuid4())
    models = (plan_models(st, args.extra_arms_only, args.only_model)
              if args.arm == "S" else [])
    tag = "_extra" if (args.arm == "S" and args.extra_arms_only) else ""
    manifest = {"arm": args.arm, "panels": panels, "event_ids": event_ids,
                "models": models, "n_seeds": args.seeds, "neutral": args.neutral,
                "signs_sha256": signs_sha, "code_version": code_version,
                "run_id": run_id, "s3_prefix": st.results_prefix,
                "study": st.key, "scores_job": st.scores_job,
                "extra_arms": {k: dict(v) for k, v in st.extra_arms.items()}}
    size = args.seeds if args.arm == "R" else len(models)
    mk = f"{st.results_prefix}/manifests/manifest_{args.arm}_{size}{tag}.json"
    s3io.write_text(json.dumps(manifest, indent=2), f"s3://{_bucket()}/{mk}")
    out = {"manifest": mk, "arm": args.arm, "array_size": size,
           "n_panels": len(panels), "code_version": code_version,
           "submitted": False}
    if not args.dry_run:
        batch = session.client("batch", region_name=settings.get("aws", "region"))
        resp = batch.submit_job(
            jobName=f"esa-{st.key}-{args.arm}-{size}{tag}", jobQueue=settings.get("batch", "job_queue"),
            jobDefinition=settings.get("batch", "job_definition"),
            arrayProperties={"size": size} if size > 1 else {},
            containerOverrides=settings.job_overrides(
                "sentiment_analysis", ["eval-child", "--manifest", mk]),
            timeout={"attemptDurationSeconds": 43200})
        out.update(submitted=True, job_id=resp["jobId"])
    print(json.dumps(out, indent=2))
    return 0


def cmd_eval_child(args) -> int:
    from s3io import S3IO
    import polars as pl_
    import signs as signs_mod
    from rules.direction import random_direction, sentiment_direction
    from scores import load_scores
    idx = int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX", "0"))
    s3io = S3IO(profile=args.profile)
    m = json.loads(s3io.read_bytes(f"s3://{_bucket()}/{args.manifest}"))
    os.environ["ESA_STUDY"] = m.get("study", "xle")
    if m["code_version"] != _code_version():
        print(f"FATAL: image {_code_version()!r} != manifest {m['code_version']!r}")
        return 3
    session = _session(args.profile)
    s3 = session.client("s3")
    meta = {"code_version": m["code_version"], "run_id": m["run_id"],
            "neutral_threshold": None, "seed": None, "sentiment_model": None}
    if m["arm"] == "R":
        table = signs_mod.load_signs(s3io)           # sha-verified read
        seed = idx
        if seed >= m["n_seeds"]:
            raise SystemExit(f"FATAL: array index {seed} >= manifest n_seeds {m['n_seeds']}")
        meta["seed"] = seed
        decision_fn = lambda e: random_direction(table[(e, seed)])
        shard = f"{m['s3_prefix']}/shards/arm=R/seed={seed:02d}.parquet"
    else:
        model = m["models"][idx]
        scores_model, scores_job = resolve_scores_source(m, model)
        scores_map = load_scores(s3io, scores_model, scores_job)
        meta.update(sentiment_model=model, neutral_threshold=m["neutral"],
                    sentiment_score_by_event=scores_map)
        decision_fn = lambda e: sentiment_direction(scores_map.get(e), m["neutral"])
        shard = f"{m['s3_prefix']}/shards/arm=S/model={model}.parquet"

    tables, n_errors = [], 0
    for k in m["panels"]:
        panel = pd.read_parquet(io.BytesIO(
            s3.get_object(Bucket=_bucket(), Key=k)["Body"].read()))
        try:
            tables.append(event_table(panel, decision_fn, meta))
        except Exception as exc:
            err = pd.DataFrame([{"status": "error", "skip_reason": repr(exc)}])
            for c in COLUMNS:
                if c not in err.columns:
                    err[c] = None
            tables.append(err[COLUMNS].astype(DTYPES))
            n_errors += 1
    df = pd.concat(tables, ignore_index=True)
    s3io.write_parquet(pl_.from_pandas(df), f"s3://{_bucket()}/{shard}")
    print(json.dumps({"index": idx, "arm": m["arm"], "rows": len(df),
                      "errors": n_errors, "shard": shard}))
    return 4 if n_errors else 0


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["plan", "eval-child"])
    ap.add_argument("--arm", choices=["R", "S"], default=None)
    ap.add_argument("--seeds", type=int, default=64)
    ap.add_argument("--neutral", type=float, default=1.0)
    ap.add_argument("--manifest", default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--profile", default=None)
    ap.add_argument("--extra-arms-only", action="store_true",
                    help="Arm S over study.extra_arms only (fine-tuned models)")
    ap.add_argument("--only-model", action="append", default=None,
                    help="restrict the planned Arm S model list (repeatable)")
    args = ap.parse_args(argv)
    if args.command == "plan":
        if not args.arm:
            raise SystemExit("plan requires --arm R|S")
        return cmd_plan(args)
    if not args.manifest:
        raise SystemExit("eval-child requires --manifest")
    return cmd_eval_child(args)


if __name__ == "__main__":
    sys.exit(main())
