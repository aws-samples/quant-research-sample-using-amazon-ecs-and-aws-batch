"""Phase 5 prep: build Arm S (sentiment-directed) shards from a scoring job.

Bit-identical to eval-child --arm S, but reuses the Arm R base: eval-child sets
weights = {reporter:+dir, peer:-dir*v} so pnl = direction * hedged_return, and
the hedged return is direction-independent. We already have it per event in the
Arm R shard (pnl * direction on ok rows). So for each model we just apply the
sentiment direction (score > +neutral LONG / < -neutral SHORT / else skip) to
that base return — no panel refit.

Writes one shard per model to <prefix>/shards/arm=S/model=<name>.parquet, so
aggregate.py / equity_matrix.py / paired_tests.py all work off shards with only
--prefix (no --scores-job needed). Run once per arm into its own prefix.

Usage:
    python run_arm_s_local.py --job semis_earnings_baseline --prefix earnings-basket-study/results-semis
    python run_arm_s_local.py --job semis_earnings_indctx  --prefix earnings-basket-study/results-semis-indctx
"""
import argparse
import io
import json
import sys
from pathlib import Path

import boto3
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from evaluate import COLUMNS, DTYPES          # noqa: E402
from rules.direction import sentiment_direction  # noqa: E402
from equity_matrix import load_job_scores      # noqa: E402

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")


def _session():
    return boto3.Session(profile_name=settings.get("aws", "profile"),
                         region_name=settings.get("aws", "region"))


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--job", required=True, help="scoring job_name (score partitions)")
    ap.add_argument("--prefix", required=True, help="output results prefix for arm=S shards")
    ap.add_argument("--arm-r-prefix", default="earnings-basket-study/results-semis",
                    help="prefix holding the Arm R seed-0 base returns")
    ap.add_argument("--neutral", type=float, default=1.0)
    ap.add_argument("--profile", default=None)
    ap.add_argument("--min-scores", type=int, default=2067,
                    help="skip models whose score partition has fewer rows than "
                         "this (excludes incomplete/killed runs; = scorable_count)")
    args = ap.parse_args()

    sess = _session()
    s3 = sess.client("s3")

    # Arm R seed-0 = the per-event base (ok rows carry hedged return via pnl*direction)
    r0 = pd.read_parquet(io.BytesIO(s3.get_object(
        Bucket=_bucket(), Key=f"{args.arm_r_prefix}/shards/arm=R/seed=00.parquet")["Body"].read()))
    base = r0.copy()
    base["base_return"] = base["pnl"].astype("Float64") * base["direction"].astype("Float64")

    all_scores = load_job_scores(args.profile, args.job)
    scores_by_model = {m: s for m, s in all_scores.items() if len(s) >= args.min_scores}
    skipped = sorted(m for m, s in all_scores.items() if len(s) < args.min_scores)
    print(f"{len(scores_by_model)}/{len(all_scores)} complete models for job={args.job} "
          f"(>= {args.min_scores} scores); skipped incomplete: {skipped}", file=sys.stderr)

    for model, scores in sorted(scores_by_model.items()):
        rows = base.copy()
        rows["sentiment_model"] = model
        rows["seed"] = pd.NA
        rows["neutral_threshold"] = args.neutral
        sc, dirs, pnls, srcs, stats, skips = [], [], [], [], [], []
        for _, r in rows.iterrows():
            eid = int(r["event_id"])
            score = scores.get(eid)
            score = float(score) if score is not None and pd.notna(score) else None
            sc.append(score)
            if str(r["status"]) != "ok":               # construction/session skip: unchanged
                dirs.append(pd.NA); pnls.append(pd.NA); srcs.append("sentiment")
                stats.append(r["status"]); skips.append(r["skip_reason"]); continue
            dec = sentiment_direction(score, args.neutral)
            srcs.append("sentiment")
            if dec.direction == 0:
                dirs.append(pd.NA); pnls.append(pd.NA)
                stats.append("skip"); skips.append(dec.skip_reason)
            else:
                dirs.append(dec.direction)
                pnls.append(float(r["base_return"]) * dec.direction)
                stats.append("ok"); skips.append(pd.NA)
        rows["sentiment_score"] = sc
        rows["direction"] = dirs
        rows["direction_source"] = srcs
        rows["pnl"] = pnls
        rows["status"] = stats
        rows["skip_reason"] = skips
        shard = rows[COLUMNS].astype(DTYPES)
        buf = io.BytesIO()
        shard.to_parquet(buf, index=False)
        key = f"{args.prefix}/shards/arm=S/model={model}.parquet"
        s3.put_object(Bucket=_bucket(), Key=key, Body=buf.getvalue())
        ok = int((shard["status"] == "ok").sum())
        print(f"  {model:28s} ok={ok:4d} -> {key}", file=sys.stderr)

    print(json.dumps({"job": args.job, "prefix": args.prefix,
                      "models": len(scores_by_model)}))


if __name__ == "__main__":
    sys.exit(main())
