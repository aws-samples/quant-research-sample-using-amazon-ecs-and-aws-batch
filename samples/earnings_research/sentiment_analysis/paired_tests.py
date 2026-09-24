"""Paired event-bootstrap tests for score-set comparisons.

THE standing methodology for comparing any two score sets (prompts, models,
bands) and for the mandatory long-reporter/short-peers benchmark (benchmark 2): the sampling unit is the
EVENT, never the model — all models trade the same events, so their Sharpe
deltas are correlated and model-count tests are invalid.

For each comparison: resample the data-viable events with replacement; on
each draw recompute per-model per-event Sharpe for both arms on the SAME
drawn events (pairing removes shared event luck); report the median
across-model uplift, its bootstrap CI, and P(uplift <= 0).

Usage:
    python paired_tests.py --profile <profile> \
        --jobs xle_earnings_prompt_<id> [...]        # score jobs to test
    # baseline arm S shards and the long-rep/short-peers benchmark are always included.
"""
import argparse
import io
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import study

sys.path.insert(0, str(Path(__file__).resolve().parent))

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
N_BOOT = 5000
BOOT_SEED = 20260822
NEUTRAL = 1.0


def _prefix():
    return study.current().results_prefix


def load_long_pnl(s3):
    r0 = pd.read_parquet(io.BytesIO(s3.get_object(
        Bucket=_bucket(), Key=f"{_prefix()}/shards/arm=R/seed=00.parquet")["Body"].read()))
    ok = r0[r0["status"] == "ok"]
    eid = ok["event_id"].astype(int).values
    return pd.Series((ok["pnl"] * ok["direction"]).astype(float).values, index=eid)


def load_long_pnl_and_days(s3):
    """(long-reporter/short-peers pnl, trade_day) for every data-viable event."""
    r0 = pd.read_parquet(io.BytesIO(s3.get_object(
        Bucket=_bucket(), Key=f"{_prefix()}/shards/arm=R/seed=00.parquet")["Body"].read()))
    ok = r0[r0["status"] == "ok"]
    eid = ok["event_id"].astype(int).values
    return (pd.Series((ok["pnl"] * ok["direction"]).astype(float).values, index=eid),
            pd.Series(ok["trade_day"].astype(str).values, index=eid))


def restrict_events(long_pnl, events_json=None, min_trade_day=None, trade_days=None):
    """Restrict the paired-test event set; both filters intersect."""
    idx = long_pnl.index
    if events_json:
        keep = set(int(i) for i in json.loads(Path(events_json).read_text())["event_ids"])
        idx = idx[[int(i) in keep for i in idx]]
    if min_trade_day is not None:
        if trade_days is None:
            raise ValueError("--min-trade-day needs a trade_day series")
        td = trade_days.reindex(idx)
        idx = idx[(td >= min_trade_day).to_numpy()]
    return long_pnl.loc[idx]


def parse_pair(spec: str):
    """'ft-model:base-model' -> (ft, base)."""
    a, b = spec.split(":")
    if not a or not b:
        raise ValueError(f"--pair needs FT:BASE, got {spec!r}")
    return a, b


def per_year(P_a, P_b, trade_days, n_boot=N_BOOT, seed=BOOT_SEED):
    """paired_bootstrap per calendar year; P_a/P_b are events x models arrays aligned
    to trade_days' index order."""
    years = pd.to_datetime(trade_days.values).year
    rows = []
    for y in sorted(set(years)):
        sel = np.where(years == y)[0]
        r = paired_bootstrap(P_a[sel], P_b[sel], n_boot=n_boot, seed=seed)
        rows.append({"year": int(y), "n_events": int(len(sel)), **r})
    return pd.DataFrame(rows)


def null_band(r_shards, events):
    """Per-event Sharpe of every random-sign seed on exactly `events`."""
    out = {}
    for seed, df in r_shards.items():
        ok = df[df["status"] == "ok"]
        s = pd.Series(ok["pnl"].astype(float).values,
                      index=ok["event_id"].astype(int).values).reindex(events)
        out[int(seed)] = float(_sharpe(s.values.reshape(-1, 1))[0])
    return out


def placement(model_sharpe: float, null_sharpes: dict) -> dict:
    vals = np.array(list(null_sharpes.values()))
    n_ge = int((vals >= model_sharpe).sum())
    return {"model_sharpe": float(model_sharpe), "n_seeds": len(vals),
            "null_median": float(np.median(vals)), "null_p5": float(np.percentile(vals, 5)),
            "null_p95": float(np.percentile(vals, 95)), "null_max": float(vals.max()),
            "n_seeds_at_or_above": n_ge, "p": (1 + n_ge) / (1 + len(vals))}


def pnl_matrix(scores_by_model, long_pnl, models, neutral=NEUTRAL):
    """events x models signed pnl; NaN where not traded at this band."""
    events = long_pnl.index.values
    M = pd.DataFrame(index=events, columns=models, dtype=float)
    for m in models:
        sc = scores_by_model[m].reindex(events)
        traded = sc[sc.notna() & (sc.abs() > neutral)]
        M.loc[traded.index, m] = (np.sign(traded.values)
                                  * long_pnl.loc[traded.index].values)
    return M.values


def _sharpe(a):
    with np.errstate(invalid="ignore"):
        return np.nanmean(a, axis=0) / np.nanstd(a, axis=0, ddof=1)


def paired_bootstrap(P_a, P_b, n_boot=N_BOOT, seed=BOOT_SEED):
    """Median across-model per-event-Sharpe uplift of arm A over arm B.

    P_a, P_b: events x models (B may be events x 1 for the long-rep/short-peers benchmark).
    Returns dict: observed, ci_lo, ci_hi, p (P(uplift<=0)).
    """
    n_events = P_a.shape[0]
    rng = np.random.default_rng(seed)
    obs = float(np.nanmedian(_sharpe(P_a) - _sharpe(P_b)))
    med = np.empty(n_boot)
    for i in range(n_boot):
        idx = rng.integers(0, n_events, n_events)
        med[i] = np.nanmedian(_sharpe(P_a[idx]) - _sharpe(P_b[idx]))
    lo, hi = np.percentile(med, [2.5, 97.5])
    return {"observed": obs, "ci_lo": float(lo), "ci_hi": float(hi),
            "p_le_0": float((med <= 0).mean())}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--profile", default=None)
    ap.add_argument("--jobs", nargs="*", default=[],
                    help="scoring job names to test (baseline + long-rep/short-peers implied)")
    ap.add_argument("--neutral", type=float, default=NEUTRAL)
    ap.add_argument("--pair", action="append", default=[],
                    help="FT:BASE — paired bootstrap of one Arm S model against another "
                         "(e.g. ministral-3-8b-sft:ministral-3-8b); repeatable")
    ap.add_argument("--models", default=None,
                    help="comma list restricting the across-model comparisons")
    ap.add_argument("--events-json", default=None,
                    help="JSON {'event_ids': [...]}: restrict every test to these events")
    ap.add_argument("--min-trade-day", default=None,
                    help="YYYY-MM-DD: keep events with trade_day >= this (clean slice)")
    ap.add_argument("--per-year", action="store_true", help="per-calendar-year table for each --pair")
    ap.add_argument("--null-band", action="store_true",
                    help="place each --pair FT model against the 64 random seeds on the same events")
    ap.add_argument("--json-out", default=None, help="write every result to this JSON file")
    args = ap.parse_args()

    import boto3
    from s3io import S3IO
    from validate import _load_arm_s_shards
    from equity_matrix import load_job_scores

    sess = (boto3.Session(profile_name=args.profile) if args.profile
            else boto3.Session())
    s3 = sess.client("s3")
    s3io = S3IO(profile=args.profile)

    long_pnl, trade_days = load_long_pnl_and_days(s3)
    long_pnl = restrict_events(long_pnl, args.events_json, args.min_trade_day, trade_days)
    trade_days = trade_days.reindex(long_pnl.index)
    AL = long_pnl.values.reshape(-1, 1)

    s_shards = _load_arm_s_shards(s3io, _bucket(), f"{_prefix()}/shards/arm=S/")
    arms = {"baseline": {
        df["sentiment_model"].dropna().iloc[0]:
            df.set_index(df["event_id"].astype(int))["sentiment_score"]
        for df in s_shards}}
    for job in args.jobs:
        arms[job] = load_job_scores(args.profile, job)

    models = sorted(set.intersection(*(set(a) for a in arms.values())))
    if args.models:
        keep = set(args.models.split(","))
        models = [m for m in models if m in keep]
    print(f"{len(models)} common models, {len(long_pnl)} data-viable events, "
          f"band ±{args.neutral:g}\n")

    results = {"n_events": int(len(long_pnl)), "band": args.neutral,
               "events_json": args.events_json, "min_trade_day": args.min_trade_day,
               "vs_long_rep": {}, "vs_baseline": {}, "pairs": {}, "null": {}, "per_year": {}}

    mats = {name: pnl_matrix(scores, long_pnl, models, args.neutral)
            for name, scores in arms.items()}

    # mandatory benchmark 2: every arm vs long-reporter/short-peers
    for name, P in mats.items():
        r = paired_bootstrap(P, AL)
        results["vs_long_rep"][name] = r
        print(f"{name} vs LONG-REP/SHORT-PEERS: uplift {r['observed']:+.4f} "
              f"[{r['ci_lo']:+.4f},{r['ci_hi']:+.4f}] p={r['p_le_0']:.4f}")
    # each job vs baseline
    for name, P in mats.items():
        if name == "baseline":
            continue
        r = paired_bootstrap(P, mats["baseline"])
        results["vs_baseline"][name] = r
        print(f"{name} vs baseline: uplift {r['observed']:+.4f} "
              f"[{r['ci_lo']:+.4f},{r['ci_hi']:+.4f}] p={r['p_le_0']:.4f}")

    base_scores = arms["baseline"]
    r_shards = None
    for spec in args.pair:
        ft, base = parse_pair(spec)
        for m in (ft, base):
            if m not in base_scores:
                raise SystemExit(f"--pair: {m!r} has no Arm S shard under {_prefix()}")
        P_ft = pnl_matrix(base_scores, long_pnl, [ft], args.neutral)
        P_base = pnl_matrix(base_scores, long_pnl, [base], args.neutral)
        key = f"{ft}_vs_{base}"
        results["pairs"][key] = paired_bootstrap(P_ft, P_base)
        results["pairs"][f"{ft}_vs_long_rep"] = paired_bootstrap(P_ft, AL)
        results["pairs"][f"{base}_vs_long_rep"] = paired_bootstrap(P_base, AL)
        for k in (key, f"{ft}_vs_long_rep", f"{base}_vs_long_rep"):
            r = results["pairs"][k]
            print(f"PAIR {k}: uplift {r['observed']:+.4f} [{r['ci_lo']:+.4f},{r['ci_hi']:+.4f}] "
                  f"p={r['p_le_0']:.4f}  n_traded_ft={int(np.isfinite(P_ft).sum())}")
        if args.per_year:
            t = per_year(P_ft, P_base, trade_days)
            results["per_year"][key] = t.to_dict(orient="records")
            print(t.to_string(index=False))
        if args.null_band:
            if r_shards is None:
                from validate import _load_arm_r_shards
                r_shards = _load_arm_r_shards(s3io, _bucket(), f"{_prefix()}/shards/arm=R/")
            traded = long_pnl.index[np.isfinite(P_ft[:, 0])]
            nb = null_band(r_shards, traded)
            results["null"][ft] = placement(float(_sharpe(P_ft)[0]), nb)
            results["null"][ft]["n_events"] = int(len(traded))
            print(f"NULL {ft}: sharpe {results['null'][ft]['model_sharpe']:+.4f} vs 64 seeds "
                  f"median {results['null'][ft]['null_median']:+.4f} max {results['null'][ft]['null_max']:+.4f} "
                  f"p={results['null'][ft]['p']:.4f} (n={len(traded)})")
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(results, indent=2, default=float))

    al_sr = float(_sharpe(AL)[0])
    print(f"\nlong-reporter/short-peers per-event Sharpe: {al_sr:+.4f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
