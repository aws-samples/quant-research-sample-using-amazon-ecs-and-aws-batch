"""stop_sim — full-basket stop-loss simulation checked at the P&L marks.

For every matrix-chart panel, every exit and every stop level L in
{500, 1000, 1500, 2000} bps (5/10/15/20% of reporter notional; the P&L is the
whole basket, reporter + hedge peers, see evaluate.py):

    walk the marks up to the exit (9:31..9:35, 10:00, 10:30, ... 15:30, close);
    at the FIRST mark where basket P&L <= -L the basket is unwound at THAT
    mark's P&L (realistic: a mark-checked stop fills at the mark, which can be
    below -L); otherwise the trade runs to the exit.

L = 0 rows are the unstopped baseline. Moves between marks are not observed.

Outputs under <s3-prefix>/portfolio_analysis/:
  stop_sim_stats.parquet    per (exit, scope, construction, universe, stop_bps):
      trade-level: n_trades, stopped_pct, stopped_ended_pos_pct (share of the
      stopped trades that would have finished >= 0 unstopped),
      stopped_unstopped_mean_bps (what they would have finished at),
      stopped_fill_mean_bps (what they were cut at), mean_bps, median_bps,
      std_bps, hit_rate, p05_bps, p95_bps, delta_mean_bps (vs baseline);
      portfolio-level (strategy only, same daily capital model as the study):
      sharpe, ann_ret_pct, total_pct, max_dd_pct, p2t_days, p2r_days,
      recovered, longest_*, beat_spy_pct
  by_exit/<code>/daily_returns_stop<L>.parquet   same schema as
      by_exit/<code>/daily_returns.parquet, strategy returns with the stop

    AWS_PROFILE=<profile> python stop_sim.py [--write]
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

import aggregate  # noqa: E402
import settings
from benchmark import spy_returns  # noqa: E402
from finalize_local import load_ok  # noqa: E402
from fees import fee_suffix  # noqa: E402
from portfolio_analysis import (_scope_iter, cell_masks, daily_multi, exit_code,  # noqa: E402
                                exit_label, panels, stats_for)
from s3io import S3IO  # noqa: E402

LEVELS = [500, 1000, 1500, 2000]


def stopped(P: np.ndarray, k: int, lvl: int):
    """P: trades x marks in bps. Path = marks[:k+1]. -> (pnl_bps, hit_mask)."""
    path = P[:, :k + 1]
    with np.errstate(invalid="ignore"):
        mask = path <= -lvl
    hit = mask.any(axis=1)
    out = path[:, -1].copy()
    idx = np.flatnonzero(hit)
    out[idx] = path[idx, mask[idx].argmax(axis=1)]
    return out, hit


def trade_stats(v, final, hit):
    n = len(v)
    if not n:
        return None
    d = {"n_trades": int(n), "stopped_pct": float(hit.mean()) * 100,
         "mean_bps": float(v.mean()), "median_bps": float(np.median(v)),
         "std_bps": float(v.std(ddof=1)) if n > 1 else 0.0, "hit_rate": float((v > 0).mean()) * 100,
         "p05_bps": float(np.percentile(v, 5)), "p95_bps": float(np.percentile(v, 95)),
         "delta_mean_bps": float(v.mean() - final.mean())}
    if hit.any():
        d["stopped_ended_pos_pct"] = float((final[hit] >= 0).mean()) * 100
        d["stopped_unstopped_mean_bps"] = float(final[hit].mean())
        d["stopped_fill_mean_bps"] = float(v[hit].mean())
    return d


def build(ok, spy, profile=None):
    marks = aggregate._exits(ok)
    days = sorted(ok["trade_day"].unique())
    spy_bh = spy["buy_hold"].reindex(days).fillna(0.0)
    levels = [0] + LEVELS
    rows, dailies = [], {}                      # dailies[(code, L)][scope] = daily cells
    for label, sub in _scope_iter(ok, profile):
        print(f"  {label:10s} {len(sub):8d} trade rows", flush=True)
        if not len(sub):
            continue
        P = sub[marks].to_numpy(dtype=float) * 1e4
        con, uni = sub["construction"].to_numpy(), sub["universe"].to_numpy()
        cols = sub[["trade_day", "construction", "universe"]].copy()
        vals, hits = {}, {}
        for k, mcol in enumerate(marks):
            code = exit_code(mcol)
            vals[(code, 0)], hits[(code, 0)] = P[:, k], np.zeros(len(P), bool)
            cols[f"{code}|0"] = P[:, k] / 1e4
            for lvl in LEVELS:
                v, h = stopped(P, k, lvl)
                vals[(code, lvl)], hits[(code, lvl)] = v, h
                cols[f"{code}|{lvl}"] = v / 1e4
        dm = daily_multi(cols, [c for c in cols.columns if "|" in c])
        masks = {(c, u): m for c, u, m in cell_masks(con, uni)}
        for k, mcol in enumerate(marks):
            code = exit_code(mcol)
            final = vals[(code, 0)]
            for lvl in levels:
                daily = dm[f"{code}|{lvl}"].reindex(days).fillna(0.0)
                dailies.setdefault((code, lvl), {})[label] = daily
                port = {(c, u): stats_for(r, spy_bh) for c, u, r in panels(daily)}
                v, h = vals[(code, lvl)], hits[(code, lvl)]
                for (c, u), m in masks.items():
                    ts = trade_stats(v[m], final[m], h[m])
                    if ts is None or (c, u) not in port:
                        continue
                    rows.append({"exit": code, "exit_label": exit_label(mcol), "scope": label,
                                 "construction": c, "universe": u, "stop_bps": lvl, **ts, **port[(c, u)]})
    out_daily = {}
    for (code, lvl), per_scope in dailies.items():
        if lvl == 0:
            continue                                  # baseline daily files already exist
        wide = pd.concat({lab: d for lab, d in per_scope.items()}, axis=1)
        wide.columns = [f"{lab}|{cell}" for lab, cell in wide.columns]
        wide["spy_buy_hold"] = spy_bh.to_numpy()
        wide.index = pd.Index(days, name="trade_day")
        out_daily[(code, lvl)] = wide.reset_index()
    return pd.DataFrame(rows), out_daily


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--s3-prefix", default="earnings-basket-study/results-10y")
    ap.add_argument("--cache", default=None)
    ap.add_argument("--profile", default=None)
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--fee-per-share", type=float, default=0.0,
                    help="round-trip per-share commission netted out of every P&L column (fees.py); "
                         "outputs go to portfolio_analysis_fee<mills>m/")
    ap.add_argument("--extra-shards", action="append", default=None,
                    help="additional consolidated shard table(s) unioned in, e.g. the reporter_only cells")
    args = ap.parse_args()
    s3io = S3IO(profile=args.profile)
    ok = load_ok(s3io, args.s3_prefix, args.cache, fee_per_share=args.fee_per_share,
                 extra=args.extra_shards or ())
    spy = spy_returns(s3io, sorted(ok["trade_day"].unique()))
    stats, dailies = build(ok, spy, profile=args.profile)
    print(f"stop_sim stats {stats.shape}, daily files {len(dailies)}", flush=True)
    if args.write:
        import polars as pl
        pa = f"s3://{settings.get("s3", "data_bucket")}/{args.s3_prefix}/portfolio_analysis{fee_suffix(args.fee_per_share)}"
        s3io.write_parquet(pl.from_pandas(stats), f"{pa}/stop_sim_stats.parquet")
        for (code, lvl), df in dailies.items():
            s3io.write_parquet(pl.from_pandas(df), f"{pa}/by_exit/{code}/daily_returns_stop{lvl}.parquet")
        print(f"uploaded {pa}/stop_sim_stats.parquet + {len(dailies)} daily files")


if __name__ == "__main__":
    main()
