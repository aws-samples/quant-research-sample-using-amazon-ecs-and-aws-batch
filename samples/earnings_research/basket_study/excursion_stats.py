"""excursion_stats — per-trade path statistics (maximum adverse / favorable
excursion) for every matrix-chart panel and every exit, from the 18 P&L marks
in the shards (9:31..9:35, 10:00, 10:30, ... 15:30, close).

Purpose (user request 2026-09-11): understand the dynamics of winning and
losing trades BEFORE debating stop-loss / take-profit rules. Pure statistics,
no rules simulated.

For a trade held to exit E the path is the marks up to and including E.
  MAE = worst mark P&L on the path (bps of basket notional), MFE = best mark.
  final = P&L at E.  Trades = event x cell shard rows with status ok;
  POOLED = union of member cells.

Outputs under <s3-prefix>/portfolio_analysis/:
  excursion_groups.parquet   per (exit, scope, construction, universe, group):
      group = final-P&L bucket (<= -1000, -1000..-500, -500..-200, -200..0,
      0..200, 200..500, 500..1000, > 1000 bps) plus LOSERS (final < 0),
      WINNERS (final >= 0) and ALL; n, share_pct, mean_final_bps,
      mae_p10/p25/p50/p75/p90/mean, mfe_p10/.../mean, worst_in_open5_pct
      (worst mark at or before 9:35), mae_time_median / mfe_time_median (mark
      label), touched_m200/m500/m1000/m1500/m2000_pct (MAE <= -T),
      reached_p200/p500/p1000/p1500/p2000_pct (MFE >= +T)
  excursion_touch.parquet    per (exit, scope, construction, universe,
      kind, threshold_bps, population): kind 'MAE' with populations ALL /
      WINNERS / BIG_WINNERS (final >= +500) and kind 'MFE' with ALL / LOSERS /
      BIG_LOSERS (final <= -500); n_pop, hit_pct (touched / reached),
      mean_final_hit_bps, mean_final_miss_bps, ended_positive_pct (of hits),
      ended_negative_pct (of hits).

    AWS_PROFILE=<profile> python excursion_stats.py \
        --s3-prefix earnings-basket-study/results-10y [--write]
"""
import argparse
import io
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

import aggregate  # noqa: E402
import settings
from finalize_local import load_ok  # noqa: E402
from fees import fee_suffix  # noqa: E402
from portfolio_analysis import _scope_iter, cell_masks, exit_code, exit_label  # noqa: E402
from s3io import S3IO  # noqa: E402

GROUP_EDGES = [-np.inf, -1000, -500, -200, 0, 200, 500, 1000, np.inf]
GROUP_LABELS = ["<= -1000", "-1000..-500", "-500..-200", "-200..0", "0..200", "200..500",
                "500..1000", "> 1000"]
MAE_T = [200, 500, 1000, 1500, 2000]
MFE_T = [200, 500, 1000, 1500, 2000]
QS = [10, 25, 50, 75, 90]


def _q(v, prefix):
    if not len(v):
        return {f"{prefix}_p{q}": np.nan for q in QS} | {f"{prefix}_mean": np.nan}
    qs = np.percentile(v, QS)
    return {f"{prefix}_p{q}": float(x) for q, x in zip(QS, qs)} | {f"{prefix}_mean": float(v.mean())}


def group_row(final, mae, mfe, amin, amax, labels, n_open5, sel, group):
    f, a, b = final[sel], mae[sel], mfe[sel]
    n = int(len(f))
    row = {"group": group, "n": n, "share_pct": n / len(final) * 100 if len(final) else np.nan,
           "mean_final_bps": float(f.mean()) if n else np.nan, **_q(a, "mae"), **_q(b, "mfe")}
    if n:
        row["worst_in_open5_pct"] = float((amin[sel] < n_open5).mean()) * 100
        row["mae_time_median"] = labels[int(np.median(amin[sel]))]
        row["mfe_time_median"] = labels[int(np.median(amax[sel]))]
        for t in MAE_T:
            row[f"touched_m{t}_pct"] = float((a <= -t).mean()) * 100
        for t in MFE_T:
            row[f"reached_p{t}_pct"] = float((b >= t).mean()) * 100
    return row


def touch_rows(final, mae, mfe):
    rows = []
    pops = {"MAE": [("ALL", np.ones(len(final), bool)), ("WINNERS", final >= 0), ("BIG_WINNERS", final >= 500)],
            "MFE": [("ALL", np.ones(len(final), bool)), ("LOSERS", final < 0), ("BIG_LOSERS", final <= -500)]}
    for kind, ts in (("MAE", MAE_T), ("MFE", MFE_T)):
        for t in ts:
            hit_all = (mae <= -t) if kind == "MAE" else (mfe >= t)
            for pname, pm in pops[kind]:
                n = int(pm.sum())
                if not n:
                    continue
                hit = hit_all & pm
                miss = pm & ~hit_all
                nh = int(hit.sum())
                rows.append({"kind": kind, "threshold_bps": (-t if kind == "MAE" else t), "population": pname,
                             "n_pop": n, "hit_pct": nh / n * 100,
                             "mean_final_hit_bps": float(final[hit].mean()) if nh else np.nan,
                             "mean_final_miss_bps": float(final[miss].mean()) if miss.any() else np.nan,
                             "ended_positive_pct": float((final[hit] >= 0).mean()) * 100 if nh else np.nan,
                             "ended_negative_pct": float((final[hit] < 0).mean()) * 100 if nh else np.nan})
    return rows


def panel_stats(M, labels, n_open5):
    """M: trades x marks (bps) for the path up to the exit (last column = exit)."""
    keep = ~np.isnan(M).any(axis=1)
    M = M[keep]
    if not len(M):
        return [], []
    final = M[:, -1]
    mae, mfe = M.min(axis=1), M.max(axis=1)
    amin, amax = M.argmin(axis=1), M.argmax(axis=1)
    g = np.digitize(final, GROUP_EDGES[1:-1], right=False)   # 0..7
    groups = []
    for i, lab in enumerate(GROUP_LABELS):
        groups.append(group_row(final, mae, mfe, amin, amax, labels, n_open5, g == i, lab))
    groups.append(group_row(final, mae, mfe, amin, amax, labels, n_open5, final < 0, "LOSERS"))
    groups.append(group_row(final, mae, mfe, amin, amax, labels, n_open5, final >= 0, "WINNERS"))
    groups.append(group_row(final, mae, mfe, amin, amax, labels, n_open5, np.ones(len(final), bool), "ALL"))
    return groups, touch_rows(final, mae, mfe)


def build(ok: pd.DataFrame, profile=None):
    marks = aggregate._exits(ok)                        # sorted pnl_HHMM ..., "pnl" (close) last
    labels = [exit_label(c) for c in marks]
    n_open5 = sum(1 for c in marks if c != "pnl" and c[4:8] <= "0935")
    g_rows, t_rows = [], []
    for label, sub in _scope_iter(ok, profile):
        print(f"  {label:10s} {len(sub):8d} trade rows", flush=True)
        if not len(sub):
            continue
        allM = sub[marks].to_numpy(dtype=float) * 1e4
        for c, u, m in cell_masks(sub["construction"].to_numpy(), sub["universe"].to_numpy()):
            P = allM[m]
            for k, col in enumerate(marks):
                groups, touches = panel_stats(P[:, :k + 1], labels[:k + 1], n_open5)
                meta = {"exit": exit_code(col), "exit_label": exit_label(col), "scope": label,
                        "construction": c, "universe": u}
                g_rows += [{**meta, **r} for r in groups]
                t_rows += [{**meta, **r} for r in touches]
    return pd.DataFrame(g_rows), pd.DataFrame(t_rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--s3-prefix", default="earnings-basket-study/results-10y")
    ap.add_argument("--cache", default=None, help="alternative s3:// parquet of all shards")
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
    groups, touch = build(ok, profile=args.profile)
    print(f"groups {groups.shape}, touch {touch.shape}", flush=True)
    if args.write:
        import polars as pl
        pa = f"s3://{settings.get("s3", "data_bucket")}/{args.s3_prefix}/portfolio_analysis{fee_suffix(args.fee_per_share)}"
        s3io.write_parquet(pl.from_pandas(groups), f"{pa}/excursion_groups.parquet")
        s3io.write_parquet(pl.from_pandas(touch), f"{pa}/excursion_touch.parquet")
        print(f"uploaded {pa}/excursion_{{groups,touch}}.parquet")


if __name__ == "__main__":
    main()
