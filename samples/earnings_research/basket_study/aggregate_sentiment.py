"""Per-(model, band) research report for the sentiment-conditioned study
(spec §6.2/§6.3): paired Sharpe (Ledoit-Wolf) vs baseline, n_flipped, fade
hit-rate on the flipped subset, the percentile grid, the mandatory
full-distribution chart, and the coded §6.3 promotion rule.
Pairwise flip validation is NOT here — that's validate_flips.py (Loop 2).

§6.3 promotion (all four criteria, no hand-waving): a cell is a FINDING only
when n_flipped >= 20, sharpe_delta > 0, the Ledoit-Wolf paired p < 0.05, the
paired sign test p < 0.05, AND an adjacent band of the same model agrees in
the direction of the delta. Cells that clear only the LW test are printed as
"candidates (not findings)".
"""

import argparse
import sys
import warnings
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
DEFAULT_PREFIX = "earnings-basket-study/results-xle-sentiment"
N_FLIP_FLOOR = 20                    # spec §6.3: fewer -> anecdotal
BANDS = ["2", "3", "4", "5"]
ALPHA = 0.05
PCTILES = [5, 25, 50, 75, 95]
# spec §6.2 item 3 / §7: the full grid, not one hand-picked cell
CONSTRUCTIONS = ["equal_weight_dollar_neutral", "ridge_returns_beta_neutral",
                 "ridge_returns_dollar_neutral", "ridge_levels_beta_neutral"]
UNIVERSES = ["all", "pure play", "functional", "correlated"]


def paired_sharpe_test(base: np.ndarray, cond: np.ndarray) -> Tuple[float, float, float, float]:
    """(sharpe_base, sharpe_cond, delta, p) — Jobson-Korkie test with the
    Memmel (2003) correction for the Sharpe difference of two correlated
    series; HAC-free variant for iid-ish event series (events are
    non-overlapping single days).

    Returns all-NaN when either series is degenerate (zero dispersion), and
    p = NaN — never 1.0 — when the asymptotic variance is non-positive or
    non-finite: "no usable p-value" must not masquerade as "no effect"."""
    n = len(base)
    if n < 2:
        return (np.nan,) * 4
    mu1, mu2 = base.mean(), cond.mean()
    s1, s2 = base.std(ddof=1), cond.std(ddof=1)
    if not np.isfinite(s1) or not np.isfinite(s2) or s1 == 0 or s2 == 0:
        # a constant series has no Sharpe ratio; a 0/0 or x/0 "Sharpe" here
        # would silently poison the delta and the variance below.
        return (np.nan,) * 4
    sr1, sr2 = mu1 / s1, mu2 / s2
    if np.allclose(base, cond):
        return float(sr1), float(sr2), 0.0, 1.0
    rho = float(np.corrcoef(base, cond)[0, 1])
    # asymptotic variance of (sr1 - sr2): Jobson-Korkie (1981) with the
    # Memmel (2003) correction. The cross term carries the 0.5 factor:
    #   V = (1/n) [ 2 - 2rho + 0.5(sr1^2 + sr2^2) - 0.5*sr1*sr2*(1 + rho^2) ]
    var = (1.0 / n) * (2 - 2 * rho + 0.5 * (sr1**2 + sr2**2)
                       - 0.5 * sr1 * sr2 * (1 + rho**2))
    if not np.isfinite(var) or var <= 0:
        warnings.warn(f"paired_sharpe_test: non-positive/non-finite variance "
                      f"({var!r}; n={n}, rho={rho:.4f}) — p-value undefined",
                      RuntimeWarning, stacklevel=2)
        return float(sr1), float(sr2), float(sr2 - sr1), float("nan")
    from scipy.stats import norm
    z = (sr2 - sr1) / np.sqrt(var)
    p = float(2 * (1 - norm.cdf(abs(z))))
    return float(sr1), float(sr2), float(sr2 - sr1), p


def sign_test_pvalue(base: np.ndarray, cond: np.ndarray) -> float:
    """Two-sided paired sign test on the per-event deltas (spec §6.3).

    Non-parametric companion to the Sharpe test: of the events whose P&L
    actually changed, how many improved? Ties (unflipped events, which are
    the majority by construction) are dropped, not counted as evidence."""
    d = np.asarray(cond, float) - np.asarray(base, float)
    d = d[np.isfinite(d)]
    nz = d[d != 0]
    if len(nz) == 0:
        return float("nan")
    from scipy.stats import binomtest
    return float(binomtest(int((nz > 0).sum()), len(nz), 0.5,
                           alternative="two-sided").pvalue)


def _assert_unique_events(index: pd.Index, label: str) -> None:
    """A duplicated event_id means two shards were concatenated for the same
    slice — classically a stale legacy model=<m>.parquet sitting next to the
    new model=<m>.part=*.parquet files. Silently averaging/first-winning
    there would corrupt every statistic downstream."""
    if index.has_duplicates:
        dups = index[index.duplicated()].unique().tolist()
        raise ValueError(
            f"duplicate event_ids in {label}: {len(dups)} ids, e.g. "
            f"{dups[:10]} — a stale single-file shard is probably sitting "
            f"alongside .part=* files; delete it and re-read")


def add_adjacent_agreement(cells: pd.DataFrame) -> pd.DataFrame:
    """spec §6.3: adjacent-band agreement. For each cell, look at bands
    {band-1, band+1} restricted to the studied bands {2,3,4,5} of the SAME
    model (and same construction/universe); True iff at least one of them has
    a sharpe_delta of the same sign. A lone significant band is NOT a
    finding — the effect must persist as the threshold moves."""
    cells = cells.copy()
    if cells.empty:
        cells["adjacent_agrees"] = pd.Series(dtype=bool)
        return cells
    group_keys = [k for k in ("construction", "universe", "model")
                  if k in cells.columns]
    bands = [int(b) for b in cells["band"]]
    signs = np.sign(cells["sharpe_delta"].to_numpy(float))
    if group_keys:
        groups = list(cells[group_keys].itertuples(index=False, name=None))
    else:
        groups = [()] * len(cells)
    lookup = {(g, b): s for g, b, s in zip(groups, bands, signs)}
    agrees = []
    for g, b, s in zip(groups, bands, signs):
        if not np.isfinite(s) or s == 0:
            agrees.append(False)                 # no direction to agree with
            continue
        neighbours = [n for n in (b - 1, b + 1) if 2 <= n <= 5]
        agrees.append(any(lookup.get((g, n)) == s for n in neighbours))
    cells["adjacent_agrees"] = agrees
    return cells


def promote(cells: pd.DataFrame) -> pd.DataFrame:
    """spec §6.3: the four-criteria conjunction. Anything less is a candidate."""
    if cells.empty:
        return cells
    return cells[(~cells["flagged"])
                 & (cells["sharpe_delta"] > 0)
                 & (cells["lw_pvalue"] < ALPHA)
                 & (cells["sign_pvalue"] < ALPHA)
                 & (cells["adjacent_agrees"])]


def candidates(cells: pd.DataFrame) -> pd.DataFrame:
    """Unflagged positive-delta cells that clear the LW test but fail at
    least one of the remaining §6.3 criteria — reported, never promoted."""
    if cells.empty:
        return cells
    lw = cells[(~cells["flagged"]) & (cells["sharpe_delta"] > 0)
               & (cells["lw_pvalue"] < ALPHA)]
    prom = promote(cells)
    return lw.drop(index=prom.index, errors="ignore")


def build_cells(base_df: pd.DataFrame, model_dfs: Dict[str, pd.DataFrame],
                construction: str, universe: str) -> pd.DataFrame:
    b = base_df[(base_df["construction"] == construction)
                & (base_df["universe"] == universe)
                & (base_df["status"] == "ok")].set_index("event_id")["pnl"]
    _assert_unique_events(b.index, f"baseline[{construction}|{universe}]")
    rows = []
    for model, df in sorted(model_dfs.items()):
        sl = df[(df["construction"] == construction) & (df["universe"] == universe)
                & (df["status"] == "ok")]
        for band in BANDS:
            g = sl[sl["flip_band"] == band].set_index("event_id")
            _assert_unique_events(g.index, f"model={model} band={band} "
                                           f"[{construction}|{universe}]")
            common = b.index.intersection(g.index)
            base_v = b.loc[common].to_numpy(float)
            cond_v = g.loc[common, "pnl"].to_numpy(float)
            flipped = g.loc[common, "direction_rule"] == "momentum_flip"
            n_flip = int(flipped.sum())
            # fade hit-rate on the flipped subset: was fade losing there?
            fade_hits = float((base_v[flipped.to_numpy()] > 0).mean()) if n_flip else np.nan
            sb, sc, delta, p = paired_sharpe_test(base_v, cond_v)
            sign_p = sign_test_pvalue(base_v, cond_v)
            qb = (np.percentile(base_v, PCTILES) * 100 if len(base_v)
                  else [np.nan] * len(PCTILES))
            qc = (np.percentile(cond_v, PCTILES) * 100 if len(cond_v)
                  else [np.nan] * len(PCTILES))
            rows.append({"construction": construction, "universe": universe,
                         "model": model, "band": band, "n_events": len(common),
                         "n_flipped": n_flip, "fade_hitrate_on_flipped": fade_hits,
                         "sharpe_base": sb, "sharpe_cond": sc,
                         "sharpe_delta": delta, "lw_pvalue": p,
                         "sign_pvalue": sign_p,
                         "hit_rate_base": (float((base_v > 0).mean())
                                           if len(base_v) else np.nan),
                         "hit_rate_cond": (float((cond_v > 0).mean())
                                           if len(cond_v) else np.nan),
                         **{f"base_p{q}_pct": v for q, v in zip(PCTILES, qb)},
                         **{f"cond_p{q}_pct": v for q, v in zip(PCTILES, qc)},
                         "flagged": n_flip < N_FLIP_FLOOR})
    return add_adjacent_agreement(pd.DataFrame(rows))


def percentile_grid(cells: pd.DataFrame) -> pd.DataFrame:
    """spec §6.2 item 3: portfolio-level percentile grid (percentiles, not
    means — the house convention), baseline vs conditioned per cell."""
    cols = ([c for c in ("construction", "universe", "model", "band",
                         "n_events", "n_flipped") if c in cells.columns]
            + [f"base_p{q}_pct" for q in PCTILES]
            + [f"cond_p{q}_pct" for q in PCTILES]
            + [c for c in ("hit_rate_base", "hit_rate_cond") if c in cells.columns])
    return cells[[c for c in cols if c in cells.columns]].copy()


def render_distribution(cells: pd.DataFrame, out_png: Path, subtitle: str = ""):
    """Spec §6.3: the full distribution of Sharpe deltas across ALL cells,
    n_flipped as point size, flagged cells greyed — never a best-cell table."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(14, 6))
    for flag, g in cells.groupby("flagged"):
        ax.scatter(g["band"].astype(int) + np.random.default_rng(0).uniform(-0.12, 0.12, len(g)),
                   g["sharpe_delta"], s=10 + 2 * g["n_flipped"],
                   alpha=0.5, c="grey" if flag else "tab:blue",
                   label="anecdotal (<20 flips)" if flag else "n_flipped >= 20")
    prom = promote(cells)
    if len(prom):
        ax.scatter(prom["band"].astype(int), prom["sharpe_delta"], s=90,
                   facecolors="none", edgecolors="tab:red", linewidths=1.4,
                   label=f"promoted findings (§6.3, n={len(prom)})")
    ax.axhline(0, lw=0.8, c="k")
    ax.set_xlabel("flip band |score| >="); ax.set_ylabel("Sharpe delta vs fade baseline")
    ax.set_title(f"Sharpe delta distribution — {cells['model'].nunique()} models x 4 bands "
                 f"(point size = n_flipped){subtitle}")
    ax.legend()
    fig.tight_layout(); fig.savefig(out_png, dpi=150); plt.close(fig)


REPORT_COLS = ["construction", "universe", "model", "band", "n_events",
               "n_flipped", "fade_hitrate_on_flipped", "sharpe_base",
               "sharpe_cond", "sharpe_delta", "lw_pvalue", "sign_pvalue",
               "adjacent_agrees"]


def _print_promotion(cells: pd.DataFrame, label: str) -> int:
    """Prints the §6.3 split and returns the promoted count."""
    prom, cand = promote(cells), candidates(cells)
    print(f"\n=== {label} — {len(cells)} cells "
          f"({int(cells['flagged'].sum())} flagged anecdotal) ===")
    print(f"expected false positives at alpha={ALPHA}: "
          f"{len(cells) * ALPHA:.1f} of {len(cells)} cells")
    cols = [c for c in REPORT_COLS if c in cells.columns]
    print(f"PROMOTED FINDINGS (§6.3: unflagged, delta>0, LW p<{ALPHA}, "
          f"sign p<{ALPHA}, adjacent band agrees): {len(prom)}")
    if len(prom):
        print(prom.sort_values("sharpe_delta", ascending=False)[cols].to_string(index=False))
    print(f"candidates (not findings — LW-significant only): {len(cand)}")
    if len(cand):
        print(cand.sort_values("sharpe_delta", ascending=False)
              .head(20)[cols].to_string(index=False))
    return len(prom)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--construction", default=None,
                    help="restrict to one construction (default: all four)")
    ap.add_argument("--universe", default=None,
                    help="restrict to one peer universe (default: all four)")
    ap.add_argument("--s3-prefix", default=DEFAULT_PREFIX)
    ap.add_argument("--profile", default=None)
    ap.add_argument("--out-dir", default=str(Path(__file__).resolve().parent / "results" / "aggregate-sentiment"))
    ap.add_argument("--no-upload", action="store_true",
                    help="write local artifacts only (no S3 PUTs)")
    args = ap.parse_args()
    import boto3
    from s3io import S3IO
    from sentiment_scores import load_model_list
    from shard_io import read_model_shards
    s3io = S3IO(profile=args.profile)

    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    s3 = session.client("s3")

    def shard(name):
        """Load a model shard, supporting both single-file and multi-part layouts."""
        return read_model_shards(s3, _bucket(), args.s3_prefix, name, args.profile)

    base = shard("baseline")
    models = load_model_list(str(Path(__file__).resolve().parent
                                 / "configs" / "sentiment_models_90pct.csv"))
    model_dfs = {m: shard(m) for m in models}

    constructions = [args.construction] if args.construction else CONSTRUCTIONS
    universes = [args.universe] if args.universe else UNIVERSES
    out = Path(args.out_dir); out.mkdir(parents=True, exist_ok=True)

    artifacts, all_cells, promoted_per_combo = [], [], {}
    for cons in constructions:
        for univ in universes:
            cells = build_cells(base, model_dfs, cons, univ)
            if cells.empty:
                print(f"WARNING: no cells for {cons}|{univ} — skipped")
                continue
            all_cells.append(cells)
            sub = out / cons / univ.replace(" ", "_")
            sub.mkdir(parents=True, exist_ok=True)
            cells.to_csv(sub / "cells.csv", index=False)
            percentile_grid(cells).to_csv(sub / "percentile_grid.csv", index=False)
            render_distribution(cells, sub / "sharpe_delta_distribution.png",
                                subtitle=f"\n{cons} | {univ}")
            for f in ("cells.csv", "percentile_grid.csv",
                      "sharpe_delta_distribution.png"):
                artifacts.append(sub / f)
            promoted_per_combo[f"{cons}|{univ}"] = _print_promotion(
                cells, f"{cons} | {univ}")

    if not all_cells:
        print("FATAL: no cells built for any construction x universe")
        return 1

    cells_all = pd.concat(all_cells, ignore_index=True)
    cells_all.to_csv(out / "cells.csv", index=False)
    percentile_grid(cells_all).to_csv(out / "percentile_grid.csv", index=False)
    render_distribution(cells_all, out / "sharpe_delta_distribution.png",
                        subtitle=f"\nall {len(constructions)} constructions x "
                                 f"{len(universes)} universes")
    artifacts += [out / "cells.csv", out / "percentile_grid.csv",
                  out / "sharpe_delta_distribution.png"]

    total_promoted = _print_promotion(cells_all, "ALL constructions x universes")
    print("\npromoted findings per (construction | universe):")
    for k, v in promoted_per_combo.items():
        print(f"  {k}: {v}")
    print(f"\nTOTAL promoted findings: {total_promoted} of {len(cells_all)} cells "
          f"(expected false positives {len(cells_all) * ALPHA:.1f})")

    if not args.no_upload:
        for p in artifacts:
            rel = p.relative_to(out).as_posix()
            s3io.write_bytes(p.read_bytes(),
                             f"s3://{_bucket()}/{args.s3_prefix}/aggregate-sentiment/{rel}")
        print(f"uploaded {len(artifacts)} artifacts to "
              f"s3://{_bucket()}/{args.s3_prefix}/aggregate-sentiment/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
