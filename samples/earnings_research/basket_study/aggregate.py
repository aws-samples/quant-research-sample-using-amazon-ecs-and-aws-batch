"""Final step: aggregate per-event shards into per-cell performance metrics.

For each grid cell (construction x universe):
  1. daily portfolio return series: that trade day's event P&Ls, equal-split
     with a PER-BASKET CAPITAL CAP (--max-basket-weight, default 2%): each
     basket gets min(1/N, cap) of the cell's capital, so day return =
     mean_pnl * min(1, cap*N); uncommitted capital idles at 0% — same for
     days with NO traded events (simulates running the program, per user
     decisions 2026-08-04 and 2026-08-06). cap=1.0 reproduces the legacy
     uncapped equal-split series.
  2. metrics: total return (compounded, %), annualized Sharpe (rf=0),
     max drawdown depth (%), drawdown durations (peak->trough and
     peak->recovery, trading days), hit rate, per-event percentiles,
     bootstrap 90% CI on Sharpe (resampling events within days).

Outputs (all S3, under .../results/aggregate/). Per MARK SHARD (close at
the root, each intraday mark under marks/<col>/):
  aggregate_metrics.parquet / .k      the metric table (K = box format)
  daily_returns.parquet               the per-cell daily series (audit) +
                                      spy_buy_hold / spy_open_close columns
                                      (when market-data/spy exists)
  equity_curves.png                   16 cumulative curves, overlay + pooled
                                      + SPY buy&hold / open->close overlay
  drawdown.png                        underwater chart per cell
  parallel_coordinates.png            categorical-axes parallel coordinates
  tier_metrics.parquet / .k           full grid per liquidity tier
  equity_by_tier.png                  4-panel equity curves by tier
  (close shard also owns run_coverage.json / .k)

Per FINALIZE (after the mark shards; the user's standing decision charts):
  exit_timing.parquet / .k / .png     which exit mark is best, paired stats
  exit_matrix.png                     11 scope rows (4 tiers + ALL + EX-MICRO +
                                      MID+SMALL + MID+LARGE + NDX + SPX +
                                      NDX+SPX) x 9 slices, open-burst vs close
                                      + SPY buy&hold per panel, Sharpe tags
  cell_matrix_midlarge.png            construction x universe grid, MID+LARGE
  cell_matrix_midsmall.png            same, MID+SMALL reporters
  cell_matrix_mid.png                 same, MID reporters only
  cell_matrix_large.png               same, LARGE reporters only
  cell_matrix_ndx.png                 same, Nasdaq-100 reporters only
  cell_matrix_spx.png                 same, S&P 500 reporters only
  cell_matrix_ndxspx.png              same, NDX or SPX members (union)
  (all cell matrices carry the SPY buy&hold curve per panel too)

Usage:
    python aggregate.py [--s3-prefix earnings-basket-study/results]
        [--index NDX] [--index SPX] [--profile ...]

Index filtering (--index): filters events to reporters that were index members
on the trade day. Repeat for multiple indices (OR logic). Outputs to
aggregate-{INDEX}/ subdir. Supported: NDX (Nasdaq-100), SPX (S&P 500).
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).resolve().parent))
from display import box_table

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
TRADING_DAYS = 252
BOOT_N = 2000
SEED = 7

# Capital cap per single basket, as a fraction of the cell's capital
# (user decision 2026-08-06: never allocate the whole book to one event).
# Weight per basket on a day with N events = min(1/N, cap), so the day's
# return = mean_pnl * min(1, cap*N) and the rest of the capital sits idle.
# Default comes from configs/study_config.json; --max-basket-weight
# overrides; 1.0 reproduces the legacy uncapped equal-split behaviour.
import study_config
MAX_BASKET_WEIGHT = study_config.load()["aggregation"]["max_basket_weight"]


# ------------------------------------------------------------------ series

def daily_returns(ok: pd.DataFrame, pnl_col: str = "pnl") -> pd.DataFrame:
    """Per-cell daily return series for ONE mark (pnl_col). Rows = every
    trade day in the sample window; a day with no traded events for a cell
    is 0. Columns = 'construction|universe'.

    Capital model: a day with N traded events gives each basket
    min(1/N, MAX_BASKET_WEIGHT) of the cell's capital — equal split,
    capped per basket; uncommitted capital earns 0."""
    ok = ok.copy()
    ok["cell"] = ok["construction"] + "|" + ok["universe"]
    days = sorted(ok["trade_day"].unique())
    g = ok.groupby(["trade_day", "cell"])[pnl_col]
    mean = g.mean().unstack("cell").reindex(days)
    n = g.size().unstack("cell").reindex(days)
    scale = (n * MAX_BASKET_WEIGHT).clip(upper=1.0)
    mat = (mean * scale).fillna(0.0)
    mat.index.name = "trade_day"
    return mat


# ------------------------------------------------------------- SPY overlay

# Portfolio model (user request 2026-08-26): long SPY with weight w at all
# times, strategy traded on top with leverage L — daily-rebalanced overlay,
# r_port = w * r_spy + L * r_strategy. w=0, L=1 is a bit-exact identity so
# existing artifacts reconcile. Set once in main() from --spy-weight /
# --strategy-leverage.
SPY_WEIGHT = 0.0
STRATEGY_LEVERAGE = 1.0


def _num(x: float) -> str:
    return str(int(x)) if float(x).is_integer() else str(x)


def spy_weight_suffix(w: float, lev: float = 1.0) -> str:
    """Output-dir suffix: '' at (0, 1) (legacy layout); '_spyw<w>' and/or
    '_lev<L>' otherwise."""
    parts = []
    if w != 0:
        parts.append(f"_spyw{_num(w)}")
    if lev != 1:
        parts.append(f"_lev{_num(lev)}")
    return "".join(parts)


def apply_spy_overlay(daily: pd.DataFrame, spy, w: float,
                      lev: float = 1.0) -> pd.DataFrame:
    """r_port = w * SPY buy&hold + lev * strategy, per cell column of a
    daily-returns matrix. spy is benchmark.spy_returns output (or None);
    days missing from spy contribute 0. (w=0, lev=1) returns the input
    unchanged (identity)."""
    if w == 0 and lev == 1:
        return daily
    if w != 0 and spy is None:
        raise SystemExit("--spy-weight != 0 requires SPY benchmark data "
                         "(market-data/spy) — refusing to write partial overlay")
    out = daily if lev == 1 else daily * lev
    if w != 0:
        add = spy["buy_hold"].reindex(daily.index).fillna(0.0).to_numpy() * w
        out = out.add(add, axis=0)
    return out


# ------------------------------------------------------------------ metrics

def drawdown_stats(equity: pd.Series) -> dict:
    peak = equity.cummax()
    dd = equity / peak - 1.0
    trough_i = int(np.argmin(dd.values))
    depth = float(dd.iloc[trough_i])
    # peak index for THIS drawdown
    peak_i = int(np.argmax(equity.values[:trough_i + 1])) if trough_i >= 0 else 0
    # recovery: first index after trough where equity regains the peak
    rec = equity.iloc[trough_i:][equity.iloc[trough_i:] >= peak.iloc[trough_i]]
    recovered = len(rec) > 0
    rec_i = int(equity.index.get_loc(rec.index[0])) if recovered else len(equity) - 1
    return {"max_dd_pct": depth * 100,
            "dd_peak_to_trough_days": trough_i - peak_i,
            "dd_peak_to_recovery_days": (rec_i - peak_i) if recovered else None,
            "dd_recovered": recovered}


def sharpe(r: np.ndarray) -> float:
    s = r.std(ddof=1)
    return float(r.mean() / s * np.sqrt(TRADING_DAYS)) if s > 0 else np.nan


def bootstrap_sharpe_ci(ok_cell: pd.DataFrame, days: list, n=BOOT_N, seed=SEED,
                        pnl_col: str = "pnl", overlay: np.ndarray = None,
                        lev: float = 1.0):
    """90% CI on Sharpe by resampling EVENTS within each day (keeps the
    day structure — zero-days stay zero). overlay: optional per-day constant
    (w * SPY) added to every bootstrap series — SPY is not resampled, it is
    the deterministic base holding."""
    rng = np.random.default_rng(seed)
    by_day = {d: g[pnl_col].to_numpy() for d, g in ok_cell.groupby("trade_day")}
    # same capital model as daily_returns: day return = mean * min(1, cap*N)
    scale = {d: min(1.0, MAX_BASKET_WEIGHT * len(v)) for d, v in by_day.items()}
    out = np.empty(n)
    for b in range(n):
        daily = np.array([by_day[d][rng.integers(0, len(by_day[d]), len(by_day[d]))].mean() * scale[d]
                          if d in by_day else 0.0 for d in days])
        if lev != 1:
            daily = daily * lev
        if overlay is not None:
            daily = daily + overlay
        out[b] = sharpe(daily)
    lo, hi = np.nanpercentile(out, [5, 95])
    return float(lo), float(hi)


def cell_metrics(ok: pd.DataFrame, daily: pd.DataFrame,
                 pnl_col: str = "pnl", overlay: np.ndarray = None,
                 lev: float = 1.0) -> pd.DataFrame:
    days = list(daily.index)
    rows = []
    for cell in daily.columns:
        cons, univ = cell.split("|")
        r = daily[cell].to_numpy()
        eq = pd.Series(np.cumprod(1 + r), index=daily.index)
        okc = ok[(ok["construction"] == cons) & (ok["universe"] == univ)]
        pnl = okc[pnl_col].to_numpy()
        lo, hi = bootstrap_sharpe_ci(okc, days, pnl_col=pnl_col, overlay=overlay,
                                     lev=lev)
        q = np.percentile(pnl, [5, 25, 50, 75, 95]) * 100 if len(pnl) else [np.nan] * 5
        rows.append({
            "construction": cons, "universe": univ,
            "n_events": len(pnl),
            "total_return_pct": (eq.iloc[-1] - 1) * 100,
            "sharpe": sharpe(r),
            "sharpe_ci90_lo": lo, "sharpe_ci90_hi": hi,
            **drawdown_stats(eq),
            "hit_rate": float((pnl > 0).mean()) if len(pnl) else np.nan,
            "event_p5_pct": q[0], "event_p25_pct": q[1], "event_p50_pct": q[2],
            "event_p75_pct": q[3], "event_p95_pct": q[4],
            "worst_event_pct": float(pnl.min() * 100) if len(pnl) else np.nan,
        })
    df = pd.DataFrame(rows)
    # Pareto: not dominated on (sharpe, total_return, -dd_depth, -dd_duration)
    crit = df[["sharpe", "total_return_pct"]].copy()
    crit["neg_dd"] = df["max_dd_pct"]                       # less negative = better
    crit["neg_dur"] = -df["dd_peak_to_trough_days"]
    dominated = []
    for i in range(len(df)):
        dom = any(all(crit.iloc[j] >= crit.iloc[i]) and any(crit.iloc[j] > crit.iloc[i])
                  for j in range(len(df)) if j != i)
        dominated.append(dom)
    df["pareto_optimal"] = ~pd.Series(dominated)
    return df


# ------------------------------------------------------------------ charts

CONS_COLORS = {"equal_weight_dollar_neutral": "tab:blue",
               "ridge_returns_beta_neutral": "tab:red",
               "ridge_returns_dollar_neutral": "tab:green",
               "ridge_levels_beta_neutral": "tab:purple"}
UNIV_STYLE = {"all": "-", "pure play": "--", "functional": ":", "correlated": "-."}


def date_ticks(days: list):
    """Adaptive x-axis ticks for a list of ISO trade days. Granularity scales
    with span so long histories stay readable:
        <= ~85 sessions (~4 months)   weekly    label MM-DD
        <= ~750 sessions (~3 years)   monthly   label YYYY-MM
        beyond                        quarterly label YYYY-Qn
    Returns (positions, labels, scale_name); position = first session of each
    period."""
    import datetime as _dt
    n = len(days)
    if n <= 85:
        scale, key, lbl = "weekly", \
            (lambda d: d.isocalendar()[:2]), (lambda d, ds: ds[5:])
    elif n <= 750:
        scale, key, lbl = "monthly", \
            (lambda d: (d.year, d.month)), (lambda d, ds: ds[:7])
    else:
        scale, key, lbl = "quarterly", \
            (lambda d: (d.year, (d.month - 1) // 3)), \
            (lambda d, ds: f"{d.year}-Q{(d.month - 1) // 3 + 1}")
    seen = {}
    for idx, ds in enumerate(days):
        d = _dt.date.fromisoformat(ds)
        seen.setdefault(key(d), (idx, lbl(d, ds)))
    ticks = sorted(seen.values())
    return [t[0] for t in ticks], [t[1] for t in ticks], scale


def _apply_date_axis(ax, days, fontsize=8):
    """Set adaptive date ticks + axis label on one axes."""
    pos, labels, scale = date_ticks(days)
    ax.set_xticks(pos)
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=fontsize)
    ax.set_xlabel(f"trade day ({scale} ticks)", fontsize=max(7, fontsize - 1))


def _plot_spy(ax, x, spy: pd.DataFrame):
    """Overlay SPY benchmark curves (black solid = buy&hold, gray dashed =
    open->close intraday, matching the strategy's holding window)."""
    bh = (np.cumprod(1 + spy["buy_hold"].to_numpy()) - 1) * 100
    oc = (np.cumprod(1 + spy["open_close"].to_numpy()) - 1) * 100
    ax.plot(x, bh, color="black", lw=2.0, alpha=0.8, label="SPY buy&hold")
    ax.plot(x, oc, color="dimgray", lw=1.6, ls="--", alpha=0.8, label="SPY open->close")


def _overlay_tag() -> str:
    """Title suffix marking overlaid artifacts; '' in the legacy layout."""
    if SPY_WEIGHT == 0 and STRATEGY_LEVERAGE == 1:
        return ""
    lev = f"{STRATEGY_LEVERAGE:g}x strategy"
    return (f"  [portfolio: +{SPY_WEIGHT:g}x SPY, {lev}]" if SPY_WEIGHT != 0
            else f"  [portfolio: {lev}]")


def fig_equity(daily: pd.DataFrame, out: str, spy: pd.DataFrame = None):
    fig, axes = plt.subplots(1, 2, figsize=(18, 7))
    x = range(len(daily))
    dates = [d[5:] for d in daily.index]        # 'MM-DD' (year in the title)
    ax = axes[0]
    for cell in daily.columns:
        cons, univ = cell.split("|")
        eq = np.cumprod(1 + daily[cell].to_numpy())
        ax.plot(x, (eq - 1) * 100, color=CONS_COLORS[cons], ls=UNIV_STYLE[univ],
                lw=1.4, label=f"{cons.replace('_neutral','')} | {univ}")
    if spy is not None:
        _plot_spy(ax, x, spy)
    ax.set_title("Equity curves — all 16 cells (color = construction, style = universe)"
                 + _overlay_tag(), loc="left")
    ax.legend(fontsize=6.5, ncol=2, loc="best")
    ax = axes[1]
    for cons in CONS_COLORS:
        cells = [c for c in daily.columns if c.startswith(cons + "|")]
        if not cells:
            continue
        pooled = daily[cells].mean(axis=1)      # equal capital across universes
        eq = np.cumprod(1 + pooled.to_numpy())
        ax.plot(x, (eq - 1) * 100, color=CONS_COLORS[cons], lw=2.0, label=cons)
    if spy is not None:
        _plot_spy(ax, x, spy)
    ax.set_title("Pooled by construction (equal weight across universes)", loc="left")
    ax.legend(fontsize=9)
    for ax in axes:
        ax.set_ylabel("cumulative return (%)")
        _apply_date_axis(ax, list(daily.index))
        ax.axhline(0, color="gray", lw=0.8)
        ax.grid(alpha=0.3)
    fig.tight_layout(); fig.savefig(out, dpi=110, format="png"); plt.close(fig)


def fig_drawdown(daily: pd.DataFrame, out: str):
    fig, ax = plt.subplots(figsize=(16, 6))
    x = range(len(daily))
    dates = [d[5:] for d in daily.index]
    for cell in daily.columns:
        cons, univ = cell.split("|")
        eq = pd.Series(np.cumprod(1 + daily[cell].to_numpy()))
        dd = (eq / eq.cummax() - 1) * 100
        ax.plot(x, dd, color=CONS_COLORS[cons], ls=UNIV_STYLE[univ], lw=1.2)
    ax.set_title("Underwater chart — drawdown from peak, all 16 cells "
                 "(color = construction, style = universe)", loc="left")
    ax.set_ylabel("drawdown (%)")
    _apply_date_axis(ax, list(daily.index))
    ax.axhline(0, color="gray", lw=0.8); ax.grid(alpha=0.3)
    fig.tight_layout(); fig.savefig(out, dpi=110, format="png"); plt.close(fig)


PC_METRICS = [("sharpe", "Sharpe"), ("total_return_pct", "total ret %"),
              ("max_dd_pct", "max DD %"), ("dd_peak_to_trough_days", "DD days"),
              ("hit_rate", "hit rate"), ("event_p50_pct", "median event %"),
              ("worst_event_pct", "worst event %")]


def fig_parallel_coordinates(metrics: pd.DataFrame, out: str):
    """Categorical-axes design: 'construction' and 'universe' ARE the first
    two x-axes — each line anchors at its own value on those axes, so a
    cell's identity is read off the axes themselves, not from a color key.
    Metric axes follow, min-max scaled, higher = better (DD axes inverted).
    Color encodes construction only as reinforcement of axis 1; thick =
    Pareto-optimal."""
    cols = [m for m, _ in PC_METRICS]
    data = metrics[cols].copy()
    data["max_dd_pct"] = -data["max_dd_pct"].abs()          # higher = better
    data["dd_peak_to_trough_days"] = -data["dd_peak_to_trough_days"]
    scaled = (data - data.min()) / (data.max() - data.min() + 1e-12)

    cons_order = list(CONS_COLORS)                     # fixed top-to-bottom order
    univ_order = ["all", "pure play", "functional", "correlated"]
    cons_pos = {c: 1 - i / (len(cons_order) - 1) for i, c in enumerate(cons_order)}
    univ_pos = {u: 1 - i / (len(univ_order) - 1) for i, u in enumerate(univ_order)}

    n_axes = 2 + len(cols)
    fig, ax = plt.subplots(figsize=(18, 9))
    for i, row in metrics.iterrows():
        ys = [cons_pos[row["construction"]], univ_pos[row["universe"]]] \
             + list(scaled.iloc[i])
        ax.plot(range(n_axes), ys, marker="o", ms=4,
                color=CONS_COLORS[row["construction"]], alpha=0.85,
                lw=2.4 if row["pareto_optimal"] else 1.1)

    # categorical tick labels ON the two identity axes
    for x, (pos, fs) in enumerate([(cons_pos, 8.5), (univ_pos, 9)]):
        for label, y in pos.items():
            ax.annotate(label, xy=(x, y),
                        xytext=(-8 if x == 0 else 8, 0),
                        textcoords="offset points", fontsize=fs,
                        ha="right" if x == 0 else "left", va="center",
                        fontweight="bold")
    ax.set_xticks(range(n_axes))
    ax.set_xticklabels(["CONSTRUCTION", "UNIVERSE"] + [lbl for _, lbl in PC_METRICS],
                       fontsize=9)
    ax.set_yticks([])
    ax.set_xlim(-1.6, n_axes - 0.5)
    ax.set_title("Parallel coordinates — cell identity on the first two axes "
                 "(follow a line from its construction through its universe into the metrics; "
                 "thick = Pareto-optimal; metric axes scaled, higher = better)", loc="left")
    ax.grid(alpha=0.3, axis="x")
    for j, c in enumerate(cols):
        ax.annotate(f"{data[c].max():.2f}", xy=(j + 2, 1.02), ha="center",
                    fontsize=7, color="gray")
        ax.annotate(f"{data[c].min():.2f}", xy=(j + 2, -0.06), ha="center",
                    fontsize=7, color="gray")
    fig.tight_layout(); fig.savefig(out, dpi=110, format="png"); plt.close(fig)


# ------------------------------------------------------------ exit timing

def _mark_label(col: str) -> str:
    return "close" if col == "pnl" else f"{col[4:6]}:{col[6:8]}"


def exit_timing_study(ok: pd.DataFrame, all_marks: dict, exits: list):
    """Which mark is the best exit? Pooled across ALL cells (equal weight),
    per-exit: mean/median event P&L, hit rate, pooled Sharpe — plus the
    statistical test: per-event PAIRED difference of each exit vs the close
    exit (same events, same entries; only the exit time differs), with a
    bootstrap 95% CI on the mean difference and the fraction of bootstrap
    draws where the exit beats close on Sharpe."""
    rng = np.random.default_rng(SEED)
    rows = []
    # per-event pooled pnl per exit: mean across that event's ok cells.
    # Event-level stats (mean/median bps, hit rate, paired diff vs close) stay
    # strategy-only by design — the SPY overlay is a portfolio-level base
    # holding, not attributable to events. Only pooled_sharpe sees the overlay
    # (via all_marks, already overlaid by the caller).
    per_event = {c: ok.groupby("event_id")[c].mean() for c in exits}
    close = per_event["pnl"]
    for col in exits:
        pe = per_event[col]
        pooled_daily = pd.concat(
            [all_marks[col][0].mean(axis=1)], axis=1).iloc[:, 0].to_numpy()
        diff = (pe - close).to_numpy()
        boots = np.array([diff[rng.integers(0, len(diff), len(diff))].mean()
                          for _ in range(BOOT_N)])
        rows.append({
            "exit": _mark_label(col),
            "mean_event_pnl_bps": pe.mean() * 1e4,
            "median_event_pnl_bps": pe.median() * 1e4,
            "hit_rate": float((pe > 0).mean()),
            "pooled_sharpe": sharpe(pooled_daily),
            "mean_diff_vs_close_bps": diff.mean() * 1e4,
            "diff_ci95_lo_bps": float(np.percentile(boots, 2.5) * 1e4),
            "diff_ci95_hi_bps": float(np.percentile(boots, 97.5) * 1e4),
            "beats_close_prob": float((boots > 0).mean()),
        })
    timing = pd.DataFrame(rows)
    best = timing.loc[timing["pooled_sharpe"].idxmax()]
    sig = timing[(timing["diff_ci95_lo_bps"] > 0)]
    verdict = (f"best exit by pooled Sharpe: {best['exit']} "
               f"(sharpe {best['pooled_sharpe']:.2f}, mean {best['mean_event_pnl_bps']:.1f} bps)"
               + ("; exits SIGNIFICANTLY better than close (95% CI>0): "
                  + ", ".join(sig["exit"]) if len(sig) else
                  "; no exit beats close at 95% confidence"))
    k = verdict + "\n" + box_table(timing.round(3).astype(str))
    return timing, k


def fig_exit_timing(timing: pd.DataFrame, ok: pd.DataFrame, exits: list, out):
    fig, axes = plt.subplots(1, 3, figsize=(19, 6.5))
    x = range(len(timing))
    labels = timing["exit"]

    ax = axes[0]
    ax.plot(x, timing["mean_event_pnl_bps"], marker="o", color="tab:blue",
            label="mean event P&L")
    ax.plot(x, timing["median_event_pnl_bps"], marker="s", color="tab:orange",
            label="median event P&L")
    ax.set_ylabel("bps per event"); ax.legend(fontsize=9)
    ax.set_title("Event P&L by exit time (pooled, all cells)", loc="left")

    ax = axes[1]
    ax.plot(x, timing["pooled_sharpe"], marker="o", color="tab:red")
    ax.set_ylabel("annualized Sharpe")
    ax.set_title("Pooled Sharpe by exit time", loc="left")

    ax = axes[2]
    ax.errorbar(x, timing["mean_diff_vs_close_bps"],
                yerr=[timing["mean_diff_vs_close_bps"] - timing["diff_ci95_lo_bps"],
                      timing["diff_ci95_hi_bps"] - timing["mean_diff_vs_close_bps"]],
                fmt="o", color="tab:purple", capsize=4)
    ax.axhline(0, color="gray", lw=1)
    ax.set_ylabel("bps vs close exit")
    ax.set_title("Paired difference vs close exit (95% CI; >0 = better than close)",
                 loc="left")

    for ax in axes:
        ax.set_xticks(list(x)); ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
        ax.grid(alpha=0.3)
    fig.suptitle("Exit-timing study — same events, same entries; only the exit mark varies"
                 + _overlay_tag(),
                 y=0.99)
    fig.tight_layout(); fig.savefig(out, dpi=110, format="png"); plt.close(fig)


# ------------------------------------------------------------------ main

def run_coverage(shards: pd.DataFrame, ok: pd.DataFrame) -> dict:
    """Descriptive stats of what this aggregation actually covered."""
    from datetime import datetime, timezone
    days = sorted(ok["trade_day"].dropna().unique())
    skips = shards[shards["status"] == "skip"]
    return {
        "run_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "trade_day_range": {"first": days[0] if days else None,
                            "last": days[-1] if days else None,
                            "n_trade_days": len(days),
                            "trade_days": days},
        "events": {"total": int(shards["event_id"].nunique()),
                   "traded": int(ok["event_id"].nunique()),
                   "skipped": int(shards["event_id"].nunique()
                                  - ok["event_id"].nunique())},
        "symbols": {"reporters": int(ok["event_symbol"].nunique()),
                    "reporter_list": sorted(ok["event_symbol"].dropna().unique()),
                    "peers_used": sorted({p for row in ok["peers_actual"].dropna()
                                          for p in str(row).split() if p})},
        "cells": {"ok": int(len(ok)), "skip": int(len(skips)),
                  "skip_reasons": skips["skip_reason"].value_counts().to_dict()},
        "events_per_day": ok.groupby("trade_day")["event_id"].nunique().to_dict(),
    }


def _exits(ok: pd.DataFrame) -> list:
    """Deterministic exit order: sorted mark columns, close last. This IS the
    fan-out list — Batch array index maps into it."""
    return sorted(c for c in ok.columns if c.startswith("pnl_")) + ["pnl"]


def _load_ok(s3io, s3_prefix):
    import polars as pl
    shards = pl.scan_parquet(f"s3://{_bucket()}/{s3_prefix}/shards/*.parquet",
                             storage_options=s3io.pl_opts).collect().to_pandas()
    ok = shards[shards["status"] == "ok"].copy()
    for c in _exits(ok):
        ok[c] = ok[c].astype(float)
    return shards, ok


def _header(cov, index_filter=None):
    hdr = (f"run {cov['run_at_utc']} | trade days {cov['trade_day_range']['first']}"
           f"..{cov['trade_day_range']['last']} ({cov['trade_day_range']['n_trade_days']}) | "
           f"events {cov['events']['traded']}/{cov['events']['total']} traded | "
           f"reporters {cov['symbols']['reporters']} | peers {len(cov['symbols']['peers_used'])}"
           f" | basket cap {MAX_BASKET_WEIGHT:.0%}")
    if index_filter:
        hdr += f" | filter {index_filter}"
    if SPY_WEIGHT != 0:
        hdr += f" | +{SPY_WEIGHT:g}x SPY overlay"
    if STRATEGY_LEVERAGE != 1:
        hdr += f" | {STRATEGY_LEVERAGE:g}x strategy leverage"
    return hdr



# ------------------------------------------------------------ liquidity tiers

TIERS = ["micro", "small", "mid", "large"]


def fig_equity_by_tier(ok: pd.DataFrame, out, pnl_col: str = "pnl", spy: pd.DataFrame = None):
    """Four panels (micro/small/mid/large): all 16 cells per panel plus the
    tier-pooled line, shared date axis and encodings — the tier lens that
    exposed mid-tier as the strategy's edge. SPY overlay when data present."""
    all_days = sorted(ok["trade_day"].unique())
    if spy is not None:
        spy = spy.reindex(all_days).fillna(0.0)
    fig, axes = plt.subplots(2, 2, figsize=(18, 11), sharex=True)
    for ax, tier in zip(axes.flat, TIERS):
        t = ok[ok["liquidity_tier"] == tier]
        if t.empty:
            ax.set_title(f"{tier.upper()} (0 events)", loc="left")
            continue
        d = daily_returns(t, pnl_col=pnl_col).reindex(all_days).fillna(0.0)
        d = apply_spy_overlay(d, spy, SPY_WEIGHT, STRATEGY_LEVERAGE)
        for cell in d.columns:
            cons, univ = cell.split("|")
            eq = np.cumprod(1 + d[cell].to_numpy())
            ax.plot(range(len(d)), (eq - 1) * 100, color=CONS_COLORS[cons],
                    ls=UNIV_STYLE[univ], lw=1.3)
        pooled = d.mean(axis=1)
        eqp = np.cumprod(1 + pooled.to_numpy())
        ax.plot(range(len(d)), (eqp - 1) * 100, color="black", lw=2.6,
                label=f"tier pooled ({(eqp[-1]-1)*100:+.1f}%)")
        if spy is not None:
            bh = (np.cumprod(1 + spy["buy_hold"].to_numpy()) - 1) * 100
            ax.plot(range(len(d)), bh, color="black", lw=1.6, ls=":",
                    alpha=0.7, label=f"SPY buy&hold ({bh[-1]:+.1f}%)")
        ax.set_title(f"{tier.upper()}  ({t['event_id'].nunique()} events)",
                     loc="left", fontsize=12, fontweight="bold")
        ax.axhline(0, color="gray", lw=0.8)
        ax.grid(alpha=0.3)
        ax.legend(loc="upper left", fontsize=9)
        ax.set_ylabel("cumulative return (%)")
    for ax in axes[1]:
        _apply_date_axis(ax, all_days, fontsize=7)
    handles = [plt.Line2D([], [], color=c, lw=2, label=n.replace("_neutral", ""))
               for n, c in CONS_COLORS.items()] + \
              [plt.Line2D([], [], color="gray", ls=st, lw=1.5, label=f"univ: {u}")
               for u, st in UNIV_STYLE.items()]
    fig.legend(handles=handles, loc="lower center", ncol=8, fontsize=8, frameon=False)
    fig.suptitle("Equity curves by liquidity tier — 16 cells per panel "
                 "(color = construction, style = universe; black = tier pooled)"
                 + _overlay_tag(), y=0.995)
    fig.tight_layout(rect=[0, 0.04, 1, 0.98])
    fig.savefig(out, dpi=110, format="png"); plt.close(fig)


def tier_metrics(ok: pd.DataFrame, pnl_col: str = "pnl", spy=None) -> pd.DataFrame:
    """Full cell_metrics grid per tier, stacked with a 'tier' column."""
    frames = []
    for tier in TIERS:
        t = ok[ok["liquidity_tier"] == tier]
        if t.empty or t["event_id"].nunique() < 3:
            continue
        d = daily_returns(t, pnl_col=pnl_col)
        d = apply_spy_overlay(d, spy, SPY_WEIGHT, STRATEGY_LEVERAGE)
        overlay = (spy["buy_hold"].reindex(d.index).fillna(0.0).to_numpy()
                   * SPY_WEIGHT) if SPY_WEIGHT != 0 else None
        m = cell_metrics(t, d, pnl_col=pnl_col, overlay=overlay,
                         lev=STRATEGY_LEVERAGE).sort_values(
            "sharpe", ascending=False).reset_index(drop=True)
        m.insert(0, "tier", tier)
        frames.append(m)
    return pd.concat(frames, ignore_index=True)


def run_mark(s3io, base: str, ok: pd.DataFrame, header: str, col: str):
    """ONE aggregation shard: the full metric+chart set for a single exit
    mark. col='pnl' (close) writes to the aggregate root; any other mark to
    marks/<col>/. No loops over marks in here — parallelism is the caller's."""
    import io as _io
    import polars as pl
    from benchmark import spy_returns
    sub = "" if col == "pnl" else f"marks/{col}/"
    daily = daily_returns(ok, pnl_col=col)
    spy = spy_returns(s3io, list(daily.index))   # None until SPY data lands
    daily = apply_spy_overlay(daily, spy, SPY_WEIGHT, STRATEGY_LEVERAGE)
    overlay = (spy["buy_hold"].reindex(daily.index).fillna(0.0).to_numpy()
               * SPY_WEIGHT) if SPY_WEIGHT != 0 else None
    metrics = cell_metrics(ok, daily, pnl_col=col, overlay=overlay,
                           lev=STRATEGY_LEVERAGE).sort_values(
        "sharpe", ascending=False).reset_index(drop=True)

    daily_out = daily.copy()
    if spy is not None:                          # per-day SPY next to the cells
        daily_out["spy_buy_hold"] = spy["buy_hold"]
        daily_out["spy_open_close"] = spy["open_close"]
    s3io.write_parquet(pl.from_pandas(daily_out.reset_index()),
                       f"{base}/{sub}daily_returns.parquet")
    s3io.write_parquet(pl.from_pandas(metrics),
                       f"{base}/{sub}aggregate_metrics.parquet")
    disp = metrics.copy()
    for c in disp.columns:
        if disp[c].dtype == float:
            disp[c] = disp[c].round(4)
    mark_note = "" if col == "pnl" else f" | mark {col}"
    s3io.write_text(header + mark_note + "\n" + box_table(disp.astype(str)) + "\n",
                    f"{base}/{sub}aggregate_metrics.k")

    buf = _io.BytesIO(); fig_equity(daily, buf, spy=spy)
    s3io.write_bytes(buf.getvalue(), f"{base}/{sub}equity_curves.png")
    buf = _io.BytesIO(); fig_drawdown(daily, buf)
    s3io.write_bytes(buf.getvalue(), f"{base}/{sub}drawdown.png")
    buf = _io.BytesIO(); fig_parallel_coordinates(metrics, buf)
    s3io.write_bytes(buf.getvalue(), f"{base}/{sub}parallel_coordinates.png")

    # liquidity-tier lens: per-tier metric grid + 4-panel equity curves
    tm = tier_metrics(ok, pnl_col=col, spy=spy)
    s3io.write_parquet(pl.from_pandas(tm), f"{base}/{sub}tier_metrics.parquet")
    tdisp = tm.copy()
    for c in tdisp.columns:
        if tdisp[c].dtype == float:
            tdisp[c] = tdisp[c].round(4)
    s3io.write_text(header + mark_note + " | by liquidity tier\n"
                    + box_table(tdisp.astype(str)) + "\n",
                    f"{base}/{sub}tier_metrics.k")
    buf = _io.BytesIO(); fig_equity_by_tier(ok, buf, pnl_col=col, spy=spy)
    s3io.write_bytes(buf.getvalue(), f"{base}/{sub}equity_by_tier.png")
    return daily, metrics


def run_coverage_outputs(s3io, base: str, shards: pd.DataFrame, ok: pd.DataFrame):
    import json
    cov = run_coverage(shards, ok)
    s3io.write_text(json.dumps(cov, indent=2, default=str), f"{base}/run_coverage.json")
    summary = pd.DataFrame([
        ("run_at_utc", cov["run_at_utc"]),
        ("trade_day_first", cov["trade_day_range"]["first"]),
        ("trade_day_last", cov["trade_day_range"]["last"]),
        ("n_trade_days", cov["trade_day_range"]["n_trade_days"]),
        ("events_total", cov["events"]["total"]),
        ("events_traded", cov["events"]["traded"]),
        ("events_skipped", cov["events"]["skipped"]),
        ("reporters", cov["symbols"]["reporters"]),
        ("peers_used", len(cov["symbols"]["peers_used"])),
        ("cells_ok", cov["cells"]["ok"]),
        ("cells_skip", cov["cells"]["skip"]),
    ], columns=["stat", "value"]).astype(str)
    per_day = pd.DataFrame(sorted(cov["events_per_day"].items()),
                           columns=["trade_day", "events_traded"]).astype(str)
    skip_tbl = pd.DataFrame(sorted(cov["cells"]["skip_reasons"].items(),
                                   key=lambda t: -t[1]),
                            columns=["skip_reason", "cells"]).astype(str)
    cov_k = "\n".join([box_table(summary), box_table(per_day), box_table(skip_tbl)])
    s3io.write_text(cov_k + "\n", f"{base}/run_coverage.k")
    return cov



# ------------------------------------------------- exit x scope x slice matrix

# Curves drawn in every matrix panel: the 9:31-9:35 open burst (blues,
# light->dark), the 10/11/12 morning marks (greens, light->dark; user request
# 2026-08-11), and close (red). Every column already exists in the shards.
OPEN_BURST = ["pnl_0931", "pnl_0932", "pnl_0933", "pnl_0934", "pnl_0935",
              "pnl_1000", "pnl_1100", "pnl_1200", "pnl"]
_EXIT_LABEL = {"pnl_0931": "9:31", "pnl_0932": "9:32", "pnl_0933": "9:33",
               "pnl_0934": "9:34", "pnl_0935": "9:35",
               "pnl_1000": "10:00", "pnl_1100": "11:00", "pnl_1200": "12:00",
               "pnl": "close"}
_EXIT_COLOR = {"pnl_0931": "#c6dbef", "pnl_0932": "#9ecae1", "pnl_0933": "#6baed6",
               "pnl_0934": "#3182bd", "pnl_0935": "#08519c",
               "pnl_1000": "#a1d99b", "pnl_1100": "#41ab5d", "pnl_1200": "#006d2c",
               "pnl": "tab:red"}


def fig_exit_matrix(ok: pd.DataFrame, out, spy: pd.DataFrame = None):
    """THE one-page decision matrix (user-blessed 2026-08-05): rows =
    liquidity scopes (4 tiers + ALL + EX-MICRO + MID+SMALL + MID+LARGE) and
    index scopes (NDX, SPX, NDX+SPX union), columns = pooled + 4
    constructions + 4 universes; each panel = equity curves for the
    open-burst exits (blues) vs close (red) + SPY buy&hold (black),
    Sharpe-colored tags (green +, red −, intensity = magnitude), shared
    y-scale per row."""
    from matplotlib.colors import TwoSlopeNorm
    from index_filter import IndexFilter

    from benchmark import spy_equity_curve
    for c in OPEN_BURST:
        ok[c] = ok[c].astype(float)
    all_days = sorted(ok["trade_day"].unique())
    spy_eq = spy_equity_curve(spy, all_days)
    if spy_eq is not None:
        spy_eq = spy_eq.to_numpy()

    # Scope rows come from the single study config (configs/study_config.json)
    rows = [(s["label"], study_config.scope_filter(s, IndexFilter))
            for s in study_config.load()["aggregation"]["exit_matrix_rows"]]
    cons_short = {"equal_weight_dollar_neutral": "eq_weight $-ntrl",
                  "ridge_returns_beta_neutral": "ridge_ret β-ntrl",
                  "ridge_returns_dollar_neutral": "ridge_ret $-ntrl",
                  "ridge_levels_beta_neutral": "ridge_lvl β-ntrl"}
    cols = [("POOLED", lambda d: d)] \
        + [(cons_short[c], (lambda cc: (lambda d: d[d["construction"] == cc]))(c))
           for c in CONS_COLORS] \
        + [(f"univ: {u}", (lambda uu: (lambda d: d[d["universe"] == uu]))(u))
           for u in UNIV_STYLE]

    cmap = plt.cm.RdYlGn
    norm = TwoSlopeNorm(vmin=-4, vcenter=0, vmax=4)

    fig, axes = plt.subplots(len(rows), len(cols), figsize=(24, 2.5 * len(rows)),
                             sharex=True,
                             gridspec_kw={"hspace": 0.10, "wspace": 0.04})
    for i, (rname, rf) in enumerate(rows):
        for j, (cname, cf) in enumerate(cols):
            ax = axes[i][j]
            sub = cf(rf(ok))
            if sub.empty or sub["event_id"].nunique() < 3:
                ax.set_facecolor("#f5f5f5"); ax.set_xticks([]); ax.set_yticks([])
                continue
            tags = []
            if spy_eq is not None:
                ax.plot(range(len(all_days)), spy_eq, color="black",
                        lw=1.2, alpha=0.65, gid="spy")
            for col in OPEN_BURST:
                d = daily_returns(sub, pnl_col=col).reindex(all_days).fillna(0.0)
                d = apply_spy_overlay(d, spy, SPY_WEIGHT, STRATEGY_LEVERAGE)
                pr = d.mean(axis=1)
                eq = (np.cumprod(1 + pr.to_numpy()) - 1) * 100
                ax.plot(range(len(d)), eq, color=_EXIT_COLOR[col],
                        lw=1.8 if col == "pnl" else 1.0)
                if col in ("pnl_0931", "pnl"):
                    tags.append((("31" if col == "pnl_0931" else "cl"),
                                 eq[-1], sharpe(pr.to_numpy())))
            for k, (tag, tot, sr) in enumerate(tags):
                ax.annotate(f"{tag} {tot:+.0f}% SR{sr:+.1f}",
                            xy=(0.02 + 0.5 * k, 0.03), xycoords="axes fraction",
                            fontsize=6.8, family="monospace",
                            bbox=dict(boxstyle="round,pad=0.15",
                                      fc=cmap(norm(np.clip(sr, -4, 4))),
                                      alpha=0.95, ec="none"))
            ax.axhline(0, color="gray", lw=0.5)
            ax.grid(axis="y", alpha=0.25)
            ax.grid(axis="x", alpha=0.10)
            ax.margins(x=0.01)
            ax.tick_params(labelsize=6, pad=1)
            if j > 0:
                ax.set_yticklabels([])
            if i == 0:
                ax.set_title(cname, fontsize=9.5, fontweight="bold", pad=3)
            if j == 0:
                ax.set_ylabel(f"{rname}\n({rf(ok)['event_id'].nunique()} ev)",
                              fontsize=9, fontweight="bold")
        ylims = [a.get_ylim() for a in axes[i] if a.lines]
        if ylims:
            lo, hi = min(y[0] for y in ylims), max(y[1] for y in ylims)
            for a in axes[i]:
                if a.lines:
                    a.set_ylim(lo, hi)

    for a in axes[-1]:
        _apply_date_axis(a, all_days, fontsize=7)
    handles = [plt.Line2D([], [], color=_EXIT_COLOR[c], lw=2, label=_EXIT_LABEL[c])
               for c in OPEN_BURST]
    if spy_eq is not None:
        handles.append(plt.Line2D([], [], color="black", lw=1.5, alpha=0.65,
                                  label="SPY buy&hold"))
    fig.legend(handles=handles, loc="lower center", ncol=7, fontsize=10,
               title="exit time", frameon=False, bbox_to_anchor=(0.5, -0.005))
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    cbar = fig.colorbar(sm, ax=axes, fraction=0.010, pad=0.004)
    cbar.set_label("Sharpe (tag color)", fontsize=9)
    fig.suptitle("Exit-time equity curves" + _overlay_tag() + " — rows: liquidity scope + index membership (shared y per row) | "
                 f"columns: pooled, construction, universe | basket cap {MAX_BASKET_WEIGHT:.0%}\n"
                 "tags: '31' = 9:31 exit, 'cl' = close — box color = Sharpe "
                 "(green +, red −, intensity = magnitude)\n"
                 "liquidity tiers (reporter trade-day RTH dollar volume): "
                 "MICRO < $2M  |  SMALL $2M-$20M  |  MID $20M-$200M  |  LARGE > $200M  |  "
                 "NDX = Nasdaq-100  |  SPX = S&P 500",
                 y=0.999, fontsize=13)
    fig.savefig(out, dpi=110, format="png", bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)



def fig_cell_matrix(ok: pd.DataFrame, out, tiers=None, index=None, title_suffix=None,
                    spy: pd.DataFrame = None, scope_fn=None):
    """Construction x peer-universe cell matrix for one liquidity scope or index
    (user-blessed 2026-08-05): rows = POOLED + 4 constructions, columns =
    POOLED + 4 universes; per panel the open-burst exits (blues) vs close
    (red) + SPY buy&hold (black) with Sharpe-colored tags; shared y per row,
    weekly date ticks.

    Args:
        ok: Full event DataFrame
        out: Output buffer
        tiers: Tuple of liquidity tier names (e.g., ("mid", "large"))
        index: Index code for filtering (e.g., "NDX", "SPX") - mutually exclusive with tiers
        title_suffix: Optional suffix for the title (e.g., "MID+LARGE", "Nasdaq-100")
        spy: aligned SPY return frame (benchmark.spy_returns); None -> no overlay
    """
    from matplotlib.colors import TwoSlopeNorm
    from index_filter import IndexFilter
    import datetime as _dt

    # Apply filter: a config-driven scope function (tiers / indices / price_bins),
    # else the legacy tiers-or-index arguments
    if scope_fn is not None:
        scope = scope_fn(ok).copy()
    elif index is not None:
        indices = [index] if isinstance(index, str) else list(index)
        idx_filter = IndexFilter(indices)
        scope = idx_filter.filter_events(ok, ticker_col="event_symbol", date_col="trade_day").copy()
        if title_suffix is None:
            names = {"NDX": "Nasdaq-100", "SPX": "S&P 500"}
            title_suffix = " | ".join(names.get(i, i) for i in indices)
    elif tiers is not None:
        scope = ok[ok["liquidity_tier"].isin(tiers)].copy()
        if title_suffix is None:
            title_suffix = "+".join([t.upper() for t in tiers])
    else:
        raise ValueError("Must specify either tiers or index")

    if scope.empty:
        return
    for c in OPEN_BURST:
        scope[c] = scope[c].astype(float)
    all_days = sorted(scope["trade_day"].unique())
    from benchmark import spy_equity_curve
    spy_eq = spy_equity_curve(spy, all_days)
    if spy_eq is not None:
        spy_eq = spy_eq.to_numpy()

    cons_short = {"equal_weight_dollar_neutral": "eq_weight $-ntrl",
                  "ridge_returns_beta_neutral": "ridge_ret β-ntrl",
                  "ridge_returns_dollar_neutral": "ridge_ret $-ntrl",
                  "ridge_levels_beta_neutral": "ridge_lvl β-ntrl"}
    row_defs = [("POOLED", lambda d: d)] + \
        [(cons_short[c], (lambda cc: (lambda d: d[d["construction"] == cc]))(c))
         for c in CONS_COLORS]
    col_defs = [("POOLED", lambda d: d)] + \
        [(u, (lambda uu: (lambda d: d[d["universe"] == uu]))(u)) for u in UNIV_STYLE]

    cmap = plt.cm.RdYlGn
    norm = TwoSlopeNorm(vmin=-4, vcenter=0, vmax=4)
    fig, axes = plt.subplots(len(row_defs), len(col_defs), figsize=(20, 15),
                             sharex=True,
                             gridspec_kw={"hspace": 0.10, "wspace": 0.04})
    n_scope = scope["event_id"].nunique()
    for i, (rname, rf) in enumerate(row_defs):
        for j, (cname, cf) in enumerate(col_defs):
            ax = axes[i][j]
            sub = cf(rf(scope))
            if sub.empty or sub["event_id"].nunique() < 3:
                ax.set_facecolor("#f5f5f5"); ax.set_xticks([]); ax.set_yticks([])
                continue
            tags = []
            if spy_eq is not None:
                ax.plot(range(len(all_days)), spy_eq, color="black",
                        lw=1.2, alpha=0.65, gid="spy")
            for col in OPEN_BURST:
                d = daily_returns(sub, pnl_col=col).reindex(all_days).fillna(0.0)
                d = apply_spy_overlay(d, spy, SPY_WEIGHT, STRATEGY_LEVERAGE)
                pr = d.mean(axis=1)
                eq = (np.cumprod(1 + pr.to_numpy()) - 1) * 100
                ax.plot(range(len(d)), eq, color=_EXIT_COLOR[col],
                        lw=1.8 if col == "pnl" else 1.0)
                if col in ("pnl_0931", "pnl"):
                    tags.append((("31" if col == "pnl_0931" else "cl"),
                                 eq[-1], sharpe(pr.to_numpy())))
            for k, (tag, tot, sr) in enumerate(tags):
                ax.annotate(f"{tag} {tot:+.0f}% SR{sr:+.1f}",
                            xy=(0.02 + 0.5 * k, 0.03), xycoords="axes fraction",
                            fontsize=7.2, family="monospace",
                            bbox=dict(boxstyle="round,pad=0.15",
                                      fc=cmap(norm(np.clip(sr, -4, 4))),
                                      alpha=0.95, ec="none"))
            ax.axhline(0, color="gray", lw=0.5)
            ax.grid(axis="y", alpha=0.25)
            ax.grid(axis="x", alpha=0.10)
            ax.margins(x=0.01)
            ax.tick_params(labelsize=6, pad=1)
            if j > 0:
                ax.set_yticklabels([])
            if i == 0:
                ax.set_title(f"univ: {cname}" if cname != "POOLED" else "POOLED",
                             fontsize=10, fontweight="bold", pad=3)
            if j == 0:
                ax.set_ylabel(rname, fontsize=9.5, fontweight="bold")
        ylims = [a.get_ylim() for a in axes[i] if a.lines]
        if ylims:
            lo, hi = min(y[0] for y in ylims), max(y[1] for y in ylims)
            for a in axes[i]:
                if a.lines:
                    a.set_ylim(lo, hi)

    for a in axes[-1]:
        _apply_date_axis(a, all_days, fontsize=7)

    handles = [plt.Line2D([], [], color=_EXIT_COLOR[c], lw=2, label=_EXIT_LABEL[c])
               for c in OPEN_BURST]
    if spy_eq is not None:
        handles.append(plt.Line2D([], [], color="black", lw=1.5, alpha=0.65,
                                  label="SPY buy&hold"))
    fig.legend(handles=handles, loc="lower center", ncol=7, fontsize=10,
               title="exit time", frameon=False, bbox_to_anchor=(0.5, -0.005))
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    cbar = fig.colorbar(sm, ax=axes, fraction=0.011, pad=0.004)
    cbar.set_label("Sharpe (tag color)", fontsize=9)
    fig.suptitle(f"{title_suffix} reporters only ({n_scope} events){_overlay_tag()} — "
                 "construction x peer-universe cell matrix\n"
                 "rows: construction (shared y per row) | columns: peer universe | "
                 f"tags: 9:31 / close — total% / Sharpe (color) | basket cap {MAX_BASKET_WEIGHT:.0%}\n"
                 "liquidity tiers (reporter trade-day RTH dollar volume): "
                 "MICRO < $2M  |  SMALL $2M-$20M  |  MID $20M-$200M  |  LARGE > $200M",
                 y=0.999, fontsize=13)
    fig.savefig(out, dpi=110, format="png", bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)


def run_finalize(s3io, base: str, ok: pd.DataFrame):
    """Exit-timing study — depends on nothing but the shards (recomputes the
    cheap pooled series per exit), so it can run after the mark shards or
    standalone."""
    import io as _io
    import polars as pl
    from benchmark import spy_returns
    exits = _exits(ok)
    spy = spy_returns(s3io, sorted(ok["trade_day"].unique()))
    all_marks = {c: (apply_spy_overlay(daily_returns(ok, pnl_col=c), spy,
                                       SPY_WEIGHT, STRATEGY_LEVERAGE), None)
                 for c in exits}
    timing, best_k = exit_timing_study(ok, all_marks, exits)
    s3io.write_parquet(pl.from_pandas(timing), f"{base}/exit_timing.parquet")
    s3io.write_text(best_k + "\n", f"{base}/exit_timing.k")
    print("\n" + best_k)
    buf = _io.BytesIO(); fig_exit_timing(timing, ok, exits, buf)
    s3io.write_bytes(buf.getvalue(), f"{base}/exit_timing.png")
    buf = _io.BytesIO(); fig_exit_matrix(ok, buf, spy=spy)
    s3io.write_bytes(buf.getvalue(), f"{base}/exit_matrix.png")

    # Cell matrices from the single study config: one chart per entry,
    # cell_matrix_<key>.png; add an entry to the config to add a chart.
    from index_filter import IndexFilter
    for s in study_config.load()["aggregation"]["cell_matrices"]:
        if s.get("price_bins") is not None and study_config.PRICE_COL not in ok.columns:
            print(f"cell_matrix_{s['key']}: skipped — {study_config.PRICE_COL} not attached "
                  f"(finalize_local.attach_reporter_price)", flush=True)
            continue
        buf = _io.BytesIO()
        fig_cell_matrix(ok, buf, title_suffix=s["title"], spy=spy,
                        scope_fn=study_config.scope_filter(s, IndexFilter))
        data = buf.getvalue()
        if data:                                   # empty scope -> no chart
            s3io.write_bytes(data, f"{base}/cell_matrix_{s['key']}.png")

    return timing


def main(args=None):
    global MAX_BASKET_WEIGHT, SPY_WEIGHT, STRATEGY_LEVERAGE
    if args is None:
        ap = argparse.ArgumentParser()
        ap.add_argument("--s3-prefix", default="earnings-basket-study/results")
        ap.add_argument("--mark", default=None,
                        help="single-shard mode: one exit column (e.g. pnl_1100, "
                             "'pnl' for close, or 'auto' = AWS_BATCH_JOB_ARRAY_INDEX)")
        ap.add_argument("--finalize", action="store_true",
                        help="exit-timing study only (post-marks step)")
        ap.add_argument("--index", action="append", dest="indices",
                        help="filter to events where reporter was in index on trade day "
                             "(NDX=Nasdaq-100, SPX=S&P 500); repeat for multiple indices")
        ap.add_argument("--max-basket-weight", type=float, default=MAX_BASKET_WEIGHT,
                        help="max fraction of a cell's capital in one basket "
                             f"(default {MAX_BASKET_WEIGHT}; 1.0 = legacy uncapped)")
        ap.add_argument("--spy-weight", type=float, default=0.0,
                        help="always-long SPY base holding added to every daily "
                             "series (r_port = w*r_spy + r_strategy, daily "
                             "rebalanced). 0 = off (legacy artifacts, default); "
                             "nonzero writes to aggregate_spyw<w>/ instead")
        ap.add_argument("--strategy-leverage", type=float, default=1.0,
                        help="leverage L on the strategy leg (r_port = "
                             "w*r_spy + L*r_strategy). 1 = unlevered "
                             "(default); != 1 adds _lev<L> to the output dir")
        ap.add_argument("--profile", default=None)
        args = ap.parse_args()

    cap = getattr(args, "max_basket_weight", None)
    if cap is not None:
        MAX_BASKET_WEIGHT = cap
    if not 0 < MAX_BASKET_WEIGHT <= 1:
        raise SystemExit(f"--max-basket-weight must be in (0, 1], got {MAX_BASKET_WEIGHT}")
    SPY_WEIGHT = getattr(args, "spy_weight", 0.0) or 0.0
    if SPY_WEIGHT < 0:
        raise SystemExit(f"--spy-weight must be >= 0, got {SPY_WEIGHT}")
    STRATEGY_LEVERAGE = getattr(args, "strategy_leverage", None)
    STRATEGY_LEVERAGE = 1.0 if STRATEGY_LEVERAGE is None else STRATEGY_LEVERAGE
    if STRATEGY_LEVERAGE <= 0:
        raise SystemExit(f"--strategy-leverage must be > 0, got {STRATEGY_LEVERAGE}")

    from s3io import S3IO
    from index_filter import IndexFilter

    s3io = S3IO(profile=args.profile)
    base = (f"s3://{_bucket()}/{args.s3_prefix}/aggregate"
            f"{spy_weight_suffix(SPY_WEIGHT, STRATEGY_LEVERAGE)}")
    shards, ok = _load_ok(s3io, args.s3_prefix)

    # Apply index filter if specified
    indices = getattr(args, "indices", None)
    if indices:
        idx_filter = IndexFilter(indices, profile=args.profile)
        print(f"Applying index filter: {idx_filter}")
        ok_before = len(ok)
        ok = idx_filter.filter_events(ok, ticker_col="event_symbol", date_col="trade_day")
        print(f"Filtered: {ok_before} -> {len(ok)} events ({len(ok)/ok_before*100:.1f}%)")
        base = (f"s3://{_bucket()}/{args.s3_prefix}/aggregate-{'_'.join(indices)}"
                f"{spy_weight_suffix(SPY_WEIGHT, STRATEGY_LEVERAGE)}")

    exits = _exits(ok)

    mark = getattr(args, "mark", None)
    finalize = getattr(args, "finalize", False)

    if mark == "auto":
        import os
        idx = os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX")
        if idx is None:
            raise SystemExit("--mark auto requires AWS_BATCH_JOB_ARRAY_INDEX")
        mark = exits[int(idx)]

    if finalize:
        run_finalize(s3io, base, ok)
        return

    cov = run_coverage(shards, ok)
    idx_filter_obj = IndexFilter(indices, profile=args.profile) if indices else None
    header = _header(cov, index_filter=idx_filter_obj)

    if mark:                                   # ONE shard: one mark, no loop
        if mark == "pnl":                      # close shard also owns coverage
            run_coverage_outputs(s3io, base, shards, ok)
        daily, metrics = run_mark(s3io, base, ok, header, mark)
        print(f"mark {mark}: wrote {'root' if mark == 'pnl' else 'marks/' + mark}"
              f" | top cell {metrics.iloc[0]['construction']}|{metrics.iloc[0]['universe']}"
              f" sharpe {metrics.iloc[0]['sharpe']:.2f}")
        return

    # local convenience mode: the fan-out executed serially (submission loop,
    # not implementation loop — each iteration is exactly one shard's work)
    print(f"run coverage: {cov['trade_day_range']['first']} .. {cov['trade_day_range']['last']} "
          f"({cov['trade_day_range']['n_trade_days']} trade days) | "
          f"events {cov['events']['traded']}/{cov['events']['total']} traded")
    run_coverage_outputs(s3io, base, shards, ok)
    for col in exits:
        run_mark(s3io, base, ok, header, col)
    run_finalize(s3io, base, ok)
    print(f"wrote -> {base}/")


if __name__ == "__main__":
    main()
