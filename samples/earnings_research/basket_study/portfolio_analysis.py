"""portfolio_analysis — SPY-overlay portfolio comparison for every matrix-chart
panel and every exit mark.

Model: r_port = w x SPY buy&hold + L x strategy (daily rebalanced).
Portfolios: 0SPY+1strat (w=0,L=1), 1SPY+1strat (w=1,L=1), 1SPY+2strat (w=1,L=2).
Exits: every pnl_<HHMM> mark column in the shards (9:31..9:35, 10:00, 10:30,
... 15:30) plus close ("pnl"); exit code = HHMM or "close".

Coverage = every panel of the exit-matrix and all cell-matrix charts, straight
from configs/study_config.json (deduped by scope label): per scope
(POOLED + 4 constructions) x (POOLED + 4 universes); POOLED = equal weight
across the member cells' daily series. Plus one SPY buy&hold reference row per
exit (so every per-exit slice is self-contained).

Outputs under <s3-prefix>/portfolio_analysis/:
  portfolio_stats.parquet/.csv          CLOSE exit only — legacy layout, unchanged
  monthly_returns.parquet/.csv          CLOSE exit only — legacy layout, unchanged
  portfolio_stats_by_exit.parquet/.csv  all exits, + columns exit, exit_label
  by_exit/<code>/portfolio_stats.parquet
  by_exit/<code>/monthly_returns.parquet  one slice per exit (viewer loads lazily)
  by_exit/<code>/daily_returns.parquet    DAILY strategy returns of every real cell of
      every scope: trade_day + columns "<SCOPE>|<construction>|<universe>" (28 x 16)
      + spy_buy_hold; days with no trade = 0. POOLED panels and the SPY-overlay
      portfolios are derived downstream (mean of member cells; w*SPY + L*strat),
      exactly as aggregate/daily_returns.parquet is used for the ALL scope.
  daily_utilization.parquet             trade_day + "<SCOPE>|<construction>|<universe>":
      share of the cell's capital deployed that day = min(1, MAX_BASKET_WEIGHT x
      baskets traded), exit-independent (--utilization-only refreshes it alone)
  trade_pnl_hist.parquet                per (exit, scope, construction, universe):
      per-TRADE (event x cell row) P&L distribution in bps — n, mean, median,
      std, skew, hit_rate, p01/p05/p25/p75/p95/p99, and a fixed-bin histogram
      (counts over HIST_LO..HIST_HI step HIST_W bps, + n_under / n_over).
      POOLED = union of the member cells' trades. Independent of the SPY leg.

  portfolio_stats columns: scope, construction, universe, portfolio, n_events,
      sharpe, ann_ret_pct, total_pct, max_dd_pct, p2t_days, p2r_days, recovered,
      longest_p2r_days, longest_dd_depth_pct, longest_dd_trough, longest_recovered,
      beat_spy_pct (% of the 2,571 trade days with r_port > r_SPY; for the SPY
      overlays this equals the share of days the strategy leg is positive)
  monthly_returns columns: month, scope, construction, universe, portfolio,
      return_pct, spy_return_pct, excess_vs_spy_pct

    AWS_PROFILE=<profile> python portfolio_analysis.py \
        --s3-prefix earnings-basket-study/results-10y [--verify] [--write]
    ... --hist-only --write      # refresh trade_pnl_hist.parquet alone

Shards come from <s3-prefix>/shards_consolidated/shards.parquet (one S3 read,
see consolidate_shards.py); --cache may name another s3:// parquet.

--verify compares the close-exit stats AND monthly rows against the existing
top-level files and refuses --write on mismatch.
"""
import argparse
import datetime as dt
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

import aggregate  # noqa: E402
import settings
import study_config  # noqa: E402
from benchmark import spy_returns  # noqa: E402
from finalize_local import load_ok
from fees import fee_suffix  # noqa: E402
from index_filter import IndexFilter  # noqa: E402
from s3io import S3IO  # noqa: E402

PORTFOLIOS = {"0SPY+1strat": (0.0, 1.0), "1SPY+1strat": (1.0, 1.0), "1SPY+2strat": (1.0, 2.0)}
CONS = list(aggregate.CONS_COLORS)
UNIS = list(aggregate.UNIV_STYLE)
# scope labels in the 2026-08-27 artifacts (short exit-matrix keys); cell-matrix
# titles map onto the same scopes and are deduped by config key
LABELS = {"midlarge": "MID+LARGE", "midsmall": "MID+SMALL", "mid": "MID", "large": "LARGE",
          "exlarge": "EX-LARGE", "ndx": "NDX", "spx": "SPX", "ndxspx": "NDX+SPX",
          "rui": "R1000", "rut": "R2000", "rua": "R3000", "sptm": "SP1500", "sec7": "SEC-7"}
STAT_COLS = ["scope", "construction", "universe", "portfolio", "n_events", "sharpe",
             "ann_ret_pct", "total_pct", "max_dd_pct", "p2t_days", "p2r_days", "recovered",
             "longest_p2r_days", "longest_dd_depth_pct", "longest_dd_trough", "longest_recovered",
             "beat_spy_pct",      # % of trade days with r_port > r_SPY (no-trade days count as 0)
             "wiped_out"]         # equity hit <= 0 on some day (floored at 0 from then on)
MONTH_COLS = ["month", "scope", "construction", "universe", "portfolio",
              "return_pct", "spy_return_pct", "excess_vs_spy_pct"]
HIST_LO, HIST_HI, HIST_W = -3000, 3000, 25          # bps; viewer re-bins to multiples of HIST_W
HIST_EDGES = np.arange(HIST_LO, HIST_HI + HIST_W, HIST_W)


def exit_code(col: str) -> str:
    return "close" if col == "pnl" else col[len("pnl_"):]


def exit_label(col: str) -> str:
    return "close" if col == "pnl" else f"{int(col[4:6])}:{col[6:8]}"


def scopes():
    """Ordered (label, scope-dict) — exit-matrix rows first, then cell-matrix-only."""
    agg = study_config.load()["aggregation"]
    out, seen = [], set()
    for s in agg["exit_matrix_rows"] + agg["cell_matrices"]:
        if s["key"] in seen:
            continue
        seen.add(s["key"])
        out.append((s.get("label") or LABELS.get(s["key"], s["key"].upper()), s))
    return out


def longest_episode(eq: pd.Series) -> dict:
    """Longest drawdown episode by peak->recovery (ongoing counted to sample
    end, longest_recovered False). First episode wins ties."""
    v = eq.to_numpy(dtype=float)
    n = len(v)
    under = v < np.maximum.accumulate(v)
    d = np.diff(under.astype(np.int8), prepend=0, append=0)
    starts, ends = np.flatnonzero(d == 1), np.flatnonzero(d == -1)   # end = first day back at peak
    best = None
    for s, e in zip(starts, ends):
        start = s - 1                                # last index at the peak
        recovered = e < n
        end = e if recovered else n - 1
        p2r = end - start
        if best is None or p2r > best["longest_p2r_days"]:
            trough_i = start + int(np.argmin(v[start:end + 1]))
            best = {"longest_p2r_days": int(p2r),
                    "longest_dd_depth_pct": float(v[trough_i] / v[start] - 1) * 100,
                    "longest_dd_trough": str(eq.index[trough_i]),
                    "longest_recovered": bool(recovered)}
    return best or {"longest_p2r_days": 0, "longest_dd_depth_pct": 0.0,
                    "longest_dd_trough": None, "longest_recovered": True}


def stats_for(r: pd.Series, spy_bh: pd.Series = None) -> dict:
    eq = pd.Series(np.cumprod(1 + r.to_numpy()), index=r.index)
    # Wipe-out: a daily loss of 100%+ (possible with large per-basket caps and
    # leverage) takes equity to <= 0; compounding past that point is meaningless
    # (sign flips, fractional powers go complex). Floor equity at 0 from the
    # first such day and flag the row.
    wiped = bool((eq <= 0).any())
    if wiped:
        first = int(np.argmax(eq.to_numpy() <= 0))
        eq.iloc[first:] = 0.0
    dd = aggregate.drawdown_stats(eq)
    n = len(r)
    beat = float((r.to_numpy() > spy_bh.to_numpy()).mean()) * 100 if spy_bh is not None else np.nan
    return {"sharpe": aggregate.sharpe(r.to_numpy()), "beat_spy_pct": beat, "wiped_out": wiped,
            "ann_ret_pct": -100.0 if wiped else (float(eq.iloc[-1]) ** (aggregate.TRADING_DAYS / n) - 1) * 100,
            "total_pct": (float(eq.iloc[-1]) - 1) * 100,
            "max_dd_pct": dd["max_dd_pct"], "p2t_days": dd["dd_peak_to_trough_days"],
            "p2r_days": dd["dd_peak_to_recovery_days"], "recovered": dd["dd_recovered"],
            **longest_episode(eq)}


def monthly_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Calendar-month compounded returns in % for every column (index = trade days)."""
    per = pd.to_datetime(df.index).to_period("M")
    return ((1 + df).groupby(per).prod() - 1) * 100


def daily_multi(sub: pd.DataFrame, exits: list) -> dict:
    """aggregate.daily_returns for every exit column from ONE groupby.
    Returns {exit_col: DataFrame(trade_day x 'construction|universe')}."""
    sub = sub.copy()
    sub["cell"] = sub["construction"] + "|" + sub["universe"]
    days = sorted(sub["trade_day"].unique())
    g = sub.groupby(["trade_day", "cell"])
    mean = g[exits].mean()
    scale = (g.size().unstack("cell").reindex(days) * aggregate.MAX_BASKET_WEIGHT).clip(upper=1.0)
    out = {}
    for c in exits:
        mat = (mean[c].unstack("cell").reindex(days) * scale).fillna(0.0)
        mat.index.name = "trade_day"
        out[c] = mat
    return out


# Unhedged cells (README decision #7, 2026-09-24): reported as their own row,
# NEVER a member of any POOLED average — POOLED stays the mean of the 4 x 4
# hedged grid, so every pre-existing number is unchanged when these rows are
# unioned in (finalize_local.load_ok(extra=...)).
EXTRA_CELLS = [("reporter_only", "none"),
               ("spy_beta_levels_neutral", "SPY"), ("spy_beta_returns_neutral", "SPY"),
               ("spy_dollar_neutral", "SPY")]


EXTRA_CONS = [c for c, _ in EXTRA_CELLS]


def n_events_by_cell(sub: pd.DataFrame):
    """(n_events of the hedged grid, {extra construction: its own n_events}).
    The grid's count is over hedged rows only, so unioning extra cells leaves
    every pre-existing n_events unchanged; each extra cell counts its own events."""
    is_extra = sub["construction"].isin(EXTRA_CONS).to_numpy()
    n_hedged = int(sub.loc[~is_extra, "event_id"].nunique())
    n_extra = {c: int(g["event_id"].nunique())
               for c, g in sub[is_extra].groupby("construction", observed=True)}
    return n_hedged, n_extra


def cells():
    """Ordered (construction, universe, member constructions, member universes):
    (POOLED + 4) x (POOLED + 4) hedged cells, then each unhedged cell alone."""
    for c in ["POOLED"] + CONS:
        for u in ["POOLED"] + UNIS:
            yield c, u, (CONS if c == "POOLED" else [c]), (UNIS if u == "POOLED" else [u])
    for c, u in EXTRA_CELLS:
        yield c, u, [c], [u]


def cell_masks(con: np.ndarray, uni: np.ndarray):
    """Yield (construction, universe, row mask) over cells() for trade rows
    with the given construction / universe arrays; empty cells are skipped."""
    for c, u, cs, us in cells():
        m = np.isin(con, cs) & np.isin(uni, us)
        if m.any():
            yield c, u, m


def panels(daily: pd.DataFrame):
    """Yield (construction, universe, series) for every cells() entry present."""
    for c, u, cs, us in cells():
        cols = [f"{cc}|{uu}" for cc in cs for uu in us if f"{cc}|{uu}" in daily.columns]
        if cols:
            yield c, u, daily[cols].mean(axis=1)


def _scope_iter(ok, profile):
    for label, sc in scopes():
        sub = study_config.scope_filter(sc, lambda idx: IndexFilter(idx, profile=profile))(ok)
        yield label, sub


def trade_hist_rows(sub: pd.DataFrame, exits: list, label: str) -> list:
    """Per-trade P&L distribution rows for one scope: every exit x
    (POOLED + 4 constructions) x (POOLED + 4 universes)."""
    rows = []
    for c, u, m in cell_masks(sub["construction"].to_numpy(), sub["universe"].to_numpy()):
        for col in exits:
            v = sub.loc[m, col].to_numpy(dtype=float) * 1e4
            v = v[~np.isnan(v)]
            if not len(v):
                continue
            counts, _ = np.histogram(v, bins=HIST_EDGES)
            q = np.percentile(v, [1, 5, 25, 50, 75, 95, 99])
            rows.append({"exit": exit_code(col), "exit_label": exit_label(col), "scope": label,
                         "construction": c, "universe": u, "n_trades": int(len(v)),
                         "mean_bps": float(v.mean()), "median_bps": float(q[3]),
                         "std_bps": float(v.std(ddof=1)) if len(v) > 1 else 0.0,
                         "skew": float(pd.Series(v).skew()) if len(v) > 2 else 0.0,
                         "hit_rate": float((v > 0).mean()),
                         "p01_bps": float(q[0]), "p05_bps": float(q[1]), "p25_bps": float(q[2]),
                         "p75_bps": float(q[4]), "p95_bps": float(q[5]), "p99_bps": float(q[6]),
                         "bin_lo_bps": HIST_LO, "bin_w_bps": HIST_W,
                         "counts": counts.astype(np.int32),
                         "n_under": int((v < HIST_LO).sum()), "n_over": int((v >= HIST_HI).sum())})
    return rows


def build_utilization(ok: pd.DataFrame, profile=None) -> pd.DataFrame:
    """Daily capital utilization per real cell of every scope (POOLED derived
    downstream as the mean of member cells, like the returns)."""
    days = sorted(ok["trade_day"].unique())
    parts = {}
    for label, sub in _scope_iter(ok, profile):
        if not len(sub):
            continue
        cell = sub["construction"] + "|" + sub["universe"]
        n = sub.groupby([sub["trade_day"], cell]).size().unstack().reindex(days).fillna(0)
        util = (n * aggregate.MAX_BASKET_WEIGHT).clip(upper=1.0)
        for c in util.columns:
            parts[f"{label}|{c}"] = util[c].to_numpy()
    out = pd.DataFrame(parts, index=pd.Index(days, name="trade_day"))
    return out.reset_index()


def build_hist(ok: pd.DataFrame, profile=None, exits=None) -> pd.DataFrame:
    exits = exits or aggregate._exits(ok)
    rows = []
    for label, sub in _scope_iter(ok, profile):
        print(f"  hist {label:10s} {len(sub):8d} trade rows", flush=True)
        if len(sub):
            rows += trade_hist_rows(sub, exits, label)
    return pd.DataFrame(rows)


def write_hist(s3io, hist: pd.DataFrame, uri: str):
    """Parquet with the list column via pyarrow (polars from_pandas chokes on
    object arrays); streamed to S3, no local file."""
    import io
    import pyarrow as pa
    import pyarrow.parquet as pq
    cols = {c: hist[c].tolist() if c == "counts" else hist[c].to_numpy() for c in hist.columns}
    table = pa.table({**{c: pa.array(v) for c, v in cols.items() if c != "counts"},
                      "counts": pa.array(cols["counts"], type=pa.list_(pa.int32()))})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    s3io.write_bytes(buf.getvalue(), uri)


def build(ok: pd.DataFrame, spy: pd.DataFrame, profile=None, exits=None):
    """-> (stats, monthly) long frames with exit/exit_label columns, all exits."""
    exits = exits or aggregate._exits(ok)
    days = sorted(ok["trade_day"].unique())
    spy_bh = spy["buy_hold"].reindex(days).fillna(0.0)
    spy_m = monthly_frame(spy_bh.to_frame("spy"))["spy"]
    spy_m.index = spy_m.index.astype(str)
    stat_rows, month_frames = [], []
    daily_by_exit = {exit_code(col): {} for col in exits}          # exit -> scope -> daily cells
    for label, sub in _scope_iter(ok, profile):
        n_events, n_extra = n_events_by_cell(sub) if len(sub) else (0, {})
        print(f"  {label:10s} {n_events:7d} events" + (f" + extra {n_extra}" if n_extra else ""), flush=True)
        if not len(sub):
            continue
        dm = daily_multi(sub, exits)
        for col in exits:
            code, elabel = exit_code(col), exit_label(col)
            daily = dm[col].reindex(days).fillna(0.0)
            daily_by_exit[code][label] = daily
            series = {}
            for c, u, strat in panels(daily):
                for pname, (w, lev) in PORTFOLIOS.items():
                    r = lev * strat + w * spy_bh
                    series[(c, u, pname)] = r
                    stat_rows.append({"exit": code, "exit_label": elabel, "scope": label,
                                      "construction": c, "universe": u, "portfolio": pname,
                                      "n_events": float(n_extra.get(c, n_events)), **stats_for(r, spy_bh)})
            frame = pd.DataFrame(series)
            frame.columns = pd.MultiIndex.from_tuples(series.keys(),
                                                      names=["construction", "universe", "portfolio"])
            mf = monthly_frame(frame)
            mf.index = mf.index.astype(str).rename("month")
            long = mf.stack(["construction", "universe", "portfolio"], future_stack=True)
            long = long.rename("return_pct").reset_index()
            long["exit"], long["exit_label"], long["scope"] = code, elabel, label
            long["spy_return_pct"] = long["month"].map(spy_m).astype(float)
            long["excess_vs_spy_pct"] = long["return_pct"] - long["spy_return_pct"]
            month_frames.append(long.sort_values(["construction", "universe", "portfolio", "month"]))
    spy_stats = stats_for(spy_bh)
    for col in exits:
        stat_rows.append({"exit": exit_code(col), "exit_label": exit_label(col), "scope": "SPY",
                          "construction": "-", "universe": "-", "portfolio": "SPY buy&hold",
                          "n_events": np.nan, **spy_stats})
    stats = pd.DataFrame(stat_rows)[["exit", "exit_label"] + STAT_COLS]
    months = pd.concat(month_frames, ignore_index=True)[["exit", "exit_label"] + MONTH_COLS]
    dailies = {}
    for code, per_scope in daily_by_exit.items():
        wide = pd.concat({lab: d for lab, d in per_scope.items()}, axis=1)
        wide.columns = [f"{lab}|{cell}" for lab, cell in wide.columns]
        wide["spy_buy_hold"] = spy_bh.to_numpy()
        wide.index = pd.Index(days, name="trade_day")
        dailies[code] = wide.reset_index()
    return stats, months, dailies


def _arrow_safe(df: pd.DataFrame) -> pd.DataFrame:
    """Object columns with mixed Python types (bool/None, str/None, numpy bools)
    break pl.from_pandas ("Unsupported numpy type 15"); normalise them."""
    df = df.copy()
    for c in df.columns[df.dtypes == object]:
        vals = df[c].tolist()
        nonnull = [v for v in vals if v is not None and not (isinstance(v, float) and np.isnan(v))]
        if nonnull and all(isinstance(v, (bool, np.bool_)) for v in nonnull):
            df[c] = pd.array([None if v is None or (isinstance(v, float) and np.isnan(v)) else bool(v) for v in vals],
                             dtype="boolean")
        else:
            df[c] = pd.array([None if v is None or (isinstance(v, float) and np.isnan(v)) else str(v) for v in vals],
                             dtype="string")
    return df


def _report(name, m, keys, num, flags):
    bad = 0
    for c in num:
        a, b = m[f"{c}_old"].astype(float), m[f"{c}_new"].astype(float)
        diff = ~(np.isclose(a, b, rtol=1e-6, atol=1e-6) | (a.isna() & b.isna()))
        if diff.any():
            bad += int(diff.sum())
            print(f"VERIFY {name}: {c}: {int(diff.sum())} mismatches, e.g.\n"
                  f"{m.loc[diff, keys + [f'{c}_old', f'{c}_new']].head(3).to_string()}")
    for c in flags:
        diff = m[f"{c}_old"].astype(str) != m[f"{c}_new"].astype(str)
        if diff.any():
            bad += int(diff.sum())
            print(f"VERIFY {name}: {c}: {int(diff.sum())} mismatches, e.g.\n"
                  f"{m.loc[diff, keys + [f'{c}_old', f'{c}_new']].head(3).to_string()}")
    return bad


def verify(new: pd.DataFrame, old: pd.DataFrame, name="stats") -> bool:
    if name == "stats":
        keys = ["scope", "construction", "universe", "portfolio"]
        num = ["n_events", "sharpe", "ann_ret_pct", "total_pct", "max_dd_pct", "p2t_days", "p2r_days",
               "longest_p2r_days", "longest_dd_depth_pct"]
        flags = ["recovered", "longest_recovered", "longest_dd_trough"]
    else:
        keys = ["month", "scope", "construction", "universe", "portfolio"]
        num = ["return_pct", "spy_return_pct", "excess_vs_spy_pct"]
        flags = []
    m = old.merge(new, on=keys, suffixes=("_old", "_new"), how="left", indicator=True)
    missing = m[m["_merge"] != "both"]
    if len(missing):
        print(f"VERIFY {name}: {len(missing)} old rows have no new counterpart:\n{missing[keys].head()}")
    bad = _report(name, m, keys, num, flags)
    ok_ = bad == 0 and len(missing) == 0
    print(f"VERIFY {name}: {'PASS' if ok_ else 'FAIL'} — {len(old)} old rows compared, "
          f"{bad} value mismatches", flush=True)
    return ok_


README_ADDENDUM = """
Changes 2026-09-10 (exit slices): every exit mark in the shards (9:31..9:35,
10:00, 10:30, ... 15:30, close) is now covered. Top-level portfolio_stats /
monthly_returns stay CLOSE-only (legacy layout). New: portfolio_stats_by_exit
(+ exit, exit_label columns; SPY row repeated per exit) and by_exit/<code>/
{portfolio_stats,monthly_returns}.parquet, code = HHMM or "close". Producer
--verify reproduced the previous close-exit stats and monthly rows exactly.
by_exit/<code>/daily_returns.parquet (2026-09-11): DAILY strategy returns of every
real cell of every scope ("<SCOPE>|<construction>|<universe>") + spy_buy_hold, so
every viewer plot is daily; POOLED/portfolios derived downstream.
trade_pnl_hist.parquet: per-trade P&L distribution (bps) per exit x scope x
panel — fixed 25-bps bins over [-3000, 3000) + under/over counts, quantiles,
hit rate, skew. Trades = event x cell shard rows with status ok.
"""


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--s3-prefix", default="earnings-basket-study/results-10y")
    ap.add_argument("--cache", default=None,
                    help="alternative s3:// parquet of all shards; default = shards_consolidated/")
    ap.add_argument("--profile", default=None)
    ap.add_argument("--verify", action="store_true",
                    help="compare close-exit rows against the existing top-level files")
    ap.add_argument("--write", action="store_true",
                    help="upload to <s3-prefix>/portfolio_analysis/ (previous files copied to _prev_<date>/)")
    ap.add_argument("--local-out", default=None, help="also write parquet/csv here")
    ap.add_argument("--hist-only", action="store_true",
                    help="only (re)build trade_pnl_hist.parquet; skips stats/monthly and --verify")
    ap.add_argument("--utilization-only", action="store_true",
                    help="only (re)build daily_utilization.parquet")
    ap.add_argument("--max-basket-weight", type=float, default=None,
                    help="per-basket capital cap as a fraction (default: study config, 0.02). A non-default "
                         "cap writes to portfolio_analysis_cap<pct>/ and skips --verify and the "
                         "cap-independent trade_pnl_hist")
    ap.add_argument("--fee-per-share", type=float, default=0.0,
                    help="round-trip per-share commission in USD (IBKR Pro Fixed = 0.005) netted out of "
                         "every P&L column before any statistic; writes to portfolio_analysis<cap>_fee<mills>m/ "
                         "(fees.fee_suffix). Needs shards with shares_per_dollar (--cache the v2 table)")
    ap.add_argument("--extra-shards", action="append", default=None,
                    help="additional consolidated shard table(s) unioned in (ok rows, study event set only), "
                         "e.g. the reporter_only cells; repeatable (finalize_local.load_ok extra=)")
    args = ap.parse_args()

    suffix = ""
    cap_suffix = ""
    if args.max_basket_weight is not None and abs(args.max_basket_weight - aggregate.MAX_BASKET_WEIGHT) > 1e-12:
        aggregate.MAX_BASKET_WEIGHT = args.max_basket_weight
        cap_suffix = f"_cap{args.max_basket_weight * 100:g}"
        args.verify = False
    fee_sfx = fee_suffix(args.fee_per_share)
    if fee_sfx:
        args.verify = False
    suffix = cap_suffix + fee_sfx
    print(f"per-basket cap {aggregate.MAX_BASKET_WEIGHT:.0%}, fee ${args.fee_per_share:g}/share "
          f"-> portfolio_analysis{suffix}/", flush=True)
    s3io = S3IO(profile=args.profile)
    ok = load_ok(s3io, args.s3_prefix, args.cache, fee_per_share=args.fee_per_share,
                 extra=args.extra_shards or ())
    pa = f"s3://{settings.get("s3", "data_bucket")}/{args.s3_prefix}/portfolio_analysis{suffix}"
    util = build_utilization(ok, profile=args.profile)
    print(f"daily utilization {util.shape} (basket cap {aggregate.MAX_BASKET_WEIGHT:.0%})", flush=True)
    if args.utilization_only:
        if args.write:
            import polars as pl
            s3io.write_parquet(pl.from_pandas(util), f"{pa}/daily_utilization.parquet")
            print(f"uploaded {pa}/daily_utilization.parquet")
        return
    hist = None
    if not cap_suffix or args.hist_only:        # per-trade histograms do not depend on the cap (they do on the fee)
        hist = build_hist(ok, profile=args.profile)
        print(f"trade histograms {hist.shape}", flush=True)
    if args.hist_only:
        if args.write:
            write_hist(s3io, hist, f"{pa}/trade_pnl_hist.parquet")
            print(f"uploaded {pa}/trade_pnl_hist.parquet")
        return
    days = sorted(ok["trade_day"].unique())
    spy = spy_returns(s3io, days)
    if spy is None:
        raise SystemExit("SPY benchmark data required")
    stats, months, dailies = build(ok, spy, profile=args.profile)
    close_stats = stats[stats["exit"] == "close"][STAT_COLS].reset_index(drop=True)
    close_months = months[months["exit"] == "close"][MONTH_COLS].reset_index(drop=True)
    print(f"stats {stats.shape} (close {close_stats.shape}), monthly {months.shape}, "
          f"daily {len(dailies)} exits x {dailies['close'].shape}", flush=True)

    if args.verify:
        old_s = s3io.read_parquet(f"{pa}/portfolio_stats.parquet").to_pandas()
        old_m = s3io.read_parquet(f"{pa}/monthly_returns.parquet").to_pandas()
        passed = verify(close_stats, old_s, "stats") & verify(close_months, old_m, "monthly")
        if not passed and args.write:
            raise SystemExit("refusing --write after VERIFY FAIL")

    if args.local_out:
        out = Path(args.local_out); out.mkdir(parents=True, exist_ok=True)
        close_stats.to_parquet(out / "portfolio_stats.parquet", index=False)
        close_months.to_parquet(out / "monthly_returns.parquet", index=False)
        stats.to_parquet(out / "portfolio_stats_by_exit.parquet", index=False)
        months.to_parquet(out / "monthly_returns_by_exit.parquet", index=False)
        print(f"wrote {out}")

    if args.write:
        import polars as pl
        tag = dt.date.today().isoformat()
        for f in ["portfolio_stats.parquet", "portfolio_stats.csv",
                  "monthly_returns.parquet", "monthly_returns.csv", "README.md"]:
            try:
                s3io.write_bytes(s3io.read_bytes(f"{pa}/{f}"), f"{pa}/_prev_{tag}/{f}")
            except Exception as e:  # first run / missing file
                print(f"backup skip {f}: {e}")
        close_stats, stats = _arrow_safe(close_stats), _arrow_safe(stats)
        s3io.write_parquet(pl.from_pandas(close_stats), f"{pa}/portfolio_stats.parquet")
        s3io.write_csv(pl.from_pandas(close_stats), f"{pa}/portfolio_stats.csv")
        s3io.write_parquet(pl.from_pandas(close_months), f"{pa}/monthly_returns.parquet")
        s3io.write_csv(pl.from_pandas(close_months), f"{pa}/monthly_returns.csv")
        s3io.write_parquet(pl.from_pandas(stats), f"{pa}/portfolio_stats_by_exit.parquet")
        s3io.write_csv(pl.from_pandas(stats), f"{pa}/portfolio_stats_by_exit.csv")
        for code, st in stats.groupby("exit"):
            s3io.write_parquet(pl.from_pandas(st.reset_index(drop=True)),
                               f"{pa}/by_exit/{code}/portfolio_stats.parquet")
            mo = months[months["exit"] == code].reset_index(drop=True)
            s3io.write_parquet(pl.from_pandas(mo), f"{pa}/by_exit/{code}/monthly_returns.parquet")
            s3io.write_parquet(pl.from_pandas(dailies[code]), f"{pa}/by_exit/{code}/daily_returns.parquet")
        if hist is not None:
            write_hist(s3io, hist, f"{pa}/trade_pnl_hist.parquet")
        s3io.write_parquet(pl.from_pandas(util), f"{pa}/daily_utilization.parquet")
        if suffix:
            fee_note = ("" if not fee_sfx else
                        f" Every P&L column is NET of a round-trip per-share commission of ${args.fee_per_share:g} "
                        f"(IBKR Pro Fixed; both sides, every leg incl. peers; fee = 2 x rate x shares_per_dollar "
                        f"from the v2 shards {args.cache or ''}). Not modelled: $1 per-order minimum, 1%-of-value "
                        f"cap, exchange/regulatory pass-throughs.")
            s3io.write_text(f"# portfolio_analysis{suffix}\n\nSame layout and producer as ../portfolio_analysis/ "
                            f"but with the per-basket capital cap set to {aggregate.MAX_BASKET_WEIGHT:.0%} "
                            f"(study default 2%).{fee_note} Shards' gross P&L is unchanged; the daily capital "
                            f"model is each basket = min(1/N, cap) of capital. trade_pnl_hist is "
                            f"cap-independent and lives only in the default-cap directory of each fee variant. "
                            f"Built {dt.date.today()}.\n",
                            f"{pa}/README.md")
        else:
            try:
                readme = s3io.read_bytes(f"{pa}/README.md").decode()
                if "exit slices" not in readme:
                    s3io.write_text(readme.rstrip() + "\n" + README_ADDENDUM, f"{pa}/README.md")
            except Exception as e:
                print(f"README update skipped: {e}")
        print(f"uploaded to {pa}/ ({stats['exit'].nunique()} exits)")


if __name__ == "__main__":
    main()
