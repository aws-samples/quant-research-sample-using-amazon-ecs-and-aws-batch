"""10-year basket-study viewer: strategy with vs. without an S&P 500 (SPY) leg.

Served by results_browser/server.py at /study10y (page) and /study10y/data
(JSON). Reads the finalized artifacts of the 10-year fade-the-gap study
straight from S3, read-only:

  results-10y/portfolio_analysis/by_exit/<exit>/daily_returns.parquet
      DAILY strategy return of every real cell of every scope (columns
      'SCOPE|construction|universe') + spy_buy_hold; one file per exit mark
      (0931..0935, 1000, 1030, ... 1530, close), loaded lazily. POOLED panels
      = mean of member cells; portfolios = w*SPY + L*strategy, computed here.
      Every plot is daily (month-end sampling hid the 2020 SPY drawdown).
  results-10y/portfolio_analysis/portfolio_stats_by_exit.parquet
      daily-based Sharpe / ann. return / total / drawdown per panel x exit
  results-10y/portfolio_analysis/trade_pnl_hist.parquet
      per-trade (event x cell) P&L distribution per exit x scope x panel:
      25-bps histogram counts over [-3000, 3000) bps + quantiles/hit rate

Variants: ?cap=<pct> per-basket capital cap (portfolio_analysis_cap<pct>/) and
?fee=<mills> per-share commission (portfolio_analysis[_cap<pct>]_fee<mills>m/,
fees.py: $0.005/share IBKR Pro Fixed, both sides, every leg). fee=0 reads the
original gross artifacts unchanged.

Portfolio model (aggregate.py --spy-weight w --strategy-leverage L):
    r_port = w x SPY buy&hold + L x strategy, daily rebalanced.
"Without S&P" = w=0, L=1.  "With S&P" = w=1, L=1 or L=2.  Exit = when every
basket is unwound on the trade day (9:31 .. 15:30 marks or the RTH close).
"""

import html
import io
import json
import math

import pandas as pd

BASE = "earnings-basket-study/results-10y/"
PA = BASE + "portfolio_analysis/"            # study default cap (2% per basket)
CAPS = [2, 3, 4, 5, 10, 15, 20, 25]          # per-basket capital caps with a portfolio_analysis run
DEFAULT_CAP = 2


# transaction-cost variants (fees.py): per-share commission in mills ($0.001/share);
# 0 = gross (the original artifacts), 5 = $0.005/share IBKR Pro Fixed, both sides,
# every leg. Each fee variant has its own portfolio_analysis[_cap<pct>]_fee<mills>m/ run.
FEES = [0, 5]
DEFAULT_FEE = 0
FEE_LABEL = {0: "none (gross)", 5: "$0.005 per share (IBKR Pro Fixed)"}


def _pa(cap, fee=DEFAULT_FEE):
    """portfolio_analysis directory for a (per-basket cap, per-share fee) variant."""
    cap_sfx = "" if cap == DEFAULT_CAP else f"_cap{cap}"
    fee_sfx = "" if fee == DEFAULT_FEE else f"_fee{fee}m"
    return BASE + f"portfolio_analysis{cap_sfx}{fee_sfx}/"
# portfolio_analysis labels -> viewer keys; viewer key -> (SPY weight w, strategy leverage L)
PA_PORTFOLIO = {"0SPY+1strat": "strat", "1SPY+1strat": "spy1_strat1", "1SPY+2strat": "spy1_strat2"}
PORTFOLIO_WL = {"strat": (0.0, 1.0), "spy1_strat1": (1.0, 1.0), "spy1_strat2": (1.0, 2.0)}
# exit marks present in the shards: (code used in files/URLs, label)
EXITS = ([(f"09{m}", f"9:{m}") for m in ("31", "32", "33", "34", "35")]
         + [(f"{h:02d}{m}", f"{h}:{m}") for h in range(10, 16) for m in ("00", "30")]
         + [("close", "close")])
EXIT_LABEL = dict(EXITS)
DEFAULT_EXIT = "close"

# Fixed entity -> colour mapping (dataviz: colour follows the entity, never
# its rank; palette validated light+dark with scripts/validate_palette.js).
SERIES = [
    ("strat",       "Strategy only (no S&P)",        "#2a78d6", "#3987e5"),
    ("spy1_strat1", "Strategy + 1x S&P (SPY)",       "#eb6834", "#d95926"),
    ("spy1_strat2", "2x Strategy + 1x S&P (SPY)",    "#1baf7a", "#199e70"),
    ("spy",         "S&P 500 buy & hold (SPY)",      "#4a3aa7", "#9085e9"),
]

SCOPES = [  # (key, label, group) — keys match portfolio_analysis 'scope'
    ("ALL", "All reporters", "Liquidity scope"),
    ("EX-MICRO", "Ex-micro (small+mid+large)", "Liquidity scope"),
    ("EX-LARGE", "Ex-large (micro+small+mid)", "Liquidity scope"),
    ("MID+LARGE", "Mid + Large", "Liquidity scope"),
    ("MID+SMALL", "Mid + Small", "Liquidity scope"),
    ("LARGE", "Large", "Liquidity scope"),
    ("MID", "Mid", "Liquidity scope"),
    ("SMALL", "Small", "Liquidity scope"),
    ("MICRO", "Micro", "Liquidity scope"),
    ("SPX", "S&P 500 reporters", "Index membership"),
    ("NDX", "Nasdaq-100 reporters", "Index membership"),
    ("NDX+SPX", "Nasdaq-100 or S&P 500", "Index membership"),
    ("SP1500", "S&P 1500 (SPTM)", "Index membership"),
    ("R1000", "Russell 1000", "Index membership"),
    ("R2000", "Russell 2000", "Index membership"),
    ("R3000", "Russell 3000", "Index membership"),
    ("SEC-7", "SEC-7 (7 sector SPDRs)", "Index membership"),
    ("XLK", "Technology (XLK)", "Sector SPDR"),
    ("XLF", "Financials (XLF)", "Sector SPDR"),
    ("XLV", "Health care (XLV)", "Sector SPDR"),
    ("XLY", "Consumer discr. (XLY)", "Sector SPDR"),
    ("XLP", "Consumer staples (XLP)", "Sector SPDR"),
    ("XLI", "Industrials (XLI)", "Sector SPDR"),
    ("XLE", "Energy (XLE)", "Sector SPDR"),
    ("XLB", "Materials (XLB)", "Sector SPDR"),
    ("XLC", "Communication (XLC)", "Sector SPDR"),
    ("XLU", "Utilities (XLU)", "Sector SPDR"),
    ("XLRE", "Real estate (XLRE)", "Sector SPDR"),
    # reporter 9:30 entry price, left-closed bins (study_config price_bins, 2026-09-23)
    ("PX<1", "Under $1", "Reporter price (9:30 entry)"),
    ("PX1-5", "$1 to $5", "Reporter price (9:30 entry)"),
    ("PX5-10", "$5 to $10", "Reporter price (9:30 entry)"),
    ("PX10-50", "$10 to $50", "Reporter price (9:30 entry)"),
    ("PX50-100", "$50 to $100", "Reporter price (9:30 entry)"),
    ("PX100+", "$100 and up", "Reporter price (9:30 entry)"),
    ("PX1+", "$1 and up", "Reporter price, cumulative"),
    ("PX5+", "$5 and up", "Reporter price, cumulative"),
    ("PX10+", "$10 and up", "Reporter price, cumulative"),
    ("PX50+", "$50 and up", "Reporter price, cumulative"),
]
# Hedged grid: (POOLED + 4 constructions) x (POOLED + 4 peer universes). Extra
# single-cell constructions (README decisions #7/#8, 2026-09-24) live under a
# pseudo-universe each and are never part of POOLED:
#   reporter_only            -> "none"  (outright reporter, no hedge)
#   spy_beta_levels_neutral, spy_beta_returns_neutral, spy_dollar_neutral -> "SPY"
CONSTRUCTIONS = ["POOLED", "equal_weight_dollar_neutral", "ridge_returns_beta_neutral",
                 "ridge_returns_dollar_neutral", "ridge_levels_beta_neutral", "reporter_only",
                 "spy_beta_levels_neutral", "spy_beta_returns_neutral", "spy_dollar_neutral"]
UNIVERSES = ["POOLED", "all", "correlated", "functional", "pure play", "none", "SPY"]
UNHEDGED = {"reporter_only": "none",          # construction -> its sole pseudo-universe
            "spy_beta_levels_neutral": "SPY", "spy_beta_returns_neutral": "SPY", "spy_dollar_neutral": "SPY"}
HEDGED_CONS = [c for c in CONSTRUCTIONS[1:] if c not in UNHEDGED]
HEDGED_UNIS = [u for u in UNIVERSES[1:] if u not in UNHEDGED.values()]


def coerce_cell(construction, universe):
    """A single-cell construction implies its pseudo-universe; a pseudo-universe
    with a grid construction selects that universe's first (default) construction."""
    if construction in UNHEDGED:
        return construction, UNHEDGED[construction]
    if universe in UNHEDGED.values():
        return next(c for c, u in UNHEDGED.items() if u == universe), universe
    return construction, universe


def members(construction, universe):
    """(constructions, universes) whose 'c|u' daily columns average into this panel."""
    construction, universe = coerce_cell(construction, universe)
    if construction in UNHEDGED:
        return [construction], [universe]
    return (HEDGED_CONS if construction == "POOLED" else [construction],
            HEDGED_UNIS if universe == "POOLED" else [universe])

_get = None          # bytes-for-key callable, injected by server.py
_cache = {}


def init(get_object):
    global _get
    _get = get_object


def _parquet(key):
    if key not in _cache:
        _cache[key] = pd.read_parquet(io.BytesIO(_get(key)))
    return _cache[key]


def _daily(exit_code, cap=DEFAULT_CAP, fee=DEFAULT_FEE):
    """Daily strategy returns of every real cell of every scope at one exit
    (columns 'SCOPE|construction|universe' + spy_buy_hold), indexed by day."""
    key = _pa(cap, fee) + f"by_exit/{exit_code}/daily_returns.parquet"
    if key not in _cache:
        df = pd.read_parquet(io.BytesIO(_get(key)))
        df["trade_day"] = pd.to_datetime(df["trade_day"])
        _cache[key] = df.set_index("trade_day")
    return _cache[key]


def _stats(cap=DEFAULT_CAP, fee=DEFAULT_FEE):
    """All exits; columns exit, exit_label + the legacy portfolio_stats layout."""
    return _parquet(_pa(cap, fee) + "portfolio_stats_by_exit.parquet")


def _hist(fee=DEFAULT_FEE):
    """Per-trade P&L histograms: cap-independent, fee-dependent (default-cap dir of the fee variant)."""
    return _parquet(_pa(DEFAULT_CAP, fee) + "trade_pnl_hist.parquet")


def _excursion(name, fee=DEFAULT_FEE):
    """excursion_groups / excursion_touch (excursion_stats.py); None until produced."""
    try:
        return _parquet(_pa(DEFAULT_CAP, fee) + f"excursion_{name}.parquet")
    except Exception:
        return None


MAX_BASKET_WEIGHT_PCT = 2        # study_config aggregation.max_basket_weight


def _util(cap=DEFAULT_CAP):
    """Daily capital utilization per real cell of every scope (fraction), indexed by day."""
    key = _pa(cap) + "daily_utilization.parquet"
    if key not in _cache:
        df = pd.read_parquet(io.BytesIO(_get(key)))
        df["trade_day"] = pd.to_datetime(df["trade_day"])
        _cache[key] = df.set_index("trade_day")
    return _cache[key]


def _util_panel(scope, construction, universe, cap=DEFAULT_CAP):
    """Daily utilization of one panel (fraction); POOLED = mean of member cells. None if absent."""
    try:
        df = _util(cap)
    except Exception:
        return None
    cons, unis = members(construction, universe)
    cols = [f"{scope}|{c}|{u}" for c in cons for u in unis if f"{scope}|{c}|{u}" in df.columns]
    return df[cols].mean(axis=1) if cols else None


def _util_summary(scope, construction, universe, cap=DEFAULT_CAP):
    """(avg utilization %, % of days with any basket) for one panel."""
    u = _util_panel(scope, construction, universe, cap)
    if u is None:
        return None, None
    return float(u.mean()) * 100, float((u > 0).mean()) * 100


STOP_LEVELS = [500, 1000, 1500, 2000]
STOP_COLS = ["stop_bps", "n_trades", "stopped_pct", "stopped_ended_pos_pct", "stopped_unstopped_mean_bps",
             "stopped_fill_mean_bps", "mean_bps", "median_bps", "std_bps", "hit_rate", "p05_bps", "p95_bps",
             "delta_mean_bps", "sharpe", "ann_ret_pct", "total_pct", "max_dd_pct", "p2r_days", "beat_spy_pct"]


def _stop_stats(fee=DEFAULT_FEE):
    try:
        return _parquet(_pa(DEFAULT_CAP, fee) + "stop_sim_stats.parquet")
    except Exception:
        return None


def _daily_stop(exit_code, lvl, fee=DEFAULT_FEE):
    key = _pa(DEFAULT_CAP, fee) + f"by_exit/{exit_code}/daily_returns_stop{lvl}.parquet"
    if key not in _cache:
        df = pd.read_parquet(io.BytesIO(_get(key)))
        df["trade_day"] = pd.to_datetime(df["trade_day"])
        _cache[key] = df.set_index("trade_day")
    return _cache[key]


def _stop_panel(exit_code, lvl, scope, construction, universe, fee=DEFAULT_FEE):
    df = _daily_stop(exit_code, lvl, fee)
    cons, unis = members(construction, universe)
    cols = [f"{scope}|{c}|{u}" for c in cons for u in unis if f"{scope}|{c}|{u}" in df.columns]
    return df[cols].mean(axis=1) if cols else None


EXC_GROUP_COLS = ["group", "n", "share_pct", "mean_final_bps", "mae_p50", "mae_p10", "mae_mean",
                  "mfe_p50", "mfe_p90", "mfe_mean", "touched_m500_pct", "touched_m1000_pct",
                  "touched_m2000_pct", "reached_p500_pct", "reached_p1000_pct", "reached_p2000_pct",
                  "worst_in_open5_pct", "mae_time_median", "mfe_time_median"]
EXC_TOUCH_COLS = ["kind", "threshold_bps", "population", "n_pop", "hit_pct", "mean_final_hit_bps",
                  "mean_final_miss_bps", "ended_positive_pct", "ended_negative_pct"]


HIST_SCALARS = ["construction", "universe", "n_trades", "mean_bps", "median_bps", "std_bps", "skew",
                "hit_rate", "p01_bps", "p05_bps", "p25_bps", "p75_bps", "p95_bps", "p99_bps",
                "bin_lo_bps", "bin_w_bps", "n_under", "n_over"]


def _hist_row(r):
    out = {c: _clean(r[c]) for c in HIST_SCALARS}
    out["counts"] = [int(x) for x in r["counts"]]
    return out


# ----------------------------------------------------------------- numerics
def _cum_dd(r):
    """r: pd.Series of fractional period returns -> (cum %, drawdown %)."""
    eq = (1 + r).cumprod()
    cum = (eq - 1) * 100
    dd = (eq / eq.cummax() - 1) * 100
    return cum, dd


def _daily_panel(exit_code, scope, construction, universe, cap=DEFAULT_CAP, fee=DEFAULT_FEE):
    """Daily fractional strategy return of one panel. POOLED = equal weight
    across the member cells (portfolio_analysis README)."""
    df = _daily(exit_code, cap, fee)
    cons, unis = members(construction, universe)
    cols = [f"{scope}|{c}|{u}" for c in cons for u in unis if f"{scope}|{c}|{u}" in df.columns]
    if not cols:
        raise ValueError(f"no daily series for {scope} / {construction} / {universe}")
    return df[cols].mean(axis=1)


def _series(key, r):
    cum, dd = _cum_dd(r)
    return {"key": key, "dates": [d.strftime("%Y-%m-%d") for d in r.index],
            "cum": [round(x, 4) for x in cum], "dd": [round(x, 4) for x in dd]}


def _clean(v):
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if hasattr(v, "item"):
        return _clean(v.item())
    return v


def _rows(df, cols):
    return [{c: _clean(x) for c, x in zip(cols, rec)} for rec in df[cols].itertuples(index=False)]


# --------------------------------------------------------------------- data
def data(scope, construction, universe, exit_code=DEFAULT_EXIT, cap=DEFAULT_CAP, fee=DEFAULT_FEE):
    if scope not in {s[0] for s in SCOPES}:
        raise ValueError(f"unknown scope {scope!r}")
    if construction not in CONSTRUCTIONS or universe not in UNIVERSES:
        raise ValueError("unknown construction/universe")
    construction, universe = coerce_cell(construction, universe)
    if exit_code not in EXIT_LABEL:
        raise ValueError(f"unknown exit {exit_code!r}")
    if cap not in CAPS:
        raise ValueError(f"unknown per-basket cap {cap!r}")
    if fee not in FEES:
        raise ValueError(f"unknown per-share fee {fee!r} (mills)")

    st_all = _stats(cap, fee)
    st = st_all[st_all.exit == exit_code]
    series, stats = [], {}

    cell_stats = st[(st.scope == scope) & (st.construction == construction) & (st.universe == universe)]
    for _, row in cell_stats.iterrows():
        k = PA_PORTFOLIO.get(row.portfolio)
        if k:
            stats[k] = {c: _clean(row[c]) for c in
                        ["sharpe", "ann_ret_pct", "total_pct", "max_dd_pct", "p2t_days",
                         "p2r_days", "recovered", "n_events", "beat_spy_pct", "wiped_out"] if c in row}
    spy_row = st[st.scope == "SPY"].iloc[0]
    stats["spy"] = {c: _clean(spy_row[c]) for c in
                    ["sharpe", "ann_ret_pct", "total_pct", "max_dd_pct", "p2t_days", "p2r_days", "recovered"]}

    # every plot is DAILY (month-end sampling hid the 2020 SPY drawdown): r_port = w*SPY + L*strategy from the per-exit daily cells
    granularity = "daily"
    strat = _daily_panel(exit_code, scope, construction, universe, cap, fee)
    spy = _daily(exit_code, cap, fee)["spy_buy_hold"]
    for pkey, (w, lev) in PORTFOLIO_WL.items():
        series.append(_series(pkey, lev * strat + w * spy))
    series.append(_series("spy", spy))

    # capital utilization (strategy only, exit-independent): daily + 63-day mean
    util = None
    u = _util_panel(scope, construction, universe, cap)
    if u is not None:
        util = {"dates": [d.strftime("%Y-%m-%d") for d in u.index],
                "pct": [round(x * 100, 2) for x in u],
                "roll63": [None if pd.isna(x) else round(x * 100, 2) for x in u.rolling(63).mean()],
                "avg_pct": float(u.mean()) * 100, "days_traded_pct": float((u > 0).mean()) * 100,
                "max_basket_weight_pct": cap}

    # comparison tables: (a) every construction x universe within this scope,
    # (b) every scope for this construction x universe, (c) every exit for this
    # scope x construction x universe — all three portfolios side by side
    cols = ["sharpe", "ann_ret_pct", "total_pct", "max_dd_pct", "beat_spy_pct", "n_events", "wiped_out"]

    def wide(df, keycols):
        out = {}
        for _, row in df.iterrows():
            k = PA_PORTFOLIO.get(row.portfolio)
            if not k:
                continue
            ident = tuple(row[c] for c in keycols)
            out.setdefault(ident, {c: row[c] for c in keycols})
            for c in cols:
                out[ident][f"{k}.{c}"] = _clean(row[c]) if c in row else None
        return list(out.values())

    cells = wide(st[st.scope == scope], ["construction", "universe"])
    scopes = wide(st[(st.construction == construction) & (st.universe == universe) & (st.scope != "SPY")],
                  ["scope"])
    for r in cells:
        r["util_avg_pct"], r["util_days_pct"] = _util_summary(scope, r["construction"], r["universe"], cap)
    for r in scopes:
        r["util_avg_pct"], r["util_days_pct"] = _util_summary(r["scope"], construction, universe, cap)
    order = {s[0]: i for i, s in enumerate(SCOPES)}
    scopes.sort(key=lambda r: order.get(r["scope"], 99))
    exits = wide(st_all[(st_all.scope == scope) & (st_all.construction == construction)
                        & (st_all.universe == universe)], ["exit", "exit_label"])
    eorder = {code: i for i, (code, _) in enumerate(EXITS)}
    exits.sort(key=lambda r: eorder.get(r["exit"], 99))
    ua, ud = _util_summary(scope, construction, universe, cap)
    for r in exits:
        r["util_avg_pct"], r["util_days_pct"] = ua, ud

    # (d) the same scope x cell x exit under every per-basket cap that has a run (same fee)
    caps = []
    for c in CAPS:
        try:
            stc = _stats(c, fee)
        except Exception:
            continue
        rows_c = wide(stc[(stc.scope == scope) & (stc.construction == construction) & (stc.universe == universe)
                          & (stc.exit == exit_code)], ["scope"])
        if rows_c:
            r = rows_c[0]; r["cap"] = c; r["cap_label"] = f"{c}% per basket"
            r["util_avg_pct"], r["util_days_pct"] = _util_summary(scope, construction, universe, c)
            caps.append(r)

    # (e) the same scope x cell x exit x cap under every per-share fee that has a run
    fees_rows = []
    for f in FEES:
        try:
            stf = _stats(cap, f)
        except Exception:
            continue
        rows_f = wide(stf[(stf.scope == scope) & (stf.construction == construction) & (stf.universe == universe)
                          & (stf.exit == exit_code)], ["scope"])
        if rows_f:
            r = rows_f[0]; r["fee"] = f; r["fee_label"] = FEE_LABEL[f]
            r["util_avg_pct"], r["util_days_pct"] = _util_summary(scope, construction, universe, cap)
            fees_rows.append(r)

    # excursion statistics (MAE/MFE on the path to this exit) for the selected panel
    exc_groups = exc_touch = None
    eg, et = _excursion("groups", fee), _excursion("touch", fee)
    sel = lambda df: df[(df.exit == exit_code) & (df.scope == scope) & (df.construction == construction)
                        & (df.universe == universe)]
    if eg is not None:
        exc_groups = _rows(sel(eg), EXC_GROUP_COLS)
    if et is not None:
        exc_touch = _rows(sel(et), EXC_TOUCH_COLS)

    # stop-loss simulation: table rows per level + daily strategy-only series per level
    stop_rows, stop_series = None, []
    ss = _stop_stats(fee)
    if ss is not None:
        stop_rows = _rows(sel(ss).sort_values("stop_bps"), STOP_COLS)
        try:
            for lvl in STOP_LEVELS:
                r = _stop_panel(exit_code, lvl, scope, construction, universe, fee)
                if r is not None:
                    stop_series.append(_series(f"stop{lvl}", r))
        except Exception:
            stop_series = []

    # per-trade P&L distribution: the selected panel + all 25 panels of this scope x exit
    h = _hist(fee)
    hs = h[(h.exit == exit_code) & (h.scope == scope)]
    hist_grid = [_hist_row(r) for _, r in hs.iterrows()]
    hist = next((r for r in hist_grid if r["construction"] == construction and r["universe"] == universe), None)

    # fee drag = gross mean trade P&L minus net mean trade P&L for the same panel (bps per trade);
    # the fee is a constant per trade, so this is exactly the average round-trip commission.
    fee_drag_bps = None
    if fee != DEFAULT_FEE and hist is not None:
        try:
            g = _hist(DEFAULT_FEE)
            gr = g[(g.exit == exit_code) & (g.scope == scope) & (g.construction == construction)
                   & (g.universe == universe)]
            if len(gr):
                fee_drag_bps = float(gr["mean_bps"].iloc[0]) - float(hist["mean_bps"])
        except Exception:
            fee_drag_bps = None

    return {
        "scope": scope, "construction": construction, "universe": universe,
        "exit": exit_code, "exit_label": EXIT_LABEL[exit_code], "cap": cap,
        "fee": fee, "fee_label": FEE_LABEL[fee], "fee_drag_bps": fee_drag_bps,
        "granularity": granularity, "series": series, "stats": stats,
        "cells": cells, "scopes": scopes, "exits": exits, "caps": caps, "fees": fees_rows,
        "hist": hist, "hist_grid": hist_grid,
        "exc_groups": exc_groups, "exc_touch": exc_touch,
        "stop_rows": stop_rows, "stop_series": stop_series, "util": util,
        "window": "2016-01-05 -> 2026-08-06 (2,571 trade days)",
    }


def data_json(qs):
    scope = qs.get("scope", ["ALL"])[0]
    construction = qs.get("construction", ["ridge_levels_beta_neutral"])[0]
    universe = qs.get("universe", ["pure play"])[0]
    exit_code = qs.get("exit", [DEFAULT_EXIT])[0]
    cap = int(qs.get("cap", [DEFAULT_CAP])[0])
    fee = int(qs.get("fee", [DEFAULT_FEE])[0])
    return json.dumps(data(scope, construction, universe, exit_code, cap, fee), allow_nan=False).encode()


# --------------------------------------------------------------------- page
def render_page() -> bytes:
    scope_opts = []
    last_group = None
    for key, label, group in SCOPES:
        if group != last_group:
            if last_group is not None:
                scope_opts.append("</optgroup>")
            scope_opts.append(f"<optgroup label='{html.escape(group)}'>")
            last_group = group
        scope_opts.append(f"<option value='{html.escape(key)}'>{html.escape(label)}</option>")
    scope_opts.append("</optgroup>")
    con_opts = "".join(f"<option value='{c}'>{c}</option>" for c in CONSTRUCTIONS)
    uni_opts = "".join(f"<option value='{u}'>{u}</option>" for u in UNIVERSES)
    exit_opts = "".join(f"<option value='{c}'>{l}</option>" for c, l in EXITS)
    cap_opts = "".join(f"<option value='{c}'{' selected' if c == DEFAULT_CAP else ''}>{c}% of capital{' (study default)' if c == DEFAULT_CAP else ''}</option>" for c in CAPS)
    fee_opts = "".join(f"<option value='{f}'{' selected' if f == DEFAULT_FEE else ''}>{html.escape(FEE_LABEL[f])}</option>" for f in FEES)
    series_js = json.dumps([{"key": k, "label": l, "light": lc, "dark": dc} for k, l, lc, dc in SERIES])
    return (PAGE
            .replace("__SCOPES__", "".join(scope_opts))
            .replace("__CONS__", con_opts)
            .replace("__UNIS__", uni_opts)
            .replace("__EXITS__", exit_opts)
            .replace("__CAPS__", cap_opts)
            .replace("__FEES__", fee_opts)
            .replace("__SERIES__", series_js)
            .replace("__BASE__", html.escape(BASE))
            ).encode()


PAGE = r"""<!doctype html><html><head><meta charset="utf-8">
<title>10-year basket study — with vs. without S&amp;P</title>
<script src="https://cdn.jsdelivr.net/npm/plotly.js-dist-min@2.35.2/plotly.min.js"></script>
<style>
:root{color-scheme:light;
  --surface:#fcfcfb;--plane:#f9f9f7;--ink:#0b0b0b;--ink2:#52514e;--muted:#898781;
  --grid:#e1e0d9;--axis:#c3c2b7;--border:rgba(11,11,11,.10);--link:#0b5394}
@media (prefers-color-scheme:dark){:root:not([data-theme=light]){color-scheme:dark;
  --surface:#1a1a19;--plane:#0d0d0d;--ink:#fff;--ink2:#c3c2b7;--muted:#898781;
  --grid:#2c2c2a;--axis:#383835;--border:rgba(255,255,255,.10);--link:#7fb2ee}}
:root[data-theme=dark]{color-scheme:dark;
  --surface:#1a1a19;--plane:#0d0d0d;--ink:#fff;--ink2:#c3c2b7;--muted:#898781;
  --grid:#2c2c2a;--axis:#383835;--border:rgba(255,255,255,.10);--link:#7fb2ee}
body{font-family:system-ui,-apple-system,"Segoe UI",sans-serif;margin:0;padding:1.2rem 1.6rem;
  background:var(--plane);color:var(--ink);font-size:.9rem}
a{color:var(--link);text-decoration:none} a:hover{text-decoration:underline}
h1{font-size:1.25rem;margin:0 0 .2rem} .sub{color:var(--ink2);margin:0 0 1rem}
.filters{display:flex;flex-wrap:wrap;gap:1rem 1.4rem;align-items:flex-end;margin-bottom:1rem}
.filters label{display:flex;flex-direction:column;gap:.25rem;color:var(--ink2);font-size:.8rem}
select,button{font:inherit;padding:.3rem .5rem;border:1px solid var(--axis);border-radius:6px;
  background:var(--surface);color:var(--ink)}
.toggles{display:flex;gap:.9rem;flex-wrap:wrap}
.toggles label{flex-direction:row;align-items:center;gap:.35rem;color:var(--ink);font-size:.85rem}
.swatch{display:inline-block;width:14px;height:3px;border-radius:2px;vertical-align:middle}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:.8rem;margin-bottom:1rem}
.tile{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:.7rem .9rem}
.tile .name{color:var(--ink2);font-size:.78rem;display:flex;align-items:center;gap:.4rem}
.tile .big{font-size:1.6rem;font-weight:600;margin:.15rem 0 .3rem}
.tile .row{display:flex;justify-content:space-between;color:var(--ink2);font-size:.78rem;gap:.5rem}
.tile .row b{color:var(--ink);font-weight:500;font-variant-numeric:tabular-nums}
.tile.na .big{color:var(--muted);font-size:1rem;font-weight:400}
.card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:.6rem .8rem;margin-bottom:1rem}
.card h2{font-size:.95rem;margin:.2rem 0 .4rem;font-weight:600}
.card h2 span{color:var(--ink2);font-weight:400;font-size:.8rem;margin-left:.6rem}
#eq{height:420px} #dd{height:230px} #hist{height:300px}
.hstats{display:flex;flex-wrap:wrap;gap:.4rem 1.4rem;color:var(--ink2);font-size:.8rem;margin:0 0 .3rem}
.hstats b{color:var(--ink);font-weight:500;font-variant-numeric:tabular-nums;margin-left:.3rem}
.hgrid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:.5rem;margin-top:.6rem}
@media(max-width:1300px){.hgrid{grid-template-columns:repeat(3,minmax(0,1fr))}}
.hcell{border:1px solid var(--border);border-radius:8px;padding:.3rem .4rem .1rem;cursor:pointer;min-width:0}
.hcell.sel{border-color:#2a78d6;box-shadow:0 0 0 1px #2a78d6 inset}
.hcell .cap{font-size:.72rem;color:var(--ink2);display:flex;justify-content:space-between;gap:.4rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.hcell .cap span{overflow:hidden;text-overflow:ellipsis}
.hcell .cap b{color:var(--ink);font-weight:500}
.hcell .plot{height:120px}
h3{font-size:.85rem;margin:.4rem 0 .3rem;color:var(--ink2);font-weight:500}
table.static tbody tr{cursor:default} table.static tbody tr:hover td{background:none}
tr.sum td{font-weight:600;border-top:2px solid var(--axis)}
th.sub{font-weight:400;font-size:.72rem}
.fade{opacity:.45;transition:opacity .15s}
table{border-collapse:collapse;font-size:.8rem;width:100%}
th,td{padding:.28rem .5rem;text-align:right;border-bottom:1px solid var(--grid);font-variant-numeric:tabular-nums;white-space:nowrap}
th{color:var(--ink2);font-weight:500;position:sticky;top:0;background:var(--surface)}
td:first-child,th:first-child,td.l,th.l{text-align:left}
tr.sel td{background:rgba(42,120,214,.10)} tbody tr{cursor:pointer} tbody tr:hover td{background:rgba(42,120,214,.05)}
.grp{border-left:2px solid var(--grid)} .pos{color:var(--ink)} .neg{color:#b93535}
:root[data-theme=dark] .neg,@media (prefers-color-scheme:dark){:root:not([data-theme=light]) .neg{color:#e66767}}
.two{display:grid;grid-template-columns:1fr 1fr;gap:1rem} @media(max-width:1700px){.two{grid-template-columns:1fr}}
.scroll{max-height:520px;overflow:auto}
.note{color:var(--ink2);font-size:.78rem;margin:.4rem 0 0}
details summary{cursor:pointer;color:var(--link)}
</style></head><body>
<h1>10-year basket study — sentiment/fade strategy with vs. without an S&amp;P 500 leg</h1>
<p class="sub">Window 2016-01-05 → 2026-08-06, 2,571 trade days, 167,644 earnings events (fade-the-gap, 9:30 open → RTH close).
Portfolio = w × SPY buy&amp;hold + L × strategy, rebalanced daily; exit = when the baskets are unwound on the trade day. Source: <a href="/browse?prefix=__BASE__">s3 …/__BASE__</a>
· <a href="/">results browser</a></p>

<div class="filters">
  <label>Scope (universe of reporters)<select id="scope">__SCOPES__</select></label>
  <label>Basket construction<select id="construction">__CONS__</select></label>
  <label>Peer universe<select id="universe">__UNIS__</select></label>
  <label>Exit (unwind time)<select id="exit">__EXITS__</select></label>
  <label>Per-basket limit<select id="cap">__CAPS__</select></label>
  <label>Commission<select id="fee">__FEES__</select></label>
  <label>Y axis<select id="yscale"><option value="linear">linear</option><option value="log">log (growth of $1)</option></select></label>
  <label>Theme<select id="theme"><option value="auto">auto</option><option value="light">light</option><option value="dark">dark</option></select></label>
  <div class="toggles" id="toggles"></div>
</div>

<div class="tiles" id="tiles"></div>

<div class="card"><h2>Cumulative return<span id="eqnote"></span></h2><div id="eq"></div></div>
<div class="card"><h2>Drawdown from peak</h2><div id="dd"></div></div>

<div class="card"><h2 id="utiltitle">Capital utilization — strategy only (no S&amp;P)<span id="utilnote"></span></h2>
  <p class="note" id="utildesc"></p>
  <div id="util" style="height:260px"></div>
</div>

<div class="card"><h2>Per-trade P&amp;L distribution<span id="histnote"></span></h2>
  <div class="filters" style="margin-bottom:.5rem">
    <label>Bin width<select id="binw"><option value="25">25 bps</option><option value="50" selected>50 bps</option><option value="100">100 bps</option><option value="200">200 bps</option></select></label>
    <label>Y axis<select id="histy"><option value="pct">% of trades</option><option value="count">trades</option><option value="log">trades (log)</option></select></label>
    <label>X range<select id="histx"><option value="auto">p1 – p99</option><option value="1000">±1,000 bps</option><option value="2000">±2,000 bps</option><option value="3000">±3,000 bps (all bins)</option></select></label>
  </div>
  <div class="hstats" id="hstats"></div>
  <div id="hist"></div>
  <p class="note">One trade = one event traded in this construction × peer-universe cell (POOLED = union of the member cells' trades), P&amp;L in bps of basket notional at the selected exit. Strategy trades only — the SPY leg does not enter here. Bars left of zero are losses.</p>
  <details id="hgdet"><summary>All constructions × peer universes for this scope and exit (small multiples, click one to select it)</summary>
    <div class="hgrid" id="histgrid"></div></details>
</div>

<div class="two">
  <div class="card"><h2>Constructions × peer universes<span id="cellnote"></span></h2>
    <div class="scroll" id="cells"></div>
    <p class="note">Click a row to plot it. Blocks left to right: strategy only (w=0), 1× strategy + 1× SPY, 2× strategy + 1× SPY. Stats are daily-based (portfolio_stats.parquet); "capital used" = average share of strategy capital deployed (selected per-basket limit, capped at 100%), "days with baskets" = share of trade days with at least one basket; "days &gt; SPY" = share of the 2,571 trade days on which the portfolio's daily return beat SPY's (for the SPY overlays this is the share of days the strategy leg was positive).</p></div>
  <div class="card"><h2>Scopes (universes)<span id="scopenote"></span></h2>
    <div class="scroll" id="scopes"></div>
    <p class="note">Click a row to plot it. Index/sector scopes filter by the <em>reporter's</em> membership on the trade day, not the peers.</p></div>
</div>

<div class="card"><h2>Exit times<span id="exitnote"></span></h2>
  <div class="scroll" id="exits"></div>
  <p class="note">Click a row to plot it. Same scope, construction and peer universe; every basket unwound at that mark (9:31–9:35 minute marks, half-hours 10:00–15:30, or the RTH close).</p></div>

<div class="card"><h2>Per-basket limit (capital cap)<span id="capnote"></span></h2>
  <div class="scroll" id="caps"></div>
  <p class="note">Click a row to switch the whole page to that cap. Same scope, construction, peer universe and exit; each basket takes min(1/N, cap) of the strategy capital, uncommitted capital earns 0, so a higher cap deploys more on thin days and hits 100% with fewer baskets (5% → 20 baskets, 25% → 4). Shards and per-trade P&amp;L are identical across caps; only the daily capital model changes.</p></div>

<div class="card"><h2>Commission (per-share fee)<span id="feenote"></span></h2>
  <div class="scroll" id="fees"></div>
  <p class="note">Click a row to switch the whole page to that fee. Same scope, construction, peer universe, exit and per-basket limit. The fee is IBKR Pro Fixed: $0.005 per share, charged on both sides (entry and exit) of every leg (reporter and every hedge peer), so per trade it equals 2 × $0.005 × shares traded per $1 of reporter notional. Not modelled: the $1 per-order minimum, the 1%-of-trade-value cap (binds only under $0.50/share), exchange and regulatory pass-throughs. Every metric and chart on the page is recomputed from the net P&amp;L; the gross ("none") figures are the original artifacts, unchanged.</p></div>

<div class="card"><details><summary>Table view — calendar-year returns of the plotted series</summary><div class="scroll" id="yearly"></div></details></div>

<div class="card"><h2>Excursion statistics — the path to the selected exit<span id="excnote"></span></h2>
  <p class="note">For each trade the path is its P&amp;L at the marks up to the exit: 9:31–9:35 by the minute, then every 30 min to 15:30, then close. MAE = worst mark on the path, MFE = best mark, final = P&amp;L at the exit; all in bps of basket notional (100 bps = 1%). Moves between marks are not observed. Left: trades grouped by their final P&amp;L. Right: how often a threshold was touched on the way, and how those trades ended.</p>
  <h3>By final P&amp;L bucket (bps)</h3><div class="scroll" id="excg"></div>
  <div class="two" id="exct"></div>
  <p class="note" id="exctnote"></p>
</div>

<div class="card"><h2>Stop-loss simulation — whole basket cut at the marks<span id="stopnote"></span></h2>
  <p class="note">At the first mark where the basket (reporter + hedge peers) is down at least the stop level, it is unwound at that mark's P&amp;L; otherwise the trade runs to the selected exit. Levels are bps of reporter notional (500 = 5%). Strategy only, no SPY leg. Moves between marks are not observed, so fills can be below the level.</p>
  <div class="scroll" id="stoptbl"></div>
  <div class="two" style="margin-top:.6rem"><div id="stopeq" style="height:320px"></div><div id="stopdd" style="height:320px"></div></div>
</div>


<script>
const SERIES = __SERIES__;
const $ = id => document.getElementById(id);
const state = {scope:"ALL", construction:"ridge_levels_beta_neutral", universe:"pure play", exit:"close", cap:"2", fee:"0",
               show:{strat:true, spy1_strat1:true, spy1_strat2:true, spy:true}};
let last = null;

function isDark(){ const t=document.documentElement.dataset.theme;
  if(t==="dark") return true; if(t==="light") return false;
  return matchMedia("(prefers-color-scheme: dark)").matches; }
function color(key){ const s=SERIES.find(x=>x.key===key); return isDark()? s.dark : s.light; }
function label(key){ return SERIES.find(x=>x.key===key).label; }
function css(v){ return getComputedStyle(document.documentElement).getPropertyValue(v).trim(); }
const fmt = (v,d=2,suf="") => (v==null||isNaN(v)) ? "—" : (v>0&&suf==="%"?"+":"") + v.toFixed(d) + suf;

function readURL(){ const p=new URLSearchParams(location.search);
  for(const k of ["scope","construction","universe","exit","cap","fee"]) if(p.get(k)) state[k]=p.get(k);
  if(p.get("grid")==="1") $("hgdet").open=true;
  $("scope").value=state.scope; $("construction").value=state.construction; $("universe").value=state.universe; $("exit").value=state.exit; $("cap").value=state.cap; $("fee").value=state.fee; }
function writeURL(){ const p=new URLSearchParams({scope:state.scope,construction:state.construction,universe:state.universe,exit:state.exit,cap:state.cap,fee:state.fee});
  history.replaceState(null,"","?"+p); }

function buildToggles(){
  $("toggles").innerHTML="";
  for(const s of SERIES){
    const l=document.createElement("label");
    const c=document.createElement("input"); c.type="checkbox"; c.checked=state.show[s.key];
    c.onchange=()=>{state.show[s.key]=c.checked; draw();};
    const sw=document.createElement("span"); sw.className="swatch"; sw.style.background=color(s.key);
    l.append(c, sw, document.createTextNode(" "+s.label)); $("toggles").append(l);
  }
}

async function load(){
  document.querySelectorAll(".card,.tiles").forEach(e=>e.classList.add("fade"));
  const p=new URLSearchParams({scope:state.scope,construction:state.construction,universe:state.universe,exit:state.exit,cap:state.cap,fee:state.fee});
  const r=await fetch("/study10y/data?"+p); if(!r.ok){alert(await r.text()); return;}
  last=await r.json();
  state.construction=last.construction; state.universe=last.universe;   // server coerces reporter_only <-> none
  $("construction").value=state.construction; $("universe").value=state.universe;
  writeURL(); draw();
  document.querySelectorAll(".card,.tiles").forEach(e=>e.classList.remove("fade"));
}

function layoutBase(){
  return {paper_bgcolor:css("--surface"), plot_bgcolor:css("--surface"),
    font:{family:"system-ui,-apple-system,Segoe UI,sans-serif", color:css("--ink2"), size:12},
    margin:{l:56,r:16,t:8,b:40}, hovermode:"x unified", showlegend:true,
    legend:{orientation:"h", y:1.08, x:0, font:{color:css("--ink")}},
    xaxis:{gridcolor:css("--grid"), linecolor:css("--axis"), zeroline:false, showspikes:true,
           spikemode:"across", spikethickness:1, spikecolor:css("--muted"), spikedash:"solid"},
    hoverlabel:{bgcolor:css("--surface"), bordercolor:css("--axis"), font:{color:css("--ink")}}};
}

function draw(){
  if(!last) return;
  const shown = last.series.filter(s=>state.show[s.key]);
  const log = $("yscale").value==="log";
  const eq = shown.map(s=>({type:"scatter", mode:"lines", name:label(s.key), x:s.dates,
      y: log ? s.cum.map(v=>1+v/100) : s.cum, line:{color:color(s.key), width:2},
      hovertemplate: log ? "%{y:.2f}x" : "%{y:+.1f}%"}));
  const L=layoutBase();
  L.yaxis = log ? {title:"growth of $1 (log)", type:"log", gridcolor:css("--grid"), zeroline:false}
                : {title:"cumulative return, %", ticksuffix:"%", gridcolor:css("--grid"), zeroline:false};
  Plotly.react("eq", eq, L, {displaylogo:false, responsive:true});
  const dd = shown.map(s=>({type:"scatter", mode:"lines", name:label(s.key), x:s.dates, y:s.dd,
      line:{color:color(s.key), width:1.5}, hovertemplate:"%{y:.1f}%"}));
  const L2=layoutBase(); L2.showlegend=false; L2.margin.t=4;
  L2.yaxis={title:"drawdown, %", ticksuffix:"%", gridcolor:css("--grid"), zeroline:false, rangemode:"tozero"};
  Plotly.react("dd", dd, L2, {displaylogo:false, responsive:true});
  $("eqnote").textContent = `${last.scope} · ${last.construction} · ${last.universe} · exit ${last.exit_label} · cap ${last.cap}%/basket · commission ${last.fee_label} · ${last.granularity} series, 2,571 trade days`;
  tiles(); utilChart(); cellsTable(); scopesTable(); exitsTable(); capsTable(); feesTable(); yearly(shown); drawHist(); drawHistGrid(); excGroups(); excTouch(); stopTable(); stopCharts();
}

function utilChart(){
  const u=last.util; const box=$("util");
  if(!u){ box.innerHTML="<p class='note'>utilization file not available</p>"; $("utilnote").textContent=""; return; }
  const cap=last.cap; $("utiltitle").childNodes[0].textContent=`Capital utilization — strategy only (no S&P) · per-basket limit ${cap}% of capital`;
  $("utildesc").innerHTML=`Each traded basket uses <b>${cap}% of the capital</b> (the per-basket limit selected above; the study default is 2%), so a day's utilization = ${cap}% × baskets traded that day, capped at 100%; ${Math.ceil(100/cap)} or more baskets = fully deployed. Idle capital earns 0. POOLED = mean of the member cells. Independent of the exit and of the SPY leg.`;
  $("utilnote").textContent=`${last.scope} · ${last.construction} · ${last.universe} · average ${u.avg_pct.toFixed(1)}% of capital deployed · baskets on ${u.days_traded_pct.toFixed(1)}% of days`;
  const L=layoutBase(); L.margin.t=8; L.hovermode="x unified";
  L.yaxis={title:"% of capital deployed", ticksuffix:"%", range:[0,100], gridcolor:css("--grid"), zeroline:false};
  const c=color("strat");
  Plotly.react("util", [
    {type:"scatter", mode:"lines", name:"daily", x:u.dates, y:u.pct, line:{color:c, width:1}, opacity:.35, fill:"tozeroy", fillcolor:c+"22", hovertemplate:"%{y:.1f}%"},
    {type:"scatter", mode:"lines", name:"63-day average", x:u.dates, y:u.roll63, line:{color:c, width:2.2}, connectgaps:false, hovertemplate:"%{y:.1f}%"}
  ], L, {displaylogo:false, responsive:true});
}

function tiles(){
  const t=$("tiles"); t.innerHTML="";
  for(const s of SERIES){
    const st=last.stats[s.key]; const d=document.createElement("div"); d.className="tile"+(st?"":" na");
    const name=document.createElement("div"); name.className="name";
    const sw=document.createElement("span"); sw.className="swatch"; sw.style.background=color(s.key);
    name.append(sw, document.createTextNode(s.label)); d.append(name);
    const big=document.createElement("div"); big.className="big";
    if(!st){ big.textContent="not in portfolio_stats for this scope"; d.append(big); t.append(d); continue; }
    big.textContent=st.wiped_out ? "WIPED OUT" : "Sharpe "+fmt(st.sharpe,2); if(st.wiped_out){ big.classList.add("neg"); big.title="equity fell to zero or below on some day (Sharpe "+fmt(st.sharpe,2)+" on the raw daily returns)"; } d.append(big);
    for(const [k,v] of [["annualised return",fmt(st.ann_ret_pct,1,"%")],["total return",fmt(st.total_pct,0,"%")],
        ["max drawdown",fmt(st.max_dd_pct,1,"%")],
        ...(st.beat_spy_pct==null ? [] : [["days beating SPY", pct(st.beat_spy_pct)]]),
        ...(s.key==="strat" && last.util ? [["capital used (avg)", pct(last.util.avg_pct)]] : []),
        ...(s.key==="strat" && last.fee_drag_bps!=null ? [["commission (avg/trade)", last.fee_drag_bps.toFixed(1)+" bps"]] : []),
        ["events", st.n_events==null?"—":Math.round(st.n_events).toLocaleString()]]){
      const r=document.createElement("div"); r.className="row"; const a=document.createElement("span"); a.textContent=k;
      const b=document.createElement("b"); b.textContent=v; r.append(a,b); d.append(r); }
    t.append(d);
  }
}

const M=[["sharpe","Sharpe",2,""],["ann_ret_pct","ann %",1,"%"],["total_pct","total %",0,"%"],["max_dd_pct","max DD",1,"%"],["beat_spy_pct","days > SPY",1,"%"]];
const pct = v => (v==null||isNaN(v)) ? "—" : v.toFixed(1)+"%";
function statTable(rows, keyCols, onPick, selFn){
  const tbl=document.createElement("table"); const th=document.createElement("thead"); const tr=document.createElement("tr");
  for(const k of keyCols){ const h=document.createElement("th"); h.className="l"; h.textContent=(k==="exit_label"?"exit":k==="cap_label"?"per-basket limit":k==="fee_label"?"commission":k); tr.append(h); }
  const h0=document.createElement("th"); h0.textContent="events"; tr.append(h0);
  for(const t of [`capital used (${last.cap}%/basket)`,"days with baskets"]){ const h=document.createElement("th"); h.textContent=t; h.title="strategy capital utilization: avg % of capital deployed / % of trade days with at least one basket"; tr.append(h); }
  for(const [pk,pl] of [["strat","strategy only"],["spy1_strat1","1× strat + 1× SPY"],["spy1_strat2","2× strat + 1× SPY"]]){
    M.forEach(([m,ml],i)=>{ const h=document.createElement("th"); if(i===0) h.className="grp";
      h.textContent=(i===0?pl+" · ":"")+ml; h.style.color=color(pk); tr.append(h); }); }
  th.append(tr); tbl.append(th);
  const tb=document.createElement("tbody");
  for(const r of rows){
    const row=document.createElement("tr"); if(selFn(r)) row.className="sel";
    for(const k of keyCols){ const c=document.createElement("td"); c.className="l"; c.textContent=r[k]; row.append(c); }
    const ne=document.createElement("td"); ne.textContent=r["strat.n_events"]==null?"—":Math.round(r["strat.n_events"]).toLocaleString(); row.append(ne);
    row.append(cellTd(pct(r.util_avg_pct)), cellTd(pct(r.util_days_pct)));
    for(const pk of ["strat","spy1_strat1","spy1_strat2"]) M.forEach(([m,,d,suf],i)=>{ const c=document.createElement("td");
      const v=r[pk+"."+m]; if(i===0) c.className="grp";
      const wiped=r[pk+".wiped_out"]===true;
      if(m==="beat_spy_pct"){ c.textContent=pct(v); if(v!=null&&v<50) c.classList.add("neg"); }
      else if(wiped && (m==="total_pct"||m==="ann_ret_pct")){ c.textContent="WIPED OUT"; c.classList.add("neg"); c.title="equity fell to zero or below on some day; floored at 0 from then on"; }
      else { c.textContent=fmt(v,d,suf); if(v!=null&&v<0) c.classList.add("neg"); } row.append(c); });
    row.onclick=()=>onPick(r); tb.append(row);
  }
  tbl.append(tb); return tbl;
}
function cellsTable(){
  $("cells").innerHTML=""; $("cellnote").textContent="scope "+last.scope+" · exit "+last.exit_label;
  $("cells").append(statTable(last.cells, ["construction","universe"],
    r=>{state.construction=r.construction; state.universe=r.universe; $("construction").value=r.construction; $("universe").value=r.universe; load();},
    r=>r.construction===last.construction && r.universe===last.universe));
}
function scopesTable(){
  $("scopes").innerHTML=""; $("scopenote").textContent=last.construction+" · "+last.universe+" · exit "+last.exit_label;
  $("scopes").append(statTable(last.scopes, ["scope"],
    r=>{state.scope=r.scope; $("scope").value=r.scope; load();}, r=>r.scope===last.scope));
}
// ---- per-trade P&L histograms -------------------------------------------
const NEG = () => isDark() ? "#e66767" : "#b93535";
function rebin(h, w){            // sum adjacent 25-bps bins into w-bps bins -> {lo[], hi[], n[]}
  const f=Math.max(1, Math.round(w/h.bin_w_bps)); const lo=[],hi=[],n=[];
  for(let i=0;i<h.counts.length;i+=f){ let c=0; for(let j=i;j<Math.min(i+f,h.counts.length);j++) c+=h.counts[j];
    lo.push(h.bin_lo_bps+i*h.bin_w_bps); hi.push(h.bin_lo_bps+Math.min(i+f,h.counts.length)*h.bin_w_bps); n.push(c); }
  return {lo,hi,n};
}
function histRange(h, w){
  const x=$("histx").value;
  if(x!=="auto") return [-(+x), +x];
  const a=Math.floor(h.p01_bps/w)*w, b=Math.ceil(h.p99_bps/w)*w; const pad=Math.max(w, (b-a)*0.05);
  return [Math.max(h.bin_lo_bps, a-pad), Math.min(h.bin_lo_bps+h.counts.length*h.bin_w_bps, b+pad)];
}
function histTrace(h, w, ymode, hover){
  const b=rebin(h,w); const tot=h.n_trades||1;
  const y = ymode==="pct" ? b.n.map(v=>v/tot*100) : b.n;
  return {type:"bar", x:b.lo.map((l,i)=>(l+b.hi[i])/2), y, width:b.hi.map((v,i)=>v-b.lo[i]),
    marker:{color:b.lo.map(l=>l<0? NEG() : color("strat")), line:{width:0}},
    customdata:b.lo.map((l,i)=>[l,b.hi[i],b.n[i],b.n[i]/tot*100]), hoverinfo: hover?"all":"skip",
    hovertemplate:"[%{customdata[0]:,}, %{customdata[1]:,}) bps<br>%{customdata[2]:,} trades · %{customdata[3]:.2f}%<extra></extra>"};
}
function drawHist(){
  const h=last.hist; const box=$("hist"); $("hstats").innerHTML="";
  if(!h){ box.innerHTML="<p class='note'>no trades for this selection</p>"; $("histnote").textContent=""; return; }
  $("histnote").textContent=`${last.scope} · ${last.construction} · ${last.universe} · exit ${last.exit_label}`;
  const outside=(h.n_under||0)+(h.n_over||0);
  for(const [k,v] of [["trades",h.n_trades.toLocaleString()],["mean",fmt(h.mean_bps,1," bps")],["median",fmt(h.median_bps,1," bps")],
      ["std",fmt(h.std_bps,0," bps")],["hit rate",fmt(h.hit_rate*100,1,"%").replace("+","")],["skew",fmt(h.skew,2)],
      ["p5 / p95",`${fmt(h.p05_bps,0)} / ${fmt(h.p95_bps,0)} bps`],["p1 / p99",`${fmt(h.p01_bps,0)} / ${fmt(h.p99_bps,0)} bps`],
      ["beyond ±3,000 bps", outside.toLocaleString()]]){
    const sp=document.createElement("span"); sp.textContent=k; const b=document.createElement("b"); b.textContent=v; sp.append(b); $("hstats").append(sp); }
  const w=+$("binw").value, ymode=$("histy").value;
  const L=layoutBase(); L.showlegend=false; L.hovermode="closest"; L.bargap=0.06; L.margin.t=22;
  L.xaxis={...L.xaxis, title:"per-trade P&L, bps", range:histRange(h,w), showspikes:false};
  L.yaxis={title: ymode==="pct"?"% of trades":"trades", gridcolor:css("--grid"), zeroline:false, type: ymode==="log"?"log":"linear", ticksuffix: ymode==="pct"?"%":""};
  L.shapes=[{type:"line",x0:0,x1:0,y0:0,y1:1,yref:"paper",line:{color:css("--ink2"),width:1}},
            {type:"line",x0:h.mean_bps,x1:h.mean_bps,y0:0,y1:1,yref:"paper",line:{color:css("--ink"),width:1.5,dash:"dot"}},
            {type:"line",x0:h.median_bps,x1:h.median_bps,y0:0,y1:1,yref:"paper",line:{color:css("--ink"),width:1.5,dash:"dash"}}];
  L.annotations=[{x:h.mean_bps,y:1,yref:"paper",yanchor:"bottom",text:"mean",showarrow:false,font:{size:10,color:css("--ink2")},xanchor:h.mean_bps>=h.median_bps?"left":"right"},
                 {x:h.median_bps,y:1,yref:"paper",yanchor:"bottom",text:"median",showarrow:false,font:{size:10,color:css("--ink2")},xanchor:h.mean_bps>=h.median_bps?"right":"left"}];
  Plotly.react("hist", [histTrace(h,w,ymode,true)], L, {displaylogo:false, responsive:true});
}
function drawHistGrid(){
  const g=$("histgrid"); g.innerHTML=""; if(!$("hgdet").open || !last.hist_grid) return;
  const w=Math.max(+$("binw").value, 50), ymode=$("histy").value;
  const rows=["POOLED","equal_weight_dollar_neutral","ridge_returns_beta_neutral","ridge_returns_dollar_neutral","ridge_levels_beta_neutral","reporter_only","spy_beta_levels_neutral","spy_beta_returns_neutral","spy_dollar_neutral"];
  const hedgedCols=["POOLED","all","correlated","functional","pure play"];
  const SINGLE={reporter_only:"none",spy_beta_levels_neutral:"SPY",spy_beta_returns_neutral:"SPY",spy_dollar_neutral:"SPY"};
  const colsFor=c=>SINGLE[c]?[SINGLE[c]]:hedgedCols;   // single-cell constructions: one pseudo-universe
  const byKey={}; for(const h of last.hist_grid) byKey[h.construction+"|"+h.universe]=h;
  const xr = $("histx").value==="auto" ? null : [-(+$("histx").value), +$("histx").value];
  let i=0;
  for(const c of rows) for(const u of colsFor(c)){
    const h=byKey[c+"|"+u]; const d=document.createElement("div"); d.className="hcell"+(c===last.construction&&u===last.universe?" sel":"");
    const cap=document.createElement("div"); cap.className="cap";
    const SHORT={POOLED:"POOLED",equal_weight_dollar_neutral:"EW $-neutral",ridge_returns_beta_neutral:"ridge ret β-neutral",ridge_returns_dollar_neutral:"ridge ret $-neutral",ridge_levels_beta_neutral:"ridge lvl β-neutral",reporter_only:"reporter only (no hedge)",spy_beta_levels_neutral:"SPY β-neutral (levels)",spy_beta_returns_neutral:"SPY β-neutral (returns)",spy_dollar_neutral:"SPY $-neutral"};
    cap.innerHTML=`<span title="${c} · ${u}">${SHORT[c]||c} · ${u}</span>`+(h?`<span>n <b>${h.n_trades.toLocaleString()}</b> · μ <b>${h.mean_bps.toFixed(0)}</b> · hit <b>${(h.hit_rate*100).toFixed(0)}%</b></span>`:"");
    const pl=document.createElement("div"); pl.className="plot"; pl.id="hg"+(i++); d.append(cap,pl); g.append(d);
    d.onclick=()=>{state.construction=c; state.universe=u; $("construction").value=c; $("universe").value=u; load();};
    if(!h) continue;
    const L=layoutBase(); L.showlegend=false; L.hovermode="closest"; L.bargap=0.05; L.margin={l:28,r:4,t:2,b:18};
    L.xaxis={...L.xaxis, range: xr||histRange(h,w), showspikes:false, tickfont:{size:9}};
    L.yaxis={gridcolor:css("--grid"), zeroline:false, type: ymode==="log"?"log":"linear", tickfont:{size:9}, ticksuffix: ymode==="pct"?"%":""};
    L.shapes=[{type:"line",x0:0,x1:0,y0:0,y1:1,yref:"paper",line:{color:css("--ink2"),width:1}}];
    Plotly.react(pl.id, [histTrace(h,w,ymode,true)], L, {displaylogo:false, responsive:true, displayModeBar:false});
  }
}
// ---- excursion statistics -------------------------------------------------
const bps = v => (v==null||isNaN(v)) ? "—" : Math.round(v).toLocaleString();
function cellTd(txt, cls){ const c=document.createElement("td"); c.textContent=txt; if(cls) c.className=cls; return c; }
function excGroups(){
  const box=$("excg"); box.innerHTML=""; $("excnote").textContent=`${last.scope} · ${last.construction} · ${last.universe} · exit ${last.exit_label}`;
  const rows=last.exc_groups; if(!rows||!rows.length){ box.innerHTML="<p class='note'>excursion statistics not produced yet for this selection</p>"; return; }
  const H=[["final P&L","l"],["trades",""],["share",""],["mean final",""],["MAE p50","grp"],["MAE p10",""],["MAE mean",""],["MFE p50","grp"],["MFE p90",""],["MFE mean",""],
           ["≤ −500","grp"],["≤ −1000",""],["≤ −2000",""],["≥ +500","grp"],["≥ +1000",""],["≥ +2000",""],["worst ≤ 9:35","grp"],["worst at",""],["best at",""]];
  const tbl=document.createElement("table"); tbl.className="static"; const th=document.createElement("thead");
  const g=document.createElement("tr"); for(const [t,span,cls] of [["",4],["worst mark (MAE), bps",3,"grp"],["best mark (MFE), bps",3,"grp"],["touched (% of trades)",3,"grp"],["reached (% of trades)",3,"grp"],["timing",3,"grp"]]){ const h=document.createElement("th"); h.colSpan=span; h.textContent=t; h.className=(cls||"")+" sub"; g.append(h);} th.append(g);
  const tr=document.createElement("tr"); for(const [t,cls] of H){ const h=document.createElement("th"); h.textContent=t; h.className=cls; tr.append(h);} th.append(tr); tbl.append(th);
  const tb=document.createElement("tbody");
  for(const r of rows){
    const row=document.createElement("tr"); if(["LOSERS","WINNERS","ALL"].includes(r.group)) row.className="sum";
    row.append(cellTd(r.group,"l"), cellTd(r.n==null?"—":r.n.toLocaleString()), cellTd(pct(r.share_pct)), cellTd(bps(r.mean_final_bps), r.mean_final_bps<0?"neg":""),
      cellTd(bps(r.mae_p50),"grp neg"), cellTd(bps(r.mae_p10),"neg"), cellTd(bps(r.mae_mean),"neg"),
      cellTd(bps(r.mfe_p50),"grp"), cellTd(bps(r.mfe_p90)), cellTd(bps(r.mfe_mean)),
      cellTd(pct(r.touched_m500_pct),"grp"), cellTd(pct(r.touched_m1000_pct)), cellTd(pct(r.touched_m2000_pct)),
      cellTd(pct(r.reached_p500_pct),"grp"), cellTd(pct(r.reached_p1000_pct)), cellTd(pct(r.reached_p2000_pct)),
      cellTd(pct(r.worst_in_open5_pct),"grp"), cellTd(r.mae_time_median||"—"), cellTd(r.mfe_time_median||"—"));
    tb.append(row);
  }
  tbl.append(tb); box.append(tbl);
}
function excTouch(){
  const box=$("exct"); box.innerHTML=""; const rows=last.exc_touch; if(!rows||!rows.length) return;
  for(const [kind,title,pops,endLabel,endKey] of [["MAE","Touched a loss of at least … on the way (MAE ≤ threshold)",["ALL","WINNERS","BIG_WINNERS"],"ended ≥ 0","ended_positive_pct"],
                                                  ["MFE","Reached a gain of at least … on the way (MFE ≥ threshold)",["ALL","LOSERS","BIG_LOSERS"],"ended < 0","ended_negative_pct"]]){
    const wrap=document.createElement("div"); box.append(wrap);
    const h3=document.createElement("h3"); h3.textContent=title; wrap.append(h3);
    const tbl=document.createElement("table"); tbl.className="static"; const th=document.createElement("thead");
    const g=document.createElement("tr"); g.append(Object.assign(document.createElement("th"),{textContent:"",colSpan:1}));
    for(const p of pops){ const sub=rows.find(r=>r.kind===kind&&r.population===p); const h=document.createElement("th"); h.colSpan=3; h.className="grp sub"; h.textContent=p.replace("_"," ").toLowerCase()+(sub?` (n ${sub.n_pop.toLocaleString()})`:""); g.append(h);} th.append(g);
    const tr=document.createElement("tr"); tr.append(Object.assign(document.createElement("th"),{textContent:"threshold, bps",className:"l"}));
    for(const p of pops) for(const [t,i] of [["hit %",0],["mean final",1],[endLabel,2]]){ const h=document.createElement("th"); h.textContent=t; if(i===0) h.className="grp"; tr.append(h);} th.append(tr); tbl.append(th);
    const tb=document.createElement("tbody"); const ths=[...new Set(rows.filter(r=>r.kind===kind).map(r=>r.threshold_bps))].sort((a,b)=>kind==="MAE"?b-a:a-b);
    for(const t of ths){ const row=document.createElement("tr"); row.append(cellTd((t>0?"+":"")+t.toLocaleString(),"l"));
      for(const p of pops){ const r=rows.find(x=>x.kind===kind&&x.threshold_bps===t&&x.population===p);
        row.append(cellTd(r?pct(r.hit_pct):"—","grp"), cellTd(r?bps(r.mean_final_hit_bps):"—", r&&r.mean_final_hit_bps<0?"neg":""), cellTd(r?pct(r[endKey]):"—")); }
      tb.append(row); }
    tbl.append(tb); wrap.append(tbl);
  }
  const n=$("exctnote"); n.textContent="BIG WINNERS = final ≥ +500 bps, BIG LOSERS = final ≤ −500 bps. \"hit %\" = share of the population whose path crossed the threshold; \"mean final\" = their average P&L at the exit.";
}
// ---- stop-loss simulation ----------------------------------------------------
// five distinct hues (levels are labelled in the legend and table); validated light+dark with the dataviz validator
const STOP_C = {light:{0:"#2a78d6",500:"#eb6834",1000:"#1baf7a",1500:"#9b2fae",2000:"#b08a00"},
                dark:{0:"#3987e5",500:"#d95926",1000:"#199e70",1500:"#b565c9",2000:"#ad860d"}};
const stopColor = l => STOP_C[isDark()?"dark":"light"][l];
const stopLabel = l => l===0 ? "no stop" : `stop −${l/100}% (${l} bps)`;
function stopTable(){
  const box=$("stoptbl"); box.innerHTML=""; $("stopnote").textContent=`${last.scope} · ${last.construction} · ${last.universe} · exit ${last.exit_label}`+(last.cap!==2?" · portfolio columns and charts are computed at the 2% cap":"");
  const rows=last.stop_rows; if(!rows||!rows.length){ box.innerHTML="<p class='note'>stop-loss simulation not produced yet for this selection</p>"; return; }
  const H=[["stop","l"],["trades",""],["stopped","grp"],["…of which would have ended ≥ 0",""],["…their unstopped mean",""],["…their fill mean",""],
           ["mean / trade","grp"],["Δ mean",""],["median",""],["std",""],["hit rate",""],["p5",""],["p95",""],
           ["Sharpe","grp"],["ann %",""],["total %",""],["max DD",""],["longest recovery, days",""],["days > SPY",""]];
  const tbl=document.createElement("table"); tbl.className="static"; const th=document.createElement("thead");
  const g=document.createElement("tr"); for(const [t,span,cls] of [["",2],["stopped trades",4,"grp"],["per-trade P&L, bps",7,"grp"],["portfolio (strategy only, daily)",6,"grp"]]){ const h=document.createElement("th"); h.colSpan=span; h.textContent=t; h.className=(cls||"")+" sub"; g.append(h);} th.append(g);
  const tr=document.createElement("tr"); for(const [t,cls] of H){ const h=document.createElement("th"); h.textContent=t; h.className=cls; tr.append(h);} th.append(tr); tbl.append(th);
  const tb=document.createElement("tbody"); const base=rows.find(r=>r.stop_bps===0);
  for(const r of rows){
    const row=document.createElement("tr"); const sw=document.createElement("span"); sw.className="swatch"; sw.style.background=stopColor(r.stop_bps);
    const c0=document.createElement("td"); c0.className="l"; c0.append(sw, document.createTextNode(" "+stopLabel(r.stop_bps))); row.append(c0);
    row.append(cellTd(r.n_trades.toLocaleString()));
    row.append(cellTd(r.stop_bps?pct(r.stopped_pct):"—","grp"), cellTd(r.stop_bps?pct(r.stopped_ended_pos_pct):"—"), cellTd(r.stop_bps?bps(r.stopped_unstopped_mean_bps):"—", r.stopped_unstopped_mean_bps<0?"neg":""), cellTd(r.stop_bps?bps(r.stopped_fill_mean_bps):"—","neg"));
    row.append(cellTd(bps(r.mean_bps),"grp"+(r.mean_bps<0?" neg":"")), cellTd(r.stop_bps?(r.delta_mean_bps>=0?"+":"")+bps(r.delta_mean_bps):"—", r.delta_mean_bps<0?"neg":""), cellTd(bps(r.median_bps), r.median_bps<0?"neg":""), cellTd(bps(r.std_bps)), cellTd(pct(r.hit_rate)), cellTd(bps(r.p05_bps),"neg"), cellTd(bps(r.p95_bps)));
    row.append(cellTd(fmt(r.sharpe,2),"grp"+(r.sharpe<0?" neg":"")), cellTd(fmt(r.ann_ret_pct,1,"%"), r.ann_ret_pct<0?"neg":""), cellTd(fmt(r.total_pct,0,"%"), r.total_pct<0?"neg":""), cellTd(fmt(r.max_dd_pct,1,"%"),"neg"), cellTd(r.p2r_days==null?"—":Math.round(r.p2r_days).toLocaleString()), cellTd(pct(r.beat_spy_pct), r.beat_spy_pct<50?"neg":""));
    if(base && r.stop_bps && r.sharpe>base.sharpe) row.cells[13].style.fontWeight="600";
    tb.append(row);
  }
  tbl.append(tb); box.append(tbl);
}
function stopCharts(){
  const baseS=last.series.find(s=>s.key==="strat"); const ser=[];
  if(baseS) ser.push({lvl:0, s:baseS});
  for(const s of (last.stop_series||[])) ser.push({lvl:+s.key.replace("stop",""), s});
  if(ser.length<2){ Plotly.purge("stopeq"); Plotly.purge("stopdd"); return; }
  const log=$("yscale").value==="log";
  const L=layoutBase(); L.legend={orientation:"h",y:1.12,x:0,font:{color:css("--ink")}}; L.margin.t=30;
  L.yaxis= log ? {title:"growth of $1 (log)",type:"log",gridcolor:css("--grid"),zeroline:false} : {title:"cumulative return, % (strategy only)",ticksuffix:"%",gridcolor:css("--grid"),zeroline:false};
  Plotly.react("stopeq", ser.map(({lvl,s})=>({type:"scatter",mode:"lines",name:stopLabel(lvl),x:s.dates,y:log?s.cum.map(v=>1+v/100):s.cum,line:{color:stopColor(lvl),width:lvl===0?2.2:1.6},hovertemplate:log?"%{y:.2f}x":"%{y:+.1f}%"})), L, {displaylogo:false,responsive:true});
  const L2=layoutBase(); L2.showlegend=false; L2.margin.t=30; L2.yaxis={title:"drawdown, %",ticksuffix:"%",gridcolor:css("--grid"),zeroline:false,rangemode:"tozero"};
  L2.annotations=[{x:0,y:1.06,xref:"paper",yref:"paper",xanchor:"left",showarrow:false,text:"Drawdown from peak, same colours",font:{size:12,color:css("--ink2")}}];
  Plotly.react("stopdd", ser.map(({lvl,s})=>({type:"scatter",mode:"lines",name:stopLabel(lvl),x:s.dates,y:s.dd,line:{color:stopColor(lvl),width:lvl===0?2:1.4},hovertemplate:"%{y:.1f}%"})), L2, {displaylogo:false,responsive:true});
}
function capsTable(){
  $("caps").innerHTML=""; $("capnote").textContent=`${last.scope} · ${last.construction} · ${last.universe} · exit ${last.exit_label}`;
  if(!last.caps||!last.caps.length){ $("caps").innerHTML="<p class='note'>no cap runs found</p>"; return; }
  $("caps").append(statTable(last.caps, ["cap_label"],
    r=>{state.cap=String(r.cap); $("cap").value=state.cap; load();}, r=>r.cap===last.cap));
}
function feesTable(){
  $("fees").innerHTML=""; $("feenote").textContent=`${last.scope} · ${last.construction} · ${last.universe} · exit ${last.exit_label} · cap ${last.cap}%/basket`;
  if(!last.fees||!last.fees.length){ $("fees").innerHTML="<p class='note'>no fee runs found</p>"; return; }
  $("fees").append(statTable(last.fees, ["fee_label"],
    r=>{state.fee=String(r.fee); $("fee").value=state.fee; load();}, r=>r.fee===last.fee));
}
function exitsTable(){
  $("exits").innerHTML=""; $("exitnote").textContent=last.scope+" · "+last.construction+" · "+last.universe;
  $("exits").append(statTable(last.exits, ["exit_label"],
    r=>{state.exit=r.exit; $("exit").value=r.exit; load();}, r=>r.exit===last.exit));
}
function yearly(shown){
  const box=$("yearly"); box.innerHTML=""; if(!shown.length) return;
  const years=[...new Set(shown[0].dates.map(d=>d.slice(0,4)))];
  const tbl=document.createElement("table"); const tr=document.createElement("tr");
  const h=document.createElement("th"); h.className="l"; h.textContent="year"; tr.append(h);
  for(const s of shown){ const c=document.createElement("th"); c.textContent=label(s.key); c.style.color=color(s.key); tr.append(c); }
  tbl.append(tr);
  for(const y of years){ const row=document.createElement("tr"); const c0=document.createElement("td"); c0.className="l"; c0.textContent=y; row.append(c0);
    for(const s of shown){ let first=-1,lastI=-1; s.dates.forEach((d,i)=>{ if(d.startsWith(y)){ if(first<0) first=i; lastI=i; } });
      const start = first>0 ? 1+s.cum[first-1]/100 : 1; const end = 1+s.cum[lastI]/100; const v=(end/start-1)*100;
      const c=document.createElement("td"); c.textContent=fmt(v,1,"%"); if(v<0) c.classList.add("neg"); row.append(c); }
    tbl.append(row); }
  box.append(tbl);
}

for(const id of ["scope","construction","universe","exit","cap","fee"]) $(id).onchange=e=>{state[id]=e.target.value; load();};
$("yscale").onchange=draw;
for(const id of ["binw","histy","histx"]) $(id).onchange=()=>{drawHist(); drawHistGrid();};
$("hgdet").ontoggle=drawHistGrid;
$("theme").onchange=e=>{ if(e.target.value==="auto") delete document.documentElement.dataset.theme; else document.documentElement.dataset.theme=e.target.value; buildToggles(); draw(); };
matchMedia("(prefers-color-scheme: dark)").addEventListener("change",()=>{buildToggles(); draw();});
readURL(); buildToggles(); load();
</script></body></html>
"""
