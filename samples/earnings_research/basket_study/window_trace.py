"""window_trace — committed, human-inspectable data trace of one date window of the
fade-the-gap basket study (regression fixture for the producer and the /study10y viewer).

  .venv/bin/python earnings_basket_study/window_trace.py --start 2026-05-01 --end 2026-06-01 \
      --out earnings_basket_study/tests/fixtures/window_2026-05 [--profile PROFILE] \
      [--s3-prefix earnings-basket-study/results-10y-h5] \
      [--scopes PX10+ PX50+] [--construction ridge_levels_beta_neutral] [--universe "pure play"] \
      [--exits 1100 close] [--caps 2 5 10 15] [--fees 0 5]

Selection (every flag optional; an omitted flag = every value of that dimension, so with no
selection flag the trace is the whole window and the one canonical deep link QS):
  --scopes        portfolio_analysis scope labels (= the viewer's scope keys)
  --construction  basket constructions      --universe  peer universes (real cells, not POOLED)
  --exits         exit codes of the daily files / viewer (0931 .. 1530, close, d2 .. d5)
  --caps          per-basket caps in %  (2 = portfolio_analysis/, N = portfolio_analysis_capN/)
  --fees          per-share fee in mills (0 = gross, 5 = $0.005/share -> <variant>_fee5m/)
The selection is recorded in manifest.json under `selection` and applied to every file:
  * basket rows: construction x universe in the selection AND able to enter at least one selected
    scope (study_config.scope_filter on the reporter 9:30 entry price for PX scopes, the liquidity
    tier for tier scopes; index scopes need S3 constituents and pass every row). Skip rows that pass
    the filter are kept; all columns are kept.
  * daily files: only the selected variants (caps x fees) and exits, only the columns
    '<scope>|<construction>|<universe>' of the selection + spy_buy_hold.
  * viewer queries: every combination of the selected dimensions (an unselected dimension uses the
    canonical value of QS); the payload's comparison tables keep only the selected rows.

Reads S3 directly (no local copies besides the fixture written here) and writes into --out (the
files below are owned by the generator and removed first, so stale files never linger):
  baskets.parquet        shards_consolidated slice, trade_day in [start, end], ALL statuses (skip rows
                         are part of the trace), selection applied, sorted by trade_day, event_id,
                         construction, universe
  reporter_marks.parquet diagnostics/reporter_marks.parquet: event_id + entry_930_open of the trace's
                         events (finalize_local.attach_reporter_price input; the PX scopes filter on it)
  events.csv             one row per event with at least one trace row, with the REF_CELL row's
                         status, P&L at the close and legs
  daily_returns.parquet, daily_utilization.parquet, daily_returns_stop.parquet
                         window slices of every selected daily file, ONE long-format parquet per kind:
                         columns variant (the S3 portfolio_analysis* dir), exit (null for utilization),
                         stop_level (null unless stop), trade_day, column (original name incl.
                         spy_buy_hold), order (original column position, trade_day = 0), value.
                         wide_slice() pivots one source back to the wide file restricted to the kept
                         columns (trade_day String + columns in original order).
  spy_daily.parquet      SPY RTH open/close (benchmark._daily_from_minutes) for the window + the prior
                         study trade day (spy_returns' base close for the first day)
  expected_viewer.json   study10y.data_json(qs) of every viewer query, computed against the FULL S3
                         files of the selected variants (tables restricted to the selected rows):
                         {"full_query": qs, "summary_keys": [...], "queries": {qs: payload}} with the
                         whole payload for full_query and summarize_payload() for the others (the
                         generator itself checks all of them in full against the trace)
  manifest.json          generation time (ET), generator commit, window, selection, trade days, counts,
                         self-checks and every S3 source read (key, ETag, size, rows/columns kept)

Multi-day marks (pnl_d2..pnl_d5) are booked on the entry trade_day and kept as they are.
rebuild_scope() re-runs the producer (portfolio_analysis.build / build_utilization with the per-basket
cap = aggregate.MAX_BASKET_WEIGHT and the fee = fees.apply_per_share_fee, exactly as load_ok and
portfolio_analysis.main apply --max-basket-weight / --fee-per-share) on the trace; the generator
self-checks every selected variant x exit x scope and tests/test_window_trace_2026_05.py asserts it.
The stop-loss daily files (stop_sim.py) are traced for the viewer but not rebuilt.
"""
import argparse
import contextlib
import datetime as dt
import io
import itertools
import json
import subprocess
import sys
import warnings
from pathlib import Path
from urllib.parse import parse_qs, quote, urlencode

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

QS = ("scope=ALL&construction=ridge_levels_beta_neutral&universe=pure%20play&exit=close"
      "&cap=2&fee=0&start=2026-05-01&end=2026-06-01")
# canonical value of every viewer dimension (the QS deep link) — used for unselected dimensions
CANONICAL = {"scope": "ALL", "construction": "ridge_levels_beta_neutral", "universe": "pure play",
             "exit": "close", "cap": 2, "fee": 0}
REF_CELL = ("ridge_levels_beta_neutral", "pure play")
SHARDS = "shards_consolidated/shards.parquet"
SPY_KEY = "market-data/spy/spy_1min_2y.parquet"
MARKS_KEY = "diagnostics/reporter_marks.parquet"             # reporter_marks.MARKS_KEY
MARKS_PRICE = "entry_930_open"
DEFAULT_CAP, DEFAULT_FEE = 2, 0
STOP_LEVELS = [500, 1000, 1500, 2000]
OWNED = ["baskets.parquet", "reporter_marks.parquet", "events.csv", "spy_daily.parquet",
         "daily_returns.parquet", "daily_utilization.parquet", "daily_returns_stop.parquet",
         "expected_viewer.json", "manifest.json",
         "leg_marks.parquet", "event_signal.parquet", "portfolio_stats.parquet", "expected_points.py"]   # + --pointwise
# viewer comparison tables -> the row keys the selection restricts
TABLE_KEYS = {"cells": ("construction", "universe"), "scopes": ("scope",), "exits": ("exit",),
              "caps": ("cap",), "fees": ("fee",)}
SEL_DIMS = ["scopes", "construction", "universe", "exits", "caps", "fees"]
# expected_viewer.json: the FULL payload for one query, these blocks for every other query (the full
# 32 payloads of the 2026-05 selection are 0.62-0.83 MB, the whole fixture budget is 1 MB)
SUMMARY_KEYS = ["scope", "construction", "universe", "exit", "exit_label", "cap", "fee", "fee_label",
                "window", "windowed", "start", "end", "stats", "stop_rows"]
UTIL_SUMMARY_KEYS = ["avg_pct", "days_traded_pct", "max_basket_weight_pct"]

SLICE_COLS = ["variant", "exit", "stop_level", "trade_day", "column", "order", "value"]


def variant_dir(cap=DEFAULT_CAP, fee=DEFAULT_FEE) -> str:
    """portfolio_analysis dir of a (per-basket cap %, fee mills) variant; mirrors study10y._pa."""
    return ("portfolio_analysis" + ("" if cap == DEFAULT_CAP else f"_cap{cap}")
            + ("" if fee == DEFAULT_FEE else f"_fee{fee}m"))


def pnl_col(exit_code: str) -> str:
    """viewer / file exit code -> shard P&L column (inverse of portfolio_analysis.exit_code)."""
    return "pnl" if exit_code == "close" else f"pnl_{exit_code}"


def query_string(start, end, scope, construction, universe, exit, cap, fee) -> str:
    """Viewer deep link; '+' and ' ' are percent-encoded (PX10+ -> PX10%2B) so parse_qs round-trips."""
    return urlencode(dict(scope=scope, construction=construction, universe=universe, exit=exit,
                          cap=cap, fee=fee, start=start, end=end), quote_via=quote)


def viewer_queries(sel: dict, start, end) -> list:
    """Every combination of the selected dimensions; an unselected dimension (None) = CANONICAL."""
    dims = [("scope", "scopes"), ("construction", "construction"), ("universe", "universe"),
            ("exit", "exits"), ("cap", "caps"), ("fee", "fees")]
    vals = [sel[s] if sel.get(s) is not None else [CANONICAL[d]] for d, s in dims]
    return [query_string(start, end, *combo) for combo in itertools.product(*vals)]


def column_selected(col: str, sel: dict) -> bool:
    """Daily-file column kept by the selection: spy_buy_hold / trade_day always, else
    '<scope>|<construction>|<universe>' with every part selected (None = every value)."""
    if col in ("trade_day", "spy_buy_hold"):
        return True
    parts = col.split("|")
    if len(parts) != 3:
        return all(sel.get(k) is None for k in ("scopes", "construction", "universe"))
    for part, k in zip(parts, ("scopes", "construction", "universe")):
        if sel.get(k) is not None and part not in sel[k]:
            return False
    return True


def restrict_payload(payload: dict, sel: dict) -> dict:
    """Viewer payload with the comparison tables restricted to the selected rows (None = all rows)."""
    keep = {"cells": lambda r: ((sel.get("construction") is None or r["construction"] in sel["construction"])
                                and (sel.get("universe") is None or r["universe"] in sel["universe"])),
            "scopes": lambda r: sel.get("scopes") is None or r["scope"] in sel["scopes"],
            "exits": lambda r: sel.get("exits") is None or r["exit"] in sel["exits"],
            "caps": lambda r: sel.get("caps") is None or r["cap"] in sel["caps"],
            "fees": lambda r: sel.get("fees") is None or r["fee"] in sel["fees"]}
    out = dict(payload)
    for t, f in keep.items():
        if out.get(t) is not None:
            out[t] = [r for r in out[t] if f(r)]
    return out


def full_query(sel: dict, start, end) -> str:
    """The query stored in full: first selected scope / construction / universe, the canonical exit,
    cap and fee when selected (close, 2 %, gross), else the first selected value."""
    pick = lambda k, d: (CANONICAL[d] if sel.get(k) is None or CANONICAL[d] in sel[k] else sel[k][0])
    return query_string(start, end, *(sel[k][0] if sel.get(k) is not None else CANONICAL[d]
                                      for k, d in [("scopes", "scope"), ("construction", "construction"),
                                                   ("universe", "universe")]),
                        pick("exits", "exit"), pick("caps", "cap"), pick("fees", "fee"))


def summarize_payload(payload: dict) -> dict:
    """Summary blocks of a payload: identity, window, window stats per portfolio (incl. n_trades),
    stop-loss rows, utilization summary."""
    out = {k: payload[k] for k in SUMMARY_KEYS}
    u = payload.get("util")
    out["util"] = None if u is None else {k: u[k] for k in UTIL_SUMMARY_KEYS}
    return out


def slice_ident(rel_key: str) -> dict:
    """S3 key relative to the study base -> {fixture, variant, exit, stop_level} of its long-format slice."""
    parts = rel_key.split("/")
    variant = parts[0]
    if parts[1:] == ["daily_utilization.parquet"]:
        return dict(fixture="daily_utilization.parquet", variant=variant, exit=None, stop_level=None)
    if len(parts) == 4 and parts[1] == "by_exit":
        exit_code, name = parts[2], parts[3]
        if name == "daily_returns.parquet":
            return dict(fixture="daily_returns.parquet", variant=variant, exit=exit_code, stop_level=None)
        if name.startswith("daily_returns_stop"):
            lvl = int(name[len("daily_returns_stop"):-len(".parquet")])
            return dict(fixture="daily_returns_stop.parquet", variant=variant, exit=exit_code, stop_level=lvl)
    raise ValueError(f"no fixture mapping for {rel_key}")


def to_long(wide: pd.DataFrame, variant, exit_code, stop_level) -> pd.DataFrame:
    cols = [c for c in wide.columns if c != "trade_day"]
    order = {c: i for i, c in enumerate(wide.columns)}
    long = wide.melt(id_vars="trade_day", value_vars=cols, var_name="column", value_name="value")
    long["order"] = long["column"].map(order).astype("int32")
    long["variant"], long["exit"] = variant, exit_code
    long["stop_level"] = pd.array([stop_level] * len(long), dtype="Int32")
    return long[SLICE_COLS]


def wide_slice(long: pd.DataFrame, variant, exit_code=None, stop_level=None) -> pd.DataFrame:
    """Inverse of to_long for one source: trade_day String + kept columns in original order."""
    m = long["variant"] == variant
    m &= long["exit"].isna() if exit_code is None else long["exit"] == exit_code
    m &= long["stop_level"].isna() if stop_level is None else long["stop_level"] == stop_level
    sub = long[m]
    if not len(sub):
        raise KeyError((variant, exit_code, stop_level))
    w = sub.pivot(index="trade_day", columns="column", values="value")
    cols = sub.drop_duplicates("column").sort_values("order")["column"].tolist()
    w = w[cols].sort_index().reset_index()
    w.columns.name = None
    return w.astype({c: "float64" for c in cols})


@contextlib.contextmanager
def _only_scopes(pa_mod, labels):
    """Restrict portfolio_analysis.scopes() to the given scope labels (index scopes need S3 constituents)."""
    orig = pa_mod.scopes
    keep = [(lab, sc) for lab, sc in orig() if lab in labels]
    missing = set(labels) - {lab for lab, _ in keep}
    if missing:
        raise KeyError(f"unknown scope label(s) {sorted(missing)}")
    pa_mod.scopes = lambda: keep
    try:
        yield
    finally:
        pa_mod.scopes = orig


@contextlib.contextmanager
def _basket_cap(aggregate_mod, cap_pct):
    """aggregate.MAX_BASKET_WEIGHT for a per-basket cap in %, set exactly as portfolio_analysis.main
    does from submit_producers' --max-basket-weight f'{cap / 100:g}' (default cap: untouched)."""
    orig = aggregate_mod.MAX_BASKET_WEIGHT
    if cap_pct != DEFAULT_CAP:
        aggregate_mod.MAX_BASKET_WEIGHT = float(f"{cap_pct / 100:g}")
    try:
        yield
    finally:
        aggregate_mod.MAX_BASKET_WEIGHT = orig


def with_reporter_price(df: pd.DataFrame, marks: pd.DataFrame) -> pd.DataFrame:
    """finalize_local.attach_reporter_price on the trace: study_config.PRICE_COL by event_id."""
    import study_config
    mk = marks[["event_id", MARKS_PRICE]].drop_duplicates("event_id").rename(
        columns={MARKS_PRICE: study_config.PRICE_COL})
    return df.merge(mk, on="event_id", how="left")


def scope_row_mask(df: pd.DataFrame, scope_labels) -> np.ndarray:
    """Rows of df (with PRICE_COL attached) that can enter at least one of the scopes:
    study_config.scope_filter for price / tier / passthrough scopes; index scopes pass every row."""
    import portfolio_analysis as pa
    import study_config
    if scope_labels is None:
        return np.ones(len(df), dtype=bool)
    cfg = dict(pa.scopes())
    pos = pd.Series(np.arange(len(df)), index=df.index)
    m = np.zeros(len(df), dtype=bool)
    for lab in scope_labels:
        sc = cfg[lab]
        if sc.get("indices"):
            return np.ones(len(df), dtype=bool)
        kept = study_config.scope_filter(sc, None)(df)
        m[pos.loc[kept.index].to_numpy()] = True
    return m


def _full_days(frame: pd.DataFrame, days, spy_bh=None) -> pd.DataFrame:
    """Reindex a producer output (trade_day + columns) to the window's study trade days: a day on
    which the trace's cells never traded is 0, exactly as build() fills them over the full table."""
    w = frame.set_index("trade_day").reindex(days)
    cells = [c for c in w.columns if c != "spy_buy_hold"]
    w[cells] = w[cells].fillna(0.0)
    if spy_bh is not None and "spy_buy_hold" in w.columns:
        w["spy_buy_hold"] = spy_bh.reindex(days).fillna(0.0).to_numpy()
    w.index.name = "trade_day"
    return w.reset_index()


def trace_ok(baskets: pd.DataFrame, marks: pd.DataFrame = None, fee=DEFAULT_FEE) -> pd.DataFrame:
    """load_ok on the trace: ok rows, P&L columns to float, reporter price attached from `marks`,
    per-share fee of `fee` mills netted by fees.apply_per_share_fee (adds fee_per_dollar)."""
    import aggregate
    import fees
    ok = baskets[baskets["status"] == "ok"].copy()
    for c in aggregate._exits(ok):
        ok[c] = ok[c].astype(float)
    if marks is not None:
        ok = with_reporter_price(ok, marks)
    return fees.apply_per_share_fee(ok, fee / 1000)


def spy_for_days(spy_daily: pd.DataFrame, days) -> pd.DataFrame:
    """benchmark.spy_returns(days) with spy_daily (index 'YYYY-MM-DD', open/close) as load_spy_daily."""
    import benchmark
    orig = benchmark.load_spy_daily
    benchmark.load_spy_daily = lambda s3io: spy_daily
    try:
        return benchmark.spy_returns(None, list(days))
    finally:
        benchmark.load_spy_daily = orig


def scope_rows(df: pd.DataFrame, scope: str) -> pd.DataFrame:
    """Rows of df (PRICE_COL attached) in one non-index scope: study_config.scope_filter, the filter
    portfolio_analysis._scope_iter applies."""
    import portfolio_analysis as pa
    import study_config
    sc = dict(pa.scopes())[scope]
    if sc.get("indices"):
        raise ValueError(f"index scope {scope} needs S3 constituents")
    return study_config.scope_filter(sc, None)(df)


def rebuild_scope(baskets: pd.DataFrame, spy_daily: pd.DataFrame, scope, exits=("close",),
                  cap=DEFAULT_CAP, fee=DEFAULT_FEE, marks: pd.DataFrame = None, days=None):
    """Producer code path on the trace for one scope label (or a list of labels): ok rows (load_ok:
    status filter, P&L columns to float, reporter price attached from `marks`, per-share fee of `fee`
    mills netted by fees.apply_per_share_fee) -> portfolio_analysis.build() daily files and
    build_utilization() under the per-basket cap `cap` %. spy_daily (index 'YYYY-MM-DD', open/close)
    replaces benchmark.load_spy_daily. days: the window's study trade days (default: the ok rows'
    days). -> (dailies {exit code: frame}, utilization frame)."""
    import aggregate
    import portfolio_analysis as pa
    labels = [scope] if isinstance(scope, str) else list(scope)
    ok = trace_ok(baskets, marks, fee)
    days = sorted(days if days is not None else ok["trade_day"].unique())
    spy = spy_for_days(spy_daily, days)
    with _only_scopes(pa, labels), _basket_cap(aggregate, cap):
        _, _, dailies = pa.build(ok, spy, exits=[pnl_col(e) for e in exits])
        util = pa.build_utilization(ok)
    dailies = {code: _full_days(d, days, spy["buy_hold"]) for code, d in dailies.items()}
    return dailies, _full_days(util, days)


def rebuild_all_scope(baskets: pd.DataFrame, spy_daily: pd.DataFrame, exits=("pnl",), marks=None, days=None):
    """ALL scope, default variant (back-compat wrapper; exits as P&L columns)."""
    import portfolio_analysis as pa
    return rebuild_scope(baskets, spy_daily, "ALL", [pa.exit_code(c) for c in exits], marks=marks, days=days)


def rebuild_stop(baskets: pd.DataFrame, spy_daily: pd.DataFrame, scope, exits=("close",), fee=DEFAULT_FEE,
                 marks: pd.DataFrame = None, days=None, levels=STOP_LEVELS) -> dict:
    """stop_sim.build on the trace (ok rows as trace_ok, default per-basket cap, as stop_sim.main runs
    it for portfolio_analysis[_fee5m]) -> {(exit code, stop level): daily frame over `days`}."""
    import aggregate
    import portfolio_analysis as pa
    import stop_sim
    labels = [scope] if isinstance(scope, str) else list(scope)
    ok = trace_ok(baskets, marks, fee)
    days = sorted(days if days is not None else ok["trade_day"].unique())
    spy = spy_for_days(spy_daily, days)
    with _only_scopes(pa, labels), _basket_cap(aggregate, DEFAULT_CAP), \
            contextlib.redirect_stdout(io.StringIO()), warnings.catch_warnings():
        warnings.simplefilter("ignore", pd.errors.PerformanceWarning)     # stop_sim's column-by-column frame
        _, out = stop_sim.build(ok, spy)
    return {(code, lvl): _full_days(d, days, spy["buy_hold"]) for (code, lvl), d in out.items()
            if code in exits and lvl in levels}


# viewer portfolio keys of portfolio_analysis.PORTFOLIOS (study10y.PA_PORTFOLIO) + the SPY row
PORTFOLIO_KEYS = {"0SPY+1strat": "strat", "1SPY+1strat": "spy1_strat1", "1SPY+2strat": "spy1_strat2"}
SPY_PORTFOLIO = "spy"
STAT_NAMES = ["n_events", "n_trades", "sharpe", "ann_ret_pct", "total_pct", "max_dd_pct", "p2t_days", "p2r_days",
              "recovered", "longest_p2r_days", "longest_dd_depth_pct", "longest_dd_trough", "longest_recovered",
              "beat_spy_pct", "wiped_out"]


def window_stats(daily: pd.DataFrame, cell: str) -> dict:
    """portfolio_analysis.stats_for of every portfolio of one cell over the frame's days, composed as
    portfolio_analysis.build composes them (r = L x strategy + w x spy_buy_hold for (w, L) in
    PORTFOLIOS, beat_spy_pct against spy_buy_hold) + the SPY buy&hold row (stats_for(spy) alone).
    daily: indexed by trade_day with `cell` and spy_buy_hold. -> {portfolio key: stats dict}."""
    import portfolio_analysis as pa
    strat, spy_bh = daily[cell], daily["spy_buy_hold"]
    out = {}
    for pname, (w, lev) in pa.PORTFOLIOS.items():
        out[PORTFOLIO_KEYS[pname]] = pa.stats_for(lev * strat + w * spy_bh, spy_bh)
    out[SPY_PORTFOLIO] = pa.stats_for(spy_bh)
    return out


def cell_counts(ok: pd.DataFrame, scope: str, construction: str, universe: str) -> dict:
    """n_events / n_trades of one cell in one scope as portfolio_analysis.build counts them
    (n_events_by_cell over the scope's ok rows, n_trades = the cell_masks row count)."""
    import portfolio_analysis as pa
    sub = scope_rows(ok, scope)
    if not len(sub):
        return {"n_events": 0.0, "n_trades": 0.0}
    n_events, n_extra = pa.n_events_by_cell(sub)
    n_trades = {(c, u): int(m.sum()) for c, u, m in
                pa.cell_masks(sub["construction"].to_numpy(), sub["universe"].to_numpy())}
    return {"n_events": float(n_extra.get((construction, universe), n_events)),
            "n_trades": float(n_trades.get((construction, universe), 0))}


def _git_head():
    """(HEAD sha, generator file dirty/untracked at generation time)."""
    run = lambda *c: subprocess.run(["git", *c], capture_output=True, text=True, cwd=HERE).stdout.strip()
    return run("rev-parse", "HEAD"), bool(run("status", "--porcelain", "--", Path(__file__).name))


# ------------------------------------------------------------------ point-wise fixture (--pointwise)
POINTWISE_FILES = ["leg_marks.parquet", "event_signal.parquet", "portfolio_stats.parquet", "expected_points.py"]
MANIFESTS_DIR = "manifests/"
CLOSE_MIN = 15 * 60 + 59          # 15:59 ET, the last RTH minute (exit mark of close and day-N holds)
STATS_BY_EXIT = "portfolio_stats_by_exit.parquet"


def mark_codes() -> list:
    """The 22 P&L marks in shard column order: 0931..1530 (evaluate.MARK_LABELS), close, d2..d5."""
    import evaluate as ev
    return [lbl[len("pnl_"):] for lbl in ev.MARK_LABELS] + ["close"] + [lbl[len("pnl_"):] for lbl in ev.HOLD_LABELS]


def short_variant(cap, fee) -> str:
    """Readable test id of a variant: cap2, cap5_fee5m, ..."""
    return f"cap{cap}" + (f"_fee{fee}m" if fee else "")


def leg_mark_terms(legs: pd.DataFrame):
    """(weight, entry_price, mark_price) of one (event, mark) in leg order, mark_price None where the
    leg had no print, + session_covered (False = the panel lacks the day-N session) -> evaluate.leg_pnl_sum."""
    legs = legs.sort_values("leg_order")
    terms = [(float(w), float(e), None if pd.isna(p) else float(p))
             for w, e, p in zip(legs["weight"], legs["entry_price"], legs["mark_price"])]
    covered = bool(legs["session_covered"].iloc[0]) if len(legs) else True
    return terms, covered


def recompute_mark(legs: pd.DataFrame):
    """evaluate.leg_pnl_sum over the leg_marks rows of one (event, mark): the basket P&L at that mark."""
    import evaluate as ev
    terms, covered = leg_mark_terms(legs)
    return ev.leg_pnl_sum(terms, start=0.0 if covered else None)


def fee_drag(baskets: pd.DataFrame, marks: pd.DataFrame, scope: str, cap, fee, days, cell: str) -> pd.Series:
    """Per-day fee drag of one cell: portfolio_analysis.daily_multi of fee_per_dollar (fees.apply_per_share_fee)
    over the scope's ok rows under the per-basket cap — the producer's day weighting (mean over the day's
    baskets x min(1, cap x N)) applied to the fee. gross daily - net daily equals it."""
    import aggregate
    import portfolio_analysis as pa
    sub = scope_rows(trace_ok(baskets, marks, fee), scope)
    with _basket_cap(aggregate, cap):
        dm = pa.daily_multi(sub, ["fee_per_dollar"])["fee_per_dollar"]
    col = cell.split("|", 1)[1]
    return dm[col].reindex(days).fillna(0.0) if col in dm.columns else pd.Series(0.0, index=pd.Index(days))


def event_scope_counts(baskets: pd.DataFrame, marks: pd.DataFrame, scope: str, days) -> pd.DataFrame:
    """ok / skip basket rows per trade day of one scope (scope_rows on the reporter 9:30 price)."""
    sub = scope_rows(with_reporter_price(baskets, marks), scope)
    n_ok = sub[sub["status"] == "ok"].groupby("trade_day").size().reindex(days).fillna(0).astype(int)
    n_skip = sub[sub["status"] != "ok"].groupby("trade_day").size().reindex(days).fillna(0).astype(int)
    return pd.DataFrame({"n_ok": n_ok, "n_skip": n_skip}, index=pd.Index(days, name="trade_day"))


def panel_index(s3, bucket, base):
    """event_id -> panel key from the run's own manifests (<base>manifests/*.json, the plan's panel lists)
    + the manifest sources (key, ETag, size). An event listed under two keys is an error."""
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=base + MANIFESTS_DIR):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".json")]
    idx, srcs = {}, []
    for k in sorted(keys):
        r = s3.get_object(Bucket=bucket, Key=k)
        body = r["Body"].read()
        panels = json.loads(body)["panels"]
        for pk in panels:
            eid = int(pk.rsplit("event_", 1)[1].split(".")[0])
            if idx.setdefault(eid, pk) != pk:
                raise ValueError(f"event {eid} under two panel keys: {idx[eid]} / {pk}")
        srcs.append(dict(key=k, etag=r["ETag"].strip('"'), size=len(body), rows_kept=len(panels),
                         fixture="leg_marks.parquet", note="panel index: the run's plan manifest (event_id -> panel key)"))
    return idx, srcs


_WORKER_S3 = None


def _worker_s3(profile):
    global _WORKER_S3
    if _WORKER_S3 is None:
        import boto3
        from botocore.config import Config
        _WORKER_S3 = boto3.Session(profile_name=profile).client(
            "s3", config=Config(connect_timeout=10, retries={"max_attempts": 3, "mode": "standard"}))
    return _WORKER_S3


def event_signal_row(panel: pd.DataFrame) -> dict:
    """The event's signal from its panel through the producer's own functions (trade_day_for_event,
    consolidate, previous_close, signal_price, fade_the_gap, entry_price)."""
    import evaluate as ev
    from rules.sessions import SIGNAL_MIN
    reporter = panel[panel["reporter_relationship"] == "primary"]["symbol"].iloc[0]
    event_ts = panel[panel["event_bar"]]["ts"].iloc[0]
    out = {"event_id": int(panel["event_id"].iloc[0]), "reporter": reporter,
           "trade_day": ev.trade_day_for_event(event_ts)}
    day = out["trade_day"]
    if day is None:
        return out | dict(prev_close=None, signal_price_928=None, gap=None, direction=None,
                          n_reporter_premarket_prints=0, reporter_entry_930=None)
    bars = ev.consolidate(panel)
    pc, sp = ev.previous_close(bars, reporter, day), ev.signal_price(bars, reporter, day)
    sig = ev.fade_the_gap(sp, pc)
    pre = bars[(bars["symbol"] == reporter) & (bars["et_date"] == day) & (bars["et_min"] <= SIGNAL_MIN)]
    return out | dict(prev_close=pc, signal_price_928=sp, gap=None if sig is None else sig.gap,
                      direction=None if sig is None else sig.direction, n_reporter_premarket_prints=len(pre),
                      reporter_entry_930=ev.entry_price(bars, reporter, day, allow_ffill=False))


def leg_mark_rows(panel: pd.DataFrame, sink: list, day: str) -> list:
    """leg_sink entries -> one row per (leg, mark): price at the mark exactly as evaluate_event booked it;
    ffilled = the leg has no consolidated bar at the mark minute itself (15:59 for close / day N)."""
    import evaluate as ev
    bars = ev.consolidate(panel)
    have = {s: set(zip(g["et_date"], g["et_min"])) for s, g in bars.groupby("symbol")}
    rows = []
    for leg in sink:
        at = have.get(leg["symbol"], set())
        base = {"event_id": leg["event_id"], "construction": leg["construction"], "universe": leg["universe"],
                "leg_order": leg["leg_order"], "symbol": leg["symbol"], "is_reporter": leg["is_reporter"],
                "weight": leg["weight"], "entry_price": leg["entry"]}
        pts = [(lbl[len("pnl_"):], day, m, leg["marks"][lbl], True)
               for m, lbl in zip(ev.MARK_MINUTES, ev.MARK_LABELS)]
        pts.append(("close", day, CLOSE_MIN, leg["close"], True))
        pts += [(lbl[len("pnl_"):], leg["hold_dates"][lbl], CLOSE_MIN, leg["holds"][lbl], leg["hold_covered"][lbl])
                for lbl in ev.HOLD_LABELS]
        for code, d, minute, price, covered in pts:
            rows.append(base | {"mark": code, "mark_day": d, "mark_minute_et": minute, "mark_price": price,
                                "ffilled": None if price is None else (d, minute) not in at,
                                "session_covered": covered})
    return rows


def _pointwise_event(job):
    """Worker: one panel -> (source, recomputed shard rows, leg mark rows, signal row)."""
    key, profile, bucket, constructions, universes = job
    import evaluate as ev
    s3 = _worker_s3(profile)
    r = s3.get_object(Bucket=bucket, Key=key)
    body = r["Body"].read()
    panel = pd.read_parquet(io.BytesIO(body))
    sink = []
    table = ev.event_table(panel, constructions=constructions, universes=universes, leg_sink=sink)
    sig = event_signal_row(panel)
    legs = leg_mark_rows(panel, sink, sig["trade_day"]) if sig["trade_day"] else []
    src = dict(key=key, etag=r["ETag"].strip('"'), size=len(body), event_id=sig["event_id"], fixture="leg_marks.parquet")
    return src, table, legs, sig


def _lit(v):
    """Python literal of one expected value: full-precision repr floats, NaN / NA -> None."""
    if v is None or v is pd.NA:
        return "None"
    if isinstance(v, (bool, np.bool_)):
        return repr(bool(v))
    if isinstance(v, (int, np.integer)):
        return repr(int(v))
    if isinstance(v, (float, np.floating)):
        return "None" if np.isnan(v) else repr(float(v))
    return repr(str(v))


def write_expected_points(path: Path, header: list, tables: dict):
    """Generated module: one list of tuples per family, every value a literal."""
    lines = ['"""' + "\n".join(header) + '\n"""', "# flake8: noqa", ""]
    for name, (fields, rows) in tables.items():
        lines.append(f"{name}_FIELDS = {tuple(fields)!r}")
        lines.append(f"{name} = [")
        lines += ["    (" + ", ".join(_lit(v) for v in row) + ("," if len(row) == 1 else "") + ")," for row in rows]
        lines.append("]")
        lines.append("")
    path.write_text("\n".join(lines))


def _stat_eq(a, b, tol=0.0):
    """(equal, abs diff) of two stats values: None/NaN equal each other, floats within tol, rest exact."""
    na = lambda v: v is None or (isinstance(v, (float, np.floating)) and np.isnan(v))
    if na(a) or na(b):
        return na(a) and na(b), 0.0
    if isinstance(a, (bool, np.bool_, str)) or isinstance(b, (bool, np.bool_, str)):
        return a == b, 0.0
    d = abs(float(a) - float(b))
    return d <= tol, d


def build_pointwise(*, s3, bucket, base, profile, bp, marks_pd, spy_w, trade_days, eff, slices, expected,
                    rebuilt, out, workers, so):
    """--pointwise: leg_marks / event_signal / portfolio_stats parquet + expected_points.py, self-checked
    family by family (A-I). -> (sources, self_check {family: max abs diff / mismatch counts})."""
    import concurrent.futures as cf
    import fees
    import polars as pl
    import evaluate as ev
    cons, unis = eff["construction"], eff["universe"]
    if len(cons) != 1 or len(unis) != 1:
        raise SystemExit("--pointwise needs exactly one --construction and one --universe")
    cname, uname = cons[0], unis[0]
    scopes_ = [lab for lab in eff["scopes"] if not dict(__import__("portfolio_analysis").scopes())[lab].get("indices")]
    cell_of = lambda scope: f"{scope}|{cname}|{uname}"
    variants = [(c, f) for c in eff["caps"] for f in eff["fees"]]
    codes = mark_codes()
    sources, chk = [], {}

    # panels: the run's manifests -> one key per trace event; every panel read through evaluate itself
    idx, man_srcs = panel_index(s3, bucket, base)
    sources += man_srcs
    eids = sorted(int(e) for e in bp["event_id"].unique())
    missing = [e for e in eids if e not in idx]
    if missing:
        raise SystemExit(f"{len(missing)} trace events have no panel in {base}{MANIFESTS_DIR}: {missing[:5]}")
    jobs = [(idx[e], profile, bucket, cons, unis) for e in eids]
    tables, leg_rows, sig_rows = [], [], []
    with cf.ProcessPoolExecutor(max_workers=workers) as pool:
        for i, (src, table, legs, sig) in enumerate(pool.map(_pointwise_event, jobs, chunksize=4), 1):
            sources.append(src)
            tables.append(table)
            leg_rows += legs
            sig_rows.append(sig)
            if i % 200 == 0 or i == len(jobs):
                print(f"  pointwise panels {i:,}/{len(jobs):,}", flush=True)
    panel_bytes = sum(s_["size"] for s_ in sources if "event_id" in s_)
    print(f"pointwise: {len(jobs):,} panels read ({panel_bytes:,} B)", flush=True)

    legs = pd.DataFrame(leg_rows)
    order = {c: i for i, c in enumerate(codes)}
    legs["_o"] = legs["mark"].map(order)
    legs = legs.sort_values(["event_id", "_o", "leg_order"]).drop(columns="_o").reset_index(drop=True)
    legs = legs.astype({"event_id": "int64", "leg_order": "int16", "mark_minute_et": "int16", "weight": "float64",
                        "entry_price": "float64", "mark_price": "Float64", "ffilled": "boolean",
                        "session_covered": "bool", "is_reporter": "bool"})
    _write_pq(legs, out / "leg_marks.parquet")
    sig = pd.DataFrame(sig_rows).sort_values("event_id").reset_index(drop=True)
    sig = sig.astype({"event_id": "int64", "prev_close": "Float64", "signal_price_928": "Float64", "gap": "Float64",
                      "direction": "Int64", "n_reporter_premarket_prints": "int32", "reporter_entry_930": "Float64"})
    _write_pq(sig, out / "event_signal.parquet")

    # re-evaluated shard rows vs the production rows (same panel, same code path)
    rec = pd.concat(tables, ignore_index=True)
    keys = ["event_id", "construction", "universe"]
    m = bp.merge(rec, on=keys, how="outer", suffixes=("", "_re"), indicator=True)
    assert (m["_merge"] == "both").all() and len(m) == len(bp), m["_merge"].value_counts()
    row_diff = {}
    for c in [c for c in bp.columns if c not in keys]:
        a, b2 = m[c], m[c + "_re"]
        if pd.api.types.is_float_dtype(a.dtype) or pd.api.types.is_float_dtype(b2.dtype):
            fa, fb = a.astype("float64").to_numpy(), b2.astype("float64").to_numpy()
            nan_mismatch = int((np.isnan(fa) != np.isnan(fb)).sum())
            both = ~np.isnan(fa) & ~np.isnan(fb)
            row_diff[c] = {"max_abs_diff": float(np.abs(fa[both] - fb[both]).max()) if both.any() else 0.0,
                           "n_not_bit_identical": int((fa[both] != fb[both]).sum()), "nan_mismatch": nan_mismatch}
        else:
            ne = ~((a == b2).fillna(False) | (a.isna() & b2.isna()))
            row_diff[c] = {"n_mismatch": int(ne.sum())}
    chk["recompute_shard_rows"] = row_diff

    ok = bp[bp["status"] == "ok"]
    # A: every mark of every ok row from the legs through evaluate.leg_pnl_sum
    groups = {k: g for k, g in legs.groupby(["event_id", "mark"], sort=False)}
    event_marks, a_max, a_none_mismatch = [], 0.0, 0
    for r in ok.itertuples(index=False):
        for code in codes:
            prod = getattr(r, pnl_col(code))
            prod = None if pd.isna(prod) else float(prod)
            got = recompute_mark(groups[(int(r.event_id), code)])
            if (got is None) != (prod is None):
                a_none_mismatch += 1
            elif got is not None:
                a_max = max(a_max, abs(got - prod))
            event_marks.append((int(r.event_id), code, prod))
    chk["A_event_mark"] = {"max_abs_diff": a_max, "none_mismatch": a_none_mismatch, "n": len(event_marks)}

    # B: signal / direction / status from event_signal
    sg = sig.set_index("event_id")
    event_rows, b_gap, b_dir = [], 0.0, 0
    for r in bp.itertuples(index=False):
        s_ = sg.loc[int(r.event_id)]
        fg = ev.fade_the_gap(None if pd.isna(s_["signal_price_928"]) else float(s_["signal_price_928"]),
                             None if pd.isna(s_["prev_close"]) else float(s_["prev_close"]))
        b_gap = max(b_gap, abs(fg.gap - float(r.signal_return_928_vs_prev_close)))
        b_dir += int(fg.direction != int(r.direction))
        event_rows.append((int(r.event_id), r.status, None if pd.isna(r.skip_reason) else r.skip_reason,
                           None if pd.isna(r.direction) else int(r.direction),
                           None if pd.isna(r.signal_return_928_vs_prev_close) else float(r.signal_return_928_vs_prev_close)))
    chk["B_event_row"] = {"max_abs_diff_gap": b_gap, "direction_mismatch": b_dir, "n": len(event_rows)}

    # C: per-share fee per ok row
    fee_rate = max(eff["fees"]) / 1000 if max(eff["fees"]) else fees.IBKR_PRO_FIXED
    net = fees.apply_per_share_fee(trace_ok(bp, marks_pd, DEFAULT_FEE), fee_rate)
    gross = trace_ok(bp, marks_pd, DEFAULT_FEE)
    c_max = max(float(np.nanmax(np.abs((gross[c] - net[c]) - net["fee_per_dollar"]))) for c in fees.pnl_columns(gross))
    fee_rows = [(int(e), float(v)) for e, v in zip(net["event_id"], net["fee_per_dollar"])]
    chk["C_fee_row"] = {"max_abs_diff": c_max, "n": len(fee_rows), "rate": fee_rate}

    # D / E: daily returns and utilization, rebuilt (already self-checked in full above) vs production slice
    daily_pts, util_pts, d_max, e_max = [], [], 0.0, 0.0
    for c, f in variants:
        vd = variant_dir(c, f)
        dailies, util = rebuilt[(c, f)]
        for scope in scopes_:
            cell = cell_of(scope)
            for e in eff["exits"]:
                prod = wide_slice(slices["daily_returns.parquet"], vd, e).set_index("trade_day")[cell]
                reb = dailies[e].set_index("trade_day")[cell]
                d_max = max(d_max, float(np.abs(reb - prod).max()))
                daily_pts += [(scope, vd, e, d, float(v)) for d, v in prod.items()]
            prod = wide_slice(slices["daily_utilization.parquet"], vd).set_index("trade_day")[cell]
            e_max = max(e_max, float(np.abs(util.set_index("trade_day")[cell] - prod).max()))
            util_pts += [(scope, vd, d, float(v)) for d, v in prod.items()]
    chk["D_daily_return"] = {"max_abs_diff": d_max, "n": len(daily_pts)}
    chk["E_daily_utilization"] = {"max_abs_diff": e_max, "n": len(util_pts)}

    # F: stop-loss daily files through stop_sim.build on the trace
    stop_pts, f_max = [], 0.0
    stop_long = slices.get("daily_returns_stop.parquet")
    for f in eff["fees"]:
        vd = variant_dir(DEFAULT_CAP, f)
        if stop_long is None or vd not in set(stop_long["variant"]):
            continue
        reb = rebuild_stop(bp, spy_w, scopes_, eff["exits"], fee=f, marks=marks_pd, days=trade_days)
        for scope in scopes_:
            cell = cell_of(scope)
            for e in eff["exits"]:
                for lvl in STOP_LEVELS:
                    prod = wide_slice(stop_long, vd, e, lvl).set_index("trade_day")[cell]
                    f_max = max(f_max, float(np.abs(reb[(e, lvl)].set_index("trade_day")[cell] - prod).max()))
                    stop_pts += [(scope, vd, e, lvl, d, float(v)) for d, v in prod.items()]
    chk["F_stop_daily"] = {"max_abs_diff": f_max, "n": len(stop_pts)}

    # G: event counts per scope per day
    count_pts = []
    for scope in scopes_:
        cnt = event_scope_counts(bp, marks_pd, scope, trade_days)
        count_pts += [(scope, d, int(r.n_ok), int(r.n_skip)) for d, r in cnt.iterrows()]
    chk["G_event_count"] = {"n": len(count_pts), "events": int(bp["event_id"].nunique()),
                            "ok": int(len(ok)), "skip": int((bp["status"] != "ok").sum())}

    # H: window totals — stats_for on the rebuilt series == on the production slices; fee drag
    totals, h_max, h_bad = [], 0.0, 0
    drag_max, net_lt_gross = 0.0, True
    for c, f in variants:
        vd = variant_dir(c, f)
        okv = trace_ok(bp, marks_pd, f)
        for scope in scopes_:
            cell = cell_of(scope)
            cnt = cell_counts(okv, scope, cname, uname)
            for e in eff["exits"]:
                reb = window_stats(rebuilt[(c, f)][0][e].set_index("trade_day"), cell)
                prod = window_stats(wide_slice(slices["daily_returns.parquet"], vd, e).set_index("trade_day"), cell)
                for pk in list(PORTFOLIO_KEYS.values()) + [SPY_PORTFOLIO]:
                    full = ({**cnt, **prod[pk]} if pk != SPY_PORTFOLIO else dict(prod[pk]))
                    for stat in STAT_NAMES:
                        if stat not in full:
                            continue
                        if stat in reb[pk]:
                            eq, d = _stat_eq(reb[pk][stat], full[stat], 1e-9)
                            h_max, h_bad = max(h_max, d), h_bad + int(not eq)
                        totals.append((scope, vd, e, pk, stat, full[stat]))
                if f and (c, DEFAULT_FEE) in rebuilt:
                    g = rebuilt[(c, DEFAULT_FEE)][0][e].set_index("trade_day")[cell]
                    n_ = rebuilt[(c, f)][0][e].set_index("trade_day")[cell]
                    drag = fee_drag(bp, marks_pd, scope, c, f, trade_days, cell)
                    drag_max = max(drag_max, float(np.abs((g - n_) - drag).max()))
                    gs = window_stats(rebuilt[(c, DEFAULT_FEE)][0][e].set_index("trade_day"), cell)
                    net_lt_gross &= all(reb[pk]["total_pct"] < gs[pk]["total_pct"] for pk in PORTFOLIO_KEYS.values())
    chk["H_totals"] = {"max_abs_diff_rebuilt_vs_production": h_max, "n_unequal": h_bad, "n": len(totals),
                       "fee_drag_max_abs_diff": drag_max, "net_total_below_gross": bool(net_lt_gross)}

    # I: viewer window summaries vs H
    i_max, i_trades = 0.0, 0
    for qs, payload in expected["queries"].items():
        q = {k: v[0] for k, v in parse_qs(qs).items()}
        if q["scope"] not in scopes_:
            continue
        vd = variant_dir(int(q["cap"]), int(q["fee"]))
        st = window_stats(wide_slice(slices["daily_returns.parquet"], vd, q["exit"]).set_index("trade_day"),
                          cell_of(q["scope"]))
        cnt = cell_counts(trace_ok(bp, marks_pd, int(q["fee"])), q["scope"], cname, uname)
        for pk in list(PORTFOLIO_KEYS.values()) + [SPY_PORTFOLIO]:
            for stat in ("sharpe", "total_pct", "max_dd_pct"):
                i_max = max(i_max, _stat_eq(payload["stats"][pk][stat], st[pk][stat], 1e-9)[1])
            if pk != SPY_PORTFOLIO:
                vt, fl = payload["stats"][pk]["n_trades"], payload["stats"][pk]["n_trades_floor"]
                i_trades += int(not (vt == cnt["n_trades"] if not fl else vt <= cnt["n_trades"]))
    chk["I_viewer_summary"] = {"max_abs_diff": i_max, "n_trades_mismatch": i_trades,
                               "n": len(expected["queries"])}

    # production full-history stats rows of the selection (pinned: the window trace cannot rebuild them)
    stat_frames, prod_stats = [], []
    for c, f in variants:
        vd = variant_dir(c, f)
        key = f"{base}{vd}/{STATS_BY_EXIT}"
        h = s3.head_object(Bucket=bucket, Key=key)
        df = (pl.scan_parquet(f"s3://{bucket}/{key}", storage_options=so)
              .filter(pl.col("exit").is_in(eff["exits"])
                      & ((pl.col("scope").is_in(scopes_) & (pl.col("construction") == cname)
                          & (pl.col("universe") == uname)) | (pl.col("scope") == "SPY")))
              .collect().to_pandas())
        df.insert(0, "variant", vd)
        stat_frames.append(df)
        sources.append(dict(key=key, etag=h["ETag"].strip('"'), size=int(h["ContentLength"]), rows_kept=len(df),
                            fixture="portfolio_stats.parquet", variant_dir=vd,
                            note="FULL-HISTORY production stats (2016-2026), not window stats"))
        for r in df.itertuples(index=False):
            pk = PORTFOLIO_KEYS.get(r.portfolio, SPY_PORTFOLIO)
            prod_stats += [(vd, r.exit, r.scope, pk, stat, getattr(r, stat)) for stat in STAT_NAMES]
    pstats = pd.concat(stat_frames, ignore_index=True)
    _write_pq(pstats, out / "portfolio_stats.parquet")

    header = ["expected_points — GENERATED by earnings_basket_study/window_trace.py --pointwise. DO NOT EDIT.",
              f"generator commit {_git_head()[0]}; window {trade_days[0]} -> {trade_days[-1]} ({len(trade_days)} trade days);",
              f"cell {cname} x {uname}; scopes {scopes_}; exits {eff['exits']}; variants {[variant_dir(*v) for v in variants]}.",
              "EVENT_MARKS / EVENT_ROWS: production shard values (baskets.parquet); DAILY / UTIL / STOP: production daily",
              "files sliced to the window; FEE_ROWS: fees.apply_per_share_fee; TOTALS: portfolio_analysis.stats_for on the",
              "production window slices (n_events / n_trades of the trace's ok rows); EVENT_COUNTS: ok / skip rows per scope",
              "per trade day; PROD_STATS: the production FULL-HISTORY portfolio_stats_by_exit rows (pinned only)."]
    write_expected_points(out / "expected_points.py", header, {
        "EVENT_MARKS": (["event_id", "mark", "expected_pnl"], event_marks),
        "EVENT_ROWS": (["event_id", "status", "skip_reason", "direction", "gap"], event_rows),
        "DAILY": (["scope", "variant", "exit", "trade_day", "expected_return"], daily_pts),
        "UTIL": (["scope", "variant", "trade_day", "value"], util_pts),
        "STOP": (["scope", "variant", "exit", "level", "trade_day", "value"], stop_pts),
        "FEE_ROWS": (["event_id", "fee_per_dollar"], fee_rows),
        "TOTALS": (["scope", "variant", "exit", "portfolio", "stat", "value"], totals),
        "EVENT_COUNTS": (["scope", "trade_day", "n_ok", "n_skip"], count_pts),
        "PROD_STATS": (["variant", "exit", "scope", "portfolio", "stat", "value"], prod_stats),
    })
    chk["panels"] = {"n": len(jobs), "bytes": panel_bytes, "leg_mark_rows": len(legs)}
    return sources, chk


def _write_pq(df: pd.DataFrame, path: Path):
    import pyarrow as pa_
    import pyarrow.parquet as pq
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa_.Table.from_pandas(df, preserve_index=False), path, compression="zstd")


def events_table(b: pd.DataFrame, ref_cell=REF_CELL) -> pd.DataFrame:
    ev_cols = ["event_id", "event_symbol", "event_date", "event_time_et", "trade_day",
               "signal_return_928_vs_prev_close", "direction", "liquidity_tier", "reporter_dollar_vol_day"]
    ev = b.groupby("event_id", sort=False)[ev_cols[1:]].first().reset_index()
    n_ok = b[b["status"] == "ok"].groupby("event_id").size().rename("n_ok_rows")
    ref = b[(b["construction"] == ref_cell[0]) & (b["universe"] == ref_cell[1])].drop_duplicates("event_id")
    lvl = b[b["construction"].isna()].drop_duplicates("event_id")       # event-level skips (no cell rows)
    pick = pd.concat([ref, lvl[~lvl["event_id"].isin(ref["event_id"])]])
    pick = pick.set_index("event_id")[["status", "skip_reason", "pnl", "legs"]].rename(
        columns={"status": "ref_status", "skip_reason": "ref_skip_reason", "pnl": "ref_pnl_close", "legs": "ref_legs"})
    out = ev.join(n_ok, on="event_id").join(pick, on="event_id")
    out["n_ok_rows"] = out["n_ok_rows"].fillna(0).astype(int)
    out["direction"] = out["direction"].astype("Int64")
    return out.sort_values(["trade_day", "event_symbol", "event_id"]).reset_index(drop=True)


def _counts(bp: pd.DataFrame) -> dict:
    ok, skips = bp[bp["status"] == "ok"], bp[bp["status"] != "ok"]
    return {"events": int(bp["event_id"].nunique()), "rows": len(bp), "ok_rows": len(ok),
            "skip_rows": len(skips), "events_with_ok_row": int(ok["event_id"].nunique()),
            "skip_rows_by_reason": {str(k): int(v) for k, v in
                                    skips["skip_reason"].fillna("<null>").value_counts().sort_index().items()}}


def _parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--profile", default=None, help="AWS profile (default: environment credentials)")
    ap.add_argument("--s3-prefix", default="earnings-basket-study/results-10y-h5")
    ap.add_argument("--scopes", nargs="+", default=None, help="scope labels (default: every scope)")
    ap.add_argument("--construction", nargs="+", default=None, help="constructions (default: every one)")
    ap.add_argument("--universe", nargs="+", default=None, help="peer universes (default: every one)")
    ap.add_argument("--exits", nargs="+", default=None, help="exit codes (default: every exit)")
    ap.add_argument("--caps", nargs="+", type=int, default=None, help="per-basket caps in %% (default: every cap)")
    ap.add_argument("--fees", nargs="+", type=int, default=None, help="per-share fees in mills (default: every fee)")
    ap.add_argument("--pointwise", action=argparse.BooleanOptionalAction, default=None,
                    help="also write leg_marks / event_signal / portfolio_stats parquet + expected_points.py "
                         "(default: on when any selection flag is set)")
    ap.add_argument("--workers", type=int, default=8, help="processes reading + re-evaluating panels (--pointwise)")
    return ap.parse_args(argv)


def _validate_selection(sel: dict, study10y):
    import portfolio_analysis as pa
    cells = [(c, u) for c in pa.CONS for u in pa.UNIS] + list(pa.EXTRA_CELLS)
    known = {"scopes": [lab for lab, _ in pa.scopes()],
             "construction": list(dict.fromkeys(c for c, _ in cells)),
             "universe": list(dict.fromkeys(u for _, u in cells)),
             "exits": [e for e, _ in study10y.EXITS], "caps": list(study10y.CAPS), "fees": list(study10y.FEES)}
    for k, vals in sel.items():
        if vals is None:
            continue
        bad = [v for v in vals if v not in known[k]]
        if bad:
            raise SystemExit(f"--{k}: unknown value(s) {bad}; known: {known[k]}")
    return known


def main(argv=None):
    a = _parse_args(argv)
    import boto3
    import polars as pl
    import settings
    import benchmark
    sys.path.insert(0, str(HERE.parent / "tools" / "results_browser"))
    import study10y

    sel = {k: (list(dict.fromkeys(getattr(a, k))) if getattr(a, k) is not None else None) for k in SEL_DIMS}
    known = _validate_selection(sel, study10y)
    eff = {k: (sel[k] if sel[k] is not None else known[k]) for k in SEL_DIMS}   # resolved values
    start, end = dt.date.fromisoformat(a.start).isoformat(), dt.date.fromisoformat(a.end).isoformat()
    bucket = settings.get("s3", "data_bucket")
    base = a.s3_prefix.strip("/") + "/"
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    for name in OWNED:                                   # stale files of an earlier selection never linger
        (out / name).unlink(missing_ok=True)
    s3 = boto3.Session(profile_name=a.profile).client("s3")
    sources = []

    def head(key):
        h = s3.head_object(Bucket=bucket, Key=key)
        return h["ETag"].strip('"'), int(h["ContentLength"])

    # 1. baskets: the full window slice, then the selection
    uri = f"s3://{bucket}/{base}{SHARDS}"
    so = {"aws_profile": a.profile} if a.profile else {}
    b_all = (pl.scan_parquet(uri, storage_options=so)
             .filter(pl.col("trade_day").is_between(pl.lit(start), pl.lit(end))).collect()
             .sort(["trade_day", "event_id", "construction", "universe"], nulls_last=False))
    prior_study_day = (pl.scan_parquet(uri, storage_options=so)
                       .filter((pl.col("status") == "ok") & (pl.col("trade_day") < pl.lit(start)))
                       .select(pl.col("trade_day").max()).collect().item())
    shards_etag, shards_size = head(base + SHARDS)
    trade_days = sorted(b_all.filter(pl.col("status") == "ok")["trade_day"].unique().to_list())

    # reporter 9:30 entry price (attach_reporter_price input), read once with its ETag
    mk_raw = s3.get_object(Bucket=bucket, Key=base + MARKS_KEY)
    mk_body = mk_raw["Body"].read()
    marks_all = (pl.read_parquet(io.BytesIO(mk_body), columns=["event_id", MARKS_PRICE])
                 .filter(pl.col("event_id").is_in(b_all["event_id"].unique().implode())))

    bp_all = b_all.to_pandas()
    m = np.ones(len(bp_all), dtype=bool)
    if sel["construction"] is not None:
        m &= bp_all["construction"].isin(sel["construction"]).to_numpy()
    if sel["universe"] is not None:
        m &= bp_all["universe"].isin(sel["universe"]).to_numpy()
    if sel["scopes"] is not None:
        priced = with_reporter_price(bp_all, marks_all.to_pandas())
        m &= scope_row_mask(priced, sel["scopes"])
    b = b_all.filter(pl.Series(m))
    b.write_parquet(out / "baskets.parquet", compression="zstd")
    sources.append(dict(key=base + SHARDS, etag=shards_etag, size=shards_size, rows_kept=b.height,
                        rows_in_window=b_all.height, fixture="baskets.parquet"))
    marks = marks_all.filter(pl.col("event_id").is_in(b["event_id"].unique().implode())).sort("event_id")
    marks.write_parquet(out / "reporter_marks.parquet", compression="zstd")
    sources.append(dict(key=base + MARKS_KEY, etag=mk_raw["ETag"].strip('"'), size=len(mk_body),
                        rows_kept=marks.height, columns_kept=["event_id", MARKS_PRICE], fixture="reporter_marks.parquet"))
    bp, marks_pd = b.to_pandas(), marks.to_pandas()
    ok = bp[bp["status"] == "ok"]
    print(f"baskets {start} -> {end}: {len(trade_days)} trade days; window {bp_all['event_id'].nunique():,} events "
          f"{len(bp_all):,} rows -> selection {bp['event_id'].nunique():,} events {len(bp):,} rows ({len(ok):,} ok); "
          f"prior study trade day {prior_study_day}", flush=True)

    # 2. events.csv
    ref_cell = REF_CELL if (eff["construction"] and REF_CELL[0] in eff["construction"]
                            and REF_CELL[1] in eff["universe"]) else (eff["construction"][0], eff["universe"][0])
    events_table(bp, ref_cell).to_csv(out / "events.csv", index=False)

    # 3. SPY daily (window + prior study trade day = spy_returns' base close)
    spy_raw = s3.get_object(Bucket=bucket, Key=SPY_KEY)
    spy_body = spy_raw["Body"].read()
    spy_d = benchmark._daily_from_minutes(pd.read_parquet(io.BytesIO(spy_body)))
    prior_spy_day = spy_d.index[spy_d.index < start].max()
    lo = min(prior_spy_day, prior_study_day)
    spy_w = spy_d[(spy_d.index >= lo) & (spy_d.index <= end)]
    spy_w.rename_axis("date").reset_index().pipe(_write_pq, out / "spy_daily.parquet")
    sources.append(dict(key=SPY_KEY, etag=spy_raw["ETag"].strip('"'), size=len(spy_body), rows_kept=len(spy_w),
                        fixture="spy_daily.parquet", note="1-min bars reduced by benchmark._daily_from_minutes"))

    # 4. daily files: every key the viewer may read for the selected variants (+ the traced extras)
    variants = [(c, f) for c in eff["caps"] for f in eff["fees"]]
    allowed = set()
    for c, f in variants:
        allowed |= {f"{base}{variant_dir(c, f)}/by_exit/{e}/daily_returns.parquet" for e in eff["exits"]}
        allowed.add(f"{base}{variant_dir(c, DEFAULT_FEE)}/daily_utilization.parquet")      # study10y._util_key
        allowed |= {f"{base}{variant_dir(DEFAULT_CAP, f)}/by_exit/{e}/daily_returns_stop{lvl}.parquet"   # _stop_key
                    for e in eff["exits"] for lvl in STOP_LEVELS}
    fetched, denied = {}, set()

    def fetch(key, viewer):
        """S3 object -> bytes; records ETag/size and its window x selection slice (checked to pivot back)."""
        if key in fetched:
            fetched[key]["viewer"] |= viewer
            return fetched[key]["body"]
        r = s3.get_object(Bucket=bucket, Key=key)
        body = r["Body"].read()
        df = pd.read_parquet(io.BytesIO(body))
        df = df[(df["trade_day"] >= start) & (df["trade_day"] <= end)].reset_index(drop=True)
        cols = [c for c in df.columns if column_selected(c, sel)]
        df = df[cols]
        ident = slice_ident(key[len(base):])
        long = to_long(df, ident["variant"], ident["exit"], ident["stop_level"])
        pd.testing.assert_frame_equal(wide_slice(long, ident["variant"], ident["exit"], ident["stop_level"]),
                                      df, check_dtype=True)
        fetched[key] = dict(body=body, long=long, viewer=viewer,
                            src=dict(key=key, etag=r["ETag"].strip('"'), size=len(body), rows_kept=len(df),
                                     columns_kept=len(cols) - 1, **ident))
        return body

    def get_object(key):                       # viewer getter: the selected variants' files only
        if key not in allowed:
            denied.add(key)
            raise KeyError(f"outside the selection: {key}")
        try:
            return fetch(key, viewer=True)
        except s3.exceptions.NoSuchKey:
            denied.add(key)
            raise

    # 5. viewer against the full S3 files of the selection
    queries = viewer_queries(sel, start, end)
    study10y.init(get_object, base=base)
    study10y._cache.clear()
    expected = {}
    for qs in queries:
        expected[qs] = restrict_payload(json.loads(study10y.data_json(parse_qs(qs))), sel)
    fq = full_query(sel, start, end)
    ref = expected[fq]
    st = ref["stats"]
    print(f"viewer: {len(queries)} queries; {fq}: {ref['window']}; strat sharpe {st['strat']['sharpe']:.3f} "
          f"total {st['strat']['total_pct']:.3f}% maxdd {st['strat']['max_dd_pct']:.3f}% n_days {st['strat']['n_days']} "
          f"n_trades {st['strat']['n_trades']}; spy total {st['spy']['total_pct']:.3f}%", flush=True)

    # 6. traced extras the viewer does not read: utilization of the fee variants, stop files of every variant
    for c, f in variants:
        vd = variant_dir(c, f)
        listed = set()
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{base}{vd}/"):
            listed |= {o["Key"] for o in page.get("Contents", [])}
        want = [f"{base}{vd}/daily_utilization.parquet"] + [
            f"{base}{vd}/by_exit/{e}/{n}" for e in eff["exits"]
            for n in ["daily_returns.parquet"] + [f"daily_returns_stop{lvl}.parquet" for lvl in STOP_LEVELS]]
        for k in want:
            if k in listed and k not in fetched:
                fetch(k, viewer=False)

    longs = {}
    for k in sorted(fetched):
        src = fetched[k]["src"]
        src["viewer"] = fetched[k]["viewer"]
        sources.append(src)
        longs.setdefault(src["fixture"], []).append(fetched[k]["long"])
    slices = {}
    for fx, parts in longs.items():
        slices[fx] = pd.concat(parts, ignore_index=True).sort_values(
            ["variant", "exit", "stop_level", "order", "trade_day"], na_position="first").reset_index(drop=True)
        _write_pq(slices[fx], out / fx)
    viewer_srcs = {s_["key"]: s_ for s_ in sources if s_.get("viewer")}

    # 7. self-check: the viewer fed ONLY the trace slices reproduces every expected payload exactly
    served = set()

    def trace_get(key):
        if key not in viewer_srcs:
            raise KeyError(f"not in the trace: {key}")
        s_ = viewer_srcs[key]
        import pyarrow as pa_
        import pyarrow.parquet as pq
        buf = io.BytesIO()
        wide = wide_slice(slices[s_["fixture"]], s_["variant"], s_["exit"], s_["stop_level"])
        pq.write_table(pa_.Table.from_pandas(wide, preserve_index=False), buf)
        served.add(key)
        return buf.getvalue()

    study10y.init(trace_get, base=base)
    study10y._cache.clear()
    n_equal = 0
    for qs in queries:
        got = restrict_payload(json.loads(study10y.data_json(parse_qs(qs))), sel)
        if got != expected[qs]:
            raise AssertionError(f"viewer on the trace differs from the full S3 files: {qs}")
        n_equal += 1
    assert served == set(viewer_srcs), sorted(set(viewer_srcs) ^ served)
    print(f"self-check viewer: {n_equal}/{len(queries)} payloads identical on the trace", flush=True)

    # 8. self-check: producer rebuild from the trace == production daily files
    rebuild_scopes = [lab for lab in eff["scopes"]
                      if not dict(__import__("portfolio_analysis").scopes())[lab].get("indices")]
    checks, rebuilt = {}, {}
    for c, f in variants:
        vd = variant_dir(c, f)
        dailies, util = rebuild_scope(bp, spy_w, rebuild_scopes, eff["exits"], cap=c, fee=f,
                                      marks=marks_pd, days=trade_days)
        rebuilt[(c, f)] = (dailies, util)
        for s_ in sources:
            if s_.get("variant") != vd or s_["stop_level"] is not None:
                continue
            prod = wide_slice(slices[s_["fixture"]], vd, s_["exit"]).set_index("trade_day")
            reb = (dailies[s_["exit"]] if s_["exit"] is not None else util).set_index("trade_day")
            cols = [col for col in prod.columns if col.split("|")[0] in rebuild_scopes or col == "spy_buy_hold"]
            assert list(reb.index) == list(prod.index) == trade_days, (vd, s_["exit"])
            name = f"{vd} {'util' if s_['exit'] is None else 'daily ' + s_['exit']}"
            checks[name] = float(np.abs(reb[cols].to_numpy() - prod[cols].to_numpy()).max())
    print("self-check max |rebuilt - production|:", json.dumps(checks), flush=True)
    bad = {k: v for k, v in checks.items() if v != 0.0}
    if bad:
        print(f"WARNING: rebuild differs from production: {bad}", flush=True)

    # 9. point-wise fixture (every mark of every event, every day of every series, window totals)
    pointwise = a.pointwise if a.pointwise is not None else any(sel[k] is not None for k in SEL_DIMS)
    self_check = None
    if pointwise:
        pw_sources, self_check = build_pointwise(
            s3=s3, bucket=bucket, base=base, profile=a.profile, bp=bp, marks_pd=marks_pd, spy_w=spy_w,
            trade_days=trade_days, eff=eff, slices=slices, expected={"queries": expected}, rebuilt=rebuilt,
            out=out, workers=a.workers, so=so)
        sources += pw_sources
        print("self-check pointwise:", json.dumps({k: v for k, v in self_check.items()
                                                   if k != "recompute_shard_rows"}), flush=True)

    full_qs = full_query(sel, start, end)
    assert full_qs in expected, full_qs
    expected_doc = {"full_query": full_qs, "summary_keys": SUMMARY_KEYS + ["util." + k for k in UTIL_SUMMARY_KEYS],
                    "queries": {qs: (p if qs == full_qs else summarize_payload(p)) for qs, p in expected.items()}}
    (out / "expected_viewer.json").write_text(json.dumps(expected_doc, indent=1, sort_keys=True) + "\n")
    manifest = {
        "generated_et": dt.datetime.now(dt.timezone.utc).astimezone(
            __import__("zoneinfo").ZoneInfo("America/New_York")).isoformat(timespec="seconds"),
        "generator": "earnings_basket_study/window_trace.py", "generator_commit": _git_head()[0],
        "generator_uncommitted": _git_head()[1],
        "bucket": bucket, "base": base,
        "selection": {**sel, "variants": [variant_dir(c, f) for c, f in variants],
                      "row_filter": ("construction x universe in the selection AND the row can enter >= 1 selected "
                                     "scope (study_config.scope_filter; reporter price = reporter_marks."
                                     f"{MARKS_PRICE}); skip rows kept when they pass"),
                      "ref_cell": list(ref_cell)},
        "viewer_queries": queries,
        "window": {"start": start, "end": end, "n_trade_days": len(trade_days), "trade_days": trade_days,
                   "prior_study_trade_day": prior_study_day, "prior_spy_trade_day": prior_spy_day},
        "counts": _counts(bp),
        "counts_window_unfiltered": _counts(bp_all),
        "self_check_max_abs_diff": checks,
        "self_check_rebuild_scopes": rebuild_scopes,
        "self_check_viewer_trace_identical": n_equal,
        "pointwise": bool(pointwise),
        "self_check": self_check,
        "viewer_denied_keys": len(denied),
        "sources": sources,
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
    sizes = {p.name: p.stat().st_size for p in sorted(out.iterdir()) if p.name in OWNED}
    print(f"wrote {out} ({len(sources)} sources, {len(denied)} viewer keys outside the selection): "
          + ", ".join(f"{k} {v:,}" for k, v in sizes.items()) + f"; total {sum(sizes.values()):,} B")


if __name__ == "__main__":
    main()
