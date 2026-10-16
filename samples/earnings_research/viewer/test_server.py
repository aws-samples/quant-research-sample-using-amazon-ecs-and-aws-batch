"""Tests for the results-browser server endpoints (no real S3 — stubbed).

Run: scripts/.venv/bin/python -m pytest tools/results_browser/test_server.py
"""
import io
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import server


@pytest.fixture()
def parquet_bytes():
    df = pd.DataFrame({"scope": ["SMALL", "MID"], "sharpe": [1.5, 0.3],
                       "ann_ret_pct": [10.2, 4.0]})
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


@pytest.fixture()
def stub_s3(monkeypatch, parquet_bytes):
    monkeypatch.setattr(server, "get_object", lambda key: parquet_bytes)
    monkeypatch.setattr(server, "_bucket", "test-bucket")


def test_arrow_roundtrip(stub_s3):
    body, ctype = server.render_arrow("x/portfolio_stats.parquet")
    assert ctype == "application/vnd.apache.arrow.stream"
    table = ipc.open_stream(pa.BufferReader(body)).read_all()
    assert table.num_rows == 2
    assert set(table.column_names) == {"scope", "sharpe", "ann_ret_pct"}


def test_table_page_embeds_viewer(stub_s3):
    body = server.render_table("x/portfolio_stats.parquet")
    text = body.decode()
    assert "perspective-viewer" in text
    assert "/arrow?key=x/portfolio_stats.parquet" in text


def test_view_links_to_grid(stub_s3):
    body = server.render_view("x/portfolio_stats.parquet")
    assert b"/table?key=" in body


def test_study10y_single_cell_coercion():
    import study10y as s
    assert s.coerce_cell("reporter_only", "pure play") == ("reporter_only", "none")
    assert s.coerce_cell("ridge_levels_beta_neutral", "SPY") == ("spy_beta_levels_neutral", "SPY")
    assert s.coerce_cell("spy_dollar_neutral", "all") == ("spy_dollar_neutral", "SPY")
    assert s.coerce_cell("ridge_levels_beta_neutral", "pure play") == ("ridge_levels_beta_neutral", "pure play")
    cons, unis = s.members("POOLED", "POOLED")
    assert "reporter_only" not in cons and not any(c.startswith("spy_") for c in cons)
    assert "none" not in unis and "SPY" not in unis
    assert s.members("spy_beta_returns_neutral", "POOLED") == (["spy_beta_returns_neutral"], ["SPY"])


def test_study10y_corr_universe_is_hedged_only_and_outside_pooled():
    """Corr-peer arm (2026-10-11): 'corr' is a universe of the four hedged
    constructions (POOLED construction = their mean), never a member of the POOLED universe."""
    import study10y as s
    assert "corr" in s.UNIVERSES and "corr" not in s.HEDGED_UNIS
    assert s.coerce_cell("ridge_levels_beta_neutral", "corr") == ("ridge_levels_beta_neutral", "corr")
    assert s.members("ridge_levels_beta_neutral", "corr") == (["ridge_levels_beta_neutral"], ["corr"])
    assert s.members("POOLED", "corr") == (s.HEDGED_CONS, ["corr"])
    assert s.members("ridge_levels_beta_neutral", "POOLED")[1] == s.HEDGED_UNIS
    assert s.coerce_cell("spy_dollar_neutral", "corr") == ("spy_dollar_neutral", "SPY")


# ---------------------------------------------------------------- /study10y date window
def _pq(df):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


_W_UNIS = ["all", "correlated", "functional", "pure play"]
_W_CELL = "ALL|ridge_levels_beta_neutral|"


@pytest.fixture()
def study10y_window(monkeypatch):
    """Synthetic study10y artifacts under base 'x/' injected via study10y.init; module
    state (_get, BASE, PA, _cache) is restored by monkeypatch after the test."""
    import numpy as np
    import study10y as s
    days = pd.bdate_range("2026-04-20", "2026-06-05")
    rng = np.random.default_rng(7)
    ret = pd.DataFrame({"trade_day": days.strftime("%Y-%m-%d")})
    util = pd.DataFrame({"trade_day": days.strftime("%Y-%m-%d")})
    for u in _W_UNIS:
        ret[_W_CELL + u] = rng.normal(0.001, 0.01, len(days))
        n = rng.integers(0, 40, len(days))      # < 50 baskets: under the 2% x 50 = 100% cap
        n[3] = 70                                   # one day at the 100% cap
        util[_W_CELL + u] = np.minimum(1.0, n * 2 / 100)
    ret["spy_buy_hold"] = rng.normal(0.0005, 0.008, len(days))
    stat_cols = dict(sharpe=9.99, ann_ret_pct=1.0, total_pct=2.0, max_dd_pct=-3.0, p2t_days=4, p2r_days=5,
                     recovered=True, n_events=123, n_trades=45, beat_spy_pct=55.0, wiped_out=False)
    rows = [dict(exit="close", exit_label="close", scope="ALL", construction="ridge_levels_beta_neutral",
                 universe="pure play", portfolio=p, **stat_cols) for p in ["0SPY+1strat", "1SPY+1strat", "1SPY+2strat"]]
    rows.append(dict(exit="close", exit_label="close", scope="SPY", construction="", universe="",
                     portfolio="SPY", **{**stat_cols, "sharpe": 0.77}))
    hist_cols = ["exit", "scope", "counts"] + s.HIST_SCALARS          # HIST_SCALARS carries construction/universe
    hist = pd.DataFrame({c: pd.Series([], dtype="object") for c in hist_cols})
    files = {"x/portfolio_analysis/by_exit/close/daily_returns.parquet": _pq(ret),
             "x/portfolio_analysis/daily_utilization.parquet": _pq(util),
             "x/portfolio_analysis/portfolio_stats_by_exit.parquet": _pq(pd.DataFrame(rows)),
             "x/portfolio_analysis/trade_pnl_hist.parquet": _pq(hist)}

    def fake_get(key):
        if key not in files:
            raise KeyError(key)
        return files[key]

    monkeypatch.setattr(s, "_get", s._get)
    monkeypatch.setattr(s, "BASE", s.BASE)
    monkeypatch.setattr(s, "PA", s.PA)
    monkeypatch.setattr(s, "_cache", {})
    s.init(fake_get, base="x/")
    ret_i = ret.assign(trade_day=pd.to_datetime(ret.trade_day)).set_index("trade_day")
    util_i = util.assign(trade_day=pd.to_datetime(util.trade_day)).set_index("trade_day")
    return s, ret_i, util_i


def _wdata(s, **kw):
    return s.data("ALL", "ridge_levels_beta_neutral", "pure play", "close", 2, 0, **kw)


def test_study10y_window_series_inside_and_label(study10y_window):
    s, ret, _ = study10y_window
    d = _wdata(s, start="2026-05-01", end="2026-06-01")
    lo, hi = pd.Timestamp("2026-05-01"), pd.Timestamp("2026-06-01")
    n = int(((ret.index >= lo) & (ret.index <= hi)).sum())
    assert d["windowed"] is True and d["start"] == "2026-05-01" and d["end"] == "2026-06-01"
    for ser in d["series"] + d["stop_series"]:
        assert ser["dates"] and all("2026-05-01" <= x <= "2026-06-01" for x in ser["dates"])
        assert len(ser["dates"]) == n
    assert all("2026-05-01" <= x <= "2026-06-01" for x in d["util"]["dates"])
    assert d["window"] == f"2026-05-01 -> 2026-06-01 ({n:,} trade days)"
    for k in ["hist", "hist_grid", "exc_groups", "exc_touch", "fee_drag_bps", "util_buckets", "adv_cap"]:
        assert d[k] is None
    import json
    json.dumps(d, allow_nan=False)


def test_study10y_window_stats_match_hand_computation(study10y_window):
    import numpy as np
    s, ret, util = study10y_window
    d = _wdata(s, start="2026-05-01", end="2026-06-01")
    sl = ret.loc["2026-05-01":"2026-06-01"]
    r = sl[_W_CELL + "pure play"].to_numpy()
    spy = sl["spy_buy_hold"].to_numpy()
    st = d["stats"]["strat"]
    assert st["sharpe"] == pytest.approx(r.mean() / r.std(ddof=1) * np.sqrt(252))
    assert st["total_pct"] == pytest.approx((np.prod(1 + r) - 1) * 100)
    assert st["n_days"] == len(r) and st["n_events"] is None
    assert st["beat_spy_pct"] == pytest.approx(float((r > spy).mean()) * 100)
    p2 = 2 * r + spy
    assert d["stats"]["spy1_strat2"]["sharpe"] == pytest.approx(p2.mean() / p2.std(ddof=1) * np.sqrt(252))
    assert d["stats"]["spy"]["total_pct"] == pytest.approx((np.prod(1 + spy) - 1) * 100)
    usl = util.loc["2026-05-01":"2026-06-01", _W_CELL + "pure play"]
    assert st["n_trades"] == int(np.round(usl.to_numpy() * 100 / 2).sum())
    assert st["n_trades_floor"] is False                    # the 100% day (2026-04-23) is outside the window
    # tables rebuilt from the windowed daily file, same row shape as the full-history path
    cell = next(x for x in d["cells"] if x["construction"] == "ridge_levels_beta_neutral" and x["universe"] == "pure play")
    assert cell["strat.sharpe"] == pytest.approx(st["sharpe"]) and cell["strat.n_events"] is None
    assert cell["util_avg_pct"] == pytest.approx(usl.mean() * 100)
    pooled = next(x for x in d["cells"] if x["construction"] == "POOLED" and x["universe"] == "POOLED")
    assert pooled["strat.total_pct"] == pytest.approx((np.prod(1 + sl[[_W_CELL + u for u in _W_UNIS]].mean(axis=1)) - 1) * 100)
    assert [x["scope"] for x in d["scopes"]] == ["ALL"]
    assert [x["exit"] for x in d["exits"]] == ["close"]
    assert [x["cap"] for x in d["caps"]] == [2] and [x["fee"] for x in d["fees"]] == [0]
    # whole-history window includes the 100%-utilization day -> lower-bound flag
    full = _wdata(s, start="2026-04-01")
    assert full["stats"]["strat"]["n_trades_floor"] is True and full["cells"][0]["n_trades_floor"] is True


def test_study10y_window_stats_drawdown_path():
    import study10y as s
    idx = pd.bdate_range("2026-05-04", periods=5)
    r = pd.Series([0.1, -0.5, 0.2, 1.0, -0.1], index=idx)   # eq 1.1, .55, .66, 1.32, 1.188
    st = s._window_stats(r, pd.Series(0.0, index=idx))
    assert st["max_dd_pct"] == pytest.approx(-50.0)
    assert st["p2t_days"] == 1 and st["p2r_days"] == 3 and st["recovered"] is True
    assert st["total_pct"] == pytest.approx(18.8)
    assert st["beat_spy_pct"] == pytest.approx(60.0) and st["wiped_out"] is False
    w = s._window_stats(pd.Series([0.1, -1.2, 0.5], index=idx[:3]), pd.Series(0.0, index=idx[:3]))
    assert w["wiped_out"] is True and w["ann_ret_pct"] == -100.0 and w["total_pct"] == pytest.approx(-100.0)


def test_study10y_window_errors(study10y_window):
    s, _, _ = study10y_window
    with pytest.raises(ValueError, match="no trade days"):
        _wdata(s, start="2026-05-02", end="2026-05-03")      # a weekend
    with pytest.raises(ValueError):
        s.data_json({"start": ["2026-06-01"], "end": ["2026-05-01"]})
    with pytest.raises(ValueError):
        s.data_json({"start": ["2026-13-01"]})
    with pytest.raises(ValueError):
        s.data_json({"end": ["06/01/2026"]})


def test_study10y_no_window_unchanged(study10y_window):
    s, ret, _ = study10y_window
    d = _wdata(s)
    assert d["windowed"] is False and d["start"] is None and d["end"] is None
    assert d["stats"]["strat"]["sharpe"] == 9.99 and d["stats"]["strat"]["n_events"] == 123
    assert d["stats"]["spy"]["sharpe"] == 0.77
    assert d["cells"][0]["strat.sharpe"] == 9.99
    assert d["window"] == f"2026-04-20 -> 2026-06-05 ({len(ret):,} trade days)"
    assert d["hist"] is None and d["hist_grid"] == []
    # data_json with empty start/end = no window
    import json
    j = json.loads(s.data_json({"scope": ["ALL"], "start": [""], "end": [""]}))
    assert j["windowed"] is False and j["stats"]["strat"]["sharpe"] == 9.99


def test_study10y_page_has_window_controls():
    import study10y as s
    page = s.render_page().decode()
    for el in ['id="start"', 'id="end"', 'id="fullhist"', 'type="date"']:
        assert el in page


# ------------------------------------------- /study10y window regression (committed trace)
# repo-relative trace dir in both layouts: this repo (earnings_basket_study/) and the exported
# aws-samples tree (samples/earnings_research/basket_study/)
_TRACE_RELS = (Path("earnings_basket_study") / "tests" / "fixtures" / "window_2026-05",
               Path("samples") / "earnings_research" / "basket_study" / "tests" / "fixtures" / "window_2026-05")
# full payload stored for this query; summary blocks for the other 31 (manifest viewer_queries)
_TRACE_QS = ("scope=PX10%2B&construction=ridge_levels_beta_neutral&universe=pure%20play&exit=close"
             "&cap=2&fee=0&start=2026-05-01&end=2026-06-01")
_TRACE_TABLE_KEYS = {"cells": ("construction", "universe"), "scopes": ("scope",), "exits": ("exit",),
                     "caps": ("cap",), "fees": ("fee",)}


def _trace_dir():
    """Walk up from this file to the dir holding the trace (either layout in _TRACE_RELS): the repo
    root in a checkout, the zip root on Batch (scripts/batch_test.py --pkg tools/results_browser
    --extra <trace dir>)."""
    for d in [Path(__file__).resolve().parent, *Path(__file__).resolve().parents]:
        for rel in _TRACE_RELS:
            if (d / rel / "manifest.json").exists():
                return d / rel
    return None


def _trace_wide(long, variant, exit_code, stop_level):
    """Pivot one source of a long-format trace slice back to the exact wide S3 file
    (trade_day String + original columns in original order); mirrors window_trace.wide_slice."""
    m = long["variant"] == variant
    m &= long["exit"].isna() if exit_code is None else long["exit"] == exit_code
    m &= long["stop_level"].isna() if stop_level is None else long["stop_level"] == stop_level
    sub = long[m]
    cols = sub.drop_duplicates("column").sort_values("order")["column"].tolist()
    w = sub.pivot(index="trade_day", columns="column", values="value")[cols].sort_index().reset_index()
    w.columns.name = None
    return w.astype({c: "float64" for c in cols})


def _close(a, b, path):
    if isinstance(a, bool) or isinstance(b, bool) or a is None or b is None or isinstance(a, str):
        assert a == b, path
    else:
        assert a == pytest.approx(b, rel=1e-9, abs=0), path


def _deep_close(got, exp, path, strict=True):
    """got matches exp: dicts on exp's keys (strict: and no other key), lists element-wise, leaves _close."""
    if isinstance(exp, dict):
        assert isinstance(got, dict), path
        if strict:
            assert set(got) == set(exp), (path, sorted(set(got) ^ set(exp)))
        for k, v in exp.items():
            assert k in got, f"{path}.{k}"
            _deep_close(got[k], v, f"{path}.{k}", strict)
    elif isinstance(exp, list):
        assert isinstance(got, list) and len(got) == len(exp), (path, len(got) if isinstance(got, list) else got, len(exp))
        for i, (g, e) in enumerate(zip(got, exp)):
            _deep_close(g, e, f"{path}[{i}]", strict)
    else:
        _close(got, exp, path)


def test_study10y_window_2026_05_regression(monkeypatch):
    """data_json for every deep link of the narrowed trace (2 scopes x 2 exits x 4 caps x 2 fees =
    32 queries), fed ONLY the committed trace slices, must equal the payload computed against the
    full S3 files on generation day (expected_viewer.json): the whole payload for _TRACE_QS (the
    comparison tables restricted to the traced rows), the summary blocks (identity, window, stats
    per portfolio incl. n_trades, stop-loss rows, utilization summary) for the other 31."""
    import json
    from urllib.parse import parse_qs
    import pyarrow as pa_
    import pyarrow.parquet as pq
    import study10y as s
    fix = _trace_dir()
    if fix is None:
        pytest.skip(f"window trace fixture not present: {_TRACE_RELS[0]} (or {_TRACE_RELS[1]})")
    man = json.loads((fix / "manifest.json").read_text())
    longs, srcs = {}, {}
    for src in man["sources"]:
        if "variant" in src:
            srcs[src["key"]] = src
            if src["fixture"] not in longs:
                longs[src["fixture"]] = pd.read_parquet(fix / src["fixture"])
    served = []

    def fake_get(key):
        if key not in srcs:
            raise KeyError(f"not in the trace: {key}")
        src = srcs[key]
        wide = _trace_wide(longs[src["fixture"]], src["variant"], src["exit"], src["stop_level"])
        buf = io.BytesIO()
        pq.write_table(pa_.Table.from_pandas(wide, preserve_index=False), buf)
        served.append(key)
        return buf.getvalue()

    monkeypatch.setattr(s, "_get", s._get)
    monkeypatch.setattr(s, "BASE", s.BASE)
    monkeypatch.setattr(s, "PA", s.PA)
    monkeypatch.setattr(s, "_cache", {})
    s.init(fake_get, base=man["base"])
    assert man["base"] == "earnings-basket-study/results-10y-h5/"
    doc = json.loads((fix / "expected_viewer.json").read_text())
    queries = man["viewer_queries"]
    assert len(queries) == len(set(queries)) == 32 and sorted(doc["queries"]) == sorted(queries)
    assert doc["full_query"] == _TRACE_QS and _TRACE_QS in queries
    sel = man["selection"]
    for qs in queries:
        q = parse_qs(qs)
        assert (q["scope"][0] in sel["scopes"] and q["exit"][0] in sel["exits"]
                and int(q["cap"][0]) in sel["caps"] and int(q["fee"][0]) in sel["fees"]), qs
        got = json.loads(s.data_json(q))
        exp = doc["queries"][qs]
        assert got["window"] == "2026-05-01 -> 2026-06-01 (21 trade days)", qs
        assert set(got["stats"]) == set(exp["stats"]) == {"strat", "spy1_strat1", "spy1_strat2", "spy"}, qs
        if qs != doc["full_query"]:
            got_sum = {k: got[k] for k in exp if k != "util"}
            got_sum["util"] = (got["util"] if exp["util"] is None or got["util"] is None
                               else {k: got["util"][k] for k in exp["util"]})
            _deep_close(got_sum, exp, qs)
            continue
        for t, keys in _TRACE_TABLE_KEYS.items():         # live rows of the traced identities, in order
            want = {tuple(r[k] for k in keys) for r in exp[t]}
            assert len(want) == len(exp[t]) > 0, (qs, t)
            got[t] = [r for r in got[t] if tuple(r[k] for k in keys) in want]
        _deep_close(got, exp, qs)
    viewer_keys = sorted(k for k, v in srcs.items() if v["viewer"])
    assert sorted(set(served)) == viewer_keys                 # every viewer-traced file was requested, nothing else
    st = json.loads(s.data_json(parse_qs(_TRACE_QS)))["stats"]   # live numbers on generation day
    strat = st["strat"]
    assert (round(strat["sharpe"], 3), round(strat["total_pct"], 3), round(strat["max_dd_pct"], 3)) == (2.238, 0.981, -1.287)
    assert strat["n_days"] == 21 and strat["n_trades"] == 407 and round(st["spy"]["total_pct"], 3) == 5.568
