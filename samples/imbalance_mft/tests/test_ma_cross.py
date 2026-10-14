"""ma_cross.py: plan content, the sig / bask builders on synthetic frames, the pure evaluation (rules x combos x lags, baseline, split-half, per-coin), controller bookkeeping — no S3."""
import numpy as np, polars as pl, pytest
import ma_cross as mx
import ma_cross_core as mc

DAY0 = 1_789_430_400_000_000; S = 1_000_000


def _mas(t_closes, vals, windows=None):
    windows = windows or mc.WINDOWS; d = {"venue": ["ALL"] * len(t_closes), "t_close": t_closes}
    for i, n in enumerate(windows):
        d[f"timb_ma{n}"] = pl.Series([v * (1 + i) for v in vals], dtype=pl.Float32); d[f"bimb_ma{n}"] = pl.Series([-v * (1 + i) for v in vals], dtype=pl.Float32)
    return pl.DataFrame(d)


def _bars(t_closes, prices):
    return pl.DataFrame({"venue": ["ALL"] * len(t_closes), "t_close": t_closes, "vwap": prices, "close": prices, "partial": [False] * len(t_closes)})


def test_plan_doc_counts():
    T = pl.DataFrame({"coin": ["SOL", "XRP"], "kind": ["perp", "spot"], "adv": [2.0, 1.0]}); dates = ["2026-09-01", "2026-09-02", "2026-09-03"]
    d = mx.plan_doc("macxA", "tfl4w2-20261009", dates, T)
    assert d["rules"] == 1026 and d["pairs"] == 171 and d["windows"] == mc.WINDOWS and d["combos"] == ["both", "btc", "eth"] and d["lags_s"] == [0, 1] and d["source_coins"] == ["BTC", "ETH"]
    assert d["targets"] == [{"coin": "SOL", "kind": "perp"}, {"coin": "XRP", "kind": "spot"}] and d["sig_elements"] == 6 and d["bask_elements"] == 3


def test_missing_work_and_tiers():
    p = {"dates": ["2026-09-01", "2026-09-02"], "source_coins": ["BTC", "ETH"]}
    m = mx.missing_work(p, have_sig={"BTC_2026-09-01", "ETH_2026-09-01", "BTC_2026-09-02"}, have_bask={"2026-09-02"}, have_done=False)
    assert m["sig"] == [["ETH", "2026-09-02"]] and m["bask"] == [["2026-09-01"]] and m["pnl"] is True
    assert mx.tier("sig", 0) == (8, 32000, 3) and mx.tier("pnl", 0) == (32, 250000, 10) and mx.tier("sig", 1)[1] == 64000 and mx.tier("bask", 0) == (8, 64000, 3)


def test_build_sig_columns_dtypes_and_states():
    grid = DAY0 + np.arange(4) * S                                                  # τ = 0, 1, 2, 3 s
    frames = {"imb_usd": _mas([DAY0 - S, DAY0 + S], [0.1, -0.1]), "imb_sec": None, "px_usd": _bars([DAY0 - S, DAY0 + 2 * S + 1], [100.0, 101.0]), "px_sec": None}
    X = mx.build_sig(frames, grid, windows=[3, 5])
    cols = mx.sig_columns(windows=[3, 5]); assert X.columns == ["t"] + cols and len(cols) == 6 and all(X.schema[c] == pl.Int8 for c in cols)
    # imb_usd timb: ma3 = v, ma5 = 2v → fast − slow = −v: v=0.1 → −1 at τ ≥ 0 (bar closed at −1 s); v=−0.1 → +1 from τ = 1 s (bar closes exactly at 1 s)
    assert X["imb_usd:timb:3:5"].to_list() == [-1, 1, 1, 1] and X["imb_usd:bimb:3:5"].to_list() == [1, -1, -1, -1]
    assert X["imb_sec:timb:3:5"].to_list() == [0, 0, 0, 0] and X["px_sec:px:3:5"].to_list() == [0, 0, 0, 0]       # arm without data → undecided
    assert X["px_usd:px:3:5"].to_list() == [0, 0, 0, 0]                                                                # two bars: no 3-bar MA yet


def test_build_sig_price_states_with_enough_bars():
    grid = DAY0 + np.arange(2) * S; tc = [DAY0 - 6 * S + k * S for k in range(6)]; px = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]      # rising: fast MA > slow MA
    X = mx.build_sig({"imb_usd": None, "imb_sec": None, "px_usd": _bars(tc, px), "px_sec": _bars(tc, px[::-1])}, grid, windows=[3, 5])
    assert X["px_usd:px:3:5"].to_list() == [1, 1] and X["px_sec:px:3:5"].to_list() == [-1, -1]


def test_build_basket_mean_and_coin_columns():
    grid = DAY0 + np.arange(3) * S
    tb = {"SOL": pl.DataFrame({"venue": ["ALL"] * 3, "t_bucket": [DAY0 - S, DAY0, DAY0 + S], "mid": [100.0, 101.0, 101.0], "spread_bp": [2.0, 4.0, 4.0]}), "XRP": pl.DataFrame({"venue": ["ALL"] * 2, "t_bucket": [DAY0 - S, DAY0], "mid": [2.0, 2.02], "spread_bp": [6.0, 6.0]}), "NONE": None}
    Bk, C = mx.build_basket(tb, grid, ["SOL", "XRP", "NONE"])
    assert Bk.columns == ["t", "r_bp", "n_coins", "spread_bp"] and C.columns == ["t", "SOL", "XRP", "NONE"] and C.schema["SOL"] == pl.Float32
    assert Bk["n_coins"].to_list() == [2, 2, 2] and Bk["r_bp"][0] == pytest.approx((1e4 * np.log(1.01) + 1e4 * np.log(1.01)) / 2, rel=1e-5) and Bk["r_bp"][1] == 0.0
    assert Bk["spread_bp"].to_list() == pytest.approx([4.0, 5.0, 5.0])                   # spread as of τ_j: the bar closing at or before τ_j (bucket τ_j − 1 s)
    assert C["NONE"].null_count() == 3


def test_primary_venue_and_single_venue_mark():
    T = pl.DataFrame({"venue": ["ALL", "ALL", "binance_futures", "binance_futures", "bybit"], "t_bucket": [DAY0 - S, DAY0, DAY0 - S, DAY0, DAY0 - S], "mid": [10.0, 10.1, 10.0, 10.2, 10.0], "spread_bp": [1.0] * 5, "usd": [300.0, 300.0, 200.0, 200.0, 100.0]})
    assert mx.primary_venue(T) == "binance_futures" and mx.primary_venue(None) is None and mx.primary_venue(T.filter(pl.col("venue") == "ALL")) is None
    grid = DAY0 + np.arange(1) * S
    Bp, _ = mx.build_basket({"SOL": T}, grid, ["SOL"], venue_of={"SOL": "binance_futures"}); Ba, _ = mx.build_basket({"SOL": T}, grid, ["SOL"])
    assert Bp["r_bp"][0] == pytest.approx(1e4 * np.log(10.2 / 10.0), rel=1e-5) and Ba["r_bp"][0] == pytest.approx(1e4 * np.log(10.1 / 10.0), rel=1e-5)
    assert list(mx.MARKS) == ["pooled", "primary", "fresh", "btc", "eth"] and mx.MARKS["fresh"]["primary"] and mx.MARKS["fresh"]["fresh"] and mx.MARKS["btc"]["coins"] == ["BTC"]
    assert mx.mark_coins("eth", [{"coin": "SOL"}]) == ["ETH"] and mx.mark_coins("primary", [{"coin": "SOL"}, {"coin": "XRP"}]) == ["SOL", "XRP"]
    Bf, Cf = mx.build_basket({"SOL": T}, DAY0 + np.arange(2) * S, ["SOL"], venue_of={"SOL": "binance_futures"}, fresh=True)
    assert Bf["r_bp"][0] == pytest.approx(1e4 * np.log(10.2 / 10.0), rel=1e-5) and Bf["r_bp"][1] is None and Bf["n_coins"].to_list() == [1, 0]


def test_with_mark_tags_every_table():
    out = {"rules": pl.DataFrame({"rule": ["a"]}), "baseline": pl.DataFrame({"name": ["always_long"]})}
    o = mx.with_mark(out, "primary", 1.5); assert o["rules"]["mark"].to_list() == ["primary"] and o["baseline"]["half_spread_bp"][0] == 1.5 and o["baseline"]["mark"][0] == "primary"


def test_evaluate_tables_shapes_and_arithmetic():
    rng = np.random.default_rng(1); n_days, spd = 2, 50; N = n_days * spd; cols = mx.sig_columns(windows=[3, 5, 10])      # 3 pairs x 6 = 18 rules
    B = rng.choice([-1, 1], size=(N, len(cols))).astype(np.int8); E = B.copy(); E[::3] *= -1
    r = rng.normal(size=N).astype(np.float32); RC = np.stack([r + 0.1, r - 0.1], axis=1).astype(np.float32); RC[0, 0] = np.nan
    dates = ["2026-09-01", "2026-09-02"]
    out = mx.evaluate(B, E, r, RC, ["SOL", "XRP"], dates, cols, secs_per_day=spd, null_workers=1)
    R = out["rules"]; assert R.height == 18 * 3 * 2 and set(R["combo"]) == {"both", "btc", "eth"} and set(R["lag_s"]) == {0, 1} and {"arm", "typ", "fast", "slow", "sharpe_ann", "flips_per_day", "p_value", "null_p95_bp"} <= set(R.columns)
    row = R.filter((pl.col("rule") == cols[0]) & (pl.col("combo") == "btc") & (pl.col("lag_s") == 0)).row(0, named=True)
    assert row["total_bp"] == pytest.approx(float(B[:, 0].astype(float) @ np.nan_to_num(r.astype(float))), rel=1e-4)
    row1 = R.filter((pl.col("rule") == cols[0]) & (pl.col("combo") == "btc") & (pl.col("lag_s") == 1)).row(0, named=True); assert row1["p_value"] is not None and 0 <= row1["p_value"] <= 1
    assert R.filter(pl.col("lag_s") == 0)["p_value"].null_count() == R.filter(pl.col("lag_s") == 0).height        # the null is computed at the executable lag only
    D = out["daily"]; assert D.height == 18 * 3 * 2 * 2 + 2 and D.filter(pl.col("rule") == "always_long")["pnl_bp"].sum() == pytest.approx(float(np.nansum(r)), rel=1e-4) and D.filter((pl.col("rule") == cols[0]) & (pl.col("combo") == "btc") & (pl.col("lag_s") == 0))["pnl_bp"].sum() == pytest.approx(row["total_bp"], rel=1e-4)
    base = out["baseline"]; assert base.filter(pl.col("name") == "always_long").row(0, named=True)["total_bp"] == pytest.approx(float(np.nansum(r)), rel=1e-4)
    Sp = out["split"]; assert set(Sp["half"]) == {1, 2} and Sp.filter(pl.col("selected")).height == 6 * 2            # one winner per arm x type per half-table (6 arm-types)
    Cn = out["coins"]; assert set(Cn["coin"]) == {"SOL", "XRP"} and Cn.filter(pl.col("rule") == Cn["rule"][0]).height == 2
    L = out["lagprofile"]; assert L.height == 18 * len(mc.LAG_PROFILE_S) and L.filter((pl.col("rule") == cols[0]) & (pl.col("lag_s") == 1))["total_bp"][0] == pytest.approx(R.filter((pl.col("rule") == cols[0]) & (pl.col("combo") == "both") & (pl.col("lag_s") == 1))["total_bp"][0], rel=1e-4)
    A = out["arms"]; assert A.height == 6 * 3 * 2 and {"median_sharpe_ann", "best_sharpe_ann", "best_rule", "share_positive", "share_p05", "median_flips_per_day"} <= set(A.columns)


def test_job_spec_dispatch_word_and_queue_routing():
    j = mx.job_spec("x", ["sig", "--run-id", "r"], 8, 32000, 3)
    assert j["containerOverrides"]["command"][:2] == ["macx", "sig"] and j["jobQueue"] == mx.sb.big_queue() and {e["name"]: e["value"] for e in j["containerOverrides"]["environment"]}["OPENBLAS_NUM_THREADS"] == "8"


def test_df_infers_schema_from_every_row():
    rows = [{"k": i, "p": None} for i in range(150)] + [{"k": 150, "p": 0.2}]                     # the first 100+ rows null, then a float (the smoke-run crash)
    F = mx._df(rows); assert F.schema["p"] == pl.Float64 and F["p"][150] == pytest.approx(0.2) and mx._df([], {"a": []}).columns == ["a"]


def test_evaluate_with_more_than_100_rules_per_lag():
    rng = np.random.default_rng(3); spd, nd = 20, 2; N = spd * nd; cols = mx.sig_columns(windows=[3, 5, 10, 20, 30, 40, 50, 75])      # 28 pairs x 6 = 168 rules > the 100-row inference window
    B = rng.choice([-1, 1], size=(N, len(cols))).astype(np.int8); r = rng.normal(size=N).astype(np.float32)
    out = mx.evaluate(B, B, r, np.stack([r, r], axis=1), ["A", "B"], ["2026-09-01", "2026-09-02"], cols, secs_per_day=spd, null_workers=1)
    assert out["rules"].height == 168 * 3 * 2 and out["rules"].schema["p_value"] == pl.Float64


def test_build_venue_matrix_columns_and_values():
    grid = DAY0 + np.arange(2) * S
    T = pl.DataFrame({"venue": ["ALL", "ALL", "binance_futures", "binance_futures", "bybit", "bybit"], "t_bucket": [DAY0 - S, DAY0] * 3, "mid": [10.0, 10.1, 10.0, 10.2, 10.0, 9.9], "spread_bp": [1.0] * 6, "usd": [1.0] * 6})
    V = mx.build_venue_matrix({"SOL": T, "NONE": None}, grid, ["SOL", "NONE"])
    assert V.columns == ["t", "SOL|binance_futures", "SOL|bybit"] and V["SOL|binance_futures"][0] == pytest.approx(1e4 * np.log(10.2 / 10.0), rel=1e-5) and V["SOL|bybit"][0] == pytest.approx(1e4 * np.log(9.9 / 10.0), rel=1e-5) and V["SOL|bybit"][1] == 0.0


def test_evaluate_hourly_and_hist_tables():
    rng = np.random.default_rng(4); spd, nd = 7200, 2; N = spd * nd; cols = mx.sig_columns(windows=[3, 5])            # 1 pair x 6 = 6 rules, 2-hour days
    B = rng.choice([-1, 1], size=(N, len(cols))).astype(np.int8); r = rng.normal(size=N).astype(np.float32)
    out = mx.evaluate(B, B, r, r[:, None], ["A"], ["2026-09-01", "2026-09-02"], cols, secs_per_day=spd, null_workers=1)
    Hh = out["hourly"]; assert Hh.filter(pl.col("rule") == cols[0]).height == 3 * 2 * 4 and Hh.filter(pl.col("rule") == "always_long").height == 4
    tot = Hh.filter((pl.col("rule") == cols[0]) & (pl.col("combo") == "btc") & (pl.col("lag_s") == 0))["pnl_bp"].sum(); assert tot == pytest.approx(float(B[:, 0].astype(float) @ r.astype(float)), rel=1e-3)
    Hd = out["hod"]; assert set(Hd["lag_s"]) == {1} and {"hod_et", "n_hours", "mean_hour_bp", "share_hours_pos", "n_trades", "hit_rate", "mean_trade_bp", "median_trade_bp"} <= set(Hd.columns)
    x = Hd.filter((pl.col("rule") == cols[0]) & (pl.col("combo") == "btc")); assert x["n_trades"].sum() == out["rules"].filter((pl.col("rule") == cols[0]) & (pl.col("combo") == "btc") & (pl.col("lag_s") == 1))["flips"][0] + 1 and x["n_hours"].sum() == 4
    assert "median_trade_bp" in out["rules"].columns
    Hs = out["hist"]; assert set(Hs["lag_s"]) == {1} and Hs.filter((pl.col("rule") == cols[0]) & (pl.col("combo") == "btc"))["count"].sum() == out["rules"].filter((pl.col("rule") == cols[0]) & (pl.col("combo") == "btc") & (pl.col("lag_s") == 1))["flips"][0] + 1


def test_venue_metrics_and_cube_tables():
    rng = np.random.default_rng(5); spd, nd = 30, 2; N = spd * nd; cols = mx.sig_columns(windows=[3, 5]); dates = ["2026-09-01", "2026-09-02"]
    B = rng.choice([-1, 1], size=(N, len(cols))).astype(np.int8); dirs = {"both": B, "btc": B, "eth": B}
    RV = rng.normal(size=(N, 3)).astype(np.float32); RV[:, 2] = np.nan; series = ["SOL|binance_futures", "XRP|binance_futures", "SOL|bybit"]
    r_v, n_v = mc.basket(RV[:, :2]); Vm = mx.venue_metrics(dirs, r_v, cols, "binance_futures", float(n_v.mean()), dates, secs_per_day=spd)
    assert Vm.height == 6 * 3 and Vm["venue"].unique().to_list() == ["binance_futures"] and {"hit_rate", "sharpe_ann", "total_bp", "n_coins"} <= set(Vm.columns)
    cubes, CI = mx.cube_tables(dirs, RV, series, dates, cols, secs_per_day=spd)
    assert cubes["both"].shape == (6, 3, 2) and CI.height == 3 * 6 * 3
    pos = mc.shift_positions(B, 1)[:, 0].astype(float); manual = float(pos[:spd] @ RV[:spd, 1].astype(float))
    assert cubes["both"][0, 1, 0] == pytest.approx(manual, rel=1e-4) and CI.filter((pl.col("combo") == "both") & (pl.col("rule") == cols[0]) & (pl.col("coin") == "SOL") & (pl.col("venue") == "bybit"))["secs_with_value"][0] == 0
    assert set(mx.pnl_job.__code__.co_names) >= {"ThreadPoolExecutor", "cube_tables", "_eval_mark", "_eval_venue"} and "venue_metrics" in mx._eval_venue.__code__.co_names


def test_build_sig_computes_long_imbalance_windows_itself():
    grid = DAY0 + np.arange(2) * S; n = 12
    F = pl.DataFrame({"venue": ["ALL"] * n, "t_close": [DAY0 - (n - i) * S for i in range(n)], "trade_imb": [1.0] * 6 + [-1.0] * 6, "book_imb": [-1.0] * 6 + [1.0] * 6, "timb_ma3": pl.Series([0.0] * n, dtype=pl.Float32), "bimb_ma3": pl.Series([0.0] * n, dtype=pl.Float32)})
    X = mx.build_sig({"imb_usd": F, "imb_sec": None, "px_usd": None, "px_sec": None}, grid, windows=[3, 10])
    assert X["imb_usd:timb:3:10"].to_list() == [1, 1] and X["imb_usd:bimb:3:10"].to_list() == [-1, -1]        # the frame's ma3 is pinned at 0; the computed ma10 = mean of 4x(+1), 6x(−1) = −0.2 → fast above slow for timb, below for bimb
    assert mx.prev_dates("2026-09-03", 3) == ["2026-08-31", "2026-09-01", "2026-09-02"] and mx.WARMUP_DAYS == 7
