"""ma_cross_viewer: the JSON documents built from the summary tables (daily median, trades per day, average bp per trade; meta; detail routes) — synthetic frames, no S3."""
import polars as pl, pytest
import ma_cross_viewer as mv


def _rules():
    return pl.DataFrame({"rule": ["a", "a", "b"], "arm": ["imb_usd"] * 3, "typ": ["timb"] * 3, "fast": [3, 3, 5], "slow": [5, 5, 10], "combo": ["both", "both", "both"], "lag_s": [0, 1, 1], "total_bp": [100.0, 80.0, -20.0], "daily_mean_bp": [50.0, 40.0, -10.0],
                         "daily_sd_bp": [1.0, 1.0, 1.0], "sharpe_daily": [1.0, 1.0, -1.0], "sharpe_ann": [19.1, 19.1, -19.1], "pos_day_share": [1.0, 1.0, 0.0], "max_drawdown_bp": [0.0, 0.0, 20.0], "flips": [9, 9, 1], "flips_per_day": [4.5, 4.5, 0.5], "median_hold_s": [10.0, 10.0, 50.0],
                         "hit_rate": [0.6, 0.6, 0.5], "long_share": [0.5, 0.5, 0.5], "breakeven_bp_per_flip": [11.1, 8.9, -20.0], "n_days": [2, 2, 2], "n_secs": [100, 100, 100], "p_value": [None, 0.01, 0.4], "null_p95_bp": [None, 5.0, 5.0], "mark": ["primary"] * 3})


def _daily():
    return pl.DataFrame({"rule": ["a", "a", "a", "a", "always_long", "always_long"], "combo": ["both"] * 4 + ["-"] * 2, "lag_s": [0, 0, 1, 1, 0, 0], "date": ["2026-09-01", "2026-09-02"] * 3, "pnl_bp": [60.0, 40.0, 30.0, 50.0, 5.0, -3.0], "mark": ["primary"] * 6})


def test_rules_doc_adds_daily_median_trades_and_avg_per_trade():
    d = mv.rules_doc(_rules(), _daily()); i = {c: k for k, c in enumerate(d["columns"])}
    rows = {(r[i["rule"]], r[i["lag_s"]]): r for r in d["rows"]}
    assert rows[("a", 0)][i["daily_median_bp"]] == 50.0 and rows[("a", 1)][i["daily_median_bp"]] == 40.0 and rows[("b", 1)][i["daily_median_bp"]] is None
    assert rows[("a", 0)][i["trades_per_day"]] == 5.0 and rows[("a", 0)][i["avg_bp_per_trade"]] == pytest.approx(10.0) and rows[("b", 1)][i["avg_bp_per_trade"]] == pytest.approx(-10.0)
    assert "n_secs" not in d["columns"] and rows[("a", 0)][i["p_value"]] is None and mv.rules_doc(None, None) == {"columns": [], "rows": []}


def test_meta_daily_lag_and_coins_docs():
    plan = {"run_id": "r", "dates": ["2026-09-01", "2026-09-02"], "targets": [{}] * 3, "windows": [3, 5, 10], "arms": {"imb_usd": ["timb"]}, "combos": ["both"], "lags_s": [0, 1]}
    B = pl.DataFrame({"name": ["always_long", "always_short"], "mark": ["primary"] * 2, "total_bp": [2.0, -2.0], "sharpe_ann": [1.0, -1.0], "half_spread_bp": [1.8, 1.8]})
    m = mv.meta_doc(plan, B, {"n_secs": 100, "marks": {"primary": {"half_spread_bp": 1.8}}})
    assert m["marks"] == ["primary"] and m["coins"] == 3 and m["half_spread_bp"] == {"primary": 1.8} and m["done"] == {"n_secs": 100} and [x["key"] for x in m["metrics"]][:2] == ["sharpe_ann", "daily_mean_bp"] and "hit_rate" in [x["key"] for x in m["metrics"]]
    d = mv.daily_doc(_daily(), "a", "both", "1", "primary"); assert d == {"dates": ["2026-09-01", "2026-09-02"], "pnl_bp": [30.0, 50.0], "always_long_bp": [5.0, -3.0]}
    L = pl.DataFrame({"rule": ["a", "a"], "mark": ["primary"] * 2, "lag_s": [1, 0], "total_bp": [80.0, 100.0]}); assert mv.lagprofile_doc(L, "a", "primary") == {"lag_s": [0, 1], "total_bp": [100.0, 80.0]}
    C = pl.DataFrame({"rule": ["a", "a"], "mark": ["primary"] * 2, "coin": ["SOL", "XRP"], "contrib_bp": [1.0, 3.0], "always_long_bp": [0.5, 0.2], "secs_with_value": [10, 20]})
    assert [r["coin"] for r in mv.coins_doc(C, "a", "primary")["rows"]] == ["XRP", "SOL"] and mv.coins_doc(C, "zzz", "primary") == {"rows": []}


def test_page_has_controls_and_run_placeholder():
    assert "__RUN__" in mv.PAGE and 'id="metric"' in mv.PAGE and 'id="pivot"' in mv.PAGE and "hit ratio" in mv.PAGE.lower() and mv.DIMS == ["fast", "slow", "arm", "typ", "combo", "lag_s", "mark"]


def test_cell_doc_neighbours_and_detail():
    R = _rules().vstack(_rules().with_columns(pl.lit("btc").alias("combo"))); D = _daily()
    H = pl.DataFrame({"rule": ["a", "a", "always_long", "always_long"], "combo": ["both", "both", "-", "-"], "lag_s": [1, 1, 0, 0], "hour": [0, 1, 0, 1], "pnl_bp": [1.0, 2.0, 0.5, -0.5], "mark": ["primary"] * 4})
    Hs = pl.DataFrame({"rule": ["a", "a"], "combo": ["both"] * 2, "lag_s": [1, 1], "bin_lo": [None, 0.0], "bin_hi": [0.0, None], "count": [3, 7], "mark": ["primary"] * 2})
    V = pl.DataFrame({"venue": ["bybit"], "n_coins": [12.0], "rule": ["a"], "combo": ["both"], "lag_s": [1], "sharpe_ann": [3.0], "total_bp": [10.0], "hit_rate": [0.5]})
    CI = pl.DataFrame({"combo": ["both", "both", "btc"], "rule": ["a", "a", "a"], "coin": ["SOL", "SOL", "SOL"], "venue": ["bybit", "okx", "bybit"], "total_bp": [1.0, 2.0, 3.0], "sharpe_ann": [1.0, None, 2.0], "days_up": [0.5, 0.5, 1.0], "secs_with_value": [10, 20, 10]})
    B = pl.DataFrame({"name": ["always_long"], "mark": ["primary"], "total_bp": [2.0], "sharpe_ann": [1.0], "half_spread_bp": [1.8]})
    Hd = pl.DataFrame({"rule": ["a", "a"], "combo": ["both"] * 2, "lag_s": [1, 1], "hod_et": [20, 9], "n_hours": [2, 2], "mean_hour_bp": [1.5, -0.5], "share_hours_pos": [1.0, 0.0], "n_trades": [3, 1], "hit_rate": [0.67, 0.0], "trades_bp": [2.0, -1.0], "mean_trade_bp": [0.67, -1.0], "median_trade_bp": [0.5, -1.0], "mark": ["primary"] * 2})
    T = {"rules": R, "daily": D, "hourly": H, "hist": Hs, "lagprofile": None, "venues": V, "cube_index": CI, "baseline": B, "hod": Hd}
    c = mv.cell_doc(T, None, "primary", "both", "1", "a")
    assert [r["hod_et"] for r in c["hod"]] == [9, 20] and c["hod"][1]["median_trade_bp"] == 0.5 and 'id="hod"' in mv.STUDY and "median_trade_bp" in [m["key"] for m in mv.meta_doc({"run_id": "r", "dates": [], "targets": []}, None, {})["metrics"]]
    assert c["cell"]["rule"] == "a" and c["cell"]["daily_median_bp"] == 40.0 and c["hourly"]["pnl_bp"] == [1.0, 2.0] and c["hourly"]["always_long_bp"] == [0.5, -0.5]
    assert c["hist"] == [{"lo": None, "hi": 0.0, "count": 3}, {"lo": 0.0, "hi": None, "count": 7}] and c["lagprofile"] == {"lag_s": [], "total_bp": []}
    assert [r["rule"] for r in c["pairs"]] == ["a", "b"] and len(c["arms"]) == 1 and {(r["combo"], r["lag_s"]) for r in c["signals"]} == {("both", 0), ("both", 1), ("btc", 0), ("btc", 1)} and [r["mark"] for r in c["targets"]] == ["primary"]
    assert c["venues"][0]["venue"] == "bybit" and [r["venue"] for r in c["coins"]] == ["bybit", "okx"] and c["coins"][1]["sharpe_ann"] is None and c["baseline"]["half_spread_bp"] == 1.8
    assert "error" in mv.cell_doc(T, None, "primary", "both", "1", "zzz") and "__RUN__" in mv.STUDY and 'id="grid"' in mv.STUDY and 'id="coins"' in mv.STUDY
