"""ma_cross_core: rules, price MAs, crossover states (no look-ahead, ties hold), grid sampling, coin / basket returns, BTC-ETH agreement with hold, lags,
metrics on a hand-built series, the random-flip null — synthetic frames, no S3."""
import numpy as np, polars as pl, pytest
import ma_cross_core as mc

DAY0 = 1_789_430_400_000_000     # 2026-09-15 00:00 UTC µs
S = 1_000_000


def test_rule_columns_count_and_order():
    cols = mc.rule_columns(); n = len(mc.PAIRS)
    assert len(mc.WINDOWS) == 19 and mc.WINDOWS[-5:] == [25_000, 50_000, 100_000, 250_000, 500_000] and n == 171 and len(cols) == 6 * n
    assert cols[0] == "imb_usd:timb:3:5" and cols[n] == "imb_usd:bimb:3:5" and cols[-1] == "px_sec:px:250000:500000"
    assert all(int(c.split(":")[2]) < int(c.split(":")[3]) for c in cols) and len(set(cols)) == 6 * n


def test_imb_mas_adds_only_missing_windows_with_warmup():
    F = pl.DataFrame({"t_close": [DAY0 + i * S for i in range(5)], "trade_imb": [1.0, 2.0, 3.0, 4.0, 5.0], "book_imb": [-1.0, -2.0, -3.0, -4.0, -5.0], "timb_ma3": pl.Series([9.0] * 5, dtype=pl.Float32), "bimb_ma3": pl.Series([9.0] * 5, dtype=pl.Float32)})
    M = mc.imb_mas(F, windows=[3, 4])
    assert M["timb_ma3"].to_list() == [9.0] * 5 and M["timb_ma4"].to_list() == [None, None, None, 2.5, 3.5] and M["bimb_ma4"].to_list() == [None, None, None, -2.5, -3.5] and M.schema["timb_ma4"] == pl.Float32
    assert mc.imb_mas(F, windows=[3]).columns == F.columns
    G = F.with_columns(pl.Series("book_imb", [-1.0, None, -3.0, -4.0, -5.0])); assert mc.imb_mas(G, windows=[4])["bimb_ma4"].to_list() == [None, None, None, -2.0, -3.0]   # a null counts as 0, it does not void the window


def test_price_mas_rolling_with_warmup():
    prev = pl.DataFrame({"t_close": [DAY0 - 3 * S, DAY0 - 2 * S], "vwap": [1.0, 2.0], "close": [1.0, 2.0]})
    B = pl.DataFrame({"t_close": [DAY0, DAY0 + S], "vwap": [3.0, None], "close": [3.0, 4.0]})
    P = mc.price_mas(B, prev, windows=[3])
    assert P.columns == ["t_close", "px_ma3"] and P.height == 2 and P["px_ma3"].to_list() == pytest.approx([2.0, 3.0])        # null vwap falls back to close; warm-up rows dropped
    assert mc.price_mas(B, None, windows=[3])["px_ma3"].to_list() == [None, None]


def test_states_sign_and_tie_hold():
    M = pl.DataFrame({"px_ma3": [None, 1.0, 2.0, 2.0, 1.0, 3.0], "px_ma5": [None, 2.0, 1.0, 2.0, 1.0, None]})
    St = mc.states(M, "px", windows=[3, 5])
    assert St.dtype == np.int8 and St.shape == (6, 1) and St[:, 0].tolist() == [0, -1, 1, 1, 1, 1]                          # null → 0 before the first state; ties and nulls hold the previous


def test_ffill_nonzero_with_init():
    A = np.array([[0, 0], [0, 1], [0, 0], [-1, 0]], dtype=np.int8)
    assert mc.ffill_nonzero(A.copy()).tolist() == [[0, 0], [0, 1], [0, 1], [-1, 1]]
    assert mc.ffill_nonzero(A.copy(), init=np.array([1, -1], dtype=np.int8)).tolist() == [[1, -1], [1, 1], [1, 1], [-1, 1]]


def test_sample_states_asof_never_looks_ahead():
    grid = np.array([DAY0, DAY0 + S, DAY0 + 2 * S, DAY0 + 3 * S]); tc = np.array([DAY0 - 1, DAY0 + S, DAY0 + S + 1, DAY0 + 3 * S + 1])
    St = np.array([[1], [-1], [1], [-1]], dtype=np.int8)
    assert mc.sample_states(tc, St, grid)[:, 0].tolist() == [1, -1, 1, 1]            # t=1 s: the bar closing exactly at 1 s counts; t=2 s: the 1.000001 s bar; the 3.000001 s bar never
    assert mc.sample_states(tc[1:], St[1:], grid)[:, 0].tolist() == [0, -1, 1, 1]    # nothing closed yet → 0


def test_coin_returns_use_prior_bar_and_forward_fill():
    grid = DAY0 + np.arange(5) * S                                             # interval starts τ_0..τ_4
    T = pl.DataFrame({"venue": ["ALL"] * 3, "t_bucket": [DAY0 - S, DAY0, DAY0 + 2 * S], "mid": [100.0, 101.0, 102.0]})
    r = mc.coin_returns(T, grid)
    # r_j = ln(mid(τ_{j+1}) / mid(τ_j)); mid(τ_{j+1}) = bar with bucket ≤ τ_j; mid(τ_j) = bar with bucket ≤ τ_j − 1 s
    assert r[0] == pytest.approx(1e4 * np.log(101 / 100)) and r[1] == 0.0 and r[2] == pytest.approx(1e4 * np.log(102 / 101)) and r[3] == 0.0 and r[4] == 0.0
    assert np.isnan(mc.coin_returns(T.filter(pl.col("t_bucket") >= DAY0), grid)[0])  # no mid before the day → the first interval is null


def test_coin_spread_asof_prior_bar():
    grid = DAY0 + np.arange(3) * S; T = pl.DataFrame({"venue": ["ALL"] * 2, "t_bucket": [DAY0 - S, DAY0 + S], "spread_bp": [3.0, 5.0]})
    assert mc.coin_spread(T, grid).tolist() == pytest.approx([3.0, 3.0, 5.0]) and np.isnan(mc.coin_spread(T.filter(pl.col("t_bucket") > DAY0), grid)[0])


def test_coin_returns_fresh_requires_a_bar_at_both_ends():
    grid = DAY0 + np.arange(4) * S; T = pl.DataFrame({"venue": ["ALL"] * 3, "t_bucket": [DAY0 - S, DAY0, DAY0 + 2 * S], "mid": [100.0, 101.0, 102.0]})
    r = mc.coin_returns(T, grid, fresh=True)
    assert r[0] == pytest.approx(1e4 * np.log(101 / 100)) and np.isnan(r[1]) and np.isnan(r[2]) and np.isnan(r[3])   # τ_1: no bar at 1 s; τ_2: bar at 2 s but none at 1 s; τ_3: none at 3 s
    assert mc.LAG_PROFILE_S[:3] == [0, 1, 2] and 300 in mc.LAG_PROFILE_S


def test_basket_mean_over_available_coins():
    R = np.array([[1.0, 3.0, np.nan], [np.nan, np.nan, np.nan]], dtype=np.float32)
    r, n = mc.basket(R)
    assert r[0] == pytest.approx(2.0) and n.tolist() == [2, 0] and np.isnan(r[1])


def test_combine_holds_on_disagreement_and_carries():
    B = np.array([[0], [1], [1], [-1], [-1], [1]], dtype=np.int8); E = np.array([[0], [1], [-1], [-1], [1], [1]], dtype=np.int8)
    assert mc.combine(B, E)[:, 0].tolist() == [0, 1, 1, -1, -1, 1]
    assert mc.combine(B, E, init=np.array([-1], dtype=np.int8))[:, 0].tolist() == [-1, 1, 1, -1, -1, 1]


def test_shift_positions_lag():
    D = np.array([[1], [-1], [1]], dtype=np.int8)
    assert mc.shift_positions(D, 0)[:, 0].tolist() == [1, -1, 1] and mc.shift_positions(D, 1)[:, 0].tolist() == [0, 1, -1]
    assert mc.shift_positions(D, 1, init=np.array([-1], dtype=np.int8))[:, 0].tolist() == [-1, 1, -1] and mc.shift_positions(D, 7)[:, 0].tolist() == [0, 0, 0]


def test_metrics_on_a_hand_series():
    pos = np.array([1, 1, -1, -1, -1, 1, 1, 1], dtype=np.int8); r = np.array([1.0, 2.0, -3.0, 1.0, np.nan, 2.0, -1.0, 0.5])
    day_starts = np.array([0, 4])                                                   # two days of 4 s
    m = mc.metrics(pos, r, day_starts)
    assert m["total_bp"] == pytest.approx(1 + 2 + 3 - 1 + 0 + 2 - 1 + 0.5) and m["flips"] == 2 and m["flips_per_day"] == 1.0
    assert m["daily_mean_bp"] == pytest.approx(6.5 / 2) and m["pos_day_share"] == 1.0 and m["long_share"] == pytest.approx(5 / 8)
    assert m["median_hold_s"] == 3.0 and m["hit_rate"] == 1.0 and m["max_drawdown_bp"] == pytest.approx(1.0)     # segments +3, +2, +1.5 all win; cum 6 → 5 is the deepest dip
    assert m["breakeven_bp_per_flip"] == pytest.approx(6.5 / 2) and m["mean_trade_bp"] == pytest.approx(6.5 / 3) and m["median_trade_bp"] == pytest.approx(2.0)   # segments +3, +2, +1.5


def test_metrics_hit_rate_counts_losing_segments():
    pos = np.array([1, -1], dtype=np.int8); r = np.array([-1.0, -1.0]); m = mc.metrics(pos, r, np.array([0]))
    assert m["hit_rate"] == 0.5 and m["flips"] == 1 and m["total_bp"] == 0.0 and m["sharpe_daily"] is None


def test_shift_null_matches_brute_force_and_p_value():
    rng = np.random.default_rng(0); pos = rng.choice([-1, 1], size=50).astype(np.int8); pos[:3] = 0; r = rng.normal(size=50); r[7] = np.nan
    tot = mc.shift_null_totals(pos, r); r0 = np.nan_to_num(r)
    brute = [float(np.dot(np.roll(pos, -o).astype(float), r0)) for o in range(50)]
    assert tot.shape == (50,) and tot.tolist() == pytest.approx(brute, abs=1e-6) and tot[0] == pytest.approx(float(pos.astype(float) @ r0))
    assert mc.p_value(np.array([1.0, -2.0, 3.0, -0.5]), 2.5) == pytest.approx(0.25) and mc.p_value(np.array([]), 1.0) is None


def test_day_index_from_dates():
    assert mc.day_starts(3, 4).tolist() == [0, 4, 8]


def test_segment_pnls_hist_and_period_sums():
    pos = np.array([0, 1, 1, -1, -1, -1, 1], dtype=np.int8); r = np.array([9.0, 1.0, 2.0, -3.0, 1.0, np.nan, 0.3])
    seg = mc.segment_pnls(pos, r); assert seg.tolist() == pytest.approx([3.0, 2.0, 0.3])                  # leading zeros are not a segment; short leg sign-flipped
    H = mc.trade_hist(seg, edges=[0, 1, 2.5]); assert H == [(None, 0, 0), (0, 1, 1), (1, 2.5, 1), (2.5, None, 1)]
    assert len(mc.trade_hist(seg)) == len(mc.HIST_EDGES_BP) + 1 and sum(c for _, _, c in mc.trade_hist(seg)) == 3
    assert mc.period_sums(pos, r, [0, 3]).tolist() == pytest.approx([3.0, 2.3]) and mc.segment_pnls(np.zeros(3, dtype=np.int8), np.ones(3)).size == 0


def test_hour_of_day_us_eastern_and_hod_stats():
    dates = ["2026-09-15", "2026-09-16"]; off = mc.et_offsets_h(dates); assert off.tolist() == [-4.0, -4.0]                        # daylight time in September
    assert mc.et_offsets_h(["2026-12-01"]).tolist() == [-5.0]
    idx = np.array([0, 3 * 3600, 4 * 3600, 86400 + 23 * 3600 + 59 * 60]); assert mc.hour_of_day(idx, dates).tolist() == [20, 23, 0, 19]   # 00:00 UTC = 20:00 ET the evening before
    sp, st = mc.segment_pnls(np.array([0, 1, 1, -1, -1, -1, 1], dtype=np.int8), np.array([9.0, 1.0, 2.0, -3.0, 1.0, np.nan, 0.3]), with_starts=True)
    assert st.tolist() == [1, 3, 6] and sp.tolist() == pytest.approx([3.0, 2.0, 0.3])
    H = mc.hod_trade_stats(np.array([1.0, -1.0, 4.0]), np.array([3, 3, 7])); assert H[3] == {"n_trades": 2, "hit_rate": 0.5, "trades_bp": 0.0, "mean_trade_bp": 0.0, "median_trade_bp": 0.0} and H[7]["median_trade_bp"] == 4.0
