"""MA crossover basket study — pure functions (numpy / polars, no S3): the 546 rules (4 arms x imbalance type x 91 fast<slow window pairs), price MAs on
bars, crossover states (sign of fast − slow, ties and nulls hold the previous state), as-of sampling on the 1 s grid (no look-ahead), coin / basket 1 s mid
returns, the BTC-ETH agreement rule with hold, execution lags, per-rule metrics and the circular-shift null. Spec: docs/superpowers/specs/2026-10-12-ma-crossover-basket-design.md."""
import numpy as np, polars as pl
import common as tf

S = 1_000_000                                                     # µs per second
LONG_WINDOWS = [25_000, 50_000, 100_000, 250_000, 500_000]       # user 2026-10-12: beyond the dataset's 14 windows; computed in the sig stage (not in the mas files)
WINDOWS = list(tf.MA_WINDOWS) + LONG_WINDOWS                      # 3 .. 500000 bars (19)
PAIRS = [(f, s) for i, f in enumerate(WINDOWS) for s in WINDOWS[i + 1:]]          # 91 fast < slow
ARMS = {"imb_usd": ("timb", "bimb"), "imb_sec": ("timb", "bimb"), "px_usd": ("px",), "px_sec": ("px",)}   # arm -> MA types; usd = $100k dollar bars, sec = 1 s time bars
COMBOS = ["both", "btc", "eth"]                                   # both = agreement with hold; btc / eth = that coin alone
LAGS_S = [0, 1]
LAG_PROFILE_S = [0, 1, 2, 3, 5, 10, 30, 60, 300]                   # execution lags of the cheap total-PnL profile (how fast the edge decays)


def rule_columns():
    """Rule ids in matrix column order: {arm}:{type}:{fast}:{slow}."""
    return [f"{arm}:{typ}:{f}:{s}" for arm, typs in ARMS.items() for typ in typs for f, s in PAIRS]


def pairs_of(windows):
    return [(f, s) for i, f in enumerate(windows) for s in windows[i + 1:]]


# ----------------------------------------------------------------------------------------------- signal side
def price_mas(B, prev=None, windows=None):
    """Simple moving averages of the bar price (vwap, close where vwap is null) over the previous N bars for every N in windows, warmed up with the previous
    day's frame (its rows are dropped): (t_close, px_ma{N} Float32) for B's rows. Null until N bars exist — mirrors trade_flow_core.moving_averages."""
    windows = windows or WINDOWS; px = pl.coalesce(pl.col("vwap"), pl.col("close")).cast(pl.Float64).alias("px")
    X = B.select("t_close", px)
    if prev is not None and prev.height: X = pl.concat([prev.select("t_close", px), X], how="vertical")
    X = X.with_columns([pl.col("px").rolling_mean(window_size=n, min_samples=n).cast(pl.Float32).alias(f"px_ma{n}") for n in windows]).drop("px")
    return X.tail(B.height)


def imb_mas(F, windows=None):
    """Add the imbalance moving averages that the frame lacks: timb_ma{N} / bimb_ma{N} = simple moving averages of trade_imb / book_imb over the previous N bars
    (min_samples = N, null until N bars exist), for every N in windows without a column. F = one series' bars (previous days + today) sorted by t_close."""
    windows = windows or WINDOWS; missing = [n for n in windows if f"timb_ma{n}" not in F.columns or f"bimb_ma{n}" not in F.columns]
    if not missing or not F.height: return F
    # a null imbalance (a bar without a usable book) is treated as neutral (0): with min_samples = N one null would otherwise void the next N windows — on
    # the 500,000-bar window that left every such pair undecided for days (full run, 2026-10-12)
    exprs = [pl.col(c).cast(pl.Float64).fill_null(0.0).fill_nan(0.0).rolling_mean(window_size=n, min_samples=n).cast(pl.Float32).alias(f"{short}_ma{n}") for n in missing for c, short in (("trade_imb", "timb"), ("book_imb", "bimb"))]
    return F.with_columns(exprs)


def ffill_nonzero(A, init=None):
    """Forward-fill zeros along axis 0 with the last non-zero value of the column; rows before the first non-zero take init (per column; 0 when None)."""
    n = A.shape[0]; idx = np.where(A != 0, np.arange(n)[:, None], -1); idx = np.maximum.accumulate(idx, axis=0)
    out = np.take_along_axis(A, np.maximum(idx, 0), axis=0)
    init = np.zeros(A.shape[1], dtype=A.dtype) if init is None else np.asarray(init, dtype=A.dtype)
    return np.where(idx < 0, init[None, :], out).astype(A.dtype)


def states(M, typ, windows=None):
    """int8 (bars x pairs): sign(fast MA − slow MA) per bar for every fast < slow pair of the {typ}_ma{N} columns (typ = timb | bimb | px); 0 where either MA is
    null or the two are equal, then forward-filled so a tie or a null holds the previous state (0 only before the first decided state)."""
    windows = windows or WINDOWS; pairs = pairs_of(windows); cols = {n: M[f"{typ}_ma{n}"].cast(pl.Float64).to_numpy() for n in windows}
    out = np.zeros((M.height, len(pairs)), dtype=np.int8)
    for k, (f, s) in enumerate(pairs):
        d = cols[f] - cols[s]
        with np.errstate(invalid="ignore"): out[:, k] = np.where(np.isnan(d), 0, np.sign(d)).astype(np.int8)
    return ffill_nonzero(out)


def sample_states(t_close, St, grid):
    """State as of each grid time: the row of the last bar with t_close <= t (0 before the first bar). t_close sorted ascending."""
    idx = np.searchsorted(np.asarray(t_close, dtype=np.int64), np.asarray(grid, dtype=np.int64), side="right") - 1
    out = St[np.maximum(idx, 0)].copy(); out[idx < 0] = 0; return out


def combine(B, E, init=None):
    """Basket direction from the BTC and ETH state matrices (same shape, int8): the common sign where they agree (and are decided), else the previous
    direction (init per column before the first agreement; 0 = not yet invested)."""
    agree = (B == E) & (B != 0); return ffill_nonzero(np.where(agree, B, 0).astype(np.int8), init)


def shift_positions(D, lag, init=None):
    """Position over interval j = direction at j − lag (whole seconds); the first lag rows take init (0 when None)."""
    n = D.shape[0]; out = np.empty_like(D); lag = min(int(lag), n)
    if lag: out[:lag] = (np.zeros(D.shape[1], dtype=D.dtype) if init is None else np.asarray(init, dtype=D.dtype))[None, :]
    if lag < n: out[lag:] = D[:n - lag]
    return out


# ----------------------------------------------------------------------------------------------- basket side
def coin_returns(T, grid, venue="ALL", fresh=False):
    """1 s log mid returns in bp over the intervals (τ_j, τ_j + 1 s] for the grid of interval starts τ_j: mid(τ) = the `mid` (book mid at bar close) of the
    1 s bar whose bucket is <= τ − 1 s, i.e. the bar closing at or before τ, forward-filled; r_j = 1e4·ln(mid(τ_j + 1 s) / mid(τ_j)). NaN where either mid is
    unknown (no bar yet). fresh=True: NaN unless BOTH seconds have their own bar (buckets exactly τ_j − 1 s and τ_j) — a forward-filled mid of a thinly traded
    coin is stale and "catches up" to BTC seconds later, which fakes a lead-lag (full run macx4w, 2026-10-12). Float32."""
    X = (T.filter(pl.col("venue") == venue) if "venue" in T.columns else T).filter(pl.col("mid").is_not_null()).sort("t_bucket")
    tb = X["t_bucket"].to_numpy().astype(np.int64); mid = X["mid"].to_numpy().astype(np.float64); g = np.asarray(grid, dtype=np.int64)
    if not mid.size: return np.full(len(g), np.nan, dtype=np.float32)
    def asof(times):
        idx = np.searchsorted(tb, times, side="right") - 1; ok = idx >= 0
        if fresh: ok &= tb[np.maximum(idx, 0)] == times
        return np.where(ok, mid[np.maximum(idx, 0)], np.nan)
    with np.errstate(invalid="ignore", divide="ignore"): r = 1e4 * (np.log(asof(g)) - np.log(asof(g - S)))
    return r.astype(np.float32)


def coin_spread(T, grid, venue="ALL"):
    """Quoted spread (bp) as of each grid time from the 1 s bars (spread_bp at bar close, the bar closing at or before τ_j, forward-filled); NaN before the first bar."""
    X = (T.filter(pl.col("venue") == venue) if "venue" in T.columns else T).filter(pl.col("spread_bp").is_not_null()).sort("t_bucket")
    tb = X["t_bucket"].to_numpy().astype(np.int64); sp = X["spread_bp"].to_numpy().astype(np.float64); g = np.asarray(grid, dtype=np.int64)
    if not sp.size: return np.full(len(g), np.nan, dtype=np.float32)
    idx = np.searchsorted(tb, g - S, side="right") - 1; return np.where(idx >= 0, sp[np.maximum(idx, 0)], np.nan).astype(np.float32)


def basket(R):
    """Equal-weight basket return per interval (mean over coins with a value) and the coin count; NaN where no coin has a value."""
    ok = ~np.isnan(R); n = ok.sum(axis=1)
    with np.errstate(invalid="ignore", divide="ignore"): r = np.where(n > 0, np.nansum(np.where(ok, R, 0.0), axis=1) / np.maximum(n, 1), np.nan)
    return r.astype(np.float32), n.astype(np.int32)


def day_starts(n_days, secs_per_day=86_400):
    return np.arange(n_days, dtype=np.int64) * int(secs_per_day)


# ----------------------------------------------------------------------------------------------- evaluation
def metrics(pos, r, day_starts_idx):
    """Gross metrics of one position series (int8, −1 / 0 / +1 per interval) against the basket return (bp per interval, NaN = 0): total, daily mean / sd /
    Sharpe (and × √365), positive-day share, max drawdown of the cumulative sum, flips (changes between two non-zero positions), flips per day, median holding
    time in intervals (seconds), hit rate per holding segment, long share of time, break-even cost per flip."""
    r0 = np.nan_to_num(np.asarray(r, dtype=np.float64), nan=0.0); pnl = pos.astype(np.float64) * r0; n = pos.shape[0]
    total = float(pnl.sum()); daily = np.add.reduceat(pnl, np.asarray(day_starts_idx, dtype=np.int64)); nd = len(daily)
    mean = float(daily.mean()); sd = float(daily.std(ddof=1)) if nd > 1 else None; sharpe = (mean / sd) if sd else None
    cum = np.concatenate([[0.0], np.cumsum(pnl)]); dd = float((np.maximum.accumulate(cum) - cum).max())
    change = np.flatnonzero(pos[1:] != pos[:-1]) + 1; flips = int(np.sum((pos[change] != 0) & (pos[change - 1] != 0)))
    starts = np.concatenate([[0], change]); ends = np.concatenate([change, [n]]); keep = pos[starts] != 0
    seg_pnl = np.add.reduceat(pnl, starts) if n else np.array([]); lengths = (ends - starts)[keep]
    sp = seg_pnl[keep] if n else np.array([])
    return {"total_bp": total, "daily_mean_bp": mean, "daily_sd_bp": sd, "sharpe_daily": sharpe, "sharpe_ann": (sharpe * np.sqrt(365.0)) if sharpe is not None else None,
            "pos_day_share": float((daily > 0).mean()), "max_drawdown_bp": dd, "flips": flips, "flips_per_day": flips / nd, "median_hold_s": float(np.median(lengths)) if lengths.size else None,
            "hit_rate": float((sp > 0).mean()) if sp.size else None, "mean_trade_bp": float(sp.mean()) if sp.size else None, "median_trade_bp": float(np.median(sp)) if sp.size else None,
            "long_share": float((pos > 0).mean()), "breakeven_bp_per_flip": (total / flips) if flips else None, "n_days": nd, "n_secs": int(n)}


def segment_pnls(pos, r, with_starts=False):
    """PnL (bp) of every holding segment (one non-zero position from flip to flip) of a position series against the returns, in time order;
    with_starts=True also returns the start index (second) of each segment."""
    r0 = np.nan_to_num(np.asarray(r, dtype=np.float64), nan=0.0); pnl = pos.astype(np.float64) * r0; n = pos.shape[0]
    if not n: return (np.array([]), np.array([], dtype=np.int64)) if with_starts else np.array([])
    change = np.flatnonzero(pos[1:] != pos[:-1]) + 1; starts = np.concatenate([[0], change]); keep = pos[starts] != 0
    sp = np.add.reduceat(pnl, starts)[keep]; return (sp, starts[keep]) if with_starts else sp


def et_offsets_h(dates, tz="America/New_York"):
    """UTC offset in hours of the local zone at the start of each UTC date (−4 in US daylight time, −5 in standard time)."""
    import datetime as dt
    from zoneinfo import ZoneInfo
    z = ZoneInfo(tz); out = []
    for d in dates:
        t = dt.datetime.fromisoformat(d).replace(tzinfo=dt.timezone.utc); out.append(t.astimezone(z).utcoffset().total_seconds() / 3600)
    return np.asarray(out, dtype=np.float64)


def hour_of_day(idx, dates, secs_per_day=86_400, offsets_h=None):
    """Local (US Eastern by default) hour of day, 0..23, of each second index of the concatenated days."""
    idx = np.asarray(idx, dtype=np.int64); spd = int(secs_per_day); day = np.minimum(idx // spd, len(dates) - 1); off = (offsets_h if offsets_h is not None else et_offsets_h(dates))[day]
    return (np.floor((idx % spd) / spd * 24 + off) % 24).astype(np.int64)


def hod_trade_stats(seg_pnl, seg_hod):
    """Per local hour of day: trades, hit rate, total, mean and median PnL per trade — {hod: dict} for the hours that have trades."""
    out = {}
    for h in np.unique(seg_hod):
        x = seg_pnl[seg_hod == h]; out[int(h)] = {"n_trades": int(x.size), "hit_rate": float((x > 0).mean()), "trades_bp": float(x.sum()), "mean_trade_bp": float(x.mean()), "median_trade_bp": float(np.median(x))}
    return out


HIST_EDGES_BP = [-50, -20, -10, -5, -2, -1, -0.5, -0.2, -0.1, 0, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50]     # per-trade PnL bins (bp); open-ended on both sides


def trade_hist(seg_pnl, edges=None):
    """Counts of per-trade PnL per bin: [(bin lower edge or None, bin upper edge or None, count)] over (-inf, e0), [e0, e1), ..., [e_last, inf)."""
    edges = list(edges or HIST_EDGES_BP); x = np.asarray(seg_pnl, dtype=np.float64); idx = np.searchsorted(edges, x, side="right")
    counts = np.bincount(idx, minlength=len(edges) + 1); lo = [None] + edges; hi = edges + [None]
    return [(lo[i], hi[i], int(counts[i])) for i in range(len(edges) + 1)]


def period_sums(pos, r, period_starts):
    """PnL summed over consecutive periods (e.g. hours) given the start index of each period (sorted, first = 0)."""
    r0 = np.nan_to_num(np.asarray(r, dtype=np.float64), nan=0.0); return np.add.reduceat(pos.astype(np.float64) * r0, np.asarray(period_starts, dtype=np.int64))


def shift_null_totals(pos, r, workers=1):
    """Total PnL of every circular shift of the position series against the returns: tot[o] = Σ_t pos[(t + o) mod N]·r[t] (tot[0] = the actual total), via FFT
    cross-correlation. Shifting keeps the flip structure of the rule exactly and breaks only its alignment with the returns — the null for the p-value."""
    from scipy import fft
    p = pos.astype(np.float64); x = np.nan_to_num(np.asarray(r, dtype=np.float64), nan=0.0); n = len(p)
    return fft.irfft(np.conj(fft.rfft(x, workers=workers)) * fft.rfft(p, workers=workers), n=n, workers=workers)


def p_value(null, actual):
    """Two-sided: share of null totals at least as large in magnitude as the actual one."""
    null = np.asarray(null, dtype=np.float64); return float(np.mean(np.abs(null) >= abs(actual))) if null.size else None
