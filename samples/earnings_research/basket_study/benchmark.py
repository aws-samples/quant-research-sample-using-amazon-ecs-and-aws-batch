"""SPY benchmark series for the basket study equity curves.

DATA CONTRACT (data grabbed in a separate session — this module only reads):
    s3://<data_bucket>/market-data/spy/spy_1min_2y.parquet
      1-minute SPY bars, ~2 years. Expected columns: a timestamp column
      (timestamp/ts/datetime/t; tz-aware or naive-UTC) plus open and close.
      Daily open/close are derived here from the RTH window (9:30-16:00 ET):
      open = first bar's open at/after 9:30, close = last bar's close
      before 16:00.

Exposed series (both simple daily returns, aligned to the study's trade days):
    buy_hold   close-to-close — SPY buy-and-hold, the natural equity benchmark
    open_close 9:30 open -> close same day — matches the strategy's holding
               window (in the market only intraday)

Missing data is not an error: loaders return None and the charts simply skip
the overlay, so aggregation never breaks while SPY isn't loaded yet.
"""
from typing import Optional

import pandas as pd

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
def spy_minute_uri() -> str:
    return f"s3://{_bucket()}/market-data/spy/spy_1min_2y.parquet"

_TS_CANDIDATES = ("timestamp", "ts", "datetime", "time", "t", "ts_event")


def _daily_from_minutes(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse 1-min bars to per-day RTH open/close (index: 'YYYY-MM-DD')."""
    ts_col = next((c for c in _TS_CANDIDATES if c in df.columns), None)
    if ts_col is None:
        raise ValueError(f"no timestamp column among {list(df.columns)}")
    ts = pd.to_datetime(df[ts_col], utc=True)
    et = ts.dt.tz_convert("America/New_York")
    out = pd.DataFrame({
        "et": et,
        "date": et.dt.strftime("%Y-%m-%d"),
        "open": df["open"].astype(float),
        "close": df["close"].astype(float),
    })
    # RTH only: 9:30 <= t < 16:00 ET
    mins = out["et"].dt.hour * 60 + out["et"].dt.minute
    out = out[(mins >= 9 * 60 + 30) & (mins < 16 * 60)]
    out = out.sort_values("et")
    daily = out.groupby("date").agg(open=("open", "first"), close=("close", "last"))
    return daily


def load_spy_daily(s3io) -> Optional[pd.DataFrame]:
    """Read SPY minute bars and reduce to daily RTH open/close; None if absent."""
    try:
        df = s3io.read_parquet(spy_minute_uri()).to_pandas()
    except Exception:
        print(f"benchmark: no SPY data at {spy_minute_uri()} — overlay skipped")
        return None
    try:
        return _daily_from_minutes(df)
    except Exception as e:
        print(f"benchmark: SPY file unreadable ({e}) — overlay skipped")
        return None


def spy_returns(s3io, days: list) -> Optional[pd.DataFrame]:
    """SPY daily returns aligned to the study's trade days.

    Returns a DataFrame indexed by trade_day with columns:
      buy_hold   close/prev_close - 1 (prior close from the bar history,
                 which may precede the study window)
      open_close close/open - 1
    Days missing from the SPY history come back as 0.0 (flat), so cumulative
    curves stay aligned with the strategy series. None if no usable data.
    """
    daily = load_spy_daily(s3io)
    if daily is None:
        return None
    out = pd.DataFrame(index=pd.Index(days, name="trade_day"))
    # buy_hold compounds BETWEEN consecutive requested days (period return),
    # so a sparse day list (e.g. an index scope trading 24 of 62 days) still
    # tracks the true SPY trajectory instead of dropping the gap days.
    closes = daily["close"]
    sampled = closes.reindex(days)
    prior = closes[closes.index < days[0]]
    base = prior.iloc[-1] if len(prior) else None
    prev = sampled.shift(1)
    if base is not None:
        prev.iloc[0] = base
    out["buy_hold"] = sampled / prev - 1
    out["open_close"] = (daily["close"] / daily["open"] - 1).reindex(days)
    covered = out["open_close"].notna().sum()
    if covered == 0:
        print("benchmark: SPY history covers none of the study days — skipped")
        return None
    if covered < len(days):
        print(f"benchmark: SPY covers {covered}/{len(days)} study days; "
              "missing days filled flat (0%)")
    return out.fillna(0.0)


def spy_equity_curve(spy: pd.DataFrame, days: list) -> "pd.Series | None":
    """Cumulative SPY buy&hold (%) SAMPLED at `days` from the trajectory
    compounded over spy's full index. Every chart that samples from the same
    `spy` frame shows values on the same curve, regardless of which subset of
    days its scope trades. Missing days forward-fill (curve holds level)."""
    if spy is None:
        return None
    eq = (1 + spy["buy_hold"]).cumprod() - 1
    return (eq.reindex(days).ffill().fillna(0.0)) * 100