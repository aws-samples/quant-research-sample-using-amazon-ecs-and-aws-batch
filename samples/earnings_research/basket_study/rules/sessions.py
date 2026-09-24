"""Session logic for the earnings basket study (rules in ../README.md).

Panels carry 1-min bars in UTC with multi-venue duplicates per minute
(publisher_id). Everything here works in ET on consolidated bars.

Conventions (README "Rules"):
  RTH            = 09:30 <= t < 16:00 ET
  signal price   = last print at/before 09:28 ET on the trade day, pre-market
                   bars of that morning only; None if the morning had none
  previous close = close of the last RTH bar of the prior session in the panel
  entry mark     = open of the 09:30 bar; reporter must have a real one,
                   peer legs forward-fill from the most recent prior print
  exit mark      = close of the last RTH bar of the trade day (forward-fill
                   never needed: a leg with no RTH print that day uses its
                   most recent prior print, same forward-fill rule)
"""

from datetime import date, timedelta
from typing import Optional

import pandas as pd
import pandas_market_calendars as mcal

ET = "America/New_York"
_XNYS = mcal.get_calendar("XNYS")

RTH_START_MIN = 9 * 60 + 30      # 09:30
RTH_END_MIN = 16 * 60            # 16:00 (exclusive)
SIGNAL_MIN = 9 * 60 + 28         # 09:28 (inclusive)


def consolidate(panel: pd.DataFrame) -> pd.DataFrame:
    """One row per (symbol, minute) from multi-venue rows: open/close from the
    max-volume venue, high=max, low=min, volume summed. Synthetic event-marker
    rows (event_bar True) are dropped — NOT via rtype, which is Databento-only
    and all-NA in ALPACA.SIP panels. Adds et_date (str) and et_min (int)."""
    real = panel[~panel["event_bar"].astype(bool)].copy()
    mv = real.sort_values("volume").groupby(["symbol", "ts"]).tail(1)
    agg = real.groupby(["symbol", "ts"]).agg(
        high=("high", "max"), low=("low", "min"), volume=("volume", "sum"))
    out = mv.set_index(["symbol", "ts"])[["open", "close"]].join(agg).reset_index()
    et = out["ts"].dt.tz_convert(ET)
    out["et_date"] = et.dt.date.astype(str)
    out["et_min"] = et.dt.hour * 60 + et.dt.minute
    return out.sort_values(["symbol", "ts"]).reset_index(drop=True)


def trade_day_for_event(event_ts_utc: pd.Timestamp) -> Optional[str]:
    """Map a release timestamp to its trade day per the scope rule:
    pre-market (ET, before 09:30 on a session day) -> that day;
    after-hours (>= 16:00, or any hour on a non-session day) -> next session;
    RTH (09:30..16:00 on a session day) -> None (excluded)."""
    et = event_ts_utc.tz_convert(ET)
    d, minute = et.date(), et.hour * 60 + et.minute
    sched = _XNYS.schedule(start_date=d - timedelta(days=1), end_date=d + timedelta(days=7))
    sessions = [s.date() for s in sched.index]
    if d in sessions and minute < RTH_START_MIN:
        return str(d)
    if d in sessions and RTH_START_MIN <= minute < RTH_END_MIN:
        return None                                   # RTH release: excluded
    return str(next(s for s in sessions if s > d))    # after-hours / non-session


def previous_close(bars: pd.DataFrame, symbol: str, trade_day: str) -> Optional[float]:
    """Close of the symbol's last RTH bar strictly before trade_day."""
    b = bars[(bars["symbol"] == symbol) & (bars["et_date"] < trade_day)
             & (bars["et_min"] >= RTH_START_MIN) & (bars["et_min"] < RTH_END_MIN)]
    return None if b.empty else float(b.iloc[-1]["close"])


def signal_price(bars: pd.DataFrame, symbol: str, trade_day: str) -> Optional[float]:
    """Last pre-market print at/before 09:28 ET on trade_day (that morning
    only). None -> no pre-market trading -> event is skipped."""
    b = bars[(bars["symbol"] == symbol) & (bars["et_date"] == trade_day)
             & (bars["et_min"] <= SIGNAL_MIN)]
    return None if b.empty else float(b.iloc[-1]["close"])


def entry_price(bars: pd.DataFrame, symbol: str, trade_day: str,
                allow_ffill: bool) -> Optional[float]:
    """Open of the 09:30 bar. Reporter (allow_ffill=False): None if absent.
    Peers (allow_ffill=True): forward-fill from the most recent print before
    09:30 (README decision 3)."""
    b = bars[bars["symbol"] == symbol]
    at_open = b[(b["et_date"] == trade_day) & (b["et_min"] == RTH_START_MIN)]
    if not at_open.empty:
        return float(at_open.iloc[0]["open"])
    if not allow_ffill:
        return None
    prior = b[(b["et_date"] < trade_day)
              | ((b["et_date"] == trade_day) & (b["et_min"] < RTH_START_MIN))]
    return None if prior.empty else float(prior.iloc[-1]["close"])


def exit_price(bars: pd.DataFrame, symbol: str, trade_day: str) -> Optional[float]:
    """Close of the last RTH bar on trade_day; forward-fill from the most
    recent prior print if the symbol had no RTH print that day."""
    b = bars[bars["symbol"] == symbol]
    rth = b[(b["et_date"] == trade_day)
            & (b["et_min"] >= RTH_START_MIN) & (b["et_min"] < RTH_END_MIN)]
    if not rth.empty:
        return float(rth.iloc[-1]["close"])
    prior = b[b["et_date"] <= trade_day]
    prior = prior[(prior["et_date"] < trade_day) | (prior["et_min"] < RTH_END_MIN)]
    return None if prior.empty else float(prior.iloc[-1]["close"])