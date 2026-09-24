"""Build the per-event basket panel: reporter + peers, market data fetched
over the [-2,+5] window, stamped to the golden peer-panel schema.

Output columns (== tests/test_golden_peer_panel.py):
  symbol, ts_event, rtype, publisher_id, instrument_id, open, high, low,
  close, volume, ts, reporter_relationship, event_id, event_bar

Per symbol:
  - event_id on every row (whole basket relatable by event)
  - reporter_relationship: 'primary' for the reporter, else the peer bucket
  - event_bar True at the fixed event minute: the real bar(s) if the symbol
    traded that minute (organic), else ONE inserted null-market row (synthetic)

Data-availability policy: symbols are fetched LIVE (no pre-validation);
whatever comes back is written as-is. Per symbol we compare the trading days
actually present against the window's expected sessions and log a GIANT
warning when coverage falls below COVERAGE_WARN (80%) — including the empty
case. The panel is still written unchanged; availability decisions are made
at analysis time, not capture time.
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from databento_client import DatabentoClient
from sharding import trading_window

logger = logging.getLogger(__name__)

# Below this fraction of expected trading days present, a symbol's coverage
# warning is emitted. Research policy: don't run analysis on <80% coverage —
# enforced downstream at analysis time, only *surfaced* here.
COVERAGE_WARN = 0.80

PANEL_COLUMNS = ["symbol", "ts_event", "rtype", "publisher_id", "instrument_id",
                 "open", "high", "low", "close", "volume", "ts",
                 "reporter_relationship", "event_id", "event_bar"]

_NULLABLE = {"ts_event": "Int64", "rtype": "Int64", "publisher_id": "Int64",
             "instrument_id": "Int64", "open": "Float64", "high": "Float64",
             "low": "Float64", "close": "Float64", "volume": "Int64",
             "event_id": "Int64"}


def _expected_sessions(start: str, end: str) -> pd.DatetimeIndex:
    """XNYS sessions inside [start, end) — the days a US-listed symbol is
    expected to have bars."""
    from sharding import _XNYS
    sched = _XNYS.schedule(start_date=start,
                           end_date=pd.Timestamp(end) - pd.Timedelta(days=1))
    return sched.index.normalize()


def _coverage_check(symbol: str, rel: str, event_id: int,
                    df: pd.DataFrame, start: str, end: str) -> None:
    """GIANT log warning when a symbol's trading-day coverage < COVERAGE_WARN.
    Capture is unaffected — this only surfaces the gap."""
    expected = _expected_sessions(start, end)
    if len(expected) == 0:
        return
    present = (set() if df.empty
               else set(pd.to_datetime(df["ts"], utc=True).dt.normalize()
                        .dt.tz_localize(None)))
    missing = sorted(d.date().isoformat() for d in expected if d not in present)
    coverage = 1 - len(missing) / len(expected)
    if coverage < COVERAGE_WARN:
        logger.warning(
            "\n"
            "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"
            "!!! DATA AVAILABILITY WARNING — event %s, symbol %s (%s)\n"
            "!!! window %s..%s: %d/%d trading days present (%.0f%% coverage)\n"
            "!!! missing days: %s\n"
            "!!! panel captured and written AS-IS; decide at analysis time\n"
            "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",
            event_id, symbol, rel, start, end,
            len(expected) - len(missing), len(expected), coverage * 100,
            ", ".join(missing),
        )


def _fetch_symbol(db: DatabentoClient, symbol: str, rel: str,
                  event_id: int, event_ts: pd.Timestamp,
                  start: str, end: str,
                  event_marker: bool = True,
                  prefetched: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    df = prefetched if prefetched is not None else db.get_range(symbol, start, end)
    event_ns = int(event_ts.value)

    _coverage_check(symbol, rel, event_id, df, start, end)

    if df.empty:
        rows = pd.DataFrame(columns=PANEL_COLUMNS)
    else:
        df = df.copy()
        df.insert(0, "symbol", symbol)
        df["reporter_relationship"] = rel
        df["event_id"] = event_id
        df["event_bar"] = df["ts_event"] == event_ns
        rows = df[PANEL_COLUMNS]

    rows = rows.astype(_NULLABLE | {c: rows[c].dtype for c in PANEL_COLUMNS
                                    if c not in _NULLABLE})

    # synthetic marker only if the symbol did NOT trade the event minute —
    # suppressed for tail top-ups (the head fetch already wrote the marker)
    if event_marker and not (not rows.empty and rows["event_bar"].any()):
        marker = pd.DataFrame([{
            "symbol": symbol, "ts_event": event_ns, "rtype": pd.NA,
            "publisher_id": pd.NA, "instrument_id": pd.NA, "open": pd.NA,
            "high": pd.NA, "low": pd.NA, "close": pd.NA, "volume": pd.NA,
            "ts": event_ts, "reporter_relationship": rel,
            "event_id": event_id, "event_bar": True,
        }]).astype(rows.dtypes.to_dict())
        rows = pd.concat([rows, marker], ignore_index=True)
    return rows


def build_panel_symbols(db: DatabentoClient, event_id: int,
                        event_datetime_utc: str,
                        sym_rel_pairs: List[tuple],
                        window: Optional[tuple] = None,
                        event_marker: bool = True) -> pd.DataFrame:
    """Fetch a subset of a basket: [(symbol, relationship), ...] rows only.
    Used for per-symbol resume — refetching just the symbols a panel lacks.
    `window` overrides the full [-2,+5] range (partial fetch / tail top-up);
    event_marker=False suppresses synthetic event rows (tail top-ups append
    to a panel whose head already carries them)."""
    start, end = window or trading_window(event_datetime_utc)
    event_ts = pd.Timestamp(event_datetime_utc, tz="UTC").floor("min")
    # Sources with a multi-symbol endpoint (Alpaca) fetch the whole basket in
    # one paginated request stream — the request-budget lever under the
    # account-wide rate limit. Per-symbol processing below stays identical.
    prefetched = {}
    if hasattr(db, "get_range_multi"):
        prefetched = db.get_range_multi([s for s, _ in sym_rel_pairs], start, end)
    frames = [_fetch_symbol(db, sym, rel, event_id, event_ts, start, end,
                            event_marker=event_marker,
                            prefetched=prefetched.get(sym))
              for sym, rel in sym_rel_pairs]
    return pd.concat(frames, ignore_index=True)


def build_panel(db: DatabentoClient, event_id: int, event_datetime_utc: str,
                reporter_symbol: str, peers: Dict[str, List[str]]) -> pd.DataFrame:
    """peers: {relationship -> [symbols]}, e.g.
    {'pure play': [...], 'functional': [...], 'correlated': [...]}."""
    pairs = [(reporter_symbol, "primary")]
    for rel, syms in peers.items():
        pairs += [(s, rel) for s in syms if s != reporter_symbol]

    panel = build_panel_symbols(db, event_id, event_datetime_utc, pairs)
    panel = panel.sort_values(
        ["symbol", "ts", "publisher_id"]).reset_index(drop=True)
    logger.info("event %s panel: %d symbols, %d rows",
                event_id, panel["symbol"].nunique(), len(panel))
    return panel
