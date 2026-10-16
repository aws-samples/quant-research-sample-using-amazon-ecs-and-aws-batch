"""Correlation-peer hedge legs for the corr-peer arm (2026-10-11).

The user's rule: take the reporter's correlation peers derived from its last four earnings
sessions (RTH window, pooled top 10 — earnings_market_data corr-peers `peersets` stage on run
full1), HARDCODE them (rules/corr_peersets_top10.csv, columns reporter, rank, symbol,
pooled_corr, n_days, days_used) and use that one set for the whole ten-year backtest
(look-ahead accepted by the user for this study).

Panels carry the reporter and its LLM peers only, so the hedge legs are attached at evaluation
time like the SPY leg (spy_bars): load the S&P 1500 universe day files overlapping the panel's
window from

    s3://<bucket>/earnings-market-data/ALPACA.SIP/universe/sp1500/date=YYYY-MM-DD/bars.parquet

(Alpaca SIP 1-min, 04:00-20:00 ET, one file per trading day; backfilled to 2015-12-28 by
`main.py corr-peers backfill`), keep the reporter's peers, and append them to the panel in the
panel's own schema under reporter_relationship CORR_REL. The LLM peer rows are DROPPED from
the panel first so they cannot leak into the universe; the reporter ('primary') and the SPY
benchmark rows are kept. evaluate.py then runs the four hedged constructions against universe
CORR_UNIVERSE only. A reporter without a hardcoded set gets no legs -> an explicit skip
("no_corr_peers"); `plan --peer-source corr` already restricts the manifest to covered events.
"""
import csv
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

import pandas as pd

import spy_bars  # noqa: E402
from spy_bars import BENCHMARK_REL, ET, PANEL_COLUMNS

CORR_REL = "corr"
CORR_UNIVERSE = "corr"
PEERSETS_CSV = Path(__file__).resolve().parent / "rules" / "corr_peersets_top10.csv"
UNI_PREFIX = "earnings-market-data/ALPACA.SIP/universe/sp1500"
KEEP_RELS = ("primary", BENCHMARK_REL)       # rows that survive the attach; every other bucket is dropped

_PEERSETS: Optional[Dict[str, List[str]]] = None


def load_peersets(path: Path = PEERSETS_CSV) -> Dict[str, List[str]]:
    """reporter -> peers in rank order (the hardcoded file is the single source)."""
    rows = sorted(csv.DictReader(open(path, newline="")), key=lambda r: (r["reporter"], int(r["rank"])))
    out: Dict[str, List[str]] = {}
    for r in rows:
        if r["symbol"] != r["reporter"]:
            out.setdefault(r["reporter"], []).append(r["symbol"])
    return out


def peersets() -> Dict[str, List[str]]:
    global _PEERSETS
    if _PEERSETS is None:
        _PEERSETS = load_peersets()
    return _PEERSETS


def day_key(day: str) -> str:
    return f"{UNI_PREFIX}/date={day}/bars.parquet"


def et_days(start: pd.Timestamp, end: pd.Timestamp) -> List[str]:
    """Every ET calendar date in [start, end]; non-session days simply have no file."""
    s, e = start.tz_convert(ET).normalize().tz_localize(None), end.tz_convert(ET).normalize().tz_localize(None)
    return [d.strftime("%Y-%m-%d") for d in pd.date_range(s, e, freq="D")]


def load_peer_bars(s3io, symbols: Iterable[str], start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """Universe-store bars for `symbols` with ts in [start, end] (UTC): symbol, ts, open, high, low, close, volume."""
    import polars as pl
    syms = sorted(set(symbols))
    parts = []
    for d in et_days(start, end):
        try:
            df = s3io.read_parquet(f"s3://{spy_bars.BUCKET}/{day_key(d)}")
        except Exception as e:                      # weekend / holiday / not backfilled: no file
            if pd.Timestamp(d).dayofweek < 5:
                print(f"WARNING: universe bars {d} unavailable ({e.__class__.__name__})", flush=True)
            continue
        parts.append(df.filter(pl.col("symbol").is_in(syms)).to_pandas())
    if not parts:
        return pd.DataFrame(columns=["symbol", "ts", "open", "high", "low", "close", "volume"])
    out = pd.concat(parts, ignore_index=True)
    out["ts"] = pd.to_datetime(out["ts"], utc=True).astype("datetime64[ns, UTC]")
    return out[(out["ts"] >= start) & (out["ts"] <= end)].sort_values(["symbol", "ts"])


def as_panel_rows(bars: pd.DataFrame, event_id: int) -> pd.DataFrame:
    """Universe-store bars -> panel-schema rows (relationship CORR_REL)."""
    n = len(bars)
    na = lambda: pd.array([pd.NA] * n, dtype="Int64")  # noqa: E731
    out = pd.DataFrame({
        "symbol": bars["symbol"].to_numpy(),
        "ts_event": na(), "rtype": na(), "publisher_id": na(), "instrument_id": na(),
        "open": pd.array(bars["open"].to_numpy(dtype=float), dtype="Float64"),
        "high": pd.array(bars["high"].to_numpy(dtype=float), dtype="Float64"),
        "low": pd.array(bars["low"].to_numpy(dtype=float), dtype="Float64"),
        "close": pd.array(bars["close"].to_numpy(dtype=float), dtype="Float64"),
        "volume": pd.array(bars["volume"].to_numpy(dtype="int64"), dtype="Int64"),
        "ts": pd.to_datetime(bars["ts"].to_numpy(), utc=True),
        "reporter_relationship": CORR_REL,
        "event_id": pd.array([event_id] * n, dtype="Int64"),
        "event_bar": False,
    })
    return out[PANEL_COLUMNS]


def attach_corr_peers(panel: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
    """Panel minus its LLM peer rows (and any earlier corr rows) plus `bars` as corr-peer legs.
    The reporter and the SPY benchmark rows are kept. Idempotent."""
    base = panel[panel["reporter_relationship"].isin(KEEP_RELS)]
    if bars is None or bars.empty:
        return base.reset_index(drop=True)
    rows = as_panel_rows(bars, int(panel["event_id"].iloc[0]))
    cols = [c for c in PANEL_COLUMNS if c in base.columns]
    out = pd.concat([base, rows[cols]], ignore_index=True)
    out["ts"] = pd.to_datetime(out["ts"], utc=True).astype(panel["ts"].dtype)
    return out


def attach_corr_peers_for_panel(panel: pd.DataFrame, s3io, peersets: Optional[Dict[str, List[str]]] = None) -> pd.DataFrame:
    """Attach the reporter's hardcoded peers' bars for the panel's window (none if the reporter has no set)."""
    from spy_bars import panel_window
    ps = peersets if peersets is not None else globals()["peersets"]()
    reporter = panel[panel["reporter_relationship"] == "primary"]["symbol"].iloc[0]
    syms = [s for s in ps.get(reporter, []) if s != reporter]
    start, end = panel_window(panel)
    if not syms or pd.isna(start) or pd.isna(end):
        return attach_corr_peers(panel, None)
    return attach_corr_peers(panel, load_peer_bars(s3io, syms, start, end))


def covered_event_ids(events, peersets: Dict[str, List[str]]) -> Set[int]:
    """Event ids whose reporter has a hardcoded set. events: polars frame with event_id, reporter."""
    import polars as pl
    return set(events.filter(pl.col("reporter").is_in(sorted(peersets)))["event_id"].cast(pl.Int64).to_list())
