"""Common construction interface (README "Rules").

A construction turns (consolidated bars, event context) into signed leg
weights per $1 of reporter notional:

    {symbol: signed_weight}

with the reporter at exactly +direction or -direction (magnitude 1.0) and
every hedge leg on the opposite side. Weights are notional fractions:
weight -0.25 on a peer means "short $0.25 of that peer per $1 long reporter".

Peer-universe filtering (all | pure play | functional | correlated) is shared
here, not per-construction. So is basket ADMISSION: admissible_peers applies
the coverage floor once, before any construction sees the peer dict — grid
cells then differ only in weighting scheme, never in peer-admission policy.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import pandas as pd

from rules.sessions import RTH_END_MIN, RTH_START_MIN

UNIVERSES = ("all", "pure play", "functional", "correlated")

# Basket-admission floor: peers present in fewer than this fraction of the
# pre-event RTH union minutes are inadmissible for EVERY construction — too
# illiquid to fit against AND too illiquid to trust as a hedge marked off
# forward-filled prints. Analysis-time enforcement of the capture-time policy
# (earnings_market_data/panel_builder.py COVERAGE_WARN = 0.80). The reporter
# is never admission-filtered; thin reporters skip downstream.
COVERAGE_FLOOR = 0.80


@dataclass
class BasketContext:
    bars: pd.DataFrame              # consolidate() output, all symbols
    reporter: str
    peers: Dict[str, List[str]]     # relationship -> symbols (from the panel)
    trade_day: str                  # 'YYYY-MM-DD'
    direction: int                  # +1 long reporter / -1 short reporter


@dataclass
class Basket:
    weights: Dict[str, float]       # symbol -> signed weight (incl. reporter)
    diagnostics: dict = field(default_factory=dict)


def admissible_peers(bars: pd.DataFrame, peers: Dict[str, List[str]],
                     trade_day: str, reporter: str
                     ) -> Tuple[Dict[str, List[str]], Dict[str, float]]:
    """Apply the coverage floor to every peer bucket, once per event.

    Coverage = fraction of the pre-event RTH union minutes (all panel symbols)
    in which the peer has a bar; a symbol absent from the panel is 0.0. Buckets
    are preserved (possibly emptied) so universe iteration is unchanged.
    Returns (filtered peers, {dropped_symbol: coverage})."""
    rth = bars[(bars["et_date"] < trade_day)
               & (bars["et_min"] >= RTH_START_MIN)
               & (bars["et_min"] < RTH_END_MIN)]
    union_minutes = rth["ts"].nunique()
    counts = rth.groupby("symbol")["ts"].nunique()
    dropped: Dict[str, float] = {}
    out: Dict[str, List[str]] = {}
    for rel, syms in peers.items():
        keep = []
        for s in syms:
            if s == reporter:
                continue
            cov = (float(counts.get(s, 0)) / union_minutes
                   if union_minutes else 0.0)
            if cov < COVERAGE_FLOOR:
                dropped[s] = cov
            else:
                keep.append(s)
        out[rel] = keep
    return out, dropped


def universe_symbols(peers: Dict[str, List[str]], universe: str,
                     reporter: str) -> List[str]:
    if universe not in UNIVERSES:
        raise ValueError(f"unknown universe {universe!r}")
    if universe == "all":
        syms = [s for v in peers.values() for s in v]
    else:
        syms = list(peers.get(universe, []))
    return sorted(set(s for s in syms if s != reporter))