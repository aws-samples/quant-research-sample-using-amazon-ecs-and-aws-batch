"""Equal-weight construction: dollar-neutral hedge.

Hedge gross notional = reporter notional (1.0), split equally across the
peer universe; every hedge leg opposite the reporter. Peers with no bars at
all in the panel are excluded up front (they cannot be marked even by
forward-fill); that exclusion is recorded in diagnostics.
"""

from construction.base import Basket, BasketContext, universe_symbols


def build(ctx: BasketContext, universe: str = "all") -> Basket:
    peers = universe_symbols(ctx.peers, universe, ctx.reporter)
    have_bars = set(ctx.bars["symbol"].unique())
    usable = [s for s in peers if s in have_bars]
    if not usable:
        return Basket(weights={}, diagnostics={"skip": "no usable peers",
                                               "universe": universe})
    w = 1.0 / len(usable)
    weights = {ctx.reporter: float(ctx.direction)}
    for s in usable:
        weights[s] = -ctx.direction * w
    return Basket(weights=weights,
                  diagnostics={"universe": universe, "n_peers": len(usable),
                               "hedge_gross": 1.0,     # dollar-neutral by construction
                               "dropped_no_bars": sorted(set(peers) - set(usable))})