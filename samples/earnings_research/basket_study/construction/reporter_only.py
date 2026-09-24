"""Outright reporter trade: no hedge (README decision #7, 2026-09-24).

The fade-the-gap signal alone — long or short exactly $1 of the reporter,
no peer legs. Same entry (9:30 open), same exit marks and skip rules as the
hedged constructions, so the difference between this cell and any hedged
cell is purely the contribution of the hedge legs.

Peer universes do not apply: the evaluator emits this construction ONCE per
event under the pseudo-universe UNHEDGED_UNIVERSE ("none") rather than four
times. Downstream POOLED averages exclude it (portfolio_analysis.EXTRA_CELLS).
Shorts assume zero borrow cost and full availability, like the hedged legs.
"""

from construction.base import Basket, BasketContext

UNHEDGED_UNIVERSE = "none"


def build(ctx: BasketContext, universe: str = UNHEDGED_UNIVERSE) -> Basket:
    return Basket(weights={ctx.reporter: float(ctx.direction)},
                  diagnostics={"universe": UNHEDGED_UNIVERSE, "n_peers": 0,
                               "hedge_gross": 0.0, "clamped": False})
