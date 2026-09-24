"""The four validated construction methods, runnable by name.

Each entry: name -> callable(ctx, universe) -> Basket. These are the exact
four baskets from the GM validation charts; the experiment grid is
constructions x peer universes.
"""

from typing import Callable, Dict

from construction import equal_weight, reporter_only, ridge, spy_hedge
from construction.base import Basket, BasketContext

# Naming: <weighting>_<fit space if fitted>_<neutrality>. Every name states
# its neutrality explicitly: dollar_neutral = hedge gross fixed at 1.0;
# beta_neutral = hedge sized by the fitted slope (clamped [0.5, 2]).
CONSTRUCTIONS: Dict[str, Callable[[BasketContext, str], Basket]] = {
    "equal_weight_dollar_neutral": lambda ctx, u: equal_weight.build(ctx, u),
    "ridge_returns_beta_neutral": lambda ctx, u: ridge.build(ctx, u, fit_space="returns", sizing="beta"),
    "ridge_returns_dollar_neutral": lambda ctx, u: ridge.build(ctx, u, fit_space="returns", sizing="dollar"),
    "ridge_levels_beta_neutral": lambda ctx, u: ridge.build(ctx, u, fit_space="levels", sizing="beta"),
}

# Unhedged constructions (2026-09-24): the reporter alone, no peer legs. Kept
# OUT of CONSTRUCTIONS so the 4 x 4 hedged grid (and every POOLED average built
# on it) is unchanged; evaluate.py emits these once per event under
# reporter_only.UNHEDGED_UNIVERSE. Select with `evaluate.py --constructions`.
OUTRIGHT: Dict[str, Callable[[BasketContext, str], Basket]] = {
    "reporter_only": lambda ctx, u: reporter_only.build(ctx, u),
}

# SPY-hedged constructions (2026-09-24, README decision #8): one hedge symbol,
# no ridge; also outside CONSTRUCTIONS/POOLED, emitted once per event under the
# pseudo-universe spy_hedge.SPY_UNIVERSE. Levels-beta first: it is the cell the
# user asked for and the one the viewer defaults to for universe "SPY".
SPY_HEDGED: Dict[str, Callable[[BasketContext, str], Basket]] = {
    "spy_beta_levels_neutral": lambda ctx, u: spy_hedge.build(ctx, u, sizing="beta", fit_space="levels"),
    "spy_beta_returns_neutral": lambda ctx, u: spy_hedge.build(ctx, u, sizing="beta", fit_space="returns"),
    "spy_dollar_neutral": lambda ctx, u: spy_hedge.build(ctx, u, sizing="dollar"),
}

ALL_CONSTRUCTIONS: Dict[str, Callable[[BasketContext, str], Basket]] = CONSTRUCTIONS | OUTRIGHT | SPY_HEDGED
UNHEDGED_UNIVERSE = reporter_only.UNHEDGED_UNIVERSE
SPY_UNIVERSE = spy_hedge.SPY_UNIVERSE

# construction -> the single pseudo-universe it is emitted under (None = the
# four peer universes). Single source for evaluate.py, producers and viewer.
PSEUDO_UNIVERSE: Dict[str, str] = {**{n: UNHEDGED_UNIVERSE for n in OUTRIGHT},
                                   **{n: SPY_UNIVERSE for n in SPY_HEDGED}}


def pseudo_universe(name: str):
    return PSEUDO_UNIVERSE.get(name)


def needs_spy(names) -> bool:
    """True if any selected construction hedges with SPY (evaluate attaches SPY bars)."""
    return any(n in SPY_HEDGED for n in (names or ()))


def select(names) -> Dict[str, Callable[[BasketContext, str], Basket]]:
    """Ordered subset of ALL_CONSTRUCTIONS by name; None/empty = the hedged four."""
    if not names:
        return dict(CONSTRUCTIONS)
    unknown = [n for n in names if n not in ALL_CONSTRUCTIONS]
    if unknown:
        raise KeyError(f"unknown construction(s) {unknown}; known: {sorted(ALL_CONSTRUCTIONS)}")
    return {n: ALL_CONSTRUCTIONS[n] for n in names}
