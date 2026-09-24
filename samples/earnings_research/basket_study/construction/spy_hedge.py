"""SPY-hedged constructions (README decision #8, 2026-09-24).

One hedge symbol, so no ridge: the reporter is +/-$1 by the gap direction and
SPY takes the opposite side sized either

  sizing='dollar'  -> exactly $1 of SPY (dollar-neutral; fit space irrelevant)
  sizing='beta'    -> beta of the reporter on SPY over the panel's pre-event
                      RTH minutes, origin-forced (y'x / x'x, no intercept, the
                      same estimator the peer baskets' slope-1 step uses),
                      in fit_space 'returns' (minute log-returns) or 'levels'
                      (cumulative log-returns), then clamped to
                      [CLAMP_LO, CLAMP_HI] x reporter notional exactly like the
                      peer baskets (user decision 2026-09-24; clamp flagged).

SPY bars are not in the panels; spy_bars.attach_spy() appends them under
reporter_relationship 'benchmark' before evaluation. The matrix helper is
shared with ridge.py so the fit window and coverage handling are identical.
"""

from typing import Dict

import numpy as np

from construction.base import Basket, BasketContext
from construction.ridge import CLAMP_HI, CLAMP_LO, FIT_SPACES, _pre_event_rth_matrix

SPY = "SPY"
SPY_UNIVERSE = "SPY"
MIN_FIT_MINUTES = 30


def beta_on_spy(ctx: BasketContext, fit_space: str):
    """(beta, n_minutes) of the reporter on SPY, origin-forced; (None, n) if unfittable."""
    mat, _ = _pre_event_rth_matrix(ctx.bars, [ctx.reporter, SPY], ctx.trade_day,
                                   fit_space, reporter=ctx.reporter)
    if ctx.reporter not in mat.columns or SPY not in mat.columns or len(mat) < MIN_FIT_MINUTES:
        return None, len(mat)
    y, x = mat[ctx.reporter].to_numpy(), mat[SPY].to_numpy()
    xx = float(x @ x)
    if xx <= 0:
        return None, len(mat)
    return float(y @ x) / xx, len(mat)


def build(ctx: BasketContext, universe: str = SPY_UNIVERSE,
          sizing: str = "beta", fit_space: str = "levels") -> Basket:
    if sizing not in ("beta", "dollar"):
        raise ValueError("sizing must be 'beta' or 'dollar'")
    if fit_space not in FIT_SPACES:
        raise ValueError(f"fit_space must be one of {FIT_SPACES}")
    diag: Dict = {"universe": SPY_UNIVERSE, "sizing": sizing, "fit_space": fit_space, "n_peers": 1}
    if SPY not in set(ctx.bars["symbol"].unique()):
        return Basket(weights={}, diagnostics=diag | {"skip": "no SPY bars"})
    if sizing == "dollar":
        w, diag["hedge_gross"], diag["clamped"] = 1.0, 1.0, False
    else:
        beta, n = beta_on_spy(ctx, fit_space)
        diag["n_minutes"] = n
        if beta is None:
            return Basket(weights={}, diagnostics=diag | {"skip": "insufficient pre-event RTH data"})
        diag["fitted_beta"] = beta
        if beta <= 0:
            return Basket(weights={}, diagnostics=diag | {"skip": "non-positive beta"})
        w = min(max(beta, CLAMP_LO), CLAMP_HI)
        diag["hedge_gross_prescale"] = beta
        diag["clamped"] = w != beta
        diag["hedge_gross"] = w
    return Basket(weights={ctx.reporter: float(ctx.direction), SPY: -ctx.direction * w},
                  diagnostics=diag)
