"""Ridge construction: beta-neutral hedge (README decisions log, item 2).

Fit reporter ~ peers on the panel's PRE-EVENT RTH bars only (ETH excluded),
then:

  1. ridge with data-scaled lambda = K * mean(diag(X'X))
  2. clip-to-zero-and-refit: iteratively drop negative-coefficient peers
  3. slope-1 normalization: scale weights so the fitted slope of reporter
     on the hedge basket equals 1
  4. clamp hedge gross notional to [0.5, 2.0] x reporter notional (logged)

NO INTERCEPT anywhere (deliberate): the weights are deployed against raw
trade-day returns — the P&L has no intercept term — so they are estimated
against raw series too. Centering would optimize a different loss (tracking
deviations-from-window-mean) and hide drift mismatch inside a discarded
constant. Consistently, the slope-1 step is the origin-forced regression
y'b / b'b, not cov/var.

fit_space switch:
  'returns' (default) — minute log-returns; stationary, one row per minute
      of genuine co-movement information. The v1-grid setting.
  'levels' — cumulative log-returns (integrated series, all anchored at 0
      at the window start). Fits trend/level co-movement; statistically
      suspect on ~2 sessions of data (spurious-regression risk, residual
      autocorrelation ~1) — provided as an explicit experiment, not a
      recommendation. Cumulative log-returns rather than raw prices so
      coefficients stay unitless notional weights and the shared lambda
      penalty is not distorted by price scale ($885 CAT vs $14 F).

All-peers-dropped or degenerate fit -> Basket(weights={}) with a skip reason.
"""

from typing import Dict, List

import numpy as np
import pandas as pd

from construction.base import (COVERAGE_FLOOR, Basket, BasketContext,
                               universe_symbols)
from rules.sessions import RTH_END_MIN, RTH_START_MIN

K_LAMBDA = 0.1
CLAMP_LO, CLAMP_HI = 0.5, 2.0
FIT_SPACES = ("returns", "levels")

# COVERAGE_FLOOR is owned by construction.base (basket-admission rule,
# applied once per event via admissible_peers). The matrix filter below is
# kept as defense-in-depth for direct callers that bypass admission: one
# illiquid peer (FCFS/CPSS: 33% coverage) would otherwise starve the inner
# join for the whole basket (780 -> 258 minutes).


def _pre_event_rth_matrix(bars: pd.DataFrame, symbols: List[str],
                          trade_day: str, fit_space: str,
                          reporter: str = None):
    """Fitting matrix (rows = minutes, cols = symbols) from RTH bars strictly
    before the trade day, inner-joined on common minutes. Peers (never the
    reporter) below COVERAGE_FLOOR of the union minutes are dropped first.
    'returns': minute log-returns. 'levels': cumulative log-returns from the
    first common minute (every series starts at exactly 0).
    Returns (matrix, dropped_low_coverage {symbol: coverage})."""
    rth = bars[(bars["et_date"] < trade_day)
               & (bars["et_min"] >= RTH_START_MIN) & (bars["et_min"] < RTH_END_MIN)]
    wide = (rth[rth["symbol"].isin(symbols)]
            .pivot_table(index="ts", columns="symbol", values="close")
            .sort_index()
            .astype("float64"))    # panels use nullable Float64; numpy needs plain floats
    coverage = wide.notna().mean()
    low = {s: float(coverage[s]) for s in wide.columns
           if coverage[s] < COVERAGE_FLOOR}
    if reporter in low:
        # thin REPORTER: the event is not fittable at all — signal the caller
        # to skip by returning an empty matrix (never fit a filtered basket
        # around a reporter we would have excluded as a peer).
        return wide.iloc[0:0], low
    wide = wide.drop(columns=list(low)).dropna(how="any")
    logp = np.log(wide)
    if fit_space == "returns":
        return logp.diff().dropna(how="any"), low
    return logp - logp.iloc[0], low  # levels: cumulative log-return, anchored at 0


def _ridge_nonneg(X: np.ndarray, y: np.ndarray, cols: List[str]):
    """No-intercept ridge with clip-to-zero-and-refit.
    Returns (weights_by_col, n_refits)."""
    keep = list(range(X.shape[1]))
    refits = 0
    while keep:
        Xk = X[:, keep]
        lam = K_LAMBDA * float(np.mean(np.sum(Xk * Xk, axis=0)))
        beta = np.linalg.solve(Xk.T @ Xk + lam * np.eye(len(keep)), Xk.T @ y)
        if (beta >= 0).all():
            return {cols[i]: float(b) for i, b in zip(keep, beta)}, refits
        keep = [i for i, b in zip(keep, beta) if b > 0]
        refits += 1
    return {}, refits


def build(ctx: BasketContext, universe: str = "all",
          fit_space: str = "returns", sizing: str = "beta") -> Basket:
    """sizing='beta' (default): slope-1 normalization + [0.5,2] clamp — the
    beta-neutral spec. sizing='dollar': same fitted MIX, rescaled so hedge
    gross notional is exactly 1.0 (dollar-neutral; isolates selection skill
    from sizing — the 'ridge mix @ gross 1.0' basket from validation)."""
    if fit_space not in FIT_SPACES:
        raise ValueError(f"fit_space must be one of {FIT_SPACES}")
    if sizing not in ("beta", "dollar"):
        raise ValueError("sizing must be 'beta' or 'dollar'")
    peers = universe_symbols(ctx.peers, universe, ctx.reporter)
    mat, low_cov = _pre_event_rth_matrix(ctx.bars, [ctx.reporter] + peers,
                                         ctx.trade_day, fit_space,
                                         reporter=ctx.reporter)
    diag: Dict = {"universe": universe, "fit_space": fit_space, "sizing": sizing,
                  "dropped_low_coverage": low_cov}

    if ctx.reporter not in mat.columns or len(mat) < 30:
        return Basket(weights={}, diagnostics=diag | {"skip": "insufficient pre-event RTH data",
                                                      "n_minutes": len(mat)})
    peer_cols = [c for c in mat.columns if c != ctx.reporter]
    if not peer_cols:
        return Basket(weights={}, diagnostics=diag | {"skip": "no peers with pre-event data"})

    y = mat[ctx.reporter].to_numpy()
    X = mat[peer_cols].to_numpy()
    w, refits = _ridge_nonneg(X, y, peer_cols)
    diag |= {"n_minutes": len(mat), "n_refits": refits,
             "dropped_negative": sorted(set(peer_cols) - set(w))}
    if not w:
        return Basket(weights={}, diagnostics=diag | {"skip": "all peers clipped"})

    if sizing == "dollar":
        # dollar-neutral: keep the fitted MIX, force hedge gross to 1.0
        gross = sum(w.values())
        w = {s: v / gross for s, v in w.items()}
        diag["hedge_gross"] = 1.0
    else:
        # slope-1, origin-forced (no intercept, matching the ridge): scaling w
        # by y'b/b'b makes the refit slope of y on the scaled basket exactly 1.
        b = mat[list(w)].to_numpy() @ np.array(list(w.values()))
        bb = float(b @ b)
        if bb <= 0:
            return Basket(weights={}, diagnostics=diag | {"skip": "degenerate basket variance"})
        slope = float(y @ b) / bb
        diag["fitted_slope"] = slope
        if slope <= 0:
            return Basket(weights={}, diagnostics=diag | {"skip": "non-positive fitted slope"})
        w = {s: v * slope for s, v in w.items()}

        gross = sum(abs(v) for v in w.values())
        diag["hedge_gross_prescale"] = gross
        clamped = min(max(gross, CLAMP_LO), CLAMP_HI)
        diag["clamped"] = clamped != gross
        if clamped != gross:
            w = {s: v * clamped / gross for s, v in w.items()}
        diag["hedge_gross"] = clamped

    weights = {ctx.reporter: float(ctx.direction)}
    for s, v in w.items():
        weights[s] = -ctx.direction * v
    return Basket(weights=weights, diagnostics=diag)