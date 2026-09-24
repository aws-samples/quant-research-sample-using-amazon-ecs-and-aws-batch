"""Transaction costs on the ok shard frame — per-share commission only.

The evaluator books gross P&L per $1 of reporter notional and (since
2026-09-23) `shares_per_dollar` = sum |w_i| / entry_i over the marked legs,
i.e. how many shares change hands per $1 of reporter notional on ONE side.
A per-share commission is therefore a constant per (event, cell):

    fee_per_dollar = sides x rate x shares_per_dollar      (sides = 2: entry + exit)

and every P&L column (all intraday marks + close) is reduced by the same
amount, because the share count does not depend on the exit price.

Schedule (IBKR Pro Fixed, US stocks, memory ibkr-us-stock-fee-schedule):
    $0.005 per share, both sides, every leg (reporter + peers).
NOT modelled here: the $1.00 per-order minimum (needs a dollar book size),
the 1%-of-trade-value cap (binds only under $0.50/share), exchange and
regulatory pass-throughs. `legs` on the shard rows carries the per-leg
weight/price detail should any of those be added later.

Output directories of every producer carry `fee_suffix(rate)`
(e.g. portfolio_analysis_fee5m/) so gross artifacts stay byte-identical.
"""
import pandas as pd

IBKR_PRO_FIXED = 0.005          # USD per share
SIDES = 2                       # entry + exit


def fee_suffix(rate) -> str:
    """'' for no fee, else '_fee<mills>m' (1 mill = $0.001/share): 0.005 -> '_fee5m'."""
    if not rate:
        return ""
    return f"_fee{rate * 1000:g}m"


def fee_label(rate) -> str:
    return f"${rate:g} per share"


def pnl_columns(df: pd.DataFrame) -> list:
    return ["pnl"] + [c for c in df.columns if c.startswith("pnl_")]


def apply_per_share_fee(ok: pd.DataFrame, rate, sides: int = SIDES) -> pd.DataFrame:
    """Return a copy of `ok` with every P&L column net of the round-trip
    per-share commission and a `fee_per_dollar` column. rate 0/None -> `ok`
    unchanged (same object, no fee column)."""
    if not rate:
        return ok
    if "shares_per_dollar" not in ok.columns:
        raise ValueError("shards lack shares_per_dollar — re-evaluate with evaluate.py >= 0f20ac63 "
                         "(results-10y-v2) before applying a per-share fee")
    out = ok.copy()
    fee = sides * float(rate) * out["shares_per_dollar"].astype(float)
    out["fee_per_dollar"] = fee
    for c in pnl_columns(out):
        out[c] = out[c].astype(float) - fee
    return out
