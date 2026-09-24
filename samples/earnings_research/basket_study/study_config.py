"""Single study config: evaluation panel sources + aggregation scopes/caps.

Edit configs/study_config.json to add index filters or chart scopes — the
evaluation and aggregation code iterate over the config; no code changes.
"""
import json
from functools import lru_cache
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parent / "configs" / "study_config.json"
# Reporter 9:30 entry fill (the strategy's own entry), attached to the ok frame by
# finalize_local.attach_reporter_price; price_bins scopes are left-closed [lo, hi).
PRICE_COL = "reporter_entry_price"


@lru_cache(maxsize=1)
def load() -> dict:
    cfg = json.loads(CONFIG_PATH.read_text())
    _validate(cfg)
    return cfg


def _validate(cfg: dict) -> None:
    agg = cfg["aggregation"]
    prefixes = agg["index_prefixes"]
    for section in ("exit_matrix_rows", "cell_matrices"):
        keys = [s["key"] for s in agg[section]]
        if len(keys) != len(set(keys)):
            raise ValueError(f"duplicate keys in {section}: {keys}")
        for s in agg[section]:
            has_tiers = s.get("tiers") is not None
            has_idx = bool(s.get("indices"))
            has_px = s.get("price_bins") is not None
            if sum([has_tiers, has_idx, has_px]) > 1:
                raise ValueError(f"{section}/{s['key']}: tiers, indices and price_bins are exclusive")
            if has_px:
                pb = s["price_bins"]
                if (not isinstance(pb, list) or len(pb) != 2 or pb[0] is None
                        or (pb[1] is not None and not pb[1] > pb[0])):
                    raise ValueError(f"{section}/{s['key']}: price_bins must be [lo, hi] with hi > lo "
                                     f"(hi null = open-ended), got {pb!r}")
            for idx in s.get("indices") or []:
                if idx not in prefixes:
                    raise ValueError(
                        f"{section}/{s['key']}: index '{idx}' missing from index_prefixes")
    if not 0 < agg["max_basket_weight"] <= 1:
        raise ValueError("max_basket_weight must be in (0, 1]")
    ev = cfg["evaluation"]
    if ev["default_source"] not in ev["panel_sources"]:
        raise ValueError("default_source missing from panel_sources")


def scope_filter(scope: dict, index_filter_cls):
    """Build a DataFrame->DataFrame filter for one scope entry (tiers OR
    indices OR passthrough). index_filter_cls is injected to avoid a hard
    import cycle."""
    tiers = scope.get("tiers")
    indices = scope.get("indices")
    price_bins = scope.get("price_bins")
    if price_bins is not None:
        lo, hi = price_bins

        def f_price(d):
            if PRICE_COL not in d.columns:
                raise KeyError(f"{PRICE_COL} missing — attach the reporter 9:30 entry price "
                               "(finalize_local.attach_reporter_price) before price scopes")
            px = d[PRICE_COL].astype(float)
            m = px >= lo
            if hi is not None:
                m &= px < hi
            return d[m]
        return f_price
    if indices:
        f = index_filter_cls(indices)
        return lambda d: f.filter_events(d, ticker_col="event_symbol",
                                         date_col="trade_day")
    if tiers:
        return lambda d: d[d["liquidity_tier"].isin(tiers)]
    return lambda d: d
