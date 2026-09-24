"""Single study config: evaluation panel sources + aggregation scopes/caps.

Edit configs/study_config.json to add index filters or chart scopes — the
evaluation and aggregation code iterate over the config; no code changes.
"""
import json
from functools import lru_cache
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parent / "configs" / "study_config.json"


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
            if has_tiers and has_idx:
                raise ValueError(f"{section}/{s['key']}: tiers and indices are exclusive")
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
    if indices:
        f = index_filter_cls(indices)
        return lambda d: f.filter_events(d, ticker_col="event_symbol",
                                         date_col="trade_day")
    if tiers:
        return lambda d: d[d["liquidity_tier"].isin(tiers)]
    return lambda d: d
