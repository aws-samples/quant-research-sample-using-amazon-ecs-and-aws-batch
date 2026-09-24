"""Pin the single study config: shape, validation rules, and that the
config-driven scope lists match the previously hardcoded chart package
(regression guard for the 2026-08-10 config refactor)."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import study_config


def test_config_loads_and_validates():
    cfg = study_config.load()
    assert cfg["aggregation"]["max_basket_weight"] == 0.02
    assert cfg["evaluation"]["default_source"] in cfg["evaluation"]["panel_sources"]


def test_exit_matrix_rows_match_package():
    """Exact labels + order of the chart package (Russell 2026-08-10,
    SPTM/S&P1500 2026-08-10; sector SPDRs are cell-matrix-only)."""
    rows = study_config.load()["aggregation"]["exit_matrix_rows"]
    assert [r["label"] for r in rows] == [
        "MICRO", "SMALL", "MID", "LARGE", "ALL", "EX-MICRO", "EX-LARGE",
        "MID+SMALL", "MID+LARGE", "NDX", "SPX", "NDX+SPX",
        "R1000", "R2000", "R3000", "SP1500", "SEC-7"]


def test_cell_matrices_match_package():
    """Exact keys of the standalone matrices (Russell + SPTM + 11 sector
    SPDRs added 2026-08-10)."""
    keys = [s["key"] for s in study_config.load()["aggregation"]["cell_matrices"]]
    assert keys == ["midlarge", "midsmall", "mid", "large", "exlarge", "ndx", "spx", "ndxspx",
                    "rui", "rut", "rua", "sptm",
                    "xlk", "xlf", "xlv", "xly", "xlp", "xli", "xle", "xlb",
                    "xlc", "xlre", "xlu", "sec7",
                    "px_lt1", "px_1_5", "px_5_10", "px_10_50", "px_50_100", "px_ge100",
                    "px_ge1", "px_ge5", "px_ge10", "px_ge50"]


def test_exmicro_tier_list_equivalent():
    """EX-MICRO expressed as explicit tiers must equal 'not micro'."""
    rows = {r["label"]: r for r in study_config.load()["aggregation"]["exit_matrix_rows"]}
    assert sorted(rows["EX-MICRO"]["tiers"]) == ["large", "mid", "small"]


def test_exlarge_tier_list_equivalent():
    """EX-LARGE (added 2026-09-10) expressed as explicit tiers must equal 'not large'."""
    rows = {r["label"]: r for r in study_config.load()["aggregation"]["exit_matrix_rows"]}
    assert sorted(rows["EX-LARGE"]["tiers"]) == ["micro", "mid", "small"]
    cms = {c["key"]: c for c in study_config.load()["aggregation"]["cell_matrices"]}
    assert cms["exlarge"]["tiers"] == rows["EX-LARGE"]["tiers"]


def test_scope_filter_tiers_and_passthrough():
    import pandas as pd

    class NoIndex:                      # index scopes not used in this test
        def __init__(self, *_): raise AssertionError("should not be built")

    df = pd.DataFrame({"liquidity_tier": ["micro", "mid", "large"],
                       "event_symbol": list("abc"), "trade_day": ["2025-01-03"] * 3})
    f = study_config.scope_filter({"tiers": ["mid", "large"]}, NoIndex)
    assert list(f(df)["liquidity_tier"]) == ["mid", "large"]
    f_all = study_config.scope_filter({"tiers": None}, NoIndex)
    assert len(f_all(df)) == 3


def test_validation_rejects_unknown_index():
    bad = {"evaluation": {"panel_sources": {"a": "p/"}, "default_source": "a"},
           "aggregation": {"max_basket_weight": 0.02, "index_prefixes": {},
                           "exit_matrix_rows": [{"key": "x", "label": "X",
                                                 "indices": ["NOPE"]}],
                           "cell_matrices": []}}
    with pytest.raises(ValueError, match="NOPE"):
        study_config._validate(bad)


def test_validation_rejects_tiers_plus_indices():
    bad = {"evaluation": {"panel_sources": {"a": "p/"}, "default_source": "a"},
           "aggregation": {"max_basket_weight": 0.02,
                           "index_prefixes": {"NDX": "n/"},
                           "exit_matrix_rows": [{"key": "x", "label": "X",
                                                 "tiers": ["mid"], "indices": ["NDX"]}],
                           "cell_matrices": []}}
    with pytest.raises(ValueError, match="exclusive"):
        study_config._validate(bad)


def test_price_scopes_partition_the_price_line():
    """Reporter-price scopes (2026-09-23): edges 1/5/10/50/100, left-closed,
    contiguous from 0 to +inf, labels PX<1 .. PX100+."""
    cms = [c for c in study_config.load()["aggregation"]["cell_matrices"] if c.get("price_bins")]
    edges = [c["price_bins"] for c in cms]
    assert edges[:6] == [[0, 1], [1, 5], [5, 10], [10, 50], [50, 100], [100, None]]
    assert [c["label"] for c in cms[:6]] == ["PX<1", "PX1-5", "PX5-10", "PX10-50", "PX50-100", "PX100+"]
    # cumulative "and up" universes (user request 2026-09-23): 1+, 5+, 10+, 50+
    assert edges[6:] == [[1, None], [5, None], [10, None], [50, None]]
    assert [c["label"] for c in cms[6:]] == ["PX1+", "PX5+", "PX10+", "PX50+"]


def test_scope_filter_price_bins():
    import pandas as pd

    class NoIndex:
        def __init__(self, *_): raise AssertionError("should not be built")

    df = pd.DataFrame({"reporter_entry_price": [0.5, 1.0, 4.99, 5.0, 99.0, 100.0, 2500.0],
                       "liquidity_tier": ["micro"] * 7})
    lo = study_config.scope_filter({"price_bins": [0, 1]}, NoIndex)(df)
    assert list(lo["reporter_entry_price"]) == [0.5]                     # 1.0 belongs to the next bin
    mid = study_config.scope_filter({"price_bins": [1, 5]}, NoIndex)(df)
    assert list(mid["reporter_entry_price"]) == [1.0, 4.99]
    top = study_config.scope_filter({"price_bins": [100, None]}, NoIndex)(df)
    assert list(top["reporter_entry_price"]) == [100.0, 2500.0]


def test_scope_filter_price_bins_needs_column():
    import pandas as pd
    with pytest.raises(KeyError, match="reporter_entry_price"):
        study_config.scope_filter({"price_bins": [0, 1]}, None)(pd.DataFrame({"liquidity_tier": ["mid"]}))


def test_validation_rejects_price_plus_tiers_and_bad_edges():
    import copy
    base = study_config.load()
    for bad_scope in ({"key": "z", "tiers": ["mid"], "price_bins": [0, 1]},
                      {"key": "z", "price_bins": [5, 1]},
                      {"key": "z", "price_bins": [1]}):
        bad = copy.deepcopy(base)
        bad["aggregation"]["cell_matrices"].append(bad_scope)
        with pytest.raises(ValueError):
            study_config._validate(bad)
