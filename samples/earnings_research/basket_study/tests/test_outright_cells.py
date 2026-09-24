"""Unhedged reporter_only|none cell in the producers (README decision #7).

POOLED must stay the mean of the 4 x 4 hedged grid; the outright cell is its
own row; load_ok(extra=) unions a second shard table restricted to the study's
event set.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import finalize_local  # noqa: E402
import portfolio_analysis as pa  # noqa: E402

HEDGED = [(c, u) for c in pa.CONS for u in pa.UNIS]


def _rows(day, pnl_hedged, pnl_outright):
    rows = [{"event_id": 1, "trade_day": day, "construction": c, "universe": u, "pnl": pnl_hedged}
            for c, u in HEDGED]
    rows.append({"event_id": 1, "trade_day": day, "construction": "reporter_only", "universe": "none",
                 "pnl": pnl_outright})
    return pd.DataFrame(rows)


def test_cells_end_with_extra_and_pooled_excludes_it():
    cs = list(pa.cells())
    assert cs[25] == ("reporter_only", "none", ["reporter_only"], ["none"])
    pooled = next(x for x in cs if x[0] == "POOLED" and x[1] == "POOLED")
    assert "reporter_only" not in pooled[2] and "none" not in pooled[3]
    assert len(cs) == 25 + len(pa.EXTRA_CELLS) and len(pa.EXTRA_CELLS) == 4
    assert [x[:2] for x in cs[-3:]] == [("spy_beta_levels_neutral", "SPY"), ("spy_beta_returns_neutral", "SPY"),
                                        ("spy_dollar_neutral", "SPY")]
    assert "SPY" not in pooled[3]


def test_cell_masks_keep_outright_out_of_pooled():
    sub = _rows("2026-06-10", 0.01, -0.05)
    masks = {(c, u): m for c, u, m in pa.cell_masks(sub["construction"].to_numpy(), sub["universe"].to_numpy())}
    assert masks[("POOLED", "POOLED")].sum() == 16
    assert masks[("POOLED", "all")].sum() == 4
    assert masks[("reporter_only", "none")].sum() == 1
    assert not masks[("POOLED", "POOLED")][sub["construction"].eq("reporter_only").to_numpy()].any()


def test_panels_and_hist_report_outright_separately():
    sub = _rows("2026-06-10", 0.01, -0.05)
    daily = pa.daily_multi(sub, ["pnl"])["pnl"]
    got = {(c, u): float(s.iloc[0]) for c, u, s in pa.panels(daily)}
    cap = pa.aggregate.MAX_BASKET_WEIGHT
    assert got[("POOLED", "POOLED")] == pytest.approx(0.01 * cap)
    assert got[("reporter_only", "none")] == pytest.approx(-0.05 * cap)
    hist = pd.DataFrame(pa.trade_hist_rows(sub, ["pnl"], "ALL"))
    pooled = hist[(hist.construction == "POOLED") & (hist.universe == "POOLED")].iloc[0]
    out = hist[hist.construction == "reporter_only"].iloc[0]
    assert pooled["n_trades"] == 16 and pooled["mean_bps"] == pytest.approx(100.0)
    assert out["universe"] == "none" and out["n_trades"] == 1 and out["mean_bps"] == pytest.approx(-500.0)


def test_load_ok_unions_extra_within_event_set(tmp_path, monkeypatch):
    monkeypatch.setattr(finalize_local, "attach_reporter_price", lambda ok, s3io, p: ok)
    main = pd.DataFrame({"event_id": [1, 1, 2, 4], "trade_day": ["2026-06-10"] * 3 + ["2026-06-12"],
                         "construction": ["equal_weight_dollar_neutral"] * 4, "universe": ["all"] * 4,
                         "status": ["ok", "ok", "skip", "skip"], "pnl": [0.01, 0.02, None, None],
                         "pnl_0935": [0.0, 0.0, None, None]})
    # event 4 is in the study but its day has NO hedged trade -> its outright row must be dropped
    extra = pd.DataFrame({"event_id": [1, 2, 3, 4], "trade_day": ["2026-06-10"] * 3 + ["2026-06-12"],
                          "construction": ["reporter_only"] * 4, "universe": ["none"] * 4,
                          "status": ["ok"] * 4, "pnl": [-0.05, -0.06, -0.07, -0.08], "pnl_0935": [0.0] * 4})
    mp, xp = tmp_path / "main.parquet", tmp_path / "extra.parquet"
    main.to_parquet(mp, index=False)
    extra.to_parquet(xp, index=False)

    class S3:
        pl_opts = {}
    ok = finalize_local.load_ok(S3(), "x", cache=str(mp), extra=[str(xp)])
    out = ok[ok.construction == "reporter_only"]
    # event 2 is in the study (skip row in main) -> admitted; event 3 is not -> dropped;
    # event 4 falls on a day without a hedged trade -> dropped (trade-day index must not grow)
    assert sorted(out.event_id.tolist()) == [1, 2]
    assert (ok.status == "ok").all() and len(ok) == 4
    assert sorted(ok.trade_day.unique()) == ["2026-06-10"]
    assert ok["pnl"].dtype == float


def test_n_events_by_cell_keeps_grid_count_hedged_only():
    sub = pd.concat([_rows("2026-06-10", 0.01, -0.05),
                     pd.DataFrame([{"event_id": 9, "trade_day": "2026-06-10", "construction": "reporter_only",
                                    "universe": "none", "pnl": 0.0},
                                   {"event_id": 9, "trade_day": "2026-06-10", "construction": "spy_dollar_neutral",
                                    "universe": "SPY", "pnl": 0.0}])], ignore_index=True)
    n_hedged, n_extra = pa.n_events_by_cell(sub)
    assert n_hedged == 1                      # event 9 has no hedged row -> not counted for the grid
    assert n_extra == {"reporter_only": 2, "spy_dollar_neutral": 1}
