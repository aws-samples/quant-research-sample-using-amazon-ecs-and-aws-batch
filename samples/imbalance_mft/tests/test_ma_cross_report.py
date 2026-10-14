"""ma_cross_report: renders every section from the evaluate() tables of a tiny synthetic run; number formatting."""
import numpy as np, polars as pl
import ma_cross as mx
import ma_cross_report as mr


def _tables():
    rng = np.random.default_rng(2); spd, nd = 40, 2; N = spd * nd; cols = mx.sig_columns(windows=[3, 5, 10])
    B = rng.choice([-1, 1], size=(N, len(cols))).astype(np.int8); E = B.copy(); E[::4] *= -1; r = rng.normal(size=N).astype(np.float32)
    RC = np.stack([r + 0.2, r - 0.2, r], axis=1).astype(np.float32)
    out = {}
    for mark, hs in (("pooled", 3.0), ("primary", 2.0), ("fresh", 2.5)):
        o = mx.with_mark(mx.evaluate(B, E, r, RC, ["SOL", "XRP", "DOGE"], ["2026-09-01", "2026-09-02"], cols, secs_per_day=spd, null_workers=1), mark, hs)
        for k, F in o.items(): out.setdefault(k, []).append(F)
    return {k: pl.concat(Fs, how="diagonal_relaxed") for k, Fs in out.items()}


def test_render_names_every_section_and_formats_numbers():
    T = _tables(); plan = {"run_id": "macx-t", "dates": ["2026-09-01", "2026-09-02"], "targets": [{}] * 3, "pairs": 3, "rules": 18, "windows": [3, 5, 10], "dataset_run": "tfl4w2-20261009"}
    doc = mr.render(T, plan, {"n_secs": 80, "finished_at": "x"}, "macx-t", 10)
    assert "macx-t" in doc and "Table 2.6" in doc and "Table 3.6" in doc and "Table 4.18" in doc and "Table 5.6" in doc and "Table 6.18" in doc and "Table 7.3" in doc and "Table 8.12" in doc and "Table 9.6" in doc and "10. Reproduction" in doc and "hour of day" in doc.lower() and mr.pr.TABLE_JS.strip()[:7] in doc
    assert "Always-long basket" in doc and "imbalance MA, $100k dollar bars" in doc and "fresh seconds only" in doc and "lag 300 s" in doc and "mean half-spread of the basket 2.50 bp" in doc and "mean half-spread of the basket 3.00 bp" in doc
    assert mr._fmt("total_bp")(1.23456) == "1.23" and mr._fmt("p_value")(0.04999) == "0.050" and mr._fmt("flips_per_day")(12.34) == "12.3" and mr._fmt("x")(None) == "–" and mr._fmt("n_rules")(91) == "91"


def test_grid_table_is_fast_by_slow():
    T = _tables(); P, cols = mr.grid_table(mr.by_mark(T["rules"], "primary"), "imb_usd", "timb")
    assert P.columns == ["fast", "5", "10"] and P["fast"].to_list() == [3, 5] and cols[0] == ("fast", "fast \\ slow") and P["10"][1] is not None and P["5"][1] is None
