"""evaluate.py produce: the image's entrypoint runs any whitelisted producer
with the rest of argv, strict shard loading on (2026-10-09)."""
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import evaluate  # noqa: E402


def test_produce_rejects_unknown():
    with pytest.raises(SystemExit):
        evaluate.cmd_produce(["not_a_producer", "--write"])


def test_produce_rehomes_argv_and_sets_strict(monkeypatch):
    seen = {}
    fake = types.ModuleType("portfolio_analysis")

    def main():
        seen["argv"] = list(sys.argv)
        seen["strict"] = os.environ.get("LOAD_OK_STRICT")
        return None
    fake.main = main
    monkeypatch.setitem(sys.modules, "portfolio_analysis", fake)
    monkeypatch.delenv("LOAD_OK_STRICT", raising=False)
    monkeypatch.setattr(evaluate, "_code_version", lambda: "test")
    rc = evaluate.cmd_produce(["portfolio_analysis", "--write", "--max-basket-weight", "0.07"])
    assert rc == 0
    assert seen["argv"] == ["portfolio_analysis.py", "--write", "--max-basket-weight", "0.07"]
    assert seen["strict"] == "1"


def test_submit_plan_covers_every_cap_and_fee():
    sp = pytest.importorskip("submit_producers", reason="submit_producers.py is not part of the sample")
    jobs = sp.plan(sp.CAPS_PCT, sp.FEES, ["portfolio_analysis", "excursion_stats", "stop_sim"])
    assert len(jobs) == 16 * 2 + 2 + 2
    names = {j["name"] for j in jobs}
    assert "basket-produce-pa-cap7-fee0m" in names and "basket-produce-pa-cap14-fee5m" in names
    pa2 = next(j for j in jobs if j["name"] == "basket-produce-pa-cap2-fee0m")["command"]
    assert "--verify" in pa2 and "--max-basket-weight" not in pa2
    pa2f = next(j for j in jobs if j["name"] == "basket-produce-pa-cap2-fee5m")["command"]
    assert "--verify" not in pa2f and "--fee-per-share" in pa2f


def test_submit_plan_has_one_corr_vs_llm_job():
    """README decision #10 report: one Batch job computes the corr-vs-LLM comparison for every fee."""
    sp = pytest.importorskip("submit_producers", reason="submit_producers.py is not part of the sample")
    jobs = sp.plan([2], [0.0, 0.005], ["corr_vs_llm"])
    assert len(jobs) == 1 and jobs[0]["name"] == "basket-produce-corr-vs-llm"
    assert jobs[0]["command"][:3] == ["produce", "corr_vs_llm", "--write"]
    assert "--fees" in jobs[0]["command"] and "0,0.005" in jobs[0]["command"]


def test_submit_plan_h5_study_points_every_producer_at_the_h5_tables():
    """--study h5 (holding periods, 2026-10-12): one h5 table serves gross and fees (no v2 cache),
    no --verify on the fresh prefix, corr_vs_llm paired per hold, holding_periods job present."""
    sp = pytest.importorskip("submit_producers", reason="submit_producers.py is not part of the sample")
    jobs = sp.plan([2, 5], [0.0, 0.005], ["portfolio_analysis", "corr_vs_llm", "holding_periods"], study="h5")
    by = {j["name"]: j["command"] for j in jobs}
    assert len(jobs) == 4 + 1 + 1
    pa = by["basket-produce-pa-cap2-fee5m-h5"]
    assert "--s3-prefix" in pa and pa[pa.index("--s3-prefix") + 1] == "earnings-basket-study/results-10y-h5"
    assert "--cache" not in pa and "--verify" not in by["basket-produce-pa-cap2-fee0m-h5"]
    assert all("results-10y-" in x and x.endswith("-h5/shards_consolidated/shards.parquet") for x in pa[pa.index("--extra-shards") + 1::2] if x.startswith("s3://"))
    cv = by["basket-produce-corr-vs-llm-h5"]
    assert cv[cv.index("--exits") + 1].split(",")[-4:] == ["pnl_d2", "pnl_d3", "pnl_d4", "pnl_d5"]
    assert cv[cv.index("--llm-prefix") + 1] == "earnings-basket-study/results-10y-h5"
    assert "results-10y-h5/shards_consolidated" in cv[cv.index("--v2-matched") + 1]
    hp = by["basket-produce-holding-periods-h5"]
    assert hp[:3] == ["produce", "holding_periods", "--write"] and hp.count("--extra-shards") == 3
    # the v1 plan is unchanged
    assert sp.plan([2], [0.0], ["portfolio_analysis"])[0]["command"][-1].endswith("--verify") or "--verify" in sp.plan([2], [0.0], ["portfolio_analysis"])[0]["command"]


def test_submit_plan_h5_runs_the_viewer_side_producers_on_the_h5_tables():
    """The /study10y viewer also reads utilization_buckets.parquet and adv_cap{,_oos}.parquet
    (laptop runs of 2026-09-28/29 on v1, never on Batch). On the h5 base those panels were
    empty (2026-10-12): the producers must run there, reading the h5 LLM table (carries `legs`)
    and the SPY h5 table instead of their v1 constants; adv_cap_oos needs adv_cap's output."""
    sp = pytest.importorskip("submit_producers", reason="submit_producers.py is not part of the sample")
    jobs = sp.plan([2], [0.0, 0.005], ["utilization_buckets", "adv_cap", "adv_cap_oos"], study="h5")
    by = {j["name"]: j for j in jobs}
    assert len(jobs) == 3                                   # fee-independent: one job each, not per fee
    ub = by["basket-produce-utilization-buckets-h5"]["command"]
    assert ub[:3] == ["produce", "utilization_buckets", "--write"]
    assert ub[ub.index("--s3-prefix") + 1] == "earnings-basket-study/results-10y-h5"
    assert ub[ub.index("--peers") + 1] == "earnings-basket-study/results-10y-h5/shards_consolidated/shards.parquet"
    assert ub[ub.index("--spy") + 1] == "earnings-basket-study/results-10y-spy-h5/shards_consolidated/shards.parquet"
    ac = by["basket-produce-adv-cap-h5"]["command"]
    assert ac[:3] == ["produce", "adv_cap", "--write"] and "--peers" in ac and "--spy" in ac
    oos = by["basket-produce-adv-cap-oos-h5"]
    assert oos["command"][:3] == ["produce", "adv_cap_oos", "--write"] and "--peers" not in oos["command"]
    assert oos["depends_on"] == ["basket-produce-adv-cap-h5"]
    # v1: same producers, the constants stay in charge (no --peers/--spy), no suffix
    v1 = {j["name"]: j for j in sp.plan([2], [0.0], ["utilization_buckets", "adv_cap", "adv_cap_oos"])}
    assert set(v1) == {"basket-produce-utilization-buckets", "basket-produce-adv-cap", "basket-produce-adv-cap-oos"}
    assert all("--peers" not in j["command"] for j in v1.values())
    assert v1["basket-produce-adv-cap-oos"]["depends_on"] == ["basket-produce-adv-cap"]
    # the default producer wave is unchanged
    assert len(sp.plan([2], [0.0], ["portfolio_analysis"])) == 1


def test_submit_resolves_depends_on_to_batch_job_ids():
    sp = pytest.importorskip("submit_producers", reason="submit_producers.py is not part of the sample")

    class FakeBatch:
        def __init__(self): self.calls = []
        def submit_job(self, **kw):
            self.calls.append(kw); return {"jobId": f"id-{len(self.calls)}"}

    fb = FakeBatch()
    out = sp.submit([{"name": "a", "command": ["x"]}, {"name": "b", "command": ["y"], "depends_on": ["a"]}], None, batch=fb)
    assert "dependsOn" not in fb.calls[0]
    assert fb.calls[1]["dependsOn"] == [{"jobId": "id-1"}]
    assert [j["job_id"] for j in out] == ["id-1", "id-2"]
