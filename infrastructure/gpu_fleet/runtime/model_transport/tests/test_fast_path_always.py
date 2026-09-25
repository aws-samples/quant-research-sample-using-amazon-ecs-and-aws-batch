"""The fast path is the DEFAULT, not a best-case optimisation.

WHY THIS FILE EXISTS. A real training log once read "fast download path:
skipped — only 20.9 GB; below the 20 GiB fast-path floor". The fast path is
what every download should use.

The two gates being removed here were both wrong, for the same reason: they
inferred "the fast path cannot help" from a proxy (size, card count) instead of
from what the fast path actually does.

  1. THE 20 GiB FLOOR. Measured cost of being under it: 20.9 GB in 266 s =
     79 MB/s (g6.12xlarge, 14 files). The slow path gives ONE
     STREAM PER FILE, so throughput is bounded by the single largest object —
     a 4 GB shard on one stream sets the floor no matter how many files there
     are. The fast path issues RANGES_PER_SHARD=32 concurrent byte-range GETs
     WITHIN each file, which is precisely the fix for a small file count. The
     floor excluded the case that needed it most.

  2. THE 2-ENI MINIMUM. `_get_shard` parallelises byte ranges, and
     `plan_assignment` accepts n_enis=1 (its only guard is `n_enis < 1`).
     Nothing in the range machinery needs a second card. The ENI count changes
     how much bandwidth is reachable, not whether the path works — and every
     AWS Batch-managed node has exactly one ENI, which is most of the
     fleet. `require_attribution=False` is already passed from fetch.py, and
     `_attribution`'s balance check self-disables at one card
     (`if len(hit) > 1 and worst > 0.25`), so a single card cannot trip it.

WHAT IS DELIBERATELY KEPT:
  - `include=...` filtered fetches. These are ~20 MB of small config files, and
    on multi-node FSDP every non-loader rank does exactly this. Spawning
    processes to fetch a tokenizer.json is pure overhead. This is a genuine
    statement about the WORK, not a guess about the host.
  - MT_FAST_DOWNLOAD=0. An operator kill switch has to keep working.
  - The fallback itself. fetch.py wraps download_model in try/except and
    degrades on ANY exception, so making the fast path universal cannot turn a
    slow download into a failed one. That safety net is what makes removing
    these gates safe rather than reckless.
"""
import pytest

from model_transport import fetch
from model_transport.download import plan_assignment


def _objs(n, each):
    return {f"p/shard-{i}.safetensors": each for i in range(n)}


@pytest.fixture
def one_card(monkeypatch):
    """A host with exactly one ENI — an AWS Batch node.

    Stubbed in every size test, not just the ENI test: real discover_enis()
    queries IMDS, so unstubbed these tests would assert the size logic while
    silently measuring the developer's laptop (they failed with "no IP-carrying
    network card found" on macOS, which is nothing to do with size).
    """
    monkeypatch.setattr("model_transport.download.discover_enis",
                        lambda *a, **k: [object()])


# ----------------------------------------------------- the gates that must go

def test_small_model_still_takes_the_fast_path(one_card):
    """The 20.9 GB case from the log: below the old floor, engaged now.

    This is the exact size that measured 79 MB/s on the slow path.
    """
    ok, why = fetch._fast_path_ok(None, "/tmp/m", _objs(14, 20_900_000_000 // 14))
    assert ok, f"20.9 GB was refused the fast path: {why}"


def test_a_single_large_file_takes_the_fast_path(one_card):
    """One file, one stream on the slow path — the worst case, and the one
    ranged GETs fix outright."""
    ok, why = fetch._fast_path_ok(None, "/tmp/m", {"p/model.safetensors": 4 << 30})
    assert ok, f"a single 4 GiB file was refused the fast path: {why}"


def test_tiny_model_takes_the_fast_path(one_card):
    """No floor at all. 32 ranges on a small file is harmless; the fallback
    covers anything that does go wrong."""
    ok, why = fetch._fast_path_ok(None, "/tmp/m", _objs(2, 50 << 20))
    assert ok, f"a 100 MB model was refused the fast path: {why}"


def test_single_eni_node_takes_the_fast_path(monkeypatch):
    """Every AWS Batch-managed node has ONE ENI."""
    monkeypatch.setattr("model_transport.download.discover_enis",
                        lambda *a, **k: [object()])
    ok, why = fetch._fast_path_ok(None, "/tmp/m", _objs(14, 3 << 30))
    assert ok, f"a single-ENI node was refused the fast path: {why}"


def test_zero_enis_is_still_refused(monkeypatch):
    """Not a proxy — with no IP-carrying card, download_model raises outright."""
    monkeypatch.setattr("model_transport.download.discover_enis",
                        lambda *a, **k: [])
    ok, why = fetch._fast_path_ok(None, "/tmp/m", _objs(14, 3 << 30))
    assert not ok
    assert "0" in why or "no" in why.lower()


# ------------------------------------------------------- what must NOT change

def test_filtered_fetch_still_skips():
    """include=... is a statement about the work, not about the host."""
    ok, why = fetch._fast_path_ok(["*.json"], "/tmp/m", _objs(3, 1 << 20))
    assert not ok
    assert "filtered" in why


def test_kill_switch_still_works(monkeypatch):
    monkeypatch.setenv("MT_FAST_DOWNLOAD", "0")
    ok, why = fetch._fast_path_ok(None, "/tmp/m", _objs(14, 3 << 30))
    assert not ok
    assert "MT_FAST_DOWNLOAD" in why


def test_the_reason_is_always_logged(caplog):
    """The silent non-engagement is the failure mode this whole module exists
    to prevent, so the decision must always be visible."""
    import logging
    with caplog.at_level(logging.INFO, logger="model_transport.fetch"):
        fetch._fast_path_ok(["*.json"], "/tmp/m", _objs(3, 1 << 20))
    # _fast_path_ok returns the reason; fetch_model logs it. Assert it is
    # non-empty so a future refactor cannot return a bare bool.
    _, why = fetch._fast_path_ok(["*.json"], "/tmp/m", _objs(3, 1 << 20))
    assert why and isinstance(why, str)


# ------------------------------------ the planner genuinely supports one card

def test_plan_assignment_handles_one_eni():
    """The claim that makes removing the 2-ENI gate safe."""
    shards = [(f"s{i}", (i + 1) << 30) for i in range(5)]
    plan, per_eni = plan_assignment(shards, 1)
    assert len(plan) == 1 and len(per_eni) == 1
    assert sum(len(b) for b in plan[0]) == 5, "shards lost with a single ENI"
    flat = [it for b in plan[0] for it in b]
    assert sorted(flat) == sorted(shards)


def test_plan_assignment_still_rejects_zero_enis():
    with pytest.raises(ValueError):
        plan_assignment([("s0", 1 << 30)], 0)
