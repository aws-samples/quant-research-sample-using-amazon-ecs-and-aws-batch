"""The image's entry router: known routes dispatch, anything else is exec'd as given."""
import json
import pathlib
import sys
import types

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import gpu_fleet_entry as entry  # noqa: E402


def test_help_and_empty_argv(capsys):
    assert entry.main(["--help"]) == 0
    assert "bench_gpu" in capsys.readouterr().out
    assert entry.main([]) == 2


def test_unknown_route_is_execd(monkeypatch):
    seen = []
    monkeypatch.setattr(entry.os, "execvp", lambda f, a: seen.append((f, a)))
    entry.main(["python", "train.py", "--x"])
    assert seen == [("python", ["python", "train.py", "--x"])]


def test_bench_route_passes_its_argv(monkeypatch):
    fake = types.SimpleNamespace(main=lambda argv: 7 if argv == ["--skip", "compute"] else 1)
    monkeypatch.setitem(sys.modules, "bench_gpu", fake)
    assert entry.main(["bench_gpu", "--skip", "compute"]) == 7


def test_stage_needs_a_bucket(monkeypatch):
    monkeypatch.delenv("GPU_FLEET_WEIGHT_BUCKET", raising=False)
    pytest.importorskip("model_transport")
    with pytest.raises(SystemExit):
        entry.main(["stage_model", "org/repo", "name"])


def test_fetch_prints_the_report(monkeypatch, capsys):
    mt = pytest.importorskip("model_transport")
    calls = {}

    def fake_fetch(uri, local, **kw):
        calls.update(uri=uri, local=local, **kw)
        return mt.FetchReport(bucket="b", files=1, bytes=2 ** 30, seconds=1.0, local_dir=local)

    monkeypatch.setattr(mt, "fetch_model", fake_fetch)
    assert entry.main(["fetch_model", "s3://b/base_models/m", "/tmp/m", "--limit-gib", "2"]) == 0
    assert calls["limit_bytes"] == 2 * 2 ** 30 and calls["force_source"] is False
    out = capsys.readouterr().out
    assert json.loads(out[out.index("{"):])["bucket"] == "b"
