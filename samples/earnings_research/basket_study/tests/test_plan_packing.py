"""Batch task sizing rule (user, 2026-10-12): every array child must run >= 1
minute and ideally <= 10, so `plan` packs several events per child instead of
one event per array index. The manifest carries `per_child`; child i evaluates
panels[i*per_child:(i+1)*per_child] and still writes ONE shard per event. A
panel that raises becomes an error shard (status 'error', skip_reason = repr)
and the child carries on with the rest of its slice.
"""
import io
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import evaluate  # noqa: E402
from _fixtures import require  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "panel_1203531964_subset.parquet"


class TestPackPanels:
    def test_slices_are_contiguous_and_cover_everything(self):
        keys = [f"k{i}" for i in range(7)]
        assert evaluate.pack_panels(keys, 3) == [["k0", "k1", "k2"], ["k3", "k4", "k5"], ["k6"]]
        assert evaluate.pack_panels(keys, 1) == [[k] for k in keys]
        assert evaluate.pack_panels([], 3) == []

    def test_child_slice(self):
        panels = [f"k{i}" for i in range(7)]
        assert evaluate.child_panels(panels, 3, 0) == ["k0", "k1", "k2"]
        assert evaluate.child_panels(panels, 3, 2) == ["k6"]
        with pytest.raises(SystemExit):
            evaluate.child_panels(panels, 3, 3)

    def test_per_child_must_be_positive(self):
        with pytest.raises(ValueError):
            evaluate.pack_panels(["a"], 0)


class _FakeS3IO:
    def __init__(self, *a, **k):
        self.texts, self.parquets = {}, {}

    def write_text(self, text, uri):
        self.texts[uri] = text

    def write_parquet(self, df, uri):
        self.parquets[uri] = df

    def read_bytes(self, uri):
        return self.texts[uri].encode()


class TestPlanPacking:
    def _args(self, per_child, s3_prefix="earnings-basket-study/results-test"):
        return SimpleNamespace(profile=None, dates=None, s3_prefix=s3_prefix, no_charts=True,
                               constructions=None, peer_source=None, dry_run=True, aggregate=False,
                               events_per_child=per_child)

    def test_manifest_carries_per_child_and_array_size_is_children(self, monkeypatch, capsys):
        keys = [f"panels/date=2026-07-21/event_{i}.parquet" for i in range(45)]
        fake = _FakeS3IO()
        monkeypatch.setattr(evaluate, "list_panels", lambda s3, dates: keys)
        monkeypatch.setattr(evaluate, "_session", lambda p: MagicMock())
        monkeypatch.setattr("s3io.S3IO", lambda *a, **k: fake)
        rc = evaluate.cmd_plan(self._args(per_child=20))
        assert rc == 0
        out = json.loads(capsys.readouterr().out)
        assert out["event_count"] == 45 and out["per_child"] == 20 and out["children"] == 3
        m = json.loads(list(fake.texts.values())[0])
        assert m["per_child"] == 20 and len(m["panels"]) == 45
        assert "_45_" in list(fake.texts)[0]

    def test_manifest_chunking_counts_children_not_panels(self, monkeypatch, capsys):
        # 10,000 children per Batch array -> 25,000 panels at 5/child = 5,000 children = ONE manifest
        keys = [f"panels/date=2026-07-21/event_{i}.parquet" for i in range(25_000)]
        fake = _FakeS3IO()
        monkeypatch.setattr(evaluate, "list_panels", lambda s3, dates: keys)
        monkeypatch.setattr(evaluate, "_session", lambda p: MagicMock())
        monkeypatch.setattr("s3io.S3IO", lambda *a, **k: fake)
        evaluate.cmd_plan(self._args(per_child=5))
        out = json.loads(capsys.readouterr().out)
        assert len(out["manifests"]) == 1 and out["children"] == 5_000
        # 10,001 children -> two manifests
        keys2 = [f"panels/date=2026-07-21/event_{i}.parquet" for i in range(50_005)]
        monkeypatch.setattr(evaluate, "list_panels", lambda s3, dates: keys2)
        evaluate.cmd_plan(self._args(per_child=5))
        out = json.loads(capsys.readouterr().out)
        assert len(out["manifests"]) == 2 and out["children"] == 10_001
        assert sum(len(json.loads(fake.texts[k])["panels"]) for k in fake.texts if "_50005_" in k) == 50_005

    def test_default_is_the_sized_pack_not_one_per_child(self):
        assert evaluate.EVENTS_PER_CHILD >= 10


class TestEvalEventPacked:
    def _run(self, monkeypatch, tmp_path, idx, per_child, panels, failing=()):
        fake = _FakeS3IO()
        fake.texts[f"s3://{evaluate.BUCKET}/m.json"] = json.dumps(
            {"panels": panels, "s3_prefix": "p/x", "charts": False, "constructions": None,
             "peer_source": None, "per_child": per_child})
        monkeypatch.setattr("s3io.S3IO", lambda *a, **k: fake)
        monkeypatch.setenv("AWS_BATCH_JOB_ARRAY_INDEX", str(idx))
        raw = require(FIXTURE).read_bytes()
        seen = []                                   # panel keys the child actually fetched, in order

        def get_object(Bucket, Key):
            seen.append(Key)
            return {"Body": io.BytesIO(raw)}
        s3 = MagicMock()
        s3.get_object = get_object
        monkeypatch.setattr(evaluate, "_session", lambda p: MagicMock(client=lambda *a, **k: s3))

        def fake_process(panel, s3io, report_dir, s3_prefix, charts, constructions=None, peer_source=None):
            key = seen[-1]
            if key in failing:
                raise RuntimeError("boom")
            eid = int(key.rsplit("event_", 1)[1].split(".")[0])
            return pd.DataFrame([{"event_id": eid, "status": "ok", "pnl": 0.01}]).reindex(columns=evaluate.CSV_COLUMNS)
        monkeypatch.setattr(evaluate, "process_panel", fake_process)
        args = SimpleNamespace(profile=None, manifest="m.json", report_dir=str(tmp_path))
        rc = evaluate.cmd_eval_event(args)
        return rc, fake, seen

    def test_child_evaluates_its_slice_and_writes_one_shard_per_event(self, monkeypatch, tmp_path):
        panels = [f"panels/date=2026-07-21/event_{i}.parquet" for i in range(100, 107)]
        rc, fake, seen = self._run(monkeypatch, tmp_path, idx=1, per_child=3, panels=panels)
        assert rc == 0
        assert seen == panels[3:6]
        assert sorted(fake.parquets) == [f"s3://{evaluate.BUCKET}/p/x/shards/event_{i}.parquet" for i in (103, 104, 105)]

    def test_last_child_takes_the_remainder(self, monkeypatch, tmp_path):
        panels = [f"panels/date=2026-07-21/event_{i}.parquet" for i in range(100, 107)]
        rc, fake, seen = self._run(monkeypatch, tmp_path, idx=2, per_child=3, panels=panels)
        assert seen == panels[6:] and len(fake.parquets) == 1

    def test_failing_event_becomes_an_error_shard_and_the_child_continues(self, monkeypatch, tmp_path):
        panels = [f"panels/date=2026-07-21/event_{i}.parquet" for i in range(100, 103)]
        rc, fake, seen = self._run(monkeypatch, tmp_path, idx=0, per_child=3, panels=panels, failing={panels[1]})
        assert rc == 0 and seen == panels
        err = fake.parquets[f"s3://{evaluate.BUCKET}/p/x/shards/event_101.parquet"].to_pandas()
        assert err["status"].tolist() == ["error"] and "boom" in err["skip_reason"].iloc[0]
        assert int(err["event_id"].iloc[0]) == 101
        assert len(fake.parquets) == 3

    def test_legacy_manifest_without_per_child_is_one_event_per_index(self, monkeypatch, tmp_path):
        panels = [f"panels/date=2026-07-21/event_{i}.parquet" for i in range(100, 103)]
        fake = _FakeS3IO()
        fake.texts[f"s3://{evaluate.BUCKET}/m.json"] = json.dumps(
            {"panels": panels, "s3_prefix": "p/x", "charts": False})
        monkeypatch.setattr("s3io.S3IO", lambda *a, **k: fake)
        monkeypatch.setenv("AWS_BATCH_JOB_ARRAY_INDEX", "2")
        raw = require(FIXTURE).read_bytes()
        s3 = MagicMock(); s3.get_object = lambda Bucket, Key: {"Body": io.BytesIO(raw)}
        monkeypatch.setattr(evaluate, "_session", lambda p: MagicMock(client=lambda *a, **k: s3))
        monkeypatch.setattr(evaluate, "process_panel", lambda *a, **k: pd.DataFrame(
            [{"event_id": 102, "status": "ok"}]).reindex(columns=evaluate.CSV_COLUMNS))
        rc = evaluate.cmd_eval_event(SimpleNamespace(profile=None, manifest="m.json", report_dir=str(tmp_path)))
        assert rc == 0 and list(fake.parquets) == [f"s3://{evaluate.BUCKET}/p/x/shards/event_102.parquet"]


class TestPanelReadRetries:
    """S3 SlowDown on GetObject (four waves x ~3,000 children on one panel prefix, 2026-10-12):
    the child backs off and retries instead of writing an error shard after boto's 4 tries."""

    def _err(self, code):
        from botocore.exceptions import ClientError
        return ClientError({"Error": {"Code": code, "Message": "x"}}, "GetObject")

    def test_retries_slowdown_with_backoff(self, monkeypatch):
        calls, sleeps = [], []
        raw = require(FIXTURE).read_bytes()

        def get_object(Bucket, Key):
            calls.append(Key)
            if len(calls) <= 2:
                raise self._err("SlowDown")
            return {"Body": io.BytesIO(raw)}
        s3 = MagicMock(); s3.get_object = get_object
        monkeypatch.setattr(evaluate.time, "sleep", lambda s: sleeps.append(s))
        panel = evaluate.read_panel(s3, "panels/date=2026-07-21/event_1.parquet")
        assert len(panel) > 0 and len(calls) == 3 and len(sleeps) == 2 and sleeps[1] > sleeps[0] > 0

    def test_gives_up_after_the_attempt_budget(self, monkeypatch):
        from botocore.exceptions import ClientError
        s3 = MagicMock(); s3.get_object = lambda Bucket, Key: (_ for _ in ()).throw(self._err("SlowDown"))
        monkeypatch.setattr(evaluate.time, "sleep", lambda s: None)
        with pytest.raises(ClientError):
            evaluate.read_panel(s3, "k", attempts=3)

    def test_other_client_errors_are_not_retried(self, monkeypatch):
        from botocore.exceptions import ClientError
        n = []

        def get_object(Bucket, Key):
            n.append(1); raise self._err("NoSuchKey")
        s3 = MagicMock(); s3.get_object = get_object
        monkeypatch.setattr(evaluate.time, "sleep", lambda s: None)
        with pytest.raises(ClientError):
            evaluate.read_panel(s3, "k")
        assert len(n) == 1


class TestPlanOnlyErrors:
    def test_only_error_events_are_planned(self, monkeypatch, capsys):
        import polars as pl
        keys = [f"panels/date=2026-07-21/event_{i}.parquet" for i in range(100, 110)]
        table = pl.DataFrame({"event_id": [100, 101, 102, 103, 103], "status": ["ok", "error", "skip", "error", "ok"]})
        fake = _FakeS3IO()
        fake.read_parquet = lambda uri: table
        monkeypatch.setattr(evaluate, "list_panels", lambda s3, dates: keys)
        monkeypatch.setattr(evaluate, "_session", lambda p: MagicMock())
        monkeypatch.setattr("s3io.S3IO", lambda *a, **k: fake)
        args = SimpleNamespace(profile=None, dates=None, s3_prefix="earnings-basket-study/results-test", no_charts=True,
                               constructions=None, peer_source=None, dry_run=True, aggregate=False,
                               events_per_child=5, only_errors=True)
        evaluate.cmd_plan(args)
        out = json.loads(capsys.readouterr().out)
        assert out["event_count"] == 2 and out["children"] == 1
        m = json.loads(list(fake.texts.values())[0])
        assert m["panels"] == [keys[1], keys[3]]
