"""Unit tests for model_transport against moto-mocked S3 (no AWS needed)."""
import os

import boto3
import pytest
from moto import mock_aws

from model_transport import resolve
from model_transport import fetch as fetch_mod
from model_transport import (
    fetch_model,
    list_model_objects,
    replica_is_complete,
    resolve_bucket,
)

SRC = "modelbucket"
PREFIX = "base_models/tiny-model"
REGION = "ap-northeast-1"
REPLICA = f"{SRC}-{REGION}"
FILES = {
    f"{PREFIX}/config.json": b'{"hidden_size": 8}',
    f"{PREFIX}/model-00001-of-00002.safetensors": b"A" * 1024,
    f"{PREFIX}/model-00002-of-00002.safetensors": b"B" * 2048,
    f"{PREFIX}/sub/tokenizer.json": b'{"v": 1}',
}


@pytest.fixture
def s3_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=SRC)
        for key, body in FILES.items():
            s3.put_object(Bucket=SRC, Key=key, Body=body)
        yield s3


def make_replica(complete=True):
    s3r = boto3.client("s3", region_name=REGION)
    s3r.create_bucket(
        Bucket=REPLICA,
        CreateBucketConfiguration={"LocationConstraint": REGION},
    )
    keys = list(FILES) if complete else list(FILES)[:2]
    for key in keys:
        s3r.put_object(Bucket=REPLICA, Key=key, Body=FILES[key])
    return s3r


def test_list_scopes_to_exact_prefix(s3_env):
    s3_env.put_object(Bucket=SRC, Key="base_models/tiny-model-2/config.json", Body=b"x")
    objs = list_model_objects(s3_env, SRC, PREFIX)
    assert set(objs) == set(FILES)


def test_replica_complete_and_incomplete():
    src = {"a": 1, "b": 2}
    assert replica_is_complete(src, {"a": 1, "b": 2})
    assert replica_is_complete(src, {"a": 1, "b": 2, "extra": 9})
    assert not replica_is_complete(src, {"a": 1})           # missing file
    assert not replica_is_complete(src, {"a": 1, "b": 999})  # size mismatch
    assert not replica_is_complete({}, {})                   # empty source


def test_resolve_prefers_complete_replica(s3_env):
    make_replica(complete=True)
    bucket, region, objs = resolve_bucket(SRC, PREFIX, region=REGION)
    assert bucket == REPLICA
    assert region == REGION
    assert set(objs) == set(FILES)


def test_resolve_rejects_incomplete_replica(s3_env):
    make_replica(complete=False)
    bucket, region, _ = resolve_bucket(SRC, PREFIX, region=REGION)
    assert bucket == SRC
    assert region is None


def test_resolve_missing_replica_falls_back(s3_env):
    bucket, region, _ = resolve_bucket(SRC, PREFIX, region="eu-north-1")
    assert bucket == SRC


def test_resolve_no_region_uses_source(s3_env):
    bucket, region, _ = resolve_bucket(SRC, PREFIX, region="")
    assert bucket == SRC


def test_fetch_downloads_all_files(s3_env, tmp_path):
    report = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path), region="")
    assert report.files == len(FILES)
    assert report.bucket == SRC
    for key, body in FILES.items():
        rel = key[len(PREFIX):].lstrip("/")
        assert (tmp_path / rel).read_bytes() == body


def test_fetch_uses_replica_when_available(s3_env, tmp_path):
    make_replica(complete=True)
    report = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path), region=REGION)
    assert report.bucket == REPLICA
    assert report.files == len(FILES)


def test_fetch_empty_prefix_raises(s3_env, tmp_path):
    with pytest.raises(FileNotFoundError):
        fetch_model(f"s3://{SRC}/base_models/nonexistent", str(tmp_path), region="")


def test_fetch_rejects_non_s3_uri(tmp_path):
    with pytest.raises(ValueError):
        fetch_model("/local/path", str(tmp_path))


def test_fetch_respects_max_streams(s3_env, tmp_path):
    report = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path),
                         max_streams=1, region="")
    assert report.files == len(FILES)


def test_fetch_include_skips_weights(s3_env, tmp_path):
    """The multi-node FSDP case: skeleton-only fetch on non-loader nodes."""
    report = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path), region="",
                         include={"config.json", "sub/tokenizer.json"})
    assert report.files == 2
    assert report.bytes == len(FILES[f"{PREFIX}/config.json"]) + \
        len(FILES[f"{PREFIX}/sub/tokenizer.json"])
    assert (tmp_path / "config.json").exists()
    assert (tmp_path / "sub" / "tokenizer.json").exists()
    assert not (tmp_path / "model-00001-of-00002.safetensors").exists()


def test_fetch_include_glob_and_unmatched_patterns(s3_env, tmp_path):
    """Globs work, and patterns matching nothing are tolerated."""
    s3_env.put_object(Bucket=SRC, Key=f"{PREFIX}/modeling_tiny.py", Body=b"x=1")
    report = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path), region="",
                         include=["*.py", "config.json", "never_shipped.model"])
    assert report.files == 2
    assert (tmp_path / "modeling_tiny.py").read_bytes() == b"x=1"


def test_fetch_include_matching_nothing_raises(s3_env, tmp_path):
    with pytest.raises(FileNotFoundError):
        fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path), region="",
                    include=["no_such_file.json"])


def test_fetch_skip_existing_resumes_partial_download(s3_env, tmp_path):
    """The retry case: a killed 1.5TB fetch must not restart from zero.

    Complete files are skipped; a truncated one (the file the kill interrupted)
    differs in size and is refetched to full length.
    """
    full = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path), region="")
    assert full.files == len(FILES)

    # Simulate the interrupted state: one file truncated, one missing.
    truncated = tmp_path / "model-00001-of-00002.safetensors"
    truncated.write_bytes(b"A" * 100)
    (tmp_path / "sub" / "tokenizer.json").unlink()

    resumed = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path), region="")
    assert resumed.files == 2, "only the truncated and missing files refetched"
    assert truncated.read_bytes() == FILES[
        f"{PREFIX}/model-00001-of-00002.safetensors"]
    assert (tmp_path / "sub" / "tokenizer.json").read_bytes() == FILES[
        f"{PREFIX}/sub/tokenizer.json"]


def test_fetch_skip_existing_noop_when_all_present(s3_env, tmp_path):
    fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path), region="")
    again = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path), region="")
    assert again.files == 0 and again.bytes == 0
    assert again.mbps == 0.0  # must not divide by zero


def test_fetch_skip_existing_false_refetches_everything(s3_env, tmp_path):
    fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path), region="")
    report = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path), region="",
                         skip_existing=False)
    assert report.files == len(FILES)


# ---------------------------------------------------------------- attribution fields
#
# Every field below was already computed by the transport and discarded at the
# FetchReport boundary. These tests exist because the discard was invisible: the
# numbers looked complete, and only their ABSENCE downstream revealed the loss.


def test_fetch_reports_where_the_bytes_came_from(s3_env, tmp_path):
    """A throughput number is not attributable without the bucket that served it.

    270 MB/s is a healthy cross-region pull and a broken region-local one. The
    replica status is the field that tells those apart, and it is the reason
    resolve_bucket_ex exists alongside resolve_bucket.
    """
    make_replica(complete=True)
    rep = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path), region=REGION)
    assert rep.bucket == REPLICA
    assert rep.replica_status == "replica"
    assert rep.node_region == REGION
    assert rep.bucket_region == REGION


def test_fetch_distinguishes_an_absent_replica_from_a_rejected_one(s3_env, tmp_path):
    """A region with NO replica bucket at all (outside the fleet, or never created).

    Reading that
    as "a replica existed and was declined" would file a standing
    infrastructure fact as a replication defect.
    """
    absent = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path / "a"),
                         region="eu-north-1")
    assert absent.bucket == SRC
    assert absent.replica_status == "replica-absent"

    make_replica(complete=False)
    rejected = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path / "b"),
                           region=REGION)
    assert rejected.bucket == SRC
    assert rejected.replica_status == "replica-incomplete"


def test_fetch_records_the_fast_path_reason_verbatim(s3_env, tmp_path):
    """The transport already knew why it fell back; nobody was reading it.

    A 32-card node downloaded at 1/30th of its capability for weeks with this
    exact string in the log. Recording it is what lets a report say so.
    """
    rep = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path), region="")
    assert rep.fast_path_reason, "the reason must never be empty after a fetch"
    assert rep.fast_path_reason.startswith(("ENGAGED", "skipped", "failed"))


def test_the_slow_path_reports_attribution_as_unmeasured_not_as_failed(s3_env, tmp_path):
    """None and False are different facts.

    The slow path binds no cards, so there is nothing to attribute — that is
    not the same as a binding that did not take, which is a real defect worth
    a flag. Collapsing them would put every laptop and CI run in the defect
    column.
    """
    rep = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path), region="",
                      include=["config.json"])
    assert rep.attributed is None
    assert rep.per_iface_gbps == {}


def test_forced_source_is_not_the_same_as_an_unknown_region(s3_env, tmp_path):
    """The A/B's control leg must be distinguishable from a placement failure.

    Both read from the source bucket. One is a deliberate measurement and the
    other is "we could not tell where we are", and a benchmark that confused
    them would report a regional delta it never measured.
    """
    make_replica(complete=True)
    forced = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path / "f"),
                         region=REGION, force_source=True)
    assert forced.bucket == SRC
    assert forced.replica_status == "forced-source"

    unknown = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path / "u"), region="")
    assert unknown.bucket == SRC
    assert unknown.replica_status == "no-region"


# ---------------------------------------------------------------- limit_bytes


def test_limit_bytes_caps_the_fetch_without_filtering_it(s3_env, tmp_path):
    """The cap must not take the fast path out of the comparison.

    `include=` is the one condition that still disables the fast path, so a
    filtered control leg would time the thread-per-file fallback against the
    fan-out path — a ratio produced by the code rather than by the region.
    That is why this is a separate parameter and not a convenience wrapper
    over include.
    """
    import model_transport.fetch as fetch_mod
    assert fetch_mod._fast_path_ok(None, "/tmp", {"k": 1})[0] is not None
    #: the gate that matters: include disables, limit_bytes does not reach it
    assert fetch_mod._fast_path_ok(["x"], "/tmp", {"k": 1})[0] is False

    rep = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path), region="",
                      limit_bytes=1100)
    assert rep.bytes <= 1100
    assert rep.files < len(FILES)


def test_limit_bytes_keeps_whole_objects_and_at_least_one(s3_env, tmp_path):
    """A truncated shard is indistinguishable from a failed download.

    Every size check in this package compares against the object's full size,
    so a partial object would make the slice look like a broken one. And a
    limit under the first object still measures something: a zero-byte
    success would be recorded as a transport that ran infinitely fast.
    """
    from model_transport.resolve import cap_objects
    objs = {"a": 100, "b": 200, "c": 300}
    assert cap_objects(objs, 350) == {"a": 100, "b": 200}
    assert cap_objects(objs, 1) == {"a": 100}, "never zero objects"
    assert cap_objects(objs, None) == objs
    with pytest.raises(ValueError):
        cap_objects(objs, 0)

    rep = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path), region="",
                      limit_bytes=1)
    assert rep.files == 1 and rep.bytes > 0


def test_capping_picks_the_same_objects_from_either_bucket(s3_env, tmp_path):
    """The A/B times the SAME slice twice; a cap that varied would be noise.

    Key order is the one ordering the two buckets agree on, which is why the
    selection is by sorted key and not by listing order or size.
    """
    make_replica(complete=True)
    a = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path / "rep"),
                    region=REGION, limit_bytes=1100)
    b = fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path / "src"),
                    region=REGION, limit_bytes=1100, force_source=True)
    assert a.bucket == REPLICA and b.bucket == SRC
    assert a.files == b.files and a.bytes == b.bytes


# ------------------------------------------------- an unroutable endpoint must fail in seconds

def test_every_listing_client_bounds_its_connect_timeout():
    """REGRESSION, 152 three-hour GPU nodes (measured on a real bench cohort).

    botocore's defaults are a 60 s connect timeout with `max_attempts: 5` in adaptive mode, so one
    unreachable endpoint costs ~2409 s before the call returns. A multi-card GPU node has NO PUBLIC
    IP and reaches S3 only via its own region's gateway endpoint, while `resolve_bucket_ex` lists
    the SOURCE bucket unconditionally — an endpoint such a node cannot route to outside us-east-1.
    Four of those calls per job filled the entire 10800 s Batch cap. The correlation over the
    cohort was exact: 152 of 152 timed-out jobs were multi-card, 0 of the single-card ones.

    Asserted on the CONFIG rather than by timing a real connect, because a test that waits out the
    old behaviour would itself take forty minutes.
    """
    for region in (None, REGION):
        cfg = resolve._listing_client(region).meta.config
        assert cfg.connect_timeout == 10, "an unroutable listing must fail in seconds"
        #: botocore normalises `max_attempts: 3` to `total_max_attempts: 4` (the first try plus
        #: three retries), so the NORMALISED key is what a client actually carries. Asserting the
        #: input key would pass against a config that never reached botocore.
        assert cfg.retries["total_max_attempts"] == 4
        assert cfg.retries["mode"] == "standard", \
            "adaptive mode adds client-side backoff on top of the attempts"


def test_the_fetch_client_bounds_connect_but_not_read(s3_env, tmp_path):
    """The transfer client needs the connect bound and the READ left alone.

    Same unroutable-endpoint fault, but this client moves multi-GB shards: a read timeout generous
    enough for a 40 GB object on a slow link would catch nothing, and a tight one would abort
    healthy downloads. Reaching the endpoint at all is not size-dependent, so connect is the
    honest signal. Pinned so neither half drifts.
    """
    seen = {}
    real = fetch_mod.boto3.client

    def spy(name, **kw):
        c = real(name, **kw)
        if name == "s3" and "max_pool_connections" in str(kw.get("config").__dict__):
            seen["cfg"] = c.meta.config
        return c

    fetch_mod.boto3.client = spy
    try:
        fetch_model(f"s3://{SRC}/{PREFIX}", str(tmp_path / "m"))
    finally:
        fetch_mod.boto3.client = real
    cfg = seen["cfg"]
    assert cfg.connect_timeout == 10
    assert cfg.retries["total_max_attempts"] == 4
    #: botocore's default read timeout is 60 s and must remain untouched here
    assert cfg.read_timeout == 60, "bounding the read would abort healthy multi-GB shards"
