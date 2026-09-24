"""Tests for EDGAR EX-99 recovery tool."""
import json
import sys
import threading
import time
import types
import urllib.error
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path

import polars as pl
import pytest

import edgar_recover
from edgar_recover import (
    resolve_ciks, pick_filing, rank_docs, to_content_row, normalise_name,
    recover_one, RateLimiter, flush_ledger, run, partition_event_ids
)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from models import CONTENT_SCHEMA  # noqa: E402


# --------------------------------------------------------------------------- #
# Fakes (no network, no AWS)
# --------------------------------------------------------------------------- #
class FakeS3:
    """In-memory S3 stub: get_object / put_object / list_objects_v2."""

    def __init__(self, objects=None):
        self.objects = dict(objects or {})       # (bucket, key) -> bytes
        self.puts = []                           # [(bucket, key, bytes), ...]

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise KeyError(f"NoSuchKey s3://{Bucket}/{Key}")
        return {"Body": BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket, Key, Body):
        self.objects[(Bucket, Key)] = Body
        self.puts.append((Bucket, Key, Body))
        return {}

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        keys = sorted(k for (b, k) in self.objects if b == Bucket and k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}


def _parquet_bytes(rows, schema=None):
    buf = BytesIO()
    pl.DataFrame(rows, schema=schema).write_parquet(buf)
    return buf.getvalue()


def _read_parquet(body: bytes) -> pl.DataFrame:
    return pl.read_parquet(BytesIO(body))


def _targets(event_ids):
    return pl.DataFrame([
        {"event_id": i, "symbol": "AAPL", "event_date": "2021-02-10",
         "event_datetime_utc": "2021-02-10 21:05:00", "ticker_region": "AAPL-US",
         "factset_entity_id": "X", "entity_proper_name": "Apple Inc.",
         "fiscal_period": "Q1", "fiscal_year": 2021}
        for i in event_ids
    ])


def _patch_boto3(monkeypatch, s3):
    monkeypatch.setattr(
        edgar_recover, "boto3",
        types.SimpleNamespace(
            Session=lambda profile_name=None: types.SimpleNamespace(client=lambda svc: s3)))


def _ok_attempt(event_id, text="Revenue was $1.0 million in the quarter."):
    return {"event_id": event_id, "status": "ok", "url": "http://x/ex991.htm",
            "accession": "0000320193-21-000001", "filing_date": "2021-02-10",
            "form": "8-K", "n_docs_tried": 1, "new_text": text}


def test_normalise_name_strips_suffixes_and_punct():
    assert normalise_name("Activision Blizzard, Inc.") == "ACTIVISION BLIZZARD"
    assert normalise_name("The Kraft Heinz Company") == "KRAFT HEINZ"


def test_resolve_ciks_current_then_name_fallback():
    tickers = {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}
    lookup = "ACTIVISION BLIZZARD, INC.:0000718877:\nAPPLE INC:0000320193:\n"
    out = resolve_ciks(["AAPL", "ATVI", "ZZZZ"], {"ATVI": "Activision Blizzard, Inc."},
                       get_json=lambda url: tickers, get_text=lambda url: lookup)
    assert out["AAPL"] == "0000320193" and out["ATVI"] == "0000718877" and "ZZZZ" not in out


def test_pick_filing_prefers_item_202_within_window():
    rows = [("8-K", "2021-02-09", "A1", "5.02"), ("8-K", "2021-02-10", "A2", "2.02,9.01"),
            ("8-K", "2021-02-11", "A3", "8.01"), ("6-K", "2021-02-10", "A4", "")]
    assert pick_filing(rows, "2021-02-10")[0] == "A2"
    rows2 = [("8-K", "2021-02-12", "B1", ""), ("10-Q", "2021-02-10", "B2", "")]
    assert pick_filing(rows2, "2021-02-10")[0] == "B1"          # nearest 8-K when no items
    assert pick_filing([("8-K", "2021-03-30", "C1", "2.02")], "2021-02-10") is None


def test_pick_filing_prefers_original_form_over_amendment_at_equal_gap():
    """Same date, same items: 8-K wins over 8-K/A even though SEC lists the amendment first."""
    rows = [("8-K/A", "2021-02-10", "AMEND", "2.02"), ("8-K", "2021-02-10", "ORIG", "2.02")]
    assert pick_filing(rows, "2021-02-10")[0] == "ORIG"
    assert pick_filing(list(reversed(rows)), "2021-02-10")[0] == "ORIG"
    # A nearer amendment still beats a farther original (gap dominates the form)
    rows2 = [("8-K/A", "2021-02-10", "AMEND", "2.02"), ("8-K", "2021-02-12", "ORIG", "2.02")]
    assert pick_filing(rows2, "2021-02-10")[0] == "AMEND"


def test_resolve_ciks_name_fallback_handles_colon_in_name():
    """cik-lookup-data.txt names may contain ':' — the CIK is the field before the last."""
    tickers = {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}
    lookup = "FOO: BAR HOLDINGS INC:0000999999:\nAPPLE INC:0000320193:\n"
    out = resolve_ciks(["XYZ"], {"XYZ": "Foo: Bar Holdings Inc."},
                       get_json=lambda url: tickers, get_text=lambda url: lookup)
    assert out["XYZ"] == "0000999999"


def test_rank_docs_ex99_first_then_size_max3():
    idx = {"directory": {"item": [
        {"name": "form8k.htm", "size": "900000"}, {"name": "ex991.htm", "size": "40000"},
        {"name": "ex992.htm", "size": "60000"}, {"name": "0001-index.htm", "size": "1000"},
        {"name": "other.htm", "size": "500000"}, {"name": "exhibit101.htm", "size": "800000"}]}}
    urls = rank_docs(idx, "0000320193", "0000320193-21-000001")
    names = [u.rsplit("/", 1)[1] for u in urls]
    assert names == ["ex992.htm", "ex991.htm", "exhibit101.htm"]
    assert urls[0].startswith("https://www.sec.gov/Archives/edgar/data/320193/000032019321000001/")


def test_to_content_row_is_content_schema_shaped():
    import polars as pl
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from models import CONTENT_SCHEMA

    target = {"event_id": 7, "symbol": "AAPL", "event_date": "2021-02-10", "event_datetime_utc": "2021-02-10 21:05:00",
              "ticker_region": "AAPL-US", "factset_entity_id": "X", "entity_proper_name": "Apple Inc.",
              "fiscal_period": "Q1", "fiscal_year": 2021}
    attempt = {"status": "ok", "url": "https://www.sec.gov/Archives/edgar/data/320193/0/ex991.htm",
               "accession": "0000320193-21-000001", "filing_date": "2021-02-10", "form": "8-K",
               "n_docs_tried": 1, "new_text": "Revenue was $1 million in the quarter."}
    row = to_content_row(target, attempt)
    assert set(row) == set(CONTENT_SCHEMA)
    assert row["fetch_status"] == "ok" and row["text_extract_status"] == "ok" and row["raw_content"] is None
    assert row["text_length"] == len(attempt["new_text"]) and row["region"] == "US" and row["shard"] == 0
    pl.DataFrame([row], schema=CONTENT_SCHEMA)          # must cast cleanly


def test_resume_accumulates_ledger_rows():
    """Real flush_ledger: run 1 writes {1,2}; resume run merges {3} and newest wins on {1}."""
    s3 = FakeS3()
    ledger_path = "s3://bkt/ledger/edgar_attempts.parquet"

    # Run 1: events {1, 2}
    attempts_run1 = [
        {"event_id": 1, "status": "ok", "url": "url1", "new_text": "text1"},
        {"event_id": 2, "status": "ok", "url": "url2", "new_text": "text2"},
    ]
    flush_ledger([], attempts_run1, ledger_path, "bkt", s3)

    assert len(s3.puts) == 1
    df1 = _read_parquet(s3.puts[-1][2])
    assert set(df1["event_id"].to_list()) == {1, 2}
    assert "new_text" not in df1.columns          # text never lands in the ledger

    # Run 2 (resume): prior rows read back from the ledger the stub captured
    obj = s3.get_object(Bucket="bkt", Key="ledger/edgar_attempts.parquet")
    prior_rows = pl.read_parquet(BytesIO(obj["Body"].read())).to_dicts()
    attempts_run2 = [
        {"event_id": 1, "status": "still_suspect_content", "url": "url1b", "new_text": ""},
        {"event_id": 3, "status": "ok", "url": "url3", "new_text": "text3"},
    ]
    flush_ledger(prior_rows, attempts_run2, ledger_path, "bkt", s3)

    df2 = _read_parquet(s3.puts[-1][2])
    by_id = {r["event_id"]: r["status"] for r in df2.to_dicts()}
    assert by_id == {1: "still_suspect_content", 2: "ok", 3: "ok"}   # newest wins on 1


def test_rate_limiter_holds_global_rate_across_threads():
    """4 threads x 10 acquires at rps=20 -> all complete, measured rate <= rps * 1.1."""
    rps = 20.0
    limiter = RateLimiter(rps=rps)
    per_thread, n_threads = 10, 4
    done = []
    done_lock = threading.Lock()

    def worker():
        for _ in range(per_thread):
            limiter.acquire()
            with done_lock:
                done.append(time.monotonic())

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    t0 = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - t0

    assert len(done) == n_threads * per_thread          # nothing lost or deadlocked
    rate = len(done) / elapsed
    assert rate <= rps * 1.1, f"measured {rate:.2f} rps for a {rps} rps limiter"


def test_403_stop_flag_halts_further_requests():
    """A 403 from submissions sets the stop flag; the next event issues no request."""
    calls = {"n": 0}

    def fake_submissions(cik, limiter):
        calls["n"] += 1
        raise urllib.error.HTTPError("https://data.sec.gov/x", 403, "Forbidden", {}, None)

    orig = edgar_recover.submissions
    edgar_recover.submissions = fake_submissions
    try:
        ciks = {"AAPL": "0000320193"}
        stop_flag = threading.Event()
        kwargs = dict(ciks=ciks, subs_cache={}, cache_locks=defaultdict(threading.Lock),
                      main_lock=threading.Lock(), limiter=RateLimiter(rps=1000),
                      stop_flag=stop_flag)
        first = recover_one({"event_id": 1, "symbol": "AAPL", "event_date": "2021-02-10"},
                            **kwargs)
        assert first["status"] == "sec_403_stop"
        assert stop_flag.is_set()
        assert calls["n"] == 1

        second = recover_one({"event_id": 2, "symbol": "AAPL", "event_date": "2021-02-10"},
                             **kwargs)
        assert second["status"] == "sec_403_stop"
        assert calls["n"] == 1              # no further SEC request after the stop
    finally:
        edgar_recover.submissions = orig


SUSPECT_DOC = "Please enable JavaScript in your browser to view this page."
FUTURE_INDEX_DOC = (
    "Revenue rose and net income improved for the quarter; diluted results "
    "included $1.2 million of charges. Notes due Feb 10, 2030, Mar 11, 2031, "
    "Apr 12, 2032, May 13, 2033 and Jun 14, 2034."
)


def test_walk_on_continues_on_suspect_and_returns_on_future_dated(monkeypatch):
    """suspect_content walks on to the next exhibit; keyword-rich future_dated returns."""
    idx = {"directory": {"item": [
        {"name": "ex991.htm", "size": "90000"},
        {"name": "press2.htm", "size": "10000"},
    ]}}
    bodies = {"ex991.htm": b"SUSPECT", "press2.htm": b"FUTURE"}
    texts = {b"SUSPECT": SUSPECT_DOC, b"FUTURE": FUTURE_INDEX_DOC}
    fetched = []

    monkeypatch.setattr(edgar_recover, "submissions",
                        lambda cik, limiter: [("8-K", "2021-02-10", "0000320193-21-000001", "2.02")])
    monkeypatch.setattr(edgar_recover, "_get_json", lambda url, limiter: idx)

    def fake_get(url, limiter, timeout=30):
        name = url.rsplit("/", 1)[1]
        fetched.append(name)
        return bodies[name]

    monkeypatch.setattr(edgar_recover, "_get", fake_get)
    monkeypatch.setattr(edgar_recover, "extract_html", lambda body: texts[body])

    attempt = recover_one({"event_id": 1, "symbol": "AAPL", "event_date": "2021-02-10"},
                         {"AAPL": "0000320193"}, {}, defaultdict(threading.Lock),
                         threading.Lock(), RateLimiter(rps=1000), threading.Event())

    assert fetched == ["ex991.htm", "press2.htm"]        # walked on past the suspect doc
    assert attempt["status"] == "still_future_dated"      # then stopped, honest exclusion
    assert attempt["n_docs_tried"] == 2
    assert attempt["new_text"] == ""


def test_to_content_row_non_ok_attempt_carries_status_without_text():
    target = {"event_id": 9, "symbol": "ZZZZ", "event_date": "2021-02-10",
              "event_datetime_utc": "2021-02-10 21:05:00", "ticker_region": "ZZZZ-US",
              "factset_entity_id": "X", "entity_proper_name": "Zed Inc.",
              "fiscal_period": "Q1", "fiscal_year": 2021}
    attempt = {"event_id": 9, "status": "no_cik", "url": "", "accession": "",
               "filing_date": "", "form": "", "n_docs_tried": 0, "new_text": ""}

    row = to_content_row(target, attempt)

    assert row["fetch_status"] == "no_cik"
    assert row["text_content"] is None
    assert row["text_extract_status"] == "not_attempted"
    assert row["text_length"] == 0 and row["content_length"] == 0
    assert row["content_sha256"] == "" and row["http_status"] == 0
    pl.DataFrame([row], schema=CONTENT_SCHEMA)          # must still cast cleanly


def test_partition_event_ids_reads_part_files():
    part = _parquet_bytes([{"event_id": 1}, {"event_id": 5}])
    s3 = FakeS3({("bkt", "content/job=x/shard=0/part-1.parquet"): part,
                 ("bkt", "content/job=x/shard=0/_SUCCESS"): b"",
                 ("bkt", "other/part-2.parquet"): _parquet_bytes([{"event_id": 99}])})
    assert partition_event_ids(s3, "bkt", "content/job=x/shard=0/") == {1, 5}


def test_resume_reattempts_ok_ids_absent_from_partition(monkeypatch):
    """ledger ok + in partition -> skipped; ok but absent -> re-attempted; non-ok -> re-attempted."""
    ledger = _parquet_bytes([
        {"event_id": 1, "status": "ok", "url": "u1", "accession": "a", "filing_date": "d",
         "form": "8-K", "n_docs_tried": 1},
        {"event_id": 2, "status": "ok", "url": "u2", "accession": "a", "filing_date": "d",
         "form": "8-K", "n_docs_tried": 1},
        {"event_id": 3, "status": "index_error:HTTPError_404", "url": "", "accession": "",
         "filing_date": "", "form": "", "n_docs_tried": 0},
    ])
    part = _parquet_bytes([{"event_id": 1}])
    s3 = FakeS3({("bkt", "ledger/edgar_attempts.parquet"): ledger,
                 ("bkt", "content/job=x/shard=0/part-old.parquet"): part})
    _patch_boto3(monkeypatch, s3)
    monkeypatch.setattr(edgar_recover, "resolve_ciks",
                        lambda *a, **k: {"AAPL": "0000320193"})

    seen = []

    def fake_recover_one(target, *a, **k):
        seen.append(target["event_id"])
        return _ok_attempt(target["event_id"])

    monkeypatch.setattr(edgar_recover, "recover_one", fake_recover_one)

    rc = run(_targets([1, 2, 3]), "s3://bkt/ledger/edgar_attempts.parquet",
             "content/job=x/shard=0/", "bkt", profile="none", workers=2, rps=1000,
             resume=True)

    assert rc == 0
    assert sorted(seen) == [2, 3]        # id 1 skipped: ok AND already in a part file

    part_keys = [k for (_, k, _) in s3.puts if k.startswith("content/job=x/shard=0/")]
    assert len(part_keys) == 1
    new_part = _read_parquet(s3.objects[("bkt", part_keys[0])])
    assert sorted(new_part["event_id"].to_list()) == [2, 3]

    final_ledger = _read_parquet(s3.objects[("bkt", "ledger/edgar_attempts.parquet")])
    assert sorted(final_ledger["event_id"].to_list()) == [1, 2, 3]


def test_worker_exception_yields_error_status_and_run_completes(monkeypatch):
    """An exception inside recover_one becomes an error:RuntimeError ledger row."""
    s3 = FakeS3()
    _patch_boto3(monkeypatch, s3)
    monkeypatch.setattr(edgar_recover, "resolve_ciks",
                        lambda *a, **k: {"AAPL": "0000320193"})

    def fake_recover_one(target, *a, **k):
        if target["event_id"] == 2:
            raise RuntimeError("boom")
        return _ok_attempt(target["event_id"])

    monkeypatch.setattr(edgar_recover, "recover_one", fake_recover_one)

    rc = run(_targets([1, 2, 3]), "s3://bkt/ledger/edgar_attempts.parquet",
             "content/job=x/shard=0/", "bkt", profile="none", workers=3, rps=1000)

    assert rc == 0
    ledger = _read_parquet(s3.objects[("bkt", "ledger/edgar_attempts.parquet")])
    by_id = {r["event_id"]: r["status"] for r in ledger.to_dicts()}
    assert by_id == {1: "ok", 2: "error:RuntimeError", 3: "ok"}      # no silent drop


def test_partition_written_on_403_stop_path(monkeypatch):
    """A 403 stop still writes the partition (and returns 2) so ok text is not lost."""
    s3 = FakeS3()
    _patch_boto3(monkeypatch, s3)
    monkeypatch.setattr(edgar_recover, "resolve_ciks",
                        lambda *a, **k: {"AAPL": "0000320193"})

    def fake_recover_one(target, ciks, subs_cache, cache_locks, main_lock, limiter, stop_flag):
        if target["event_id"] == 1:
            return _ok_attempt(1)
        stop_flag.set()
        return {"event_id": target["event_id"], "status": "sec_403_stop", "url": "",
                "accession": "", "filing_date": "", "form": "", "n_docs_tried": 0,
                "new_text": ""}

    monkeypatch.setattr(edgar_recover, "recover_one", fake_recover_one)

    rc = run(_targets([1, 2]), "s3://bkt/ledger/edgar_attempts.parquet",
             "content/job=x/shard=0/", "bkt", profile="none", workers=1, rps=1000)

    assert rc == 2
    part_keys = [k for (_, k, _) in s3.puts if k.startswith("content/job=x/shard=0/")]
    assert len(part_keys) == 1
    part = _read_parquet(s3.objects[("bkt", part_keys[0])])
    assert sorted(part["event_id"].to_list()) == [1, 2]
    assert part.filter(pl.col("event_id") == 1)["text_content"].to_list()[0].startswith("Revenue")


def test_submissions_cache_single_fetch_per_cik():
    """8 threads with same CIK → exactly 1 submissions call."""
    call_count = {"n": 0}
    call_lock = threading.Lock()

    def fake_submissions(cik, limiter):
        with call_lock:
            call_count["n"] += 1
        # Simulate some work
        import time
        time.sleep(0.01)
        return [("8-K", "2021-01-01", "ACC1", "")]

    # Patch submissions globally (for this test)
    import edgar_recover
    orig_submissions = edgar_recover.submissions
    edgar_recover.submissions = fake_submissions

    try:
        # Shared state
        ciks = {"AAPL": "0000320193"}
        subs_cache = {}
        cache_locks = defaultdict(threading.Lock)
        main_lock = threading.Lock()
        limiter = RateLimiter(rps=100)  # High rate for test
        stop_flag = threading.Event()

        # 8 targets, all same symbol/CIK
        targets = [
            {"event_id": i, "symbol": "AAPL", "event_date": "2021-01-01"}
            for i in range(8)
        ]

        results = []
        def worker(target):
            result = recover_one(target, ciks, subs_cache, cache_locks, main_lock, limiter, stop_flag)
            results.append(result)

        # Execute in parallel
        with ThreadPoolExecutor(max_workers=8) as executor:
            executor.map(worker, targets)

        # Verify exactly 1 call
        assert call_count["n"] == 1, f"Expected 1 call, got {call_count['n']}"

        # Verify all 8 events got the submissions (either ok or downstream status)
        assert len(results) == 8
        # All should be no_filing_within_5_days or later status (not submissions_error)
        for r in results:
            assert not r["status"].startswith("submissions_error")

    finally:
        # Restore original
        edgar_recover.submissions = orig_submissions
