#!/usr/bin/env python3
"""EDGAR EX-99 recovery tool — parallel, resumable, CONTENT_SCHEMA partition output.

Recovers earnings press releases from SEC EDGAR filings when original URLs are
dead or serve junk content. Parallel multi-threaded fetcher with rate limiting,
resume capability, and structured status reporting.

Origin: Based on /tmp/large_univ/harness/repair_semis_content.py (commit 83d89539,
branch feat/oss-sft-semis). Retains _get/_get_json, submissions, extract_html, and
the ranked walk-on loop logic. Extended with parallel execution, a global
next-slot rate limiter, ledger persistence, and CONTENT_SCHEMA output.

Usage:
    python analysis/edgar_recover.py \\
        --targets s3://.../targets.parquet \\
        --ledger s3://.../edgar_attempts.parquet \\
        --partition-prefix earnings-content/content/job=er-large-edgar/shard=0/ \\
        --bucket <data_bucket> \\
        --profile <profile> \\
        --workers 4 --rps 5 [--limit N] [--resume]
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path

import boto3
import polars as pl

# Import from package root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from models import CONTENT_SCHEMA
from content_integrity import audit_text

import settings  # noqa: E402


def _ua() -> dict:
    """SEC EDGAR requires a user agent that identifies the requester."""
    return {"User-Agent": f"quant-research-earnings-pipeline/1.0 (contact: {settings.get('contact_email')})"}
FILING_FORMS = ("8-K", "8-K/A", "6-K", "6-K/A")
EX99 = re.compile(r"ex[-_]?99|99[-_.]?1|press|news|earnings|release", re.I)
MAX_DOC_CANDIDATES = 3

# Suffixes to strip when normalizing company names
NAME_SUFFIXES = [
    "INC", "CORP", "CORPORATION", "CO", "LTD", "PLC", "HOLDINGS", "GROUP",
    "THE", "COMPANY", "LP", "NV", "SA", "AG"
]


# --------------------------------------------------------------------------- #
# HTTP with rate limiting and failure handling
# --------------------------------------------------------------------------- #
class RateLimiter:
    """Global rate limiter: N threads together never exceed `rps` requests/second.

    Implemented as a monotonic "next allowed time" scheduler. Each caller reserves
    its own slot *under the lock* (so two threads can never be handed the same slot),
    then sleeps outside the lock until that slot arrives, re-checking after each wake
    to absorb short sleeps. Slots are spaced `1/rps` apart, so the request rate is
    bounded by `rps` regardless of the number of workers.
    """

    def __init__(self, rps: float = 5.0):
        self.rps = rps
        self.interval = 1.0 / rps
        self.lock = threading.Lock()
        # First acquire may proceed immediately.
        self.next_free = time.monotonic()

    def acquire(self):
        """Reserve the next slot and block until it arrives."""
        with self.lock:
            slot = max(self.next_free, time.monotonic())
            self.next_free = slot + self.interval

        wait = slot - time.monotonic()
        while wait > 0:
            time.sleep(wait)
            wait = slot - time.monotonic()


def _get(url: str, limiter: RateLimiter, timeout: int = 30) -> bytes:
    """Fetch URL with rate limiting."""
    limiter.acquire()
    req = urllib.request.Request(url, headers=_ua())
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _get_json(url: str, limiter: RateLimiter) -> dict:
    """Fetch and parse JSON with rate limiting."""
    return json.loads(_get(url, limiter).decode())


def _get_text(url: str, limiter: RateLimiter) -> str:
    """Fetch text with rate limiting."""
    return _get(url, limiter).decode()


# --------------------------------------------------------------------------- #
# Name normalization and CIK resolution
# --------------------------------------------------------------------------- #
def normalise_name(name: str) -> str:
    """Normalize company name for matching: uppercase, strip punctuation and suffixes."""
    # Convert to uppercase
    name = name.upper()
    # Strip punctuation except spaces
    name = re.sub(r"[^\w\s]", " ", name)
    # Split into words
    words = name.split()
    # Remove suffix words
    filtered = [w for w in words if w not in NAME_SUFFIXES]
    # Return joined
    return " ".join(filtered).strip()


def resolve_ciks(symbols: list[str], names: dict[str, str],
                 get_json=None, get_text=None) -> dict[str, str]:
    """Resolve symbols to 10-digit zero-padded CIKs.

    First tries SEC's company_tickers.json for current tickers. For misses,
    falls back to exact normalized name match in cik-lookup-data.txt.
    When multiple CIKs match a name, would need submissions probe (not implemented
    in this pure function — caller handles that).

    Args:
        symbols: List of ticker symbols
        names: Dict mapping symbol -> entity proper name
        get_json: Function to fetch JSON (for testing)
        get_text: Function to fetch text (for testing)

    Returns:
        Dict mapping symbol -> zero-padded 10-digit CIK (only symbols found)
    """
    if get_json is None:
        raise ValueError("get_json must be provided")
    if get_text is None:
        raise ValueError("get_text must be provided")

    # First pass: company_tickers.json
    tickers_data = get_json("https://www.sec.gov/files/company_tickers.json")
    ticker_to_cik = {
        str(row["ticker"]).upper(): str(row["cik_str"]).zfill(10)
        for row in tickers_data.values()
    }

    result = {}
    missing = []
    for symbol in symbols:
        sym_upper = symbol.upper()
        if sym_upper in ticker_to_cik:
            result[symbol] = ticker_to_cik[sym_upper]
        else:
            missing.append(symbol)

    # Second pass: name fallback via cik-lookup-data.txt
    if missing:
        lookup_text = get_text("https://www.sec.gov/Archives/edgar/cik-lookup-data.txt")
        # Build normalized name -> CIK map
        name_to_cik = {}
        for line in lookup_text.strip().split("\n"):
            if ":" not in line:
                continue
            # Lines are "NAME:CIK:" and NAME itself may contain ':', so parse from
            # the right rather than the left.
            parts = line.rstrip().rsplit(":", 2)
            if len(parts) == 3 and parts[2] == "":
                raw_name, cik = parts[0], parts[1]
            elif len(parts) >= 2:
                raw_name, cik = ":".join(parts[:-1]), parts[-1]
            else:
                continue
            cik = cik.strip()
            if not cik:
                continue
            normalized = normalise_name(raw_name)
            if normalized:
                # Store first CIK for each normalized name (could be improved with submissions probe)
                if normalized not in name_to_cik:
                    name_to_cik[normalized] = cik.zfill(10)

        # Match missing symbols by name
        for symbol in missing:
            if symbol in names:
                norm_name = normalise_name(names[symbol])
                if norm_name in name_to_cik:
                    result[symbol] = name_to_cik[norm_name]

    return result


# --------------------------------------------------------------------------- #
# SEC metadata and document selection
# --------------------------------------------------------------------------- #
def submissions(cik: str, limiter: RateLimiter) -> list[tuple[str, str, str, str]]:
    """All (form, filingDate, accessionNumber, items) for a CIK."""
    root = _get_json(f"https://data.sec.gov/submissions/CIK{cik}.json", limiter)
    frames = [root["filings"]["recent"]]
    for extra in root["filings"].get("files", []):
        frames.append(_get_json(f"https://data.sec.gov/submissions/{extra['name']}", limiter))

    rows: list[tuple[str, str, str, str]] = []
    for f in frames:
        # Extract items if present, otherwise empty string
        items_list = f.get("items", [""] * len(f["form"]))
        rows.extend(zip(f["form"], f["filingDate"], f["accessionNumber"], items_list))
    return rows


def pick_filing(rows: list[tuple], event_date: str, window_days: int = 5):
    """Pick best 8-K/6-K within window, preferring Item 2.02.

    Args:
        rows: List of (form, filing_date, accession, items)
        event_date: Event date string (YYYY-MM-DD)
        window_days: Window around event date

    Returns:
        (accession, filing_date, form, items) or None
    """
    ev = dt.date.fromisoformat(event_date)
    candidates = []

    for form, fdate, acc, items in rows:
        if form not in FILING_FORMS:
            continue
        gap = abs((dt.date.fromisoformat(fdate) - ev).days)
        if gap <= window_days:
            has_202 = "2.02" in items if items else False
            candidates.append((gap, has_202, fdate, form, acc, items))

    if not candidates:
        return None

    # Sort: prefer Item 2.02, then smallest gap, then the original form over its
    # amendment (8-K before 8-K/A) — SEC returns newest-first, so without the form
    # in the key an amendment filed the same day would win the tie.
    candidates.sort(key=lambda x: (not x[1], x[0], "/A" in x[3]))
    gap, has_202, fdate, form, acc, items = candidates[0]
    return (acc, fdate, form, items)


def rank_docs(items_json: dict, cik: str, accession: str) -> list[str]:
    """Rank candidate documents: EX-99 named first by size, then unnamed, max 3."""
    acc = accession.replace("-", "")
    base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}"

    items = items_json["directory"]["item"]
    sizes = {i["name"]: int(i.get("size") or 0) for i in items}
    htm = [i["name"] for i in items
           if i["name"].lower().endswith((".htm", ".html"))]

    # EX-99 named documents, sorted by size desc
    named = sorted(
        (n for n in htm if EX99.search(n) and "index" not in n.lower()),
        key=lambda n: -sizes.get(n, 0)
    )

    # Other HTML docs (not index, not form), sorted by size desc
    rest = sorted(
        (n for n in htm if n not in named
         and "index" not in n.lower()
         and not n.lower().startswith("form")),
        key=lambda n: -sizes.get(n, 0)
    )

    pool = (named + rest)[:MAX_DOC_CANDIDATES]
    return [f"{base}/{n}" for n in pool]


def extract_html(body: bytes) -> str:
    """Extract plain text from HTML document."""
    try:
        import trafilatura
        txt = trafilatura.extract(
            body.decode("utf-8", errors="replace"),
            include_tables=True,
            no_fallback=False
        )
        if txt and txt.strip():
            return txt
    except Exception:
        pass

    try:
        from bs4 import BeautifulSoup
        return BeautifulSoup(body, "lxml").get_text(" ", strip=True)
    except Exception:
        return ""


# --------------------------------------------------------------------------- #
# Content schema row construction
# --------------------------------------------------------------------------- #
def to_content_row(target: dict, attempt: dict) -> dict:
    """Map target + attempt to CONTENT_SCHEMA row."""
    text = attempt.get("new_text", "")
    url = attempt.get("url", "")
    status = attempt.get("status", "")

    # Extract region from ticker_region suffix (e.g., "AAPL-US" -> "US")
    ticker_region = target.get("ticker_region", "")
    region = ticker_region.split("-")[-1] if "-" in ticker_region else "UNKNOWN"

    # Compute content hash
    text_bytes = text.encode("utf-8") if text else b""
    content_sha = hashlib.sha256(text_bytes).hexdigest() if text_bytes else ""

    # Build error detail
    acc = attempt.get("accession", "")
    form = attempt.get("form", "")
    fd = attempt.get("filing_date", "")
    n_docs = attempt.get("n_docs_tried", 0)
    error_detail = f"edgar accession={acc} form={form} filing_date={fd} docs_tried={n_docs}"

    return {
        # From target (manifest columns)
        "event_id": target["event_id"],
        "event_datetime_utc": target["event_datetime_utc"],
        "event_date": target["event_date"],
        "region": region,
        "url_pr": url,
        "factset_entity_id": target.get("factset_entity_id", ""),
        "entity_proper_name": target.get("entity_proper_name", ""),
        "ticker_region": ticker_region,
        "fiscal_period": target.get("fiscal_period", ""),
        "fiscal_year": target.get("fiscal_year", 0),
        "shard": 0,
        # Fetch metadata
        "final_url": url,
        "fetch_status": status,
        "http_status": 200 if status == "ok" else 0,
        "content_type_header": "text/html",
        "sniffed_type": "html",
        "content_length": len(text_bytes),
        "content_sha256": content_sha,
        "raw_content": None,
        "text_content": text if status == "ok" else None,
        "text_extract_status": "ok" if status == "ok" else "not_attempted",
        "text_length": len(text) if text else 0,
        "tls_insecure": False,
        "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "attempt": 1,
        "error_detail": error_detail,
    }


# --------------------------------------------------------------------------- #
# Per-event recovery
# --------------------------------------------------------------------------- #
def recover_one(target: dict, ciks: dict, subs_cache: dict, cache_locks: dict,
                main_lock: threading.Lock, limiter: RateLimiter,
                stop_flag: threading.Event) -> dict:
    """Attempt EDGAR recovery for one event."""
    if stop_flag.is_set():
        return {
            "event_id": target["event_id"],
            "status": "sec_403_stop",
            "url": "",
            "accession": "",
            "filing_date": "",
            "form": "",
            "n_docs_tried": 0,
            "new_text": ""
        }

    symbol = target["symbol"]
    event_date = target["event_date"]

    attempt = {
        "event_id": target["event_id"],
        "status": "",
        "url": "",
        "accession": "",
        "filing_date": "",
        "form": "",
        "n_docs_tried": 0,
        "new_text": ""
    }

    # Resolve CIK
    cik = ciks.get(symbol)
    if not cik:
        attempt["status"] = "no_cik"
        return attempt

    # Get submissions with per-CIK lock to prevent duplicate fetches
    with main_lock:
        cik_lock = cache_locks[cik]

    with cik_lock:
        if cik not in subs_cache:
            try:
                subs_cache[cik] = submissions(cik, limiter)
            except urllib.error.HTTPError as e:
                if e.code == 403:
                    stop_flag.set()
                    attempt["status"] = "sec_403_stop"
                    return attempt
                # Don't cache empty list on error - let next event retry
                attempt["status"] = f"submissions_error:HTTPError_{e.code}"
                return attempt
            except Exception as e:
                # Don't cache empty list on error - let next event retry
                attempt["status"] = f"submissions_error:{type(e).__name__}"
                return attempt

    # Read from cache (may have been populated by another thread)
    cached = subs_cache.get(cik)
    if not cached:
        attempt["status"] = "submissions_error:cached_empty"
        return attempt

    # Pick filing
    filing = pick_filing(cached, event_date)
    if not filing:
        attempt["status"] = "no_filing_within_5_days"
        return attempt

    acc, fdate, form, items = filing
    attempt["accession"] = acc
    attempt["filing_date"] = fdate
    attempt["form"] = form

    # Get document index
    try:
        acc_clean = acc.replace("-", "")
        idx_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_clean}/index.json"
        idx = _get_json(idx_url, limiter)
        urls = rank_docs(idx, cik, acc)
    except urllib.error.HTTPError as e:
        if e.code == 403:
            stop_flag.set()
            attempt["status"] = "sec_403_stop"
            return attempt
        attempt["status"] = f"index_error:HTTPError_{e.code}"
        return attempt
    except Exception as e:
        attempt["status"] = f"index_error:{type(e).__name__}"
        return attempt

    if not urls:
        attempt["status"] = "no_html_doc_in_filing"
        return attempt

    # Walk ranked candidates
    event_date_obj = dt.date.fromisoformat(event_date)
    for i, url in enumerate(urls):
        attempt["url"] = url
        attempt["n_docs_tried"] = i + 1

        try:
            body = _get(url, limiter)
        except urllib.error.HTTPError as e:
            if e.code == 403:
                stop_flag.set()
                attempt["status"] = "sec_403_stop"
                return attempt
            attempt["status"] = f"fetch_error:http_{e.code}"
            continue
        except Exception as e:
            attempt["status"] = f"fetch_error:{type(e).__name__}"
            continue

        text = extract_html(body)
        if not text.strip():
            attempt["status"] = "extract_empty"
            continue

        # Audit
        audit = audit_text(text, event_date_obj)
        if audit["clean"]:
            attempt["status"] = "ok"
            attempt["new_text"] = text
            return attempt

        # Walk-on logic: only walk on if suspect_content (not a release)
        if audit["suspect_content"] and audit["future_dated"]:
            attempt["status"] = "still_suspect_content+still_future_dated"
            continue
        elif audit["suspect_content"]:
            attempt["status"] = "still_suspect_content"
            continue
        else:
            # future_dated but keyword-rich — honest exclusion
            attempt["status"] = "still_future_dated"
            return attempt

    # Exhausted all candidates
    if not attempt["status"]:
        attempt["status"] = "no_html_doc_in_filing"
    return attempt


# --------------------------------------------------------------------------- #
# Parallel runner
# --------------------------------------------------------------------------- #
def partition_event_ids(s3, bucket: str, partition_prefix: str) -> set:
    """event_ids already persisted in part files under partition_prefix.

    Used by --resume: an id is only "done" if its text actually landed in a part
    file, not merely because the ledger says 'ok'.
    """
    ids: set = set()
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": partition_prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []) or []:
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            part = pl.read_parquet(BytesIO(body), columns=["event_id"])
            ids.update(part["event_id"].to_list())
        if resp.get("IsTruncated") and resp.get("NextContinuationToken"):
            token = resp["NextContinuationToken"]
        else:
            break
    return ids


def _error_attempt(event_id, exc: Exception) -> dict:
    """Ledger row for an event whose processing raised — never a silent drop."""
    return {
        "event_id": event_id,
        "status": f"error:{type(exc).__name__}",
        "url": "",
        "accession": "",
        "filing_date": "",
        "form": "",
        "n_docs_tried": 0,
        "new_text": "",
    }


def run(targets: pl.DataFrame, ledger_path: str, partition_prefix: str, bucket: str,
        profile: str, workers: int = 4, rps: float = 5.0, limit: int = None,
        resume: bool = False) -> int:
    """Run parallel EDGAR recovery.

    Resume semantics (--resume): the ledger is flushed every 100 events but the
    CONTENT_SCHEMA partition is written once per run, so a ledger 'ok' row does not
    by itself prove the recovered text was persisted. On resume an event is therefore
    skipped ONLY if its ledger status is 'ok' AND its event_id appears in a part file
    already under --partition-prefix (see partition_event_ids). 'ok' ids with no
    partition row, and every non-'ok' id, are re-attempted. The partition is written
    on the SEC-403 stop path too (before returning 2), so a stopped run never leaves
    ledgered-ok text unpersisted.
    """
    session = boto3.Session(profile_name=profile)
    s3 = session.client("s3")

    # Resolve CIKs
    symbols = targets["symbol"].unique().to_list()
    names = dict(zip(targets["symbol"].to_list(), targets["entity_proper_name"].to_list()))
    limiter = RateLimiter(rps=rps)

    print(f"Resolving CIKs for {len(symbols)} symbols...")
    ciks = resolve_ciks(
        symbols, names,
        get_json=lambda url: _get_json(url, limiter),
        get_text=lambda url: _get_text(url, limiter)
    )
    print(f"Resolved {len(ciks)}/{len(symbols)} CIKs")

    # Load existing ledger if resuming
    done_ids = set()
    prior_ledger_rows = []
    if resume:
        try:
            ledger_bucket, ledger_key = ledger_path.replace("s3://", "").split("/", 1)
            obj = s3.get_object(Bucket=ledger_bucket, Key=ledger_key)
            ledger_df = pl.read_parquet(BytesIO(obj["Body"].read()))
            # Keep prior rows (without new_text if it exists)
            prior_ledger_rows = [
                {k: v for k, v in row.items() if k != "new_text"}
                for row in ledger_df.to_dicts()
            ]
            # An 'ok' ledger row only counts as done if the text actually landed in
            # a part file; everything else is re-attempted.
            persisted = partition_event_ids(s3, bucket, partition_prefix)
            ok_ids = {row["event_id"] for row in prior_ledger_rows
                      if row.get("status") == "ok"}
            done_ids = ok_ids & persisted
            print(f"Resuming: ledger has {len(prior_ledger_rows)} rows "
                  f"({len(ok_ids)} ok), {len(persisted)} ids persisted in partition, "
                  f"{len(done_ids)} events skipped")
        except Exception as e:
            print(f"No existing ledger found (resume will start fresh): {e}")

    # Filter targets
    work = targets.filter(~pl.col("event_id").is_in(done_ids))
    if limit:
        work = work.head(limit)

    print(f"Processing {len(work)} events with {workers} workers at {rps} req/s...")

    # Shared state
    subs_cache = {}
    cache_locks = defaultdict(threading.Lock)
    main_lock = threading.Lock()
    stop_flag = threading.Event()
    attempts = []
    rows = []

    def process_target(target_dict):
        # Every event must end with a named status, so no exception may escape:
        # a failure becomes an error:<ExceptionName> ledger row.
        event_id = target_dict.get("event_id")
        row = None
        try:
            attempt = recover_one(target_dict, ciks, subs_cache, cache_locks,
                                  main_lock, limiter, stop_flag)
        except Exception as e:
            traceback.print_exc()
            attempt = _error_attempt(event_id, e)
        try:
            row = to_content_row(target_dict, attempt)
        except Exception as e:
            traceback.print_exc()
            attempt = _error_attempt(event_id, e)

        with main_lock:
            attempts.append(attempt)
            if row is not None:
                rows.append(row)

            # Flush ledger every 100 events
            if len(attempts) % 100 == 0:
                flush_ledger(prior_ledger_rows, attempts, ledger_path, bucket, s3)
                print(f"  Progress: {len(attempts)}/{len(work)} events")

    # Execute in parallel; results are iterated so nothing is swallowed
    target_dicts = work.to_dicts()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(process_target, td) for td in target_dicts]
        for fut in futures:
            fut.result()

    # Final ledger flush
    flush_ledger(prior_ledger_rows, attempts, ledger_path, bucket, s3)

    assert len(attempts) == len(work), (
        f"attempt accounting broken: {len(attempts)} attempts for {len(work)} events")

    # Write partition — also on the 403 stop path, so ledgered-ok text is never lost
    print(f"Writing {len(rows)} rows to partition...")
    if rows:
        df = pl.DataFrame(rows, schema=CONTENT_SCHEMA)
        epoch_ms = int(time.time() * 1000)
        part_key = f"{partition_prefix}part-{epoch_ms}.parquet"
        buf = BytesIO()
        df.write_parquet(buf)
        buf.seek(0)
        s3.put_object(Bucket=bucket, Key=part_key, Body=buf.read())
        print(f"Wrote partition: s3://{bucket}/{part_key}")

    # Print summary
    status_counts = {}
    for att in attempts:
        st = att["status"]
        status_counts[st] = status_counts.get(st, 0) + 1

    print("\nStatus summary:")
    for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
        print(f"  {status}: {count}")

    # Check for 403 stop (partition already written above)
    if stop_flag.is_set():
        print("SEC 403 detected, marking remaining events and exiting")
        return 2

    return 0


def flush_ledger(prior_rows: list, attempts: list, ledger_path: str, bucket: str, s3):
    """Flush ledger to S3 (prior + current, dedupe by event_id, current wins)."""
    if not prior_rows and not attempts:
        return

    # Current attempts without new_text
    current_rows = [{k: v for k, v in att.items() if k != "new_text"} for att in attempts]

    # Dedupe: current wins over prior
    current_ids = {row["event_id"] for row in current_rows}
    merged = [row for row in prior_rows if row["event_id"] not in current_ids] + current_rows

    if not merged:
        return

    df = pl.DataFrame(merged)

    ledger_bucket, ledger_key = ledger_path.replace("s3://", "").split("/", 1)
    buf = BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    s3.put_object(Bucket=ledger_bucket, Key=ledger_key, Body=buf.read())


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--targets", required=True, help="s3:// targets parquet")
    ap.add_argument("--ledger", required=True, help="s3:// ledger parquet path")
    ap.add_argument("--partition-prefix", required=True, help="S3 prefix for partition output")
    ap.add_argument("--bucket", required=True, help="S3 bucket name")
    ap.add_argument("--profile", default=None)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--rps", type=float, default=5.0, help="Requests per second")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    session = boto3.Session(profile_name=args.profile)
    s3 = session.client("s3")

    # Load targets
    targets_bucket, targets_key = args.targets.replace("s3://", "").split("/", 1)
    obj = s3.get_object(Bucket=targets_bucket, Key=targets_key)
    targets = pl.read_parquet(BytesIO(obj["Body"].read()))

    return run(
        targets=targets,
        ledger_path=args.ledger,
        partition_prefix=args.partition_prefix,
        bucket=args.bucket,
        profile=args.profile,
        workers=args.workers,
        rps=args.rps,
        limit=args.limit,
        resume=args.resume
    )


if __name__ == "__main__":
    sys.exit(main())
