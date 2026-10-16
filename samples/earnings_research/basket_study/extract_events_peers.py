"""Event x peer extract of a basket-study run: every panel the run evaluated,
its reporter, release date/time (ET) and every LLM peer with its bucket.

The event set is the run's OWN manifests (<s3_prefix>/manifests/*.json, the
panel keys the eval children were handed), not a fresh listing of the panel
prefix, so the extract matches the run by construction. Peers come from the
panel parquet itself (symbol + reporter_relationship), i.e. exactly what the
evaluator saw; skipped events have no peer rows in the shards, so the shards
cannot be the source.

One row per (event, peer). An event whose panel holds no peer keeps one row
with peer = null so every evaluated event is present. SPY benchmark rows
(spy_bars.BENCHMARK_REL) are not peers and are excluded.

Columns
  event_id        FactSet event id (panel column)
  event_date      release date, ET  (event-marker bar ts -> America/New_York,
  event_time_et   release time, ET   the evaluator's own derivation)
  reporter        symbol with reporter_relationship == 'primary'
  peer            peer symbol
  peer_bucket     pure_play | functional | correlated
  peer_has_bars   False when the peer has only its synthetic event-marker row
  stub_panel      True when NO symbol in the panel has a real bar (all-synthetic
                  stub, e.g. the Databento 422 panels)
  co_primary      33 panels of results-10y-v2 carry TWO symbols labelled primary
                  (PAA+PAGP, NEE+XIFR, TK+TGP ...). The evaluator takes the
                  symbol of the FIRST primary row as the reporter and ignores
                  the other (neither reporter nor peer); so does this extract,
                  and names the ignored symbol(s) here. Null otherwise.
  panel_key       S3 key of the panel, for audit

Not a unique key: duplicate FactSet event ids for one reporter/date/time and
many events sharing a timestamp are kept as they are (user decision).

    evaluate.py extract-peers --s3-prefix earnings-basket-study/results-10y-v2
        -> s3://<bucket>/<s3-prefix>/extracts/events_peers.parquet
"""
import io
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import boto3
import polars as pl
import pyarrow.parquet as pq

from spy_bars import BENCHMARK_REL
import settings  # noqa: E402

PANEL_COLS = ["symbol", "reporter_relationship", "event_id", "event_bar", "ts"]
SCHEMA = {"event_id": pl.Int64, "event_date": pl.String, "event_time_et": pl.String,
          "reporter": pl.String, "peer": pl.String, "peer_bucket": pl.String,
          "peer_has_bars": pl.Boolean, "stub_panel": pl.Boolean, "co_primary": pl.String,
          "panel_key": pl.String}
OUT_NAME = "events_peers.parquet"


def panel_keys_from_manifests(s3, bucket: str, s3_prefix: str) -> List[str]:
    """Union of the 'panels' lists of every manifest under <s3_prefix>/manifests/,
    de-duplicated, sorted (a re-run manifest may repeat keys of the full ones)."""
    keys: List[str] = []
    prefix = f"{s3_prefix}/manifests/"
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".json"):
                m = json.loads(s3.get_object(Bucket=bucket, Key=o["Key"])["Body"].read())
                keys.extend(m["panels"])
    return sorted(set(keys))


def extract_panel(data: bytes, key: str) -> List[Dict]:
    """Rows for one panel (see module doc). Pure: bytes in, rows out."""
    tbl = pq.read_table(io.BytesIO(data), columns=PANEL_COLS)
    df = pl.from_arrow(tbl)
    ev = df.filter(pl.col("event_bar"))
    if ev.is_empty():
        raise ValueError(f"{key}: no event-marker row")
    ts_et = ev["ts"][0].astimezone(ZoneInfo("America/New_York"))
    event_id = int(df["event_id"][0])
    primary = df.filter(pl.col("reporter_relationship") == "primary")["symbol"]
    if primary.is_empty():
        raise ValueError(f"{key}: no primary symbol")
    reporter = primary[0]                      # == evaluate.py: first primary row (row order kept)
    others = sorted(set(primary.to_list()) - {reporter})
    per_sym = (df.group_by("symbol", "reporter_relationship")
                 .agg((~pl.col("event_bar")).sum().alias("real_bars"))
                 .sort("symbol"))
    stub = int(per_sym["real_bars"].sum()) == 0
    base = {"event_id": event_id, "event_date": ts_et.strftime("%Y-%m-%d"),
            "event_time_et": ts_et.strftime("%H:%M:%S"), "reporter": reporter,
            "stub_panel": stub, "co_primary": " ".join(others) or None, "panel_key": key}
    rows = []
    for sym, rel, n in per_sym.iter_rows():
        if rel == "primary" or rel == BENCHMARK_REL:
            continue
        rows.append(base | {"peer": sym, "peer_bucket": rel.replace(" ", "_"),
                            "peer_has_bars": n > 0})
    if not rows:
        rows.append(base | {"peer": None, "peer_bucket": None, "peer_has_bars": None})
    return rows


def run(bucket: str, s3_prefix: str, out_uri: Optional[str] = None, workers: int = 64,
        limit: Optional[int] = None, profile: Optional[str] = None) -> pl.DataFrame:
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client("s3", config=boto3.session.Config(max_pool_connections=workers + 8))
    keys = panel_keys_from_manifests(s3, bucket, s3_prefix)
    if limit:
        keys = keys[:limit]
    print(f"{len(keys)} panels from {s3_prefix}/manifests/", flush=True)

    def one(k):
        body = s3.get_object(Bucket=bucket, Key=k)["Body"].read()
        return extract_panel(body, k)

    rows: List[Dict] = []
    errors: List[str] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(one, k): k for k in keys}
        for i, f in enumerate(as_completed(futs), 1):
            try:
                rows.extend(f.result())
            except Exception as e:                       # noqa: BLE001 - logged, reported
                errors.append(f"{futs[f]}: {e!r}")
            if i % 5000 == 0:
                el = time.time() - t0
                print(f"{i}/{len(keys)} panels, {len(rows)} rows, {el:.0f}s, "
                      f"eta {el / i * (len(keys) - i):.0f}s", flush=True)
    df = pl.DataFrame(rows, schema=SCHEMA).sort("event_date", "event_time_et", "reporter", "event_id", "peer")
    print(f"done: {df.height} rows, {df['event_id'].n_unique()} events, "
          f"{len(errors)} errors, {time.time() - t0:.0f}s", flush=True)
    for e in errors[:50]:
        print("ERROR", e, flush=True)
    if out_uri:
        buf = io.BytesIO()
        df.write_parquet(buf, compression="zstd")
        b, k = out_uri.replace("s3://", "").split("/", 1)
        s3.put_object(Bucket=b, Key=k, Body=buf.getvalue())
        print(f"wrote {out_uri} ({buf.tell() / 1e6:.1f} MB)", flush=True)
        if errors:
            s3.put_object(Bucket=b, Key=k.rsplit("/", 1)[0] + "/events_peers.errors.txt",
                          Body="\n".join(errors).encode())
    if errors:
        raise SystemExit(f"{len(errors)} panels failed; see errors file")
    return df


def main_from_args(args, bucket: str) -> int:
    """evaluate.py extract-peers entry: manifests under --s3-prefix, output next to them."""
    out = f"s3://{bucket}/{args.s3_prefix}/extracts/{OUT_NAME}"
    run(bucket, args.s3_prefix, out_uri=out,
        workers=int(os.environ.get("EXTRACT_WORKERS", "64")),
        limit=getattr(args, "limit", None), profile=args.profile)
    return 0


if __name__ == "__main__":                                   # local smoke: python extract_events_peers.py 50
    import evaluate
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    d = run(evaluate.BUCKET, "earnings-basket-study/results-10y-v2", workers=16, limit=n, profile=settings.get("aws", "profile"))
    print(d.head(20))
    print(d.group_by("peer_bucket").len())
