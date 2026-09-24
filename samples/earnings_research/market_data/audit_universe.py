"""Audit already-collected panels against the shared US common-equity universe.

Reuses universe.py (the single source of truth) to answer, for every panel in
S3: is the REPORTER in-universe, and which PEERS are out-of-universe? Anything
flagged was selected under an older/looser definition (ETFs, ADRs, foreign
shares, foreign reporters) and is now erroneous.

Reporter = the panel row with reporter_relationship='primary'. Peers = the
other distinct symbols. Each symbol's <SYM>-US ticker is resolved once via the
shared peer_universe_sql and judged by in_universe().

Read-only. Redshift queries are free. Usage:
    python audit_universe.py [--start YYYY-MM-DD] [--end YYYY-MM-DD]
"""

import argparse
import io
import json
import logging
import sys
from collections import defaultdict

import boto3
import pandas as pd

from redshift_client import RedshiftClient
from universe import in_universe, peer_universe_sql

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
logger = logging.getLogger(__name__)

PANELS = "earnings-market-data/panels"
import settings


def _rs() -> dict:
    """RedshiftClient kwargs for the FactSet datashare workgroup."""
    return dict(workgroup=settings.get("redshift", "workgroup"),
                database=settings.get("redshift", "database"),
                secret_arn=settings.get("redshift", "secret_arn"),
                region=settings.get("aws", "region"))


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
_CHUNK = 400   # max tickers per IN-list query


def _panel_keys(s3, start=None, end=None):
    """(date, key) for every event panel, optionally within [start, end]."""
    out = []
    token = None
    while True:
        kw = {"Bucket": _bucket(), "Prefix": f"{PANELS}/date="}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            key = o["Key"]
            if not key.endswith(".parquet"):
                continue
            day = key.split("date=", 1)[1].split("/", 1)[0]
            if (start and day < start) or (end and day > end):
                continue
            out.append((day, key))
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return sorted(out)


def _reporter_and_peers(s3, key):
    """(reporter_symbol, [peer_symbols]) from one panel."""
    body = s3.get_object(Bucket=_bucket(), Key=key)["Body"].read()
    df = pd.read_parquet(io.BytesIO(body), columns=["symbol", "reporter_relationship"])
    rep = df.loc[df["reporter_relationship"] == "primary", "symbol"].unique().tolist()
    reporter = rep[0] if rep else None
    peers = sorted(set(df["symbol"].unique()) - {reporter})
    return reporter, peers


def _resolve_universe(rs, symbols):
    """{bare_symbol: {listing_exchange, reg_type, prim_type, in_universe}} for
    every resolvable symbol (batched). Missing key = unresolved."""
    syms = sorted(symbols)
    attrs = {}
    for i in range(0, len(syms), _CHUNK):
        chunk = syms[i:i + _CHUNK]
        for r in rs.fetch_all(peer_universe_sql(chunk), timeout_s=180):
            tr = (r.get("ticker_region") or "")
            if tr.endswith("-US"):
                exch = r.get("listing_exchange")
                reg, prim = r.get("reg_security_type"), r.get("prim_security_type")
                attrs[tr[:-3]] = {"listing_exchange": exch,
                                  "reg_type": reg, "prim_type": prim,
                                  "in_universe": in_universe(exch, reg, prim)}
    return attrs


def audit(start=None, end=None, boto3_session=None):
    session = boto3_session or boto3.Session(region_name="us-east-1")
    s3 = session.client("s3")
    rs = RedshiftClient(**_rs(), boto3_session=session)

    keys = _panel_keys(s3, start, end)
    logger.info("scanning %d panels ...", len(keys))

    per_event = []            # (date, event_id, reporter, peers)
    all_symbols = set()
    for day, key in keys:
        eid = key.rsplit("event_", 1)[1].split(".", 1)[0]
        reporter, peers = _reporter_and_peers(s3, key)
        per_event.append((day, eid, reporter, peers))
        all_symbols.update(peers)
        if reporter:
            all_symbols.add(reporter)

    attrs = _resolve_universe(rs, all_symbols)   # bare -> {exch,reg,prim,in_universe}

    def _bad(sym):
        a = attrs.get(sym)
        return a is not None and not a["in_universe"]

    def _row(day, eid, role, sym, reporter):
        a = attrs[sym]
        return {"date": day, "event_id": eid, "role": role, "symbol": sym,
                "reporter": reporter, "listing_exchange": a["listing_exchange"],
                "reg_security_type": a["reg_type"],
                "prim_security_type": a["prim_type"]}

    rows = []                 # flat rows for CSV: one per flagged symbol
    bad_reporters = []
    bad_peers = defaultdict(list)
    for day, eid, reporter, peers in per_event:
        if reporter and _bad(reporter):
            bad_reporters.append((day, eid, reporter))
            rows.append(_row(day, eid, "reporter", reporter, reporter))
        for p in peers:
            if _bad(p):
                bad_peers[(day, eid, reporter)].append(p)
                rows.append(_row(day, eid, "peer", p, reporter))

    return {"panels": len(keys), "distinct_symbols": len(all_symbols),
            "unresolved_symbols": sorted(s for s in all_symbols if s not in attrs),
            "bad_reporters": bad_reporters, "bad_peers": dict(bad_peers),
            "rows": rows}


def _print_report(rep):
    print("=" * 68)
    print(f"UNIVERSE AUDIT — {rep['panels']} panels, "
          f"{rep['distinct_symbols']} distinct symbols")
    print("=" * 68)

    print(f"\nERRONEOUS REPORTERS (now out-of-universe): {len(rep['bad_reporters'])}")
    for day, eid, sym in rep["bad_reporters"]:
        print(f"  {day}  event_{eid}  {sym}")

    n_peer_panels = len(rep["bad_peers"])
    n_peer_syms = sum(len(v) for v in rep["bad_peers"].values())
    print(f"\nPANELS WITH OUT-OF-UNIVERSE PEERS: {n_peer_panels} "
          f"({n_peer_syms} peer entries)")
    for (day, eid, reporter), offenders in sorted(rep["bad_peers"].items()):
        print(f"  {day}  event_{eid}  ({reporter}): {offenders}")

    if rep["unresolved_symbols"]:
        print(f"\nUNRESOLVED (not in FactSet ticker_region; kept, not flagged): "
              f"{len(rep['unresolved_symbols'])}")
        print("  " + ", ".join(rep["unresolved_symbols"]))


def _write_csv(rows, path):
    import csv
    cols = ["date", "event_id", "role", "symbol", "reporter",
            "listing_exchange", "reg_security_type", "prim_security_type"]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in sorted(rows, key=lambda x: (x["date"], x["event_id"],
                                             x["role"], x["symbol"])):
            w.writerow(r)


def main():
    p = argparse.ArgumentParser(description="Audit panels vs universe")
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--profile", default=None)
    p.add_argument("--csv", help="write flagged reporters/peers to this CSV path")
    p.add_argument("--json", action="store_true", help="emit raw JSON too")
    args = p.parse_args()
    session = boto3.Session(profile_name=args.profile, region_name="us-east-1") \
        if args.profile else boto3.Session(region_name="us-east-1")
    rep = audit(args.start, args.end, boto3_session=session)
    _print_report(rep)
    if args.csv:
        _write_csv(rep["rows"], args.csv)
        logger.info("wrote %d flagged rows to %s", len(rep["rows"]), args.csv)
    if args.json:
        print("\n" + json.dumps({k: v for k, v in rep.items() if k != "rows"},
                                indent=2, default=list))


if __name__ == "__main__":
    sys.exit(main())
