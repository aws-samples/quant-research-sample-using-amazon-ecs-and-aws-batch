"""Earnings market-data CLI — date-driven entry point.

Subcommands:
  list-dates --start --end   Print the XNYS trading-day fan-out list for a range.
  events --date YYYY-MM-DD    Fetch-ready US ER events for one date.
  fetch --date YYYY-MM-DD     For each event on the date: build the basket
                              (reporter + on-the-fly cached peers) and fetch its
                              1-min OHLCV panel to S3. PAID (Databento).
                              (date-sharded; superseded by plan/fetch-event.)
  estimate-cost --date        Same iteration, free get_cost only -> total $.

  plan --start --end          STEP ZERO: enumerate every ER event across the
                              range into an ordered S3 manifest, then submit one
                              Batch array job of that size running fetch-event.
                              Sharding unit = one EVENT.

                              Optional --tickers: filter to specific tickers
                              (e.g., --tickers APA COP DVN for XLE subset).

  fetch-event --manifest KEY  Array child: fetch the basket panel for
                              manifest.events[AWS_BATCH_JOB_ARRAY_INDEX]. PAID.

  --date resolves from the flag or, in a Batch array job, from
  AWS_BATCH_JOB_ARRAY_INDEX mapped against the --start/--end trading-day list.

Examples:
  # All events in 2025
  python main.py plan --start 2025-01-01 --end 2025-12-31 --source alpaca

  # XLE energy sector events only (2007-2026)
  python main.py plan --start 2007-01-01 --end 2026-12-31 \
    --tickers APA COP DVN EOG EQT EXE HAL MPC SLB TPL VLO \
    --source alpaca
"""

import argparse
import json
import os
import sys

import boto3

from logging_config import configure_logging

import settings


def _rs() -> dict:
    """RedshiftClient kwargs for the FactSet datashare workgroup."""
    return dict(workgroup=settings.get("redshift", "workgroup"),
                database=settings.get("redshift", "database"),
                secret_arn=settings.get("redshift", "secret_arn"),
                region=settings.get("aws", "region"))


def _bucket() -> str:
    return settings.get("s3", "data_bucket")

OUT_PREFIX = "earnings-market-data"


def _panel_prefix(dataset: str) -> str:
    """Panels are segregated by Databento dataset so feeds never mix: e.g.
    earnings-market-data/XNAS.BASIC/panels/date=.../. Peer cache and manifests
    stay on the shared OUT_PREFIX (dataset-independent). Legacy DBEQ.BASIC panels
    remain at the un-scoped earnings-market-data/panels/ and are left untouched."""
    return f"{OUT_PREFIX}/{dataset}"

# Batch target for the array job the `plan` pre-pass submits: settings
# batch.job_queue / batch.job_definitions.market_data.


def _make_client(source: str, session):
    """Market-data source: alpaca (default, free SIP) or databento (plan B,
    paid; still honors EMD_DATASET). Same six-member surface either way —
    the orchestration never knows which it got."""
    if source == "alpaca":
        from alpaca_client import AlpacaClient
        return AlpacaClient(boto3_session=session)
    if source == "databento":
        from databento_client import DatabentoClient
        return DatabentoClient(boto3_session=session)
    raise SystemExit(f"unknown --source {source!r} (alpaca | databento)")


def _resolve_date(args) -> str:
    if args.date:
        return args.date
    idx = os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX")
    if idx is not None:
        from sharding import trading_days
        if not (args.start and args.end):
            raise SystemExit("array-index mode needs --start/--end to map index->date")
        dates = trading_days(args.start, args.end)
        i = int(idx)
        if i >= len(dates):
            raise SystemExit(f"array index {i} out of range ({len(dates)} dates)")
        return dates[i]
    raise SystemExit("events requires --date or AWS_BATCH_JOB_ARRAY_INDEX")


def _cmd_plan(args, session) -> int:
    """Step zero: build the event manifest and submit the sized array job."""
    if not (args.start and args.end):
        raise SystemExit("plan requires --start and --end")
    from redshift_client import RedshiftClient
    from manifest import build_manifest, write_manifest

    rs = RedshiftClient(**_rs(), boto3_session=session)
    s3 = session.client("s3")
    m = build_manifest(rs, args.start, args.end, ticker_filter=args.tickers)
    key = write_manifest(s3, _bucket(), OUT_PREFIX, m)

    n = m["event_count"]
    if n == 0:
        print(json.dumps({"manifest": key, "event_count": 0,
                          "submitted": False, "reason": "no events"}, indent=2))
        return 0

    if args.dry_run:
        print(json.dumps({"manifest": key, "event_count": n,
                          "submitted": False, "reason": "dry-run"}, indent=2))
        return 0

    batch = session.client("batch")
    resp = batch.submit_job(
        jobName=f"emd-events-{args.start}-{args.end}",
        jobQueue=settings.get("batch", "job_queue"),
        jobDefinition=settings.get("batch", "job_definitions", "market_data"),
        arrayProperties={"size": n},
        containerOverrides={"command": ["fetch-event", "--manifest", key,
                                        "--source", args.source]},
    )
    print(json.dumps({"manifest": key, "event_count": n, "submitted": True,
                      "array_size": n, "job_id": resp["jobId"],
                      "job_queue": settings.get("batch", "job_queue")}, indent=2))
    return 0


def _cmd_fetch_event(args, session) -> int:
    """Array child: fetch the one event at AWS_BATCH_JOB_ARRAY_INDEX."""
    if not args.manifest:
        raise SystemExit("fetch-event requires --manifest")
    idx = os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX")
    if idx is None:
        raise SystemExit("fetch-event requires AWS_BATCH_JOB_ARRAY_INDEX")
    from manifest import read_manifest
    from peer_cache import PeerCache
    from peer_deriver import PeerDeriver
    from redshift_client import RedshiftClient
    from basket_fetch import run_event

    s3 = session.client("s3")
    m = read_manifest(s3, _bucket(), args.manifest)
    i = int(idx)
    events = m["events"]
    if i >= len(events):
        raise SystemExit(f"array index {i} out of range ({len(events)} events)")
    entry = events[i]

    db = _make_client(args.source, session)
    cache = PeerCache(_bucket(), OUT_PREFIX, boto3_session=session)  # shared cache
    rs = RedshiftClient(**_rs(), boto3_session=session)
    deriver = PeerDeriver(session, db, rs=rs)   # rs enables ETF peer filtering
    # panels segregated by dataset so DBEQ.BASIC and XNAS.BASIC never mix
    result = run_event(entry, _bucket(), _panel_prefix(db.dataset),
                       db, cache, deriver, s3)
    result["dataset"] = db.dataset
    print(json.dumps(result, indent=2))
    return 0


def main():
    p = argparse.ArgumentParser(description="Earnings market-data entry point")
    p.add_argument("command",
                   choices=["list-dates", "events", "fetch", "estimate-cost",
                            "plan", "fetch-event"])
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--date")
    p.add_argument("--manifest")
    p.add_argument("--source", default="alpaca", choices=["alpaca", "databento"],
                   help="market-data source (default alpaca; databento = plan B)")
    p.add_argument("--dry-run", action="store_true",
                   help="plan: build+write manifest but do not submit the array job")
    p.add_argument("--tickers", nargs='+', default=None,
                   help="Optional: filter events to specific tickers (e.g., --tickers APA COP DVN)")
    p.add_argument("--profile", default=None)
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args()

    configure_logging(args.log_level)

    if args.command == "list-dates":
        from sharding import trading_days
        if not (args.start and args.end):
            raise SystemExit("list-dates requires --start and --end")
        dates = trading_days(args.start, args.end)
        print(json.dumps({"count": len(dates), "dates": dates}, indent=2))
        return 0

    session = boto3.Session(profile_name=args.profile, region_name=settings.get("aws", "region")) \
        if args.profile else boto3.Session(region_name=settings.get("aws", "region"))

    if args.command == "plan":
        return _cmd_plan(args, session)

    if args.command == "fetch-event":
        return _cmd_fetch_event(args, session)

    from redshift_client import RedshiftClient

    day = _resolve_date(args)
    rs = RedshiftClient(**_rs(), boto3_session=session)

    if args.command == "events":
        from event_source import events_for_date
        rows = events_for_date(rs, day)
        print(json.dumps({"date": day, "count": len(rows),
                          "events": [r.to_dict() for r in rows]}, indent=2))
        return 0

    # fetch / estimate-cost: build baskets and fetch (or price) their panels
    from peer_cache import PeerCache
    from peer_deriver import PeerDeriver
    from basket_fetch import run_date

    db = _make_client(args.source, session)
    cache = PeerCache(_bucket(), OUT_PREFIX, boto3_session=session)  # shared cache
    deriver = PeerDeriver(session, db)
    result = run_date(day, _bucket(), _panel_prefix(db.dataset), rs, db, cache,
                      deriver, session.client("s3"),
                      estimate_only=(args.command == "estimate-cost"))
    result["dataset"] = db.dataset
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
