"""Refresh already-collected panels to the current universe (universe.py),
WITHOUT deleting anything not proven erroneous.

Given a date range this driver reconciles what FactSet now returns (corrected
universe) against what is in S3, and produces a four-part plan:

  ADD reporters   - events now in-universe with NO panel        -> fetch (Batch)
  DELETE reporters- panels whose event is NO LONGER in-universe -> delete (S3)
  REFRESH peers   - force re-derive every in-range reporter's    -> cache delete
                    peer set with the corrected filter              + Batch refetch
  PRUNE peers     - panel rows for peer symbols no longer in the -> rewrite panel
                    (freshly derived) basket                        (drop rows)

Dry-run by default: prints the full plan and touches nothing. Pass --apply to
execute. Deletes/prunes happen locally over S3; adds/refreshes are submitted as
the event-sharded plan/fetch-event Batch array (reusing the existing image).

The refresh itself relies on run_event's per-symbol resume (already in place):
once a reporter's peer cache is invalidated and a fetch-event shard runs, newly
added peers are fetched and previously-synthetic symbols are refreshed. This
driver adds only the two things run_event does NOT do: delete stale reporter
panels and prune stale peer rows.
"""

import argparse
import io
import json
import logging
import sys

import boto3
import pandas as pd

from redshift_client import RedshiftClient
from event_source import events_for_date
from peer_cache import PeerCache
from peer_deriver import PeerDeriver
from databento_client import DatabentoClient
from sharding import trading_days
from universe import in_universe, peer_universe_sql

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
logger = logging.getLogger(__name__)

PREFIX = "earnings-market-data"
import settings


def _rs() -> dict:
    """RedshiftClient kwargs for the FactSet datashare workgroup."""
    return dict(workgroup=settings.get("redshift", "workgroup"),
                database=settings.get("redshift", "database"),
                secret_arn=settings.get("redshift", "secret_arn"),
                region=settings.get("aws", "region"))


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
_CHUNK = 400


def _panel_key(day, eid):
    return f"{PREFIX}/panels/date={day}/event_{eid}.parquet"


def _list_panels(s3, day):
    """{event_id: key} for every panel on a date."""
    out = {}
    token = None
    pfx = f"{PREFIX}/panels/date={day}/"
    while True:
        kw = {"Bucket": _bucket(), "Prefix": pfx}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            nm = o["Key"].rsplit("/", 1)[-1]
            if nm.startswith("event_") and nm.endswith(".parquet"):
                try:
                    out[int(nm[len("event_"):-len(".parquet")])] = o["Key"]
                except ValueError:
                    pass
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return out


def _resolve(rs, symbols):
    """{bare: in_universe bool} for resolvable symbols (missing = unresolved)."""
    syms = sorted({s for s in symbols if s})
    status = {}
    for i in range(0, len(syms), _CHUNK):
        for r in rs.fetch_all(peer_universe_sql(syms[i:i + _CHUNK]), timeout_s=180):
            tr = r.get("ticker_region") or ""
            if tr.endswith("-US"):
                status[tr[:-3]] = in_universe(r.get("listing_exchange"),
                                              r.get("reg_security_type"),
                                              r.get("prim_security_type"))
    return status


def plan_date(rs, s3, cache, deriver, day):
    """Compute the four-part refresh plan for one date (no side effects)."""
    fs_events = {e.event_id: e for e in events_for_date(rs, day)}  # already universe-filtered
    panels = _list_panels(s3, day)

    add_reporters = sorted(set(fs_events) - set(panels))        # in FS, no panel

    # A panel must exist ONLY if its event is currently a confirmed
    # (projected=false), in-universe ER event. Everything in (panels - fs_events)
    # is deleted. Two causes, BOTH warrant deletion:
    #   (a) reporter out-of-universe (ADR/PREF)  -> panel never belonged
    #   (b) event now projected=true             -> its datetime is a placeholder
    #       (midnight); the panel's [-2,+5] window is anchored to a possibly
    #       WRONG day, so the data is untrustworthy. Delete; a later confirmed
    #       re-fetch rebuilds it around the correct time.
    # Label each deletion by cause for the report (diagnostic only).
    delete_reporters = sorted(set(panels) - set(fs_events))
    cand_reporter = {}
    for eid in delete_reporters:
        try:
            df = pd.read_parquet(io.BytesIO(
                s3.get_object(Bucket=_bucket(), Key=panels[eid])["Body"].read()),
                columns=["symbol", "reporter_relationship"])
            rep = df.loc[df["reporter_relationship"] == "primary", "symbol"].unique()
            if len(rep):
                cand_reporter[eid] = rep[0]
        except Exception as e:
            logger.warning("read candidate panel %s failed: %s", eid, e)
    cand_status = _resolve(rs, set(cand_reporter.values()))
    delete_reason = {}
    for eid in delete_reporters:
        sym = cand_reporter.get(eid)
        delete_reason[eid] = ("out-of-universe" if cand_status.get(sym) is False
                              else "projected-or-absent")

    # For events that stay: force-refresh peers, and prune ONLY panel symbols
    # that are positively OUT-OF-UNIVERSE (ETFs/ADRs) — never symbols that a
    # re-derivation merely happens not to repeat (LLM output varies run-to-run;
    # those are valid data). Prune criterion == universe filter, so it matches
    # the audit and is deterministic.
    refresh_peers = []     # (event_id, reporter_symbol)
    prune = {}             # event_id -> [out-of-universe peer symbols to drop]
    # collect all panel symbols across staying events, resolve once (batched)
    panel_syms_by_event = {}
    all_panel_syms = set()
    for eid in sorted(set(fs_events) & set(panels)):
        refresh_peers.append((eid, fs_events[eid].symbol))
        try:
            df = pd.read_parquet(io.BytesIO(
                s3.get_object(Bucket=_bucket(), Key=panels[eid])["Body"].read()),
                columns=["symbol"])
            syms = set(df["symbol"].unique())
            panel_syms_by_event[eid] = syms
            all_panel_syms |= syms
        except Exception as e:
            logger.warning("read panel %s failed: %s", eid, e)
    status = _resolve(rs, all_panel_syms)   # bare -> in_universe bool
    for eid, syms in panel_syms_by_event.items():
        rep = fs_events[eid].symbol
        # drop a peer only if POSITIVELY out-of-universe; never the reporter,
        # never an unresolved symbol (status.get is False only when resolved bad)
        stale = sorted(s for s in syms
                       if s != rep and status.get(s) is False)
        if stale:
            prune[eid] = stale
    return {"date": day, "add_reporters": add_reporters,
            "delete_reporters": delete_reporters, "delete_reason": delete_reason,
            "refresh_peers": refresh_peers,
            "prune": prune, "panels": panels, "events": fs_events}


def apply_deletes_and_prunes(s3, cache, plan):
    """Execute the destructive part locally: delete stale reporter panels,
    prune stale peer rows, invalidate peer caches for refresh."""
    day = plan["date"]
    # delete stale reporter panels
    for eid in plan["delete_reporters"]:
        s3.delete_object(Bucket=_bucket(), Key=plan["panels"][eid])
        logger.info("DELETED reporter panel event_%s (%s)", eid, day)
    # prune stale peer rows
    for eid, stale in plan["prune"].items():
        key = plan["panels"][eid]
        df = pd.read_parquet(io.BytesIO(
            s3.get_object(Bucket=_bucket(), Key=key)["Body"].read()))
        kept = df[~df["symbol"].isin(stale)].reset_index(drop=True)
        buf = io.BytesIO(); kept.to_parquet(buf, index=False)
        s3.put_object(Bucket=_bucket(), Key=key, Body=buf.getvalue())
        logger.info("PRUNED event_%s: dropped %d peers %s", eid, len(stale), stale)
    # invalidate peer caches so the Batch refetch re-derives with the new filter
    for _eid, sym in plan["refresh_peers"]:
        cache.delete(sym)


def _print_plan(plans):
    tot = lambda k: sum(len(p[k]) for p in plans)
    print("=" * 64)
    print("REFRESH PLAN")
    print("=" * 64)
    for p in plans:
        pr = sum(len(v) for v in p["prune"].values())
        print(f"\n{p['date']}:  +{len(p['add_reporters'])} reporters  "
              f"-{len(p['delete_reporters'])} reporters  "
              f"~{len(p['refresh_peers'])} peer-refresh  "
              f"prune {pr} peer rows")
        for eid in p["delete_reporters"]:
            print(f"    DELETE event_{eid} ({p['delete_reason'].get(eid, '?')})")
        if p["add_reporters"]:
            print(f"    ADD:    {p['add_reporters']}")
        for eid, stale in p["prune"].items():
            print(f"    PRUNE event_{eid}: {stale}")
    print(f"\nTOTAL: +{tot('add_reporters')} reporters, "
          f"-{tot('delete_reporters')} reporters, "
          f"prune {sum(sum(len(v) for v in p['prune'].values()) for p in plans)} peer rows")


def main():
    p = argparse.ArgumentParser(description="Refresh panels to current universe")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--profile", default=None)
    p.add_argument("--apply", action="store_true",
                   help="execute deletes/prunes + submit Batch refresh (default: dry-run)")
    args = p.parse_args()

    session = boto3.Session(profile_name=args.profile, region_name="us-east-1") \
        if args.profile else boto3.Session(region_name="us-east-1")
    s3 = session.client("s3")
    rs = RedshiftClient(**_rs(), boto3_session=session)
    cache = PeerCache(_bucket(), PREFIX, boto3_session=session)
    deriver = PeerDeriver(session, DatabentoClient(boto3_session=session), rs=rs)

    days = trading_days(args.start, args.end)
    plans = [plan_date(rs, s3, cache, deriver, d) for d in days]
    _print_plan(plans)

    if not args.apply:
        print("\n(dry-run — pass --apply to execute deletes/prunes and submit refetch)")
        return 0

    for plan in plans:
        apply_deletes_and_prunes(s3, cache, plan)

    # submit the event-sharded refetch for the whole range: the plan pre-job
    # rebuilds the manifest from the corrected universe (adds new reporters),
    # and per-symbol resume fills newly-added peers + refreshes synthetic rows.
    batch = session.client("batch")
    resp = batch.submit_job(
        jobName=f"emd-refresh-{args.start}-{args.end}",
        jobQueue=settings.get("batch", "job_queue"),
        jobDefinition=settings.get("batch", "job_definitions", "market_data"),
        containerOverrides={"command": ["plan", "--start", args.start,
                                        "--end", args.end]})
    print(f"\nAPPLIED. Submitted refresh plan job {resp['jobId']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
