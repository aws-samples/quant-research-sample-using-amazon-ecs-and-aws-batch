"""Per-date orchestration: for each ER event on a date, build its basket
(reporter + cached/derived peers), fetch the panel, write it to S3.

Panel destination:
  s3://<bucket>/<prefix>/panels/date=YYYY-MM-DD/event_<id>.parquet
where <prefix> is dataset-scoped (e.g. earnings-market-data/XNAS.BASIC) so feeds
never mix. The peer cache (symbol-keyed) is passed in separately on the shared
prefix — peer relationships are dataset-independent.
"""

import io
import logging
from typing import List

import boto3

from databento_client import DatabentoClient
from event_source import events_for_date
from panel_builder import build_panel
from peer_cache import PeerCache
from peer_deriver import PeerDeriver
from redshift_client import RedshiftClient

logger = logging.getLogger(__name__)


def _panel_key(prefix: str, day: str, event_id: int) -> str:
    return f"{prefix}/panels/date={day}/event_{event_id}.parquet"


def _unrecoverable_key(prefix: str, day: str, event_id: int) -> str:
    return f"{prefix}/panels/date={day}/event_{event_id}.unrecoverable.json"


def _partial_key(prefix: str, day: str, event_id: int) -> str:
    """Marker: panel written from a CLAMPED window (dataset availability ended
    inside [-2,+5]). Records fetched_end/window_end so a later run tops up just
    the missing tail. Presence of this marker = panel incomplete by design."""
    return f"{prefix}/panels/date={day}/event_{event_id}.partial.json"


def _load_unrecoverable(s3, bucket: str, key: str) -> dict:
    """{symbol: reason} of symbols permanently marked unfetchable for an event."""
    import json
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception:
        return {}


def _window_closed(event_dt: str) -> bool:
    """True when the event's [-2,+5] window is fully in the past — an empty
    fetch can then never improve, so the symbol is permanently unrecoverable."""
    import pandas as pd
    from sharding import trading_window
    _, end = trading_window(event_dt)
    return pd.Timestamp(end, tz="UTC") <= pd.Timestamp.now(tz="UTC")


def _rename_resolver(db, deriver):
    """RenameResolver on the deriver's Bedrock client (lazy; separate hook so
    tests can inject). None when the deriver has no bedrock client."""
    bedrock = getattr(deriver, "bedrock", None)
    if bedrock is None:
        return None
    from rename_resolver import RenameResolver
    return RenameResolver(bedrock, db)


def _heal_head_gaps(fetched, names, db, deriver, event_id, win_start):
    """Backfill symbols whose data starts after the window start (ticker
    renamed mid-window: the VSXY/VSCO case) from their verified previous
    ticker. `names` maps symbol -> company name (only the reporter's is
    known from the manifest; peers fall back to the bare symbol).
    Returns (fetched_with_heads, healed_symbols)."""
    import pandas as pd
    from rename_resolver import head_gap

    real = fetched[fetched["open"].notna()]
    if real.empty:
        return fetched, []
    resolver = _rename_resolver(db, deriver)
    if resolver is None:
        return fetched, []
    healed = []
    heads = []
    ts = pd.to_datetime(real["ts"], utc=True, errors="coerce")
    real = real[ts.notna()]
    if real.empty:
        return fetched, []
    first_days = (real.assign(d=ts[ts.notna()].dt.strftime("%Y-%m-%d"))
                  .groupby("symbol")["d"].min())
    for sym, first_day in first_days.items():
        gap = head_gap(first_day, (win_start, first_day))
        if gap is None:
            continue
        head = resolver.fetch_head(sym, names.get(sym) or sym, *gap)
        if head is not None and len(head):
            # stamp the panel-schema columns from the symbol's fetched rows
            # (raw get_range output lacks them; NA event_bar breaks masks)
            proto = real[real["symbol"] == sym].iloc[0]
            head = head.copy()
            head["reporter_relationship"] = proto.get("reporter_relationship")
            head["event_id"] = proto.get("event_id", event_id)
            head["event_bar"] = False
            heads.append(head)
            healed.append(sym)
            logger.info("event %s: healed head gap for %s (%d bars via "
                        "previous ticker)", event_id, sym, len(head))
    if heads:
        fetched = pd.concat([fetched] + heads, ignore_index=True)
    return fetched, healed


def _basket_peers(cache: PeerCache, deriver: PeerDeriver, symbol: str,
                  name: str) -> dict:
    entry = cache.get(symbol)
    # non-empty 'dropped' marks a pre-fix cache entry whose peers were culled
    # by the fixed-date resolve() bug (e.g. AIOT) — re-derive once to heal it
    if entry is None or entry.get("dropped"):
        if entry is not None:
            logger.info("stale peer cache for %s (dropped=%s): re-deriving",
                        symbol, entry.get("dropped"))
        entry = deriver.derive(symbol, name)
        cache.put(symbol, entry)          # cache even empty results (avoids re-deriving)
        logger.info("derived peers for %s: %s", symbol,
                    {k: len(v) for k, v in entry["peers"].items()})
    return entry["peers"]


def _existing_event_ids(s3, bucket: str, prefix: str, day: str) -> set:
    """event_ids whose panel already exists in S3 (idempotent resume)."""
    done = set()
    token = None
    pfx = f"{prefix}/panels/date={day}/"
    while True:
        kw = {"Bucket": bucket, "Prefix": pfx}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            name = o["Key"].rsplit("/", 1)[-1]
            if name.startswith("event_") and name.endswith(".parquet"):
                try:
                    done.add(int(name[len("event_"):-len(".parquet")]))
                except ValueError:
                    pass
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return done


def run_event(entry: dict, bucket: str, prefix: str,
              db: DatabentoClient, cache: PeerCache, deriver: PeerDeriver,
              s3, estimate_only: bool = False) -> dict:
    """Fetch (or price) a single event's basket panel — one array shard.

    `entry` is one manifest event dict (event_id, date, symbol,
    entity_proper_name, event_datetime_utc, ...). Idempotent: if the panel
    already exists it is skipped. A failure here isolates to this shard.
    """
    day = entry["date"]
    event_id = int(entry["event_id"])
    symbol = entry["symbol"]
    name = entry.get("entity_proper_name")
    event_dt = entry["event_datetime_utc"]

    key = _panel_key(prefix, day, event_id)
    peers = _basket_peers(cache, deriver, symbol, name)
    basket = [(symbol, "primary")] + [
        (s, rel) for rel, syms in peers.items() for s in syms if s != symbol]

    if estimate_only:
        from sharding import trading_window
        start, end = trading_window(event_dt)
        total_cost = 0.0
        for s, _rel in basket:
            try:
                total_cost += db.get_cost(s, start, end)
            except Exception as e:
                logger.warning("get_cost failed %s: %s", s, e)
        return {"event_id": event_id, "date": day, "symbol": symbol,
                "basket_size": len(basket),
                "estimated_cost_usd": round(total_cost, 4)}

    # Availability clamp (partial-fetch policy): when the dataset's available
    # end falls inside [-2,+5], fetch whatever exists as long as -2..+1 is
    # fully covered; defer the event otherwise. Fail-open on metadata errors.
    import json
    import pandas as pd
    from sharding import clamp_window, trading_window

    clamped_win = clamp_window(event_dt, db.available_end())
    if clamped_win is None:
        logger.info("event %s (%s %s): dataset availability does not yet "
                    "cover -2..+1 — deferred", event_id, symbol, day)
        return {"event_id": event_id, "date": day, "symbol": symbol,
                "deferred": True}
    win_start, win_end, clamped = clamped_win
    full_end = trading_window(event_dt)[1]

    # Per-symbol resume: an existing panel is completed, not skipped wholesale.
    # A symbol counts as fetched if it has any REAL (non-synthetic) rows;
    # synthetic-only symbols (all-null marker rows) are refetched — cheap when
    # data still doesn't exist (empty response = $0) and self-healing when the
    # earlier fetch failed or the peer set has since grown.
    unrec_key = _unrecoverable_key(prefix, day, event_id)
    unrecoverable = _load_unrecoverable(s3, bucket, unrec_key)
    partial_key = _partial_key(prefix, day, event_id)

    existing = None
    todo = basket
    tail_topup = None                    # (tail_start, tail_end) when set
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        existing = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        real = set(existing.loc[existing["open"].notna(), "symbol"])
        done = real | set(unrecoverable)
        todo = [(s, rel) for s, rel in basket if s not in done]

        # pending partial marker: the panel's head is real but its tail is
        # missing — top up [fetched_end, min(window_end, now-available))
        partial = _load_unrecoverable(s3, bucket, partial_key)  # same loader: {} if absent
        if partial:
            tail_start = partial["fetched_end"]
            tail_end = min(win_end, partial["window_end"])
            if tail_end <= tail_start:
                logger.info("event %s (%s %s): partial panel, no new "
                            "availability (fetched to %s) — waiting",
                            event_id, symbol, day, tail_start)
                return {"event_id": event_id, "date": day, "symbol": symbol,
                        "partial_pending": True}
            tail_topup = (tail_start, tail_end)
        elif not todo:
            logger.info("event %s (%s %s): panel complete (%d real, %d "
                        "unrecoverable), skipping",
                        event_id, symbol, day, len(real), len(unrecoverable))
            return {"event_id": event_id, "date": day, "symbol": symbol,
                    "skipped_existing": True}
        if todo and not tail_topup:
            logger.info("event %s (%s %s): panel exists but missing %d/%d "
                        "symbols (%s) — fetching just those",
                        event_id, symbol, day, len(todo), len(basket),
                        ",".join(s for s, _ in todo))
    except s3.exceptions.NoSuchKey:
        pass  # no panel -> fetch the full basket
    except Exception:
        pass  # unreadable panel -> refetch the full basket, overwrite

    from panel_builder import build_panel_symbols
    if tail_topup:
        # whole basket over just the missing tail; head rows already carry
        # the event markers, so none are added here
        fetched = build_panel_symbols(db, event_id, event_dt, basket,
                                      window=tail_topup, event_marker=False)
        todo = basket
        rename_healed = []
    else:
        fetched = build_panel_symbols(db, event_id, event_dt, todo,
                                      window=(win_start, win_end))
        # rename healing: a symbol whose data starts after the window start
        # was likely renamed mid-window — backfill its head from the
        # LLM-proposed, Databento-verified previous ticker (VSXY/VSCO case)
        fetched, rename_healed = _heal_head_gaps(
            fetched, {symbol: name}, db, deriver, event_id, win_start)

    # Symbols still empty after this fetch: only a FULL-window fetch may
    # tombstone (clamped/tail fetches can't distinguish "never trades" from
    # "not yet published"), and only once the window is fully in the past.
    got = set(fetched.loc[fetched["open"].notna(), "symbol"])
    still_empty = [s for s, _rel in todo if s not in got]
    may_tombstone = (still_empty and not clamped and not tail_topup
                     and _window_closed(event_dt))
    if may_tombstone:
        for s in still_empty:
            unrecoverable.setdefault(s, "empty fetch after window closed")
        s3.put_object(Bucket=bucket, Key=unrec_key,
                      Body=json.dumps(unrecoverable, indent=1).encode())
        logger.info("event %s (%s %s): marked unrecoverable: %s",
                    event_id, symbol, day, ",".join(sorted(still_empty)))

    if existing is not None:
        if tail_topup:
            # append-only: keep every existing row, add the tail's new bars
            panel = pd.concat([existing, fetched], ignore_index=True)
        else:
            # drop the todo symbols' old (synthetic) rows, append fresh ones
            keep = existing[~existing["symbol"].isin({s for s, _ in todo})]
            panel = pd.concat([keep, fetched], ignore_index=True)
    else:
        panel = fetched
    panel = panel.sort_values(
        ["symbol", "ts", "publisher_id"]).reset_index(drop=True)

    buf = io.BytesIO()
    panel.to_parquet(buf, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

    # partial bookkeeping: a clamped fetch records how far it got; a fetch
    # that reached the full window end clears any pending marker
    now_partial = win_end < full_end
    if now_partial:
        s3.put_object(Bucket=bucket, Key=partial_key,
                      Body=json.dumps({"fetched_end": win_end,
                                       "window_end": full_end}, indent=1).encode())
        logger.info("event %s (%s %s): PARTIAL panel to %s (window end %s)",
                    event_id, symbol, day, win_end, full_end)
    elif tail_topup:
        s3.delete_object(Bucket=bucket, Key=partial_key)

    logger.info("event %s (%s %s): wrote panel (%d symbols, %d rows, %d fetched)",
                event_id, symbol, day, panel["symbol"].nunique(), len(panel),
                len(todo))
    return {"event_id": event_id, "date": day, "symbol": symbol,
            "basket_size": len(basket), "rows": len(panel),
            "symbols_fetched": len(todo),
            "partial": now_partial, "fetched_end": win_end,
            "rename_healed": rename_healed,
            "marked_unrecoverable": sorted(still_empty) if may_tombstone else [],
            "panels_written": 1}


def run_date(day: str, bucket: str, prefix: str,
             rs: RedshiftClient, db: DatabentoClient,
             cache: PeerCache, deriver: PeerDeriver,
             s3, estimate_only: bool = False) -> dict:
    events = events_for_date(rs, day)
    logger.info("date %s: %d events", day, len(events))

    done = set() if estimate_only else _existing_event_ids(s3, bucket, prefix, day)
    if done:
        logger.info("date %s: %d panels already exist, skipping them", day, len(done))

    total_cost = 0.0
    written = 0
    skipped = 0
    for ev in events:
        if not ev.symbol:
            continue
        if ev.event_id in done:
            skipped += 1
            continue
        peers = _basket_peers(cache, deriver, ev.symbol, ev.entity_proper_name)
        basket_syms = [ev.symbol] + [s for v in peers.values() for s in v]

        if estimate_only:
            from sharding import trading_window
            start, end = trading_window(ev.event_datetime_utc)
            for s in basket_syms:
                try:
                    total_cost += db.get_cost(s, start, end)
                except Exception as e:
                    logger.warning("get_cost failed %s: %s", s, e)
            continue

        panel = build_panel(db, ev.event_id, ev.event_datetime_utc,
                            ev.symbol, peers)
        buf = io.BytesIO()
        panel.to_parquet(buf, index=False)
        s3.put_object(Bucket=bucket, Key=_panel_key(prefix, day, ev.event_id),
                      Body=buf.getvalue())
        written += 1

    if estimate_only:
        logger.info("date %s ESTIMATE: $%.4f across %d events", day, total_cost, len(events))
        return {"date": day, "events": len(events), "estimated_cost_usd": round(total_cost, 4)}
    logger.info("date %s: wrote %d panels (%d already existed)", day, written, skipped)
    return {"date": day, "events": len(events),
            "panels_written": written, "skipped_existing": skipped}
