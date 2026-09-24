"""Prototype: extract Databento 1-min OHLCV around one earnings event.

End-to-end for a single event_id, to validate the approach before building
the full pipeline:
  1. look up the event in Redshift (timestamp + ticker)
  2. reduce ticker_region -> bare symbol (drop region; multiple regions all
     map to the same underlying ticker)
  3. compute the [-2, +5] trading-day window (XNYS calendar)
  4. free get_cost check
  5. paid get_range fetch of ohlcv-1m on DBEQ.BASIC
  6. validate the returned bars

Run:  .venv/bin/python proto_market_data.py 1203024235
"""

import io
import json
import sys
import boto3
import pandas as pd
import pandas_market_calendars as mcal
import requests

import settings
DATASET = "DBEQ.BASIC"
SCHEMA = "ohlcv-1m"
DAYS_BEFORE = 2
DAYS_AFTER = 5


def _session() -> boto3.Session:
    return boto3.Session(profile_name=settings.get("aws", "profile"),
                         region_name=settings.get("aws", "region"))


# ------------------------------------------------------------------ redshift
def lookup_event(event_id: int) -> dict:
    rs = _session().client("redshift-data")
    sql = f"""
    with c as (
      select e.event_id,
             e.event_datetime_utc::varchar as dt,
             ent.entity_proper_name,
             tr.ticker_region,
             row_number() over (partition by e.event_id
                                 order by tr.ticker_region asc nulls last) rn
      from factset_ce_events.evt_v1.ce_events e
      join factset_ce_events.evt_v1.ce_events_coverage ec on ec.event_id = e.event_id
      left join factset_ce_events.sym_v1.sym_entity ent on ent.factset_entity_id = ec.factset_entity_id
      left join factset_ce_events.evt_v1.ce_sec_entity cse on cse.factset_entity_id = ec.factset_entity_id
      left join factset_ce_events.sym_v1.sym_coverage sc on sc.fsym_id = cse.fsym_id
      left join factset_ce_events.sym_v1.sym_ticker_region tr on tr.fsym_id = sc.fsym_regional_id
      where e.event_id = {int(event_id)}
    )
    select event_id, dt, entity_proper_name, ticker_region from c where rn = 1
    """
    sid = rs.execute_statement(WorkgroupName=settings.get("redshift", "workgroup"),
                               Database=settings.get("redshift", "database"),
                               SecretArn=settings.get("redshift", "secret_arn"), Sql=sql)["Id"]
    import time
    while True:
        d = rs.describe_statement(Id=sid)
        if d["Status"] == "FINISHED":
            break
        if d["Status"] in ("FAILED", "ABORTED"):
            raise RuntimeError(d.get("Error"))
        time.sleep(1)
    rec = rs.get_statement_result(Id=sid)["Records"][0]
    cols = ["event_id", "dt", "entity_proper_name", "ticker_region"]
    vals = [list(c.values())[0] for c in rec]
    return dict(zip(cols, vals))


def bare_symbol(ticker_region: str) -> str:
    """Drop the FactSet region suffix; the underlying ticker is region-agnostic."""
    return ticker_region.rsplit("-", 1)[0] if ticker_region else ticker_region


# ------------------------------------------------------------ trading window
def trading_window(event_dt_utc: str) -> tuple[str, str]:
    event_date = pd.Timestamp(event_dt_utc[:10])
    xnys = mcal.get_calendar("XNYS")
    sched = xnys.schedule(
        start_date=event_date - pd.Timedelta(days=20),
        end_date=event_date + pd.Timedelta(days=20),
    )
    sessions = sched.index.normalize()
    # anchor = first trading session on or after the event date
    future = sessions[sessions >= event_date]
    anchor = future[0] if len(future) else sessions[sessions <= event_date][-1]
    pos = sessions.get_loc(anchor)
    start = sessions[max(0, pos - DAYS_BEFORE)]
    end = sessions[min(len(sessions) - 1, pos + DAYS_AFTER)]
    # get_range end is exclusive → add a day so the +5th session is included
    return start.strftime("%Y-%m-%d"), (end + pd.Timedelta(days=1)).strftime("%Y-%m-%d")


# ------------------------------------------------------------------ databento
def db_key() -> str:
    sm = _session().client("secretsmanager")
    return json.loads(sm.get_secret_value(SecretId=settings.get("secrets", "databento"))["SecretString"])["api_key"]


def db_get(endpoint: str, key: str, params: dict, stream=False):
    return requests.get(f"https://hist.databento.com/v0/{endpoint}",
                        auth=(key, ""), params=params, timeout=120, stream=stream)


def get_cost(key, symbol, start, end) -> float:
    r = db_get("metadata.get_cost", key, {
        "dataset": DATASET, "symbols": symbol, "schema": SCHEMA,
        "start": start, "end": end, "stype_in": "raw_symbol", "mode": "historical"})
    r.raise_for_status()
    return float(r.text)


def get_range(key, symbol, start, end) -> pd.DataFrame:
    r = db_get("timeseries.get_range", key, {
        "dataset": DATASET, "symbols": symbol, "schema": SCHEMA,
        "start": start, "end": end, "stype_in": "raw_symbol", "encoding": "csv"})
    r.raise_for_status()
    if not r.text.strip() or r.text.count("\n") <= 1:
        return pd.DataFrame()
    df = pd.read_csv(io.StringIO(r.text))
    for col in ("open", "high", "low", "close"):
        df[col] = df[col] / 1e9  # fixed-point -> dollars
    df["ts"] = pd.to_datetime(df["ts_event"], utc=True)
    return df


# ------------------------------------------------------------------ validate
def validate(df: pd.DataFrame, symbol: str, start: str, end: str, event_dt: str):
    print("\n=== VALIDATION ===")
    assert not df.empty, "no bars returned"
    ts = df["ts"]
    print(f"bars: {len(df)}")
    print(f"time span: {ts.min()} .. {ts.max()} (window {start} .. {end})")
    assert ts.min() >= pd.Timestamp(start, tz='UTC'), "bar before window start"
    assert ts.max() < pd.Timestamp(end, tz='UTC'), "bar after window end"
    # price sanity
    assert (df["low"] <= df["close"]).all() and (df["close"] <= df["high"]).all(), "OHLC bounds violated"
    assert (df["low"] <= df["open"]).all() and (df["open"] <= df["high"]).all(), "OHLC bounds violated"
    assert (df[["open", "high", "low", "close"]] > 0).all().all(), "non-positive price"
    # distinct trading days present
    days = sorted(ts.dt.tz_convert("US/Eastern").dt.date.unique())
    print(f"trading days covered: {len(days)} -> {days}")
    # the event day should be inside the window
    ev_day = pd.Timestamp(event_dt[:10]).date()
    print(f"event date {ev_day} within covered range: {days[0] <= ev_day <= days[-1]}")
    px = df.sort_values("ts")
    print(f"price range: ${df['low'].min():.2f} .. ${df['high'].max():.2f}")
    print(f"first close ${px['close'].iloc[0]:.2f}  last close ${px['close'].iloc[-1]:.2f}")
    print("ALL CHECKS PASSED ✓")


def main():
    event_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1203024235
    ev = lookup_event(event_id)
    symbol = bare_symbol(ev["ticker_region"])
    print(f"event {event_id}: {ev['entity_proper_name']} | {ev['ticker_region']} -> symbol '{symbol}' | {ev['dt']}")
    start, end = trading_window(ev["dt"])
    print(f"[-{DAYS_BEFORE}, +{DAYS_AFTER}] trading-day window: {start} .. {end} (end-exclusive)")

    key = db_key()
    cost = get_cost(key, symbol, start, end)
    print(f"estimated cost: ${cost:.6f}")

    df = get_range(key, symbol, start, end)
    out = f"/tmp/proto_{symbol}_{event_id}.parquet"
    if not df.empty:
        df.to_parquet(out)
        print(f"wrote {len(df)} bars -> {out}")
    validate(df, symbol, start, end, ev["dt"])


if __name__ == "__main__":
    main()
