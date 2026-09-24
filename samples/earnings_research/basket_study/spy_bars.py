"""SPY 1-minute bars for the SPY-hedged cells (README decision #8, 2026-09-24).

Panels carry the reporter and its peers only, so the benchmark hedge leg is
attached at evaluation time: load the ET calendar months overlapping the
panel's timestamp window from

    s3://<bucket>/market-data/spy/1min_by_month/month=YYYY-MM.parquet

(Alpaca SIP, 2016-01-01 .. 2026-08-13, extended hours included; split from
market-data/spy/spy_1min_2y.parquet on 2026-09-24) and append them to the
panel in the panel's own schema under reporter_relationship BENCHMARK_REL.
evaluate.py excludes that relationship from every peer universe, so the
hedged 4 x 4 grid is unchanged whether or not SPY rows are present.

One-off repartition (already done):  python spy_bars.py --repartition
"""
import argparse
import io
from typing import Iterable, Tuple

import pandas as pd

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
SOURCE_KEY = "market-data/spy/spy_1min_2y.parquet"
MONTH_PREFIX = "market-data/spy/1min_by_month/"
SPY_SYMBOL = "SPY"
BENCHMARK_REL = "benchmark"
ET = "America/New_York"

PANEL_COLUMNS = ["symbol", "ts_event", "rtype", "publisher_id", "instrument_id", "open", "high",
                 "low", "close", "volume", "ts", "reporter_relationship", "event_id", "event_bar"]


def month_key(month: str) -> str:
    return f"{MONTH_PREFIX}month={month}.parquet"


def panel_window(panel: pd.DataFrame) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """[first, last] real-bar timestamp of the panel (UTC)."""
    real = panel[~panel["event_bar"].astype(bool)]
    return real["ts"].min(), real["ts"].max()


def months_between(start: pd.Timestamp, end: pd.Timestamp) -> Iterable[str]:
    s, e = start.tz_convert(ET).tz_localize(None), end.tz_convert(ET).tz_localize(None)
    return [str(p) for p in pd.period_range(s.to_period("M"), e.to_period("M"), freq="M")]


def load_spy_bars(s3io, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """SPY bars with timestamp in [start, end] (UTC), native Alpaca columns."""
    parts = []
    for m in months_between(start, end):
        try:
            parts.append(s3io.read_parquet(f"s3://{_bucket()}/{month_key(m)}").to_pandas())
        except Exception as e:  # month outside the landed range: nothing to hedge with
            print(f"WARNING: SPY month {m} unavailable ({e.__class__.__name__})", flush=True)
    if not parts:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    df = pd.concat(parts, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).astype("datetime64[ns, UTC]")
    return df[(df["timestamp"] >= start) & (df["timestamp"] <= end)].sort_values("timestamp")


def as_panel_rows(spy: pd.DataFrame, event_id: int) -> pd.DataFrame:
    """Alpaca SPY bars -> panel-schema rows (relationship BENCHMARK_REL)."""
    out = pd.DataFrame({
        "symbol": SPY_SYMBOL,
        "ts_event": pd.array([pd.NA] * len(spy), dtype="Int64"),
        "rtype": pd.array([pd.NA] * len(spy), dtype="Int64"),
        "publisher_id": pd.array([pd.NA] * len(spy), dtype="Int64"),
        "instrument_id": pd.array([pd.NA] * len(spy), dtype="Int64"),
        "open": pd.array(spy["open"].to_numpy(dtype=float), dtype="Float64"),
        "high": pd.array(spy["high"].to_numpy(dtype=float), dtype="Float64"),
        "low": pd.array(spy["low"].to_numpy(dtype=float), dtype="Float64"),
        "close": pd.array(spy["close"].to_numpy(dtype=float), dtype="Float64"),
        "volume": pd.array(spy["volume"].to_numpy(dtype="int64"), dtype="Int64"),
        "ts": spy["timestamp"].to_numpy(),
        "reporter_relationship": BENCHMARK_REL,
        "event_id": pd.array([event_id] * len(spy), dtype="Int64"),
        "event_bar": False,
    })
    out["ts"] = pd.to_datetime(out["ts"], utc=True)
    return out[PANEL_COLUMNS]


def attach_spy(panel: pd.DataFrame, spy: pd.DataFrame) -> pd.DataFrame:
    """Panel + SPY rows for the panel's window. Idempotent (existing SPY rows dropped first)."""
    base = panel[panel["symbol"] != SPY_SYMBOL]
    if spy.empty:
        return base
    rows = as_panel_rows(spy, int(panel["event_id"].iloc[0]))
    cols = [c for c in PANEL_COLUMNS if c in base.columns]
    return pd.concat([base, rows[cols]], ignore_index=True)


def attach_spy_for_panel(panel: pd.DataFrame, s3io) -> pd.DataFrame:
    start, end = panel_window(panel)
    return attach_spy(panel, load_spy_bars(s3io, start, end))


def repartition(profile=None):
    """One-off: split the 10y file into ET-month objects (+ README)."""
    import boto3
    import polars as pl
    s3 = boto3.Session(profile_name=profile).client("s3")
    d = pl.read_parquet(io.BytesIO(s3.get_object(Bucket=_bucket(), Key=SOURCE_KEY)["Body"].read()))
    d = d.with_columns(pl.col("timestamp").dt.convert_time_zone(ET).dt.strftime("%Y-%m").alias("month")).sort("timestamp")
    n = 0
    for (m,), g in d.group_by(["month"], maintain_order=True):
        buf = io.BytesIO()
        g.drop("month").write_parquet(buf)
        s3.put_object(Bucket=_bucket(), Key=month_key(m), Body=buf.getvalue())
        n += 1
    print(f"{n} monthly files, {d.height} rows")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--repartition", action="store_true")
    ap.add_argument("--profile", default=None)
    a = ap.parse_args()
    if a.repartition:
        repartition(a.profile)
