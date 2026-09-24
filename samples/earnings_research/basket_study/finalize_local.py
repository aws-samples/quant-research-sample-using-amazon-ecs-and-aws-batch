"""Run the finalize step (exit-timing study + exit_matrix + every cell matrix)
LOCALLY for one or more (spy_weight, strategy_leverage) portfolio variants,
from a single load of the shards.

Why: adding a chart scope to configs/study_config.json only needs the
finalize step re-run; on Batch that means a commit + image rebuild + one job
per variant, each re-reading 168k shards. Locally the consolidated shard
table (shards_consolidated/shards.parquet, one S3 object) is read once and
all variants render from memory. No local downloads.

    AWS_PROFILE=<profile> python finalize_local.py \
        --s3-prefix earnings-basket-study/results-10y \
        --variant 0,1 --variant 1,1 --variant 1,2

Writes the same artifacts to the same places as `aggregate.py --finalize
--spy-weight w --strategy-leverage L` would (aggregate{_spyw<w>}{_lev<L>}/).
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd  # noqa: E402
import polars as pl  # noqa: E402

import aggregate  # noqa: E402
import settings
from consolidate_shards import consolidated_uri  # noqa: E402
from s3io import S3IO  # noqa: E402


def attach_reporter_price(ok, s3io, s3_prefix: str):
    """Add study_config.PRICE_COL (reporter 9:30 entry fill) by event_id from
    <s3-prefix>/diagnostics/reporter_marks.parquet (reporter_marks.py). The
    reporter-price scopes (PX<1 .. PX100+) filter on it. Missing table ->
    column absent, warning printed, price scopes then raise a clear KeyError."""
    import study_config
    from reporter_marks import marks_uri
    if study_config.PRICE_COL in ok.columns:
        return ok
    try:
        mk = pl.read_parquet(marks_uri(s3_prefix), storage_options=s3io.pl_opts,
                             columns=["event_id", "entry_930_open"]).to_pandas()
    except Exception as e:
        print(f"WARNING: {marks_uri(s3_prefix)} unavailable ({e.__class__.__name__}); "
              f"reporter-price scopes cannot be built", flush=True)
        return ok
    mk = mk.drop_duplicates("event_id").rename(columns={"entry_930_open": study_config.PRICE_COL})
    out = ok.merge(mk, on="event_id", how="left")
    miss = int(out[study_config.PRICE_COL].isna().sum())
    if miss:
        print(f"WARNING: {miss} ok rows without a reporter entry price (excluded from price scopes)", flush=True)
    return out


def _read_table(s3io, src: str) -> pd.DataFrame:
    if src.startswith("s3://"):
        return pl.read_parquet(src, storage_options=s3io.pl_opts).to_pandas()
    return pl.read_parquet(src).to_pandas()


def load_ok(s3io, s3_prefix: str, cache: str | None = None, fee_per_share: float = 0.0,
            extra: list | tuple = ()):
    """All status=='ok' shard rows. Default source is the consolidated table
    in S3 (<s3-prefix>/shards_consolidated/shards.parquet, see
    consolidate_shards.py) — one S3 read, no local copies. --cache may point
    at another s3:// object; the raw per-shard glob is the fallback only.
    fee_per_share > 0 nets a round-trip per-share commission out of every P&L
    column (fees.apply_per_share_fee; needs the shares_per_dollar column).
    extra: further consolidated shard tables (s3:// or local) whose ok rows are
    UNIONED in — e.g. the unhedged reporter_only cells evaluated separately
    (README decision #7). Only events present in the main table (any status)
    AND trade days on which the main table has an ok row are admitted: the
    study's event universe and its trade-day index must not grow, otherwise
    every existing daily series gains zero days and Sharpe / annualised return
    / drawdown durations of the old cells shift.
    """
    t = time.time()
    src = cache or consolidated_uri(s3_prefix)
    if src.startswith("s3://"):
        try:
            shards = pl.read_parquet(src, storage_options=s3io.pl_opts).to_pandas()
            print(f"shards from {src}: {shards.shape} in {time.time() - t:.0f}s", flush=True)
        except Exception as e:  # consolidated object missing -> slow glob scan
            print(f"{src} unavailable ({e.__class__.__name__}); scanning shard glob (slow)", flush=True)
            shards = pl.scan_parquet(
                f"s3://{settings.get("s3", "data_bucket")}/{s3_prefix}/shards/*.parquet",
                storage_options=s3io.pl_opts).collect().to_pandas()
            print(f"shards from S3 glob: {shards.shape} in {time.time() - t:.0f}s", flush=True)
    else:
        shards = pl.read_parquet(src).to_pandas()
        print(f"shards from local file {src}: {shards.shape}", flush=True)
    ok = shards[shards["status"] == "ok"].copy()
    if extra:
        events = set(shards["event_id"].dropna().astype("int64"))
        days = set(ok["trade_day"].unique())
        parts = [ok]
        for uri in extra:
            x = _read_table(s3io, uri)
            x = x[(x["status"] == "ok") & x["event_id"].astype("int64").isin(events)]
            off_days = int((~x["trade_day"].isin(days)).sum())
            x = x[x["trade_day"].isin(days)]
            cells = sorted((x["construction"] + "|" + x["universe"]).unique())
            print(f"extra shards {uri}: {len(x)} ok rows in the study's event set and trade days "
                  f"({off_days} dropped on days without a hedged trade), cells {cells}", flush=True)
            parts.append(x)
        ok = pd.concat(parts, ignore_index=True)
    for c in aggregate._exits(ok):
        ok[c] = ok[c].astype(float)
    ok = attach_reporter_price(ok, s3io, s3_prefix)
    if fee_per_share:
        from fees import apply_per_share_fee
        ok = apply_per_share_fee(ok, fee_per_share)
        f = ok["fee_per_dollar"] * 1e4
        print(f"per-share fee ${fee_per_share:g} applied: median {f.median():.1f} bps / trade "
              f"(p90 {f.quantile(0.9):.1f})", flush=True)
    return ok


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--s3-prefix", default="earnings-basket-study/results-10y")
    ap.add_argument("--cache", default=None,
                    help="alternative s3:// (or local) parquet of all shards; default = shards_consolidated/")
    ap.add_argument("--variant", action="append", default=None,
                    help="spy_weight,strategy_leverage (repeatable); default 0,1")
    ap.add_argument("--max-basket-weight", type=float, default=aggregate.MAX_BASKET_WEIGHT)
    ap.add_argument("--profile", default=None)
    args = ap.parse_args()

    aggregate.MAX_BASKET_WEIGHT = args.max_basket_weight
    variants = [tuple(float(x) for x in v.split(",")) for v in (args.variant or ["0,1"])]
    s3io = S3IO(profile=args.profile)
    ok = load_ok(s3io, args.s3_prefix, args.cache)

    for w, lev in variants:
        aggregate.SPY_WEIGHT, aggregate.STRATEGY_LEVERAGE = w, lev
        base = (f"s3://{settings.get("s3", "data_bucket")}/{args.s3_prefix}/aggregate"
                f"{aggregate.spy_weight_suffix(w, lev)}")
        t = time.time()
        print(f"\n=== finalize w={w:g} L={lev:g} -> {base}", flush=True)
        aggregate.run_finalize(s3io, base, ok.copy())
        print(f"=== done w={w:g} L={lev:g} in {time.time() - t:.0f}s", flush=True)


if __name__ == "__main__":
    main()
