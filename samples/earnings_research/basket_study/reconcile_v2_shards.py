"""Gate for the transaction-cost shards: results-10y-v2 must reproduce
results-10y per (event, construction, universe) — same status/skip_reason/
direction, every P&L mark equal to float precision — before any fee variant
is produced from it. Then write the v2 rows restricted to the v1 EVENT SET
(v2 re-evaluated 33 panels that appeared after the Aug run) as
<v2-prefix>/shards_consolidated/shards_matched_v1.parquet, the --cache the
fee producers read, so gross and net figures cover identical events.

    AWS_PROFILE=<profile> python reconcile_v2_shards.py [--write]
"""
import argparse
import io
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

import aggregate  # noqa: E402
import settings
from consolidate_shards import consolidated_uri  # noqa: E402

KEYS = ["event_id", "construction", "universe"]
CAT_COLS = ["status", "skip_reason", "direction"]
ATOL = 1e-9
V1 = "earnings-basket-study/results-10y"
V2 = "earnings-basket-study/results-10y-v2"
MATCHED = "shards_consolidated/shards_matched_v1.parquet"


def reconcile(ref: pd.DataFrame, cand: pd.DataFrame, allow_events=()) -> dict:
    """Vectorised comparison. Returns a summary dict; summary['ok'] is True
    when every v1 row has a v2 twin with equal categorical columns and every
    P&L mark within ATOL (NaN == NaN). allow_events: event_ids whose panel is
    KNOWN to have been re-fetched between the runs — their rows may differ
    (listed under 'allowed_diff_rows') without failing the gate; any
    difference outside that set still fails."""
    pnl_cols = [c for c in ref.columns if c.startswith("pnl") and c in cand.columns]
    m = ref.merge(cand, on=KEYS, how="outer", suffixes=("_ref", "_new"), indicator=True)
    both = m[m["_merge"] == "both"]
    out = {"ref_rows": len(ref), "cand_rows": len(cand),
           "ref_only_rows": int((m["_merge"] == "left_only").sum()),
           "cand_only_rows": int((m["_merge"] == "right_only").sum()),
           "cand_only_events": int(m.loc[m["_merge"] == "right_only", "event_id"].nunique()),
           "cat_mismatch": {}, "pnl_max_abs_diff": {}, "pnl_rows_over_atol": 0}
    for c in CAT_COLS:
        a, b = both[f"{c}_ref"], both[f"{c}_new"]
        bad = ~((a == b) | (a.isna() & b.isna()))
        out["cat_mismatch"][c] = int(bad.sum())
    over = np.zeros(len(both), dtype=bool)
    for c in pnl_cols:
        a = both[f"{c}_ref"].astype(float).to_numpy()
        b = both[f"{c}_new"].astype(float).to_numpy()
        d = np.abs(a - b)
        bothnan = np.isnan(a) & np.isnan(b)
        d = np.where(bothnan, 0.0, d)
        d = np.where(np.isnan(d), np.inf, d)          # NaN on one side only
        out["pnl_max_abs_diff"][c] = float(np.nanmax(d)) if len(d) else 0.0
        over |= d > ATOL
    out["pnl_rows_over_atol"] = int(over.sum())
    allowed = both["event_id"].isin(set(allow_events)).to_numpy()
    out["allowed_diff_rows"] = int((over & allowed).sum())
    out["diff_events"] = sorted(int(e) for e in both.loc[over, "event_id"].unique())
    out["ok"] = (out["ref_only_rows"] == 0 and sum(out["cat_mismatch"].values()) == 0
                 and int((over & ~allowed).sum()) == 0)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--v1", default=V1)
    ap.add_argument("--v2", default=V2)
    ap.add_argument("--profile", default=None)
    ap.add_argument("--write", action="store_true", help="write shards_matched_v1.parquet under the v2 prefix")
    ap.add_argument("--allow-events", default="",
                    help="comma-separated event_ids whose panels were re-fetched between runs (their P&L may differ)")
    args = ap.parse_args()
    allow = [int(x) for x in args.allow_events.split(",") if x.strip()]
    import polars as pl
    from s3io import S3IO
    s3io = S3IO(profile=args.profile)
    ref = pl.read_parquet(consolidated_uri(args.v1), storage_options=s3io.pl_opts).to_pandas()
    cand = pl.read_parquet(consolidated_uri(args.v2), storage_options=s3io.pl_opts).to_pandas()
    print(f"v1 {ref.shape}  v2 {cand.shape}", flush=True)
    summ = reconcile(ref, cand, allow_events=allow)
    for k, v in summ.items():
        print(f"  {k}: {v}")
    if not summ["ok"]:
        print("RECONCILE FAIL")
        return 1
    print(f"RECONCILE OK — v2 reproduces v1 on every common row"
          + (f" except {summ['allowed_diff_rows']} rows of whitelisted events {allow}" if summ["allowed_diff_rows"] else ""))
    if args.write:
        keep = cand[cand["event_id"].isin(set(ref["event_id"]))]
        buf = io.BytesIO()
        keep.to_parquet(buf, index=False)
        uri = f"s3://{settings.get("s3", "data_bucket")}/{args.v2}/{MATCHED}"
        s3io.write_bytes(buf.getvalue(), uri)
        print(f"wrote {uri}: {keep.shape} ({keep['event_id'].nunique()} events = v1 event set)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
