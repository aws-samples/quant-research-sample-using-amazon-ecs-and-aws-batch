"""ridge_reject_diag — why did the ridge construction reject one event?

Renders a two-panel diagnostic PNG for a single event: (top) pre-event RTH
minute closes of the reporter and its peers, indexed to 100 at each symbol's
first pre-event print, exactly the window the ridge fit would use (every RTH
minute in the panel strictly before the trade day); (bottom) per-symbol data
presence per minute, so the inner join that produces the fitting matrix is
visible. The title states the fit outcome (n common minutes vs the 30
required, coverage drops), reproduced with construction.ridge's own matrix
builder.

    AWS_PROFILE=<profile> python ridge_reject_diag.py --key <panel s3 key> \
        --trade-day 2025-06-16 --universe "pure play" --out-key <s3 key for png>

Writes the PNG to S3 only (results-10y/diagnostics/...), viewable in the
results browser via /view?key=<out-key>.
"""
import argparse
import io
import sys
from pathlib import Path

import boto3
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from construction import ridge  # noqa: E402
from construction.base import COVERAGE_FLOOR, admissible_peers, universe_symbols  # noqa: E402
from rules.sessions import RTH_END_MIN, RTH_START_MIN, consolidate  # noqa: E402

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
ET = "America/New_York"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--key", required=True, help="panel parquet S3 key")
    ap.add_argument("--universe", default="pure play")
    ap.add_argument("--fit-space", default="levels")
    ap.add_argument("--trade-day", required=True, help="YYYY-MM-DD (from the shard row)")
    ap.add_argument("--out-key", required=True)
    ap.add_argument("--profile", default=None)
    args = ap.parse_args()
    s3 = boto3.Session(profile_name=args.profile).client("s3") if args.profile else boto3.client("s3")
    panel = pd.read_parquet(io.BytesIO(s3.get_object(Bucket=_bucket(), Key=args.key)["Body"].read()))

    reporter = panel.loc[panel["reporter_relationship"] == "primary", "symbol"].iloc[0]
    event_id = int(panel["event_id"].iloc[0])
    ev_ts = pd.Timestamp(panel.loc[panel["event_bar"], "ts"].iloc[0])
    ev_ts = (ev_ts.tz_localize("UTC") if ev_ts.tzinfo is None else ev_ts).tz_convert(ET)
    day = args.trade_day
    bars = consolidate(panel)          # adds et_date / et_min, same as evaluate.py
    peers_raw = {rel: sorted(g["symbol"].unique())
                 for rel, g in panel[panel["reporter_relationship"] != "primary"].groupby("reporter_relationship")}
    peers, dropped_low_cov = admissible_peers(bars, peers_raw, day, reporter)
    syms = universe_symbols(peers, args.universe, reporter)
    mat, low = ridge._pre_event_rth_matrix(bars, [reporter] + syms, day, args.fit_space, reporter=reporter)
    n_min = len(mat)
    outcome = ("REJECTED: insufficient pre-event RTH data" if (reporter not in mat.columns or n_min < 30)
               else f"fittable ({n_min} common minutes)")

    pre = bars[(bars["et_date"] < day) & (bars["et_min"] >= RTH_START_MIN) & (bars["et_min"] < RTH_END_MIN)]
    wide = (pre[pre["symbol"].isin([reporter] + syms)].pivot_table(index="ts", columns="symbol", values="close")
            .sort_index().astype("float64"))
    cov = wide.notna().mean()
    et = wide.index.tz_convert(ET)
    days = sorted(set(et.date))
    starts = [next(i for i, t in enumerate(et) if t.date() == d) for d in days]

    fig, (ax, ax2) = plt.subplots(2, 1, figsize=(16, 9), gridspec_kw={"height_ratios": [3, 1.2]}, sharex=True)
    x = np.arange(len(wide))
    order = [reporter] + [s for s in wide.columns if s != reporter]
    for i, sym in enumerate(order):
        col = wide[sym]
        first = col.first_valid_index()
        idx = col / col.loc[first] * 100 if first is not None else col
        if sym == reporter:
            ax.plot(x, idx, lw=2.4, color="black", label=f"{reporter} (REPORTER) cov {cov[sym]:.0%}", zorder=5)
        else:
            ax.plot(x, idx, lw=1.1, alpha=.8, color=plt.cm.tab10.colors[i % 10],
                    label=f"{sym} cov {cov[sym]:.0%}" + (" (dropped <floor)" if sym in low else ""))
    for ds, d in zip(starts, days):
        for a in (ax, ax2):
            a.axvline(ds, color="gray", lw=.8, ls=":")
        ax.annotate(str(d), xy=(ds, .01), xycoords=("data", "axes fraction"), fontsize=9, color="gray",
                    xytext=(4, 0), textcoords="offset points")
    ax.axhline(100, color="gray", lw=.8, alpha=.5)
    ax.set_ylabel("close, indexed to 100 at each symbol's first pre-event print")
    ax.grid(alpha=.3); ax.legend(loc="best", ncol=2, fontsize=9); ax.margins(x=.01)
    present = wide[order].notna().to_numpy().T.astype(float)
    ax2.imshow(present, aspect="auto", cmap="Blues", vmin=0, vmax=1.4, interpolation="nearest")
    ax2.set_yticks(range(len(order))); ax2.set_yticklabels(order, fontsize=9)
    ax2.set_xlabel(f"pre-event RTH minutes in the panel (union across symbols): {len(wide)}"
                   f" · common to ALL kept symbols (the fitting matrix): {n_min} · required: 30")
    ax2.set_title("data presence per minute (dark = print available); the fit uses only minutes where every kept symbol prints",
                  fontsize=10, loc="left")
    fig.suptitle(f"Ridge ({args.fit_space}, {args.universe}) fitting window — event {event_id} {reporter}"
                 f" · released {ev_ts:%Y-%m-%d %H:%M} ET · trade day {day}\n"
                 f"{outcome} · coverage floor {COVERAGE_FLOOR:.0%} · dropped below floor: {sorted(low) or 'none'}"
                 f" · admission drops: {sorted(dropped_low_cov) if dropped_low_cov else 'none'}", fontsize=12)
    fig.tight_layout()
    buf = io.BytesIO(); fig.savefig(buf, dpi=110, format="png"); plt.close(fig)
    s3.put_object(Bucket=_bucket(), Key=args.out_key, Body=buf.getvalue(), ContentType="image/png")
    print(f"{outcome}; union minutes {len(wide)}, common {n_min}; coverage: "
          + ", ".join(f"{s} {cov[s]:.0%}" for s in order) + f"\nwrote s3://{_bucket()}/{args.out_key}")


if __name__ == "__main__":
    main()
