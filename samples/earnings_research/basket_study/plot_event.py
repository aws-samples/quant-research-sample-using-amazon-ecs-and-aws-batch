"""Validation chart for one earnings event: reporter price action + rule marks.

Two stacked panels — the day before (prev-close reference) and the trade day
(release line, 9:28 signal, ±0.5% neutral band, 9:30 entry, 15:59 exit) — the
format blessed on GM event 1203531964.

Usage:
    python plot_event.py --date 2026-07-21 --event 1203531964 \
        [--out chart.png] [--profile <profile>]

Reads the XNAS.BASIC panel from S3 (or --fixture <local parquet>).
"""

import argparse
import io
import sys
from pathlib import Path

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).resolve().parent))
from rules.sessions import (RTH_START_MIN, RTH_END_MIN, consolidate,
                            entry_price, exit_price, previous_close,
                            signal_price, trade_day_for_event)
from rules.direction import fade_the_gap

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
PANEL_KEY = "earnings-market-data/XNAS.BASIC/panels/date={date}/event_{event}.parquet"
ET = "America/New_York"


def load_panel(args) -> pd.DataFrame:
    if args.fixture:
        return pd.read_parquet(args.fixture)
    import boto3
    from botocore.config import Config
    s3 = boto3.Session(profile_name=args.profile).client(
        "s3", config=Config(retries={"max_attempts": 2, "mode": "standard"}))
    key = PANEL_KEY.format(date=args.date, event=args.event)
    return pd.read_parquet(io.BytesIO(s3.get_object(Bucket=_bucket(), Key=key)["Body"].read()))


def plot(panel: pd.DataFrame, out: str) -> None:
    reporter = panel[panel["reporter_relationship"] == "primary"]["symbol"].iloc[0]
    event_id = int(panel["event_id"].iloc[0])
    event_ts = panel[panel["event_bar"]]["ts"].iloc[0]
    event_et = event_ts.tz_convert(ET)

    day = trade_day_for_event(event_ts)
    if day is None:
        raise SystemExit(f"event {event_id}: RTH release, out of study scope")

    bars = consolidate(panel)
    rep = bars[bars["symbol"] == reporter].copy()
    rep["et"] = rep["ts"].dt.tz_convert(ET)

    days = sorted(rep[rep["et_date"] < day]["et_date"].unique())
    prev_day = days[-1] if days else None

    pc = previous_close(bars, reporter, day)
    sig_px = signal_price(bars, reporter, day)
    sig = fade_the_gap(sig_px, pc)
    entry = entry_price(bars, reporter, day, allow_ffill=False)
    exit_ = exit_price(bars, reporter, day)

    fig, axes = plt.subplots(2, 1, figsize=(16, 9), sharey=True,
                             gridspec_kw={"height_ratios": [1, 1.6]})
    titles = [f"{prev_day} (day before)",
              f"{day} — {reporter} ER released {event_et:%H:%M} ET — XNAS.BASIC"]
    for ax, d, title in zip(axes, [prev_day, day], titles):
        sub = rep[rep["et_date"] == d]
        rth = sub[(sub["et_min"] >= RTH_START_MIN) & (sub["et_min"] < RTH_END_MIN)]
        eth = sub[(sub["et_min"] < RTH_START_MIN) | (sub["et_min"] >= RTH_END_MIN)]
        ax.plot(rth["et"], rth["close"], lw=1.0, color="tab:blue", label="RTH close (1m)")
        ax.plot(eth["et"], eth["close"], lw=0.9, color="tab:purple", alpha=0.8,
                label="pre/post-market (1m)")
        ax.set_title(title, loc="left", fontsize=11)
        ax.grid(alpha=0.3)
        if len(sub):
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=sub["et"].iloc[0].tz))

    ax0, ax1 = axes
    if pc is not None:
        for ax in axes:
            ax.axhline(pc, color="tab:gray", ls="--", lw=1)
        ax1.axhspan(pc * 0.995, pc * 1.005, color="tab:orange", alpha=0.12)
        ax0.annotate(f"prev RTH close {pc}", xy=(0.99, pc), xycoords=("axes fraction", "data"),
                     ha="right", xytext=(0, 6), textcoords="offset points",
                     fontsize=9, color="tab:gray")

    if event_et.date().isoformat() == day:
        ax1.axvline(event_et, color="black", lw=1.2, ls=":")
        ax1.annotate(f"release {event_et:%H:%M} ET", xy=(event_et, ax1.get_ylim()[1]),
                     xytext=(4, -14), textcoords="offset points", fontsize=9)

    if sig_px is not None and sig is not None:
        sig_t = pd.Timestamp(f"{day} 09:28", tz=ET)
        ax1.scatter([sig_t], [sig_px], s=110, color="tab:orange", zorder=5, marker="D")
        action = {1: f"LONG {reporter}", -1: f"SHORT {reporter}", 0: "NEUTRAL -> SKIP"}[sig.direction]
        ax1.annotate(f"signal {sig_px} (last print <= 9:28)\n"
                     f"gap = {sig.gap:+.2%} -> {action}",
                     xy=(sig_t, sig_px), xytext=(-250, -42), textcoords="offset points",
                     fontsize=9, color="tab:orange")

    if entry is not None:
        op_t = pd.Timestamp(f"{day} 09:30", tz=ET)
        ax1.scatter([op_t], [entry], s=110, color="tab:green", zorder=5, marker="^")
        ax1.annotate(f"entry {entry} (9:30 open)", xy=(op_t, entry),
                     xytext=(10, 16), textcoords="offset points", fontsize=9, color="tab:green")

    if exit_ is not None:
        ex_t = pd.Timestamp(f"{day} 15:59", tz=ET)
        ax1.scatter([ex_t], [exit_], s=110, color="tab:red", zorder=5, marker="v")
        note = ""
        if entry is not None and sig is not None and sig.direction != 0:
            pnl = sig.direction * (exit_ / entry - 1)
            note = f"\nreporter leg P&L: {pnl:+.2%}"
        ax1.annotate(f"exit {exit_} (15:59 close){note}", xy=(ex_t, exit_),
                     xytext=(-160, -30), textcoords="offset points", fontsize=9, color="tab:red")

    for ax in axes:
        ax.legend(loc="upper left", fontsize=9)
    fig.suptitle(f"{reporter} — event {event_id} — XNAS.BASIC price action and rule marks", y=0.995)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out, dpi=110)
    print(f"wrote {out}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="panel partition date (trade date folder)")
    ap.add_argument("--event", help="event_id")
    ap.add_argument("--fixture", help="local panel parquet instead of S3")
    ap.add_argument("--out", default=None)
    ap.add_argument("--profile", default=None)
    args = ap.parse_args()
    if not args.fixture and not (args.date and args.event):
        raise SystemExit("need --date + --event, or --fixture")
    panel = load_panel(args)
    out = args.out or f"event_{args.event or 'fixture'}_validation.png"
    plot(panel, out)


if __name__ == "__main__":
    main()