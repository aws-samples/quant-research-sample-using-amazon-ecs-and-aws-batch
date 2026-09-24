"""One-command validation bundle for a single earnings event.

Regenerates, deterministically, every artifact from the GM-event validation
session (2026-08-04) so any event can be spot-checked the same way:

  validation/<event_id>/
    facts.json                 every computed number (marks, signal, grid P&L,
                               per-leg audit, diagnostics, coverage stats)
    event_marks.png            price action + rule marks (prev close, 9:28
                               signal, neutral band, entry, exit)  [plot_event]
    ridge_input.png            indexed overlay of the ridge fitting window
    ridge_input_prices.csv     the literal post-filter fitting dataset
    pnl_components.png         weight-scaled P&L component time series,
                               one panel per construction (universe=all)
    perleg_<construction>.png  per-leg audit: price (left $) vs weighted
                               delta (right %), entries/exits marked, all
                               numbers in panel titles
    pnl_grid.csv               constructions x universes final P&L table

Usage:
    python validate_event.py --date 2026-07-21 --event 1203531964
        [--construction ridge_returns_beta_neutral] [--out-dir validation/]
        [--profile <profile>]

The companion skill (.claude/skills/event-validation) holds the REASONING
checklist to apply over these artifacts.
"""

import argparse
import io
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).resolve().parent))

from rules.sessions import (RTH_END_MIN, RTH_START_MIN, consolidate,
                            entry_price, exit_price, previous_close,
                            signal_price, trade_day_for_event)
from rules.direction import fade_the_gap
from construction.base import BasketContext, admissible_peers
from construction.registry import CONSTRUCTIONS
from construction.ridge import _pre_event_rth_matrix
from evaluate import evaluate_event
import plot_event

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
PANEL_KEY = "earnings-market-data/XNAS.BASIC/panels/date={date}/event_{event}.parquet"
ET = "America/New_York"


def load_panel(date, event, profile):
    import boto3
    from botocore.config import Config
    s3 = boto3.Session(profile_name=profile).client(
        "s3", config=Config(retries={"max_attempts": 2, "mode": "standard"}))
    key = PANEL_KEY.format(date=date, event=event)
    return pd.read_parquet(io.BytesIO(s3.get_object(Bucket=_bucket(), Key=key)["Body"].read()))


def rth_prices(bars, day):
    rth = bars[(bars["et_date"] == day) & (bars["et_min"] >= RTH_START_MIN)
               & (bars["et_min"] < RTH_END_MIN)]
    return (rth.pivot_table(index="ts", columns="symbol", values="close")
            .sort_index().astype("float64").ffill())


def fig_ridge_input(mat_prices, reporter, out):
    idx = mat_prices / mat_prices.iloc[0] * 100
    et = idx.index.tz_convert(ET)
    days = sorted(set(et.date))
    starts = [next(i for i, t in enumerate(et) if t.date() == d) for d in days]
    fig, ax = plt.subplots(figsize=(16, 8))
    for i, sym in enumerate([c for c in idx.columns if c != reporter]):
        ax.plot(range(len(idx)), idx[sym], lw=1.0, alpha=0.75,
                color=plt.cm.tab10.colors[i % 10], label=sym)
    ax.plot(range(len(idx)), idx[reporter], lw=2.4, color="black",
            label=f"{reporter} (REPORTER)", zorder=5)
    for ds, d in zip(starts, days):
        ax.axvline(ds, color="gray", lw=0.8, ls=":")
        ax.annotate(str(d), xy=(ds, 0.01), xycoords=("data", "axes fraction"),
                    fontsize=9, color="gray", xytext=(4, 0), textcoords="offset points")
    ax.axhline(100, color="gray", lw=0.8, alpha=0.5)
    ax.set_ylabel("price indexed to 100 at first fitted minute")
    ax.set_title(f"Ridge fitting input — pre-event RTH minutes ({len(idx)}) — indexed overlay")
    ax.grid(alpha=0.3); ax.legend(loc="best", ncol=2, fontsize=9); ax.margins(x=0.01)
    fig.tight_layout(); fig.savefig(out, dpi=110); plt.close(fig)


def fig_components(bars, baskets, reporter, day, out):
    px = rth_prices(bars, day)
    xt = px.index.tz_convert(ET).strftime("%H:%M")
    step = max(1, len(px) // 13)
    fig, axes = plt.subplots(2, 2, figsize=(17, 11), sharex=True, sharey=True)
    for ax, (name, b) in zip(axes.flat, baskets.items()):
        if not b.weights:
            ax.set_title(f"{name} — SKIPPED: {b.diagnostics.get('skip')}", loc="left", fontsize=10)
            continue
        total = pd.Series(0.0, index=px.index)
        legs = []
        for sym, w in b.weights.items():
            e = entry_price(bars, sym, day, allow_ffill=(sym != reporter))
            c = w * (px[sym] / e - 1) * 100
            total += c
            legs.append((sym, w, c))
        legs.sort(key=lambda t: (t[0] != reporter, -abs(t[2].iloc[-1])))
        ci = 0
        for sym, w, c in legs:
            if sym == reporter:
                ax.plot(range(len(px)), c.values, lw=1.8, color="tab:green",
                        label=f"{sym} w={w:+.2f} ({c.iloc[-1]:+.2f}%)")
            else:
                ax.plot(range(len(px)), c.values, lw=1.0, alpha=0.85,
                        color=plt.cm.tab10.colors[ci % 10],
                        label=f"{sym} w={w:+.2f} ({c.iloc[-1]:+.2f}%)")
                ci += 1
        ax.plot(range(len(px)), total.values, lw=2.6, color="black",
                label=f"BASKET ({total.iloc[-1]:+.3f}%)", zorder=5)
        ax.axhline(0, color="gray", lw=0.8)
        ax.set_title(name, loc="left", fontsize=11, fontweight="bold")
        ax.grid(alpha=0.3); ax.legend(loc="best", fontsize=7)
    for ax in axes[1]:
        ax.set_xticks(range(0, len(px), step)); ax.set_xticklabels(xt[::step])
    fig.suptitle("P&L components, weight-scaled — universe=all — black = basket total", y=0.995)
    fig.tight_layout(); fig.savefig(out, dpi=110); plt.close(fig)


def fig_perleg(bars, basket, reporter, day, name, out):
    px = rth_prices(bars, day)
    xt = px.index.tz_convert(ET).strftime("%H:%M")
    step = max(1, len(px) // 8)
    legs = sorted(basket.weights.items(), key=lambda t: (t[0] != reporter, -abs(t[1])))
    n = len(legs)
    ncols = 3
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(18, 4 * nrows), sharex=True, squeeze=False)
    total = 0.0
    audit = []
    for ax, (sym, w) in zip(axes.flat, legs):
        e = entry_price(bars, sym, day, allow_ffill=(sym != reporter))
        x = exit_price(bars, sym, day)
        raw = x / e - 1
        contrib = w * raw
        total += contrib
        audit.append({"symbol": sym, "weight": w, "entry": e, "exit": x,
                      "raw_return": raw, "contribution": contrib})
        ax.plot(range(len(px)), px[sym].values, lw=1.2, color="tab:blue")
        ax.scatter([0], [e], color="tab:blue", s=70, zorder=6, marker="^")
        ax.scatter([len(px)-1], [x], color="tab:blue", s=70, zorder=6, marker="v")
        ax.set_ylabel("price ($)", color="tab:blue", fontsize=8)
        ax.tick_params(axis="y", labelcolor="tab:blue", labelsize=8)
        ax2 = ax.twinx()
        scaled = (w * (px[sym] / e - 1)) * 100
        ax2.plot(range(len(px)), scaled.values, lw=1.6, color="tab:red")
        ax2.scatter([0], [0], color="tab:red", s=70, zorder=6, marker="^")
        ax2.scatter([len(px)-1], [contrib*100], color="tab:red", s=70, zorder=6, marker="v")
        ax2.axhline(0, color="tab:red", lw=0.6, alpha=0.4, ls="--")
        ax2.set_ylabel("weighted Δ (%)", color="tab:red", fontsize=8)
        ax2.tick_params(axis="y", labelcolor="tab:red", labelsize=8)
        ax.set_title(f"{sym}  w={w:+.4f}\nentry {e}  exit {x}  raw {raw:+.3%}  xw {contrib:+.3%}",
                     loc="left", fontsize=8.5, fontweight="bold",
                     color=("tab:green" if sym == reporter else "black"))
        ax.grid(alpha=0.3)
    for ax in axes.flat[n:]:
        ax.axis("off")
    for ax in axes[-1]:
        ax.set_xticks(range(0, len(px), step)); ax.set_xticklabels(xt[::step], fontsize=8)
    fig.suptitle(f"Per-leg P&L validation — {name}, universe=all — "
                 f"sum of weighted exits = {total*100:+.3f}%", y=0.998, fontsize=11)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    fig.savefig(out, dpi=110); plt.close(fig)
    return audit, total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--event", required=True)
    ap.add_argument("--construction", default="ridge_returns_beta_neutral",
                    help="construction for the per-leg audit chart")
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--profile", default=None)
    args = ap.parse_args()

    panel = load_panel(args.date, args.event, args.profile)
    outdir = Path(args.out_dir or f"validation/{args.event}")
    outdir.mkdir(parents=True, exist_ok=True)

    reporter = panel[panel["reporter_relationship"] == "primary"]["symbol"].iloc[0]
    event_ts = panel[panel["event_bar"]]["ts"].iloc[0]
    day = trade_day_for_event(event_ts)
    bars = consolidate(panel)
    peers_raw = {rel: sorted(g["symbol"].unique())
                 for rel, g in panel[panel["reporter_relationship"] != "primary"]
                 .groupby("reporter_relationship")}
    # basket admission (mirrors evaluate.py): coverage floor applied once,
    # before any construction — RTH-release events (day None) skip anyway
    if day is not None:
        peers, admission_dropped = admissible_peers(bars, peers_raw, day, reporter)
    else:
        peers, admission_dropped = peers_raw, {}

    facts = {
        "event_id": int(panel["event_id"].iloc[0]),
        "reporter": reporter,
        "event_ts_utc": str(event_ts),
        "event_ts_et": str(event_ts.tz_convert(ET)),
        "trade_day": day,
        "peers": peers,
        "admission_dropped_low_coverage": admission_dropped,
        "premarket_prints_reporter": int(((bars["symbol"] == reporter)
                                          & (bars["et_date"] == day)
                                          & (bars["et_min"] < RTH_START_MIN)).sum()) if day else None,
    }

    if day is None:
        facts["status"] = "out_of_scope_rth_release"
        (outdir / "facts.json").write_text(json.dumps(facts, indent=2, default=str))
        print(f"event is an RTH release — out of scope; facts written to {outdir}")
        return

    pc = previous_close(bars, reporter, day)
    sp = signal_price(bars, reporter, day)
    sig = fade_the_gap(sp, pc)
    facts["marks"] = {"previous_close": pc, "signal_928": sp,
                      "entry_930_open": entry_price(bars, reporter, day, allow_ffill=False),
                      "exit_1559_close": exit_price(bars, reporter, day)}
    facts["signal"] = (None if sig is None else
                       {"gap": sig.gap, "direction": sig.direction, "reason": sig.reason})

    # 1. event marks chart (reuses plot_event)
    plot_event.plot(panel, str(outdir / "event_marks.png"))

    # 2. ridge input dump + overlay
    mat, low_cov = _pre_event_rth_matrix(bars, [reporter] + sorted(
        {s for v in peers.values() for s in v}), day, "levels",
        reporter=reporter)
    prices = np.exp(mat) if len(mat) else mat   # back to relative prices for the dump
    raw_px = (bars[(bars["et_date"] < day) & (bars["et_min"] >= RTH_START_MIN)
                   & (bars["et_min"] < RTH_END_MIN)]
              .pivot_table(index="ts", columns="symbol", values="close")
              .sort_index().astype("float64").dropna(how="any"))
    raw_px.to_csv(outdir / "ridge_input_prices.csv")
    facts["ridge_input"] = {"n_minutes": len(raw_px),
                            "n_symbols": raw_px.shape[1] if len(raw_px) else 0,
                            "sessions": sorted({str(t.date()) for t in
                                                raw_px.index.tz_convert(ET)}),
                            "dropped_low_coverage": low_cov}
    if len(raw_px):
        fig_ridge_input(raw_px, reporter, outdir / "ridge_input.png")

    # 3. full grid P&L + components chart + per-leg audit
    rows = evaluate_event(panel)
    facts["grid"] = rows
    ok = pd.DataFrame([r for r in rows if r["status"] == "ok"])
    if len(ok):
        (ok.pivot_table(index="construction", columns="universe", values="pnl") * 100
         ).round(4).to_csv(outdir / "pnl_grid.csv")
        if sig and sig.direction != 0:
            ctx = BasketContext(bars=bars, reporter=reporter, peers=peers,
                                trade_day=day, direction=sig.direction)
            baskets = {n: b(ctx, "all") for n, b in CONSTRUCTIONS.items()}
            fig_components(bars, baskets, reporter, day, outdir / "pnl_components.png")
            target = baskets.get(args.construction)
            if target and target.weights:
                audit, total = fig_perleg(bars, target, reporter, day, args.construction,
                                          outdir / f"perleg_{args.construction}.png")
                facts["perleg_audit"] = {"construction": args.construction,
                                         "legs": audit, "total_pnl": total}
                facts["perleg_diagnostics"] = target.diagnostics

    (outdir / "facts.json").write_text(json.dumps(facts, indent=2, default=str))
    print(f"validation bundle -> {outdir}/")
    for f in sorted(outdir.iterdir()):
        print("  ", f.name)


if __name__ == "__main__":
    main()
