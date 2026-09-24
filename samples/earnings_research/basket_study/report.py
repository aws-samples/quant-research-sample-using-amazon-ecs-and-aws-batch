"""Per-event report bundle: the K table + all seven validated charts.

Every evaluated event produces (locally, then synced to S3 by evaluate.py):

  event_<id>.k              box-format table of the full result grid (screen format)
  event_<id>.csv            same data, machine-readable (aggregation input)
  facts.json                every computed number (marks, signal, diagnostics)
  event_marks.png           1. price action + rule marks (prev close, 9:28 signal,
                               neutral band, entry, exit)
  ridge_input.png           2. indexed overlay of the ridge fitting window
  ridge_input_prices.csv       (+ the literal post-filter fitting dataset)
  pnl_components.png        3. weight-scaled P&L component lines per construction
  perleg_<construction>.png 4. per-leg audit: price ($, left) vs weighted delta
                               (%, right), entries/exits marked
  pnl_intraday.png          5. intraday mark-to-market basket P&L, 4 curves
  pnl_waterfall.png         6. leg-contribution waterfall per construction
  basket_comparison.png     7. fitting-window cumulative returns + hedged
                               residual, reporter vs all 4 baskets

Charts 3-7 require a traded direction; neutral/skip events get 1-2 + K + facts.
"""

import json
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from rules.sessions import (RTH_END_MIN, RTH_START_MIN, consolidate,
                            entry_price, exit_price, previous_close,
                            signal_price, trade_day_for_event)
from rules.direction import fade_the_gap
from construction.base import BasketContext
from construction.registry import CONSTRUCTIONS
from display import box_table
import plot_event

ET = "America/New_York"


def _rth_prices(bars, day, symbols=None):
    """Trade-day RTH minute closes, wide. If `symbols` is given, guarantee a
    column for each (a basket leg may have NO RTH print that day — thin peers
    whose entry forward-filled; its column is filled with its last known
    print so charts can mark the flat/stale leg instead of crashing)."""
    rth = bars[(bars["et_date"] == day) & (bars["et_min"] >= RTH_START_MIN)
               & (bars["et_min"] < RTH_END_MIN)]
    px = (rth.pivot_table(index="ts", columns="symbol", values="close")
          .sort_index().astype("float64").ffill())
    if symbols:
        for sym in symbols:
            if sym not in px.columns:
                prior = bars[(bars["symbol"] == sym)
                             & ((bars["et_date"] < day)
                                | ((bars["et_date"] == day)
                                   & (bars["et_min"] < RTH_START_MIN)))]
                px[sym] = float(prior.iloc[-1]["close"]) if len(prior) else float("nan")
        px = px.ffill().bfill()
    return px


def _pre_event_prices(bars, day, reporter=None):
    """Pre-event RTH closes, wide, inner-joined on common minutes — the ridge
    fitting view. The strict inner join can empty out when one thin symbol
    never overlaps; if that would drop the REPORTER, retry with only the
    columns that survive a reporter-anchored join (drop thin peers, not
    minutes)."""
    pre = bars[(bars["et_date"] < day) & (bars["et_min"] >= RTH_START_MIN)
               & (bars["et_min"] < RTH_END_MIN)]
    wide = (pre.pivot_table(index="ts", columns="symbol", values="close")
            .sort_index().astype("float64"))
    joined = wide.dropna(how="any")
    if reporter is not None and (joined.empty or reporter not in joined.columns):
        if reporter not in wide.columns:
            return pd.DataFrame()          # reporter has no pre-event RTH bars at all
        anchored = wide[wide[reporter].notna()]
        keep = [c for c in anchored.columns if anchored[c].notna().all()]
        joined = anchored[keep]
    return joined


def _legmarks(bars, basket, reporter, day):
    out = {}
    for sym in basket.weights:
        e = entry_price(bars, sym, day, allow_ffill=(sym != reporter))
        x = exit_price(bars, sym, day)
        out[sym] = (e, x)
    return out


# ------------------------------------------------------------------ figures

def fig_ridge_input(prices, reporter, out):
    idx = prices / prices.iloc[0] * 100
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
    syms = {s for b in baskets.values() for s in b.weights}
    px = _rth_prices(bars, day, symbols=syms)
    xt = px.index.tz_convert(ET).strftime("%H:%M")
    step = max(1, len(px) // 13)
    fig, axes = plt.subplots(2, 2, figsize=(17, 11), sharex=True, sharey=True)
    for ax, (name, b) in zip(axes.flat, baskets.items()):
        if not b.weights:
            ax.set_title(f"{name} — SKIPPED: {b.diagnostics.get('skip')}",
                         loc="left", fontsize=10)
            continue
        total = pd.Series(0.0, index=px.index)
        legs = []
        dropped = []
        for sym, w in b.weights.items():
            e = entry_price(bars, sym, day, allow_ffill=(sym != reporter))
            if e is None or e <= 0:
                dropped.append(sym)     # unmarkable leg: P&L layer drops it too
                continue
            c = w * (px[sym] / e - 1) * 100
            total += c
            legs.append((sym, w, c))
        if not legs:
            ax.set_title(f"{name} — no markable legs", loc="left", fontsize=10)
            continue
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
        title = name + (f"  (unmarkable dropped: {' '.join(dropped)})" if dropped else "")
        ax.set_title(title, loc="left", fontsize=11, fontweight="bold")
        ax.grid(alpha=0.3); ax.legend(loc="best", fontsize=7)
    for ax in axes[1]:
        ax.set_xticks(range(0, len(px), step)); ax.set_xticklabels(xt[::step])
    fig.suptitle("P&L components, weight-scaled — universe=all — black = basket total", y=0.995)
    fig.tight_layout(); fig.savefig(out, dpi=110); plt.close(fig)


def fig_perleg(bars, basket, reporter, day, name, out):
    px = _rth_prices(bars, day, symbols=set(basket.weights))
    xt = px.index.tz_convert(ET).strftime("%H:%M")
    step = max(1, len(px) // 8)
    legs = sorted(basket.weights.items(), key=lambda t: (t[0] != reporter, -abs(t[1])))
    n = len(legs)
    ncols = 3
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(18, 4 * nrows),
                             sharex=True, squeeze=False)
    total = 0.0
    audit = []
    for ax, (sym, w) in zip(axes.flat, legs):
        e = entry_price(bars, sym, day, allow_ffill=(sym != reporter))
        x = exit_price(bars, sym, day)
        if e is None or x is None or e <= 0:
            # unmarkable leg (no prints anywhere): the P&L layer dropped it;
            # record it in the audit and leave a labeled empty panel
            audit.append({"symbol": sym, "weight": w, "entry": e, "exit": x,
                          "raw_return": None, "contribution": None})
            ax.set_title(f"{sym}  w={w:+.4f}\nUNMARKABLE (no prints) — dropped from P&L",
                         loc="left", fontsize=8.5, color="tab:red", fontweight="bold")
            ax.grid(alpha=0.3)
            continue
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


def fig_intraday(bars, baskets, reporter, day, out):
    syms = {s for b in baskets.values() for s in b.weights}
    px = _rth_prices(bars, day, symbols=syms)
    xt = px.index.tz_convert(ET).strftime("%H:%M")
    step = max(1, len(px) // 13)
    colors = dict(zip(baskets, ["tab:blue", "tab:red", "tab:green", "tab:purple"]))
    fig, ax = plt.subplots(figsize=(16, 7))
    for name, b in baskets.items():
        if not b.weights:
            continue
        curve = pd.Series(0.0, index=px.index)
        for sym, w in b.weights.items():
            e = entry_price(bars, sym, day, allow_ffill=(sym != reporter))
            if e is None or e <= 0:
                continue                # unmarkable leg: dropped, as in P&L
            curve = curve + w * (px[sym] / e - 1) * 100
        ax.plot(range(len(px)), curve.values, lw=1.5, color=colors[name],
                label=f"{name}  (final {curve.iloc[-1]:+.3f}%)")
    ax.axhline(0, color="gray", lw=0.8)
    ax.set_xticks(range(0, len(px), step)); ax.set_xticklabels(xt[::step])
    ax.set_ylabel("basket P&L (%, per $1 reporter notional)")
    ax.set_title("Intraday mark-to-market basket P&L — universe=all — "
                 "final value must equal the K table", loc="left")
    ax.grid(alpha=0.3); ax.legend(loc="best", fontsize=10)
    fig.tight_layout(); fig.savefig(out, dpi=110); plt.close(fig)


def fig_waterfall(bars, baskets, reporter, day, out):
    fig, axes = plt.subplots(2, 2, figsize=(16, 10), sharey=True)
    for ax, (name, b) in zip(axes.flat, baskets.items()):
        if not b.weights:
            ax.set_title(f"{name} — SKIPPED", loc="left", fontsize=10)
            continue
        marks = _legmarks(bars, b, reporter, day)
        legs = [(s, w, marks[s][0], marks[s][1],
                 w * (marks[s][1] / marks[s][0] - 1) * 100)
                for s, w in b.weights.items()
                if marks[s][0] and marks[s][1]]
        legs.sort(key=lambda t: (t[0] != reporter, -abs(t[4])))
        cum = 0.0
        for i, (sym, w, e, x, c) in enumerate(legs):
            ax.bar(i, c, bottom=cum, color=("tab:green" if c >= 0 else "tab:red"),
                   alpha=0.85, width=0.7)
            ax.annotate(f"{c:+.2f}", xy=(i, cum + c),
                        xytext=(0, 4 if c >= 0 else -12),
                        textcoords="offset points", ha="center", fontsize=8)
            cum += c
        total = sum(t[4] for t in legs)
        ax.bar(len(legs), total, color="black", alpha=0.8, width=0.7)
        ax.annotate(f"TOTAL\n{total:+.3f}%", xy=(len(legs), total), xytext=(0, 6),
                    textcoords="offset points", ha="center", fontsize=9, fontweight="bold")
        ax.set_xticks(range(len(legs) + 1))
        ax.set_xticklabels([f"{t[0]}\nw={t[1]:+.2f}" for t in legs] + ["Σ"], fontsize=8)
        ax.axhline(0, color="gray", lw=0.8)
        ax.set_title(name, loc="left", fontsize=11, fontweight="bold")
        ax.grid(alpha=0.3, axis="y")
    fig.suptitle("Leg-by-leg P&L waterfall — universe=all — each bar = "
                 "weight x (exit/entry - 1); stacks to the K-table total", y=0.995)
    fig.tight_layout(); fig.savefig(out, dpi=110); plt.close(fig)


def fig_basket_comparison(bars, baskets, reporter, day, out):
    px = _pre_event_prices(bars, day, reporter=reporter)
    if px.empty or reporter not in px.columns:
        return
    rets = np.log(px).diff().dropna(how="any")
    rep = rets[reporter]
    et = px.index.tz_convert(ET)
    days = sorted(set(et.date))
    starts = [next(i for i, t in enumerate(et) if t.date() == d) for d in days]
    colors = dict(zip(baskets, ["tab:blue", "tab:red", "tab:green", "tab:purple"]))
    fig, axes = plt.subplots(2, 1, figsize=(16, 10), sharex=True,
                             gridspec_kw={"height_ratios": [1.4, 1]})
    ax0, ax1 = axes
    x = range(len(rep))
    ax0.plot(x, rep.cumsum() * 100, lw=2.2, color="black", label=f"{reporter} (reporter)")
    stats = []
    for name, b in baskets.items():
        hedge = {s: abs(w) for s, w in b.weights.items() if s != reporter and s in rets.columns}
        if not hedge:
            continue
        basket = rets[list(hedge)].to_numpy() @ np.array(list(hedge.values()))
        basket = pd.Series(basket, index=rep.index)
        ax0.plot(x, basket.cumsum() * 100, lw=1.3, color=colors[name],
                 label=f"{name} (gross {sum(hedge.values()):.2f})")
        resid = rep - basket
        ax1.plot(x, resid.cumsum() * 100, lw=1.3, color=colors[name],
                 label=f"{reporter} − {name}")
        stats.append((name, rep.corr(basket), resid.std() * np.sqrt(390) * 100))
    txt = "corr w/ reporter | daily TE\n" + "\n".join(
        f"{n:<30s} {c:.2f} | {te:.2f}%" for n, c, te in stats)
    ax0.annotate(txt, xy=(0.985, 0.03), xycoords="axes fraction", ha="right",
                 fontsize=9, family="monospace",
                 bbox=dict(boxstyle="round", fc="white", alpha=0.85))
    ax0.set_ylabel("cumulative log-return (%)")
    ax0.set_title("Reporter vs hedge baskets — pre-event RTH fitting window "
                  "(in-sample; trade-day P&L is the out-of-sample test)", loc="left")
    ax1.axhline(0, color="gray", lw=0.8)
    ax1.set_ylabel("cumulative residual (%)")
    ax1.set_title("Hedged residual (flatter = better hedge)", loc="left")
    for ax in axes:
        for ds in starts:
            ax.axvline(ds, color="gray", lw=0.8, ls=":")
        ax.grid(alpha=0.3); ax.legend(loc="upper right", fontsize=9); ax.margins(x=0.01)
    fig.tight_layout(); fig.savefig(out, dpi=110); plt.close(fig)


# ------------------------------------------------------------------ bundle

def build_event_report(panel: pd.DataFrame, table: pd.DataFrame, outdir: Path,
                       construction: str = "ridge_returns_beta_neutral") -> dict:
    """Write the full report bundle for one event; returns the facts dict.
    `table` is evaluate.event_table(panel) — passed in to avoid a circular
    import and recomputation."""
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    reporter = panel[panel["reporter_relationship"] == "primary"]["symbol"].iloc[0]
    event_id = int(panel["event_id"].iloc[0])
    event_ts = panel[panel["event_bar"]]["ts"].iloc[0]
    day = trade_day_for_event(event_ts)
    bars = consolidate(panel)
    peers = {rel: sorted(g["symbol"].unique())
             for rel, g in panel[panel["reporter_relationship"] != "primary"]
             .groupby("reporter_relationship")}

    # K table + CSV — the same content in both formats
    table.to_csv(outdir / f"event_{event_id}.csv", index=False)
    (outdir / f"event_{event_id}.k").write_text(box_table(table) + "\n")

    facts = {"event_id": event_id, "reporter": reporter,
             "event_ts_utc": str(event_ts),
             "event_ts_et": str(event_ts.tz_convert(ET)),
             "trade_day": day, "peers": peers}
    if day is None:
        (outdir / "facts.json").write_text(json.dumps(facts, indent=2, default=str))
        return facts

    pc = previous_close(bars, reporter, day)
    sp = signal_price(bars, reporter, day)
    sig = fade_the_gap(sp, pc)
    facts["marks"] = {"previous_close": pc, "signal_928": sp,
                      "entry_930_open": entry_price(bars, reporter, day, allow_ffill=False),
                      "exit_1559_close": exit_price(bars, reporter, day)}
    facts["signal"] = (None if sig is None else
                       {"gap": sig.gap, "direction": sig.direction, "reason": sig.reason})
    facts["premarket_prints_reporter"] = int(
        ((bars["symbol"] == reporter) & (bars["et_date"] == day)
         & (bars["et_min"] < RTH_START_MIN)).sum())

    plot_event.plot(panel, str(outdir / "event_marks.png"))                    # 1
    pre_px = _pre_event_prices(bars, day, reporter=reporter)
    facts["ridge_input"] = {"n_minutes": len(pre_px),
                            "n_symbols": pre_px.shape[1] if len(pre_px) else 0}
    if len(pre_px):
        pre_px.to_csv(outdir / "ridge_input_prices.csv")
        fig_ridge_input(pre_px, reporter, outdir / "ridge_input.png")          # 2

    if sig and sig.direction != 0:
        ctx = BasketContext(bars=bars, reporter=reporter, peers=peers,
                            trade_day=day, direction=sig.direction)
        baskets = {n: b(ctx, "all") for n, b in CONSTRUCTIONS.items()}
        fig_components(bars, baskets, reporter, day, outdir / "pnl_components.png")   # 3
        target = baskets.get(construction)
        if target and target.weights:
            audit, total = fig_perleg(bars, target, reporter, day, construction,
                                      outdir / f"perleg_{construction}.png")   # 4
            facts["perleg_audit"] = {"construction": construction,
                                     "legs": audit, "total_pnl": total}
            facts["perleg_diagnostics"] = target.diagnostics
        fig_intraday(bars, baskets, reporter, day, outdir / "pnl_intraday.png")       # 5
        fig_waterfall(bars, baskets, reporter, day, outdir / "pnl_waterfall.png")     # 6
        fig_basket_comparison(bars, baskets, reporter, day,
                              outdir / "basket_comparison.png")                # 7

    (outdir / "facts.json").write_text(json.dumps(facts, indent=2, default=str))
    return facts


def sync_dir_to_s3(s3io, outdir: Path, base_uri: str) -> int:
    """Mirror one event's report bundle to s3://.../<event_id>/ via
    polars/fsspec streams (no boto3 uploads)."""
    import polars as pl
    n = 0
    for f in sorted(Path(outdir).iterdir()):
        if not f.is_file():
            continue
        uri = f"{base_uri}/{f.name}"
        if f.suffix == ".csv":
            s3io.write_csv(pl.read_csv(f, infer_schema_length=0), uri)
        else:                       # .png / .k / .json
            s3io.write_bytes(f.read_bytes(), uri)
        n += 1
    return n