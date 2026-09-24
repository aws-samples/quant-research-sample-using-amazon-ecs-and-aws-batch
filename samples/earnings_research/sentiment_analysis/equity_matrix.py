"""Equity-curve matrix: one panel per sentiment model vs the 64-seed null.

Each panel shows the model's cumulative P&L curves — one per neutral band
(±0..±3, blues light→dark; ±1 is the official arm; ±4 omitted as ~10-trade
noise) — over the full study window against all 64 frozen random-sign
portfolios (gray hairlines) plus the null p5-p95 band, the null median, and
the long-reporter/short-peers benchmark. Band curves other than ±1 are reconstructed as
sign(score) x long_pnl on |score| > n (same reconstruction as neutral_sweep,
which reproduces the official ±1 shard bit-exactly). Panels sort by the
official ±1 annualized Sharpe and share one y-scale. Writes
equity_matrix.png locally under results/aggregate/ and uploads to the S3
aggregate prefix.
"""
import argparse
import io
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import study

sys.path.insert(0, str(Path(__file__).resolve().parent))

from aggregate import always_long_frame, sharpe_annualized  # noqa: E402
from validate import _load_arm_r_shards, _load_arm_s_shards  # noqa: E402

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")


def _prefix():
    return study.current().results_prefix

# dataviz reference palette (light mode)
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
MUTED = "#898781"
GRID = "#e1e0d9"
BASELINE = "#c3c2b7"
NULL_GRAY = "#c3c2b7"
BAND_GRAY = "#e1e0d9"
NULL_MEDIAN = "#52514e"
ALWAYS_LONG = "#eb6834"
SPY_GREEN = "#008300"

def spy_minute_uri() -> str:
    return f"s3://{_bucket()}/market-data/spy/spy_1min_2y.parquet"


def spy_cum_curve(s3io, calendar: pd.DatetimeIndex):
    """SPY buy&hold as CUMULATIVE SUM of daily close-to-close returns on the
    shared calendar — the same accumulation convention as the strategy
    curves (sum of per-event returns, not compounded), so the axis stays
    comparable. Logic COPIED (not imported) from the frozen basket study's
    benchmark.py daily reduction. Returns None if the data is absent.
    """
    try:
        df = s3io.read_parquet(spy_minute_uri()).to_pandas()
    except Exception:
        print(f"benchmark: no SPY data at {spy_minute_uri()} — overlay skipped")
        return None
    ts = pd.to_datetime(df["timestamp"]).dt.tz_convert("America/New_York")
    df = df.assign(et_date=ts.dt.normalize().dt.tz_localize(None),
                   et_min=ts.dt.hour * 60 + ts.dt.minute)
    rth = df[(df["et_min"] >= 570) & (df["et_min"] < 960)]
    closes = rth.sort_values("et_min").groupby("et_date")["close"].last()
    rets = closes.pct_change().dropna()
    return rets.reindex(calendar, fill_value=0.0).cumsum()

# neutral bands: blues light→dark (sequential ramp steps 200/350/450/650);
# ±1 is the official arm and is drawn heaviest
NEUTRALS = [0.0, 1.0, 2.0, 3.0]
BAND_BLUES = {0.0: "#9ec5f4", 1.0: "#5598e7", 2.0: "#256abf", 3.0: "#104281"}
BAND_WIDTH = {0.0: 1.0, 1.0: 1.9, 2.0: 1.0, 3.0: 1.0}


def daily_equity(df: pd.DataFrame, calendar: pd.DatetimeIndex) -> pd.Series:
    """Cumulative P&L on a shared business-day calendar (zeros off-days)."""
    ok = df[df["status"] == "ok"]
    daily = ok.assign(trade_day=pd.to_datetime(ok["trade_day"])) \
              .groupby("trade_day")["pnl"].sum()
    return daily.reindex(calendar, fill_value=0.0).cumsum()


def load_job_scores(profile, job):
    """model -> Series(event_id -> score) straight from a scoring job's
    partitions (for prompt sweeps, where no Arm S shards exist)."""
    import boto3
    sess = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = sess.client("s3")
    prefix = f"earnings-sentiment/output/job={job}/"
    out = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=_bucket(), Prefix=prefix):
        for o in page.get("Contents", []):
            if not o["Key"].endswith("data.parquet"):
                continue
            model = o["Key"].split("model=")[1].split("/")[0]
            df = pd.read_parquet(io.BytesIO(
                s3.get_object(Bucket=_bucket(), Key=o["Key"])["Body"].read()))
            out[model] = df.set_index(df["event_id"].astype(int))["sentiment_score"]
    return out


def build_figure(r_shards, s_shards, scores_by_model=None, title_note="",
                 spy_curve=None):
    all_days = pd.concat(
        [pd.to_datetime(df.loc[df["status"] == "ok", "trade_day"])
         for df in list(r_shards.values()) + s_shards])
    calendar = pd.bdate_range(all_days.min(), all_days.max(), freq="B")

    null_curves = pd.DataFrame(
        {seed: daily_equity(df, calendar) for seed, df in r_shards.items()})
    null_p5 = null_curves.quantile(0.05, axis=1)
    null_p95 = null_curves.quantile(0.95, axis=1)
    null_median = null_curves.median(axis=1)

    al = always_long_frame(r_shards[min(r_shards.keys())])
    al_daily = al.assign(trade_day=pd.to_datetime(al["trade_day"])) \
                 .groupby("trade_day")["pnl"].sum()
    al_curve = al_daily.reindex(calendar, fill_value=0.0).cumsum()

    # per-event long P&L reference for band reconstruction (any R shard)
    r0 = r_shards[min(r_shards.keys())]
    ok0 = r0[r0["status"] == "ok"]
    eid = ok0["event_id"].astype(int).values
    long_pnl = pd.Series((ok0["pnl"] * ok0["direction"]).astype(float).values,
                         index=eid)
    tday = pd.Series(pd.to_datetime(ok0["trade_day"]).values, index=eid)

    if scores_by_model is None:
        scores_by_model = {
            df["sentiment_model"].dropna().iloc[0]:
                df.set_index(df["event_id"].astype(int))["sentiment_score"]
            for df in s_shards}

    models = []
    for name, scores in scores_by_model.items():
        sc = scores.reindex(long_pnl.index)
        curves = {}
        band_dfs = {}
        for n in NEUTRALS:
            traded = sc[sc.notna() & (sc.abs() > n)]
            pnl = np.sign(traded.values) * long_pnl.loc[traded.index].values
            band_dfs[n] = pd.DataFrame(
                {"trade_day": tday.loc[traded.index].values, "pnl": pnl})
            daily = pd.Series(pnl, index=tday.loc[traded.index].values) \
                      .groupby(level=0).sum()
            curves[n] = daily.reindex(calendar, fill_value=0.0).cumsum()
        models.append((name, curves, sharpe_annualized(band_dfs[1.0])))
    models.sort(key=lambda m: m[2], reverse=True)

    all_curve_vals = [c.values for _, cs, _ in models for c in cs.values()]
    spy_vals = [spy_curve.values] if spy_curve is not None else []
    lo = min(null_curves.values.min(), al_curve.min(),
             *[v.min() for v in spy_vals],
             min(v.min() for v in all_curve_vals))
    hi = max(null_curves.values.max(), al_curve.max(),
             *[v.max() for v in spy_vals],
             max(v.max() for v in all_curve_vals))
    pad = 0.05 * (hi - lo)

    ncols, nrows = 7, 7
    fig, axes = plt.subplots(nrows, ncols, figsize=(26, 22), sharex=True,
                             sharey=True, facecolor=SURFACE)
    fig.subplots_adjust(hspace=0.10, wspace=0.04,
                        left=0.03, right=0.995, top=0.925, bottom=0.03)
    sr_cmap = plt.get_cmap("RdYlGn")

    for ax, (name, curves, sr) in zip(axes.flat, models):
        ax.set_facecolor(SURFACE)
        ax.fill_between(calendar, null_p5, null_p95,
                        color=BAND_GRAY, alpha=0.55, linewidth=0)
        ax.plot(calendar, null_curves.values, color=NULL_GRAY,
                linewidth=0.4, alpha=0.5)
        ax.plot(calendar, null_median, color=NULL_MEDIAN, linewidth=1.4,
                linestyle="--")
        ax.plot(calendar, al_curve, color=ALWAYS_LONG, linewidth=1.2,
                linestyle="-.")
        if spy_curve is not None:
            ax.plot(calendar, spy_curve, color=SPY_GREEN, linewidth=1.2,
                    linestyle=":")
        for n in NEUTRALS:
            ax.plot(calendar, curves[n], color=BAND_BLUES[n],
                    linewidth=BAND_WIDTH[n])
        ax.axhline(0, color=BASELINE, linewidth=0.8)

        tag_color = sr_cmap(np.clip(sr, -1, 1) / 2 + 0.5)
        ax.set_title(name, fontsize=9, color=INK, pad=2, loc="left")
        ax.text(0.985, 0.96, f"SR {sr:+.2f}", transform=ax.transAxes,
                ha="right", va="top", fontsize=8, fontweight="bold",
                color=INK, bbox=dict(boxstyle="round,pad=0.25",
                                     facecolor=tag_color, alpha=0.85,
                                     edgecolor="none"))
        ax.set_ylim(lo - pad, hi + pad)
        ax.grid(axis="x", color=GRID, alpha=0.10)
        ax.grid(axis="y", color=GRID, alpha=0.25)
        ax.tick_params(colors=MUTED, labelsize=7)
        for s in ax.spines.values():
            s.set_color(BASELINE)
        ax.xaxis.set_major_locator(mdates.YearLocator(2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    for ax in axes.flat[len(models):]:
        ax.set_visible(False)

    start, end = calendar[0].date(), calendar[-1].date()
    fig.suptitle(
        f"Sentiment-model equity curves by neutral band vs the 64-portfolio "
        f"random null{title_note} — {study.current().key.upper()} earnings, {start} → {end} "
        f"({len(calendar)} business days)\n"
        "gray: 64 frozen random-sign portfolios (band = null p5–p95, "
        "dashed = null median) · orange dash-dot: long-reporter/short-peers · "
        "blues light→dark: neutral band ±0/±1/±2/±3 (±1 = official arm, "
        "heavy; trade |score|>n) · SR tag = official ±1 · "
        "lookahead-bias caveat per spec §8",
        fontsize=13, color=INK, y=0.988)

    import matplotlib.lines as mlines
    handles = [mlines.Line2D([], [], color=BAND_BLUES[n], linewidth=BAND_WIDTH[n],
                             label=f"band ±{int(n)}") for n in NEUTRALS]
    handles += [
        mlines.Line2D([], [], color=ALWAYS_LONG, linestyle="-.",
                      linewidth=1.2, label="long-rep/short-peers"),
        mlines.Line2D([], [], color=SPY_GREEN, linestyle=":",
                      linewidth=1.2, label="SPY buy&hold"),
        mlines.Line2D([], [], color=NULL_MEDIAN, linestyle="--",
                      linewidth=1.4, label="null median"),
        mlines.Line2D([], [], color=NULL_GRAY, linewidth=0.8,
                      label="64 random"),
    ]
    fig.legend(handles=handles, loc="upper center", ncol=8, fontsize=9,
               frameon=False, bbox_to_anchor=(0.5, 0.952))
    return fig


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--profile", default=None)
    ap.add_argument("--scores-job", default=None,
                    help="Scoring job name: draw band curves from that job's "
                         "score partitions instead of the official Arm S "
                         "shards (prompt sweeps). Output file gets the job "
                         "as a suffix.")
    args = ap.parse_args()

    from s3io import S3IO
    s3io = S3IO(profile=args.profile)
    print("Loading Arm R shards...")
    r_shards = _load_arm_r_shards(s3io, _bucket(), f"{_prefix()}/shards/arm=R/")
    print("Loading Arm S shards...")
    s_shards = _load_arm_s_shards(s3io, _bucket(), f"{_prefix()}/shards/arm=S/")

    scores_by_model, note, fname = None, "", "equity_matrix.png"
    if args.scores_job:
        print(f"Loading scores from job={args.scores_job}...")
        scores_by_model = load_job_scores(args.profile, args.scores_job)
        note = f" — scores: {args.scores_job}"
        fname = f"equity_matrix_{args.scores_job}.png"
    print(f"Rendering vs {len(r_shards)} null curves...")

    all_days = pd.concat(
        [pd.to_datetime(df.loc[df["status"] == "ok", "trade_day"])
         for df in list(r_shards.values()) + s_shards])
    calendar = pd.bdate_range(all_days.min(), all_days.max(), freq="B")
    spy_curve = spy_cum_curve(s3io, calendar)

    fig = build_figure(r_shards, s_shards, scores_by_model, note,
                       spy_curve=spy_curve)
    out = Path(__file__).resolve().parent / "results" / "aggregate" / fname
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=110, facecolor=SURFACE)
    plt.close(fig)
    print(f"wrote {out}")

    import boto3
    sess = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    key = f"{_prefix()}/aggregate/{fname}"
    sess.client("s3").upload_fileobj(io.BytesIO(out.read_bytes()), _bucket(), key)
    print(f"uploaded s3://{_bucket()}/{key}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
