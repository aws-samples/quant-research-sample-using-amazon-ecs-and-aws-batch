"""Neutral-band sweep: re-derive Arm S at multiple neutral thresholds.

No new Batch compute. Per-event long P&L is recovered from Arm R rows
(pnl * direction), model scores come from the Arm S shards, and the frozen
signs.parquet supplies a MATCHED null per (model, threshold): the 64 random
portfolios restricted to exactly the events that model trades at that
threshold. Empirical p = (1 + #null >= model) / (1 + 64), same formula as
aggregate.placement.

Thresholds swept: neutral in {0, 1, 2, 3, 4} on the -5..+5 integer score
scale (score > +n -> LONG, score < -n -> SHORT, |score| <= n -> skip).

Outputs (local results/aggregate/ + S3 aggregate prefix):
- neutral_sweep.csv: model x threshold -> sharpe_annualized, p, n_traded, ...
- neutral_sweep_matrix.png: annotated model x threshold Sharpe heatmap
"""
import argparse
import io
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import study

sys.path.insert(0, str(Path(__file__).resolve().parent))

from aggregate import sharpe_annualized  # noqa: E402
import signs as signs_mod  # noqa: E402

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
NEUTRALS = [0.0, 1.0, 2.0, 3.0, 4.0]


def _prefix():
    return study.current().results_prefix
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
MUTED = "#898781"


def sweep_frames(long_pnl: pd.Series, trade_day: pd.Series,
                 scores: pd.Series, signs_by_seed: pd.DataFrame,
                 neutral: float):
    """One (model, threshold) cell + its matched 64-seed null.

    long_pnl/trade_day: indexed by event_id (data-viable events).
    scores: event_id -> score (may contain NaN).
    signs_by_seed: events x seeds frame of +-1 from signs.parquet.
    Returns (model_row_dict, null_sharpes array).
    """
    sc = scores.reindex(long_pnl.index)
    traded = sc[sc.notna() & (sc.abs() > neutral)]
    idx = traded.index
    n_long = int((traded > 0).sum())
    if len(idx) < 2:
        return {"n_traded": len(idx), "n_long": n_long,
                "sharpe_annualized": np.nan, "p_annualized": np.nan,
                "hit_rate": np.nan}, np.array([])

    pnl = np.sign(traded.values) * long_pnl.loc[idx].values
    model_df = pd.DataFrame({"trade_day": trade_day.loc[idx].values,
                             "pnl": pnl})
    model_sharpe = sharpe_annualized(model_df)

    null_sharpes = []
    sub_signs = signs_by_seed.loc[idx]
    days = trade_day.loc[idx].values
    base = long_pnl.loc[idx].values
    for seed in sub_signs.columns:
        null_df = pd.DataFrame({"trade_day": days,
                                "pnl": sub_signs[seed].values * base})
        null_sharpes.append(sharpe_annualized(null_df))
    null_sharpes = np.array([s for s in null_sharpes if not np.isnan(s)])

    p = np.nan
    if not np.isnan(model_sharpe) and len(null_sharpes):
        p = (1 + (null_sharpes >= model_sharpe).sum()) / (1 + len(null_sharpes))
    return {"n_traded": len(idx), "n_long": n_long,
            "sharpe_annualized": model_sharpe, "p_annualized": p,
            "hit_rate": float((pnl > 0).mean())}, null_sharpes


def render_matrix(sweep: pd.DataFrame, out_png: Path) -> None:
    """Annotated model x threshold heatmap: cell = annualized Sharpe."""
    piv_sr = sweep.pivot(index="model", columns="neutral",
                         values="sharpe_annualized")
    piv_sr = piv_sr.loc[piv_sr[1.0].sort_values(ascending=False).index]
    piv_p = sweep.pivot(index="model", columns="neutral",
                        values="p_annualized").loc[piv_sr.index]
    piv_n = sweep.pivot(index="model", columns="neutral",
                        values="n_traded").loc[piv_sr.index]

    fig, ax = plt.subplots(figsize=(11, 16), facecolor=SURFACE)
    vmax = np.nanmax(np.abs(piv_sr.values))
    im = ax.imshow(piv_sr.values, cmap="RdYlGn", vmin=-vmax, vmax=vmax,
                   aspect="auto")
    ax.set_xticks(range(len(piv_sr.columns)))
    ax.set_xticklabels([f"±{int(c)}\n(trade |s|>{int(c)})"
                        for c in piv_sr.columns], fontsize=9, color=INK)
    ax.set_yticks(range(len(piv_sr.index)))
    ax.set_yticklabels(piv_sr.index, fontsize=7.5, color=INK)
    for i in range(piv_sr.shape[0]):
        for j in range(piv_sr.shape[1]):
            sr, p, n = piv_sr.iat[i, j], piv_p.iat[i, j], piv_n.iat[i, j]
            if np.isnan(sr):
                txt = f"n={int(n)}" if not np.isnan(n) else "—"
            else:
                star = "*" if (not np.isnan(p) and p < 0.05) else ""
                txt = f"{sr:+.2f}{star}\np={p:.2f} n={int(n)}"
            ax.text(j, i, txt, ha="center", va="center", fontsize=6.5,
                    color=INK)
    ax.set_xlabel("neutral band (scores in [−n, +n] skip)", fontsize=10,
                  color=INK)
    ax.set_title("Neutral-band sweep — annualized Sharpe per model "
                 "(p vs matched 64-seed null; * p<0.05)\n"
                 "sorted by the official neutral=1 column",
                 fontsize=11, color=INK)
    fig.colorbar(im, ax=ax, shrink=0.4, label="annualized Sharpe")
    fig.tight_layout()
    fig.savefig(out_png, dpi=130, facecolor=SURFACE)
    plt.close(fig)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--profile", default=None)
    args = ap.parse_args()

    import boto3
    from s3io import S3IO
    s3io = S3IO(profile=args.profile)
    sess = (boto3.Session(profile_name=args.profile) if args.profile
            else boto3.Session())
    s3 = sess.client("s3")

    print("Loading Arm R seed 0 (long P&L reference)...")
    r0 = pd.read_parquet(io.BytesIO(s3.get_object(
        Bucket=_bucket(), Key=f"{_prefix()}/shards/arm=R/seed=00.parquet")["Body"].read()))
    ok = r0[r0["status"] == "ok"]
    eid = ok["event_id"].astype(int).values
    long_pnl = pd.Series((ok["pnl"] * ok["direction"]).astype(float).values,
                         index=eid)
    trade_day = pd.Series(ok["trade_day"].values, index=eid)

    print("Loading frozen signs (sha-verified)...")
    signs_table = signs_mod.load_signs(s3io)
    signs_by_seed = pd.Series(signs_table).unstack()  # events x seeds
    signs_by_seed = signs_by_seed.loc[signs_by_seed.index.intersection(long_pnl.index)]

    print("Loading Arm S shards for scores...")
    from validate import _load_arm_s_shards
    s_shards = _load_arm_s_shards(s3io, _bucket(), f"{_prefix()}/shards/arm=S/")

    rows = []
    for shard in s_shards:
        model = shard["sentiment_model"].dropna().iloc[0]
        scores = shard.set_index(shard["event_id"].astype(int))["sentiment_score"]
        for neutral in NEUTRALS:
            cell, _ = sweep_frames(long_pnl, trade_day, scores,
                                   signs_by_seed, neutral)
            rows.append({"model": model, "neutral": neutral} | cell)
        print(f"  {model} done")
    sweep = pd.DataFrame(rows)

    out_dir = Path(__file__).resolve().parent / "results" / "aggregate"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "neutral_sweep.csv"
    png_path = out_dir / "neutral_sweep_matrix.png"
    sweep.to_csv(csv_path, index=False)
    render_matrix(sweep, png_path)
    print(f"wrote {csv_path}\nwrote {png_path}")

    for local in (csv_path, png_path):
        key = f"{_prefix()}/aggregate/{local.name}"
        s3.upload_fileobj(io.BytesIO(local.read_bytes()), _bucket(), key)
        print(f"uploaded s3://{_bucket()}/{key}")

    best = sweep.loc[sweep.groupby("neutral")["sharpe_annualized"].idxmax()]
    print("\nBest model per band:")
    print(best[["neutral", "model", "sharpe_annualized", "p_annualized",
                "n_traded"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
