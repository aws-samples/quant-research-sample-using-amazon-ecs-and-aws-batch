"""Aggregation and null-band placement for sentiment analysis (design spec §8).

Library functions:
- sharpe_per_event: per-event Sharpe (mean/std of PnLs)
- sharpe_annualized: annualized Sharpe over a business-day calendar
- null_band: compute null band from 64 random-sign shards
- placement: model placement against null (with empirical p-values)
- render_placement_chart: horizontal violin/scatter placement chart

CLI: aggregate.py --profile <profile>
Loads all R and S shards, computes null band and model placements,
writes results to local results/aggregate/ and S3.
"""
import argparse
import io
import re
import sys
from pathlib import Path
from typing import Dict, List

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import study

sys.path.insert(0, str(Path(__file__).resolve().parent))
import settings  # noqa: E402


def sharpe_per_event(pnl: np.ndarray) -> float:
    """Per-event Sharpe: mean(pnl) / std(pnl, ddof=1).

    Returns np.nan if len(pnl) < 2 or std == 0.

    Args:
        pnl: 1D array of PnL values

    Returns:
        Sharpe ratio or np.nan if degenerate
    """
    if len(pnl) < 2:
        return np.nan

    mu = np.mean(pnl)
    sigma = np.std(pnl, ddof=1)

    if sigma == 0:
        return np.nan

    return mu / sigma


def sharpe_annualized(df: pd.DataFrame) -> float:
    """Annualized Sharpe for one strategy.

    Args:
        df: DataFrame with 'trade_day' (date) and 'pnl' (float) columns
            containing only OK rows for ONE strategy

    Returns:
        Annualized Sharpe: mean/std * sqrt(252)
        Returns np.nan if degenerate (no events, zero variance, etc.)
    """
    if df.empty:
        return np.nan

    # Shards store trade_day as string (spec §6) — normalize so the groupby
    # index aligns with the bdate_range index below
    df = df.assign(trade_day=pd.to_datetime(df["trade_day"]))

    # Build business-day calendar over [min, max] of trade_day
    min_date = df["trade_day"].min()
    max_date = df["trade_day"].max()

    if pd.isna(min_date) or pd.isna(max_date):
        return np.nan

    # Create business day range
    bday_range = pd.bdate_range(start=min_date, end=max_date, freq="B")

    # Initialize series with zeros
    daily_pnl = pd.Series(0.0, index=bday_range)

    # Sum PnL per day (handles multiple events on same day)
    grouped = df.groupby("trade_day")["pnl"].sum()
    daily_pnl.update(grouped)

    # Compute annualized Sharpe
    if len(daily_pnl) < 2:
        return np.nan

    mu = daily_pnl.mean()
    sigma = daily_pnl.std(ddof=1)

    if sigma == 0:
        return np.nan

    return mu / sigma * np.sqrt(252)


def null_band(shards_R: Dict[int, pd.DataFrame]) -> pd.DataFrame:
    """Compute null band from 64 random-sign shards (Arm R).

    Args:
        shards_R: dict[seed -> DataFrame] with 64 shards

    Returns:
        DataFrame with columns: seed, sharpe_event, sharpe_annualized, n_traded
        One row per seed.
    """
    rows = []

    for seed in sorted(shards_R.keys()):
        df = shards_R[seed]
        ok_df = df[df["status"] == "ok"].copy()

        n_traded = len(ok_df)

        if n_traded == 0:
            sharpe_ev = np.nan
            sharpe_ann = np.nan
        else:
            sharpe_ev = sharpe_per_event(ok_df["pnl"].values)
            sharpe_ann = sharpe_annualized(ok_df)

        rows.append({
            "seed": seed,
            "sharpe_event": sharpe_ev,
            "sharpe_annualized": sharpe_ann,
            "n_traded": n_traded,
        })

    return pd.DataFrame(rows)


def placement(model_df: pd.DataFrame, null: pd.DataFrame) -> dict:
    """Compute placement for one model against the null band.

    Args:
        model_df: DataFrame with OK rows for one model
        null: null_band DataFrame (64 rows)

    Returns:
        dict with keys:
            - sharpe_event: model's per-event Sharpe
            - sharpe_annualized: model's annualized Sharpe
            - p_event: empirical p-value vs null (event Sharpe)
            - p_annualized: empirical p-value vs null (annualized Sharpe)
            - n_traded: count of traded events
            - n_neutral: count of skip_reason=='neutral_score'
            - n_noscore: count of skip_reason=='no_score'
            - hit_rate: fraction of positive PnL among traded
    """
    ok_df = model_df[model_df["status"] == "ok"].copy()

    n_traded = len(ok_df)
    n_neutral = len(model_df[model_df["skip_reason"] == "neutral_score"])
    n_noscore = len(model_df[model_df["skip_reason"] == "no_score"])

    if n_traded == 0:
        return {
            "sharpe_event": np.nan,
            "sharpe_annualized": np.nan,
            "p_event": np.nan,
            "p_annualized": np.nan,
            "n_traded": 0,
            "n_neutral": n_neutral,
            "n_noscore": n_noscore,
            "hit_rate": np.nan,
        }

    model_sharpe_ev = sharpe_per_event(ok_df["pnl"].values)
    model_sharpe_ann = sharpe_annualized(ok_df)
    hit_rate = (ok_df["pnl"] > 0).sum() / n_traded

    # Compute empirical p-values
    # p = (1 + count(null_sharpe >= model_sharpe)) / (1 + n_null)
    null_sharpes_ev = null["sharpe_event"].dropna().values
    null_sharpes_ann = null["sharpe_annualized"].dropna().values

    n_null_ev = len(null_sharpes_ev)
    n_null_ann = len(null_sharpes_ann)

    if np.isnan(model_sharpe_ev) or n_null_ev == 0:
        p_event = np.nan
    else:
        p_event = (1 + (null_sharpes_ev >= model_sharpe_ev).sum()) / (1 + n_null_ev)

    if np.isnan(model_sharpe_ann) or n_null_ann == 0:
        p_annualized = np.nan
    else:
        p_annualized = (1 + (null_sharpes_ann >= model_sharpe_ann).sum()) / (1 + n_null_ann)

    return {
        "sharpe_event": model_sharpe_ev,
        "sharpe_annualized": model_sharpe_ann,
        "p_event": p_event,
        "p_annualized": p_annualized,
        "n_traded": n_traded,
        "n_neutral": n_neutral,
        "n_noscore": n_noscore,
        "hit_rate": hit_rate,
    }


def always_long_frame(shard_r: pd.DataFrame) -> pd.DataFrame:
    """STANDING BENCHMARK 2: the long-reporter/short-peers portfolio.

    Long the reporter, short its ridge-fitted beta-neutral peer basket,
    entry 9:30 open / exit RTH close, for EVERY data-viable event — i.e.
    the study's hedged structure with the direction decision deleted.
    Together with the 64-seed random null (benchmark 1) it brackets every
    result: the null answers "better than luck?", this answers "better
    than the strategy's own long-reporter bias?". Both are MANDATORY in
    every placement, chart, and paired test.

    Recovered from one Arm R shard: rows store
    pnl = direction * hedged_long_return, so pnl * direction backs the
    sign out, independent of which coin flip the seed made.

    Returns: DataFrame with trade_day, pnl (hedged long) for OK rows only.
    """
    ok = shard_r[shard_r["status"] == "ok"]
    return pd.DataFrame({
        "trade_day": ok["trade_day"].values,
        "pnl": (ok["pnl"] * ok["direction"]).astype(float).values,
    })


def render_placement_chart(null: pd.DataFrame, placements: pd.DataFrame,
                          out_png: Path,
                          always_long_sharpe: float = None) -> None:
    """Render horizontal placement chart: null band + model points.

    Args:
        null: null_band DataFrame
        placements: model_placement DataFrame (sorted by sharpe_annualized desc)
        out_png: output PNG path
    """
    fig, ax = plt.subplots(figsize=(12, 8))

    # Extract annualized sharpes
    null_sharpes = null["sharpe_annualized"].dropna().values

    # Violin plot for null band (horizontal orientation)
    parts = ax.violinplot([null_sharpes], positions=[0], orientation="horizontal",
                          widths=0.7, showmeans=True, showextrema=True)

    # Style the violin
    for pc in parts["bodies"]:
        pc.set_facecolor("#cccccc")
        pc.set_alpha(0.5)

    # Plot model points
    model_names = placements["model_name"].values
    model_sharpes = placements["sharpe_annualized"].values

    # Position models vertically with spacing
    y_positions = np.arange(1, len(model_names) + 1)

    colors = plt.cm.viridis(np.linspace(0, 1, len(model_names)))

    ax.scatter(model_sharpes, y_positions, c=colors, s=100, alpha=0.7, zorder=10)

    # Label top 5 models
    for i in range(min(5, len(model_names))):
        # Truncate model name if too long
        label = model_names[i]
        if len(label) > 40:
            label = label[:37] + "..."
        ax.text(model_sharpes[i], y_positions[i], f"  {label}",
               va="center", ha="left", fontsize=8)

    ax.axhline(0, color="black", linestyle="--", linewidth=0.5, alpha=0.5)
    ax.axvline(0, color="black", linestyle="-", linewidth=1, alpha=0.7)

    null_median = float(np.median(null_sharpes))
    ax.axvline(null_median, color="#52514e", linestyle="--", linewidth=1.2,
               alpha=0.9, zorder=5)
    ax.text(null_median, len(model_names) + 0.6,
            f" random median {null_median:+.2f}", color="#52514e",
            fontsize=9, ha="left", va="top")

    if always_long_sharpe is not None and not np.isnan(always_long_sharpe):
        ax.axvline(always_long_sharpe, color="#eb6834", linestyle="-.",
                   linewidth=1.2, alpha=0.9, zorder=5)
        ax.text(always_long_sharpe, len(model_names) - 1.4,
                f" long-rep/short-peers {always_long_sharpe:+.2f}", color="#eb6834",
                fontsize=9, ha="left", va="top")

    ax.set_xlabel("Annualized Sharpe", fontsize=12)
    ax.set_ylabel("Models (sorted by Sharpe)", fontsize=12)
    ax.set_title("Model Placement vs. Null Band (64 Random Signs)", fontsize=14)

    # Set y-axis limits to include null band at y=0 and all models
    ax.set_ylim(-1, len(model_names) + 1)

    plt.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)


def _load_arm_r_shards(s3io, bucket: str, prefix: str) -> Dict[int, pd.DataFrame]:
    """Load all Arm R shards (seed=NN.parquet)."""
    import boto3
    session = boto3.Session(profile_name=s3io.pl_opts.get("aws_profile"))
    s3 = session.client("s3", region_name=s3io.pl_opts["aws_region"])

    shards = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            m = re.search(r"seed=(\d+)\.parquet$", key)
            if m:
                seed = int(m.group(1))
                uri = f"s3://{bucket}/{key}"
                df = pd.read_parquet(io.BytesIO(s3io.read_bytes(uri)))
                shards[seed] = df

    return shards


def _load_arm_s_shards(s3io, bucket: str, prefix: str) -> List[pd.DataFrame]:
    """Load all Arm S shards (model=<name>.parquet)."""
    import boto3
    session = boto3.Session(profile_name=s3io.pl_opts.get("aws_profile"))
    s3 = session.client("s3", region_name=s3io.pl_opts["aws_region"])

    shards = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet") and "model=" in key:
                uri = f"s3://{bucket}/{key}"
                df = pd.read_parquet(io.BytesIO(s3io.read_bytes(uri)))
                shards.append(df)

    return shards


def cmd_aggregate(args) -> int:
    """Main CLI: load shards, compute null + placements, write outputs."""
    from s3io import S3IO

    s3io = S3IO(profile=args.profile)
    bucket = settings.get("s3", "data_bucket")
    base_prefix = study.current().results_prefix

    # Load shards
    print("Loading Arm R shards (64 random seeds)...")
    r_prefix = f"{base_prefix}/shards/arm=R/"
    shards_r = _load_arm_r_shards(s3io, bucket, r_prefix)
    print(f"Loaded {len(shards_r)} Arm R shards")

    print("Loading Arm S shards (47 sentiment models)...")
    s_prefix = f"{base_prefix}/shards/arm=S/"
    shards_s = _load_arm_s_shards(s3io, bucket, s_prefix)
    print(f"Loaded {len(shards_s)} Arm S shards")

    # Compute null band
    print("\nComputing null band...")
    null_df = null_band(shards_r)

    # Compute model placements
    print("Computing model placements...")
    placement_rows = []
    for shard in shards_s:
        model_name = shard["sentiment_model"].iloc[0] if "sentiment_model" in shard.columns else "unknown"
        p = placement(shard, null_df)
        p["model_name"] = model_name
        placement_rows.append(p)

    placements_df = pd.DataFrame(placement_rows)

    # Sort by annualized Sharpe (descending)
    placements_df = placements_df.sort_values("sharpe_annualized", ascending=False)

    # Prepare output directory
    out_dir = Path(__file__).parent / "results" / "aggregate"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Write local outputs
    null_csv = out_dir / "null_band.csv"
    placement_csv = out_dir / "model_placement.csv"
    chart_png = out_dir / "placement_chart.png"

    # STANDING BENCHMARK 2: long-reporter/short-peers (recovered from any
    # R shard; seed 0 by convention). Placed against the null like any model
    # and written into the placement CSV so it appears everywhere downstream.
    al = always_long_frame(shards_r[min(shards_r.keys())])
    al_sharpe = sharpe_annualized(al)
    null_ann = null_df["sharpe_annualized"].dropna().values
    al_p = (1 + (null_ann >= al_sharpe).sum()) / (1 + len(null_ann))
    al_pnl = al["pnl"].values
    bench_row = {
        "sharpe_event": sharpe_per_event(al_pnl),
        "sharpe_annualized": al_sharpe,
        "p_event": np.nan, "p_annualized": al_p,
        "n_traded": len(al), "n_neutral": 0, "n_noscore": 0,
        "hit_rate": float((al_pnl > 0).mean()),
        "model_name": "BENCHMARK:long-reporter-short-peers",
    }
    placements_df = pd.concat([placements_df, pd.DataFrame([bench_row])],
                              ignore_index=True)
    print(f"Benchmark 2 (long-reporter/short-peers): annualized Sharpe "
          f"{al_sharpe:+.4f}, p={al_p:.3f} vs null ({len(al)} events)")

    print(f"\nWriting outputs to {out_dir}/")
    null_df.to_csv(null_csv, index=False)
    placements_df.to_csv(placement_csv, index=False)
    render_placement_chart(null_df, placements_df, chart_png,
                           always_long_sharpe=al_sharpe)

    # Upload to S3
    s3_agg_prefix = f"{base_prefix}/aggregate"
    print(f"\nUploading to s3://{bucket}/{s3_agg_prefix}/")

    s3io.write_text(null_df.to_csv(index=False),
                   f"s3://{bucket}/{s3_agg_prefix}/null_band.csv")
    s3io.write_text(placements_df.to_csv(index=False),
                   f"s3://{bucket}/{s3_agg_prefix}/model_placement.csv")
    s3io.write_bytes(chart_png.read_bytes(),
                    f"s3://{bucket}/{s3_agg_prefix}/placement_chart.png")

    # Print summary
    print("\n" + "=" * 70)
    print("NULL BAND SUMMARY (annualized Sharpe)")
    print("=" * 70)
    null_ann = null_df["sharpe_annualized"].dropna()
    print(f"  min:    {null_ann.min():.4f}")
    print(f"  p5:     {null_ann.quantile(0.05):.4f}")
    print(f"  median: {null_ann.median():.4f}")
    print(f"  p95:    {null_ann.quantile(0.95):.4f}")
    print(f"  max:    {null_ann.max():.4f}")

    print("\n" + "=" * 70)
    print("TOP 5 MODELS")
    print("=" * 70)
    for i, row in placements_df.head(5).iterrows():
        print(f"\n{row['model_name']}")
        print(f"  Sharpe (annualized): {row['sharpe_annualized']:.4f}")
        print(f"  p-value (ann):       {row['p_annualized']:.4f}")
        print(f"  p-value (event):     {row['p_event']:.4f}")
        print(f"  Traded:              {row['n_traded']}")
        print(f"  Hit rate:            {row['hit_rate']:.3f}")

    print("\n" + "=" * 70)
    print("CAVEAT: lookahead bias accepted (models trained on 2016-2024")
    print("        outcomes; spec §8).")
    print("=" * 70)

    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--profile", default=None,
                    help="AWS profile name (default: standard credential chain)")
    args = ap.parse_args()

    return cmd_aggregate(args)


if __name__ == "__main__":
    sys.exit(main())
