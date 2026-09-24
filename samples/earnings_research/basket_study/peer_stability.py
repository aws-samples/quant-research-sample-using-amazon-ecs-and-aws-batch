"""peer_stability — are a reporter's peers the same from event to event?

Scope: EX-LARGE reporters (liquidity tiers micro/small/mid at event time).
Peer set per event and universe = peers_theo of the equal_weight_dollar_neutral
row (the universe BEFORE any data-driven drops; equal weight has the highest
ok rate so it carries the most events). Rows without peers_theo are ignored.

For each reporter x universe, events in date order; for every consecutive
pair: Jaccard = |A n B| / |A u B|, identical = (A == B). Per reporter:
n_events, n_pairs, mean_jaccard, pct_identical, mean_set_size, n_distinct_sets,
core_share = |peers in EVERY event| / |union|.

Outputs (S3 only, under <s3-prefix>/diagnostics/peer_stability/):
  per_reporter.parquet   reporter x universe rows (above)
  per_pair.parquet       every consecutive pair: reporter, universe, date_a,
                         date_b, gap_days, size_a, size_b, jaccard, identical
  summary.csv            per universe: reporters, pairs, mean/median jaccard,
                         pct identical pairs, pct reporters with all pairs
                         identical, mean set size
  peer_stability.png     (a) ECDF of pair Jaccard per universe, (b) mean pair
                         Jaccard by year of the later event, (c) pct identical
                         pairs by gap between events

    AWS_PROFILE=<profile> python peer_stability.py [--write]
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
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
import aggregate  # noqa: E402
import settings
from s3io import S3IO  # noqa: E402

UNIVERSES = ["all", "correlated", "functional", "pure play"]
COLORS = {"all": "#2a78d6", "correlated": "#eb6834", "functional": "#1baf7a", "pure play": "#9b2fae"}
TIERS = ["micro", "small", "mid"]


def load(s3io, prefix):
    df = pl.read_parquet(f"s3://{settings.get("s3", "data_bucket")}/{prefix}/shards_consolidated/shards.parquet",
                         storage_options=s3io.pl_opts)
    rows = (df.filter(pl.col("liquidity_tier").is_in(TIERS)
                      & (pl.col("construction") == "equal_weight_dollar_neutral")
                      & pl.col("peers_theo").is_not_null())
            .select("event_id", "event_symbol", "event_date", "universe", "peers_theo", "liquidity_tier")
            .unique(["event_id", "universe"]).sort(["event_symbol", "universe", "event_date"]))
    return rows.to_pandas()


def pairs(rows: pd.DataFrame) -> pd.DataFrame:
    out = []
    for (sym, uni), g in rows.groupby(["event_symbol", "universe"], sort=False):
        sets = [frozenset(p.split()) for p in g["peers_theo"]]
        dates = g["event_date"].tolist()
        for i in range(1, len(sets)):
            a, b = sets[i - 1], sets[i]
            u = len(a | b)
            out.append({"reporter": sym, "universe": uni, "date_a": dates[i - 1], "date_b": dates[i],
                        "gap_days": (pd.Timestamp(dates[i]) - pd.Timestamp(dates[i - 1])).days,
                        "size_a": len(a), "size_b": len(b),
                        "jaccard": len(a & b) / u if u else 1.0, "identical": a == b})
    return pd.DataFrame(out)


def per_reporter(rows: pd.DataFrame, pr: pd.DataFrame) -> pd.DataFrame:
    out = []
    pg = pr.groupby(["reporter", "universe"])
    for (sym, uni), g in rows.groupby(["event_symbol", "universe"], sort=False):
        sets = [frozenset(p.split()) for p in g["peers_theo"]]
        union = frozenset().union(*sets)
        core = frozenset.intersection(*sets) if sets else frozenset()
        rec = {"reporter": sym, "universe": uni, "n_events": len(sets), "n_pairs": max(len(sets) - 1, 0),
               "mean_set_size": float(np.mean([len(s) for s in sets])), "n_distinct_sets": len(set(sets)),
               "union_size": len(union), "core_size": len(core),
               "core_share": len(core) / len(union) if union else np.nan,
               "first_event": g["event_date"].iloc[0], "last_event": g["event_date"].iloc[-1]}
        if (sym, uni) in pg.groups:
            p = pg.get_group((sym, uni))
            rec["mean_jaccard"] = float(p["jaccard"].mean())
            rec["pct_identical"] = float(p["identical"].mean()) * 100
        else:
            rec["mean_jaccard"] = rec["pct_identical"] = np.nan
        out.append(rec)
    return pd.DataFrame(out)


def summary(rep: pd.DataFrame, pr: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for uni in UNIVERSES:
        r = rep[(rep.universe == uni) & (rep.n_pairs > 0)]
        p = pr[pr.universe == uni]
        rows.append({"universe": uni, "reporters": int(rep[rep.universe == uni].shape[0]),
                     "reporters_with_pairs": int(len(r)), "pairs": int(len(p)),
                     "mean_set_size": float(rep[rep.universe == uni]["mean_set_size"].mean()),
                     "mean_jaccard": float(p["jaccard"].mean()), "median_jaccard": float(p["jaccard"].median()),
                     "pct_pairs_identical": float(p["identical"].mean()) * 100,
                     "pct_pairs_jaccard_lt_50": float((p["jaccard"] < .5).mean()) * 100,
                     "pct_reporters_always_identical": float((r["pct_identical"] == 100).mean()) * 100,
                     "mean_core_share": float(r["core_share"].mean()),
                     "mean_distinct_sets_per_reporter": float(r["n_distinct_sets"].mean())})
    return pd.DataFrame(rows)


def figure(pr: pd.DataFrame) -> bytes:
    fig, axes = plt.subplots(1, 3, figsize=(20, 6.2))
    ax = axes[0]
    for uni in UNIVERSES:
        j = np.sort(pr.loc[pr.universe == uni, "jaccard"].to_numpy())
        ax.step(j, np.arange(1, len(j) + 1) / len(j), where="post", color=COLORS[uni], lw=2,
                label=f"{uni} (n={len(j):,})")
    ax.set_xlabel("Jaccard similarity of peer sets, consecutive events of the same reporter")
    ax.set_ylabel("share of pairs ≤ x"); ax.set_title("How different are peers from one event to the next?", loc="left")
    ax.grid(alpha=.3); ax.legend(loc="upper left"); ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    ax = axes[1]
    pr = pr.assign(year=pr["date_b"].str[:4])
    for uni in UNIVERSES:
        g = pr[pr.universe == uni].groupby("year")["jaccard"].mean()
        ax.plot(g.index, g.values, marker="o", color=COLORS[uni], lw=2, label=uni)
    ax.set_ylim(0, 1); ax.set_ylabel("mean pair Jaccard"); ax.set_title("Stability over time (year of the later event)", loc="left")
    ax.grid(alpha=.3); ax.legend(); ax.tick_params(axis="x", rotation=45)
    ax = axes[2]
    bins = [0, 45, 135, 200, 400, 10000]; labels = ["<45d", "45–135d\n(one quarter)", "135–200d", "200–400d", ">400d"]
    pr = pr.assign(gap=pd.cut(pr["gap_days"], bins=bins, labels=labels, right=False))
    w = 0.2
    for i, uni in enumerate(UNIVERSES):
        g = pr[pr.universe == uni].groupby("gap", observed=False)["identical"].mean() * 100
        ax.bar(np.arange(len(labels)) + (i - 1.5) * w, g.reindex(labels).values, width=w, color=COLORS[uni], label=uni)
    ax.set_xticks(range(len(labels))); ax.set_xticklabels(labels)
    ax.set_ylabel("% of consecutive pairs with IDENTICAL peers"); ax.set_title("Identical peer sets by gap between the two events", loc="left")
    ax.grid(alpha=.3, axis="y"); ax.legend()
    fig.suptitle("EX-LARGE reporters — peer-set stability event to event (theoretical peers before data drops, "
                 "equal-weight rows)", fontsize=13)
    fig.tight_layout()
    buf = io.BytesIO(); fig.savefig(buf, dpi=110, format="png"); plt.close(fig)
    return buf.getvalue()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--s3-prefix", default="earnings-basket-study/results-10y")
    ap.add_argument("--profile", default=None)
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    s3io = S3IO(profile=args.profile)
    rows = load(s3io, args.s3_prefix)
    print(f"{len(rows)} event x universe rows, {rows['event_symbol'].nunique()} reporters", flush=True)
    pr = pairs(rows)
    rep = per_reporter(rows, pr)
    sm = summary(rep, pr)
    pd.set_option("display.width", 200)
    print(sm.round(3).to_string(index=False))
    if args.write:
        base = f"s3://{settings.get("s3", "data_bucket")}/{args.s3_prefix}/diagnostics/peer_stability"
        s3io.write_parquet(pl.from_pandas(rep), f"{base}/per_reporter.parquet")
        s3io.write_parquet(pl.from_pandas(pr), f"{base}/per_pair.parquet")
        s3io.write_csv(pl.from_pandas(sm), f"{base}/summary.csv")
        s3io.write_bytes(figure(pr), f"{base}/peer_stability.png")
        print(f"wrote {base}/")


if __name__ == "__main__":
    main()
