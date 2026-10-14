"""MA crossover basket study — report (PDF + HTML twin) of ONE run: arm distributions, top rules, fast x slow Sharpe grids, split-half, per-coin contributions,
daily PnL of the arm winners, BTC-alone / ETH-alone / agreement. Laptop only (renders what the Batch pnl job wrote). All numbers gross.
  python ma_cross_report.py [--run-id <id>] [--out <pdf path>]      (defaults: runs.default and paths.viewer_out of config.json)"""
import argparse, html, os, subprocess, sys
from datetime import datetime
from zoneinfo import ZoneInfo
import polars as pl
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as tv
import report_common as trp
import report_common as tcr
import report_common as pr
import ma_cross as mx
import ma_cross_core as mc

import settings
ET = ZoneInfo("America/New_York")
def viewer_dir(): return os.path.expanduser(settings.get("paths", "viewer_out"))
ARM_LABEL = {"imb_usd": "imbalance MA, $100k dollar bars", "imb_sec": "imbalance MA, 1 s time bars", "px_usd": "price MA, $100k dollar bars", "px_sec": "price MA, 1 s time bars"}
TYP_LABEL = {"timb": "trade imbalance", "bimb": "book imbalance", "px": "vwap"}
RULE_COLS = [("rule", "rule"), ("total_bp", "total bp"), ("sharpe_ann", "Sharpe (ann.)"), ("pos_day_share", "days > 0"), ("flips_per_day", "flips / day"), ("median_hold_s", "median hold s"),
             ("long_share", "long share"), ("hit_rate", "hit rate"), ("mean_trade_bp", "mean bp / trade"), ("median_trade_bp", "median bp / trade"), ("breakeven_bp_per_flip", "break-even bp / flip"), ("max_drawdown_bp", "max DD bp"), ("p_value", "p (shift null)"), ("null_p95_bp", "null p95 |total| bp")]


def load(run_id):
    root = f"{tv.out_root()}/{run_id}/macx"; f = tv.fs()
    def rd(name): k = tv._key(f"{root}/summary/{name}.parquet"); return pl.read_parquet(f"s3://{k}", storage_options=tv.so()) if f.exists(k) else None
    T = {k: rd(k) for k in ("rules", "arms", "split", "coins", "daily", "baseline", "lagprofile", "hod")}
    return T, tv.read_json(f"{root}/plan.json"), tv.read_json(f"{root}/summary/_done.json")


def _fmt(key):
    """Cell formatter: bp and Sharpe to 2 decimals, shares / p-values to 3, flips per day to 1, ints as is, None as a dash."""
    dec = 3 if key in ("pos_day_share", "long_share", "hit_rate", "p_value", "share_positive", "share_p05", "mean_trade_bp", "median_trade_bp") else 1 if "flips" in key or key == "median_hold_s" else 2
    def f(v):
        if v is None: return "–"
        if isinstance(v, float): return f"{v:.{dec}f}"
        return html.escape(str(v))
    return f


def tbl(num, scope, measure, selection, sort, X, cols, n=None, note="", order="desc"):
    if X is None or not X.height: return f"{pr.title(num, scope, measure, selection, sort, 'no rows', order=order)}<p class='note'>no rows</p>"
    R = X.head(n) if n else X; cols = [c if len(c) > 2 else (c[0], c[1], _fmt(c[0])) for c in cols]
    return (f"<div class='fig'>{pr.title(num, scope, measure, selection, sort, f'{R.height} of {X.height} rows' if n and X.height > n else f'all {X.height} rows', order=order)}" + (f"<p class='note'>{note}</p>" if note else "") + pr.table_html(R.to_dicts(), cols) + "</div>")


def both_exec(R, lag=1): return R.filter((pl.col("combo") == "both") & (pl.col("lag_s") == lag))


MARK_LABEL = {"pooled": "alt basket, pooled ALL mid (every venue, quotes up to 10 s old; unusable at 1 s)", "primary": "alt basket, primary-venue mid, forward-filled (the readable mark; still stale on thin coins)",
              "fresh": "alt basket, primary-venue mid, fresh seconds only (biased long: discard)", "btc": "BTC perp itself, primary-venue mid", "eth": "ETH perp itself, primary-venue mid"}
MARK_ORDER = ["pooled", "primary", "fresh", "btc", "eth"]


def marks_of(F): return [m for m in MARK_ORDER if F is not None and "mark" in F.columns and m in set(F["mark"].to_list())] or ([None] if F is not None and F.height else [])


def by_mark(F, mark): return F if mark is None or F is None or "mark" not in F.columns else F.filter(pl.col("mark") == mark)


def grid_table(R, arm, typ):
    """fast (rows) x slow (columns) annualised Sharpe of one arm x type, agreement rule at the executable lag."""
    X = both_exec(R).filter((pl.col("arm") == arm) & (pl.col("typ") == typ)).select("fast", "slow", "sharpe_ann")
    if not X.height: return None, []
    P = X.pivot(index="fast", on="slow", values="sharpe_ann", sort_columns=True).sort("fast"); slows = [c for c in P.columns if c != "fast"]
    P = P.select(["fast"] + sorted(slows, key=int)); return P, [("fast", "fast \\ slow")] + [(c, c) for c in P.columns if c != "fast"]


def reading(T):
    R, A, Bs = T["rules"], T["arms"], T["baseline"]; parts = []; f = lambda v, d=2: "–" if v is None else f"{v:.{d}f}"
    for mark in marks_of(A):
        lab = MARK_LABEL.get(mark, "basket"); parts.append(f"<b>{lab}.</b>")
        B1 = by_mark(Bs, mark)
        if B1 is not None and B1.height:
            b = B1.filter(pl.col("name") == "always_long").row(0, named=True)
            parts.append(f"Always-long basket: total {f(b['total_bp'])} bp over {b['n_days']} days, Sharpe (ann.) {f(b['sharpe_ann'])}, days &gt; 0 {f(b['pos_day_share'], 3)}; mean half-spread of the basket {f(b.get('half_spread_bp'))} bp (the cost of one flip at the touch, not charged here).")
        X = by_mark(A, mark).filter((pl.col("combo") == "both") & (pl.col("lag_s") == 1)).sort("median_sharpe_ann", descending=True, nulls_last=True)
        for row in X.to_dicts():
            best = by_mark(R, mark).filter((pl.col("rule") == row["best_rule"]) & (pl.col("combo") == "both") & (pl.col("lag_s") == 1)).row(0, named=True)
            parts.append(f"{ARM_LABEL.get(row['arm'], row['arm'])}, {TYP_LABEL.get(row['typ'], row['typ'])}: median Sharpe {f(row['median_sharpe_ann'])} over {row['n_rules']} pairs, best {f(row['best_sharpe_ann'])} ({html.escape(row['best_rule'])}: "
                         f"{f(best['total_bp'], 0)} bp, {f(best['flips_per_day'], 0)} flips / day, break-even {f(best['breakeven_bp_per_flip'])} bp / flip), {row['share_positive']:.0%} of pairs positive, {row['share_p05']:.0%} with p &lt; 0.05 against the shift null.")
    return "<h2>Reading</h2><p class='note'>Agreement rule (basket follows BTC and ETH when their crossover states agree, holds otherwise), executable lag 1 s, gross, marked at mid. Sharpe = daily mean / daily sd × √365. " \
           "The shift null moves the whole position series by a random number of seconds, keeping its flip structure and breaking only the alignment with the returns. A forward-filled mid is stale: the pooled ALL mid averages every venue's quote up to 10 s old, and even on a coin's primary venue " \
           "the 1 s bars exist only in seconds with a trade (220 of 298 coins trade in under 30 % of seconds), so the carried mid catches up to BTC seconds later and fakes a lead-lag. The fresh mark counts a coin's return only between two consecutive seconds that both have a bar; it is the number to read.<br>" + "<br>".join(parts) + "</p>"


def render(T, plan, done, run_id, n):
    R = T["rules"]; parts = [f"<h1>MA crossover on pooled BTC/ETH flow → long/short basket — run {html.escape(run_id)}</h1>",
                             f"<p>Signal: BTC perp and ETH perp pooled across every venue and quote (the dataset's ALL series). Four arms: trade / book imbalance MAs on $100k dollar bars and on 1 s time bars (the rule), vwap MAs on the same two clocks (the benchmark); "
                             f"the {plan['pairs']} fast &lt; slow pairs of the windows {plan['windows']} bars = {plan['rules']} rules. State = sign(fast − slow) at each bar close (ties hold), sampled every second from the last closed bar. "
                             f"Basket: {len(plan['targets'])} coins (no BTC / ETH / tokenised equities), equal weight, 1 s mid returns on two marks (pooled ALL mid; each coin's primary-venue mid), direction = the common BTC/ETH sign when they agree, held otherwise; lags 0 s (upper bound) and 1 s (executable). "
                             f"{plan['dates'][0]} → {plan['dates'][-1]} ({len(plan['dates'])} days, {done.get('n_secs', 0):,} seconds); finished {done.get('finished_at')}. Rendered {datetime.now(ET).strftime('%Y-%m-%d %H:%M ET')}. Gross, no costs.</p>",
                             reading(T),
                             "<h2>Contents</h2><p>1 setup + reading (above), 2 arm distributions, 3 top / worst rules, 4 fast × slow Sharpe grids, 5 split-half check, 6 per-coin contributions, 7 daily PnL of the arm winners, 8 BTC alone / ETH alone / agreement, lag 0 vs 1, lag profiles, 9 hour of day (US Eastern), 10 reproduction. Each section runs over the targets: alt basket on three marks (pooled, primary, fresh), then BTC itself, then ETH itself.</p>"]
    A = T["arms"]; acols = [("arm", "arm"), ("typ", "type"), ("median_sharpe_ann", "median Sharpe"), ("best_sharpe_ann", "best Sharpe"), ("best_rule", "best rule"), ("median_total_bp", "median total bp"), ("share_positive", "pairs > 0"), ("share_p05", "pairs p < 0.05"), ("median_flips_per_day", "median flips / day"), ("n_rules", "pairs")]
    k = 0
    for mark in marks_of(A):
        for lag in mc.LAGS_S:
            k += 1; parts.append(tbl(f"2.{k}", f"{MARK_LABEL.get(mark, 'basket')}, agreement rule, lag {lag} s", "distribution over the window pairs per arm x type", "every arm x type", "median Sharpe", by_mark(A, mark).filter((pl.col("combo") == "both") & (pl.col("lag_s") == lag)).sort("median_sharpe_ann", descending=True, nulls_last=True), acols))
    k = 0
    for mark in marks_of(R):
        X = both_exec(by_mark(R, mark)).sort("sharpe_ann", descending=True, nulls_last=True); lab = MARK_LABEL.get(mark, "basket")
        k += 1; parts.append(tbl(f"3.{k}", f"{lab}, agreement rule, lag 1 s", "best rules by annualised Sharpe", f"top {min(n, X.height)} of {X.height}", "Sharpe", X, [("arm", "arm"), ("typ", "type")] + RULE_COLS, n))
        k += 1; parts.append(tbl(f"3.{k}", f"{lab}, agreement rule, lag 1 s", "worst rules by annualised Sharpe", "bottom 15", "Sharpe", X.sort("sharpe_ann", nulls_last=True), [("arm", "arm"), ("typ", "type")] + RULE_COLS, 15, order="asc"))
    k = 0
    for mark in marks_of(R):
        for arm, typs in mc.ARMS.items():
            for typ in typs:
                k += 1; P, cols = grid_table(by_mark(R, mark), arm, typ)
                parts.append(tbl(f"4.{k}", f"{MARK_LABEL.get(mark, 'basket')}: {ARM_LABEL[arm]}, {TYP_LABEL[typ]}", "annualised Sharpe, agreement rule, lag 1 s", "fast (rows) x slow (columns)", "fast", P, cols, order="asc"))
    Sp = T["split"]; k = 0
    for mark in marks_of(Sp):
        S1 = by_mark(Sp, mark)
        if not S1.height or "selected" not in S1.columns: continue
        W = S1.filter(pl.col("selected")).pivot(index=["arm", "typ", "rule"], on="half", values=["sharpe_ann", "total_bp", "flips_per_day"]).sort("arm", "typ")
        cols = [("arm", "arm"), ("typ", "type"), ("rule", "rule chosen on half 1")] + [(c, c.replace("_", " ").replace("half ", "h")) for c in W.columns if c not in ("arm", "typ", "rule")]
        k += 1; parts.append(tbl(f"5.{k}", f"{MARK_LABEL.get(mark, 'basket')}, agreement rule, lag 1 s", "winner of each arm x type on the first half, scored on the second", "best Sharpe on half 1", "arm", W, cols, order="asc",
                                 note=f"half 1 = the first {S1['days'].min() if 'days' in S1.columns else ''} days, half 2 = the rest; the half-2 Sharpe is the honest number, the half-1 Sharpe is a selection maximum over 91 pairs."))
        k += 1; parts.append(tbl(f"5.{k}", f"{MARK_LABEL.get(mark, 'basket')}, agreement rule, lag 1 s", "half-2 Sharpe of EVERY rule vs its half-1 Sharpe", "all rules, pivot", "half-2 Sharpe", S1.pivot(index=["arm", "typ", "rule"], on="half", values="sharpe_ann").rename({"1": "sharpe_h1", "2": "sharpe_h2"}).sort("sharpe_h2", descending=True, nulls_last=True),
                                 [("arm", "arm"), ("typ", "type"), ("rule", "rule"), ("sharpe_h1", "Sharpe half 1"), ("sharpe_h2", "Sharpe half 2")], 30))
    Cn = T["coins"]; k = 0
    for mark in marks_of(Cn):
        C1 = by_mark(Cn, mark)
        for rule in C1["rule"].unique(maintain_order=True).to_list():
            k += 1; X = C1.filter(pl.col("rule") == rule).sort("contrib_bp", descending=True); top = pl.concat([X.head(10), X.tail(10)]).unique(subset=["coin"], maintain_order=True)
            parts.append(tbl(f"6.{k}", f"{MARK_LABEL.get(mark, 'basket')}: best rule {html.escape(rule)}", "per-coin contribution to the basket PnL (bp of basket) vs the coin's always-long contribution", "top 10 and bottom 10 coins", "contribution", top, [("coin", "coin"), ("contrib_bp", "contribution bp"), ("always_long_bp", "always-long bp"), ("secs_with_value", "seconds with a mid")]))
    D = T["daily"]; k = 0
    for mark in marks_of(D):
        C1 = by_mark(Cn, mark) if Cn is not None else None
        if C1 is None or not C1.height: continue
        winners = C1["rule"].unique(maintain_order=True).to_list(); X = by_mark(D, mark).filter(((pl.col("combo") == "both") & (pl.col("lag_s") == 1) & pl.col("rule").is_in(winners)) | (pl.col("rule") == "always_long"))
        P = X.pivot(index="date", on="rule", values="pnl_bp").sort("date"); cols = [("date", "date")] + [(c, c) for c in P.columns if c != "date"]
        k += 1; parts.append(tbl(f"7.{k}", f"{MARK_LABEL.get(mark, 'basket')}, arm winners, agreement rule, lag 1 s", "daily PnL (bp) next to the always-long basket", "every day", "date", P, cols, order="asc"))
    k = 0
    for mark in marks_of(R):
        C1 = by_mark(Cn, mark) if Cn is not None else None
        if C1 is None or not C1.height: continue
        winners = C1["rule"].unique(maintain_order=True).to_list(); R1 = by_mark(R, mark)
        X = R1.filter(pl.col("rule").is_in(winners) & (pl.col("lag_s") == 1)).pivot(index=["arm", "typ", "rule"], on="combo", values=["sharpe_ann", "total_bp", "flips_per_day"]).sort("arm", "typ")
        k += 1; parts.append(tbl(f"8.{k}", f"{MARK_LABEL.get(mark, 'basket')}, arm winners, lag 1 s", "BTC alone / ETH alone / agreement with hold", "Sharpe, total, flips per day by combination", "arm", X, [("arm", "arm"), ("typ", "type"), ("rule", "rule")] + [(c, c.replace("_", " ")) for c in X.columns if c not in ("arm", "typ", "rule")], order="asc"))
        X0 = R1.filter(pl.col("rule").is_in(winners) & (pl.col("combo") == "both")).pivot(index=["arm", "typ", "rule"], on="lag_s", values=["sharpe_ann", "total_bp"]).sort("arm", "typ")
        k += 1; parts.append(tbl(f"8.{k}", f"{MARK_LABEL.get(mark, 'basket')}, arm winners, agreement rule", "lag 0 s (upper bound) vs lag 1 s (executable)", "Sharpe and total by lag", "arm", X0, [("arm", "arm"), ("typ", "type"), ("rule", "rule")] + [(c, c.replace("_", " ").replace("sharpe ann", "Sharpe") + " s") for c in X0.columns if c not in ("arm", "typ", "rule")], order="asc"))
        L = T.get("lagprofile")
        if L is not None and L.height:
            L1 = by_mark(L, mark).filter(pl.col("rule").is_in(winners)).pivot(index=["arm", "typ", "rule"], on="lag_s", values="total_bp", sort_columns=True).sort("arm", "typ")
            lagcols = sorted([c for c in L1.columns if c not in ("arm", "typ", "rule")], key=int); L1 = L1.select(["arm", "typ", "rule"] + lagcols)
            k += 1; parts.append(tbl(f"8.{k}", f"{MARK_LABEL.get(mark, 'basket')}, arm winners, agreement rule", "total PnL (bp) by execution lag — how fast the edge decays", "lags 0 .. 300 s", "arm", L1, [("arm", "arm"), ("typ", "type"), ("rule", "rule")] + [(c, f"lag {c} s") for c in lagcols], order="asc"))
            M1 = by_mark(L, mark).group_by("arm", "typ", "lag_s").agg(pl.col("total_bp").median().alias("median_total_bp")).pivot(index=["arm", "typ"], on="lag_s", values="median_total_bp", sort_columns=True).sort("arm", "typ")
            lagcols = sorted([c for c in M1.columns if c not in ("arm", "typ")], key=int); M1 = M1.select(["arm", "typ"] + lagcols)
            k += 1; parts.append(tbl(f"8.{k}", f"{MARK_LABEL.get(mark, 'basket')}, agreement rule", "median total PnL (bp) over the 91 pairs by execution lag", "every arm x type", "arm", M1, [("arm", "arm"), ("typ", "type")] + [(c, f"lag {c} s") for c in lagcols], order="asc"))
    Hd = T.get("hod"); k = 0
    for mark in marks_of(Hd) if Hd is not None else []:
        C1 = by_mark(Cn, mark) if Cn is not None else None
        if C1 is None or not C1.height: continue
        winners = C1["rule"].unique(maintain_order=True).to_list(); X = by_mark(Hd, mark).filter(pl.col("rule").is_in(winners) & (pl.col("combo") == "both"))
        if not X.height: continue
        P1 = X.pivot(index="hod_et", on="rule", values="mean_hour_bp").sort("hod_et"); k += 1
        parts.append(tbl(f"9.{k}", f"{MARK_LABEL.get(mark, 'basket')}, arm winners, agreement rule, lag 1 s", "mean PnL per clock hour (bp, averaged over the days) by hour of day, US Eastern", "24 hours x winners", "hour", P1, [("hod_et", "hour (ET)")] + [(c, c) for c in P1.columns if c != "hod_et"], order="asc"))
        P2 = X.pivot(index="hod_et", on="rule", values="hit_rate").sort("hod_et"); k += 1
        parts.append(tbl(f"9.{k}", f"{MARK_LABEL.get(mark, 'basket')}, arm winners, agreement rule, lag 1 s", "hit ratio of the trades opened in each hour, US Eastern", "24 hours x winners", "hour", P2, [("hod_et", "hour (ET)")] + [(c, c, _fmt("hit_rate")) for c in P2.columns if c != "hod_et"], order="asc"))
    parts.append(f"<h2>10. Reproduction</h2><p class='note'>ma_cross.py plan --run-id {html.escape(run_id)} (controller on Batch: sig → bask → pnl, the sample's Batch image, dispatch word macx); this report: ma_cross_report.py --run-id {html.escape(run_id)}. Dataset: {html.escape(plan['dataset_run'])}.</p>")
    parts.append(pr.TABLE_JS)
    return "<!doctype html><html><head><meta charset='utf-8'><title>MA crossover basket</title><style>" + tcr.CSS + "th.l{text-align:left}.fig{break-inside:avoid;page-break-inside:avoid}</style></head><body>" + "".join(parts) + "</body></html>"


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--run-id", default=None); ap.add_argument("--n", type=int, default=30); ap.add_argument("--out"); a = ap.parse_args(); a.run_id = a.run_id or settings.get("runs", "default"); a.out = a.out or os.path.join(viewer_dir(), f"imbalance_mft_{a.run_id}.pdf")
    T, plan, done = load(a.run_id); doc = render(T, plan, done, a.run_id, a.n); hp = a.out[:-4] + ".html"; os.makedirs(os.path.dirname(a.out), exist_ok=True); open(hp, "w").write(doc)
    r = subprocess.run([trp.chrome(), "--headless=new", "--disable-gpu", "--no-pdf-header-footer", "--virtual-time-budget=10000", f"--print-to-pdf={a.out}", f"file://{hp}"], capture_output=True, text=True)
    if r.returncode: print(r.stderr[-2000:], file=sys.stderr); return 1
    print("pdf:", a.out, os.path.getsize(a.out), "twin:", hp); return 0


if __name__ == "__main__":
    sys.exit(main())
