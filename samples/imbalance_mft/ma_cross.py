"""MA crossover basket study — controller. Spec: docs/superpowers/specs/2026-10-12-ma-crossover-basket-design.md.

Fan-out (everything after `plan` runs on Batch; the laptop plans, submits, monitors, renders):
  plan    laptop: basket = the flow model's target coins (every universe coin but BTC / ETH / equities, perp over spot), dates, the 546 rules -> plan.json; submits ONE
          controller job (`run`). --coins / --dates narrow it (smoke); --resubmit starts a new controller on the existing plan.
  run     controller (CPU Batch, the sample's image, dispatch word macx): stages sig -> bask -> pnl from the S3 markers, rounds with memory doubling.
  sig     one element = [coin, date] (BTC or ETH): crossover states of every rule on the 1 s grid -> sig/{coin}/{date}.parquet (t + 546 int8), sig_done/{coin}_{date}.json
  bask    one element = [date]: equal-weight basket 1 s mid returns + per-coin returns -> basket/{date}.parquet, basket_coins/{date}.parquet, bask_done/{date}.json
  pnl     one job: directions (agreement with hold), lags, metrics, circular-shift null, split-half, per-coin contributions -> summary/*.parquet + summary/_done.json
  status  laptop monitor
Output root: s3://<data_bucket>/<dataset_prefix>/<run_id>/macx/  (dataset inputs from <dataset_run>/flow/; both from config.json)."""
import argparse, json, os, sys, time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import numpy as np, polars as pl
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as tv
import common as sb
import common as tfl
import common as fm
import ma_cross_core as mc

ET = ZoneInfo("America/New_York")
import settings
DATASET_RUN = None                                 # default = settings s3.dataset_run
SOURCE_COINS = ["BTC", "ETH"]
USD_TAG = "usd100000"; BAR_USD = 100000; SEC_TAG = "sec1"
WARMUP_DAYS = 7                                   # previous days read before a signal day: a 500,000-bar window is ~1 day of BTC $100k bars and 5.8 days of 1 s bars
ROUNDS = 3                                        # stage tiers (vCPU, MiB, hours) come from settings batch.resources; memory doubles per retry round
STAGES = ("sig", "bask", "pnl")


def root_of(a): return f"{a.out}/macx"
def dataset_root(run): return f"{tv.out_root()}/{run}/flow"


def job_spec(name, cmd, vcpus, mem_mib, hours, array_size=None):
    j = tv.job_spec(name, ["macx"] + cmd, vcpus, mem_mib, hours, array_size, None)
    j["containerOverrides"]["environment"] = [{"name": "POLARS_MAX_THREADS", "value": str(vcpus)}, {"name": "OPENBLAS_NUM_THREADS", "value": "8"}, {"name": "OMP_NUM_THREADS", "value": "8"}]   # the pnl job evaluates marks in threads; unbounded BLAS pools oversubscribe
    if mem_mib > sb.mem_cap(): j["jobQueue"] = sb.big_queue()
    return j


def sig_columns(windows=None):
    if windows is None: return mc.rule_columns()
    return [f"{arm}:{typ}:{f}:{s}" for arm, typs in mc.ARMS.items() for typ in typs for f, s in mc.pairs_of(windows)]


def rule_parts(col):
    arm, typ, f, s = col.split(":"); return arm, typ, int(f), int(s)


# ----------------------------------------------------------------------------------------------- plan
def plan_doc(run_id, dataset_run, dates, T):
    return {"run_id": run_id, "dataset_run": dataset_run, "dates": dates, "targets": T.select("coin", "kind").to_dicts(), "source_coins": SOURCE_COINS, "arms": {k: list(v) for k, v in mc.ARMS.items()},
            "windows": mc.WINDOWS, "pairs": len(mc.PAIRS), "rules": len(mc.rule_columns()), "combos": mc.COMBOS, "lags_s": mc.LAGS_S, "usd_tag": USD_TAG, "sec_tag": SEC_TAG,
            "sig_elements": len(dates) * len(SOURCE_COINS), "bask_elements": len(dates), "planned_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}


def plan(a):
    root = root_of(a)
    if a.resubmit: return submit_controller(a, root)
    ds = dataset_root(a.dataset_run); U = pl.read_parquet(f"{ds}/ref/universe.parquet", storage_options=tv.so()); dp = tv.read_json(f"{ds}/plan.json")
    T = fm.target_coins(U)
    if a.coins: T = T.filter(pl.col("coin").is_in(a.coins))
    dates = a.dates or dp["dates"]; doc = plan_doc(a.run_id, a.dataset_run, dates, T)
    print(json.dumps({"targets": len(doc["targets"]), "dates": len(dates), "rules": doc["rules"], "sig_elements": doc["sig_elements"], "bask_elements": doc["bask_elements"]}), file=sys.stderr, flush=True)
    if a.dry_run: return 0
    f = tv.fs()
    if f.exists(tv._key(f"{root}/plan.json")) and not a.force: print(f"{root}/plan.json exists — --resubmit or --force", file=sys.stderr); return 2
    tv.put_parquet(T, f"{root}/ref/targets.parquet"); tv.write_json(f"{root}/plan.json", doc)
    if a.no_submit: return 0
    return submit_controller(a, root)


def submit_controller(a, root):
    f = tv.fs(); batch = tv.batch_client(); cj = f"{root}/controller_job.json"
    if f.exists(tv._key(cj)) and not a.force:
        prev = tv.read_json(cj); js = sb.describe(batch, [prev["jobId"]])
        if js and js[0]["status"] not in sb.TERMINAL: print(f"controller {prev['jobId']} is {js[0]['status']} — it resumes by itself; --force submits another", file=sys.stderr); return 2
    r = batch.submit_job(**job_spec(f"macx-{a.run_id}-controller", ["run", "--run-id", a.run_id, "--out", a.out], *sb.controller())); tv.write_json(cj, {"jobId": r["jobId"], "submitted_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")})
    print(f"controller {r['jobId']} submitted; monitor: ma_cross.py status --run-id {a.run_id} --watch", file=sys.stderr, flush=True); print(r["jobId"]); return 0


# ----------------------------------------------------------------------------------------------- sig stage
def _read(path): return tfl._read_or_none(path)


def _concat(*frames):
    fs = [f for f in frames if f is not None and f.height]; return pl.concat(fs, how="vertical_relaxed") if fs else None


def _all(F, cols, drop_partial=False):
    """The pooled ALL rows of a dataset frame, the wanted columns (those present), sorted by t_close; None when nothing."""
    if F is None: return None
    X = F.filter(pl.col("venue") == "ALL") if "venue" in F.columns else F
    if drop_partial and "partial" in X.columns: X = X.filter(~pl.col("partial"))
    X = X.select([c for c in cols if c in X.columns]).sort("t_close"); return X if X.height else None


def build_sig(frames, grid, windows=None):
    """Signal matrix of one source coin-day: t (the 1 s grid) + one Int8 column per rule — the crossover state as of each grid time. frames = arm -> frame of the
    series' bars (previous day + today, ALL rows, sorted by t_close): imbalance arms carry the dataset's {timb,bimb}_ma{N} columns, price arms carry
    vwap / close (the MAs are computed here). An arm without data is undecided (0) all day."""
    windows = windows or mc.WINDOWS; pairs = mc.pairs_of(windows); out = {"t": np.asarray(grid, dtype=np.int64)}
    for arm, typs in mc.ARMS.items():
        F = frames.get(arm)
        for typ in typs:
            if F is None or F.height == 0: St = np.zeros((len(grid), len(pairs)), dtype=np.int8)
            else:
                M = mc.price_mas(F, None, windows) if typ == "px" else mc.imb_mas(F, windows)          # long windows are not in the dataset's MA files: computed here
                St = mc.sample_states(M["t_close"].to_numpy(), mc.states(M, typ, windows), grid)
            for j, (f, s) in enumerate(pairs): out[f"{arm}:{typ}:{f}:{s}"] = pl.Series(St[:, j], dtype=pl.Int8)
    return pl.DataFrame(out)


def prev_dates(date, n):
    out = []; d = date
    for _ in range(n): d = tfl.prev_date(d); out.append(d)
    return out[::-1]


def sig_day(root, ds, coin, date, threads=8, warmup_days=WARMUP_DAYS):
    t0 = time.time(); days = prev_dates(date, warmup_days) + [date]; macols = ["t_close", "trade_imb", "book_imb"] + [f"{p}_ma{n}" for n in mc.WINDOWS for p in ("timb", "bimb")]
    srcs = {"imb_usd": (f"{ds}/mas/{USD_TAG}/{coin}/perp", macols, False), "imb_sec": (f"{ds}/mas/{SEC_TAG}/{coin}/perp", macols, False),
            "px_usd": (f"{ds}/bars/{BAR_USD}/{coin}/perp", ["t_close", "vwap", "close", "partial"], True), "px_sec": (f"{ds}/tbars/1/{coin}/perp", ["t_close", "vwap", "close"], False)}
    def load(item):
        arm, (base, cols, dp) = item; return arm, _all(_concat(*[_read(f"{base}/{d}.parquet") for d in days]), cols, dp)
    with ThreadPoolExecutor(threads) as ex: frames = dict(ex.map(load, srcs.items()))
    X = build_sig(frames, fm.grid_t(date, 1)); tv.put_parquet(X, f"{root}/sig/{coin}/{date}.parquet")
    last = X.tail(1).drop("t").row(0); cols = X.columns[1:]
    done = {"coin": coin, "date": date, "rows": X.height, "rules": len(cols), "warmup_days": warmup_days, "bars": {arm: (F.height if F is not None else 0) for arm, F in frames.items()},
            "undecided_at_day_end": {arm: round(float(np.mean([v == 0 for c, v in zip(cols, last) if c.startswith(arm + ":")])), 3) for arm in mc.ARMS}, "secs": round(time.time() - t0, 1)}
    tv.write_json(f"{root}/sig_done/{coin}_{date}.json", done); return done


def sig(a):
    root = root_of(a); f = tv.fs(); p = tv.read_json(f"{root}/plan.json"); ds = dataset_root(p["dataset_run"])
    coin, date = tv.read_json(a.map)["elements"][tfl._idx(a)] if a.map else (a.coin, a.dates[0])
    if not a.force and f.exists(tv._key(f"{root}/sig_done/{coin}_{date}.json")): print(f"{coin} {date}: already done", file=sys.stderr); return 0
    print(json.dumps(sig_day(root, ds, coin, date)), file=sys.stderr, flush=True); return 0


# ----------------------------------------------------------------------------------------------- bask stage
# What is traded and how it is marked. pooled = the approved ALL mid of the alt basket (quotes up to 10 s old; unusable at 1 s); primary = each coin's top venue,
# forward-filled mid (the readable one; still stale on thin coins: 220 of 298 coins have a bar in < 30 % of seconds); fresh = primary venue, returns only between
# two consecutive seconds that both have a bar (biased long: discard); btc / eth = the source coin ITSELF traded on its primary venue (user request 2026-10-12:
# the same rules evaluated on BTC and ETH, so BTC flow -> BTC, ETH flow -> ETH and the cross pairs come out of the combos). Rulings in the spec's run record.
MARKS = {"pooled": {"bp": "basket", "cp": "basket_coins", "primary": False, "fresh": False, "coins": None},
         "primary": {"bp": "basket_primary", "cp": "basket_primary_coins", "primary": True, "fresh": False, "coins": None},
         "fresh": {"bp": "basket_fresh", "cp": "basket_fresh_coins", "primary": True, "fresh": True, "coins": None},
         "btc": {"bp": "basket_btc", "cp": "basket_btc_coins", "primary": True, "fresh": False, "coins": ["BTC"]},
         "eth": {"bp": "basket_eth", "cp": "basket_eth_coins", "primary": True, "fresh": False, "coins": ["ETH"]}}


def mark_coins(mark, targets):
    """The coins a mark trades: the plan's basket, or the single source coin."""
    return MARKS[mark]["coins"] or [t["coin"] for t in targets]


def primary_venue(T):
    """The venue with the largest traded notional in the coin-day's 1 s bars (ALL excluded); None when no venue row. The pooled ALL mid averages quotes up to
    10 s old across venues (median oldest age 2.7 s on SOL), which fakes a lead-lag from BTC flow; the primary venue's own mid is fresh (smoke macxA, 2026-10-12)."""
    if T is None or not T.height or "usd" not in T.columns: return None
    G = T.filter(pl.col("venue") != "ALL").group_by("venue").agg(pl.col("usd").sum().alias("usd")).sort("usd", descending=True)
    return G["venue"][0] if G.height else None


def build_basket(tbars_by_coin, grid, coins, venue_of=None, fresh=False):
    """(basket frame: t, r_bp, n_coins, spread_bp; coin frame: t + one Float32 column per coin) from the coins' 1 s bar frames (rows per venue: t_bucket, mid,
    spread_bp) marked at venue_of[coin] (ALL = the pooled mid when absent); fresh = returns only between two consecutive seconds that both have a bar.
    spread_bp = the equal-weight mean quoted spread of the coins with a value."""
    venue_of = venue_of or {}; R = np.full((len(grid), len(coins)), np.nan, dtype=np.float32); Sp = np.full((len(grid), len(coins)), np.nan, dtype=np.float32)
    for j, c in enumerate(coins):
        T = tbars_by_coin.get(c); v = venue_of.get(c) or "ALL"
        if T is not None and T.height:
            R[:, j] = mc.coin_returns(T, grid, venue=v, fresh=fresh)
            if "spread_bp" in T.columns: Sp[:, j] = mc.coin_spread(T, grid, venue=v)
    r, n = mc.basket(R); sp, _ = mc.basket(Sp); g = np.asarray(grid, dtype=np.int64)
    Bk = pl.DataFrame({"t": g, "r_bp": pl.Series(r, dtype=pl.Float32), "n_coins": pl.Series(n, dtype=pl.Int32), "spread_bp": pl.Series(sp, dtype=pl.Float32)}).with_columns(pl.col("r_bp").fill_nan(None), pl.col("spread_bp").fill_nan(None))
    C = pl.DataFrame({"t": g, **{c: pl.Series(R[:, j], dtype=pl.Float32) for j, c in enumerate(coins)}}).with_columns([pl.col(c).fill_nan(None) for c in coins])
    return Bk, C


def build_venue_matrix(tbars_by_coin, grid, coins):
    """t + one Float32 column per (coin, venue) series present in the coins' 1 s bar frames (ALL excluded), named "coin|venue": the coin's 1 s log mid return
    (bp) marked at that venue's own forward-filled mid — the raw material for "which pairs work on which exchanges"."""
    out = {"t": np.asarray(grid, dtype=np.int64)}
    for c in coins:
        T = tbars_by_coin.get(c)
        if T is None or not T.height or "venue" not in T.columns: continue
        for v in sorted(T["venue"].unique().to_list()):
            if v == "ALL": continue
            out[f"{c}|{v}"] = pl.Series(mc.coin_returns(T, grid, venue=v), dtype=pl.Float32)
    F = pl.DataFrame(out); return F.with_columns([pl.col(c).fill_nan(None) for c in F.columns if c != "t"])


def bask_day(root, ds, date, targets, threads=16):
    t0 = time.time()
    def load(t):
        path = f"{ds}/tbars/1/{t['coin']}/{t['kind']}/{date}.parquet"
        if not tv.fs().exists(tv._key(path)): return t["coin"], None
        return t["coin"], pl.scan_parquet(f"s3://{tv._key(path)}", storage_options=tv.so()).select("venue", "t_bucket", "mid", "spread_bp", "usd").collect()
    items = list(targets) + [{"coin": c, "kind": "perp"} for c in SOURCE_COINS]                      # the source coins are traded too (marks btc / eth)
    with ThreadPoolExecutor(threads) as ex: tb = dict(ex.map(load, items))
    coins = [t["coin"] for t in targets]; grid = fm.grid_t(date, 1); prim = {c: primary_venue(tb.get(c)) for c in tb}
    done = {"date": date, "rows": len(grid), "coins_planned": len(coins), "coins_with_bars": sum(1 for c in coins if tb.get(c) is not None and tb[c].height), "primary_venue": prim, "marks": {}}
    for mark, m in MARKS.items():
        mcoins = mark_coins(mark, targets); Bk, C = build_basket(tb, grid, mcoins, venue_of=prim if m["primary"] else None, fresh=m["fresh"])
        tv.put_parquet(Bk, f"{root}/{m['bp']}/{date}.parquet"); tv.put_parquet(C, f"{root}/{m['cp']}/{date}.parquet")
        done["marks"][mark] = {"mean_n_coins": round(float(Bk["n_coins"].mean()), 1), "basket_sum_bp": round(float(Bk["r_bp"].sum()), 2), "mean_spread_bp": round(float(Bk["spread_bp"].mean()), 3) if Bk["spread_bp"].null_count() < Bk.height else None}
    V = build_venue_matrix(tb, grid, coins); tv.put_parquet(V, f"{root}/venue_coins/{date}.parquet"); done["venue_series"] = len(V.columns) - 1
    done["secs"] = round(time.time() - t0, 1); tv.write_json(f"{root}/bask_done/{date}.json", done); return done


def bask(a):
    root = root_of(a); f = tv.fs(); p = tv.read_json(f"{root}/plan.json"); ds = dataset_root(p["dataset_run"])
    (date,) = tv.read_json(a.map)["elements"][tfl._idx(a)] if a.map else (a.dates[0],)
    if not a.force and f.exists(tv._key(f"{root}/bask_done/{date}.json")): print(f"{date}: already done", file=sys.stderr); return 0
    print(json.dumps(bask_day(root, ds, date, p["targets"])), file=sys.stderr, flush=True); return 0


# ----------------------------------------------------------------------------------------------- pnl (pure evaluation + the job)
def _df(rows, empty=None):
    """DataFrame from dicts with the schema inferred from EVERY row: the first rows of a column may all be null (p_value at lag 0) and polars' default 100-row
    inference would then type it Null and reject the later floats (smoke run macxA, 2026-10-12)."""
    return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame(empty or {})


def evaluate(B, E, r, RC, coins, dates, cols, secs_per_day=86_400, null_workers=8, null_lag=1):
    """Every rule x combination x lag against the basket: metrics (+ the circular-shift null at the executable lag), daily PnL, per-arm distributions, the
    split-half check (combination both, executable lag: winners by Sharpe on the first half, scored on the second), per-coin contributions of each arm x type's
    best rule, and the always-long baseline. B, E: int8 state matrices (N x rules) of BTC / ETH; r: basket bp per interval (NaN = 0); RC: N x coins bp."""
    from scipy import fft
    N = len(r); nd = len(dates); spd = int(secs_per_day); ds_idx = mc.day_starts(nd, spd); r64 = np.nan_to_num(np.asarray(r, dtype=np.float64), nan=0.0)
    FR = np.conj(fft.rfft(r64, workers=null_workers)); dirs = {"both": mc.combine(B, E), "btc": B, "eth": E}
    half = nd // 2; halves = {1: (0, half * spd), 2: (half * spd, N)} if nd >= 2 else {}
    rows, daily, split_rows, best_pos, hourly, hist, hod_rows = [], [], [], {}, [], [], []
    hours_per_day = max(1, spd // 3600) if spd >= 3600 else 1; hr_idx = np.arange(nd * hours_per_day, dtype=np.int64) * (spd // hours_per_day)
    offs = mc.et_offsets_h(dates); hr_hod = mc.hour_of_day(hr_idx, dates, spd, offs)                     # local (US Eastern) hour of day of every hour slot
    for combo, D in dirs.items():
        for lag in mc.LAGS_S:
            P = mc.shift_positions(D, lag)
            hod_hours = {}
            for a0 in range(0, P.shape[1], 64):                                                    # hourly PnL of every rule (equity curves), 64 rules at a time
                Hm = np.add.reduceat(P[:, a0:a0 + 64].astype(np.float32) * r64.astype(np.float32)[:, None], hr_idx, axis=0)
                hourly.extend({"rule": cols[a0 + j], "combo": combo, "lag_s": lag, "hour": int(h), "pnl_bp": float(Hm[h, j])} for j in range(Hm.shape[1]) for h in range(Hm.shape[0]))
                if lag == null_lag:
                    for j in range(Hm.shape[1]):
                        hod_hours[cols[a0 + j]] = {int(h): (float(Hm[hr_hod == h, j].mean()), float((Hm[hr_hod == h, j] > 0).mean()), int((hr_hod == h).sum())) for h in np.unique(hr_hod)}
            for k, col in enumerate(cols):
                pos = P[:, k]; p64 = pos.astype(np.float64); m = mc.metrics(pos, r64, ds_idx); arm, typ, f, s = rule_parts(col)
                row = {"rule": col, "arm": arm, "typ": typ, "fast": f, "slow": s, "combo": combo, "lag_s": lag, **m, "p_value": None, "null_p95_bp": None}
                if lag == null_lag:
                    tot = fft.irfft(FR * fft.rfft(p64, workers=null_workers), n=N, workers=null_workers); row["p_value"] = mc.p_value(tot[1:], tot[0]); row["null_p95_bp"] = float(np.quantile(np.abs(tot[1:]), 0.95)) if N > 1 else None
                    sp, st_ = mc.segment_pnls(pos, r64, with_starts=True); hist.extend({"rule": col, "combo": combo, "lag_s": lag, "bin_lo": lo, "bin_hi": hi, "count": c} for lo, hi, c in mc.trade_hist(sp))
                    ts_ = mc.hod_trade_stats(sp, mc.hour_of_day(st_, dates, spd, offs)) if sp.size else {}; hh = hod_hours.get(col, {})
                    for h in sorted(set(hh) | set(ts_)):
                        mh, sh, nh = hh.get(h, (None, None, 0)); t_ = ts_.get(h, {})
                        hod_rows.append({"rule": col, "combo": combo, "lag_s": lag, "hod_et": h, "n_hours": nh, "mean_hour_bp": mh, "share_hours_pos": sh, **{k: t_.get(k) for k in ("n_trades", "hit_rate", "trades_bp", "mean_trade_bp", "median_trade_bp")}})
                rows.append(row); pnl_d = np.add.reduceat(p64 * r64, ds_idx)
                daily.extend({"rule": col, "combo": combo, "lag_s": lag, "date": d, "pnl_bp": float(v)} for d, v in zip(dates, pnl_d))
                if combo == "both" and lag == null_lag:
                    best_pos[col] = pos
                    for h, (a0, b0) in halves.items():
                        mh = mc.metrics(pos[a0:b0], r64[a0:b0], mc.day_starts((b0 - a0) // spd, spd))
                        split_rows.append({"rule": col, "arm": arm, "typ": typ, "fast": f, "slow": s, "half": h, "days": (b0 - a0) // spd, "total_bp": mh["total_bp"], "sharpe_ann": mh["sharpe_ann"], "flips_per_day": mh["flips_per_day"], "pos_day_share": mh["pos_day_share"]})
    R = _df(rows)
    A = (R.sort("sharpe_ann", descending=True, nulls_last=True).group_by("arm", "typ", "combo", "lag_s", maintain_order=True)
         .agg(pl.col("sharpe_ann").median().alias("median_sharpe_ann"), pl.col("sharpe_ann").first().alias("best_sharpe_ann"), pl.col("rule").first().alias("best_rule"), pl.col("total_bp").median().alias("median_total_bp"),
              (pl.col("total_bp") > 0).mean().alias("share_positive"), (pl.col("p_value") < 0.05).mean().alias("share_p05"), pl.col("flips_per_day").median().alias("median_flips_per_day"), pl.len().alias("n_rules")).sort("arm", "typ", "combo", "lag_s"))
    Sp = _df(split_rows, {"rule": [], "half": [], "selected": []})
    if split_rows:
        W = Sp.filter(pl.col("half") == 1).sort("sharpe_ann", descending=True, nulls_last=True).group_by("arm", "typ", maintain_order=True).agg(pl.col("rule").first().alias("winner"))
        Sp = Sp.join(W, on=["arm", "typ"], how="left").with_columns((pl.col("rule") == pl.col("winner")).alias("selected")).drop("winner")
    best = R.filter((pl.col("combo") == "both") & (pl.col("lag_s") == null_lag)).sort("sharpe_ann", descending=True, nulls_last=True).group_by("arm", "typ", maintain_order=True).agg(pl.col("rule").first()).sort("arm", "typ")
    RC64 = np.nan_to_num(np.asarray(RC, dtype=np.float64), nan=0.0); n_t = np.maximum((~np.isnan(np.asarray(RC, dtype=np.float32))).sum(axis=1), 1).astype(np.float64); W_ = RC64 / n_t[:, None]
    long_c = W_.sum(axis=0); crow = []
    for rule in best["rule"].to_list():
        contrib = best_pos[rule].astype(np.float64) @ W_
        crow.extend({"rule": rule, "coin": c, "contrib_bp": float(contrib[j]), "always_long_bp": float(long_c[j]), "secs_with_value": int((~np.isnan(RC[:, j])).sum())} for j, c in enumerate(coins))
    Cn = _df(crow, {"rule": [], "coin": [], "contrib_bp": []})
    prof = []; Dboth = dirs["both"]
    for lag in mc.LAG_PROFILE_S:
        P = mc.shift_positions(Dboth, lag); tot = np.concatenate([r64 @ P[:, a0:a0 + 64].astype(np.float32) for a0 in range(0, P.shape[1], 64)])
        prof.extend({"rule": col, "arm": rule_parts(col)[0], "typ": rule_parts(col)[1], "fast": rule_parts(col)[2], "slow": rule_parts(col)[3], "lag_s": lag, "total_bp": float(v)} for col, v in zip(cols, tot))
    ones = np.ones(N, dtype=np.int8); ml = mc.metrics(ones, r64, ds_idx)
    Bs = _df([{"name": "always_long", **ml}, {"name": "always_short", **mc.metrics(-ones, r64, ds_idx)}])
    daily.extend({"rule": "always_long", "combo": "-", "lag_s": 0, "date": d, "pnl_bp": float(v)} for d, v in zip(dates, np.add.reduceat(r64, ds_idx)))     # the basket itself, day by day
    hourly.extend({"rule": "always_long", "combo": "-", "lag_s": 0, "hour": int(h), "pnl_bp": float(v)} for h, v in enumerate(np.add.reduceat(r64, hr_idx)))
    return {"rules": R, "daily": _df(daily), "arms": A, "split": Sp, "coins": Cn, "baseline": Bs, "lagprofile": _df(prof), "hourly": _df(hourly), "hist": _df(hist), "hod": _df(hod_rows)}


def with_mark(out, mark, half_spread_bp=None):
    """Tag every evaluate() table with the basket mark (pooled | primary); the baseline rows also carry the basket's mean half-spread (bp) for the break-even comparison."""
    out = {k: F.with_columns(pl.lit(mark).alias("mark")) for k, F in out.items()}
    out["baseline"] = out["baseline"].with_columns(pl.lit(half_spread_bp, dtype=pl.Float64).alias("half_spread_bp")); return out


def venue_metrics(dirs, r, cols, venue, n_coins, dates, secs_per_day=86_400, lag=1):
    """Metrics of every rule x combination at one lag against one venue's equal-weight basket return (the coins marked at that venue's own mid)."""
    r64 = np.nan_to_num(np.asarray(r, dtype=np.float64), nan=0.0); ds_idx = mc.day_starts(len(dates), secs_per_day); rows = []
    for combo, D in dirs.items():
        P = mc.shift_positions(D, lag)
        for k, col in enumerate(cols):
            arm, typ, f, s = rule_parts(col); rows.append({"venue": venue, "n_coins": float(n_coins), "rule": col, "arm": arm, "typ": typ, "fast": f, "slow": s, "combo": combo, "lag_s": lag, **mc.metrics(P[:, k], r64, ds_idx)})
    return _df(rows)


def cube_tables(dirs, RV, series, dates, cols, secs_per_day=86_400, lag=1):
    """Per (combination, rule, coin|venue series, date) PnL in bp of that series — positions (lag applied) against every series' own return — as
    {combo: float32 array [rules, series, days]} plus the per-series index (combo, rule, coin, venue, total_bp, sharpe_ann, days_up, secs_with_value)."""
    nd = len(dates); spd = int(secs_per_day); RV0 = np.nan_to_num(np.asarray(RV, dtype=np.float32), nan=0.0); cubes = {}; idx_rows = []
    cover = (~np.isnan(np.asarray(RV, dtype=np.float32))).sum(axis=0)
    for combo, D in dirs.items():
        P = mc.shift_positions(D, lag); C = np.zeros((len(cols), len(series), nd), dtype=np.float32)
        for d in range(nd):
            a0, b0 = d * spd, (d + 1) * spd; C[:, :, d] = P[a0:b0].astype(np.float32).T @ RV0[a0:b0]
        cubes[combo] = C; tot = C.sum(axis=2); mean = C.mean(axis=2); sd = C.std(axis=2, ddof=1) if nd > 1 else np.full(tot.shape, np.nan)
        with np.errstate(invalid="ignore", divide="ignore"): sh = np.where(sd > 0, mean / sd * np.sqrt(365.0), np.nan)
        up = (C > 0).mean(axis=2)
        for k, col in enumerate(cols):
            for j, sname in enumerate(series):
                coin, venue = sname.split("|", 1)
                idx_rows.append({"combo": combo, "rule": col, "coin": coin, "venue": venue, "total_bp": float(tot[k, j]), "sharpe_ann": (None if np.isnan(sh[k, j]) else float(sh[k, j])), "days_up": float(up[k, j]), "secs_with_value": int(cover[j])})
    return cubes, _df(idx_rows)


_G = {}                                                                                              # arrays shared with the worker threads (numpy releases the GIL inside its loops; forked processes deadlocked on the Polars / BLAS thread pools, 2026-10-12)


def _eval_mark(mark):
    m = MARKS[mark]; r, RC, coins, hs = _G["marks"][mark]
    out = with_mark(evaluate(_G["B"], _G["E"], r, RC, coins, _G["dates"], _G["cols"], null_workers=2), mark, hs); return mark, out


def _eval_venue(venue):
    r, n = _G["venues"][venue]; return venue, venue_metrics(_G["dirs"], r, _G["cols"], venue, n, _G["dates"])


def pnl_job(root, p, workers=16):
    t0 = time.time(); dates = p["dates"]; cols = mc.rule_columns(); so = tv.so()
    def rd(path): return pl.read_parquet(f"s3://{tv._key(path)}", storage_options=so)
    def load_sig(coin):
        X = pl.concat([rd(f"{root}/sig/{coin}/{d}.parquet") for d in dates], how="vertical"); return np.ascontiguousarray(X.select(cols).to_numpy().astype(np.int8))
    B = load_sig(p["source_coins"][0]); E = load_sig(p["source_coins"][1]); print(f"sig loaded {B.shape} {time.time() - t0:.0f}s", file=sys.stderr, flush=True)
    outs = {}; done = {"marks": {}}; _G.update({"B": B, "E": E, "dates": dates, "cols": cols, "marks": {}})
    for mark, m in MARKS.items():
        coins = mark_coins(mark, p["targets"]); bp, cp = m["bp"], m["cp"]
        Bk = pl.concat([rd(f"{root}/{bp}/{d}.parquet") for d in dates], how="vertical"); r = Bk["r_bp"].cast(pl.Float64).fill_null(float("nan")).to_numpy()
        RC = np.ascontiguousarray(pl.concat([rd(f"{root}/{cp}/{d}.parquet") for d in dates], how="vertical").select(coins).to_numpy().astype(np.float32))
        hs = float(Bk["spread_bp"].mean()) / 2 if "spread_bp" in Bk.columns and Bk["spread_bp"].null_count() < Bk.height else None
        _G["marks"][mark] = (r, RC, coins, hs); done["marks"][mark] = {"n_secs": int(len(r)), "mean_n_coins": float(Bk["n_coins"].mean()), "half_spread_bp": hs, "basket_total_bp": float(np.nansum(r))}
    print(f"baskets loaded {time.time() - t0:.0f}s; evaluating {len(MARKS)} marks in parallel threads", file=sys.stderr, flush=True)
    with ThreadPoolExecutor(len(MARKS)) as ex:
        for mark, out in ex.map(_eval_mark, list(MARKS)):
            for k, F in out.items(): outs.setdefault(k, []).append(F)
            print(f"{mark} evaluated {time.time() - t0:.0f}s", file=sys.stderr, flush=True)
    for k, Fs in outs.items(): tv.put_parquet(pl.concat(Fs, how="diagonal_relaxed"), f"{root}/summary/{k}.parquet")
    _G["marks"] = None
    # ---- coins x venues: per-venue baskets (full metrics) and the coin|venue x rule cube (daily PnL), lag 1 s, all combinations
    V = pl.concat([rd(f"{root}/venue_coins/{d}.parquet") for d in dates], how="diagonal_relaxed"); series = [c for c in V.columns if c != "t"]
    RV = np.ascontiguousarray(V.select(series).to_numpy().astype(np.float32)); del V; print(f"venue matrix {RV.shape} {time.time() - t0:.0f}s", file=sys.stderr, flush=True)
    dirs = {"both": mc.combine(B, E), "btc": B, "eth": E}; venues = sorted({sname.split("|", 1)[1] for sname in series}); _G["dirs"] = dirs; _G["venues"] = {}
    for v in venues:
        js = [j for j, sname in enumerate(series) if sname.endswith("|" + v)]; r_v, n_v = mc.basket(RV[:, js]); _G["venues"][v] = (r_v, float(n_v.mean()))
    with ThreadPoolExecutor(min(len(venues), 12)) as ex: VT = [F for _, F in ex.map(_eval_venue, venues)]
    tv.put_parquet(pl.concat(VT, how="diagonal_relaxed"), f"{root}/summary/venues.parquet"); print(f"venues evaluated {time.time() - t0:.0f}s", file=sys.stderr, flush=True)
    cubes, CI = cube_tables(dirs, RV, series, dates, cols); tv.put_parquet(CI, f"{root}/summary/cube_index.parquet")
    def put_rule(args):
        combo, k = args; C = cubes[combo][k]; rows = {"coin": [], "venue": [], "date": [], "pnl_bp": []}
        for j, sname in enumerate(series):
            coin, venue = sname.split("|", 1); rows["coin"] += [coin] * len(dates); rows["venue"] += [venue] * len(dates); rows["date"] += list(dates); rows["pnl_bp"] += [float(x) for x in C[j]]
        tv.put_parquet(pl.DataFrame(rows), f"{root}/cube/{combo}/{cols[k].replace(':', '_')}.parquet")
    with ThreadPoolExecutor(16) as ex: list(ex.map(put_rule, [(combo, k) for combo in dirs for k in range(len(cols))]))
    done.update({"venue_series": len(series), "venues": venues}); print(f"cube written {time.time() - t0:.0f}s", file=sys.stderr, flush=True)
    done.update({"rules_rows": sum(F.height for F in outs["rules"]), "n_secs": done["marks"]["pooled"]["n_secs"], "n_days": len(dates), "coins": len(p["targets"]), "secs": round(time.time() - t0), "finished_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")})
    tv.write_json(f"{root}/summary/_done.json", done); return done


def pnl(a):
    root = root_of(a); p = tv.read_json(f"{root}/plan.json"); print(json.dumps(pnl_job(root, p, workers=int(os.environ.get("POLARS_MAX_THREADS", "16")))), file=sys.stderr, flush=True); return 0


# ----------------------------------------------------------------------------------------------- controller
def missing_work(p, have_sig, have_bask, have_done):
    return {"sig": [[c, d] for d in p["dates"] for c in p["source_coins"] if f"{c}_{d}" not in have_sig], "bask": [[d] for d in p["dates"] if d not in have_bask], "pnl": not have_done}


def tier(stage, rnd):
    v0, m0, h = sb.tier_of(stage); m = min(m0 * 2 ** rnd, sb.big_cap()); return sb.shape(v0, m), m, h


def survey(root, p):
    f = tv.fs(); f.invalidate_cache()
    have_sig = {os.path.basename(x)[:-5] for x in f.glob(tv._key(f"{root}/sig_done/*.json"))}; have_bask = {os.path.basename(x)[:-5] for x in f.glob(tv._key(f"{root}/bask_done/*.json"))}
    m = missing_work(p, have_sig, have_bask, f.exists(tv._key(f"{root}/summary/_done.json")))
    m["counts"] = {"sig_done": len(have_sig), "sig_elements": p["sig_elements"], "bask_done": len(have_bask), "bask_elements": p["bask_elements"]}; return m


def submit_stage(a, root, stage, todo, rnd):
    batch = tv.batch_client(); tag = f"macx-{a.run_id}"; ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"); v, m, h = tier(stage, rnd); ids = []
    if stage == "pnl":
        r = batch.submit_job(**job_spec(f"{tag}-pnl", ["pnl", "--run-id", a.run_id, "--out", a.out], v, m, h)); ids.append(r["jobId"]); print(f"pnl r{rnd}: {r['jobId']} ({v} vCPU / {m} MiB)", file=sys.stderr, flush=True); return ids
    for k in range(0, len(todo), 10000):
        chunk = todo[k:k + 10000]; mp = f"{root}/map-{stage}-r{rnd}-{ts}-{k // 10000}.json"; tv.write_json(mp, {"elements": chunk})
        r = batch.submit_job(**job_spec(f"{tag}-{stage}", [stage, "--run-id", a.run_id, "--out", a.out, "--map", mp] + (["--force"] if rnd else []), v, m, h, array_size=len(chunk) if len(chunk) > 1 else None)); ids.append(r["jobId"])
        print(f"{stage} r{rnd}: {'array ' if len(chunk) > 1 else ''}{r['jobId']} x{len(chunk)} ({v} vCPU / {m} MiB)", file=sys.stderr, flush=True)
    return ids


def run(a):
    root = root_of(a); p = tv.read_json(f"{root}/plan.json"); batch = tv.batch_client(); f = tv.fs(); sp = f"{root}/controller.json"
    st = sb.controller_state(tv.read_json(sp) if f.exists(tv._key(sp)) else {"run_id": a.run_id, "jobs": {}}, os.environ.get("AWS_BATCH_JOB_ID"))
    def save(summ=None):
        st["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if summ is not None: st["summaries"] = summ
        tv.write_json(sp, st)
    for stage in STAGES:
        for rnd in range(ROUNDS + 1):
            m = survey(root, p); todo = m[stage]; st.update({"stage": stage, "round": rnd, "counts": m["counts"]}); save()
            print(json.dumps({"stage": stage, "round": rnd, "todo": len(todo) if isinstance(todo, list) else int(todo), **m["counts"]}), file=sys.stderr, flush=True)
            if not todo: break
            key = f"{stage}:{rnd}"; live = sb.live_jobs(batch, st["jobs"].get(key, []))
            if not live:
                if rnd == ROUNDS: st["phase"] = "failed"; st["error"] = f"{stage}: still missing after {ROUNDS} rounds"; save(); return 1
                live = submit_stage(a, root, stage, todo if isinstance(todo, list) else None, rnd); st["jobs"][key] = live; save()
            sb.wait_jobs(batch, live, on_tick=save)
    st["phase"] = "done"; st["counts"] = survey(root, p)["counts"]; save(); print("DONE", file=sys.stderr, flush=True); return 0


def status(a):
    root = root_of(a); f = tv.fs()
    if not f.exists(tv._key(f"{root}/plan.json")): print("no plan yet"); return 0
    p = tv.read_json(f"{root}/plan.json"); batch = tv.batch_client()
    while True:
        m = survey(root, p); k = m["counts"]; line = f"{datetime.now(ET).strftime('%H:%M:%S ET')}  coins {len(p['targets'])}  dates {len(p['dates'])}  sig {k['sig_done']}/{k['sig_elements']}  bask {k['bask_done']}/{k['bask_elements']}" + ("  PNL DONE" if not m["pnl"] else ""); cjob = cstat = None
        if f.exists(tv._key(f"{root}/controller_job.json")):
            cjob = tv.read_json(f"{root}/controller_job.json")["jobId"]; js = sb.describe(batch, [cjob]); cstat = js[0]["status"] if js else "GONE"; line += f"  controller {cstat}"
        c = tv.read_json(f"{root}/controller.json") if f.exists(tv._key(f"{root}/controller.json")) else None
        if c: line += f"  phase {c.get('phase')}  stage {c.get('stage')} r{c.get('round')}" + (f"  ERROR {c['error']}" if c.get("error") else "")
        print(line, flush=True)
        if not a.watch or sb.watch_over(c, cjob, cstat): return 0
        time.sleep(sb.POLL_S)


def main() -> int:
    ap = argparse.ArgumentParser(allow_abbrev=False); ap.add_argument("mode", choices=["plan", "run", "sig", "bask", "pnl", "status"])
    ap.add_argument("--run-id", required=True); ap.add_argument("--out"); ap.add_argument("--dataset-run", default=DATASET_RUN)
    ap.add_argument("--coins", type=lambda s: s.split(",")); ap.add_argument("--dates", nargs="*"); ap.add_argument("--coin"); ap.add_argument("--map"); ap.add_argument("--index", type=int)
    ap.add_argument("--force", action="store_true"); ap.add_argument("--resubmit", action="store_true"); ap.add_argument("--dry-run", action="store_true"); ap.add_argument("--no-submit", action="store_true"); ap.add_argument("--watch", action="store_true")
    a = ap.parse_args(); a.out = a.out or f"{tv.out_root()}/{a.run_id}"; a.dataset_run = a.dataset_run or settings.get("s3", "dataset_run")
    return {"plan": plan, "run": run, "sig": sig, "bask": bask, "pnl": pnl, "status": status}[a.mode](a)


if __name__ == "__main__":
    sys.exit(main())
