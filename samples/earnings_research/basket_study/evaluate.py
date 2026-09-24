"""Phase 3: per-event P&L across the experiment grid.

For every panel: signal (fade-the-gap, ±0.5% neutral band) -> direction ->
for each (construction x universe) cell, build the basket and book gross
simple-return P&L per $1 of reporter notional:

    pnl = sum_i  weight_i * (exit_i / entry_i - 1)

Entry = 9:30 open bar (reporter strict, peers forward-fill); exit = last RTH
close bar (forward-fill). Every event yields either a P&L row per cell or a
logged skip reason, PLUS the report bundle (K table, CSV, facts, 7 charts)
written to S3 under s3://<bucket>/earnings-basket-study/results/events/<id>/.

Subcommands (mirrors earnings_market_data's plan/array-child pattern):

  run          serial sweep over panels (local; --dates to slice)
  plan         STEP ZERO for AWS Batch: list panel keys into an S3 manifest,
               submit one array job of that size running eval-event.
               Sharding unit = ONE EVENT per array index.
  eval-event   array child: evaluate manifest.panels[AWS_BATCH_JOB_ARRAY_INDEX],
               write that event's bundle + a per-event parquet shard.
               Aggregation globs shards: .../results/shards/*.parquet.

Usage:
    python evaluate.py run [--dates 2026-07-16,...] [--no-charts]
    python evaluate.py plan [--dates ...]            # submits the Batch array
    python evaluate.py eval-event --manifest <s3 key>
"""

import argparse
import io
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from rules.sessions import (consolidate, entry_price, exit_price,
                            previous_close, signal_price, trade_day_for_event)
from rules.direction import fade_the_gap
from rules.sentiment_direction import apply_sentiment
from construction.base import BasketContext, admissible_peers, universe_symbols
from construction.registry import (CONSTRUCTIONS, OUTRIGHT, SPY_HEDGED, UNHEDGED_UNIVERSE,  # noqa: F401
                                   needs_spy, pseudo_universe, select as select_constructions)
from spy_bars import BENCHMARK_REL, SPY_SYMBOL, attach_spy_for_panel

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
# Panel sources come from the single study config (configs/study_config.json)
import study_config
_EVAL_CFG = study_config.load()["evaluation"]
PANEL_PREFIXES = _EVAL_CFG["panel_sources"]
PANEL_PREFIX = PANEL_PREFIXES[_EVAL_CFG["default_source"]]   # --source overrides
UNIVERSES = ["all", "pure play", "functional", "correlated"]
# Batch target for the array job `plan` submits: settings batch.job_queue and
# batch.job_definitions.basket_study (job definition owned by the infrastructure stack).

# Sentiment study configuration (Task 6)
BANDS = [2, 3, 4, 5]
SENTIMENT_S3_PREFIX = "earnings-basket-study/results-xle-sentiment"
MODELS_CSV = Path(__file__).resolve().parent / "configs" / "sentiment_models_90pct.csv"

# Intraday P&L mark times (ET minutes-of-day): every minute 9:31-9:35 (the
# open burst, minute resolution), 10:00, then every 30 minutes through 15:30;
# `pnl` itself remains the 15:59-close exit. Legs are marked at their last
# print at/or-before each time (same forward-fill convention as entries/exits).
MARK_MINUTES = [9 * 60 + m for m in (31, 32, 33, 34, 35)] + [10 * 60] \
    + [h * 60 + m for h in range(10, 16) for m in (30, 0) if h * 60 + m > 600]
MARK_MINUTES = sorted(set(MARK_MINUTES))
MARK_LABELS = [f"pnl_{m // 60:02d}{m % 60:02d}" for m in MARK_MINUTES]

# Reporter liquidity tier — proxy for cap category (no shares-outstanding in
# our data): trade-day dollar volume of the reporter, RTH only.
LIQ_TIERS = [(2e6, "micro"), (2e7, "small"), (2e8, "mid"), (float("inf"), "large")]

# per-event CSV column order: event identity first (repeated on every row so
# per-event files concatenate into one aggregable table), then the cell, then
# outcomes. Missing keys (e.g. on skip rows) are emitted as empty.
CSV_COLUMNS = ["event_id", "event_symbol", "event_date", "event_time_et",
               "trade_day", "signal_return_928_vs_prev_close", "direction",
               "reporter_dollar_vol_day", "liquidity_tier",
               "construction", "universe", "status", "skip_reason", "pnl",
               *MARK_LABELS,
               "n_peers_theo", "n_peers_actual", "peers_theo", "peers_actual",
               "n_ffill_entries", "hedge_gross", "clamped",
               "n_dropped_negative",
               "n_dropped_low_coverage", "dropped_low_coverage",
               # transaction-cost hook (2026-09-23): shares traded per $1 of
               # reporter notional (sum |w_i| / entry_i over marked legs) and the
               # per-leg detail "SYM:weight:entry ..." for order-level fee rules
               "shares_per_dollar", "legs"]


# canonical dtypes: skip-only events yield all-null columns that pandas types
# as object; without this, parquet shards get per-event schemas (String vs
# Int64) and the aggregation glob fails on a schema mismatch.
CSV_DTYPES = {"event_id": "Int64", "event_symbol": "string",
              "event_date": "string", "event_time_et": "string",
              "trade_day": "string",
              "signal_return_928_vs_prev_close": "Float64",
              "direction": "Int64",
              "reporter_dollar_vol_day": "Float64", "liquidity_tier": "string",
              "construction": "string",
              "universe": "string", "status": "string",
              "skip_reason": "string", "pnl": "Float64",
              **{lbl: "Float64" for lbl in MARK_LABELS},
              "n_peers_theo": "Int64", "n_peers_actual": "Int64",
              "peers_theo": "string", "peers_actual": "string",
              "n_ffill_entries": "Int64", "hedge_gross": "Float64",
              "clamped": "boolean", "n_dropped_negative": "Int64",
              "n_dropped_low_coverage": "Int64",
              "dropped_low_coverage": "string",
              "shares_per_dollar": "Float64", "legs": "string"}

# Sentiment-study extension columns (spec §6.1). ONLY appended when a
# sentiment_ctx is passed — the legacy no-ctx path keeps the exact legacy
# schema so results-xle-full reconciliation is byte-comparable.
SENTIMENT_COLUMNS = ["sentiment_model", "sentiment_score", "flip_band",
                     "direction_rule", "score_gap_agree", "code_version"]
SENTIMENT_DTYPES = {"sentiment_model": "string", "sentiment_score": "Float64",
                    "flip_band": "string", "direction_rule": "string",
                    "score_gap_agree": "boolean", "code_version": "string"}


def _code_version() -> str:
    """Git SHA written into the image at zip-packaging time; 'dev' locally.

    CHILD side of the spec §5 gate. Reads the baked file only — never git —
    so the value is whatever the image was built from.
    """
    p = Path(__file__).resolve().parent / "CODE_VERSION"
    return p.read_text().strip() if p.exists() else "dev"


def _git_code_version() -> str:
    """PLAN side of the spec §5 gate: the CURRENT git SHA of the working tree.

    Must NOT read CODE_VERSION — that file is what package_source.sh baked
    into the image, so comparing it against itself is a tautology and a
    stale image would sail through the child's assertion. Aborts when
    earnings_basket_study/ is dirty: a dirty tree has no SHA that can
    honestly describe the code the children would run.
    """
    import subprocess
    root = Path(__file__).resolve().parent.parent
    try:
        sha = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                             cwd=root, capture_output=True, text=True,
                             check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as e:
        raise SystemExit(f"FATAL: cannot determine git SHA for the manifest: {e}")
    # against HEAD, not the index: a staged-but-uncommitted change is just as
    # invisible to the image build as an unstaged one.
    dirty = subprocess.run(["git", "diff", "--quiet", "HEAD", "--",
                            str(Path(__file__).resolve().parent)], cwd=root)
    if dirty.returncode != 0:
        raise SystemExit(
            "FATAL: uncommitted changes under earnings_basket_study/ — commit "
            "them, re-run package_source.sh, and wait for the image build "
            "before planning (spec §5 code-version gate)")
    if not sha:
        raise SystemExit("FATAL: empty git SHA for the manifest")
    return sha


def event_table(panel: pd.DataFrame, sentiment_ctx: Optional[dict] = None,
                constructions: Optional[List[str]] = None) -> pd.DataFrame:
    """One event's results as a flat table, event fields on EVERY row —
    the per-event unit that aggregation concatenates. Dtypes are enforced so
    every shard has the identical parquet schema regardless of skip status."""
    df = pd.DataFrame(evaluate_event(panel, sentiment_ctx, constructions))
    cols = CSV_COLUMNS + (SENTIMENT_COLUMNS if sentiment_ctx is not None else [])
    dtypes = CSV_DTYPES | (SENTIMENT_DTYPES if sentiment_ctx is not None else {})
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df[cols].astype(dtypes)


def evaluate_event(panel: pd.DataFrame, sentiment_ctx: Optional[dict] = None,
                   constructions: Optional[List[str]] = None) -> List[dict]:
    """All grid cells for one panel. Returns long-format rows (incl. skips).

    constructions: names to evaluate (registry.select); default = the four
    hedged constructions x the four peer universes. Unhedged constructions
    (registry.OUTRIGHT, e.g. reporter_only) are emitted ONCE per event under
    universe UNHEDGED_UNIVERSE, since peer universes do not apply to them."""
    reporter = panel[panel["reporter_relationship"] == "primary"]["symbol"].iloc[0]
    event_id = int(panel["event_id"].iloc[0])
    event_ts = panel[panel["event_bar"]]["ts"].iloc[0]
    event_et = event_ts.tz_convert("America/New_York")
    base = {"event_id": event_id, "event_symbol": reporter,
            "event_date": str(event_et.date()), "event_time_et": str(event_et.time()),
            "event_ts_utc": str(event_ts)}

    day = trade_day_for_event(event_ts)
    if day is None:
        return [base | {"status": "skip", "skip_reason": "rth_release"}]
    base["trade_day"] = day

    bars = consolidate(panel)
    pc = previous_close(bars, reporter, day)
    sp = signal_price(bars, reporter, day)
    sig = fade_the_gap(sp, pc)
    if sig is None:
        return [base | {"status": "skip",
                        "skip_reason": "no_premarket_print" if sp is None else "no_prev_close"}]
    if sentiment_ctx is not None:
        score = sentiment_ctx["scores"].get(event_id)
        ssig = apply_sentiment(sig, score, sentiment_ctx["band"])
        base["sentiment_model"] = sentiment_ctx["model"]
        base["sentiment_score"] = ssig.sentiment_score
        base["flip_band"] = str(sentiment_ctx["band"]) if sentiment_ctx["band"] is not None else "none"
        base["direction_rule"] = ssig.direction_rule
        base["score_gap_agree"] = (None if score is None or sig.gap == 0
                                   else (score > 0) == (sig.gap > 0))
        base["code_version"] = sentiment_ctx["code_version"]
        sig_dir = ssig.direction
    else:
        sig_dir = sig.direction
    # the signal itself: reporter return from prior RTH close to the 9:28 mark
    base["signal_return_928_vs_prev_close"] = sig.gap
    if sig_dir == 0:
        return [base | {"status": "skip", "skip_reason": "neutral_band"}]
    base["direction"] = sig_dir

    rep_entry = entry_price(bars, reporter, day, allow_ffill=False)
    if rep_entry is None:
        return [base | {"status": "skip", "skip_reason": "reporter_no_open_bar"}]
    rep_exit = exit_price(bars, reporter, day)
    if rep_exit is None:
        return [base | {"status": "skip", "skip_reason": "reporter_no_exit"}]

    # reporter liquidity tier: trade-day RTH dollar volume (cap-category proxy)
    rep_rth = bars[(bars["symbol"] == reporter) & (bars["et_date"] == day)
                   & (bars["et_min"] >= 570) & (bars["et_min"] < 960)]
    dvol = float((rep_rth["close"] * rep_rth["volume"]).sum())
    base["reporter_dollar_vol_day"] = dvol
    base["liquidity_tier"] = next(t for lim, t in LIQ_TIERS if dvol < lim)

    # benchmark rows (SPY, spy_bars.attach_spy) are NOT peers: excluded from
    # every peer universe so the hedged grid is identical with or without them
    peers_raw = {rel: sorted(g["symbol"].unique())
                 for rel, g in panel[~panel["reporter_relationship"].isin(["primary", BENCHMARK_REL])]
                 .groupby("reporter_relationship")}
    # basket admission: coverage floor applied ONCE — every construction
    # weights the same admissible peer set (base.admissible_peers)
    peers, dropped_low_cov = admissible_peers(bars, peers_raw, day, reporter)
    base["n_dropped_low_coverage"] = len(dropped_low_cov)
    base["dropped_low_coverage"] = " ".join(sorted(dropped_low_cov))
    ctx = BasketContext(bars=bars, reporter=reporter, peers=peers,
                        trade_day=day, direction=sig_dir)

    def mark_price(sym: str, minute: int):
        """Last print at/before `minute` ET on the trade day (any session
        that morning), same forward-fill spirit as entry/exit marks."""
        b = bars[(bars["symbol"] == sym)
                 & ((bars["et_date"] < day)
                    | ((bars["et_date"] == day) & (bars["et_min"] <= minute)))]
        return float(b.iloc[-1]["close"]) if len(b) else None

    rows = []
    for cname, build in select_constructions(constructions).items():
        for univ in ([pseudo_universe(cname)] if pseudo_universe(cname) else UNIVERSES):
            cell = base | {"construction": cname, "universe": univ}
            basket = build(ctx, univ)
            if not basket.weights:
                rows.append(cell | {"status": "skip",
                                    "skip_reason": f"construction:{basket.diagnostics.get('skip')}"})
                continue
            pnl, n_ffill = 0.0, 0
            peers_actual = []
            marks = dict.fromkeys(MARK_LABELS, 0.0)
            shares_per_dollar, legs = 0.0, []
            for sym, w in basket.weights.items():
                if sym == reporter:
                    e, x = rep_entry, rep_exit
                else:
                    e = entry_price(bars, sym, day, allow_ffill=True)
                    x = exit_price(bars, sym, day)
                    at_open = bars[(bars["symbol"] == sym) & (bars["et_date"] == day)
                                   & (bars["et_min"] == 570)]
                    n_ffill += int(at_open.empty)
                if e is None or x is None or e <= 0:
                    continue                       # unmarkable leg: drop, log count
                pnl += w * (x / e - 1.0)
                shares_per_dollar += abs(w) / e
                legs.append(f"{sym}:{w:.10g}:{e:.10g}")
                for minute, lbl in zip(MARK_MINUTES, MARK_LABELS):
                    mp = mark_price(sym, minute)
                    if mp is not None:
                        marks[lbl] += w * (mp / e - 1.0)
                if sym != reporter:
                    peers_actual.append(sym)
            # theo = peer universe before any drops (clip/no-bars/unmarkable);
            # actual = peer legs that made it into the P&L. Symbol lists are
            # space-separated so the CSV stays flat.
            if cname in OUTRIGHT:
                peers_theo = []
            elif cname in SPY_HEDGED:
                peers_theo = [SPY_SYMBOL]
            else:
                peers_theo = universe_symbols(peers, univ, reporter)
            rows.append(cell | marks | {
                "status": "ok", "pnl": pnl,
                "n_peers_theo": len(peers_theo),
                "n_peers_actual": len(peers_actual),
                "peers_theo": " ".join(peers_theo),
                "peers_actual": " ".join(sorted(peers_actual)),
                "n_ffill_entries": n_ffill,
                "hedge_gross": basket.diagnostics.get("hedge_gross"),
                "clamped": basket.diagnostics.get("clamped"),
                "n_dropped_negative": len(basket.diagnostics.get("dropped_negative", []) or []),
                "shares_per_dollar": shares_per_dollar,
                "legs": " ".join(legs),
            })
    return rows


def list_panels(s3, dates: Optional[List[str]]) -> List[str]:
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=_bucket(), Prefix=PANEL_PREFIX):
        for o in page.get("Contents", []):
            k = o["Key"]
            if not k.endswith(".parquet"):
                continue
            if dates and not any(f"date={d}/" in k for d in dates):
                continue
            keys.append(k)
    return keys


def process_panel(panel: pd.DataFrame, s3io, report_dir: Path, s3_prefix: str,
                  charts: bool, constructions: Optional[List[str]] = None) -> pd.DataFrame:
    """Evaluate one panel, write its report bundle locally + to S3.
    Returns the event's result table. The per-EVENT unit of work — identical
    under the serial sweep and the Batch array child."""
    from report import build_event_report, sync_dir_to_s3
    from display import box_table

    if needs_spy(constructions):
        panel = attach_spy_for_panel(panel, s3io)       # SPY leg for the SPY-hedged cells
    t = event_table(panel, constructions=constructions)
    event_id = int(t["event_id"].iloc[0])
    evdir = Path(report_dir) / str(event_id)
    if charts:
        build_event_report(panel, t, evdir)
    else:
        evdir.mkdir(parents=True, exist_ok=True)
        t.to_csv(evdir / f"event_{event_id}.csv", index=False)
        (evdir / f"event_{event_id}.k").write_text(box_table(t) + "\n")
    if s3_prefix:
        sync_dir_to_s3(s3io, evdir, f"s3://{_bucket()}/{s3_prefix}/events/{event_id}")
    return t


def _constructions(args) -> Optional[List[str]]:
    """--constructions 'a,b' -> [a, b]; None = the hedged four (registry default).
    Validated here so a typo fails at plan time, not in 167k array children."""
    raw = getattr(args, "constructions", None)
    if not raw:
        return None
    names = [n.strip() for n in raw.split(",") if n.strip()]
    select_constructions(names)
    return names


def _session(profile):
    import boto3
    return boto3.Session(profile_name=profile) if profile else boto3.Session()


def cmd_run(args) -> int:
    """Serial sweep (local)."""
    from botocore.config import Config
    from s3io import S3IO
    import polars as pl_

    session = _session(args.profile)
    s3 = session.client("s3", config=Config(max_pool_connections=16,
                                            retries={"max_attempts": 2, "mode": "standard"}))
    s3io = S3IO(profile=args.profile)
    dates = args.dates.split(",") if args.dates else None
    keys = list_panels(s3, dates)
    print(f"panels: {len(keys)}")

    tables = []
    for k in keys:
        panel = pd.read_parquet(io.BytesIO(s3.get_object(Bucket=_bucket(), Key=k)["Body"].read()))
        try:
            tables.append(process_panel(panel, s3io, Path(args.report_dir),
                                        args.s3_prefix, charts=not args.no_charts,
                                        constructions=_constructions(args)))
        except Exception as e:
            tables.append(pd.DataFrame([{"status": "error", "skip_reason": repr(e)}
                                        ]).reindex(columns=CSV_COLUMNS))

    df = pd.concat(tables, ignore_index=True)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    if args.s3_prefix:
        s3io.write_parquet(pl_.from_pandas(df),
                           f"s3://{_bucket()}/{args.s3_prefix}/{out.name}")

    ok = df[df["status"] == "ok"]
    print(f"rows: {len(df)} | ok: {len(ok)} | events with >=1 ok cell: "
          f"{ok['event_id'].nunique() if len(ok) else 0}")
    if len(df[df["status"] == "skip"]):
        print("skip reasons:")
        print(df[df["status"] == "skip"]["skip_reason"].value_counts().to_string())
    print(f"\nwrote {out} and {args.report_dir}/<event_id>/ bundles x "
          f"{df['event_id'].nunique()}")
    return 0


def cmd_plan(args) -> int:
    """STEP ZERO: panel-key manifest -> one Batch array job, one event/index."""
    import json
    from s3io import S3IO

    session = _session(args.profile)
    s3 = session.client("s3")
    s3io = S3IO(profile=args.profile)
    dates = args.dates.split(",") if args.dates else None
    keys = list_panels(s3, dates)
    n = len(keys)
    # Batch caps an array at 10,000 children -> chunk the manifest
    CHUNK = 10_000
    chunks = [keys[i:i + CHUNK] for i in range(0, n, CHUNK)] or [[]]
    tag = dates[0] if dates else "all"
    manifest_keys = []
    for ci, chunk in enumerate(chunks):
        mk = f"{args.s3_prefix}/manifests/manifest_{tag}_{n}_part{ci}.json"
        s3io.write_text(json.dumps({"panels": chunk, "s3_prefix": args.s3_prefix,
                                    "charts": not args.no_charts,
                                    "constructions": _constructions(args)}, indent=2),
                        f"s3://{_bucket()}/{mk}")
        manifest_keys.append(mk)
    if n == 0:
        print(json.dumps({"manifests": manifest_keys, "event_count": 0,
                          "submitted": False}))
        return 0
    if args.dry_run:
        print(json.dumps({"manifests": manifest_keys, "event_count": n,
                          "submitted": False, "dry_run": True}, indent=2))
        return 0
    batch = session.client("batch", region_name=settings.get("aws", "region"))
    eval_job_ids = []
    for mk, chunk in zip(manifest_keys, chunks):
        resp = batch.submit_job(
            jobName=f"basket-study-eval-{len(chunk)}",
            jobQueue=settings.get("batch", "job_queue"),
            jobDefinition=settings.get("batch", "job_definitions", "basket_study"),
            arrayProperties={"size": len(chunk)},
            containerOverrides={"command": ["eval-event", "--manifest", mk]},
        )
        eval_job_ids.append(resp["jobId"])
    out = {"manifests": manifest_keys, "event_count": n, "submitted": True,
           "job_ids": eval_job_ids, "job_queue": settings.get("batch", "job_queue"),
           "constructions": _constructions(args)}
    if args.aggregate:
        # aggregation fan-out: one array child PER EXIT MARK (intraday marks
        # + close; index -> exit via --mark auto), dependent on the eval
        # array; then a finalize job (exit-timing study) dependent on the
        # mark array. Array size derives from MARK_LABELS so new marks are
        # picked up automatically.
        n_exits = len(MARK_LABELS) + 1
        spy_w = getattr(args, "spy_weight", 0.0) or 0.0
        lev = getattr(args, "strategy_leverage", 1.0) or 1.0
        agg_cmd_extra = (["--spy-weight", str(spy_w)] if spy_w else []) + \
                        (["--strategy-leverage", str(lev)] if lev != 1 else [])
        # >100k-shard aggregations OOM at the job def's 4GB (learned on the
        # 10y run, 2026-08-25) — override to 16GB/2vCPU unconditionally.
        agg_resources = [{"type": "MEMORY", "value": "16384"},
                         {"type": "VCPU", "value": "2"}]
        agg = batch.submit_job(
            jobName=f"basket-study-agg-marks-{n_exits}",
            jobQueue=settings.get("batch", "job_queue"),
            jobDefinition=settings.get("batch", "job_definitions", "basket_study"),
            dependsOn=[{"jobId": j} for j in eval_job_ids],
            arrayProperties={"size": n_exits},
            containerOverrides={"command": ["aggregate", "--mark", "auto",
                                            "--s3-prefix", args.s3_prefix]
                                            + agg_cmd_extra,
                                "resourceRequirements": agg_resources},
        )
        fin = batch.submit_job(
            jobName="basket-study-agg-finalize",
            jobQueue=settings.get("batch", "job_queue"),
            jobDefinition=settings.get("batch", "job_definitions", "basket_study"),
            dependsOn=[{"jobId": agg["jobId"]}],
            containerOverrides={"command": ["aggregate", "--finalize",
                                            "--s3-prefix", args.s3_prefix]
                                            + agg_cmd_extra,
                                "resourceRequirements": agg_resources},
        )
        out["aggregate_marks_job_id"] = agg["jobId"]
        out["aggregate_finalize_job_id"] = fin["jobId"]
    print(json.dumps(out, indent=2))
    return 0


def cmd_eval_event(args) -> int:
    """Batch array child: one panel = manifest.panels[AWS_BATCH_JOB_ARRAY_INDEX].
    Writes the event bundle + its own parquet shard (no cross-child merge —
    aggregation globs .../results/shards/*.parquet)."""
    import json
    import os
    from s3io import S3IO
    import polars as pl_

    idx = os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX")
    if idx is None:
        raise SystemExit("eval-event requires AWS_BATCH_JOB_ARRAY_INDEX")
    s3io = S3IO(profile=args.profile)
    m = json.loads(s3io.read_bytes(f"s3://{_bucket()}/{args.manifest}"))
    panels, s3_prefix = m["panels"], m["s3_prefix"]
    i = int(idx)
    if i >= len(panels):
        raise SystemExit(f"array index {i} out of range ({len(panels)} panels)")

    session = _session(args.profile)
    s3 = session.client("s3")
    panel = pd.read_parquet(io.BytesIO(
        s3.get_object(Bucket=_bucket(), Key=panels[i])["Body"].read()))
    t = process_panel(panel, s3io, Path(args.report_dir), s3_prefix,
                      charts=m.get("charts", True),
                      constructions=m.get("constructions"))
    event_id = int(t["event_id"].iloc[0])
    s3io.write_parquet(pl_.from_pandas(t),
                       f"s3://{_bucket()}/{s3_prefix}/shards/event_{event_id}.parquet")
    print(json.dumps({"index": i, "event_id": event_id, "rows": len(t),
                      "panel": panels[i]}))
    return 0


def cmd_plan_sentiment(args) -> int:
    """Spec §5: one manifest, one array job; child index 0 = baseline
    (band=none), indices 1..N = one sentiment model each (bands 2-5).
    Filters panels to the XLE event universe.

    Task 11 extension: --shards-per-model N fans out each model into N
    event slices (stride slicing). Array becomes 1 + n_models*N children."""
    import json
    import re
    from s3io import S3IO
    from sentiment_scores import load_model_list

    session = _session(args.profile)
    s3 = session.client("s3")
    s3io = S3IO(profile=args.profile)

    # Load universe event IDs from the specified JSON
    universe_uri = args.universe_json
    universe_data = json.loads(s3io.read_bytes(universe_uri))
    universe_event_ids = {int(e["event_id"]) for e in universe_data["events"]}
    n_universe = len(universe_event_ids)

    # List all panels and filter to universe events
    all_keys = list_panels(s3, args.dates.split(",") if args.dates else None)
    event_pattern = re.compile(r"event_(\d+)\.parquet$")
    keys = []
    for k in all_keys:
        m = event_pattern.search(k)
        if m and int(m.group(1)) in universe_event_ids:
            keys.append(k)

    models = [] if args.baseline_only else load_model_list(str(MODELS_CSV))
    # spec §5: the manifest carries the CURRENT git SHA; the child compares it
    # against the SHA baked into its image. Using _code_version() here would
    # compare the baked file to itself (tautology) — see _git_code_version.
    code_version = _git_code_version()
    prefix = args.sentiment_s3_prefix
    shards_per_model = args.shards_per_model
    manifest = {"panels": keys, "models": models, "s3_prefix": prefix,
                "code_version": code_version, "universe_json": universe_uri,
                "shards_per_model": shards_per_model}
    mk = f"{prefix}/manifests/manifest_{len(models)}models_{len(keys)}panels.json"
    s3io.write_text(json.dumps(manifest, indent=2), f"s3://{_bucket()}/{mk}")
    size = 1 + len(models) * shards_per_model
    if args.dry_run:
        print(json.dumps({"manifest": mk, "array_size": size,
                          "code_version": code_version, "n_panels": len(keys),
                          "n_universe": n_universe, "shards_per_model": shards_per_model,
                          "submitted": False}, indent=2))
        return 0
    batch = session.client("batch", region_name=settings.get("aws", "region"))
    resp = batch.submit_job(
        jobName=f"basket-sentiment-eval-{size}",
        jobQueue=settings.get("batch", "job_queue"), jobDefinition=settings.get("batch", "job_definitions", "basket_study"),
        arrayProperties={"size": size} if size > 1 else {},
        containerOverrides={"command": ["eval-model", "--manifest", mk]},
    )
    print(json.dumps({"manifest": mk, "array_size": size,
                      "code_version": code_version, "n_panels": len(keys),
                      "n_universe": n_universe, "shards_per_model": shards_per_model,
                      "job_id": resp["jobId"], "job_queue": settings.get("batch", "job_queue")}, indent=2))
    return 0


def cmd_eval_model(args) -> int:
    """Array child: index 0 -> baseline (band None); index i -> model i-1,
    all bands. Reads every panel once; one atomic shard write at the end.

    Task 11 extension: with shards_per_model > 1, child i>=1 maps to
    (model_idx, slice_idx) and evaluates only panels[slice_idx::N] for all
    bands, writing model=<m>.part=<slice_idx>.parquet."""
    import json
    import os
    from s3io import S3IO
    import polars as pl_
    from sentiment_scores import load_scores
    from shard_io import child_index_to_model_slice, panels_for_slice

    idx = int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX", "0"))
    s3io = S3IO(profile=args.profile)
    m = json.loads(s3io.read_bytes(f"s3://{_bucket()}/{args.manifest}"))
    if m["code_version"] != _code_version():
        print(f"FATAL: image code_version {_code_version()!r} != manifest "
              f"{m['code_version']!r} — refresh source.zip and rebuild")
        return 3
    panels, models, prefix = m["panels"], m["models"], m["s3_prefix"]
    shards_per_model = m.get("shards_per_model", 1)  # backward compat: default 1

    if idx == 0:
        # Baseline: always single-file, all panels, no slicing
        model_name, bands, scores = "baseline", [None], {}
        panels_to_eval = panels
        slice_idx = None
    else:
        model_idx, slice_idx = child_index_to_model_slice(idx, len(models), shards_per_model)
        model_name = models[model_idx]
        bands = BANDS
        scores = load_scores(s3io, model_name)
        panels_to_eval = panels_for_slice(panels, slice_idx, shards_per_model)

    session = _session(args.profile)
    s3 = session.client("s3")
    sentiment_cols = CSV_COLUMNS + SENTIMENT_COLUMNS
    sentiment_dtypes = CSV_DTYPES | SENTIMENT_DTYPES
    tables = []
    n_errors = 0
    for k in panels_to_eval:
        panel = pd.read_parquet(io.BytesIO(s3.get_object(Bucket=_bucket(), Key=k)["Body"].read()))
        for band in bands:
            ctx = {"model": model_name, "band": band, "scores": scores,
                   "code_version": m["code_version"]}
            try:
                tables.append(event_table(panel, ctx))
            except Exception as e:
                # project the fallback row through the SAME columns/dtypes as a
                # normal row: a raw dict frame would give this shard a different
                # parquet schema and break the aggregation glob.
                n_errors += 1
                err = pd.DataFrame([{"status": "error", "skip_reason": repr(e),
                                     "sentiment_model": model_name,
                                     "flip_band": str(band) if band is not None else "none",
                                     "code_version": m["code_version"]}])
                for c in sentiment_cols:
                    if c not in err.columns:
                        err[c] = None
                tables.append(err[sentiment_cols].astype(sentiment_dtypes))
    df = pd.concat(tables, ignore_index=True)

    # Write shard: legacy single-file when N=1, multi-part when N>1
    if shards_per_model == 1:
        shard_key = f"s3://{_bucket()}/{prefix}/shards/model={model_name}.parquet"
    else:
        # Multi-part naming (slice_idx is None for baseline, but baseline always N=1)
        part_suffix = f".part={slice_idx}" if slice_idx is not None else ""
        shard_key = f"s3://{_bucket()}/{prefix}/shards/model={model_name}{part_suffix}.parquet"

    s3io.write_parquet(pl_.from_pandas(df), shard_key)
    print(json.dumps({"index": idx, "model": model_name, "slice": slice_idx,
                      "bands": [str(b) for b in bands], "rows": len(df),
                      "n_panels": len(panels_to_eval), "n_errors": n_errors}))
    # the shard is written first so the partial result is inspectable, then we
    # fail the child so Batch marks it FAILED — silent error rows in a shard
    # that Batch calls SUCCEEDED are exactly the failure mode of spec §8.
    if n_errors:
        print(f"FATAL: {n_errors} event(s) raised during evaluation; shard "
              f"written to {shard_key} for inspection")
        return 4
    return 0


def cmd_aggregate(args) -> int:
    """Aggregation as a subcommand — same code path locally and on Batch
    (the image's entrypoint is evaluate.py, so `aggregate` is invokable via
    containerOverrides)."""
    import aggregate
    aggregate.main(args)
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("command", nargs="?", default="run",
                    choices=["run", "plan", "eval-event", "aggregate", "plan-sentiment", "eval-model"])
    ap.add_argument("--dates", default=None, help="comma-separated panel dates; default all")
    ap.add_argument("--out", default=str(Path(__file__).resolve().parent / "results" / "results_v1.parquet"))
    ap.add_argument("--report-dir", default=str(Path(__file__).resolve().parent / "results" / "events"),
                    help="local staging dir for report bundles")
    ap.add_argument("--s3-prefix", default="earnings-basket-study/results",
                    help="S3 destination under the panels bucket; '' disables upload")
    ap.add_argument("--no-charts", action="store_true",
                    help="tables only (fast rerun; skips the 7 report charts)")
    ap.add_argument("--constructions", default=None,
                    help="run/plan: comma-separated construction names to evaluate "
                         "(registry.ALL_CONSTRUCTIONS); default = the four hedged ones. "
                         "Unhedged names (reporter_only) emit one row per event under "
                         f"universe {UNHEDGED_UNIVERSE!r}. Carried in the manifest to array children.")
    ap.add_argument("--manifest", default=None, help="eval-event: manifest S3 key")
    ap.add_argument("--dry-run", action="store_true",
                    help="plan: write the manifest but do not submit the job")
    ap.add_argument("--aggregate", action="store_true",
                    help="plan: chain the aggregation mark-array + finalize "
                         "after the eval array (dependsOn)")
    ap.add_argument("--mark", default=None,
                    help="aggregate: one exit column, or 'auto' (array index)")
    ap.add_argument("--max-basket-weight", type=float, default=None,
                    help="aggregate: max fraction of a cell's capital per basket "
                         "(default = aggregate.MAX_BASKET_WEIGHT; 1.0 = uncapped)")
    ap.add_argument("--finalize", action="store_true",
                    help="aggregate: exit-timing study only")
    ap.add_argument("--spy-weight", type=float, default=0.0,
                    help="aggregate / plan --aggregate: always-long SPY base "
                         "holding (r_port = w*r_spy + r_strategy, daily "
                         "rebalanced). 0 = off (legacy artifacts, default); "
                         "nonzero writes to aggregate_spyw<w>/")
    ap.add_argument("--strategy-leverage", type=float, default=1.0,
                    help="aggregate / plan --aggregate: leverage L on the "
                         "strategy leg (r_port = w*r_spy + L*r_strategy); "
                         "1 = unlevered (default)")
    ap.add_argument("--source", default=_EVAL_CFG["default_source"],
                    choices=list(PANEL_PREFIXES),
                    help="panel source (from configs/study_config.json; "
                         f"default {_EVAL_CFG['default_source']})")
    ap.add_argument("--profile", default=None,
                    help="AWS profile (default: standard credential chain, "
                         "i.e. the Batch task role in-container)")
    ap.add_argument("--baseline-only", action="store_true",
                    help="plan-sentiment: submit only the baseline child (Loop 1)")
    ap.add_argument("--sentiment-s3-prefix", default=SENTIMENT_S3_PREFIX)
    ap.add_argument("--universe-json",
                    default=f"s3://{settings.get('s3', 'diagnostics_bucket')}/diagnostics/earnings_universe/xle_412.json",
                    help="plan-sentiment: universe event list (default: XLE 412 events)")
    ap.add_argument("--shards-per-model", type=int, default=1,
                    help="plan-sentiment: fan-out factor per model (1 = legacy single-file, "
                         "N>1 = stride-slice events into N children per model)")
    args = ap.parse_args()

    global PANEL_PREFIX
    PANEL_PREFIX = PANEL_PREFIXES[args.source]

    if args.command == "plan":
        return cmd_plan(args)
    if args.command == "eval-event":
        if not args.manifest:
            raise SystemExit("eval-event requires --manifest")
        return cmd_eval_event(args)
    if args.command == "aggregate":
        return cmd_aggregate(args)
    if args.command == "plan-sentiment":
        return cmd_plan_sentiment(args)
    if args.command == "eval-model":
        if not args.manifest:
            raise SystemExit("eval-model requires --manifest")
        return cmd_eval_model(args)
    return cmd_run(args)


if __name__ == "__main__":
    sys.exit(main())