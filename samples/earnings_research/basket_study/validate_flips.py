"""Loop-2 gate (spec §9 Phase 2): pairwise per-event validation of every
(band) slice of a model shard against the baseline. This is the UNIT TEST
of the flip plumbing, not a research metric (spec §6.2):

  fade / fade_no_score rows  -> value identical to baseline       (atol 1e-9)
  momentum_flip rows         -> value identical in ABS, opposite sign
  whole-run magnitude        -> abs(value) identical to baseline
  skip rows                  -> same cells, same skip_reason as baseline
  anything else              -> code bug; investigate, never rationalize

"value" means `pnl` AND every intraday mark column `pnl_HHMM`: the flip is a
direction change, so it must negate the whole P&L path, not just the close.
Checking `pnl` alone would let a mark-time bug through the gate.

Run in a write-code-until-green loop across all 47 shards.
"""

import argparse
import io
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

KEYS = ["event_id", "construction", "universe"]
ATOL = 1e-9
EXPECTED_BANDS = {"2", "3", "4", "5"}
BANDS_LIST = ["2", "3", "4", "5"]


def value_columns(base: pd.DataFrame, model: pd.DataFrame) -> list:
    """`pnl` plus every intraday mark column present in BOTH frames, in a
    stable order. The flip negates the entire P&L path, so every mark is
    subject to the same invariant as the close."""
    shared = set(base.columns) & set(model.columns)
    marks = sorted(c for c in shared if c.startswith("pnl_"))
    return (["pnl"] if "pnl" in shared else []) + marks


def validate(base: pd.DataFrame, model: pd.DataFrame) -> pd.DataFrame:
    """Empty frame = valid. base: flip_band-less legacy-schema baseline rows
    (status ok + skip). model: one model's rows, all bands, sentiment columns."""
    diffs = []
    b = base[base["status"] == "ok"].set_index(KEYS)
    model_ok = model[model["status"] == "ok"]
    cols = value_columns(base, model)

    # CRITICAL 2: Check for missing bands
    present_bands = set(model_ok["flip_band"].unique())
    for missing_band in EXPECTED_BANDS - present_bands:
        diffs.append({"band": missing_band, "key": None, "problem": f"missing_band {missing_band}"})

    for band, g in model_ok.groupby("flip_band"):
        band_index = g.set_index(KEYS)

        # CRITICAL 1: Check for baseline cells missing from model
        for baseline_key in b.index:
            if baseline_key not in band_index.index:
                diffs.append({"band": band, "key": baseline_key, "problem": "missing_in_model"})

        for key, r in band_index.iterrows():
            if key not in b.index:
                diffs.append({"band": band, "key": key, "problem": "cell_missing_in_baseline"})
                continue
            rule = r["direction_rule"]
            if rule not in ("fade", "fade_no_score", "momentum_flip"):
                diffs.append({"band": band, "key": key,
                              "problem": f"unknown_rule {rule}"})
                continue
            for col in cols:
                ref = float(b.loc[key, col]) if b.loc[key, col] is not None else float("nan")
                new = float(r[col]) if r[col] is not None else float("nan")

                # MINOR 3: NaN on an ok row (marks may legitimately be null
                # only if BOTH sides are null — a one-sided NaN is a bug).
                if pd.isna(ref) and pd.isna(new):
                    continue
                if pd.isna(ref) or pd.isna(new):
                    diffs.append({"band": band, "key": key,
                                  "problem": f"{col}_is_nan"})
                    continue

                if rule in ("fade", "fade_no_score"):
                    if abs(new - ref) > ATOL:
                        diffs.append({"band": band, "key": key, "problem":
                                      f"non_flipped_drift col={col} "
                                      f"ref={ref} new={new}"})
                else:   # momentum_flip
                    if abs(new + ref) > ATOL:
                        diffs.append({"band": band, "key": key, "problem":
                                      f"flip_not_exact_negation col={col} "
                                      f"ref={ref} new={new}"})
                # magnitude invariant holds given the checks above, but assert
                # it explicitly (spec: signs only, never magnitudes)
                if abs(abs(new) - abs(ref)) > ATOL:
                    diffs.append({"band": band, "key": key, "problem":
                                  f"magnitude_drift col={col} "
                                  f"ref={ref} new={new}"})
    diffs.extend(_validate_skips(base, model))
    return pd.DataFrame(diffs)


def _validate_skips(base: pd.DataFrame, model: pd.DataFrame) -> list:
    """Skip-row pass. A flip can only change the DIRECTION of a traded event;
    it can never change WHY an event was unsellable. So for every band:

      - a skip cell present in both must carry the identical skip_reason
      - a baseline skip cell missing from the band is a dropped row

    (A pre-signal skip like rth_release has no per-cell keys in either frame,
    so it simply doesn't participate; only keyed skip cells are compared.)"""
    out = []
    if "skip_reason" not in base.columns or "skip_reason" not in model.columns:
        return out
    bs = base[base["status"] == "skip"].dropna(subset=KEYS)
    if bs.empty and (model["status"] == "skip").sum() == 0:
        return out
    bs = bs.set_index(KEYS)
    ms = model[model["status"] == "skip"].dropna(subset=KEYS)
    bands = sorted(set(model["flip_band"].dropna().unique()) | EXPECTED_BANDS)
    for band in bands:
        gi = ms[ms["flip_band"] == band].set_index(KEYS)
        for key, r in gi.iterrows():
            if key not in bs.index:
                continue        # sentiment-side-only skip: covered elsewhere
            ref, new = bs.loc[key, "skip_reason"], r["skip_reason"]
            if str(ref) != str(new):
                out.append({"band": band, "key": key, "problem":
                            f"skip_reason_mismatch ref={ref!r} new={new!r}"})
        for key in bs.index:
            if key not in gi.index:
                out.append({"band": band, "key": key,
                            "problem": "baseline_skip_missing_in_model"})
    return out


def _load(uri: str, profile):
    """Load a shard from a URI. Supports both full S3 URIs (backward compat)
    and model name + prefix (globbing multi-part shards)."""
    if uri.startswith("s3://"):
        from s3io import S3IO
        return pd.read_parquet(io.BytesIO(S3IO(profile=profile).read_bytes(uri)))
    return pd.read_parquet(uri)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=True,
                    help="Full S3 URI or local path to baseline shard")
    ap.add_argument("--model-shard", required=False,
                    help="Full S3 URI or local path to model shard (legacy single-file)")
    ap.add_argument("--model", required=False,
                    help="Model name (for multi-part shards; requires --s3-prefix)")
    ap.add_argument("--s3-prefix", required=False,
                    help="S3 prefix (e.g., 'earnings-basket-study/results-xle-sentiment')")
    ap.add_argument("--profile", default=None)
    args = ap.parse_args()

    # Support EITHER --model-shard (full URI, backward compat) OR --model + --s3-prefix
    if args.model_shard:
        model_df = _load(args.model_shard, args.profile)
    elif args.model and args.s3_prefix:
        import boto3
        from shard_io import read_model_shards
        import settings
        BUCKET = settings.get("s3", "data_bucket")
        session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
        s3 = session.client("s3")
        model_df = read_model_shards(s3, BUCKET, args.s3_prefix, args.model, args.profile)
    else:
        raise SystemExit("validate_flips requires EITHER --model-shard (full URI) OR "
                        "(--model + --s3-prefix)")

    d = validate(_load(args.baseline, args.profile), model_df)
    if d.empty:
        print("VALID: flips are exact sign changes; non-flipped rows reconcile")
        return 0
    print(f"INVALID: {len(d)} problems")
    print(d.head(50).to_string())
    return 1


def _selftest():
    base = pd.DataFrame({"event_id": [1, 2], "construction": ["ew", "ew"],
                         "universe": ["all", "all"], "status": ["ok", "ok"],
                         "pnl": [0.01, -0.02]})
    # Good model with all expected bands
    good = pd.DataFrame({
        "event_id": [1, 2, 1, 2, 1, 2, 1, 2],
        "construction": ["ew"] * 8,
        "universe": ["all"] * 8,
        "status": ["ok"] * 8,
        "flip_band": ["2", "2", "3", "3", "4", "4", "5", "5"],
        "direction_rule": ["momentum_flip", "fade", "momentum_flip", "fade", "momentum_flip", "fade", "momentum_flip", "fade"],
        "pnl": [-0.01, -0.02, -0.01, -0.02, -0.01, -0.02, -0.01, -0.02]
    })
    # Original cases still pass
    assert validate(base, good).empty
    bad = good.copy(); bad.loc[0, "pnl"] = -0.012          # magnitude drift
    assert len(validate(base, bad)) >= 1
    bad2 = good.copy(); bad2.loc[1, "pnl"] = 0.02          # fade row flipped
    assert len(validate(base, bad2)) == 1

    # CRITICAL 1: Missing baseline cell in model (band 4 missing event_id 1)
    incomplete_model = pd.DataFrame({
        "event_id": [2, 2, 2, 2], "construction": ["ew"] * 4,
        "universe": ["all"] * 4, "status": ["ok"] * 4,
        "flip_band": ["2", "3", "4", "5"],
        "direction_rule": ["fade"] * 4,
        "pnl": [-0.02] * 4
    })
    missing_cell_result = validate(base, incomplete_model)
    assert len(missing_cell_result) >= 1
    assert any(r["problem"] == "missing_in_model" for _, r in missing_cell_result.iterrows())

    # CRITICAL 2: Missing band (only band 4, missing bands 2,3,5)
    missing_bands_model = pd.DataFrame({
        "event_id": [1, 2], "construction": ["ew", "ew"],
        "universe": ["all", "all"], "status": ["ok", "ok"],
        "flip_band": ["4", "4"],
        "direction_rule": ["momentum_flip", "fade"],
        "pnl": [-0.01, -0.02]
    })
    missing_band_result = validate(base, missing_bands_model)
    assert len(missing_band_result) >= 3  # 3 missing bands (2,3,5)
    assert any("missing_band" in str(r["problem"]) for _, r in missing_band_result.iterrows())

    # MINOR 3: NaN pnl on ok row
    nan_model = good.copy()
    nan_model.loc[0, "pnl"] = np.nan
    nan_result = validate(base, nan_model)
    assert len(nan_result) >= 1
    assert any(r["problem"] == "pnl_is_nan" for _, r in nan_result.iterrows())

    # --- IMPORTANT 5(a): mark columns are gated exactly like pnl -------------
    base_marks = base.assign(pnl_1000=[0.004, -0.008],
                             pnl_1530=[0.008, -0.015])
    good_marks = good.assign(
        pnl_1000=[-0.004, -0.008] * 4,     # flip row negated, fade row equal
        pnl_1530=[-0.008, -0.015] * 4)
    assert validate(base_marks, good_marks).empty, \
        "correctly negated/equal marks must pass"

    # a fade row whose mark drifts must FAIL, naming the column
    mark_drift = good_marks.copy()
    mark_drift.loc[1, "pnl_1000"] = -0.0079      # fade row, mark moved
    r_drift = validate(base_marks, mark_drift)
    assert len(r_drift) >= 1
    assert any("pnl_1000" in str(r["problem"]) and "non_flipped_drift" in str(r["problem"])
               for _, r in r_drift.iterrows()), \
        f"expected a pnl_1000 drift problem, got {r_drift['problem'].tolist()}"

    # a momentum row whose mark negates exactly must PASS (already covered by
    # good_marks) while a momentum row whose mark does NOT negate must FAIL
    mark_noflip = good_marks.copy()
    mark_noflip.loc[0, "pnl_1530"] = 0.008       # flip row kept baseline sign
    r_noflip = validate(base_marks, mark_noflip)
    assert any("pnl_1530" in str(r["problem"]) for _, r in r_noflip.iterrows())

    # --- IMPORTANT 5(b): skip-row pass --------------------------------------
    base_skip = pd.concat([base, pd.DataFrame([{
        "event_id": 3, "construction": "ew", "universe": "all",
        "status": "skip", "skip_reason": "no_admissible_peers", "pnl": None}])],
        ignore_index=True)
    good_skip = pd.concat([
        good.assign(skip_reason=None),
        pd.DataFrame([{"event_id": 3, "construction": "ew", "universe": "all",
                       "status": "skip", "flip_band": b,
                       "direction_rule": None, "pnl": None,
                       "skip_reason": "no_admissible_peers"} for b in BANDS_LIST])],
        ignore_index=True)
    assert validate(base_skip, good_skip).empty, "matching skip reasons must pass"

    bad_skip = good_skip.copy()
    bad_skip.loc[bad_skip["event_id"] == 3, "skip_reason"] = "neutral_band"
    r_skip = validate(base_skip, bad_skip)
    assert len(r_skip) >= 1
    assert any("skip_reason_mismatch" in str(r["problem"]) for _, r in r_skip.iterrows()), \
        f"expected skip_reason_mismatch, got {r_skip['problem'].tolist()}"

    # a baseline skip cell absent from a model band is flagged
    dropped_skip = good_skip[~((good_skip["event_id"] == 3)
                               & (good_skip["flip_band"] == "3"))].copy()
    r_dropped = validate(base_skip, dropped_skip)
    assert any("baseline_skip_missing_in_model" in str(r["problem"])
               for _, r in r_dropped.iterrows())

    print("selftest OK")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        _selftest()
        sys.exit(0)
    sys.exit(main())
