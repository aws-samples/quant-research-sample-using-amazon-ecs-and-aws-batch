"""Validation gates for the earnings sentiment analysis study (design spec §7).

Gate 1 (Arm R): verify direction == signs table & |pnl| magnitude invariance
Gate 2 (Arm S): verify direction == sign(score), score fidelity, skip reasons
Gate 3: verify identical pre-skip event set & row conservation across all shards

All gates return empty DataFrame when clean; problems DataFrame otherwise.
"""
import argparse
import io
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np
import study

sys.path.insert(0, str(Path(__file__).resolve().parent))
import settings  # noqa: E402


def gate1_signs(shards_R: Dict[int, pd.DataFrame],
                signs: Dict[Tuple[int, int], int]) -> pd.DataFrame:
    """Gate 1: verify Arm R direction fidelity and magnitude invariance.

    Every traded row's direction must equal signs[(event_id, seed)].
    Per-event |pnl| must be identical across all seeds (atol=1e-9).
    Per-event status/skip_reason must be identical across all seeds.

    Returns: DataFrame of problems (empty if clean).
    """
    problems = []

    # CRITICAL FIX 1: Check for NaN pnl on traded rows first
    for seed, df in shards_R.items():
        traded = df[df["status"] == "ok"].copy()
        for _, row in traded.iterrows():
            eid = int(row["event_id"])
            if pd.isna(row["pnl"]):
                problems.append({
                    "gate": "gate1_signs",
                    "issue": "nan_pnl_on_traded_row",
                    "seed": seed,
                    "event_id": eid,
                    "direction": row["direction"],
                })

    # Check direction fidelity for each shard
    for seed, df in shards_R.items():
        traded = df[df["status"] == "ok"].copy()
        for _, row in traded.iterrows():
            eid = int(row["event_id"])
            expected_sign = signs.get((eid, seed))
            if expected_sign is None:
                problems.append({
                    "gate": "gate1_signs",
                    "issue": "missing_sign",
                    "seed": seed,
                    "event_id": eid,
                    "direction": row["direction"],
                })
            elif int(row["direction"]) != expected_sign:
                problems.append({
                    "gate": "gate1_signs",
                    "issue": "direction_mismatch",
                    "seed": seed,
                    "event_id": eid,
                    "expected": expected_sign,
                    "actual": int(row["direction"]),
                })

    # CRITICAL FIX 2: Check cross-seed status consistency
    # Group by event_id and collect (status, skip_reason) from all seeds
    event_statuses: Dict[int, List[Tuple[int, str, Optional[str]]]] = {}
    for seed, df in shards_R.items():
        for _, row in df.iterrows():
            eid = int(row["event_id"])
            status = str(row["status"])
            skip_reason = row.get("skip_reason")
            skip_reason = str(skip_reason) if pd.notna(skip_reason) else None
            if eid not in event_statuses:
                event_statuses[eid] = []
            event_statuses[eid].append((seed, status, skip_reason))

    # All seeds for a given event must have identical status and skip_reason
    for eid, statuses in event_statuses.items():
        if len(statuses) < 2:
            continue
        ref_seed, ref_status, ref_skip = statuses[0]
        for seed, status, skip_reason in statuses[1:]:
            if status != ref_status or skip_reason != ref_skip:
                problems.append({
                    "gate": "gate1_signs",
                    "issue": "cross_seed_status_divergence",
                    "event_id": eid,
                    "seed_ref": ref_seed,
                    "seed_divergent": seed,
                    "status_ref": ref_status,
                    "status_divergent": status,
                    "skip_ref": ref_skip,
                    "skip_divergent": skip_reason,
                })

    # Check magnitude invariance across seeds
    # Group by event_id and collect |pnl| from all seeds
    event_magnitudes: Dict[int, List[Tuple[int, float]]] = {}
    for seed, df in shards_R.items():
        traded = df[df["status"] == "ok"].copy()
        for _, row in traded.iterrows():
            eid = int(row["event_id"])
            # Skip NaN values (already flagged above)
            if pd.isna(row["pnl"]):
                continue
            mag = abs(float(row["pnl"]))
            if eid not in event_magnitudes:
                event_magnitudes[eid] = []
            event_magnitudes[eid].append((seed, mag))

    # Check that all magnitudes for each event are the same (atol=1e-9)
    for eid, mags in event_magnitudes.items():
        if len(mags) < 2:
            continue
        ref_mag = mags[0][1]
        for seed, mag in mags[1:]:
            if not np.isclose(mag, ref_mag, atol=1e-9):
                problems.append({
                    "gate": "gate1_signs",
                    "issue": "magnitude_drift",
                    "event_id": eid,
                    "seed_ref": mags[0][0],
                    "seed_drift": seed,
                    "mag_ref": ref_mag,
                    "mag_drift": mag,
                    "delta": abs(mag - ref_mag),
                })

    return pd.DataFrame(problems)


def gate2_scores(shard_S: pd.DataFrame,
                 scores: Dict[int, Optional[float]],
                 neutral: float) -> pd.DataFrame:
    """Gate 2: verify Arm S direction and score fidelity.

    Traded rows (status='ok') must have:
    - direction == sign(score)
    - |score| > neutral

    Skipped rows must have:
    - |score| <= neutral -> skip_reason='neutral_score'
    - score is None -> skip_reason='no_score'

    Returns: DataFrame of problems (empty if clean).
    """
    problems = []

    for _, row in shard_S.iterrows():
        eid = int(row["event_id"])
        score = scores.get(eid)

        if row["status"] == "ok":
            # Traded row checks
            if score is None:
                problems.append({
                    "gate": "gate2_scores",
                    "issue": "traded_with_null_score",
                    "event_id": eid,
                    "direction": row["direction"],
                })
                continue

            if abs(score) <= neutral:
                problems.append({
                    "gate": "gate2_scores",
                    "issue": "traded_neutral_score",
                    "event_id": eid,
                    "score": score,
                    "neutral": neutral,
                    "direction": row["direction"],
                })
                continue

            expected_dir = 1 if score > 0 else -1
            actual_dir = int(row["direction"])
            if actual_dir != expected_dir:
                problems.append({
                    "gate": "gate2_scores",
                    "issue": "direction_mismatch",
                    "event_id": eid,
                    "score": score,
                    "expected": expected_dir,
                    "actual": actual_dir,
                })

        elif row["status"] == "skip":
            # Skip reason checks
            skip_reason = row.get("skip_reason")

            if score is None:
                if skip_reason != "no_score":
                    problems.append({
                        "gate": "gate2_scores",
                        "issue": "null_score_wrong_skip",
                        "event_id": eid,
                        "expected_skip": "no_score",
                        "actual_skip": skip_reason,
                    })
            elif abs(score) <= neutral:
                if skip_reason != "neutral_score":
                    problems.append({
                        "gate": "gate2_scores",
                        "issue": "neutral_score_wrong_skip",
                        "event_id": eid,
                        "score": score,
                        "neutral": neutral,
                        "expected_skip": "neutral_score",
                        "actual_skip": skip_reason,
                    })

    return pd.DataFrame(problems)


def expected_arm_s_models(csv_models, extra_arms, present) -> list:
    """Gate 3's expected Arm S set: every CSV model, plus each registry extra arm
    whose shard is already present. An extra arm that has not been evaluated yet
    is not a pairing failure; an unknown shard still is (gate3_pairing flags extras)."""
    extras = [m for m in sorted(extra_arms) if m in set(present)]
    return list(csv_models) + extras


def gate3_pairing(all_shards: List[pd.DataFrame],
                  shards_R: Optional[Dict[int, pd.DataFrame]] = None,
                  shards_S: Optional[List[pd.DataFrame]] = None,
                  expected_n_seeds: int = 64,
                  expected_models: Optional[List[str]] = None,
                  expected_n_events: int = None) -> pd.DataFrame:
    """Gate 3: verify row conservation and identical pre-skip event set.

    Every shard must contain all expected_n_events event_ids exactly once (row conservation).
    The pre-skip (data-viable) event set must be identical across all shards.

    CRITICAL FIX 3: Verify expected shard counts:
    - Arm R must have exactly expected_n_seeds (default 64) seed files, seeds 0..N-1
    - Arm S must have exactly len(expected_models) model files matching the list

    Data-viable = rows whose skip_reason NOT IN ('neutral_score', 'no_score').

    Returns: DataFrame of problems (empty if clean).
    """
    problems = []

    if not all_shards:
        return pd.DataFrame(problems)

    # CRITICAL FIX 3: Check expected shard counts
    if shards_R is not None:
        # Check we have exactly expected_n_seeds
        if len(shards_R) != expected_n_seeds:
            problems.append({
                "gate": "gate3_pairing",
                "issue": "arm_r_shard_count_mismatch",
                "expected_seeds": expected_n_seeds,
                "actual_seeds": len(shards_R),
                "missing_or_extra": "missing" if len(shards_R) < expected_n_seeds else "extra",
            })

        # Check we have seeds 0..expected_n_seeds-1
        expected_seeds = set(range(expected_n_seeds))
        actual_seeds = set(shards_R.keys())
        missing_seeds = expected_seeds - actual_seeds
        extra_seeds = actual_seeds - expected_seeds

        if missing_seeds:
            problems.append({
                "gate": "gate3_pairing",
                "issue": "arm_r_missing_seeds",
                "missing_seeds": sorted(missing_seeds)[:20],  # limit output
                "n_missing": len(missing_seeds),
            })

        if extra_seeds:
            problems.append({
                "gate": "gate3_pairing",
                "issue": "arm_r_extra_seeds",
                "extra_seeds": sorted(extra_seeds)[:20],
                "n_extra": len(extra_seeds),
            })

    if shards_S is not None and expected_models is not None:
        # Check we have exactly len(expected_models) shards
        if len(shards_S) != len(expected_models):
            problems.append({
                "gate": "gate3_pairing",
                "issue": "arm_s_shard_count_mismatch",
                "expected_models": len(expected_models),
                "actual_shards": len(shards_S),
                "missing_or_extra": "missing" if len(shards_S) < len(expected_models) else "extra",
            })

        # Extract model names from shards
        actual_models = set()
        for shard in shards_S:
            if "sentiment_model" in shard.columns and not shard.empty:
                actual_models.add(shard["sentiment_model"].iloc[0])

        expected_model_set = set(expected_models)
        missing_models = expected_model_set - actual_models
        extra_models = actual_models - expected_model_set

        if missing_models:
            problems.append({
                "gate": "gate3_pairing",
                "issue": "arm_s_missing_models",
                "missing_models": sorted(missing_models)[:20],
                "n_missing": len(missing_models),
            })

        if extra_models:
            problems.append({
                "gate": "gate3_pairing",
                "issue": "arm_s_extra_models",
                "extra_models": sorted(extra_models)[:20],
                "n_extra": len(extra_models),
            })

    # Check row conservation: each shard must have all event_ids exactly once
    shard_events = []

    for i, shard in enumerate(all_shards):
        event_ids = set(shard["event_id"].astype(int))
        shard_events.append((i, event_ids))

        if len(shard) != expected_n_events:
            problems.append({
                "gate": "gate3_pairing",
                "issue": "row_count_mismatch",
                "shard_idx": i,
                "expected_rows": expected_n_events,
                "actual_rows": len(shard),
            })

        if len(event_ids) != expected_n_events:
            problems.append({
                "gate": "gate3_pairing",
                "issue": "unique_events_mismatch",
                "shard_idx": i,
                "expected_unique": expected_n_events,
                "actual_unique": len(event_ids),
            })

        # Check for duplicates within shard
        dup_counts = shard["event_id"].value_counts()
        duplicates = dup_counts[dup_counts > 1]
        if not duplicates.empty:
            for eid, count in duplicates.items():
                problems.append({
                    "gate": "gate3_pairing",
                    "issue": "duplicate_event",
                    "shard_idx": i,
                    "event_id": int(eid),
                    "count": int(count),
                })

    # Check data-viability consistency across shards.
    # An event arm-skipped in S (neutral_score/no_score) short-circuits BEFORE
    # the data checks (reporter_no_open_bar, reporter_no_exit, construction:*),
    # so its data-viability is unknowable — it is excluded from the comparison,
    # not counted as a mismatch. On the remaining (comparable) events, the
    # traded set must be identical across all shards: same panels + same code
    # means data-viability cannot depend on the arm or the seed.
    viable_sets = []
    for i, shard in enumerate(all_shards):
        arm_skipped = set(shard[shard["skip_reason"]
                                .isin(["neutral_score", "no_score"])]
                          ["event_id"].astype(int))
        traded = set(shard[shard["status"] == "ok"]["event_id"].astype(int))
        viable_sets.append((i, traded, arm_skipped))

    # All traded sets must agree on the events both shards actually data-checked
    if len(viable_sets) > 1:
        ref_idx, ref_traded, ref_arm_skipped = viable_sets[0]
        for i, traded, arm_skipped in viable_sets[1:]:
            excluded = ref_arm_skipped | arm_skipped
            missing = (ref_traded - traded) - excluded
            extra = (traded - ref_traded) - excluded

            if missing:
                problems.append({
                    "gate": "gate3_pairing",
                    "issue": "viable_set_mismatch",
                    "ref_shard": ref_idx,
                    "compare_shard": i,
                    "missing_from_compare": sorted(missing)[:10],  # limit output
                    "n_missing": len(missing),
                })

            if extra:
                problems.append({
                    "gate": "gate3_pairing",
                    "issue": "viable_set_mismatch",
                    "ref_shard": ref_idx,
                    "compare_shard": i,
                    "extra_in_compare": sorted(extra)[:10],  # limit output
                    "n_extra": len(extra),
                })

    return pd.DataFrame(problems)


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


def cmd_gate1(args) -> int:
    """Run Gate 1: verify Arm R signs fidelity."""
    from s3io import S3IO
    from signs import load_signs

    s3io = S3IO(profile=args.profile)
    signs = load_signs(s3io)

    bucket = settings.get("s3", "data_bucket")
    prefix = f"{study.current().results_prefix}/shards/arm=R/"

    print(f"Loading Arm R shards from s3://{bucket}/{prefix}")
    shards = _load_arm_r_shards(s3io, bucket, prefix)
    print(f"Loaded {len(shards)} shards (seeds: {sorted(shards.keys())[:5]}...)")

    print("Running Gate 1 (signs fidelity)...")
    problems = gate1_signs(shards, signs)

    if problems.empty:
        print("PASS: Gate 1 clean")
        return 0
    else:
        print(f"FAIL: Gate 1 found {len(problems)} problems")
        print(problems.head(50).to_string())
        return 1


def cmd_gate2(args) -> int:
    """Run Gate 2: verify Arm S score fidelity."""
    from s3io import S3IO
    from scores import load_scores

    s3io = S3IO(profile=args.profile)

    bucket = settings.get("s3", "data_bucket")
    prefix = f"{study.current().results_prefix}/shards/arm=S/"

    print(f"Loading Arm S shards from s3://{bucket}/{prefix}")
    shards = _load_arm_s_shards(s3io, bucket, prefix)
    print(f"Loaded {len(shards)} shards")

    # For each shard, extract the model name and load its scores
    all_problems = []
    for shard in shards:
        model = shard["sentiment_model"].iloc[0] if "sentiment_model" in shard.columns else "unknown"
        neutral = shard["neutral_threshold"].iloc[0] if "neutral_threshold" in shard.columns else 1.0

        print(f"Checking model {model} (neutral={neutral})...")
        scores_model, scores_job = study.arm_s_source(study.current(), model)
        scores = load_scores(s3io, scores_model, scores_job)
        problems = gate2_scores(shard, scores, neutral)
        if not problems.empty:
            problems["model"] = model
            all_problems.append(problems)

    if not all_problems:
        print("PASS: Gate 2 clean")
        return 0
    else:
        combined = pd.concat(all_problems, ignore_index=True)
        print(f"FAIL: Gate 2 found {len(combined)} problems")
        print(combined.head(50).to_string())
        return 1


def cmd_gate3(args) -> int:
    """Run Gate 3: verify pairing across all shards."""
    from s3io import S3IO
    from scores import load_model_list
    import signs as signs_mod

    s3io = S3IO(profile=args.profile)

    bucket = settings.get("s3", "data_bucket")
    st = study.current()

    # Load expected configurations
    expected_n_seeds = signs_mod.N_SEEDS
    models_csv = str(Path(__file__).parent / "configs" / st.models_csv)
    expected_n_events = len(study.universe_event_ids(s3io, st))

    # Load both arms
    print("Loading all shards...")
    r_shards = _load_arm_r_shards(s3io, bucket,
                                   f"{st.results_prefix}/shards/arm=R/")
    s_shards = _load_arm_s_shards(s3io, bucket,
                                   f"{st.results_prefix}/shards/arm=S/")

    present = [sh["sentiment_model"].dropna().iloc[0] for sh in s_shards
               if "sentiment_model" in sh.columns and sh["sentiment_model"].notna().any()]
    expected_models = expected_arm_s_models(load_model_list(models_csv), st.extra_arms, present)
    absent = sorted(set(st.extra_arms) - set(present))
    if absent:
        print(f"note: extra arms not yet evaluated (not required): {absent}")

    all_shards = list(r_shards.values()) + s_shards
    print(f"Loaded {len(r_shards)} Arm R shards + {len(s_shards)} Arm S shards = {len(all_shards)} total")
    print(f"Expected: {expected_n_seeds} seeds, {len(expected_models)} models, {expected_n_events} events")

    print("Running Gate 3 (pairing + shard counts)...")
    problems = gate3_pairing(all_shards, shards_R=r_shards, shards_S=s_shards,
                            expected_n_seeds=expected_n_seeds,
                            expected_models=expected_models,
                            expected_n_events=expected_n_events)

    if problems.empty:
        print("PASS: Gate 3 clean")
        return 0
    else:
        print(f"FAIL: Gate 3 found {len(problems)} problems")
        print(problems.head(50).to_string())
        return 1


def cmd_all(args) -> int:
    """Run all three gates in sequence."""
    failures = []

    print("=" * 70)
    print("GATE 1: Signs fidelity (Arm R)")
    print("=" * 70)
    ret1 = cmd_gate1(args)
    if ret1 != 0:
        failures.append("gate1")

    print("\n" + "=" * 70)
    print("GATE 2: Score fidelity (Arm S)")
    print("=" * 70)
    ret2 = cmd_gate2(args)
    if ret2 != 0:
        failures.append("gate2")

    print("\n" + "=" * 70)
    print("GATE 3: Pairing (all shards)")
    print("=" * 70)
    ret3 = cmd_gate3(args)
    if ret3 != 0:
        failures.append("gate3")

    print("\n" + "=" * 70)
    if not failures:
        print("ALL GATES PASSED")
        return 0
    else:
        print(f"FAILED: {', '.join(failures)}")
        return 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("gate", choices=["gate1", "gate2", "gate3", "all"])
    ap.add_argument("--profile", default=None,
                    help="AWS profile name (default: standard credential chain)")
    args = ap.parse_args()

    if args.gate == "gate1":
        return cmd_gate1(args)
    elif args.gate == "gate2":
        return cmd_gate2(args)
    elif args.gate == "gate3":
        return cmd_gate3(args)
    elif args.gate == "all":
        return cmd_all(args)

    return 1


if __name__ == "__main__":
    sys.exit(main())
