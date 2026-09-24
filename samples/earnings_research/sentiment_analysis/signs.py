"""The PERMANENT random-sign artifact (design spec §4).

Generated ONCE EVER; `generate` refuses to run if the S3 object exists and
no force flag exists. The canonical sha256 lives in configs/signs.sha256 —
plan refuses to submit and children refuse to evaluate on mismatch.
"""
import argparse
import hashlib
import io
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import study

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
N_SEEDS = 64


def paths():
    """(signs_uri, meta_uri, sha_path) for the current study."""
    s = study.current()
    uri = f"s3://{_bucket()}/{s.signs_key}"
    sha = Path(__file__).resolve().parent / "configs" / s.signs_sha_file
    return uri, uri.replace(".parquet", ".meta.json"), sha


def generate_frame(event_ids: List[int], n_seeds: int = N_SEEDS,
                   master_seed: int = None) -> pd.DataFrame:
    master_seed = study.current().master_seed if master_seed is None else master_seed
    rng = np.random.Generator(np.random.PCG64(master_seed))
    ev = sorted(int(e) for e in event_ids)
    draws = rng.integers(0, 2, size=(len(ev), n_seeds)) * 2 - 1   # {-1,+1}
    rows = [(e, s, int(draws[i, s]))
            for i, e in enumerate(ev) for s in range(n_seeds)]
    return pd.DataFrame(rows, columns=["event_id", "seed", "sign"]).astype(
        {"event_id": "int64", "seed": "int32", "sign": "int8"})


def sha256_of_frame(df: pd.DataFrame) -> str:
    """Canonical digest: CSV bytes of the sorted frame (stable across
    parquet writer versions — the parquet container is NOT the digest)."""
    canon = df.sort_values(["event_id", "seed"]).to_csv(index=False).encode()
    return hashlib.sha256(canon).hexdigest()


def cmd_generate(s3io, event_ids: List[int], n_seeds: int = N_SEEDS) -> str:
    signs_uri, meta_uri, sha_file = paths()
    if s3io.exists(signs_uri):
        raise SystemExit(f"REFUSED: {signs_uri} already exists — the signs "
                         "artifact is permanent (spec §4); no force flag exists")
    df = generate_frame(event_ids, n_seeds)
    digest = sha256_of_frame(df)
    import polars as pl
    s3io.write_parquet(pl.from_pandas(df), signs_uri)
    s3io.write_text(json.dumps({
        "master_seed": study.current().master_seed, "algorithm": "PCG64",
        "n_events": len(set(event_ids)), "n_seeds": n_seeds,
        "sha256": digest,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }, indent=2), meta_uri)
    sha_file.write_text(digest + "\n")
    print(f"signs: {len(df)} rows -> {signs_uri}\nsha256: {digest}")
    return digest


def load_signs(s3io) -> Dict[Tuple[int, int], int]:
    signs_uri, _, sha_file = paths()
    df = pd.read_parquet(io.BytesIO(s3io.read_bytes(signs_uri)))
    digest = sha256_of_frame(df)
    want = sha_file.read_text().strip()
    if digest != want:
        raise SystemExit(f"FATAL: signs.parquet sha256 {digest} != pinned {want}")
    return {(int(r.event_id), int(r.seed)): int(r.sign) for r in df.itertuples()}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("command", choices=["generate", "verify"])
    ap.add_argument("--universe-json", default=None)
    ap.add_argument("--profile", default=None)
    args = ap.parse_args()
    from s3io import S3IO
    s3io = S3IO(profile=args.profile)
    if args.command == "generate":
        if args.universe_json is None:
            event_ids = study.universe_event_ids(s3io, study.current())
        else:
            uni = json.loads(s3io.read_bytes(args.universe_json))
            event_ids = [e["event_id"] for e in uni["events"]]
        cmd_generate(s3io, event_ids)
        return 0
    load_signs(s3io)
    print("VERIFIED: signs.parquet matches pinned sha256")
    return 0


if __name__ == "__main__":
    sys.exit(main())
