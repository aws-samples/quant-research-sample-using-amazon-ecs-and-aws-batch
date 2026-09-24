"""Loop-1 gate (spec §9 Phase 1): the sentiment-plumbed baseline must
reproduce results-xle-full PER EVENT — same P&L per (event, construction,
universe) cell to float precision, same skip reasons, same event counts.
Run in a write-code-until-green loop; NO Phase 2 work until exit 0.

Comparison keys: event_id, construction, universe. Compared: status,
skip_reason, pnl (atol 1e-9), direction, all pnl_HHMM marks.
Reference rows are the legacy schema; candidate rows may carry the
sentiment columns (ignored here — only shared columns are compared).
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


def reconcile(ref: pd.DataFrame, cand: pd.DataFrame) -> pd.DataFrame:
    """Empty frame = reconciled. Otherwise one row per difference."""
    shared_val_cols = [c for c in ref.columns
                       if c in cand.columns and c not in KEYS
                       and (c.startswith("pnl") or c in
                            ("status", "skip_reason", "direction"))]
    m = ref.merge(cand, on=KEYS, how="outer", suffixes=("_ref", "_new"),
                  indicator=True)
    diffs = []
    for _, r in m.iterrows():
        if r["_merge"] != "both":
            diffs.append({**{k: r[k] for k in KEYS},
                          "column": "_presence", "ref": str(r["_merge"]), "new": ""})
            continue
        for c in shared_val_cols:
            a, b = r[f"{c}_ref"], r[f"{c}_new"]
            if pd.isna(a) and pd.isna(b):
                continue
            if isinstance(a, (int, float, np.floating)) and isinstance(b, (int, float, np.floating)):
                if pd.isna(a) != pd.isna(b) or abs(float(a) - float(b)) > ATOL:
                    diffs.append({**{k: r[k] for k in KEYS}, "column": c,
                                  "ref": a, "new": b})
            elif a != b:
                diffs.append({**{k: r[k] for k in KEYS}, "column": c,
                              "ref": a, "new": b})
    return pd.DataFrame(diffs)


def _load(uri: str, profile):
    if uri.startswith("s3://"):
        from s3io import S3IO
        return pd.read_parquet(io.BytesIO(S3IO(profile=profile).read_bytes(uri)))
    return pd.read_parquet(uri)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reference", required=True)
    ap.add_argument("--candidate", required=True)
    ap.add_argument("--profile", default=None)
    args = ap.parse_args()
    d = reconcile(_load(args.reference, args.profile),
                  _load(args.candidate, args.profile))
    if d.empty:
        print("RECONCILED: candidate matches reference")
        return 0
    print(f"NOT RECONCILED: {len(d)} differences")
    print(d.head(50).to_string())
    return 1


def _selftest():
    ref = pd.DataFrame({"event_id": [1, 1], "construction": ["ew", "ew"],
                        "universe": ["all", "pure play"],
                        "status": ["ok", "ok"], "skip_reason": [None, None],
                        "direction": [1, 1], "pnl": [0.01, 0.02]})
    assert reconcile(ref, ref.copy()).empty
    bad = ref.copy(); bad.loc[0, "pnl"] = 0.011
    assert len(reconcile(ref, bad)) == 1
    missing = ref.iloc[[0]]
    assert (reconcile(ref, missing)["column"] == "_presence").any()
    print("selftest OK")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        _selftest()
        sys.exit(0)
    sys.exit(main())
