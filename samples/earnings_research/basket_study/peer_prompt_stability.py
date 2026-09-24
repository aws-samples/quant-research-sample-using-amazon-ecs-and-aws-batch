"""peer_prompt_stability — how stable is the peer-derivation prompt across
repeated calls and across models?

Runs earnings_market_data/peer_deriver's EXACT prompt (same system text, same
user prompt, temperature 0, maxTokens 800) N times per model for one reporter
and compares the returned pure-play sets (other buckets are kept in the raw
output too).

    AWS_PROFILE=<profile> python peer_prompt_stability.py --symbol GPRO \
        --name "GoPro, Inc." --n 100 --models backtest,sonnet,opus,fable

Writes <s3-prefix>/diagnostics/peer_stability/prompt_runs_<SYM>.parquet (one
row per call: model, run, bucket lists, latency) and prints the comparison.
"""
import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
import pandas as pd
import polars as pl

import settings  # noqa: E402
sys.path.insert(0, str(settings.sibling("market_data")))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from peer_deriver import _PROMPT, _SYSTEM, MODEL_ID as BACKTEST_MODEL, PeerDeriver  # noqa: E402
import aggregate  # noqa: E402
import settings
from s3io import S3IO  # noqa: E402

MODELS = {"backtest": BACKTEST_MODEL,                      # what the backtest used (Sonnet 4.5)
          "sonnet": "us.anthropic.claude-sonnet-5",
          "opus": "us.anthropic.claude-opus-5",
          "fable": "us.anthropic.claude-fable-5-1"}
RETRY = {"ThrottlingException", "TooManyRequestsException", "ServiceUnavailableException",
         "InternalServerError", "ModelTimeoutException", "ModelNotReadyException"}


def call(brt, model_id, prompt, run):
    delay = 1.0
    for attempt in range(8):
        t = time.time()
        try:
            # backtest model: exact pipeline config. Newer models: `temperature` is rejected
            # (deprecated) and 800 tokens are consumed by reasoning blocks before the text
            # (observed: empty/truncated JSON), so they get a larger budget. Prompt is identical.
            cfg = {"maxTokens": 800, "temperature": 0.0} if model_id == BACKTEST_MODEL else {"maxTokens": 6000}
            resp = brt.converse(modelId=model_id, system=[{"text": _SYSTEM}],
                                messages=[{"role": "user", "content": [{"text": prompt}]}],
                                inferenceConfig=cfg)
            blocks = resp["output"]["message"]["content"]
            text = " ".join(b.get("text", "") for b in blocks if b.get("text"))
            parsed = PeerDeriver._parse(text)
            norm = {k: sorted(x if isinstance(x, str) else x.get("ticker", "?") for x in v) for k, v in parsed.items()}
            return {"run": run, "ok": True, "latency_s": time.time() - t, "raw": text,
                    "stop_reason": resp.get("stopReason"), "n_blocks": len(blocks),
                    "n_text_blocks": sum(1 for b in blocks if b.get("text")),
                    "out_tokens": resp.get("usage", {}).get("outputTokens"),
                    "pure_play": " ".join(norm.get("pure play", [])), "functional": " ".join(norm.get("functional", [])),
                    "correlated": " ".join(norm.get("correlated", []))}
        except Exception as e:
            code = type(e).__name__
            if code in RETRY and attempt < 7:
                time.sleep(delay); delay = min(delay * 2, 20); continue
            return {"run": run, "ok": False, "error": f"{code}: {e}"[:200]}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--symbol", default="GPRO"); ap.add_argument("--name", default="GoPro, Inc.")
    ap.add_argument("--n", type=int, default=100); ap.add_argument("--models", default="backtest,sonnet,opus,fable")
    ap.add_argument("--workers", type=int, default=6, help="concurrent calls per model")
    ap.add_argument("--append", action="store_true", help="keep rows of models not re-run from the existing file")
    ap.add_argument("--s3-prefix", default="earnings-basket-study/results-10y"); ap.add_argument("--profile", default=None)
    args = ap.parse_args()
    ses = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    brt = ses.client("bedrock-runtime", region_name="us-east-1",
                     config=boto3.session.Config(max_pool_connections=64, read_timeout=120))
    prompt = _PROMPT.format(symbol=args.symbol, name=args.name)
    rows = []
    t0 = time.time()
    with ThreadPoolExecutor(args.workers * len(args.models.split(","))) as ex:
        futs = {}
        for key in args.models.split(","):
            for i in range(args.n):
                futs[ex.submit(call, brt, MODELS[key], prompt, i)] = key
        done = 0
        for f in as_completed(futs):
            r = f.result(); r["model"] = futs[f]; r["model_id"] = MODELS[futs[f]]; rows.append(r); done += 1
            if done % 50 == 0:
                print(f"  {done}/{len(futs)} calls, {time.time() - t0:.0f}s", flush=True)
    df = pd.DataFrame(rows).sort_values(["model", "run"])
    s3io = S3IO(profile=args.profile)
    key = f"s3://{settings.get("s3", "data_bucket")}/{args.s3_prefix}/diagnostics/peer_stability/prompt_runs_{args.symbol}.parquet"
    if args.append:
        try:
            old = s3io.read_parquet(key).to_pandas()
            df = pd.concat([old[~old.model.isin(df.model.unique())], df], ignore_index=True)
        except Exception as e:
            print(f"append: no previous file ({e.__class__.__name__})")
    s3io.write_parquet(pl.from_pandas(df.astype({"raw": str}) if "raw" in df else df), key)
    print(f"wrote {key}")
    pd.set_option("display.width", 250)
    for key_ in sorted(df.model.unique(), key=lambda k: list(MODELS).index(k)):
        d = df[(df.model == key_)]
        ok = d[d.ok == True]
        print(f"\n=== {key_} ({MODELS[key_]}): {len(ok)}/{len(d)} ok, median latency {ok.latency_s.median():.1f}s")
        if not len(ok):
            print(d.error.value_counts().head(3)); continue
        vc = ok.pure_play.value_counts()
        print("pure-play sets:"); print(vc.to_string())
        names = pd.Series([n for s in ok.pure_play for n in s.split()]).value_counts() / len(ok) * 100
        print("per-name inclusion %:", names.round(0).astype(int).to_dict())
        print("functional distinct sets:", ok.functional.nunique(), "| correlated distinct sets:", ok.correlated.nunique())
        if "stop_reason" in ok:
            print("stop reasons:", ok.stop_reason.value_counts().to_dict(), "| empty pure-play:", int((ok.pure_play == "").sum()),
                  "| median output tokens:", ok.out_tokens.median())


if __name__ == "__main__":
    main()
