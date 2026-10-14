"""Shared helpers of the imbalance_mft sample: S3 access (Polars / s3fs straight from S3, no local copies), AWS Batch submission and polling, the
dataset's universe / grid helpers and the 14 moving-average windows of the trade-flow dataset. Every account-specific value comes from config.json
through settings.py, read at the point of use (nothing is read at import time)."""
import json, os, random, time
from datetime import datetime, timezone
import numpy as np, polars as pl
import settings

THROTTLE = ("SlowDown", "503", "Service Unavailable", "RequestTimeout", "InternalError", "timed out", "Timeout")
MA_WINDOWS = [3, 5, 10, 20, 30, 40, 50, 75, 100, 200, 500, 1000, 5000, 10000]   # the dataset's imbalance MA windows (bars)
POLL_S = 60; TERMINAL = ("SUCCEEDED", "FAILED")


def bucket(): return f"s3://{settings.get('s3', 'data_bucket')}"
def out_root(): return f"{bucket()}/{settings.get('s3', 'dataset_prefix')}"            # <run_id>/... lives under here
def region(): return settings.get("aws", "region")
def queue(): return settings.get("batch", "job_queue")
def big_queue(): return settings.get("batch", "big_queue")
def job_def(): return settings.get("batch", "job_definition")
def mem_cap(): return int(settings.get("batch", "mem_cap_mib"))
def big_cap(): return int(settings.get("batch", "big_cap_mib"))
def controller(): return tuple(settings.get("batch", "resources", "controller"))
def tier_of(stage): return tuple(settings.get("batch", "resources", stage))
def so():
    """Polars / s3fs storage options: the configured profile on a laptop, the task role (None) on Batch."""
    prof = None if os.environ.get("AWS_BATCH_JOB_ID") else settings.get("aws", "profile"); return {"profile": prof} if prof else None


def fs():
    import s3fs; o = so(); return s3fs.S3FileSystem(**({"profile": o["profile"]} if o else {}))


def _key(p): return p.replace("s3://", "")


def read_json(path):
    with fs().open(_key(path)) as fh: return json.load(fh)


def write_json(path, doc):
    with fs().open(_key(path), "w") as fh: json.dump(doc, fh, separators=(",", ":"))


def put_parquet(df, path, attempts=12, base=0.5, cap=60.0):
    """Serialise to parquet in memory and upload with ONE S3 put, exponential backoff on throttling (thousands of array children writing into fresh prefixes draw 503 SlowDown)."""
    import io
    buf = io.BytesIO(); df.write_parquet(buf); data = buf.getvalue()
    for k in range(attempts):
        try: return fs().pipe(_key(path), data)
        except Exception as e:
            if k == attempts - 1 or not any(t in str(e) for t in THROTTLE): raise
            time.sleep(min(cap, base * 2 ** k) * (0.5 + random.random()))


def _read_or_none(path):
    f = fs(); return pl.read_parquet(f"s3://{_key(path)}", storage_options=so()) if f.exists(_key(path)) else None


def prev_date(d):
    from datetime import date, timedelta
    return (date.fromisoformat(d) - timedelta(days=1)).isoformat()


def _idx(a): return a.index if a.index is not None else int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX", "0"))


def batch_client():
    import boto3
    from botocore.config import Config
    o = so(); return boto3.Session(profile_name=o["profile"] if o else None).client("batch", region_name=region(), config=Config(retries={"max_attempts": 5, "mode": "adaptive"}, connect_timeout=20))


def job_spec(name, cmd, vcpus, mem_mib, hours, array_size=None, depends_on=None):
    job = {"jobName": name[:128], "jobQueue": queue(), "jobDefinition": job_def(),
           "containerOverrides": {"command": list(cmd), "resourceRequirements": [{"type": "VCPU", "value": str(vcpus)}, {"type": "MEMORY", "value": str(mem_mib)}]},
           "retryStrategy": {"attempts": 3, "evaluateOnExit": [{"onStatusReason": "Host EC2*", "action": "RETRY"}, {"onReason": "*", "action": "EXIT"}]},
           "timeout": {"attemptDurationSeconds": int(hours * 3600)}}
    if array_size and array_size > 1: job["arrayProperties"] = {"size": array_size}
    if depends_on: job["dependsOn"] = [{"jobId": j} for j in depends_on]
    return job


def shape(vcpus, mem_mib):
    """vCPUs for a memory ask on the family queue: m6i.xlarge steps (4 vCPU per 32 GB), never below the planned count."""
    return max(vcpus, 4 * -(-mem_mib // 32000))


def status_of(j):
    return "UNPLACEABLE" if j["status"] == "RUNNABLE" and (j.get("statusReason") or "").startswith("MISCONFIGURATION") else j["status"]


def describe(batch, ids):
    out = []
    for k in range(0, len(ids), 100): out += batch.describe_jobs(jobs=ids[k:k + 100])["jobs"]
    return out


def live_jobs(batch, ids):
    return [j["jobId"] for j in describe(batch, ids) if status_of(j) not in TERMINAL + ("UNPLACEABLE",)] if ids else []


def wait_jobs(batch, ids, on_tick=None, poll_s=POLL_S):
    while True:
        js = describe(batch, ids); st = {j["jobId"]: {"status": status_of(j), "summary": {k: v for k, v in (j.get("arrayProperties", {}).get("statusSummary") or {}).items() if v}} for j in js}
        for i in ids: st.setdefault(i, {"status": "GONE", "summary": {}})
        if on_tick: on_tick(st)
        if all(v["status"] in TERMINAL + ("GONE", "UNPLACEABLE") for v in st.values()): return {k: v["status"] for k, v in st.items()}
        time.sleep(poll_s)


def controller_state(st, job_id):
    st = dict(st); st.update({"job_id": job_id, "phase": "running", "error": None, "started_at": st.get("started_at") or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}); return st


def watch_over(c, cjob, cstat):
    if cjob and cstat in TERMINAL: return True
    if cjob and c and c.get("phase") in ("done", "failed") and c.get("job_id") == cjob: return True
    return bool(c and c.get("phase") in ("done", "failed") and not cjob)


# ---- dataset universe / grid (from the cross-coin flow model)
EQUITY_TICKERS = {"SNDK", "SOXL", "CL", "SPCX", "SKHYNIX", "SKHX", "SKHY", "MU", "AVGO", "ANTHROPIC", "OPENAI", "NVDA", "TSLA", "AAPL", "META", "GOOGL", "AMD", "INTC", "HOOD", "MSTR",
                  "CRCL", "SMSN", "NBIS", "EWY", "XYZ100", "SP500", "BRENTOIL", "GOLD", "SILVER", "DRAM", "SPY", "QQQ", "COIN", "PLTR", "AMZN", "MSFT", "NFLX", "ORCL", "UNH", "NKE", "MCD",
                  "TSM", "ASML", "ARM", "SMCI", "RDDT", "GME", "AMC", "BABA", "PDD", "JPM", "GS", "BRK", "XOM", "CVX", "COP", "BZ", "NG", "USOIL", "WTI", "XAG", "XAU", "XPT", "XPD"}


def target_coins(U, top=None):
    """Target coins from the universe table: every coin except BTC / ETH and EQUITY_TICKERS, kind = perp if the coin has a perp series else spot, by pooled ADV."""
    X = U.filter(pl.col("in_universe") & ~pl.col("coin").is_in(["BTC", "ETH"]) & ~pl.col("coin").is_in(sorted(EQUITY_TICKERS)) & ~pl.col("coin").str.contains(":"))
    K = X.group_by("coin").agg(pl.col("coin_adv_usd").first().alias("adv"), pl.col("kind").unique().alias("kinds"))
    K = K.with_columns(pl.when(pl.col("kinds").list.contains("perp")).then(pl.lit("perp")).otherwise(pl.lit("spot")).alias("kind")).sort("adv", descending=True).select("coin", "kind", "adv")
    return K.head(top) if top else K


def day0_us(date):
    import datetime as dt
    d = dt.date.fromisoformat(date); return int(dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc).timestamp() * 1_000_000)


def grid_t(date, step_s=10):
    d0 = day0_us(date); return np.arange(d0, d0 + 86_400_000_000, int(step_s) * 1_000_000, dtype=np.int64)
