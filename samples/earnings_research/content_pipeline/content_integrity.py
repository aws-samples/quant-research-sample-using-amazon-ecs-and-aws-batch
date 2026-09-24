#!/usr/bin/env python3
"""Content-integrity classifier for earnings press-release text.

Ported from experiment_harness/tools/content_integrity.py @ d9e500c9 (feat/oss-sft-semis).

Flags two classes of contamination:
  1. suspect_content  — <min_keywords financial keywords AND no dollar/percentage figure
  2. future_dated     — text names a date >future_days after the event

Usage:
    python content_integrity.py --universe-parquet s3://.../large_tier.parquet \
        --out s3://.../large_tier_integrity_v1.parquet --profile <profile>
"""
from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
from io import BytesIO

import boto3
import pandas as pd

MONTHS = {m: i + 1 for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"])}

# Deliberately plain and auditable rather than clever: a real earnings release
# says "revenue" and prints a number. Both patterns are reported per event so a
# reader can re-derive every count.
FIN_KEYWORD = re.compile(
    r"(revenue|net income|earnings per share|diluted|gross margin|"
    r"operating income|guidance|quarter)", re.I)
FIGURE = re.compile(
    r"\$\s?[\d,]+(?:\.\d+)?\s*(?:million|billion|M\b|B\b)|\d+\.\d+\s*%", re.I)
DATE = re.compile(
    r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+"
    r"(\d{1,2}),?\s+(20\d{2})\b", re.I)


def dates_in(text: str) -> list[dt.date]:
    """All parseable dates in text order."""
    result = []
    for mon, day, year in DATE.findall(text):
        try:
            result.append(dt.date(int(year), MONTHS[mon[:3].lower()], int(day)))
        except ValueError:
            continue          # e.g. "Feb 30, 2021" — a parse artefact, not a date
    return result


def newest_date_in(text: str) -> dt.date | None:
    """The latest calendar date the text names, or None."""
    newest = None
    for mon, day, year in DATE.findall(text):
        try:
            cand = dt.date(int(year), MONTHS[mon[:3].lower()], int(day))
        except ValueError:
            continue          # e.g. "Feb 30, 2021" — a parse artefact, not a date
        if newest is None or cand > newest:
            newest = cand
    return newest


def dateline_date(text: str, head_chars: int = 1500) -> dt.date | None:
    """The EARLIEST date among the dates found in text[:head_chars], or None."""
    dates = dates_in(text[:head_chars])
    return min(dates) if dates else None


def index_page_like(text: str, event_date: dt.date, future_days: int = 365,
                    head_chars: int = 1500, min_future_dates: int = 5,
                    dateline_tolerance_days: int = 7) -> bool:
    """True if text looks like an index page (many future dates, no good dateline)."""
    all_dates = dates_in(text)
    future_dates = {d for d in all_dates if (d - event_date).days > future_days}
    n_future = len(future_dates)

    if n_future >= min_future_dates:
        return True

    if n_future >= 3:
        dl = dateline_date(text, head_chars)
        if dl is None or (dl - event_date).days > dateline_tolerance_days:
            return True

    return False


def audit_frame(df: pd.DataFrame, future_days: int = 365) -> pd.DataFrame:
    """audit() over a DataFrame; accepts event_datetime_utc or event_date.

    future_days is the same threshold classify()/audit_text() use, so the
    n_future_dates column stays consistent with the rule applied downstream.
    """
    rows = []
    for r in df.itertuples(index=False):
        text = getattr(r, "text_content", None) or ""
        raw = getattr(r, "event_datetime_utc", None) or getattr(r, "event_date")
        event_date = dt.date.fromisoformat(str(raw)[:10])
        newest = newest_date_in(text)
        all_dates = dates_in(text)
        future_dates = {d for d in all_dates
                        if newest and (d - event_date).days > future_days}
        dl = dateline_date(text)
        rows.append({"event_id": int(r.event_id), "symbol": r.symbol, "event_date": event_date,
                     "n_chars": len(text), "n_fin_keywords": len(FIN_KEYWORD.findall(text)),
                     "n_figures": len(FIGURE.findall(text)), "newest_date_in_text": newest,
                     "days_after_event": (newest - event_date).days if newest else None,
                     "n_future_dates": len(future_dates), "dateline_date": dl})
    return pd.DataFrame(rows)


def audit_text(text: str, event_date: dt.date, min_keywords: int = 3, future_days: int = 365, rule: str = "index") -> dict:
    text = text or ""
    n_kw, n_fig = len(FIN_KEYWORD.findall(text)), len(FIGURE.findall(text))
    newest = newest_date_in(text)
    days = (newest - event_date).days if newest else None
    suspect = n_kw < min_keywords and n_fig == 0

    if rule == "harness":
        future = days is not None and days > future_days
    else:  # rule == "index"
        future = index_page_like(text, event_date, future_days)

    return {"clean": not (suspect or future), "suspect_content": suspect, "future_dated": future,
            "n_fin_keywords": n_kw, "n_figures": n_fig, "days_after_event": days}


def classify(df: pd.DataFrame, min_keywords: int, future_days: int, rule: str = "index") -> pd.DataFrame:
    df = df.copy()
    df["suspect_content"] = (df.n_fin_keywords < min_keywords) & (df.n_figures == 0)

    if rule == "harness":
        df["future_dated"] = df.days_after_event.notna() & (df.days_after_event > future_days)
    else:  # rule == "index"
        # Use index_page_like logic: either many future dates or 3+ without good dateline
        many_future = df.n_future_dates >= 5
        # Compute dateline days after event manually
        dateline_days = df.apply(
            lambda r: (r.dateline_date - r.event_date).days if pd.notna(r.dateline_date) else None,
            axis=1
        )
        moderate_future_no_dateline = (df.n_future_dates >= 3) & (
            df.dateline_date.isna() | (dateline_days > 7)
        )
        df["future_dated"] = many_future | moderate_future_no_dateline

    df["excluded"] = df.suspect_content | df.future_dated
    df["exclusion_reason"] = ""
    df.loc[df.suspect_content, "exclusion_reason"] = "suspect_content"
    df.loc[df.future_dated, "exclusion_reason"] = "future_dated"
    both = df.suspect_content & df.future_dated
    df.loc[both, "exclusion_reason"] = "suspect_content+future_dated"
    return df


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--universe-parquet", required=True, help="s3:// universe parquet")
    ap.add_argument("--out", required=True, help="s3:// output parquet path")
    ap.add_argument("--profile", default=None)
    ap.add_argument("--min-keywords", type=int, default=3)
    ap.add_argument("--future-days", type=int, default=365)
    ap.add_argument("--rule", choices=["index", "harness"], default="index",
                    help="Rule for future_dated: index=dateline+many-dates, harness=newest>threshold")
    args = ap.parse_args()

    session = boto3.Session(profile_name=args.profile)
    s3 = session.client("s3")

    # Read input parquet from S3
    bucket, key = args.universe_parquet.replace("s3://", "").split("/", 1)
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_parquet(BytesIO(obj["Body"].read()))

    # Classify
    result = classify(audit_frame(df, args.future_days),
                      args.min_keywords, args.future_days, args.rule)
    result["year"] = result.event_date.apply(lambda d: d.year)

    # Write output parquet to S3
    out_bucket, out_key = args.out.replace("s3://", "").split("/", 1)
    buf = BytesIO()
    result.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=out_bucket, Key=out_key, Body=buf.read())

    # Print summary
    n_excluded = result.excluded.sum()
    n_suspect = result.suspect_content.sum()
    n_future = result.future_dated.sum()
    print(f"Classified {len(result)} events")
    print(f"Excluded: {n_excluded} ({n_excluded/len(result)*100:.1f}%)")
    print(f"  suspect_content: {n_suspect}")
    print(f"  future_dated: {n_future}")
    print(f"\nBy year:")
    by_year = result.groupby("year").agg({
        "event_id": "count",
        "excluded": "sum",
        "suspect_content": "sum",
        "future_dated": "sum"
    }).rename(columns={"event_id": "n_events"})
    print(by_year.to_string())
    print(f"\nWrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
