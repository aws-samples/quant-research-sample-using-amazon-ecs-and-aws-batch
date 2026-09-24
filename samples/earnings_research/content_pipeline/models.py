"""Schema definitions for the pipeline's parquet artifacts.

Everything is parquet, Polars-native:
  - manifest shards: the SQL result (one row per ER event with a URL)
  - content shards:  manifest columns + fetch metadata + raw_content bytes
                     + extracted text_content

MANIFEST_SCHEMA and CONTENT_SCHEMA are the single source of truth; the
manifest builder and workers construct their DataFrames from them.
"""

from dataclasses import dataclass
from typing import Optional

import polars as pl

# ---------------------------------------------------------------- statuses

# fetch_status values (terminal outcomes; every one stays a row)
FETCH_OK = "ok"
FETCH_DNS_ERROR = "dns_error"
FETCH_CONNECT_ERROR = "connect_error"
FETCH_TLS_ERROR = "tls_error"
FETCH_TIMEOUT = "timeout"
FETCH_HTTP_404 = "http_404"
FETCH_HTTP_403 = "http_403"
FETCH_HTTP_429 = "http_429"
FETCH_HTTP_5XX = "http_5xx"
FETCH_HTTP_OTHER = "http_other"
FETCH_REDIRECT_LOOP = "redirect_loop"
FETCH_TOO_LARGE = "too_large"
FETCH_OTHER_ERROR = "other_error"
# HTTP 200, but the deep document path redirected to a site root / IR landing
# page — the document is gone and the body is chrome. See soft_404.py.
FETCH_SOFT_404 = "soft_404_redirect"

# text_extract_status values
EXTRACT_OK = "ok"
EXTRACT_PDF_ERROR = "pdf_error"
EXTRACT_PDF_ENCRYPTED = "pdf_encrypted"
EXTRACT_HTML_EMPTY = "html_empty"
EXTRACT_DECODE_ERROR = "decode_error"
EXTRACT_SKIPPED = "skipped_bad_type"
EXTRACT_NOT_ATTEMPTED = "not_attempted"

# ----------------------------------------------------------------- schemas

# Manifest: what the Redshift SQL yields, plus derived partition/shard cols.
MANIFEST_SCHEMA: dict = {
    "event_id": pl.Int64,
    "event_datetime_utc": pl.Utf8,
    "event_date": pl.Utf8,        # YYYY-MM-DD, derived
    "region": pl.Utf8,            # from ticker_region, 'UNKNOWN' fallback
    "url_pr": pl.Utf8,
    "factset_entity_id": pl.Utf8,
    "entity_proper_name": pl.Utf8,
    "ticker_region": pl.Utf8,
    "fiscal_period": pl.Utf8,
    "fiscal_year": pl.Int64,
    "shard": pl.Int32,
}

# Columns the worker adds to each manifest row.
FETCH_COLUMNS: dict = {
    "final_url": pl.Utf8,
    "fetch_status": pl.Utf8,
    "http_status": pl.Int32,
    "content_type_header": pl.Utf8,
    "sniffed_type": pl.Utf8,      # pdf | html | text | unknown
    "content_length": pl.Int64,
    "content_sha256": pl.Utf8,
    "raw_content": pl.Binary,     # original document bytes (None on failure)
    "text_content": pl.Utf8,      # extracted plain text (None on failure)
    "text_extract_status": pl.Utf8,
    "text_length": pl.Int64,
    "tls_insecure": pl.Boolean,
    "fetched_at": pl.Utf8,        # ISO timestamp
    "attempt": pl.Int32,
    "error_detail": pl.Utf8,
}

CONTENT_SCHEMA: dict = {**MANIFEST_SCHEMA, **FETCH_COLUMNS}


@dataclass
class FetchResult:
    """Outcome of one HTTP fetch (before extraction)."""

    fetch_status: str
    http_status: Optional[int] = None
    final_url: Optional[str] = None
    content_type_header: Optional[str] = None
    body: Optional[bytes] = None
    tls_insecure: bool = False
    attempt: int = 1
    error_detail: Optional[str] = None
