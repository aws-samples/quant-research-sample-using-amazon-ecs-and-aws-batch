"""Databento historical client — promoted from the validated prototype
(earnings_content_pipeline/proto_market_data.py, kept as reference).

ohlcv-1m on a consolidated US-equities dataset. Key from Secrets Manager.
Free metadata (get_cost) vs paid data (get_range). Prices are
Databento fixed-point (1e-9) → scaled to dollars; ts_event is UTC ns.

Dataset is configurable (constructor arg or EMD_DATASET env), default
XNAS.BASIC. We migrated off DBEQ.BASIC: it carried NO extended hours and
under-reported volume; XNAS.BASIC gives full 04:00-20:00 ET coverage from the
major lit venues.
"""

import io
import json
import logging
import os
import time
from typing import Optional

import boto3
import pandas as pd
import requests

import settings

logger = logging.getLogger(__name__)

DEFAULT_DATASET = "XNAS.BASIC"      # consolidated + full extended hours + statistics
SCHEMA = "ohlcv-1m"
_BASE = "https://hist.databento.com/v0/"
_RETRY_STATUS = {429, 500, 502, 503, 504}
_MAX_ATTEMPTS = 5


class TransientFetchError(Exception):
    """get_range returned fewer rows than the API's record_count — a transient
    empty/truncated body, NOT a genuine no-data result. Caller should retry."""



def _422_case(response) -> str:
    """Databento 422 detail.case. 422 is OVERLOADED: only
    symbology_invalid_request means 'no such symbol'. Anything else (notably
    data_end_after_available_end — the Jul 28-31 2026 stub-panel bug) must
    never be treated as an empty result."""
    try:
        return json.loads(response.text)["detail"]["case"]
    except Exception:
        return ""


class DatabentoClient:
    def __init__(self, boto3_session: Optional[boto3.Session] = None,
                 api_key: Optional[str] = None, dataset: Optional[str] = None):
        # dataset precedence: explicit arg > EMD_DATASET env > DEFAULT_DATASET
        self.dataset = dataset or os.environ.get("EMD_DATASET") or DEFAULT_DATASET
        if api_key:
            self._key = api_key
        else:
            sess = boto3_session or boto3.Session()
            sm = sess.client("secretsmanager")
            self._key = json.loads(
                sm.get_secret_value(SecretId=settings.get("secrets", "databento"))["SecretString"])["api_key"]
        logger.info("DatabentoClient dataset=%s schema=%s", self.dataset, SCHEMA)

    def _get(self, endpoint: str, params: dict, stream=False):
        """GET with retry+backoff on transient 5xx/429 and connection errors."""
        delay = 2.0
        last = None
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                r = requests.get(_BASE + endpoint, auth=(self._key, ""),
                                params=params, timeout=120, stream=stream)
                if r.status_code in _RETRY_STATUS and attempt < _MAX_ATTEMPTS:
                    time.sleep(delay); delay = min(delay * 2, 60)
                    continue
                return r
            except requests.exceptions.RequestException as e:
                last = e
                if attempt < _MAX_ATTEMPTS:
                    time.sleep(delay); delay = min(delay * 2, 60)
                    continue
                raise
        return last  # unreachable

    def is_active(self, symbol: str, start: str, end: str) -> Optional[bool]:
        """Free: does `symbol` have any ohlcv-1m records in [start, end)?

        True/False when the metadata call succeeds; None on transient error
        (caller should fail-open on None, never dropping on uncertainty). Used
        to detect dead/renamed tickers (e.g. BK after the BNY rename returns 0).
        """
        try:
            r = self._get("metadata.get_record_count", {
                "dataset": self.dataset, "symbols": symbol, "schema": SCHEMA,
                "start": start, "end": end, "stype_in": "raw_symbol"})
            if r.status_code == 422:      # symbology_invalid_request
                return False
            r.raise_for_status()
            return int(r.text.strip()) > 0
        except Exception as e:
            logger.warning("is_active check failed for %s: %s", symbol, e)
            return None

    def available_end(self) -> Optional[str]:
        """Free: the dataset's available end timestamp for our SCHEMA (ISO str),
        None on any error (callers fail open to the full window)."""
        try:
            r = self._get("metadata.get_dataset_range", {"dataset": self.dataset})
            r.raise_for_status()
            meta = json.loads(r.text)
            return meta["schema"][SCHEMA]["end"]
        except Exception as e:
            logger.warning("available_end failed for %s: %s", self.dataset, e)
            return None

    def get_cost(self, symbol: str, start: str, end: str) -> float:
        """Free: estimated USD cost of an ohlcv-1m pull for the window."""
        r = self._get("metadata.get_cost", {
            "dataset": self.dataset, "symbols": symbol, "schema": SCHEMA,
            "start": start, "end": end, "stype_in": "raw_symbol",
            "mode": "historical"})
        r.raise_for_status()
        return float(r.text)

    def record_count(self, symbol: str, start: str, end: str) -> Optional[int]:
        """Free: exact ohlcv-1m record count for [start, end). None on 422
        (symbol invalid on the dataset) — the ONLY genuine 'no such symbol'.

        Raises TransientFetchError on a non-422 HTTP error (e.g. 504 gateway
        timeout that survives _get's retries) so the caller can degrade to an
        unverified fetch — a metadata hiccup must NOT crash the whole event."""
        r = self._get("metadata.get_record_count", {
            "dataset": self.dataset, "symbols": symbol, "schema": SCHEMA,
            "start": start, "end": end, "stype_in": "raw_symbol"})
        if r.status_code == 422:
            if _422_case(r) == "symbology_invalid_request":
                return None
            # any other 422 (e.g. data_end_after_available_end) is a bad
            # REQUEST, not a no-data symbol — must be loud, never empty
            raise TransientFetchError(
                f"record_count {symbol}: 422 {_422_case(r) or r.text[:120]}")
        if r.status_code != 200:
            raise TransientFetchError(
                f"record_count {symbol}: HTTP {r.status_code} after retries")
        return int(r.text.strip())

    def get_range(self, symbol: str, start: str, end: str,
                  verify: bool = True) -> pd.DataFrame:
        """Paid: ohlcv-1m bars for [start, end). Prices scaled to dollars; adds
        a UTC `ts`.

        A truly-empty window and a TRANSIENT fetch failure both yield an empty
        body — indistinguishable alone, and the bug behind live tickers (UBER)
        being frozen as synthetic-only. When verify=True we cross-check the free
        record_count: if the API says the symbol HAS N>0 records but the data
        pull returned fewer, that's a transient empty/truncated body and we
        RETRY (up to _MAX_ATTEMPTS) rather than silently returning empty. A
        genuine empty (count 0, or 422 invalid symbol) returns an empty frame."""
        expected = None
        if verify:
            try:
                expected = self.record_count(symbol, start, end)
            except TransientFetchError as e:
                # metadata call failed transiently (e.g. 504). Do NOT crash and
                # do NOT treat as empty — degrade to an unverified fetch (expected
                # stays None), so a live ticker isn't frozen as synthetic.
                logger.warning("record_count unavailable for %s (%s); "
                               "fetching without count verification", symbol, e)
            else:
                if expected == 0 or expected is None:
                    return pd.DataFrame()         # count 0, or 422 invalid symbol

        delay = 2.0
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            r = self._get("timeseries.get_range", {
                "dataset": self.dataset, "symbols": symbol, "schema": SCHEMA,
                "start": start, "end": end, "stype_in": "raw_symbol",
                "encoding": "csv"})
            if r.status_code == 422:
                if _422_case(r) == "symbology_invalid_request":
                    return pd.DataFrame()
                raise TransientFetchError(
                    f"get_range {symbol}: 422 {_422_case(r) or r.text[:120]}")
            if r.status_code not in (200, 206):
                # 206 Partial Content is a SUCCESS: Databento streams some
                # responses as 206 with the complete body (VSXY 2026-06);
                # the row-count check below still guards real truncation.
                # Other non-200s that survived _get's retries (e.g. persistent
                # 504) — back off and retry the full loop rather than crash.
                if attempt < _MAX_ATTEMPTS:
                    logger.warning("get_range %s: HTTP %d, retry %d",
                                   symbol, r.status_code, attempt)
                    time.sleep(delay); delay = min(delay * 2, 60)
                    continue
                raise TransientFetchError(
                    f"{symbol} {start}..{end}: HTTP {r.status_code} after "
                    f"{_MAX_ATTEMPTS} attempts")
            n_rows = 0 if (not r.text.strip()) else r.text.count("\n") - 1
            if expected and n_rows < expected:
                # fewer rows than the API says exist -> transient; retry
                if attempt < _MAX_ATTEMPTS:
                    logger.warning("get_range %s %s..%s: got %d/%d rows, retry %d",
                                   symbol, start, end, n_rows, expected, attempt)
                    time.sleep(delay); delay = min(delay * 2, 60)
                    continue
                raise TransientFetchError(
                    f"{symbol} {start}..{end}: expected {expected} records, "
                    f"got {n_rows} after {_MAX_ATTEMPTS} attempts")
            if n_rows <= 0:
                return pd.DataFrame()
            df = pd.read_csv(io.StringIO(r.text))
            for col in ("open", "high", "low", "close"):
                df[col] = df[col] / 1e9
            df["ts"] = pd.to_datetime(df["ts_event"], utc=True)
            return df
        return pd.DataFrame()  # unreachable
