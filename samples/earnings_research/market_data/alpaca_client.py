"""Alpaca market-data client — drop-in for DatabentoClient (same six-member
surface: dataset, get_range, get_cost, available_end, is_active, record_count).

Free consolidated SIP feed for *historical* 1-min bars: feed=sip succeeds as
long as the window ends >~15 min in the past; only recent/real-time SIP is
gated (403). NO silent IEX fallback — sparse IEX pre-market would recreate the
DBEQ stale-signal failure mode, so a 403 is a loud TransientFetchError.

Output matches DatabentoClient.get_range's raw shape (ts_event ns int, OHLCV
in dollars, no `symbol` column — panel_builder inserts it) so the orchestration
never knows which source it's talking to. Selected via EMD_SOURCE=alpaca.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
import pandas as pd
import requests

import settings

from databento_client import TransientFetchError

logger = logging.getLogger(__name__)

DATASET = "ALPACA.SIP"
_BARS_URL = "https://data.alpaca.markets/v2/stocks/{symbol}/bars"
_MULTI_BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"
# One multi-symbol request covers a whole basket (~17 symbols) in ~6 shared
# pagination pages vs 17+ per-symbol calls — the lever that fits the
# account-wide 200 req/min limit. Chunk defensively; baskets never get close.
_MULTI_CHUNK = 50
_RETRY_STATUS = {429, 500, 502, 503, 504}
_MAX_ATTEMPTS = 5
# 400/404/422 from the bars endpoint = the request names a symbol Alpaca does
# not know — the analogue of Databento's symbology_invalid_request.
_BAD_SYMBOL_STATUS = {400, 404, 422}
# Free-tier SIP recency gate: windows must end >~15 min in the past.
_SIP_GATE_MINUTES = 16


def _to_alpaca(symbol: str) -> str:
    """Share-class tickers arrive hyphenated from the FactSet/Databento chain
    (BF-B, BRK-B); Alpaca's symbology wants dots (BF.B). US common-stock
    tickers never contain a hyphen otherwise, so the swap is safe."""
    return symbol.replace("-", ".")


def _rfc3339(ts: str) -> str:
    """Window bounds arrive as 'YYYY-MM-DD' or ISO strings; Alpaca wants
    RFC-3339. Date-only strings mean midnight UTC (Databento convention)."""
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    return t.isoformat().replace("+00:00", "Z")


class AlpacaClient:
    def __init__(self, boto3_session: Optional[boto3.Session] = None,
                 key_id: Optional[str] = None, secret_key: Optional[str] = None):
        self.dataset = DATASET
        if key_id and secret_key:
            self._key_id, self._secret = key_id, secret_key
        elif os.environ.get("APCA_API_KEY_ID"):
            self._key_id = os.environ["APCA_API_KEY_ID"]
            self._secret = os.environ["APCA_API_SECRET_KEY"]
        else:
            sess = boto3_session or boto3.Session()
            sm = sess.client("secretsmanager")
            creds = json.loads(
                sm.get_secret_value(SecretId=settings.get("secrets", "alpaca"))["SecretString"])
            self._key_id, self._secret = creds["key_id"], creds["secret_key"]
        logger.info("AlpacaClient dataset=%s feed=sip", self.dataset)

    # ------------------------------------------------------------------ http
    def _get(self, symbol: str, params: dict):
        """One GET with retry+backoff on 429/5xx (honoring Retry-After) and
        connection errors. Returns the final Response."""
        url = _BARS_URL.format(symbol=symbol)
        headers = {"APCA-API-KEY-ID": self._key_id,
                   "APCA-API-SECRET-KEY": self._secret}
        delay = 2.0
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                r = requests.get(url, headers=headers, params=params, timeout=60)
            except requests.exceptions.RequestException as e:
                if attempt < _MAX_ATTEMPTS:
                    time.sleep(delay); delay = min(delay * 2, 60)
                    continue
                raise
            if r.status_code in _RETRY_STATUS and attempt < _MAX_ATTEMPTS:
                retry_after = r.headers.get("Retry-After", "")
                wait = int(retry_after) if retry_after.isdigit() else delay
                time.sleep(wait); delay = min(delay * 2, 60)
                continue
            return r
        return r  # last response after exhausted retries

    def _bar_params(self, start: str, end: str, limit: int = 10000) -> dict:
        return {"timeframe": "1Min", "start": _rfc3339(start),
                "end": _rfc3339(end), "limit": limit,
                "adjustment": "raw", "feed": "sip"}

    def _paged_bars(self, symbol: str, start: str, end: str) -> Optional[list]:
        """All bars for the window, following next_page_token. None = invalid
        symbol. Raises TransientFetchError on 403 (SIP gate / no subscription)
        or any transport failure that survives retries."""
        symbol = _to_alpaca(symbol)
        params = self._bar_params(start, end)
        bars = []
        while True:
            r = self._get(symbol, params)
            if r.status_code in _BAD_SYMBOL_STATUS:
                return None
            if r.status_code == 403:
                raise TransientFetchError(
                    f"{symbol} {start}..{end}: 403 on feed=sip — recency gate "
                    "or missing SIP access; NOT falling back to IEX")
            if r.status_code != 200:
                raise TransientFetchError(
                    f"{symbol} {start}..{end}: HTTP {r.status_code} after "
                    f"{_MAX_ATTEMPTS} attempts")
            payload = r.json()
            bars.extend(payload.get("bars") or [])
            token = payload.get("next_page_token")
            if not token:
                return bars
            params["page_token"] = token

    # ------------------------------------------------- DatabentoClient surface
    def is_active(self, symbol: str, start: str, end: str) -> Optional[bool]:
        """Free: does `symbol` have any 1-min bars in [start, end)?
        True/False on success; None on transient error (callers fail open)."""
        try:
            r = self._get(symbol, self._bar_params(start, end, limit=1))
            if r.status_code in _BAD_SYMBOL_STATUS:
                return False
            if r.status_code != 200:
                return None
            return bool(r.json().get("bars"))
        except Exception as e:
            logger.warning("is_active check failed for %s: %s", symbol, e)
            return None

    def available_end(self) -> Optional[str]:
        """The feed's available end: now minus the free-SIP recency gate.
        NAIVE UTC string — clamp_window tz_localizes it and must not choke."""
        end = datetime.now(timezone.utc) - timedelta(minutes=_SIP_GATE_MINUTES)
        return end.strftime("%Y-%m-%d %H:%M:%S")

    def get_cost(self, symbol: str, start: str, end: str) -> float:
        """Alpaca historical bars are free."""
        return 0.0

    def record_count(self, symbol: str, start: str, end: str) -> Optional[int]:
        """Exact 1-min bar count for [start, end). None = invalid symbol
        (the only genuine 'no such symbol'); raises TransientFetchError on
        transport failure — mirrors DatabentoClient.record_count exactly."""
        bars = self._paged_bars(symbol, start, end)
        if bars is None:
            return None
        end_ts = pd.Timestamp(_rfc3339(end))
        return sum(1 for b in bars if pd.Timestamp(b["t"]) < end_ts)

    def _bars_to_frame(self, bars: list, end_ts: pd.Timestamp) -> pd.DataFrame:
        """Alpaca bar dicts -> DatabentoClient-shaped frame (ts_event ns int,
        NA venue columns, dollar OHLC). Empty frame when nothing survives the
        exclusive-end cut."""
        if not bars:
            return pd.DataFrame()
        df = pd.DataFrame(bars).rename(columns={
            "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
        df["ts"] = pd.to_datetime(df["t"], utc=True)
        # Databento end is exclusive; Alpaca's is inclusive — drop the boundary
        df = df[df["ts"] < end_ts]
        if df.empty:
            return pd.DataFrame()
        df["ts_event"] = df["ts"].astype("int64")     # UTC ns, bar-open time
        df["rtype"] = pd.NA
        df["publisher_id"] = pd.NA
        df["instrument_id"] = pd.NA
        return (df[["ts_event", "rtype", "publisher_id", "instrument_id",
                    "open", "high", "low", "close", "volume", "ts"]]
                .sort_values("ts").reset_index(drop=True))

    def get_range_multi(self, symbols: list, start: str,
                        end: str) -> dict:
        """1-min SIP bars for MANY symbols in one paginated request stream:
        {symbol: frame} with the same shape as get_range. A symbol absent from
        the response (unknown to Alpaca / no bars) maps to an empty frame —
        indistinguishable by design, matching get_range's empty-frame contract.

        This is the request-budget lever: one basket = a handful of shared
        pages instead of one+ requests per symbol (200 req/min account cap)."""
        out = {s: [] for s in symbols}
        back = {_to_alpaca(s): s for s in symbols}   # response key -> our key
        for i in range(0, len(symbols), _MULTI_CHUNK):
            chunk = [_to_alpaca(s) for s in symbols[i:i + _MULTI_CHUNK]]
            params = self._bar_params(start, end)
            params["symbols"] = ",".join(chunk)
            while True:
                r = self._multi_get(params)
                # Unlike unknown symbols (absent from a 200), a symbol Alpaca
                # considers MALFORMED (share-class tickers like BF-B) 400s the
                # whole batch. Drop it and retry the rest; it keeps its empty
                # frame — the same contract as the single-symbol path.
                bad = self._invalid_symbol(r)
                if bad is not None:
                    if bad not in chunk:
                        raise TransientFetchError(
                            f"multi-bars: 400 invalid symbol {bad!r} not in "
                            "request — refusing to loop")
                    logger.warning("multi-bars: dropping invalid symbol %s", bad)
                    chunk.remove(bad)
                    if not chunk:
                        break
                    params["symbols"] = ",".join(chunk)
                    params.pop("page_token", None)   # restart this chunk's pages
                    for s in chunk:
                        out[back[s]] = []            # discard partial pages
                    continue
                payload = r.json()
                for sym, bars in (payload.get("bars") or {}).items():
                    if sym in back:
                        out[back[sym]].extend(bars)
                token = payload.get("next_page_token")
                if not token:
                    break
                params["page_token"] = token
        end_ts = pd.Timestamp(_rfc3339(end))
        return {s: self._bars_to_frame(b, end_ts) for s, b in out.items()}

    @staticmethod
    def _invalid_symbol(r) -> Optional[str]:
        """The offending ticker when a multi-bars 400 names one
        ('invalid symbol: BF-B'), else None."""
        if r.status_code != 400:
            return None
        try:
            msg = r.json().get("message", "")
        except Exception:
            return None
        prefix = "invalid symbol: "
        return msg[len(prefix):].strip() or None if msg.startswith(prefix) else None

    def _multi_get(self, params: dict):
        """GET the multi-symbol bars endpoint; same retry/error envelope as
        _paged_bars. A 400 naming an invalid symbol is returned to the caller
        (get_range_multi drops the ticker and retries); other 400/404/422
        raise — a malformed BATCH request is a bug, not a dead symbol."""
        headers = {"APCA-API-KEY-ID": self._key_id,
                   "APCA-API-SECRET-KEY": self._secret}
        delay = 2.0
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                r = requests.get(_MULTI_BARS_URL, headers=headers,
                                 params=params, timeout=60)
            except requests.exceptions.RequestException:
                if attempt < _MAX_ATTEMPTS:
                    time.sleep(delay); delay = min(delay * 2, 60)
                    continue
                raise
            if r.status_code in _RETRY_STATUS and attempt < _MAX_ATTEMPTS:
                retry_after = r.headers.get("Retry-After", "")
                wait = int(retry_after) if retry_after.isdigit() else delay
                time.sleep(wait); delay = min(delay * 2, 60)
                continue
            if r.status_code == 403:
                raise TransientFetchError(
                    "multi-bars: 403 on feed=sip — recency gate or missing "
                    "SIP access; NOT falling back to IEX")
            if r.status_code != 200 and self._invalid_symbol(r) is None:
                raise TransientFetchError(
                    f"multi-bars: HTTP {r.status_code} after "
                    f"{_MAX_ATTEMPTS} attempts")
            return r
        raise TransientFetchError("multi-bars: retries exhausted")  # unreachable

    def get_range(self, symbol: str, start: str, end: str,
                  verify: bool = True) -> pd.DataFrame:
        """1-min SIP bars for [start, end), shaped like DatabentoClient output:
        columns ts_event/rtype/publisher_id/instrument_id/open/high/low/close/
        volume/ts, NO symbol column (panel_builder inserts it). Empty frame for
        an invalid symbol or a genuinely bar-less window.

        `verify` is accepted for signature compatibility and ignored: Alpaca
        separates transport errors (HTTP status) from genuinely-empty (200 with
        no bars), so Databento's record_count cross-check is unnecessary."""
        bars = self._paged_bars(symbol, start, end)
        if not bars:                      # None (bad symbol) or genuinely empty
            return pd.DataFrame()
        return self._bars_to_frame(bars, pd.Timestamp(_rfc3339(end)))
