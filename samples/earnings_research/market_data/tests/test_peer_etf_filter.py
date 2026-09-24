"""Unit tests for the peer UNIVERSE filter in PeerDeriver (offline).

Drop any LLM-proposed peer that positively resolves OUTSIDE the US common-equity
universe (universe.py): ADRs (reg 'ADR' != prim 'SHARE'), ETFs, funds, preferreds
— anything not US-listed single-class common stock. NO domicile filter: US-listed
foreign issuers (AEHL) are KEPT. One batched Redshift query. Fail-open: no rs
handle, a query error, or an unresolved symbol keeps the peer.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import peer_deriver
from peer_deriver import PeerDeriver

# LLM returns a mix: US operating cos + an ETF (XLF) + an ADR (BABA) in correlated.
_LLM_JSON = ('{"pure_play": ["BAC", "WFC"], '
             '"functional": ["C", "USB"], '
             '"correlated": ["XLF", "BABA", "GS"]}')


def _row(ticker, exch, reg, prim):
    return {"ticker_region": ticker, "listing_exchange": exch,
            "reg_security_type": reg, "prim_security_type": prim}


def _deriver(rs=None):
    d = PeerDeriver.__new__(PeerDeriver)          # skip boto3 in __init__
    d.bedrock = MagicMock(); d.db = MagicMock()
    d.rs = rs; d.model_id = "test-model"; d.max_attempts = 1
    return d


class TestPeerUniverseFilter:
    def test_drops_etf_adr_keeps_us_common(self):
        rs = MagicMock()
        rs.fetch_all.return_value = iter([
            _row("BAC-US", "NYS", "SHARE", "SHARE"),
            _row("WFC-US", "NYS", "SHARE", "SHARE"),
            _row("C-US",   "NYS", "SHARE", "SHARE"),
            _row("USB-US", "NYS", "SHARE", "SHARE"),
            _row("GS-US",  "NYS", "SHARE", "SHARE"),
            _row("XLF-US", "PSE", "ETF_ETF", "ETF_ETF"),  # ETF -> drop
            _row("BABA-US","NYS", "ADR", "SHARE"),          # ADR (reg!=prim) -> drop
        ])
        d = _deriver(rs)
        with patch.object(d, "_invoke", return_value=_LLM_JSON):
            out = d.derive("JPM", "JPMorgan Chase & Co.")
        allsyms = [s for v in out["peers"].values() for s in v]
        assert set(allsyms) == {"BAC", "WFC", "C", "USB", "GS"}
        assert out["dropped"] == ["BABA", "XLF"]
        assert out["peers"]["correlated"] == ["GS"]

    def test_keeps_us_listed_foreign_share(self):
        # AEHL case: US-listed (NAS), single-class SHARE, foreign domicile (CN).
        # NO domicile filter -> KEPT, same basis as any US-listed common stock.
        rs = MagicMock()
        rs.fetch_all.return_value = iter([
            _row("BAC-US",  "NYS", "SHARE", "SHARE"),
            _row("AEHL-US", "NAS", "SHARE", "SHARE"),   # US-listed foreign share -> KEEP
        ])
        d = _deriver(rs)
        with patch.object(d, "_invoke",
                          return_value='{"pure_play":["BAC","AEHL"],"functional":[],"correlated":[]}'):
            out = d.derive("X", "X Corp")
        allsyms = [s for v in out["peers"].values() for s in v]
        assert set(allsyms) == {"BAC", "AEHL"}
        assert out["dropped"] == []

    def test_no_rs_handle_keeps_all(self):
        d = _deriver(rs=None)                          # filter is a no-op
        with patch.object(d, "_invoke", return_value=_LLM_JSON):
            out = d.derive("JPM", "JPMorgan")
        allsyms = [s for v in out["peers"].values() for s in v]
        assert "XLF" in allsyms and "BABA" in allsyms
        assert out["dropped"] == []

    def test_query_error_fails_open(self):
        rs = MagicMock()
        rs.fetch_all.side_effect = RuntimeError("redshift boom")
        d = _deriver(rs)
        with patch.object(d, "_invoke", return_value=_LLM_JSON):
            out = d.derive("JPM", "JPMorgan")
        allsyms = [s for v in out["peers"].values() for s in v]
        assert "XLF" in allsyms                        # not dropped on error
        assert out["dropped"] == []

    def test_unresolved_symbol_not_dropped(self):
        # a peer FactSet doesn't return (rename lag) must survive — only
        # POSITIVELY out-of-universe symbols are dropped.
        rs = MagicMock()
        rs.fetch_all.return_value = iter([_row("XLF-US", "PSE", "ETF_ETF", "ETF_ETF")])
        d = _deriver(rs)
        with patch.object(d, "_invoke", return_value=_LLM_JSON):
            out = d.derive("JPM", "JPMorgan")
        allsyms = [s for v in out["peers"].values() for s in v]
        assert "GS" in allsyms and "BAC" in allsyms    # unresolved -> kept
        assert "XLF" not in allsyms
        assert out["dropped"] == ["XLF"]

    def test_batched_single_query(self):
        rs = MagicMock()
        rs.fetch_all.return_value = iter([])
        d = _deriver(rs)
        with patch.object(d, "_invoke", return_value=_LLM_JSON):
            d.derive("JPM", "JPMorgan")
        assert rs.fetch_all.call_count == 1            # one batched call, not per-symbol
        sql = rs.fetch_all.call_args[0][0]
        assert "'XLF-US'" in sql and "'BAC-US'" in sql
