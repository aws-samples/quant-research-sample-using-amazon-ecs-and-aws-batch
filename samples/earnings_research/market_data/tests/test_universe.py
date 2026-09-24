"""Unit tests for the shared universe definition (offline)."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import universe
from universe import (US_PRIMARY_EXCHANGES, EQUITY_SECURITY_TYPE,
                      in_universe, reporter_events_sql, peer_universe_sql)


class TestConstants:
    def test_values(self):
        assert US_PRIMARY_EXCHANGES == ("NAS", "NYS", "ASE", "PSE")
        assert EQUITY_SECURITY_TYPE == "SHARE"

    def test_no_domicile_constant(self):
        # domicile is deliberately NOT part of the universe (US-listed foreign
        # issuers are in-universe).
        assert not hasattr(universe, "US_DOMICILE")


class TestReporterSql:
    def test_has_all_universe_predicates(self):
        sql = reporter_events_sql()
        assert "e.event_type = 'ER'" in sql
        assert "e.projected = false" in sql
        assert "fref_listing_exchange in ('NAS', 'NYS', 'ASE', 'PSE')" in sql
        # reg == prim == SHARE (both lines), NO domicile filter
        assert "reg.fref_security_type = 'SHARE'" in sql
        assert "prim.fref_security_type = 'SHARE'" in sql
        assert "iso_country" not in sql
        # resolution hops through primary equity, not an alphabetical pick
        assert "fsym_primary_equity_id" in sql
        assert "fsym_primary_listing_id" in sql
        assert "row_number" not in sql

    def test_date_placeholders_formattable(self):
        sql = reporter_events_sql().format(start="2025-02-06 00:00:00",
                                           end="2025-02-07 00:00:00")
        assert "'2025-02-06 00:00:00'" in sql
        assert "'2025-02-07 00:00:00'" in sql


class TestPeerSql:
    def test_expands_bare_symbols_to_us_tickers(self):
        sql = peer_universe_sql(["XLF", "BAC", "BABA"])
        assert "'XLF-US'" in sql and "'BAC-US'" in sql and "'BABA-US'" in sql
        # returns exchange + BOTH security-type lines, no domicile
        assert "fref_listing_exchange" in sql
        assert "reg_security_type" in sql
        assert "prim_security_type" in sql
        assert "iso_country" not in sql

    def test_quote_stripping(self):
        # defensive: a stray quote in a symbol must not break the IN-list
        sql = peer_universe_sql(["A'B"])
        assert "'AB-US'" in sql


class TestInUniverse:
    @pytest.mark.parametrize("exch,reg,prim,expected", [
        ("NAS", "SHARE", "SHARE", True),      # Amazon / Chubb / AEHL: US-listed common
        ("NYS", "SHARE", "SHARE", True),
        ("NYS", "ADR",   "SHARE", False),     # BABA/JD ADR: reg != prim
        ("NAS", "ADR",   "ADR",   False),     # SIFY ADR-both: equal but not SHARE
        ("PSE", "ETF_ETF", "ETF_ETF", False), # XLF ETF
        ("NAS", "PREF",  "SHARE", False),     # preferred
        ("TKS", "SHARE", "SHARE", False),     # non-US listing
        (None, None, None,        False),
    ])
    def test_cases(self, exch, reg, prim, expected):
        assert in_universe(exch, reg, prim) is expected
