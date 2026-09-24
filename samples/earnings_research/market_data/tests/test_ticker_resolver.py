"""Golden test for web-grounded ticker resolution (user-approved 45-list).

The mapping below was researched via web search and APPROVED by the user as the
reference answer for the 45 peer tickers that did not resolve to live Databento
data. It encodes, for each old ticker: the company name and the CURRENT US
exchange ticker (or None when the security is dead — bankrupt/delisted/OTC-only).

Two layers:
  * offline (always): pin the approved reference mapping as a contract so the
    intended answer can never silently drift, and unit-test the resolver's
    extraction/parse logic with a mocked web fetch + mocked Bedrock.
  * live (opt-in, RUN_TICKER_RESOLVER_LIVE=1): run the real TickerResolver
    (web search + Bedrock extraction) over the renamed cases and assert it
    recovers the approved current ticker.
"""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import ticker_resolver
from ticker_resolver import TickerResolver

ASOF = "2026-08"

# APPROVED reference (old_ticker -> (company_name, current_ticker_or_None)).
# current_ticker None == dead as a US common share (bankrupt / delisted / OTC ADR).
APPROVED = {
    # --- renamed / ticker changed, still a live US common share ---
    "ASGN": ("ASGN Incorporated (Everforth)", "EFOR"),
    "BK":   ("Bank of New York Mellon", "BNY"),
    "BLDE": ("Blade Air Mobility (Strata Critical Medical)", "SRTA"),
    "BSGM": ("BioSig Technologies (Streamex)", "STEX"),
    "DGLY": ("Digital Ally (Kust)", "KUST"),
    "EXPI": ("eXp World Holdings (AGNT)", "AGNT"),
    "LC":   ("LendingClub (Happen)", "HAPN"),
    "MESA": ("Mesa Air Group (Republic Airways)", "RJET"),
    "MMC":  ("Marsh & McLennan Companies", "MRSH"),
    "NYCB": ("New York Community Bancorp (Flagstar Financial)", "FLG"),
    "NYMT": ("New York Mortgage Trust (Adamas Trust)", "ADAM"),
    "ORCC": ("Blue Owl Capital Corp (Owl Rock)", "OBDC"),
    "SATS": ("EchoStar", "ECHO"),
    "SJW":  ("SJW Group (H2O America)", "HTO"),
    "SKLZ": ("Skillz (Firy)", "FIRY"),
    "UCBI": ("United Community Banks", "UCB"),
    "BRP":  ("Baldwin Insurance Group (BRP Group)", "BWIN"),
    "FLT":  ("FLEETCOR Technologies (Corpay)", "CPAY"),
    "RE":   ("Everest Re Group (Everest Group)", "EG"),
    "PARA": ("Paramount Global (Paramount Skydance)", "PSKY"),
    "SQ":   ("Block Inc (Square)", "XYZ"),
    # --- ticker unchanged, still live ---
    "ATGE": ("Adtalem Global Education", "ATGE"),
    "CNHI": ("CNH Industrial", "CNH"),
    "COMM": ("CommScope Holding", "COMM"),
    "COMP": ("Compass Inc", "COMP"),
    "LANC": ("Lancaster Colony", "LANC"),
    "PSTG": ("Pure Storage (Everpure)", "PSTG"),
    "BHLB": ("Berkshire Hills Bancorp", "BHLB"),
    # --- dead as US common share (no current US ticker) ---
    "ABB":  ("ABB Ltd (Swiss; US OTC ADR only)", None),
    "AMRS": ("Amyris", None),
    "ARVL": ("Arrival", None),
    "BIG":  ("Big Lots", None),
    "DNMR": ("Danimer Scientific", None),
    "FRC":  ("First Republic Bank", None),
    "FSR":  ("Fisker", None),
    "GOEV": ("Canoo", None),
    "LEV":  ("Lion Electric", None),
    "LILM": ("Lilium", None),
    "LKCO": ("Luokung Technology", None),
    "MRIN": ("Marin Software", None),
    "NKLA": ("Nikola", None),
    "PTRA": ("Proterra", None),
    "SAVE": ("Spirit Airlines", None),
    "CVCY": ("Central Valley Community Bancorp (merged)", None),
    "VLCN": ("Volcon", None),
}

RENAMED_LIVE = {old: new for old, (nm, new) in APPROVED.items()
                if new and new != old}
DEAD = {old for old, (nm, new) in APPROVED.items() if new is None}


class TestApprovedContract:
    def test_count(self):
        assert len(APPROVED) == 45

    def test_renamed_live_examples(self):
        # the canonical cases that motivated this feature
        assert RENAMED_LIVE["BK"] == "BNY"
        assert RENAMED_LIVE["ASGN"] == "EFOR"
        assert RENAMED_LIVE["EXPI"] == "AGNT"
        assert RENAMED_LIVE["SQ"] == "XYZ"

    def test_dead_examples(self):
        for d in ("FRC", "NKLA", "FSR", "BIG"):
            assert d in DEAD


class TestResolverExtraction:
    """Resolver logic with web fetch + Bedrock both mocked (offline)."""

    def _resolver(self, extraction_json):
        br = MagicMock()
        br.converse.return_value = {"output": {"message": {"content": [
            {"text": extraction_json}]}}}
        return TickerResolver(br, "test-model", max_attempts=1)

    def test_extracts_current_ticker_from_evidence(self):
        r = self._resolver('{"current_ticker":"BNY","status":"renamed","confidence":"high","evidence_quote":"BK->BNY"}')
        with patch.object(ticker_resolver, "_search_text",
                          return_value="Bank of New York Mellon now trades as BNY on NYSE"):
            out = r.resolve("Bank of New York Mellon", "BK", ASOF)
        assert out["current_ticker"] == "BNY"
        assert out["status"] == "renamed"

    def test_null_when_dead(self):
        r = self._resolver('{"current_ticker":null,"status":"bankrupt_delisted","confidence":"high","evidence_quote":"Chapter 7"}')
        with patch.object(ticker_resolver, "_search_text",
                          return_value="First Republic Bank failed, seized by FDIC, delisted"):
            out = r.resolve("First Republic Bank", "FRC", ASOF)
        assert out["current_ticker"] is None

    def test_search_failure_returns_none(self):
        r = self._resolver('{"current_ticker":"X"}')  # should not even be used
        with patch.object(ticker_resolver, "_search_text",
                          side_effect=Exception("network")):
            out = r.resolve("Anything", "ANY", ASOF)
        assert out["current_ticker"] is None
        assert out["status"] == "unknown"

    def test_empty_name_no_search(self):
        r = self._resolver('{}')
        out = r.resolve("", "X", ASOF)
        assert out["current_ticker"] is None


@pytest.mark.skipif(os.environ.get("RUN_TICKER_RESOLVER_LIVE") != "1",
                    reason="set RUN_TICKER_RESOLVER_LIVE=1 for live web+Bedrock")
class TestLiveResolution:
    """Real web search + Bedrock extraction must recover the approved tickers."""

    @pytest.fixture(scope="class")
    def resolver(self):
        import boto3
        sess = boto3.Session(region_name="us-east-1")
        return TickerResolver(sess.client("bedrock-runtime"),
                              "us.anthropic.claude-sonnet-4-5-20250929-v1:0")

    @pytest.mark.parametrize("old,name,expected", [
        ("BK", "Bank of New York Mellon", "BNY"),
        ("ASGN", "ASGN Incorporated", "EFOR"),
        ("SJW", "SJW Group H2O America", "HTO"),
        ("RE", "Everest Group insurance", "EG"),
    ])
    def test_live_recovers_current_ticker(self, resolver, old, name, expected):
        out = resolver.resolve(name, old, ASOF)
        assert out["current_ticker"] == expected, f"{old}: got {out}"
