import pytest

from backfill_strategies import (
    STRATEGY_BROWSER,
    STRATEGY_RETRY,
    STRATEGY_REWRITE,
    STRATEGY_SKIP,
    classify,
    rewrite_url,
)


class TestClassify:
    @pytest.mark.parametrize("region,status,url,expected", [
        # US bot-wall classes -> headed browser
        ("US", "timeout", "https://investor.ultralifecorporation.com/node/16926/pdf", STRATEGY_BROWSER),
        ("US", "http_403", "https://s27.q4cdn.com/214071222/files/x.pdf", STRATEGY_BROWSER),
        ("US", "http_other", "https://investors.example.com/news", STRATEGY_BROWSER),
        # non-US bot-wall on a live host is also browser-eligible
        ("GB", "timeout", "https://otp.tools.investis.com/x", STRATEGY_BROWSER),
        # BSE India rewrite
        ("IN", "http_404", "https://www.bseindia.com/xml-data/corpfiling/AttachLive/abc.pdf", STRATEGY_REWRITE),
        # transient -> gentle retry
        ("US", "http_5xx", "https://compxinternational.com/x", STRATEGY_RETRY),
        ("US", "http_429", "https://example.com/x", STRATEGY_RETRY),
        # dead / gone -> skip
        ("US", "http_404", "https://www.pacificwestbank.com/ContentDocumentHandler.ashx?documentId=81154", STRATEGY_SKIP),
        ("US", "dns_error", "https://www.spartannash.com/x", STRATEGY_SKIP),
        ("US", "timeout", "http://phx.corporate-ir.net/phoenix.zhtml?c=1", STRATEGY_SKIP),
        ("MY", "http_403", "https://www.bursamalaysia.com/x", STRATEGY_SKIP),
        ("ID", "http_403", "https://www.idx.co.id/x", STRATEGY_SKIP),
        # ok never actioned
        ("US", "ok", "https://example.com/x", STRATEGY_SKIP),
    ])
    def test_cases(self, region, status, url, expected):
        assert classify(region, status, url) == expected

    def test_bseindia_non_attachlive_not_rewritten(self):
        # a bseindia 404 without /AttachLive/ isn't a rewrite candidate
        assert classify("IN", "http_404",
                        "https://www.bseindia.com/other/path.pdf") == STRATEGY_SKIP


class TestRewriteUrl:
    def test_attachlive_to_attachhis(self):
        assert rewrite_url(
            "https://www.bseindia.com/xml-data/corpfiling/AttachLive/abc.pdf"
        ) == "https://www.bseindia.com/xml-data/corpfiling/AttachHis/abc.pdf"

    def test_noop_when_absent(self):
        assert rewrite_url("https://example.com/x.pdf") == "https://example.com/x.pdf"
