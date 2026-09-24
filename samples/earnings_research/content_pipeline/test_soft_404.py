"""Redirect-collapse detection, calibrated on the real XLE failures."""
from soft_404 import looks_collapsed


def test_valero_phoenix_link_collapsing_to_home_is_soft_404():
    # The exact shape of all 17 Valero events recorded as ok/200/1181 chars.
    assert looks_collapsed(
        "http://www.investorvalero.com/news-releases/news-release-details/"
        "valero-energy-reports-first-quarter-2019-results",
        "https://investorvalero.com/home/default.aspx")


def test_devon_link_collapsing_to_bare_root_is_soft_404():
    assert looks_collapsed(
        "http://www.devonenergy.com/Newsroom/Pages/news-release-detail.aspx?id=123",
        "https://www.devonenergy.com/")


def test_conocophillips_link_collapsing_to_ir_landing_is_soft_404():
    assert looks_collapsed(
        "http://www.conocophillips.com/newsroom/Pages/news-releases.aspx?docid=7272017",
        "https://www.conocophillips.com/investor-relations/")


def test_real_document_arriving_at_its_own_deep_path_is_not_soft_404():
    url = ("https://investorvalero.com/news/news-details/2016/"
           "Valero-Energy-Reports-First-Quarter-2016-Results/default.aspx")
    assert not looks_collapsed(url, url)


def test_http_to_https_canonicalisation_is_not_soft_404():
    assert not looks_collapsed(
        "http://investorvalero.com/news/news-details/2016/X-Results/default.aspx",
        "https://investorvalero.com/news/news-details/2016/X-Results/default.aspx")


def test_redirect_that_keeps_depth_is_not_soft_404():
    # Slug rename, not a collapse — still a document path.
    assert not looks_collapsed(
        "https://ir.eqt.com/news-releases/news-release-details/old-slug",
        "https://ir.eqt.com/news-releases/news-release-details/new-slug")


def test_pdf_hotlink_404_path_is_not_reported_as_collapse():
    # The 4 Valero S3 PDFs return an honest 404; final_url == requested.
    url = ("https://s3.amazonaws.com/nadq2gcsccs-pro-www/202101/"
           "VLO%204Q20%20Earnings%20Release.pdf")
    assert not looks_collapsed(url, url)


def test_shallow_request_is_never_a_collapse():
    # Nothing was deep, so there is no lost document.
    assert not looks_collapsed("https://example.com/news",
                               "https://example.com/")


def test_missing_urls_do_not_crash():
    assert not looks_collapsed("", "https://example.com/")
    assert not looks_collapsed("https://example.com/a/b/c", "")
    assert not looks_collapsed(None, None)
