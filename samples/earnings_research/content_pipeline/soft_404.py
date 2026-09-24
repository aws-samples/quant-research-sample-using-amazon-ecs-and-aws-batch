"""Detect redirect-collapse soft 404s.

A dead IR deep link often does not 404. The platform 301s it to the site root
or the IR landing page and serves HTTP 200, so the fetcher records `ok` with a
short body. The XLE run collected 23 of these: 17 Valero events whose
phoenix.zhtml links landed on investorvalero.com/home/default.aspx (all with
text_length 1181), 5 Devon events on devonenergy.com/ (365), and 1
ConocoPhillips on /investor-relations/ (486). A later browser-strategy backfill
re-fetched the Valero 17 and recovered 0 — it followed the same redirect. The
document was never there to get.

The signal is structural, not textual: a request for a deep document path came
back from a shallow one. `looks_collapsed` compares path depth, so it needs no
per-site body fingerprints.

Not covered here: a URL that returns 200 at the *requested* path but serves a
stub (EOG's news-details pages return exactly 2244 chars of chrome). Same
symptom, different cause — depth is unchanged, so this check passes them.
"""
from urllib.parse import urlsplit

# A landing page is shallow. Two segments is enough to allow the common
# /home/default.aspx and /investor-relations/index.html shapes while still
# rejecting a real document path like /news/news-details/2016/<slug>/default.aspx.
_MAX_LANDING_DEPTH = 2


def _segments(url: str) -> list:
    return [s for s in urlsplit(url).path.split("/") if s]


def looks_collapsed(requested_url: str, final_url: str) -> bool:
    """True when a deep document request ended on a shallow landing page.

    Requires an actual loss of depth: a same-depth or deeper redirect is a
    normal canonicalisation, and a shallow request that stays shallow was
    never a document request to begin with.
    """
    if not requested_url or not final_url:
        return False

    req, fin = _segments(requested_url), _segments(final_url)

    # The request itself must have been for something deeper than a landing
    # page, or there is no collapse to detect.
    if len(req) <= _MAX_LANDING_DEPTH:
        return False

    return len(fin) < len(req) and len(fin) <= _MAX_LANDING_DEPTH


def describe(requested_url: str, final_url: str) -> str:
    """Redirect trail for error_detail, so a miss can be researched later."""
    return f"soft 404: {requested_url} -> {final_url}"
