"""Text extraction from fetched documents.

PDF via pypdf (per-page, salvages what it can); HTML via trafilatura with a
BeautifulSoup fallback. These are blocking/CPU-bound — the worker runs them
in a thread pool via loop.run_in_executor.
"""

import io
import logging
from typing import Optional, Tuple

from content_type import TYPE_HTML, TYPE_PDF, TYPE_TEXT
from models import (
    EXTRACT_DECODE_ERROR,
    EXTRACT_HTML_EMPTY,
    EXTRACT_OK,
    EXTRACT_PDF_ENCRYPTED,
    EXTRACT_PDF_ERROR,
    EXTRACT_SKIPPED,
)

logger = logging.getLogger(__name__)


def extract_text(body: bytes, sniffed_type: str) -> Tuple[Optional[str], str]:
    """Extract plain text. Returns (text, extract_status); text is None on failure."""
    if sniffed_type == TYPE_PDF:
        text, status = _extract_pdf(body)
    elif sniffed_type == TYPE_HTML:
        text, status = _extract_html(body)
    elif sniffed_type == TYPE_TEXT:
        text, status = _decode_text(body)
    else:
        return None, EXTRACT_SKIPPED
    return _sanitize(text), status


def _sanitize(text: Optional[str]) -> Optional[str]:
    """Strip lone surrogates etc. — must be valid UTF-8 for parquet."""
    if text is None:
        return None
    return text.encode("utf-8", errors="replace").decode("utf-8")


def _extract_pdf(body: bytes) -> Tuple[Optional[str], str]:
    try:
        import pypdf

        reader = pypdf.PdfReader(io.BytesIO(body))
        if reader.is_encrypted:
            try:
                reader.decrypt("")
            except Exception:
                return None, EXTRACT_PDF_ENCRYPTED
        pages = []
        for page in reader.pages:
            try:
                pages.append(page.extract_text() or "")
            except Exception as e:  # salvage the rest of the document
                logger.debug("pdf page extraction failed: %s", e)
        text = "\n\n".join(p for p in pages if p.strip())
        if not text.strip():
            return None, EXTRACT_PDF_ERROR
        return text, EXTRACT_OK
    except Exception as e:
        logger.debug("pdf extraction failed: %s", e)
        return None, EXTRACT_PDF_ERROR


def _extract_html(body: bytes) -> Tuple[Optional[str], str]:
    html, status = _decode_text(body)
    if html is None:
        return None, status

    # trafilatura strips IR-site chrome and keeps the press-release body
    try:
        import trafilatura

        text = trafilatura.extract(html)
        if text and text.strip():
            return text, EXTRACT_OK
    except Exception as e:
        logger.debug("trafilatura failed, falling back to bs4: %s", e)

    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "lxml")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text(separator="\n")
        text = "\n".join(line.strip() for line in text.splitlines() if line.strip())
        if text:
            return text, EXTRACT_OK
    except Exception as e:
        logger.debug("bs4 fallback failed: %s", e)

    return None, EXTRACT_HTML_EMPTY


def _decode_text(body: bytes) -> Tuple[Optional[str], str]:
    for encoding in ("utf-8", "cp1252", "latin-1"):
        try:
            return body.decode(encoding), EXTRACT_OK
        except (UnicodeDecodeError, LookupError):
            continue
    return None, EXTRACT_DECODE_ERROR
