"""Content-type sniffing: magic bytes first, HTTP header second, never the URL.

IR sites routinely serve PDFs with text/html Content-Type and vice versa, so
the body bytes are the primary signal.
"""

from typing import Optional

TYPE_PDF = "pdf"
TYPE_HTML = "html"
TYPE_TEXT = "text"
TYPE_UNKNOWN = "unknown"

# File extensions for raw object keys, by sniffed type
EXTENSIONS = {TYPE_PDF: "pdf", TYPE_HTML: "html", TYPE_TEXT: "txt", TYPE_UNKNOWN: "bin"}

_HTML_MARKERS = (b"<!doctype", b"<html", b"<?xml", b"<head", b"<body")


def sniff_type(body: bytes, content_type_header: Optional[str] = None) -> str:
    """Classify body as pdf | html | text | unknown."""
    if not body:
        return TYPE_UNKNOWN

    # Strip BOM and leading whitespace for marker checks
    head = body[:2048].lstrip(b"\xef\xbb\xbf\xff\xfe\x00 \t\r\n")

    if head.startswith(b"%PDF-"):
        return TYPE_PDF

    lowered = head[:512].lower()
    if any(lowered.startswith(m) or m in lowered[:256] for m in _HTML_MARKERS):
        return TYPE_HTML

    header = (content_type_header or "").lower()
    if "pdf" in header:
        # Header claims PDF but magic bytes disagree — trust bytes, but a
        # missing %PDF- prefix with a pdf header usually means an error page
        return TYPE_HTML if b"<" in head[:64] else TYPE_UNKNOWN
    if "html" in header or "xml" in header:
        return TYPE_HTML
    if header.startswith("text/"):
        return TYPE_TEXT

    # Last resort: printable-looking bodies are text
    sample = head[:512]
    if sample and _mostly_printable(sample):
        return TYPE_TEXT
    return TYPE_UNKNOWN


def _mostly_printable(sample: bytes) -> bool:
    printable = sum(1 for b in sample if 32 <= b < 127 or b in (9, 10, 13))
    return printable / len(sample) > 0.9


def extension_for(sniffed_type: str) -> str:
    return EXTENSIONS.get(sniffed_type, "bin")
