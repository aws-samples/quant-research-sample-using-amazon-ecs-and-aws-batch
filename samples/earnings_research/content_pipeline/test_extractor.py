import io

import pypdf
import pytest

from content_type import TYPE_HTML, TYPE_PDF, TYPE_TEXT, TYPE_UNKNOWN
from extractor import extract_text
from models import (
    EXTRACT_DECODE_ERROR,
    EXTRACT_HTML_EMPTY,
    EXTRACT_OK,
    EXTRACT_PDF_ERROR,
    EXTRACT_SKIPPED,
)


def _make_pdf(text: str) -> bytes:
    """Build a minimal one-page PDF containing `text`."""
    writer = pypdf.PdfWriter()
    page = writer.add_blank_page(width=612, height=792)
    # pypdf can't draw text on a blank page without reportlab; instead embed
    # the text via a simple content stream
    from pypdf.generic import DecodedStreamObject, DictionaryObject, NameObject

    stream = DecodedStreamObject()
    stream.set_data(
        f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET".encode("latin-1")
    )
    stream_ref = writer._add_object(stream)
    page[NameObject("/Contents")] = stream_ref
    page[NameObject("/Resources")] = DictionaryObject({
        NameObject("/Font"): DictionaryObject({
            NameObject("/F1"): DictionaryObject({
                NameObject("/Type"): NameObject("/Font"),
                NameObject("/Subtype"): NameObject("/Type1"),
                NameObject("/BaseFont"): NameObject("/Helvetica"),
            })
        })
    })
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


class TestPdf:
    def test_simple_pdf(self):
        body = _make_pdf("Net sales increased 17 percent")
        text, status = extract_text(body, TYPE_PDF)
        assert status == EXTRACT_OK
        assert "Net sales increased" in text

    def test_garbage_pdf(self):
        text, status = extract_text(b"%PDF-1.4 garbage not really a pdf", TYPE_PDF)
        assert status == EXTRACT_PDF_ERROR
        assert text is None


class TestHtml:
    def test_simple_html(self):
        html = (b"<html><head><title>t</title><script>var x=1;</script></head>"
                b"<body><h1>Q1 Results</h1><p>Revenue grew 20% to $5 billion in "
                b"the quarter, driven by strong demand across all segments.</p>"
                b"</body></html>")
        text, status = extract_text(html, TYPE_HTML)
        assert status == EXTRACT_OK
        assert "Revenue grew 20%" in text
        assert "var x=1" not in text

    def test_empty_html(self):
        text, status = extract_text(b"<html><body></body></html>", TYPE_HTML)
        assert status == EXTRACT_HTML_EMPTY
        assert text is None


class TestText:
    def test_utf8(self):
        text, status = extract_text("résultats trimestriels".encode("utf-8"), TYPE_TEXT)
        assert status == EXTRACT_OK
        assert "résultats" in text

    def test_cp1252_fallback(self):
        text, status = extract_text("smart “quotes”".encode("cp1252"), TYPE_TEXT)
        assert status == EXTRACT_OK


class TestSkip:
    def test_unknown_skipped(self):
        text, status = extract_text(b"\x00\x01\x02", TYPE_UNKNOWN)
        assert status == EXTRACT_SKIPPED
        assert text is None


class TestSanitize:
    def test_lone_surrogate_survives_parquet_encoding(self):
        import polars as pl
        # html that decodes to a lone surrogate via cp1252-ish content
        html = ("<html><body><p>Results \ud83c broken emoji surrogate "
                "plus enough text to extract properly here.</p></body></html>"
                ).encode("utf-8", errors="surrogatepass")
        text, status = extract_text(html, TYPE_HTML)
        assert status == EXTRACT_OK
        # must be writable to parquet without UnicodeEncodeError
        df = pl.DataFrame({"t": [text]})
        assert df.height == 1
