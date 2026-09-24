import pytest

from content_type import (
    TYPE_HTML,
    TYPE_PDF,
    TYPE_TEXT,
    TYPE_UNKNOWN,
    extension_for,
    sniff_type,
)


class TestSniffType:
    def test_pdf_magic(self):
        assert sniff_type(b"%PDF-1.4 rest of doc") == TYPE_PDF

    def test_pdf_magic_beats_html_header(self):
        # IR sites serve PDFs as text/html all the time
        assert sniff_type(b"%PDF-1.7 ...", "text/html") == TYPE_PDF

    def test_html_doctype(self):
        assert sniff_type(b"<!DOCTYPE html><html>...", "application/pdf") == TYPE_HTML

    def test_html_with_bom_and_whitespace(self):
        assert sniff_type(b"\xef\xbb\xbf\n  <html><body>x</body></html>") == TYPE_HTML

    def test_html_error_page_claiming_pdf(self):
        # header says pdf, body is an error page
        assert sniff_type(b"<h1>404 Not Found</h1>", "application/pdf") == TYPE_HTML

    def test_text_from_header(self):
        assert sniff_type(b"Quarterly results were strong.", "text/plain") == TYPE_TEXT

    def test_printable_body_no_header(self):
        assert sniff_type(b"Earnings release Q1 2026. Net sales grew.") == TYPE_TEXT

    def test_binary_junk(self):
        assert sniff_type(bytes(range(256)) * 4) == TYPE_UNKNOWN

    def test_empty(self):
        assert sniff_type(b"") == TYPE_UNKNOWN


class TestExtensionFor:
    @pytest.mark.parametrize("t,ext", [
        (TYPE_PDF, "pdf"), (TYPE_HTML, "html"),
        (TYPE_TEXT, "txt"), (TYPE_UNKNOWN, "bin"), ("weird", "bin"),
    ])
    def test_cases(self, t, ext):
        assert extension_for(t) == ext
