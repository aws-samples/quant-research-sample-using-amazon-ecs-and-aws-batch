"""Tests for the results-browser server endpoints (no real S3 — stubbed).

Run: scripts/.venv/bin/python -m pytest tools/results_browser/test_server.py
"""
import io
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import server


@pytest.fixture()
def parquet_bytes():
    df = pd.DataFrame({"scope": ["SMALL", "MID"], "sharpe": [1.5, 0.3],
                       "ann_ret_pct": [10.2, 4.0]})
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


@pytest.fixture()
def stub_s3(monkeypatch, parquet_bytes):
    monkeypatch.setattr(server, "get_object", lambda key: parquet_bytes)
    monkeypatch.setattr(server, "_bucket", "test-bucket")


def test_arrow_roundtrip(stub_s3):
    body, ctype = server.render_arrow("x/portfolio_stats.parquet")
    assert ctype == "application/vnd.apache.arrow.stream"
    table = ipc.open_stream(pa.BufferReader(body)).read_all()
    assert table.num_rows == 2
    assert set(table.column_names) == {"scope", "sharpe", "ann_ret_pct"}


def test_table_page_embeds_viewer(stub_s3):
    body = server.render_table("x/portfolio_stats.parquet")
    text = body.decode()
    assert "perspective-viewer" in text
    assert "/arrow?key=x/portfolio_stats.parquet" in text


def test_view_links_to_grid(stub_s3):
    body = server.render_view("x/portfolio_stats.parquet")
    assert b"/table?key=" in body


def test_study10y_single_cell_coercion():
    import study10y as s
    assert s.coerce_cell("reporter_only", "pure play") == ("reporter_only", "none")
    assert s.coerce_cell("ridge_levels_beta_neutral", "SPY") == ("spy_beta_levels_neutral", "SPY")
    assert s.coerce_cell("spy_dollar_neutral", "all") == ("spy_dollar_neutral", "SPY")
    assert s.coerce_cell("ridge_levels_beta_neutral", "pure play") == ("ridge_levels_beta_neutral", "pure play")
    cons, unis = s.members("POOLED", "POOLED")
    assert "reporter_only" not in cons and not any(c.startswith("spy_") for c in cons)
    assert "none" not in unis and "SPY" not in unis
    assert s.members("spy_beta_returns_neutral", "POOLED") == (["spy_beta_returns_neutral"], ["SPY"])
