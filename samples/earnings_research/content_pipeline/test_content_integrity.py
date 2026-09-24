# earnings_content_pipeline/test_content_integrity.py
import datetime as dt
import pandas as pd
from content_integrity import audit_frame, classify, audit_text

RELEASE = ("ACME Corp. today reported fourth quarter 2021 results. Revenue was $1,234.5 million, "
           "up 12.3% year over year; diluted earnings per share were $1.02. Gross margin 45.1%. "
           "Guidance for the first quarter: revenue of $1.2 billion. Dated February 10, 2022.")
JS_PAGE = "JavaScript is disabled in your browser. Please enable it to continue."
INDEX_PAGE = ("Press Releases | Investor Relations. "
              "ACME Reports Fourth Quarter 2025 Results — February 3, 2026. ACME Names CFO — January 12, 2026. "
              "ACME Reports Third Quarter 2025 Results — October 28, 2025. ACME Declares Dividend — September 3, 2025. "
              "ACME Reports Second Quarter 2025 Results — July 29, 2025. ACME to Present at Conference — June 2, 2025. "
              "revenue quarter guidance")
NOTES_RELEASE = ("BOISE, Idaho, January 7, 2026 -- Albertsons Companies today reported fourth quarter results. "
                 "Revenue $18,000 million; diluted earnings per share $0.72; gross margin 27.8%. The company issued "
                 "$600 million of 5.750% senior unsecured notes due March 31, 2034. A replay is available until February 8, 2027.")
INDEX_LATE = ("Press Releases | Investor Relations. "
              "ACME Reports Fourth Quarter 2025 Results — February 3, 2026. ACME Names CFO — January 12, 2026. "
              "ACME Reports Third Quarter 2025 Results — October 28, 2025. ACME Declares Dividend — September 3, 2025. "
              "ACME Reports Second Quarter 2025 Results — July 29, 2025. ACME to Present at Conference — June 2, 2025. "
              "revenue quarter guidance")


def _df(rows):
    return pd.DataFrame(rows, columns=["event_id", "symbol", "event_datetime_utc", "text_content"])


def test_clean_release_passes():
    a = classify(audit_frame(_df([(1, "ACME", "2022-02-10 21:05:00", RELEASE)])), 3, 365)
    assert not a.excluded.iloc[0] and a.n_fin_keywords.iloc[0] >= 3 and a.n_figures.iloc[0] >= 1


def test_error_page_is_suspect_content():
    a = classify(audit_frame(_df([(2, "MPWR", "2021-05-05 20:05:00", JS_PAGE)])), 3, 365)
    assert a.exclusion_reason.iloc[0] == "suspect_content"


def test_late_index_page_is_future_dated():
    a = classify(audit_frame(_df([(3, "ACME", "2021-10-28 20:05:00", INDEX_PAGE)])), 3, 365)
    assert a.future_dated.iloc[0] and a.days_after_event.iloc[0] > 365


def test_empty_text_is_suspect():
    a = classify(audit_frame(_df([(4, "X", "2021-01-01 00:00:00", None)])), 3, 365)
    assert a.exclusion_reason.iloc[0] == "suspect_content"


def test_audit_text_matches_frame_rule():
    r = audit_text(RELEASE, dt.date(2022, 2, 10), 3, 365)
    assert r["clean"] and not r["suspect_content"] and not r["future_dated"]
    r2 = audit_text(INDEX_PAGE, dt.date(2021, 10, 28), 3, 365)
    assert r2["future_dated"] and not r2["clean"]


def test_forward_reference_with_good_dateline_is_not_future_dated():
    r = audit_text(NOTES_RELEASE, dt.date(2026, 1, 7), 3, 365)
    assert r["clean"] and not r["future_dated"]


def test_index_page_many_future_dates_is_future_dated():
    r = audit_text(INDEX_LATE, dt.date(2023, 10, 31), 3, 365)      # event years before the listed headlines
    assert r["future_dated"] and not r["clean"]


def test_harness_rule_still_available():
    r = audit_text(NOTES_RELEASE, dt.date(2026, 1, 7), 3, 365, rule="harness")
    assert r["future_dated"]                                        # old rule flags the 2034 notes


def test_dateline_is_earliest_date_in_head():
    from content_integrity import dateline_date
    assert dateline_date(NOTES_RELEASE) == dt.date(2026, 1, 7)
