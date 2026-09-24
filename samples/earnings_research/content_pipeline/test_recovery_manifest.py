"""Recovery-manifest construction and its guards."""
from unittest.mock import patch

import polars as pl
import pytest

import recovery_manifest as rm
from config import Config
from models import MANIFEST_SCHEMA

SOURCE_META = pl.DataFrame([
    {"event_id": 4730435, "event_datetime_utc": "2016-01-28 00:00:00",
     "event_date": "2016-01-28", "region": "US", "factset_entity_id": "000Y8N-E",
     "entity_proper_name": "Valero Energy Corp.", "ticker_region": "VLO-US",
     "fiscal_period": "4", "fiscal_year": 2015},
    {"event_id": 4730432, "event_datetime_utc": "2016-05-03 00:00:00",
     "event_date": "2016-05-03", "region": "US", "factset_entity_id": "000Y8N-E",
     "entity_proper_name": "Valero Energy Corp.", "ticker_region": "VLO-US",
     "fiscal_period": "1", "fiscal_year": 2016},
])


def _cfg():
    return Config.from_dict({
        "job_name": "er-xle-recovery",
        "s3": {"bucket": "b", "prefix": "earnings-content"},
        "manifest": {"num_shards": 200},
    })


def _build(replacements):
    with patch.object(rm, "S3Io"), \
         patch.object(rm, "_storage_options", return_value={}), \
         patch.object(rm.pl, "scan_parquet",
                      return_value=SOURCE_META.lazy()):
        return rm.build_recovery_frame(_cfg(), "er-xle-full", replacements)


def test_replacement_url_supersedes_stored_url_and_schema_matches():
    reps = pl.DataFrame({"event_id": [4730435],
                         "url": ["https://investorvalero.com/news/news-details/"
                                 "2016/Valero-Q4/default.aspx"]})
    df = _build(reps)
    assert list(df.columns) == list(MANIFEST_SCHEMA)
    assert df["url_pr"][0].endswith("Valero-Q4/default.aspx")
    # metadata inherited from the source run, not invented
    assert df["event_date"][0] == "2016-01-28"
    assert df["ticker_region"][0] == "VLO-US"
    assert df["fiscal_year"][0] == 2015


def test_same_event_shards_identically_to_source_run():
    # Recovered rows must land in the shard the event already belongs to, so
    # resume logic and content layout stay consistent across jobs.
    from sharding import shard_for_event
    reps = pl.DataFrame({"event_id": [4730432], "url": ["https://x/a/b/c"]})
    df = _build(reps)
    assert df["shard"][0] == shard_for_event(4730432, 200)


def test_event_id_absent_from_source_run_is_refused():
    # Silently carrying null dates would poison the partition layout.
    reps = pl.DataFrame({"event_id": [999999], "url": ["https://x/a/b/c"]})
    with pytest.raises(ValueError, match="not found in job="):
        _build(reps)


def test_missing_url_column_is_refused(tmp_path):
    p = tmp_path / "r.csv"
    p.write_text("event_id,note\n1,hello\n")
    with pytest.raises(ValueError, match="missing columns"):
        rm.load_replacements(str(p))


def test_blank_url_is_refused(tmp_path):
    p = tmp_path / "r.csv"
    p.write_text("event_id,url\n1,https://x/a/b/c\n2,\n")
    with pytest.raises(ValueError, match="no URL"):
        rm.load_replacements(str(p))


def test_duplicate_event_id_is_refused(tmp_path):
    # Two URLs for one event would fetch twice and double-count coverage.
    p = tmp_path / "r.csv"
    p.write_text("event_id,url\n1,https://x/a/b/c\n1,https://y/a/b/c\n")
    with pytest.raises(ValueError, match="duplicate event_ids"):
        rm.load_replacements(str(p))


def test_load_replacements_reads_valid_csv(tmp_path):
    p = tmp_path / "r.csv"
    p.write_text("event_id,url\n4730435,https://x/a/b/c\n")
    df = rm.load_replacements(str(p))
    assert df.height == 1
    assert df["event_id"].dtype == pl.Int64
