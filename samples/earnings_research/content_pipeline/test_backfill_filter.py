"""--filter-events: the backfill scan keeps only the chosen event ids."""
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
import backfill_manifest


def test_load_filter_events_local(tmp_path):
    p = tmp_path / "chosen.parquet"
    pl.DataFrame({"event_id": [11, 22]}).write_parquet(p)
    assert backfill_manifest.load_filter_events(str(p)) == {11, 22}


def test_scan_applies_filter(tmp_path):
    """Rows outside filter_events never reach strategy classification."""
    src = pl.DataFrame({
        "event_id": [11, 22, 33],
        "event_datetime_utc": ["2025-02-01 12:00:00"] * 3,
        "event_date": ["2025-02-01"] * 3,
        "region": ["US"] * 3,
        "url_pr": ["http://a", "http://b", "http://c"],
        "factset_entity_id": ["E1", "E2", "E3"],
        "entity_proper_name": ["A", "B", "C"],
        "ticker_region": ["A-US", "B-US", "C-US"],
        "fiscal_period": ["Q1"] * 3,
        "fiscal_year": [2025] * 3,
        "fetch_status": ["timeout", "timeout", "timeout"],
    })
    classified = []

    def fake_classify(region, status, url):
        classified.append(url)
        return "direct"

    cfg = MagicMock()
    cfg.manifest.num_shards = 4
    cfg.job_name = "er-panels-backfill"
    written = []
    s3io = MagicMock()
    s3io.bucket, s3io.prefix = "b", "earnings-content"
    s3io.put_frame = lambda key, frame: written.append((key, frame))

    with patch.object(backfill_manifest, "S3Io", return_value=s3io), \
         patch.object(backfill_manifest, "classify", side_effect=fake_classify), \
         patch.object(backfill_manifest, "_storage_options", return_value={}), \
         patch.object(backfill_manifest.pl, "scan_parquet") as scan:
        scan.return_value.select.return_value.filter.return_value \
            .unique.return_value.collect.return_value = src
        backfill_manifest.scan_source_shard(cfg, "er-full", 0,
                                            filter_events={11, 33})

    assert len(classified) == 2                       # 22 filtered out
    kept = pl.concat([f for _, f in written])
    assert sorted(kept["event_id"].to_list()) == [11, 33]
