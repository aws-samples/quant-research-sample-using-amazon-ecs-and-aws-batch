# S3 Tables Operations Guide

This guide covers operational aspects of S3 Tables for the sentiment analysis system, including write modes, incremental updates, schema evolution, and maintenance procedures.

## Table of Contents

- [Overview](#overview)
- [Write Modes](#write-modes)
- [Incremental Update Workflows](#incremental-update-workflows)
- [Schema Evolution](#schema-evolution)
- [Table Maintenance](#table-maintenance)
- [Snapshot Management](#snapshot-management)
- [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)

## Overview

S3 Tables use Apache Iceberg format, providing:

- **ACID Transactions**: Atomic, consistent, isolated, durable operations
- **Schema Evolution**: Add/rename columns without rewriting data
- **Time Travel**: Query historical versions of data
- **Partition Evolution**: Change partitioning without data migration
- **Automatic Maintenance**: Compaction and snapshot expiration

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    S3 Table Bucket                       │
│  ┌─────────────────────────────────────────────────┐   │
│  │  Metadata (Iceberg)                              │   │
│  │  ├── metadata/                                   │   │
│  │  │   ├── v1.metadata.json                       │   │
│  │  │   ├── v2.metadata.json                       │   │
│  │  │   └── snap-*.avro (manifests)                │   │
│  │  └── data/                                       │   │
│  │      ├── model_id=claude-v2/date=2024-01-15/    │   │
│  │      │   └── *.parquet                          │   │
│  │      └── model_id=titan/date=2024-01-15/        │   │
│  │          └── *.parquet                          │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

## Write Modes

### Append Mode

Append mode adds new records without checking for duplicates. Use when:
- Processing new articles that haven't been analyzed before
- Speed is critical and duplicates are acceptable
- You'll handle deduplication at query time

**Configuration:**
```json
{
  "write_mode": "append"
}
```

**Behavior:**
- Fastest write performance
- No duplicate checking
- Each write creates new data files
- Multiple records with same primary key possible

**Example:**
```python
# Append mode writes all records as new
writer.write_results_batch(results, mode="append")
# Result: {"written": 1000, "failed": 0}
```

### Upsert Mode

Upsert mode updates existing records or inserts new ones based on primary key. Use when:
- Reprocessing articles that may already exist
- Updating sentiment scores with new model versions
- Ensuring only one record per article/symbol/component/model combination

**Configuration:**
```json
{
  "write_mode": "upsert"
}
```

**Primary Key:**
- `(article_id, symbol, component, model_id)`

**Behavior:**
- Checks for existing records with matching primary key
- Updates existing records with new values
- Inserts new records if no match found
- Slightly slower than append due to merge operation

**Example:**
```python
# Upsert mode updates existing or inserts new
writer.write_results_batch(results, mode="upsert")
# Result: {"written": 800, "updated": 200}
```

### Choosing Write Mode

| Scenario | Recommended Mode | Reason |
|----------|------------------|--------|
| Initial data load | append | Faster, no existing data |
| Daily incremental | upsert | May have overlapping articles |
| Reprocessing with new model | upsert | Update existing scores |
| Parallel job processing | append | Avoid conflicts, dedupe later |
| Single model processing | upsert | Ensure latest scores |

## Incremental Update Workflows

### Daily Incremental Processing

Process only new articles since last run:

```python
# 1. Get last processed timestamp
last_run = get_last_successful_run_timestamp()

# 2. Filter articles newer than last run
new_articles = filter_articles_by_date(articles, after=last_run)

# 3. Process in streaming batches
for batch in read_articles_batch(new_articles, batch_size=1000):
    results = process_batch(batch)
    writer.write_results_batch(results, mode="append")
```

### Reprocessing Specific Articles

Update sentiment for specific articles:

```python
# 1. Identify articles to reprocess
article_ids = ["article-123", "article-456", "article-789"]

# 2. Load and process
articles = load_articles_by_ids(article_ids)
results = process_articles(articles)

# 3. Upsert to update existing records
writer.write_results_batch(results, mode="upsert")
```

### Model Version Update

Reprocess all articles with a new model version:

```python
# 1. Configure new model
config.models = [new_model_config]

# 2. Process all articles with upsert
# Existing records for this model_id will be updated
for batch in read_all_articles_batch(batch_size=1000):
    results = process_batch(batch, model=new_model_config)
    writer.write_results_batch(results, mode="upsert")
```

### Handling Failed Batches

Retry failed batches without duplicating successful ones:

```python
# Track processed batches
processed_batches = load_checkpoint()

for batch_id, batch in enumerate(read_articles_batch()):
    if batch_id in processed_batches:
        continue  # Skip already processed
    
    try:
        results = process_batch(batch)
        writer.write_results_batch(results, mode="upsert")
        save_checkpoint(batch_id)
    except Exception as e:
        log_failed_batch(batch_id, e)
        # Continue with next batch
```

## Schema Evolution

Apache Iceberg supports schema evolution without rewriting data.

### Adding New Columns

```python
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType

# Add a new column
table.update_schema() \
    .add_column("confidence_score", DoubleType()) \
    .commit()
```

**Supported Operations:**
- Add columns (nullable by default)
- Rename columns
- Widen types (e.g., int → long)
- Make required columns optional

**Not Supported Without Rewrite:**
- Remove columns (use soft delete)
- Change column types incompatibly
- Reorder columns

### Renaming Columns

```python
table.update_schema() \
    .rename_column("old_name", "new_name") \
    .commit()
```

### Example: Adding Confidence Score

```python
# 1. Update schema
table.update_schema() \
    .add_column("confidence_score", DoubleType(), doc="Model confidence 0-1") \
    .commit()

# 2. New writes include the column
results_with_confidence = [
    SentimentResult(
        article_id="123",
        sentiment_score=7.5,
        confidence_score=0.95,  # New field
        ...
    )
]

# 3. Old data returns NULL for new column
# SELECT confidence_score FROM table WHERE article_id = 'old-article'
# Returns: NULL
```



## Table Maintenance

### Automatic Maintenance (S3 Tables)

S3 Tables provides automatic maintenance:

- **Compaction**: Merges small files into larger ones
- **Snapshot Expiration**: Removes old snapshots
- **Orphan File Cleanup**: Removes unreferenced data files

**CDK Configuration:**
```typescript
// Configured in CDK stack
properties = {
  "commit.manifest.min-count-to-merge": "5",
  "commit.manifest-merge.enabled": "true",
  "history.expire.max-snapshot-age-ms": "604800000",  // 7 days
}
```

### Manual Compaction

If needed, trigger manual compaction:

```python
from pyiceberg.table import Table

# Compact small files
table.rewrite_data_files(
    target_file_size_bytes=128 * 1024 * 1024,  # 128MB target
    min_file_size_bytes=64 * 1024 * 1024,      # 64MB minimum
)
```

### Expire Old Snapshots

Remove snapshots older than retention period:

```python
from datetime import datetime, timedelta

# Expire snapshots older than 7 days
cutoff = datetime.now() - timedelta(days=7)
table.expire_snapshots() \
    .expire_older_than(cutoff) \
    .commit()
```

### Remove Orphan Files

Clean up unreferenced data files:

```python
# Remove orphan files older than 3 days
table.remove_orphan_files(
    older_than=datetime.now() - timedelta(days=3)
)
```

### Maintenance Schedule

| Task | Frequency | Automated |
|------|-----------|-----------|
| Compaction | Daily | Yes (S3 Tables) |
| Snapshot expiration | Daily | Yes (S3 Tables) |
| Orphan file cleanup | Weekly | Manual recommended |
| Statistics update | After large writes | Manual |

## Snapshot Management

### Understanding Snapshots

Each write creates a new snapshot:
- Snapshots are immutable
- Enable time-travel queries
- Consume storage until expired

### View Snapshots

```sql
-- Via Athena
SELECT 
  snapshot_id,
  committed_at,
  operation,
  summary
FROM "sentiment_results$snapshots"
ORDER BY committed_at DESC;
```

```python
# Via PyIceberg
for snapshot in table.snapshots():
    print(f"ID: {snapshot.snapshot_id}")
    print(f"Timestamp: {snapshot.timestamp_ms}")
    print(f"Operation: {snapshot.operation}")
```

### Rollback to Previous Snapshot

If a bad write occurs, rollback to a previous snapshot:

```python
# Find the snapshot to rollback to
snapshots = list(table.snapshots())
target_snapshot = snapshots[-2]  # Previous snapshot

# Rollback
table.manage_snapshots() \
    .rollback_to(target_snapshot.snapshot_id) \
    .commit()
```

### Cherry-Pick Snapshot

Apply changes from a specific snapshot:

```python
# Cherry-pick specific snapshot
table.manage_snapshots() \
    .cherry_pick(snapshot_id) \
    .commit()
```

### Snapshot Retention Configuration

```json
{
  "s3TablesSnapshotRetentionDays": 7
}
```

**Retention Guidelines:**

| Environment | Retention | Reason |
|-------------|-----------|--------|
| Development | 1-3 days | Minimize storage costs |
| Staging | 7 days | Allow debugging |
| Production | 7-14 days | Support rollback and auditing |

## Monitoring and Troubleshooting

### CloudWatch Metrics

Monitor S3 Tables operations:

```bash
# S3 request metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/S3 \
  --metric-name NumberOfObjects \
  --dimensions Name=BucketName,Value=sentiment-results-bucket \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum
```

### Application Logs

S3 Tables operations are logged:

```
S3 Tables write statistics: operation=append, records_written=1000, errors=0
S3 Tables write statistics: operation=upsert, records_written=800, records_updated=200
S3 Tables append error: CommitFailedException - Concurrent modification
```

### Common Issues

#### Concurrent Write Conflicts

**Symptom:** `CommitFailedException` errors

**Cause:** Multiple writers modifying the same partition

**Solution:**
1. Automatic retry with exponential backoff (built-in)
2. Reduce concurrent writers
3. Use different partitions for parallel jobs

#### Slow Writes

**Symptom:** Write latency > 5 seconds

**Cause:** Too many small files, large batch sizes

**Solution:**
1. Run compaction
2. Adjust batch size (1000-5000 optimal)
3. Check S3 throttling

#### Query Performance Degradation

**Symptom:** Queries slower over time

**Cause:** Too many small files, outdated statistics

**Solution:**
1. Run compaction
2. Update table statistics
3. Check partition pruning is working

### Health Checks

```python
def check_table_health(table):
    """Check S3 Table health metrics."""
    
    # Count data files
    files = list(table.scan().plan_files())
    file_count = len(files)
    
    # Check average file size
    total_size = sum(f.file.file_size_in_bytes for f in files)
    avg_size = total_size / file_count if file_count > 0 else 0
    
    # Check snapshot count
    snapshot_count = len(list(table.snapshots()))
    
    health = {
        "file_count": file_count,
        "total_size_gb": total_size / (1024**3),
        "avg_file_size_mb": avg_size / (1024**2),
        "snapshot_count": snapshot_count,
        "needs_compaction": avg_size < 64 * 1024 * 1024,  # < 64MB
        "needs_snapshot_cleanup": snapshot_count > 100
    }
    
    return health
```

### Recovery Procedures

#### Recover from Failed Write

```python
# 1. Check last successful snapshot
snapshots = list(table.snapshots())
last_good = snapshots[-1]

# 2. Verify data integrity
count = table.scan().count()
print(f"Records in table: {count}")

# 3. If needed, rollback
if data_corrupted:
    table.manage_snapshots() \
        .rollback_to(last_good.snapshot_id) \
        .commit()
```

#### Recover from Accidental Delete

```python
# Use time-travel to recover deleted data
deleted_data = table.scan(
    snapshot_id=snapshot_before_delete
).to_pandas()

# Re-insert the data
writer.write_results_batch(deleted_data, mode="append")
```

## Best Practices

### Write Operations

1. **Use appropriate batch sizes** (1000-5000 records)
2. **Choose correct write mode** (append vs upsert)
3. **Handle retries gracefully** (built-in exponential backoff)
4. **Log write statistics** for monitoring

### Maintenance

1. **Enable automatic maintenance** in S3 Tables
2. **Monitor file counts** and sizes
3. **Set appropriate snapshot retention**
4. **Schedule periodic health checks**

### Performance

1. **Partition by high-cardinality columns** (model_id, date)
2. **Keep file sizes 64-256MB**
3. **Compact after large batch loads**
4. **Update statistics after significant changes**

## Additional Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [PyIceberg Documentation](https://py.iceberg.apache.org/)
- [AWS S3 Tables Documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html)
- [Athena Query Guide](ATHENA_QUERY_GUIDE.md)
- [Configuration Migration Guide](CONFIG_MIGRATION_GUIDE.md)
