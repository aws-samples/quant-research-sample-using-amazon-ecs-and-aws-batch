# Configuration Migration Guide

This guide helps you migrate existing configurations to support S3 Tables and streaming batch processing.

## Table of Contents

- [Overview](#overview)
- [New Configuration Fields](#new-configuration-fields)
- [Example Configurations](#example-configurations)
- [Backward Compatibility](#backward-compatibility)
- [Troubleshooting](#troubleshooting)
- [Cost Estimation](#cost-estimation)

## Overview

The streaming batch S3 Tables feature introduces new configuration fields while maintaining full backward compatibility with existing configurations.

### What's New

| Feature | Old Approach | New Approach |
|---------|--------------|--------------|
| Data Loading | Load all into DataFrame | Stream in batches |
| Output Format | Parquet files | S3 Tables (Iceberg) |
| Memory Usage | 3-4GB for 265K articles | ~100MB constant |
| Querying | Manual Parquet parsing | SQL via Athena |
| Updates | Full reprocessing | Upsert support |

## New Configuration Fields

### Streaming Batch Parameters

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enable_streaming_batch` | boolean | `true` | Enable streaming batch processing |
| `batch_size` | integer | `1000` | Articles per batch (100-10,000) |

### S3 Tables Parameters

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `s3_table_name` | string | Required* | S3 Table name for results |
| `s3_table_warehouse` | string | Required* | S3 warehouse path for Iceberg |
| `catalog_name` | string | `"default"` | Iceberg catalog name |
| `write_mode` | string | `"upsert"` | Write mode: `append` or `upsert` |

*Required when `enable_streaming_batch` is `true`

### Field Details

#### enable_streaming_batch

Controls whether to use streaming batch processing with S3 Tables output.

```json
{
  "enable_streaming_batch": true
}
```

- `true`: Process articles in batches, write to S3 Tables
- `false`: Load all articles into DataFrame, write to Parquet (legacy)

#### batch_size

Number of articles to process in each streaming batch.

```json
{
  "batch_size": 1000
}
```

**Guidelines:**
- Minimum: 100 (too small = excessive overhead)
- Maximum: 10,000 (too large = memory pressure)
- Recommended: 1,000-5,000

**Memory Impact:**
| Batch Size | Approx. Memory |
|------------|----------------|
| 100 | ~10MB |
| 1,000 | ~100MB |
| 5,000 | ~500MB |
| 10,000 | ~1GB |

#### s3_table_name

Name of the S3 Table for storing results.

```json
{
  "s3_table_name": "sentiment-results-prod"
}
```

**Naming Conventions:**
- Use environment suffix: `sentiment-results-{env}`
- Lowercase with hyphens
- Must match CDK-created table name

#### s3_table_warehouse

S3 path to the Iceberg warehouse.

```json
{
  "s3_table_warehouse": "s3://my-table-bucket/warehouse/"
}
```

**Format:** `s3://bucket-name/path/`

Get from CDK outputs:
```bash
aws cloudformation describe-stacks \
  --stack-name bedrock-sentiment-stack-prod \
  --query 'Stacks[0].Outputs[?OutputKey==`TableWarehousePath`].OutputValue' \
  --output text
```

#### catalog_name

Iceberg catalog name for PyIceberg.

```json
{
  "catalog_name": "default"
}
```

Usually `"default"` unless using custom catalog configuration.

#### write_mode

How to handle existing records.

```json
{
  "write_mode": "upsert"
}
```

| Mode | Behavior | Use Case |
|------|----------|----------|
| `append` | Add all records as new | Initial load, no duplicates |
| `upsert` | Update existing, insert new | Incremental updates |



## Example Configurations

### Minimal S3 Tables Configuration

Add only required fields to existing config:

```json
{
  "input_s3_bucket": "s3://my-input-bucket/",
  "output_s3_bucket": "s3://my-output-bucket/",
  "prompt_s3_uri": "s3://my-prompts/user-prompt.txt",
  "system_prompt_s3_uri": "s3://my-prompts/system-prompt.txt",
  "rating_scale": { "min": -5, "max": 5 },
  "job_name": "daily-analysis",
  "models": [...],
  
  "s3_table_name": "sentiment-results-prod",
  "s3_table_warehouse": "s3://my-table-bucket/warehouse/"
}
```

### Full S3 Tables Configuration

All streaming batch parameters:

```json
{
  "input_s3_bucket": "s3://my-input-bucket/",
  "output_s3_bucket": "s3://my-output-bucket/",
  "prompt_s3_uri": "s3://my-prompts/user-prompt.txt",
  "system_prompt_s3_uri": "s3://my-prompts/system-prompt.txt",
  "rating_scale": { "min": -5, "max": 5 },
  "max_articles": null,
  "job_name": "production-run",
  
  "parallel_reading_enabled": true,
  "concurrent_download_limit": 75,
  "download_chunk_size": 1000,
  
  "enable_streaming_batch": true,
  "batch_size": 1000,
  "s3_table_name": "sentiment-results-prod",
  "s3_table_warehouse": "s3://my-table-bucket/warehouse/",
  "catalog_name": "default",
  "write_mode": "upsert",
  
  "models": [
    {
      "model_id": "global.anthropic.claude-haiku-4-5-20251001-v1:0",
      "model_name": "claude-haiku-4-5",
      "requests_per_minute": 250,
      "tokens_per_minute": 1000000
    }
  ]
}
```

### Development Configuration

Lower batch size for faster iteration:

```json
{
  "input_s3_bucket": "s3://dev-input/",
  "output_s3_bucket": "s3://dev-output/",
  "prompt_s3_uri": "s3://dev-prompts/user-prompt.txt",
  "system_prompt_s3_uri": "s3://dev-prompts/system-prompt.txt",
  "rating_scale": { "min": -5, "max": 5 },
  "max_articles": 100,
  "job_name": "dev-test",
  
  "enable_streaming_batch": true,
  "batch_size": 100,
  "s3_table_name": "sentiment-results-dev",
  "s3_table_warehouse": "s3://dev-table-bucket/warehouse/",
  "write_mode": "append",
  
  "models": [
    {
      "model_id": "global.anthropic.claude-haiku-4-5-20251001-v1:0",
      "model_name": "claude-haiku-4-5",
      "requests_per_minute": 100,
      "tokens_per_minute": 500000
    }
  ]
}
```

### Legacy Mode Configuration

Disable streaming batch to use DataFrame + Parquet:

```json
{
  "input_s3_bucket": "s3://my-input-bucket/",
  "output_s3_bucket": "s3://my-output-bucket/",
  "prompt_s3_uri": "s3://my-prompts/user-prompt.txt",
  "system_prompt_s3_uri": "s3://my-prompts/system-prompt.txt",
  "rating_scale": { "min": -5, "max": 5 },
  "job_name": "legacy-run",
  
  "enable_streaming_batch": false,
  
  "models": [...]
}
```

## Backward Compatibility

### Existing Configurations Work Unchanged

Old configurations without new fields continue to work:

```json
{
  "input_s3_bucket": "s3://my-bucket/",
  "output_s3_bucket": "s3://my-output/",
  "prompt_s3_uri": "s3://prompts/user.txt",
  "system_prompt_s3_uri": "s3://prompts/system.txt",
  "rating_scale": { "min": -5, "max": 5 },
  "job_name": "my-job",
  "models": [...]
}
```

**Behavior with missing fields:**
- `enable_streaming_batch`: Defaults to `true`
- `batch_size`: Defaults to `1000`
- `s3_table_name`: Required if streaming enabled
- `s3_table_warehouse`: Required if streaming enabled
- `catalog_name`: Defaults to `"default"`
- `write_mode`: Defaults to `"upsert"`

### Preserved Features

All existing features work unchanged:
- ✅ Rate limiting per model
- ✅ Retry logic with exponential backoff
- ✅ Prompt template loading from S3
- ✅ Parallel S3 reading
- ✅ Model filtering with `--model-id`
- ✅ CloudWatch logging

### Migration Steps

1. **Add S3 Tables fields** to existing config
2. **Deploy CDK** with S3 Tables enabled
3. **Test** with small batch (`max_articles: 100`)
4. **Validate** results in Athena
5. **Remove** `max_articles` limit for production

## Troubleshooting

### Validation Errors

#### "batch_size must be between 100 and 10000"

```json
{
  "batch_size": 50  // Invalid: too small
}
```

**Fix:** Use value between 100 and 10,000.

#### "write_mode must be 'append' or 'upsert'"

```json
{
  "write_mode": "overwrite"  // Invalid
}
```

**Fix:** Use `"append"` or `"upsert"`.

#### "s3_table_name is required when streaming enabled"

```json
{
  "enable_streaming_batch": true
  // Missing s3_table_name
}
```

**Fix:** Add `s3_table_name` and `s3_table_warehouse`.

### Runtime Errors

#### "NoSuchTableError: Table not found"

**Cause:** S3 Table doesn't exist or wrong name.

**Fix:**
1. Verify CDK deployed S3 Tables
2. Check table name matches CDK output
3. Verify warehouse path is correct

```bash
# Get correct table name from CDK
aws cloudformation describe-stacks \
  --stack-name bedrock-sentiment-stack-prod \
  --query 'Stacks[0].Outputs[?OutputKey==`SentimentTableName`].OutputValue' \
  --output text
```

#### "AccessDenied for S3 Tables"

**Cause:** IAM role missing S3 Tables permissions.

**Fix:** Ensure job role has:
```json
{
  "Action": [
    "s3tables:PutTableData",
    "s3tables:GetTable",
    "s3tables:UpdateTableMetadata"
  ],
  "Resource": "arn:aws:s3tables:*:*:bucket/*/table/*"
}
```

#### "CommitFailedException: Concurrent modification"

**Cause:** Multiple writers to same partition.

**Fix:** Built-in retry handles this. If persistent:
1. Reduce concurrent jobs
2. Use different partitions
3. Increase retry attempts

### Performance Issues

#### Slow Batch Processing

**Symptoms:** Each batch takes > 5 minutes.

**Causes & Fixes:**
1. **Batch too large:** Reduce `batch_size` to 1000
2. **Rate limiting:** Check model rate limits
3. **S3 throttling:** Reduce `concurrent_download_limit`

#### High Memory Usage

**Symptoms:** OOM errors despite streaming.

**Causes & Fixes:**
1. **Batch too large:** Reduce `batch_size`
2. **Memory leak:** Check for unclosed resources
3. **Large articles:** Filter oversized content

## Cost Estimation

### S3 Tables Storage Costs

| Data Volume | Monthly Storage | Notes |
|-------------|-----------------|-------|
| 1M records | ~$0.50 | ~500MB compressed |
| 10M records | ~$5.00 | ~5GB compressed |
| 100M records | ~$50.00 | ~50GB compressed |

**Storage Pricing:** $0.023/GB/month (S3 Standard)

### S3 Tables Write Costs

| Operation | Cost | Notes |
|-----------|------|-------|
| PUT request | $0.005/1000 | Per batch write |
| S3 Tables overhead | ~$0.01/1000 | Metadata operations |

**Example:** 1M records in 1000-record batches = 1000 writes = ~$0.015

### Athena Query Costs

| Query Type | Data Scanned | Cost |
|------------|--------------|------|
| Full scan (100M records) | ~50GB | $0.25 |
| Partition filter | ~5GB | $0.025 |
| Point lookup | ~0.1GB | $0.0005 |

**Query Pricing:** $5/TB scanned

### Monthly Cost Examples

#### Small Workload (1M records/month)

| Component | Cost |
|-----------|------|
| Storage | $0.50 |
| Writes (1000 batches) | $0.02 |
| Queries (100/month) | $2.50 |
| **Total** | **~$3/month** |

#### Medium Workload (10M records/month)

| Component | Cost |
|-----------|------|
| Storage | $5.00 |
| Writes (10,000 batches) | $0.15 |
| Queries (500/month) | $12.50 |
| **Total** | **~$18/month** |

#### Large Workload (100M records/month)

| Component | Cost |
|-----------|------|
| Storage | $50.00 |
| Writes (100,000 batches) | $1.50 |
| Queries (1000/month) | $25.00 |
| **Total** | **~$77/month** |

### Cost Optimization Tips

1. **Use partition filters** in queries to reduce data scanned
2. **Select specific columns** instead of SELECT *
3. **Batch writes** (1000+ records) to minimize API calls
4. **Set snapshot retention** to limit storage growth
5. **Use lifecycle policies** for old data

## Additional Resources

- [README.md](../README.md) - System overview
- [DEPLOYMENT_GUIDE.md](../DEPLOYMENT_GUIDE.md) - Deployment instructions
- [Athena Query Guide](ATHENA_QUERY_GUIDE.md) - SQL query patterns
- [S3 Tables Operations](S3_TABLES_OPERATIONS.md) - Maintenance procedures
