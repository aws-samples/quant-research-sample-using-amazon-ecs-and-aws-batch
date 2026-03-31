# Athena Query Guide for S3 Tables

This guide provides comprehensive SQL query patterns for analyzing sentiment results stored in S3 Tables using Amazon Athena.

## Table of Contents

- [Overview](#overview)
- [Table Schema](#table-schema)
- [Basic Queries](#basic-queries)
- [Filtering Queries](#filtering-queries)
- [Aggregation Queries](#aggregation-queries)
- [Time-Travel Queries](#time-travel-queries)
- [Join Examples](#join-examples)
- [Query Optimization](#query-optimization)
- [Cost Optimization](#cost-optimization)

## Overview

The sentiment results are stored in an S3 Table using Apache Iceberg format. This provides:

- **SQL Access**: Query using standard SQL via Athena
- **Partition Pruning**: Automatic optimization for filtered queries
- **Time Travel**: Query historical versions of data
- **ACID Transactions**: Consistent reads during writes

### Athena Workgroup

Use the pre-configured Athena workgroup for optimal performance:

```bash
# Get workgroup name from CDK outputs
WORKGROUP=$(aws cloudformation describe-stacks \
  --stack-name bedrock-sentiment-stack-prod \
  --query 'Stacks[0].Outputs[?OutputKey==`AthenaWorkgroupName`].OutputValue' \
  --output text)

# Run query with workgroup
aws athena start-query-execution \
  --query-string "SELECT * FROM sentiment_results LIMIT 10" \
  --work-group $WORKGROUP
```

## Table Schema

```sql
-- Table: sentiment_results
-- Partitioned by: model_id, date(timestamp)

CREATE TABLE sentiment_results (
  article_id        STRING,      -- Article identifier
  symbol            STRING,      -- Stock ticker (e.g., 'AAPL')
  component         STRING,      -- 'content', 'headline', or 'summary'
  model_id          STRING,      -- Bedrock model ID
  sentiment_score   DOUBLE,      -- Sentiment score (nullable)
  model_name        STRING,      -- Human-readable model name
  timestamp         TIMESTAMP,   -- Article publication date
  job_name          STRING,      -- Processing job identifier
  rating_scale_min  INT,         -- Minimum rating value
  rating_scale_max  INT,         -- Maximum rating value
  created_at        TIMESTAMP    -- Record creation time
)
PARTITIONED BY (model_id, date)
STORED AS ICEBERG;
```

## Basic Queries

### Get All Results (Limited)

```sql
-- Always use LIMIT for exploratory queries
SELECT * FROM sentiment_results
LIMIT 100;
```

### Get Results for Specific Article

```sql
SELECT 
  article_id,
  symbol,
  component,
  model_name,
  sentiment_score,
  timestamp
FROM sentiment_results
WHERE article_id = 'article-12345';
```

### Get Latest Results

```sql
SELECT * FROM sentiment_results
ORDER BY created_at DESC
LIMIT 100;
```

## Filtering Queries

### Filter by Article ID

```sql
SELECT * FROM sentiment_results
WHERE article_id = 'article-12345';
```

### Filter by Symbol

```sql
SELECT 
  article_id,
  component,
  model_name,
  sentiment_score,
  timestamp
FROM sentiment_results
WHERE symbol = 'AAPL'
ORDER BY timestamp DESC;
```

### Filter by Component

```sql
-- Get only headline sentiment
SELECT * FROM sentiment_results
WHERE component = 'headline';

-- Get content and summary (exclude headline)
SELECT * FROM sentiment_results
WHERE component IN ('content', 'summary');
```

### Filter by Model (Uses Partition Pruning)

```sql
-- Efficient: Uses partition pruning
SELECT * FROM sentiment_results
WHERE model_id = 'anthropic.claude-v2';

-- Filter by model name
SELECT * FROM sentiment_results
WHERE model_name = 'claude-haiku-4-5';
```

### Filter by Date Range (Uses Partition Pruning)

```sql
-- Efficient: Uses partition pruning on date
SELECT * FROM sentiment_results
WHERE date(timestamp) BETWEEN date('2024-01-01') AND date('2024-01-31');

-- Filter by specific date
SELECT * FROM sentiment_results
WHERE date(timestamp) = date('2024-01-15');

-- Last 7 days
SELECT * FROM sentiment_results
WHERE timestamp >= current_timestamp - interval '7' day;
```

### Combined Filters

```sql
-- Symbol + Model + Date Range
SELECT 
  article_id,
  symbol,
  sentiment_score,
  timestamp
FROM sentiment_results
WHERE symbol = 'AAPL'
  AND model_id = 'anthropic.claude-v2'
  AND date(timestamp) >= date('2024-01-01')
ORDER BY timestamp DESC;
```

## Aggregation Queries

### Average Sentiment by Symbol

```sql
SELECT 
  symbol,
  AVG(sentiment_score) as avg_sentiment,
  COUNT(*) as result_count,
  MIN(sentiment_score) as min_sentiment,
  MAX(sentiment_score) as max_sentiment
FROM sentiment_results
WHERE sentiment_score IS NOT NULL
GROUP BY symbol
ORDER BY avg_sentiment DESC;
```

### Average Sentiment by Model

```sql
SELECT 
  model_name,
  AVG(sentiment_score) as avg_sentiment,
  STDDEV(sentiment_score) as stddev_sentiment,
  COUNT(*) as result_count
FROM sentiment_results
WHERE sentiment_score IS NOT NULL
GROUP BY model_name
ORDER BY avg_sentiment DESC;
```

### Sentiment Distribution

```sql
SELECT 
  CASE 
    WHEN sentiment_score < -3 THEN 'Very Negative'
    WHEN sentiment_score < 0 THEN 'Negative'
    WHEN sentiment_score = 0 THEN 'Neutral'
    WHEN sentiment_score < 3 THEN 'Positive'
    ELSE 'Very Positive'
  END as sentiment_category,
  COUNT(*) as count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM sentiment_results
WHERE sentiment_score IS NOT NULL
GROUP BY 1
ORDER BY 
  CASE sentiment_category
    WHEN 'Very Negative' THEN 1
    WHEN 'Negative' THEN 2
    WHEN 'Neutral' THEN 3
    WHEN 'Positive' THEN 4
    WHEN 'Very Positive' THEN 5
  END;
```

### Daily Sentiment Trend

```sql
SELECT 
  date(timestamp) as analysis_date,
  AVG(sentiment_score) as avg_sentiment,
  COUNT(DISTINCT article_id) as articles_analyzed
FROM sentiment_results
WHERE sentiment_score IS NOT NULL
  AND date(timestamp) >= date('2024-01-01')
GROUP BY date(timestamp)
ORDER BY analysis_date;
```

### Model Comparison for Same Articles

```sql
SELECT 
  article_id,
  symbol,
  MAX(CASE WHEN model_name = 'claude-haiku-4-5' THEN sentiment_score END) as claude_haiku,
  MAX(CASE WHEN model_name = 'claude-sonnet-4' THEN sentiment_score END) as claude_sonnet,
  MAX(CASE WHEN model_name = 'nova-pro-v1' THEN sentiment_score END) as nova_pro
FROM sentiment_results
WHERE component = 'content'
GROUP BY article_id, symbol
HAVING COUNT(DISTINCT model_name) >= 2
LIMIT 100;
```

### Count by Job

```sql
SELECT 
  job_name,
  COUNT(*) as total_results,
  COUNT(DISTINCT article_id) as unique_articles,
  COUNT(DISTINCT model_id) as models_used,
  MIN(created_at) as started_at,
  MAX(created_at) as completed_at
FROM sentiment_results
GROUP BY job_name
ORDER BY started_at DESC;
```



## Time-Travel Queries

Apache Iceberg supports time-travel queries to view historical data.

### Query Data as of Specific Timestamp

```sql
-- View data as it existed at a specific time
SELECT * FROM sentiment_results
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-15 10:00:00'
WHERE symbol = 'AAPL';
```

### Query Data as of Specific Snapshot

```sql
-- First, find available snapshots
SELECT * FROM "sentiment_results$snapshots";

-- Query specific snapshot by ID
SELECT * FROM sentiment_results
FOR VERSION AS OF 1234567890123
WHERE symbol = 'AAPL';
```

### Compare Current vs Historical Data

```sql
-- Compare current sentiment with 7 days ago
WITH current_data AS (
  SELECT symbol, AVG(sentiment_score) as current_sentiment
  FROM sentiment_results
  WHERE model_id = 'anthropic.claude-v2'
  GROUP BY symbol
),
historical_data AS (
  SELECT symbol, AVG(sentiment_score) as historical_sentiment
  FROM sentiment_results
  FOR SYSTEM_TIME AS OF (current_timestamp - interval '7' day)
  WHERE model_id = 'anthropic.claude-v2'
  GROUP BY symbol
)
SELECT 
  c.symbol,
  c.current_sentiment,
  h.historical_sentiment,
  c.current_sentiment - h.historical_sentiment as sentiment_change
FROM current_data c
JOIN historical_data h ON c.symbol = h.symbol
ORDER BY ABS(sentiment_change) DESC;
```

### View Table History

```sql
-- View snapshot history
SELECT 
  snapshot_id,
  parent_id,
  operation,
  manifest_list,
  summary
FROM "sentiment_results$snapshots"
ORDER BY committed_at DESC
LIMIT 10;

-- View table metadata
SELECT * FROM "sentiment_results$manifests";
```

## Join Examples

### Join with External Symbol Data

```sql
-- Assuming you have a symbols table with company info
SELECT 
  s.article_id,
  s.symbol,
  c.company_name,
  c.sector,
  s.sentiment_score,
  s.model_name
FROM sentiment_results s
JOIN company_info c ON s.symbol = c.symbol
WHERE s.model_id = 'anthropic.claude-v2'
  AND date(s.timestamp) = date('2024-01-15');
```

### Self-Join for Model Comparison

```sql
-- Compare two models' scores for same article/symbol/component
SELECT 
  a.article_id,
  a.symbol,
  a.component,
  a.sentiment_score as model_a_score,
  b.sentiment_score as model_b_score,
  ABS(a.sentiment_score - b.sentiment_score) as score_difference
FROM sentiment_results a
JOIN sentiment_results b 
  ON a.article_id = b.article_id 
  AND a.symbol = b.symbol 
  AND a.component = b.component
WHERE a.model_name = 'claude-haiku-4-5'
  AND b.model_name = 'claude-sonnet-4'
ORDER BY score_difference DESC
LIMIT 100;
```

## Query Optimization

### Use Partition Filters

The table is partitioned by `model_id` and `date`. Always include these in WHERE clauses:

```sql
-- GOOD: Uses partition pruning
SELECT * FROM sentiment_results
WHERE model_id = 'anthropic.claude-v2'
  AND date(timestamp) = date('2024-01-15');

-- BAD: Full table scan
SELECT * FROM sentiment_results
WHERE model_name = 'Claude 2';  -- model_name is not a partition key
```

### Select Only Needed Columns

```sql
-- GOOD: Select specific columns
SELECT article_id, symbol, sentiment_score
FROM sentiment_results
WHERE model_id = 'anthropic.claude-v2';

-- BAD: Select all columns
SELECT * FROM sentiment_results
WHERE model_id = 'anthropic.claude-v2';
```

### Use LIMIT for Exploration

```sql
-- Always use LIMIT when exploring data
SELECT * FROM sentiment_results
WHERE symbol = 'AAPL'
LIMIT 100;
```

### Avoid Functions on Partition Columns

```sql
-- GOOD: Direct comparison on partition column
SELECT * FROM sentiment_results
WHERE date(timestamp) = date('2024-01-15');

-- BAD: Function prevents partition pruning
SELECT * FROM sentiment_results
WHERE YEAR(timestamp) = 2024 AND MONTH(timestamp) = 1;
```

## Cost Optimization

### Understanding Athena Pricing

- Athena charges $5 per TB of data scanned
- Partition pruning reduces data scanned
- Column selection reduces data scanned
- Compression (Parquet/Iceberg) reduces data size

### Estimate Query Cost

```sql
-- Check data scanned for a query pattern
EXPLAIN ANALYZE
SELECT symbol, AVG(sentiment_score)
FROM sentiment_results
WHERE model_id = 'anthropic.claude-v2'
  AND date(timestamp) = date('2024-01-15')
GROUP BY symbol;
```

### Cost-Effective Patterns

| Query Pattern | Data Scanned | Est. Cost (100GB table) |
|---------------|--------------|-------------------------|
| Full table scan | 100GB | $0.50 |
| Filter by model_id | ~15GB | $0.075 |
| Filter by model_id + date | ~0.5GB | $0.0025 |
| Filter by article_id | ~0.1GB | $0.0005 |

### Tips for Reducing Costs

1. **Always filter by partition columns** (model_id, date)
2. **Select only needed columns** instead of SELECT *
3. **Use LIMIT** for exploratory queries
4. **Cache frequent queries** using Athena query results
5. **Schedule queries** during off-peak hours if possible

### Monitor Query Costs

```bash
# Check recent query costs
aws athena list-query-executions \
  --work-group sentiment-analysis-prod \
  --max-results 10

# Get query execution details including data scanned
aws athena get-query-execution \
  --query-execution-id <execution-id>
```

## Troubleshooting

### Query Timeout

If queries timeout, try:
1. Add more specific filters
2. Reduce date range
3. Use LIMIT clause
4. Check if table needs compaction

### No Results Returned

Check:
1. Partition filters match existing data
2. Date format is correct (`date('YYYY-MM-DD')`)
3. Model ID is exact match (case-sensitive)

### Slow Queries

Optimize by:
1. Adding partition filters (model_id, date)
2. Selecting fewer columns
3. Using approximate functions for aggregations
4. Checking table statistics are up to date

## Additional Resources

- [Amazon Athena Documentation](https://docs.aws.amazon.com/athena/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [S3 Tables Operations Guide](S3_TABLES_OPERATIONS.md)
- [Configuration Migration Guide](CONFIG_MIGRATION_GUIDE.md)
