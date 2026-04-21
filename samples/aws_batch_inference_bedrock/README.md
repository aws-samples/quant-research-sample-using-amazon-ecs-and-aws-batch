# AWS Bedrock News Sentiment Analysis System

A scalable batch processing system that analyzes news articles using AWS Bedrock AI models to generate sentiment scores for stock symbols. Runs multiple models in parallel on AWS Batch with streaming output to S3 Tables (Apache Iceberg).

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Scripts](#scripts)
- [Configuration](#configuration)
- [Usage](#usage)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Project Structure](#project-structure)
- [Documentation](#documentation)

## Overview

The system processes news articles from S3, analyzes sentiment using multiple AWS Bedrock models in parallel, and writes structured results to S3 Tables (Apache Iceberg) or legacy Parquet files. Each model runs as a separate AWS Batch job with independent rate limiting.

**Key Capabilities:**
- Parallel processing across 7+ Bedrock models simultaneously
- Parallel S3 reading — 10-20 minutes for 265K+ articles (vs 2+ hours sequential)
- Streaming batch processing — constant ~100MB memory regardless of dataset size
- S3 Tables output — Apache Iceberg format with SQL queries via Athena
- ACID transactions with upsert support for incremental updates
- Independent rate limiting per model (respects AWS quotas)
- Automatic retry with exponential backoff
- Support for both standard model IDs and custom model ARNs (provisioned throughput)
- Infrastructure as Code (AWS CDK) with no hardcoded account values

## Architecture

```
┌─────────────┐
│   S3 Input  │ ← News Articles (JSON)
└──────┬──────┘
       │
       ↓
┌─────────────────────────────────────────────────┐
│           AWS Batch (7 Parallel Jobs)           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐     │
│  │ Claude   │  │ Claude   │  │  Nova    │ ... │
│  │ Haiku    │  │ Sonnet   │  │  Pro     │     │
│  └──────────┘  └──────────┘  └──────────┘     │
│         ↓              ↓              ↓         │
│    ┌────────────────────────────────────┐      │
│    │      AWS Bedrock (Converse API)    │      │
│    └────────────────────────────────────┘      │
└─────────────────────────────────────────────────┘
       │
       ↓
┌─────────────────────────────────────────────────┐
│  S3 Tables (Apache Iceberg)  ← Recommended      │
│  • SQL queries via Athena                       │
│  • ACID transactions & upserts                  │
│  • Time-travel queries                          │
│  • Automatic compaction                         │
└─────────────────────────────────────────────────┘
```

**Components:** AWS Batch (Fargate), AWS Bedrock, Amazon S3, S3 Tables, Amazon Athena, Amazon ECR, CloudWatch, VPC with private subnets.

## Quick Start

### Prerequisites

- AWS account with Bedrock model access enabled
- Docker or Podman
- AWS CLI v2 configured
- Node.js 18+ (for CDK)
- `jq` (for `generate-config.sh`)

### First Time Setup

```bash
# 1. Deploy everything (bootstraps CDK, deploys infra, generates config, uploads prompts, submits jobs)
./scripts/deploy-and-run.sh examples/config-example.json prod

# 2. Monitor
aws logs tail /aws/batch/bedrock-sentiment-prod --follow
```

The script automatically:
- Bootstraps CDK if not already done
- Deploys all infrastructure
- Detects placeholder values in the example config and generates `config.json` with real bucket names
- Uploads example prompts to S3 (if not already present)
- Submits jobs for all models defined in the config

### Iterating on Configuration

```bash
vim config.json  # Change max_articles, rate limits, models, etc.
./scripts/update-config-and-run.sh config.json prod   # ~30 seconds, no rebuild
```

## Scripts

All scripts live in `scripts/` and are run from the project root.

### Main Scripts

| Script | When to Use | Time |
|--------|-------------|------|
| `scripts/deploy.sh [config] [env]` | Deploy infrastructure only (run jobs later) | ~10-15 min |
| `scripts/deploy-and-run.sh [config] [env]` | Deploy infrastructure and submit jobs | ~10-15 min |
| `scripts/update-config-and-run.sh [config] [env]` | Config-only changes (fast iteration) | ~30 sec |
| `scripts/generate-config.sh [env] [template]` | Generate `config.json` from stack outputs | ~5 sec |

### Monitoring Scripts

| Script | Description |
|--------|-------------|
| `scripts/check-model-rates.sh [minutes]` | One-time Bedrock invocation rate snapshot |
| `scripts/monitor-model-rates.sh [minutes]` | Continuous rate monitoring (refreshes every 30s) |

### What `deploy.sh` Does

1. Generates unique timestamped image tag
2. Builds Docker image (linux/amd64)
3. Bootstraps CDK if not already done
4. Deploys CDK infrastructure with image tag
5. Pushes image to ECR
6. Auto-generates `config.json` from stack outputs, uploads config and example prompts to S3

After deploying, run jobs with: `./scripts/update-config-and-run.sh config.json prod`

### What `deploy-and-run.sh` Does

1. Generates unique timestamped image tag
2. Builds Docker image (linux/amd64)
3. Bootstraps CDK if not already done
4. Deploys CDK infrastructure with image tag
5. Auto-generates `config.json` from stack outputs (if config has placeholder values)
6. Pushes image to ECR
7. Uploads example prompts to S3 (if not already present)
8. Uploads config to S3 and submits jobs for all models in config

### What `update-config-and-run.sh` Does

1. Validates JSON syntax
2. Looks up infrastructure from CloudFormation
3. Uploads config to S3
4. Reads models from config and submits a job for each

### Rollback

```bash
# Find previous image tags
aws ecr describe-images --repository-name bedrock-sentiment-analyzer-prod \
  --query 'sort_by(imageDetails,& imagePushedAt)[-5:]' --output table

# Deploy with a specific tag
IMAGE_TAG=20250201-120000 ./scripts/deploy-and-run.sh config.json prod
```

## Configuration

### Generating a Config

After deploying the CDK stack, generate a working `config.json` with actual bucket names:

```bash
./scripts/generate-config.sh prod                          # uses examples/config-example.json as template
./scripts/generate-config.sh prod examples/config-aws.json  # use a different template
```

### config.json Reference

```json
{
  "input_s3_bucket": "s3://your-input-bucket/",
  "output_s3_bucket": "s3://your-output-bucket/",
  "prompt_s3_uri": "s3://your-prompts-bucket/user-prompt.txt",
  "system_prompt_s3_uri": "s3://your-prompts-bucket/system-prompt.txt",
  "rating_scale": { "min": -5, "max": 5 },
  "max_articles": 100,
  "job_name": "production-run",
  "parallel_reading_enabled": true,
  "concurrent_download_limit": 75,
  "download_chunk_size": 1000,
  "enable_streaming_batch": true,
  "batch_size": 1000,
  "s3_table_name": "sentiment-results-prod",
  "s3_table_warehouse": "s3://my-table-bucket/warehouse/",
  "write_mode": "upsert",
  "models": [
    {
      "model_id": "global.anthropic.claude-haiku-4-5-20251001-v1:0",
      "model_name": "claude-haiku-4-5",
      "requests_per_minute": 120,
      "tokens_per_minute": 1000000
    }
  ]
}
```

### Parameter Reference

**Core:**
| Parameter | Description | Default |
|-----------|-------------|---------|
| `input_s3_bucket` | S3 URI for input articles (JSON) | Required |
| `output_s3_bucket` | S3 URI for output (Parquet, legacy mode) | Required |
| `prompt_s3_uri` | S3 URI for user prompt template | Required |
| `system_prompt_s3_uri` | S3 URI for system prompt | Required |
| `rating_scale` | `{min, max}` for sentiment scores | Required |
| `max_articles` | Limit articles processed (`null` = all) | `null` |
| `job_name` | Identifier for this run | Required |

**Parallel Reading:**
| Parameter | Description | Default |
|-----------|-------------|---------|
| `parallel_reading_enabled` | Enable parallel S3 reading | `true` |
| `concurrent_download_limit` | Max simultaneous S3 downloads (50-100) | `75` |
| `download_chunk_size` | Files per batch chunk | `1000` |

**Streaming Batch & S3 Tables:**
| Parameter | Description | Default |
|-----------|-------------|---------|
| `enable_streaming_batch` | Enable streaming batch mode | `true` |
| `batch_size` | Articles per streaming batch (100-10,000) | `1000` |
| `s3_table_name` | S3 Table name for results | Required when streaming |
| `s3_table_warehouse` | S3 warehouse path for Iceberg | Required when streaming |
| `catalog_name` | Iceberg catalog name | `default` |
| `write_mode` | `append` or `upsert` | `upsert` |

**Models:** Each entry needs either `model_id` or `model_arn` (not both), plus `model_name`, `requests_per_minute`, and `tokens_per_minute`. See [docs/MODEL_ARN_SUPPORT.md](docs/MODEL_ARN_SUPPORT.md).

### Output Schema

```
article_id        : string    # Article identifier
symbol            : string    # Stock ticker
component         : string    # content/headline/summary
sentiment_score   : float     # Sentiment score (or null)
model_id          : string    # Full Bedrock model ID
model_name        : string    # Human-readable name
timestamp         : datetime  # Article publication date (from created_at)
job_name          : string    # Job identifier
rating_scale_min  : int       # Min rating value
rating_scale_max  : int       # Max rating value
created_at        : datetime  # Record creation time
```

## Usage

### Submit Jobs

```bash
# All models from config (recommended)
./scripts/update-config-and-run.sh config.json prod

# Manual single-model submission
STACK_NAME="bedrock-sentiment-stack-prod"
JOB_QUEUE=$(aws cloudformation describe-stacks --stack-name $STACK_NAME \
  --query 'Stacks[0].Outputs[?OutputKey==`JobQueueArn`].OutputValue' --output text)
JOB_DEFINITION=$(aws cloudformation describe-stacks --stack-name $STACK_NAME \
  --query 'Stacks[0].Outputs[?OutputKey==`JobDefinitionArn`].OutputValue' --output text)
PROMPT_BUCKET=$(aws cloudformation describe-stacks --stack-name $STACK_NAME \
  --query 'Stacks[0].Outputs[?OutputKey==`PromptBucketName`].OutputValue' --output text)

aws batch submit-job \
  --job-name sentiment-test-$(date +%Y%m%d-%H%M%S) \
  --job-queue $JOB_QUEUE \
  --job-definition $JOB_DEFINITION \
  --container-overrides "{
    \"command\": [
      \"--config\", \"s3://$PROMPT_BUCKET/configs/prod/config.json\",
      \"--model-id\", \"global.anthropic.claude-haiku-4-5-20251001-v1:0\"
    ]
  }"
```

### Query Results with Athena

```sql
SELECT symbol, model_name, AVG(sentiment_score) as avg_sentiment
FROM sentiment_results
WHERE date(timestamp) BETWEEN date('2024-01-01') AND date('2024-01-31')
GROUP BY symbol, model_name
ORDER BY avg_sentiment DESC;
```

See [docs/ATHENA_QUERY_GUIDE.md](docs/ATHENA_QUERY_GUIDE.md) for comprehensive query examples.

### Local Testing

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python src/main.py --config examples/config-test.json --log-level DEBUG
```

### Running Tests

```bash
pytest                    # all tests
pytest tests/test_config.py  # single file
```

## Monitoring

### Job Status

```bash
aws batch list-jobs --job-queue $JOB_QUEUE --job-status RUNNING \
  --query 'jobSummaryList[*].[jobName,status,createdAt]' --output table

aws batch describe-jobs --jobs <job-id>
```

### Logs

```bash
# Application logs
aws logs tail /aws/batch/bedrock-sentiment-prod --follow

# Filter errors
aws logs filter-log-events \
  --log-group-name /aws/batch/bedrock-sentiment-prod \
  --filter-pattern "ERROR"

# VPC Flow Logs
aws logs tail /aws/vpc/bedrock-sentiment-prod --follow
```

### Rate Monitoring

```bash
./scripts/check-model-rates.sh 10     # One-time snapshot (last 10 min)
./scripts/monitor-model-rates.sh 10   # Continuous (refreshes every 30s)
```

## Troubleshooting

### Job Fails with AccessDeniedException

Missing Bedrock permissions for inference profiles. Ensure IAM role has:
```json
{ "Action": "bedrock:InvokeModel", "Resource": "arn:aws:bedrock:*:ACCOUNT:inference-profile/*" }
```

### Out of Memory (exit code 137)

Increase Fargate memory in CDK or reduce workload:

| Articles | Recommended Memory |
|----------|--------------------|
| < 100K | 8 GB |
| 100K-300K | 16 GB |
| 300K-500K | 24 GB |
| 500K-1M | 30 GB (Fargate max) |

Formula: `(Article Count × 15KB) + 8GB overhead`

Temporary workaround — disable parallel reading:
```json
{ "parallel_reading_enabled": false }
```

### S3 Config Errors (NoSuchKey, NoSuchBucket, AccessDenied)

```bash
aws s3 ls s3://your-bucket/configs/config.json
aws s3 cp config.json s3://your-bucket/configs/config.json
python -m json.tool config.json
```

### Rate Limiting Too Restrictive

```bash
aws service-quotas list-service-quotas --service-code bedrock \
  --query 'Quotas[?contains(QuotaName, `Claude`)]'
```

### Architecture Mismatch

Always build for AMD64 (Fargate requirement):
```bash
podman build --platform linux/amd64 -t image:tag .
```

## Project Structure

```
.
├── src/                        # Application source code
│   ├── main.py                 #   CLI entry point
│   ├── orchestrator.py         #   Batch workflow orchestration
│   ├── sentiment_analyzer.py   #   Sentiment analysis logic
│   ├── bedrock_client.py       #   Bedrock API client with retry
│   ├── rate_limiter.py         #   Token bucket rate limiting
│   ├── article_processor.py    #   Article parsing and task creation
│   ├── s3_operations.py        #   S3 read/write operations
│   ├── s3_tables_writer.py     #   S3 Tables (Iceberg) writer
│   ├── config.py               #   Configuration management
│   └── logging_config.py       #   Logging setup
├── tests/                      # Test suite
│   ├── conftest.py             #   Pytest configuration
│   ├── test_config.py
│   ├── test_main.py
│   ├── test_orchestrator.py
│   ├── test_streaming_orchestrator.py
│   ├── test_integration.py
│   ├── test_streaming_batch_integration.py
│   ├── test_article_processor.py
│   ├── test_rate_limiter.py
│   ├── test_s3_operations.py
│   ├── test_s3_tables_writer.py
│   └── test_submit_script.py
├── scripts/                    # Deployment and monitoring
│   ├── deploy.sh               #   Infrastructure-only deployment
│   ├── deploy-and-run.sh       #   Full deployment + job submission
│   ├── update-config-and-run.sh #  Config-only update
│   ├── generate-config.sh      #   Generate config from stack outputs
│   ├── submit-parallel-jobs.sh #   Job submission (internal)
│   ├── check-model-rates.sh    #   One-time rate check
│   └── monitor-model-rates.sh  #   Continuous rate monitoring
├── benchmarks/                 # Performance benchmarks (dev only)
│   ├── benchmark_performance.py
│   ├── benchmark_streaming_batch.py
│   └── calculate_processing_time.py
├── cdk/                        # Infrastructure as Code
│   ├── bin/app.ts
│   ├── lib/bedrock-sentiment-analyzer-stack.ts
│   ├── lib/s3-tables-construct.ts
│   └── test/
├── docs/                       # Reference documentation
│   ├── MODEL_ARN_SUPPORT.md
│   ├── ATHENA_QUERY_GUIDE.md
│   ├── S3_TABLES_OPERATIONS.md
│   └── CONFIG_MIGRATION_GUIDE.md
├── examples/                   # Example configs and prompts
│   ├── config-example.json
│   ├── config-test.json
│   ├── system-prompt-example.txt
│   └── user-prompt-example.txt
├── Dockerfile
├── requirements.txt
├── pyproject.toml
├── README.md
└── DEPLOYMENT.md
```

## Documentation

| Document | Description |
|----------|-------------|
| `README.md` (this file) | Overview, quick start, configuration, usage |
| `DEPLOYMENT.md` | CDK infrastructure, deployment steps, security, memory sizing |
| `docs/MODEL_ARN_SUPPORT.md` | Custom model ARNs (provisioned throughput) |
| `docs/ATHENA_QUERY_GUIDE.md` | SQL query patterns for S3 Tables |
| `docs/S3_TABLES_OPERATIONS.md` | Upsert vs append, maintenance, snapshots |
| `docs/CONFIG_MIGRATION_GUIDE.md` | Migrating configs to streaming batch |
| `examples/README.md` | Example configs and prompt files |
