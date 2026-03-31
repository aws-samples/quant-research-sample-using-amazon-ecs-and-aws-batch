# Deployment Guide

Detailed deployment instructions for the AWS Bedrock News Sentiment Analysis System. For a quick overview, see [README.md](README.md).

## Table of Contents

- [Prerequisites](#prerequisites)
- [CDK Infrastructure](#cdk-infrastructure)
- [Step-by-Step Deployment](#step-by-step-deployment)
- [Memory Configuration](#memory-configuration)
- [S3 Tables](#s3-tables)
- [Security](#security)
- [Performance Tuning](#performance-tuning)
- [Rollback](#rollback)
- [Cleanup](#cleanup)
- [Cost Estimates](#cost-estimates)

## Prerequisites

### AWS Account Requirements

- Bedrock model access enabled (Claude, Nova, Titan, Llama, etc.)
- Service quotas for AWS Batch and Fargate
- Sufficient S3 storage for input/output data

### Required Tools

- AWS CLI v2 configured with appropriate credentials
- Docker or Podman
- Node.js 18+ (for CDK)
- `jq` (for `generate-config.sh`)
- Python 3.9+ with pip (for local testing)

### IAM Permissions for Deploying

Your deployment user/role needs permissions for: CloudFormation, IAM role creation, VPC/networking, S3, S3 Tables, ECR, AWS Batch, Bedrock, Athena, CloudWatch.

## CDK Infrastructure

The CDK stack (`cdk/lib/bedrock-sentiment-analyzer-stack.ts`) creates:

- **3 S3 Buckets** — input, output, prompts (encrypted, versioned, no public access)
- **S3 Table Bucket** — Apache Iceberg tables for streaming output (optional, default: enabled)
- **Athena Workgroup** — pre-configured for querying S3 Tables
- **ECR Repository** — Docker images with scanning and immutable tags
- **VPC** — private subnets, NAT Gateway, VPC Flow Logs
- **AWS Batch** — Fargate compute environment, job queue, job definition
- **IAM Roles** — execution role, job role (least privilege), service role
- **CloudWatch Log Groups** — application and VPC flow logs

### CDK Context Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `environment` | `dev` | Environment name (`dev`, `staging`, `prod`) |
| `resourcePrefix` | `bedrock-sentiment` | Prefix for all resource names |
| `maxVcpus` | `16` | Max vCPUs for Batch compute |
| `vpcCidr` | `10.0.0.0/16` | VPC CIDR block |
| `costCenter` | `Engineering` | Cost allocation tag |
| `enableS3Tables` | `true` | Create S3 Tables infrastructure |
| `s3TablesSnapshotRetentionDays` | `7` | Snapshot retention |
| `externalInputBuckets` | (empty) | Comma-separated external bucket names for read access |
| `crossEnvironmentAccess` | `false` | Allow dev/prod to share resources |

### Stack Outputs

After deployment, the stack exports:

- `InputBucketName`, `OutputBucketName`, `PromptBucketName`
- `EcrRepositoryUri`
- `JobQueueArn`, `JobDefinitionArn`
- `TableBucketArn`, `SentimentTableName`, `TableWarehousePath` (if S3 Tables enabled)
- `AthenaWorkgroupName` (if S3 Tables enabled)
- `VpcId`

## Step-by-Step Deployment

### One-Command Deployment (Recommended)

For deploy-only (run jobs later):

```bash
# Deploy infrastructure, generate config, upload prompts — no jobs submitted
./scripts/deploy.sh examples/config-example.json prod

# Review/edit config, then run
vim config.json
./scripts/update-config-and-run.sh config.json prod
```

To deploy and immediately submit jobs:

```bash
./scripts/deploy-and-run.sh examples/config-example.json prod
```

Both scripts automatically:
1. Build Docker image (`podman build --platform linux/amd64` or docker)
2. Bootstrap CDK if not already done
3. Deploy CDK infrastructure with timestamped image tag
4. Detect placeholder values in config → auto-generate `config.json` with real bucket names
5. Push image to ECR
6. Upload example prompts to S3 (if not already present)

`deploy-and-run.sh` additionally uploads config to S3 and submits jobs for all models defined in the config.

### Generate a Config Separately (Optional)

If you need to regenerate `config.json` from stack outputs without redeploying:

```bash
./scripts/generate-config.sh prod
```

This queries CloudFormation outputs and patches `examples/config-example.json` with your actual bucket names (including S3 Tables fields), producing `config.json`.

### Upload Custom Prompts (Optional)

The deploy script uploads example prompts automatically on first run. To use custom prompts:

```bash
PROMPT_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name bedrock-sentiment-stack-prod \
  --query 'Stacks[0].Outputs[?OutputKey==`PromptBucketName`].OutputValue' --output text)

aws s3 cp your-system-prompt.txt s3://$PROMPT_BUCKET/system-prompt.txt
aws s3 cp your-user-prompt.txt s3://$PROMPT_BUCKET/user-prompt.txt
```

### Iterate on Configuration

```bash
./scripts/update-config-and-run.sh config.json prod
```

### Manual CDK Deployment (Advanced)

If you need more control:

```bash
cd cdk
npx cdk diff --context environment=prod
npx cdk deploy \
  --context environment=prod \
  --context maxVcpus=32 \
  --context externalInputBuckets=my-shared-data-bucket \
  --context crossEnvironmentAccess=true \
  --outputs-file outputs.json \
  --require-approval never
```

## Memory Configuration

Parallel reading loads articles into a pandas DataFrame. Memory scales with article count.

### Sizing Guidelines

| Articles | Memory | vCPU | Est. Cost/Job |
|----------|--------|------|---------------|
| < 100K | 8 GB | 2 | ~$0.23 |
| 100K-300K | 16 GB | 4 | ~$0.46 |
| 300K-500K | 24 GB | 4 | ~$0.53 |
| 500K-1M | 30 GB | 8 | ~$0.92 |

**Formula:** `Required Memory (GB) = (Article Count × 15KB) / 1024³ + 8GB overhead`

Memory is configured in the CDK stack's job definition:

```typescript
memory: cdk.Size.mebibytes(16384),  // 16GB
cpu: 4,
```

Redeploy after changing: `./scripts/deploy-and-run.sh config.json prod`

### Fargate Memory Tiers

| vCPU | Memory Options |
|------|----------------|
| 2 | 4-16 GB (1 GB increments) |
| 4 | 8-30 GB (1 GB increments) |
| 8 | 16-60 GB (4 GB increments) |
| 16 | 32-120 GB (8 GB increments) |

## S3 Tables

S3 Tables are enabled by default and provide Apache Iceberg format output.

### What Gets Created

1. **S3 Table Bucket** — managed bucket for Iceberg tables
2. **Sentiment Results Table** — schema for sentiment data, partitioned by `model_id` and date
3. **Athena Workgroup** — pre-configured for Athena v3

### Verify Deployment

```bash
STACK_NAME="bedrock-sentiment-stack-prod"

aws cloudformation describe-stacks --stack-name $STACK_NAME \
  --query 'Stacks[0].Outputs[?contains(OutputKey, `Table`)].{Key:OutputKey,Value:OutputValue}' \
  --output table
```

### Querying with Athena

```sql
SELECT symbol, model_name, AVG(sentiment_score) as avg_sentiment
FROM sentiment_results
WHERE date(timestamp) BETWEEN date('2024-01-01') AND date('2024-01-31')
GROUP BY symbol, model_name
ORDER BY avg_sentiment DESC;
```

See [docs/ATHENA_QUERY_GUIDE.md](docs/ATHENA_QUERY_GUIDE.md) for full query patterns.

### Disabling S3 Tables

```bash
npx cdk deploy --context environment=prod --context enableS3Tables=false
```

### Migrating from Parquet to S3 Tables

See [docs/CONFIG_MIGRATION_GUIDE.md](docs/CONFIG_MIGRATION_GUIDE.md) for step-by-step migration.

## Security

### No Hardcoded Values

- All resource names use `${this.account}` and `${this.region}`
- Configuration via CDK context parameters and `generate-config.sh`
- No secrets or credentials in code
- Working config files are `.gitignore`d

### Least Privilege IAM

| Role | Permissions | Scope |
|------|-------------|-------|
| Execution Role | ECR pull, CloudWatch logs | `AmazonECSTaskExecutionRolePolicy` (managed) |
| Job Role | S3 read (input, prompts), S3 write (output), Bedrock invoke, CloudWatch logs | Scoped to specific buckets and model ARN patterns |
| Service Role | Batch operations | `AWSBatchServiceRole` (managed) |

All IAM actions are specific — no `s3:*`, `bedrock:*`, or `Resource: "*"`.

### Encryption

- S3 buckets: SSE-S3 (AES-256) at rest, SSL/TLS enforced in transit
- ECR: encrypted at rest, image scanning on push, immutable tags
- CloudWatch Logs: encrypted at rest

### Network

- Batch compute runs in private subnets (no direct internet access)
- NAT Gateway for outbound to AWS services
- VPC Flow Logs enabled for audit
- Security group: outbound-only (required for ECR, S3, Bedrock, CloudWatch)

### Access Control

- S3: `BlockPublicAccess.BLOCK_ALL` on all buckets
- ECR: private repository, IAM-based access
- Bucket versioning enabled (configurable)
- `removalPolicy: RETAIN` prevents accidental bucket deletion

## Performance Tuning

### Concurrent Downloads

Optimal range is 50-100. Higher values increase S3 throughput but use more memory:

```json
{ "concurrent_download_limit": 75 }
```

### Batch Size

Controls streaming batch granularity (100-10,000):

| Batch Size | Memory | Use Case |
|------------|--------|----------|
| 100 | ~10 MB | Development/testing |
| 1,000 | ~100 MB | Production (recommended) |
| 5,000 | ~500 MB | Large datasets with ample memory |

### Rate Limits

Match your AWS Bedrock service quotas:

```bash
aws service-quotas list-service-quotas --service-code bedrock \
  --query 'Quotas[?contains(QuotaName, `Claude`)]'
```

## Rollback

### Config-Only Rollback

```bash
# Edit config and resubmit (no rebuild)
./scripts/update-config-and-run.sh config.json prod
```

### Disable Parallel Reading (Emergency)

```json
{ "parallel_reading_enabled": false }
```

Upload and resubmit — no code change needed.

### Image Rollback

```bash
# Find previous image tags
aws ecr describe-images --repository-name bedrock-sentiment-analyzer-prod \
  --query 'sort_by(imageDetails,& imagePushedAt)[-5:]' --output table

# Deploy with a specific tag
IMAGE_TAG=20250201-120000 ./scripts/deploy-and-run.sh config.json prod
```

### S3 Tables Rollback

Disable streaming to fall back to Parquet:

```json
{ "enable_streaming_batch": false }
```

S3 Tables also support time-travel queries for data recovery:

```sql
SELECT * FROM sentiment_results
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-15 10:00:00';
```

## Cleanup

```bash
# Delete stack (S3 buckets are retained by default)
cd cdk && npx cdk destroy --context environment=prod

# To fully remove, empty buckets first
aws s3 rm s3://BUCKET_NAME --recursive
```

## Cost Estimates

### Per-Job Costs (2-hour run, us-east-1)

| Configuration | vCPU Cost | Memory Cost | Total |
|---------------|-----------|-------------|-------|
| 2 vCPU, 8 GB | $0.16 | $0.07 | $0.23 |
| 4 vCPU, 16 GB | $0.32 | $0.14 | $0.46 |
| 4 vCPU, 24 GB | $0.32 | $0.21 | $0.53 |
| 8 vCPU, 30 GB | $0.65 | $0.27 | $0.92 |

### Monthly Infrastructure

| Component | Dev | Prod |
|-----------|-----|------|
| S3 storage | $1-2 | $10-20 |
| ECR | $1 | $1 |
| VPC (NAT Gateway) | $30 | $30 |
| Batch compute | $5-10 | $50-100 |
| Bedrock inference | $10-20 | $200-500 |
| **Total** | **~$50-65** | **~$300-650** |

### S3 Tables Costs

| Records/Month | Storage | Writes | Athena Queries | Total |
|----------------|---------|--------|----------------|-------|
| 1M | $0.50 | $0.02 | $2.50 | ~$3 |
| 10M | $5.00 | $0.15 | $12.50 | ~$18 |
| 100M | $50.00 | $1.50 | $25.00 | ~$77 |
