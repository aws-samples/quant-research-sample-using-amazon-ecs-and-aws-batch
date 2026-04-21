# Example Configuration Files

This directory contains example configuration and prompt files for testing and reference.

## Files

### Configuration Files

#### config-example.json
Production-ready configuration template with all 7 models.

**Usage:**
```bash
# Copy and customize for your environment
cp examples/config-example.json config.json
# Edit config.json with your S3 bucket names
```

#### config-test.json
Test configuration with limited articles (10) for quick testing.

**Usage:**
```bash
# Use for local testing
python src/main.py --config examples/config-test.json --log-level DEBUG
```

#### config-aws.json
AWS-specific configuration reference.

### Prompt Files

#### system-prompt-example.txt
System prompt that defines the AI's role and behavior.

**Customization:**
- Adjust the role description
- Modify analysis guidelines
- Change output format requirements

**Upload to S3:**
```bash
aws s3 cp examples/system-prompt-example.txt s3://YOUR-PROMPT-BUCKET/system-prompt.txt
```

#### user-prompt-example.txt
User prompt template with placeholders for task-specific instructions.

**Placeholders:**
- `{article_id}` - Article identifier
- `{symbol}` - Stock ticker symbol
- `{component}` - Component type (content/headline/summary)
- `{rating_min}` - Minimum rating value
- `{rating_max}` - Maximum rating value

**Upload to S3:**
```bash
aws s3 cp examples/user-prompt-example.txt s3://YOUR-PROMPT-BUCKET/user-prompt.txt
```

## Quick Start

### 1. Set Up Configuration

```bash
# Copy example config
cp examples/config-example.json config.json

# Edit with your bucket names
vim config.json
```

Update these fields:
- `input_s3_bucket`: Your input bucket URI
- `output_s3_bucket`: Your output bucket URI
- `prompt_s3_uri`: Your user prompt S3 URI
- `system_prompt_s3_uri`: Your system prompt S3 URI
- `max_articles`: Number of articles to process (set to `null` for unlimited, or a number like `100` for testing)
- `job_name`: Identifier for this run (e.g., "test-run", "production-20251111")

### 2. Upload Prompts to S3

```bash
# Get your prompt bucket name from CDK output
PROMPT_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name bedrock-sentiment-stack-dev \
  --query 'Stacks[0].Outputs[?OutputKey==`PromptBucketName`].OutputValue' \
  --output text)

# Upload prompts
aws s3 cp examples/system-prompt-example.txt s3://$PROMPT_BUCKET/system-prompt.txt
aws s3 cp examples/user-prompt-example.txt s3://$PROMPT_BUCKET/user-prompt.txt
```

### 3. Test Locally

```bash
# Test with limited articles
python src/main.py --config examples/config-test.json --log-level DEBUG
```

### 4. Deploy to AWS Batch

```bash
# Submit all models in parallel
./scripts/submit-parallel-jobs.sh
```

## Configuration Parameters

### Article Processing

**max_articles**: Controls how many articles to process

- `"max_articles": 100` - Process only 100 articles (good for testing)
- `"max_articles": null` - Process ALL articles (unlimited, for production)
- `"max_articles": 1000` - Process 1000 articles

**Important:** Use `null` (not `0`, not `""`) for unlimited processing.

**Example for testing:**
```json
{
  "max_articles": 10,
  "job_name": "quick-test"
}
```

**Example for production:**
```json
{
  "max_articles": null,
  "job_name": "production-full-run"
}
```

### Model Configuration

Each model in the `models` array requires:

```json
{
  "model_id": "global.anthropic.claude-haiku-4-5-20251001-v1:0",
  "model_name": "claude-haiku-4-5",
  "requests_per_minute": 120,
  "tokens_per_minute": 1000000
}
```

**Parameters:**
- `model_id`: Full Bedrock model identifier
- `model_name`: Human-readable name (used in output)
- `requests_per_minute`: Rate limit for API requests
- `tokens_per_minute`: Rate limit for tokens

### Rate Limits by Model

Based on AWS quotas (as of deployment):

| Model | Requests/Min | Tokens/Min |
|-------|--------------|------------|
| Claude Haiku 4.5 (global) | 120 | 1,000,000 |
| Claude Sonnet 4 (global) | 200 | 200,000 |
| Claude Sonnet 4.5 (global) | 200 | 500,000 |
| Claude 3 Haiku (us) | 2,000 | 4,000,000 |
| Nova Pro v1 (us) | 500 | 2,000,000 |
| Llama4 Scout 17B (us) | 800 | 600,000 |
| Llama4 Maverick 17B (us) | 800 | 600,000 |

**Note:** Verify current quotas with:
```bash
aws service-quotas list-service-quotas --service-code bedrock
```

## Customizing Prompts

### System Prompt Best Practices

1. **Be Specific**: Clearly define the AI's role
2. **Set Boundaries**: Specify what the AI should and shouldn't do
3. **Output Format**: Define expected response structure
4. **Tone**: Set the appropriate tone (analytical, neutral, etc.)

### User Prompt Best Practices

1. **Use Placeholders**: Keep prompts reusable with `{variable}` syntax
2. **Be Clear**: Provide explicit instructions
3. **Include Examples**: Show expected output format
4. **Rating Scale**: Always reference the rating scale

## Testing Configurations

### Local Testing

```bash
# Test with debug logging
python src/main.py \
  --config examples/config-test.json \
  --log-level DEBUG
```

### Single Model Testing

Create a test config with one model:

```json
{
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

### Batch Job Testing

```bash
# Submit single model job
aws batch submit-job \
  --job-name test-$(date +%Y%m%d-%H%M%S) \
  --job-queue YOUR_QUEUE_ARN \
  --job-definition YOUR_JOB_DEF_ARN \
  --container-overrides '{
    "command": [
      "--config", "config.json",
      "--model-id", "global.anthropic.claude-haiku-4-5-20251001-v1:0"
    ]
  }'
```

## Troubleshooting

### Configuration Validation Errors

If you see validation errors:

1. Check JSON syntax with `jq`:
   ```bash
   jq . examples/config-example.json
   ```

2. Verify S3 URIs are correct:
   ```bash
   aws s3 ls s3://your-bucket/
   ```

3. Ensure model IDs are valid:
   ```bash
   aws bedrock list-foundation-models
   ```

### Prompt Issues

If sentiment analysis fails:

1. Test prompts manually with Bedrock console
2. Check placeholder syntax matches code
3. Verify rating scale is consistent
4. Review CloudWatch logs for errors

## Additional Resources

- [Main README](../README.md) - Full documentation
- [Scripts Reference](../SCRIPTS.md) - Helper scripts guide
- [AWS Bedrock Models](https://docs.aws.amazon.com/bedrock/latest/userguide/models-supported.html) - Available models
