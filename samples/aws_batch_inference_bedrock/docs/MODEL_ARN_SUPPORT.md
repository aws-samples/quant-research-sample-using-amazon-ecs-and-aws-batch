# Model ARN Support

## Overview

The inference code now supports both standard Bedrock model IDs and custom model ARNs. This allows you to use:

- **Standard Models**: Pre-built foundation models from AWS Bedrock
- **Custom Models**: Provisioned throughput models or imported custom models

## Configuration

In your `config.json`, each model in the `models` array can specify **either** `model_id` **or** `model_arn` (but not both).

### Using Standard Model ID

```json
{
  "model_id": "us.anthropic.claude-3-haiku-20240307-v1:0",
  "model_name": "claude-3-haiku",
  "requests_per_minute": 2000,
  "tokens_per_minute": 4000000
}
```

### Using Custom Model ARN

```json
{
  "model_arn": "arn:aws:bedrock:us-east-1:123456789012:provisioned-model/abc123def456",
  "model_name": "custom-provisioned-model",
  "requests_per_minute": 500,
  "tokens_per_minute": 2000000
}
```

### Mixed Configuration

You can mix both types in the same config file:

```json
{
  "models": [
    {
      "model_id": "global.anthropic.claude-haiku-4-5-20251001-v1:0",
      "model_name": "claude-haiku-4-5",
      "requests_per_minute": 250,
      "tokens_per_minute": 1000000
    },
    {
      "model_arn": "arn:aws:bedrock:us-east-1:123456789012:provisioned-model/abc123",
      "model_name": "custom-model",
      "requests_per_minute": 500,
      "tokens_per_minute": 2000000
    }
  ]
}
```

## Validation Rules

1. **Exactly one identifier required**: Each model must have either `model_id` or `model_arn`, but not both
2. **Non-empty values**: The identifier cannot be empty or whitespace
3. **Rate limits required**: `requests_per_minute` and `tokens_per_minute` must be positive integers
4. **Optional model_name**: If not provided, the system will derive a name from the identifier

## Use Cases

### Provisioned Throughput

Use `model_arn` when you have provisioned throughput for a model:

```json
{
  "model_arn": "arn:aws:bedrock:us-east-1:123456789012:provisioned-model/my-provisioned-claude",
  "model_name": "provisioned-claude",
  "requests_per_minute": 1000,
  "tokens_per_minute": 5000000
}
```

### Imported Custom Models

Use `model_arn` for custom models you've imported into Bedrock:

```json
{
  "model_arn": "arn:aws:bedrock:us-east-1:123456789012:custom-model/my-fine-tuned-model",
  "model_name": "fine-tuned-sentiment",
  "requests_per_minute": 100,
  "tokens_per_minute": 500000
}
```

## Implementation Details

### Code Changes

1. **ModelConfig** (`config.py`):
   - Added `model_arn` field (optional)
   - Made `model_id` optional
   - Added `get_model_identifier()` method to return the appropriate identifier
   - Updated validation to ensure exactly one identifier is provided

2. **BedrockClient** (`bedrock_client.py`):
   - Changed `analyze_sentiment()` parameter from `model_id` to `model_identifier`
   - Works with both model IDs and ARNs (Bedrock Converse API accepts both)

3. **SentimentAnalyzer** (`sentiment_analyzer.py`):
   - Updated `analyze_task()` to use `model_identifier` parameter

4. **Orchestrator** (`orchestrator.py`):
   - Updated all references to use `model_config.get_model_identifier()`
   - Enhanced table name generation to handle ARN format with proper normalization

### S3 Table Name Normalization

When using ARNs, the system automatically generates valid S3 table names by:

1. Extracting the model identifier from the ARN (the part after the last `/`)
2. Converting to lowercase
3. Replacing hyphens and special characters with underscores
4. Removing consecutive underscores
5. Removing leading/trailing underscores
6. Falling back to 'model' if the result is empty

**Examples:**

| ARN | Generated Table Name |
|-----|---------------------|
| `arn:aws:bedrock:us-east-1:123456789012:provisioned-model/abc123` | `sentiment_results_abc123` |
| `arn:aws:bedrock:us-east-1:123456789012:custom-model/my-fine-tuned-model-v2` | `sentiment_results_my_fine_tuned_model_v2` |
| `arn:aws:bedrock:us-east-1:123456789012:provisioned-model/-test-model-` | `sentiment_results_test_model` |

**Best Practice:** Always provide a `model_name` in your config to have full control over the table name:

```json
{
  "model_arn": "arn:aws:bedrock:us-east-1:123456789012:provisioned-model/abc123",
  "model_name": "my_custom_model",
  "requests_per_minute": 500,
  "tokens_per_minute": 2000000
}
```

This ensures the table will be named `sentiment_results_my_custom_model` regardless of the ARN format.

### Backward Compatibility

All existing config files using `model_id` will continue to work without any changes. The system is fully backward compatible.

## Example

See `config-with-arn-example.json` for a complete example configuration using both model IDs and ARNs.

## Testing

Run the validation tests:

```bash
python test_model_identifier.py
python test_config_loading.py
```
