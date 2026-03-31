#!/bin/bash
# generate-config.sh - Generate config.json from CloudFormation stack outputs
# Usage: ./scripts/generate-config.sh [environment] [template-file]

set -e

ENVIRONMENT="${1:-dev}"
TEMPLATE="${2:-examples/config-example.json}"
STACK_NAME="${STACK_NAME:-bedrock-sentiment-stack-${ENVIRONMENT}}"
AWS_REGION="${AWS_REGION:-us-east-1}"
OUTPUT_FILE="config.json"

echo "Generating $OUTPUT_FILE from stack: $STACK_NAME (template: $TEMPLATE)"

# Validate template exists
if [ ! -f "$TEMPLATE" ]; then
    echo "Error: Template file '$TEMPLATE' not found"
    exit 1
fi

# Look up bucket names from CloudFormation outputs
INPUT_BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$AWS_REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`InputBucketName`].OutputValue' --output text 2>/dev/null)
OUTPUT_BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$AWS_REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`OutputBucketName`].OutputValue' --output text 2>/dev/null)
PROMPT_BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$AWS_REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`PromptBucketName`].OutputValue' --output text 2>/dev/null)
TABLE_WAREHOUSE=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$AWS_REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`TableWarehousePath`].OutputValue' --output text 2>/dev/null)

if [ -z "$INPUT_BUCKET" ] || [ -z "$OUTPUT_BUCKET" ] || [ -z "$PROMPT_BUCKET" ]; then
    echo "Error: Could not retrieve bucket names from stack $STACK_NAME"
    echo "Make sure the stack is deployed: ./scripts/deploy-and-run.sh config.json $ENVIRONMENT"
    exit 1
fi

# Look up S3 Table name
TABLE_NAME=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$AWS_REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`SentimentTableName`].OutputValue' --output text 2>/dev/null)

# Patch template with actual bucket names and S3 Tables config
jq \
  --arg input "s3://${INPUT_BUCKET}/" \
  --arg output "s3://${OUTPUT_BUCKET}/" \
  --arg prompt "s3://${PROMPT_BUCKET}/user-prompt.txt" \
  --arg system "s3://${PROMPT_BUCKET}/system-prompt.txt" \
  --arg warehouse "${TABLE_WAREHOUSE}warehouse/" \
  --arg table_name "${TABLE_NAME}" \
  '.input_s3_bucket = $input | .output_s3_bucket = $output | .prompt_s3_uri = $prompt | .system_prompt_s3_uri = $system | .s3_table_warehouse = $warehouse | .s3_table_name = $table_name | .enable_streaming_batch //= true | .batch_size //= 1000 | .write_mode //= "upsert"' \
  "$TEMPLATE" > "$OUTPUT_FILE"

echo "Generated $OUTPUT_FILE:"
echo "  input_s3_bucket:      s3://${INPUT_BUCKET}/"
echo "  output_s3_bucket:     s3://${OUTPUT_BUCKET}/"
echo "  prompt_s3_uri:        s3://${PROMPT_BUCKET}/user-prompt.txt"
echo "  system_prompt_s3_uri: s3://${PROMPT_BUCKET}/system-prompt.txt"
echo "  s3_table_warehouse:   ${TABLE_WAREHOUSE}warehouse/"
echo "  s3_table_name:        ${TABLE_NAME}"
