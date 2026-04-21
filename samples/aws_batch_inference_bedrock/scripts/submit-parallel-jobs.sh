#!/bin/bash
# Submit parallel AWS Batch jobs for each model (reads models from config.json)

set -e

# Configuration
JOB_QUEUE="${JOB_QUEUE:-}"
JOB_DEFINITION="${JOB_DEFINITION:-}"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# Config path - supports S3 URI or local file
CONFIG_PATH="${CONFIG_S3_URI:-config.json}"

# Local config file for reading model list
LOCAL_CONFIG="${LOCAL_CONFIG_FILE:-config.json}"

# Auto-lookup from CloudFormation if not set
ENVIRONMENT="${ENVIRONMENT:-dev}"
if [ -z "$JOB_QUEUE" ] || [ -z "$JOB_DEFINITION" ]; then
    STACK_NAME="${STACK_NAME:-bedrock-sentiment-stack-${ENVIRONMENT}}"
    AWS_REGION="${AWS_REGION:-us-east-1}"

    echo "JOB_QUEUE/JOB_DEFINITION not set, looking up from stack: $STACK_NAME"

    JOB_QUEUE=$(aws cloudformation describe-stacks \
      --stack-name "$STACK_NAME" --region "$AWS_REGION" \
      --query 'Stacks[0].Outputs[?OutputKey==`JobQueueArn`].OutputValue' \
      --output text 2>/dev/null)

    JOB_DEFINITION=$(aws cloudformation describe-stacks \
      --stack-name "$STACK_NAME" --region "$AWS_REGION" \
      --query 'Stacks[0].Outputs[?OutputKey==`JobDefinitionArn`].OutputValue' \
      --output text 2>/dev/null)

    if [ -z "$JOB_QUEUE" ] || [ -z "$JOB_DEFINITION" ]; then
        echo "Error: Could not look up JOB_QUEUE/JOB_DEFINITION from stack $STACK_NAME"
        echo "Either deploy the stack first, or export JOB_QUEUE and JOB_DEFINITION manually."
        exit 1
    fi
fi

# Read models from config file
if [ ! -f "$LOCAL_CONFIG" ]; then
    echo "Error: Local config file '$LOCAL_CONFIG' not found (needed to read model list)"
    echo "Set LOCAL_CONFIG_FILE to point to your config.json"
    exit 1
fi

MODEL_COUNT=$(jq '.models | length' "$LOCAL_CONFIG")
if [ "$MODEL_COUNT" -eq 0 ]; then
    echo "Error: No models found in $LOCAL_CONFIG"
    exit 1
fi

echo "=========================================="
echo "Submitting $MODEL_COUNT parallel batch jobs"
echo "=========================================="
echo "Job Queue: $JOB_QUEUE"
echo "Job Definition: $JOB_DEFINITION"
echo "Timestamp: $TIMESTAMP"
echo "Config Source: $CONFIG_PATH"
echo "Models from: $LOCAL_CONFIG"
echo ""

# Submit a job for each model from config
JOB_IDS=()
MODEL_NAMES_LIST=()
for i in $(seq 0 $((MODEL_COUNT - 1))); do
    MODEL_ID=$(jq -r ".models[$i].model_id // empty" "$LOCAL_CONFIG")
    MODEL_ARN=$(jq -r ".models[$i].model_arn // empty" "$LOCAL_CONFIG")
    MODEL_NAME=$(jq -r ".models[$i].model_name" "$LOCAL_CONFIG")

    # Use model_id or model_arn (model_id takes precedence)
    if [ -n "$MODEL_ID" ]; then
        MODEL_FLAG="--model-id"
        MODEL_VALUE="$MODEL_ID"
    elif [ -n "$MODEL_ARN" ]; then
        MODEL_FLAG="--model-arn"
        MODEL_VALUE="$MODEL_ARN"
    else
        echo "  ⚠️  Skipping model $MODEL_NAME: no model_id or model_arn"
        continue
    fi

    JOB_NAME="sentiment-${MODEL_NAME}-${TIMESTAMP}"

    echo "[$((i+1))/${MODEL_COUNT}] Submitting job for model: $MODEL_NAME"
    echo "  Model: $MODEL_VALUE"

    JOB_ID=$(aws batch submit-job \
        --job-name "$JOB_NAME" \
        --job-queue "$JOB_QUEUE" \
        --job-definition "$JOB_DEFINITION" \
        --container-overrides "{
            \"command\": [
                \"--config\", \"$CONFIG_PATH\",
                \"$MODEL_FLAG\", \"$MODEL_VALUE\"
            ]
        }" \
        --query 'jobId' \
        --output text)

    JOB_IDS+=("$JOB_ID")
    MODEL_NAMES_LIST+=("$MODEL_NAME")
    echo "  ✅ Job submitted: $JOB_ID"
    echo ""
done

echo "=========================================="
echo "All jobs submitted successfully!"
echo "=========================================="
echo ""
echo "Job IDs:"
for i in "${!JOB_IDS[@]}"; do
    echo "  ${MODEL_NAMES_LIST[$i]}: ${JOB_IDS[$i]}"
done
echo ""
echo "Monitor jobs with:"
echo "  aws batch describe-jobs --jobs ${JOB_IDS[*]}"
echo ""
echo "Check logs in CloudWatch:"
echo "  Log Group: /aws/batch/bedrock-sentiment-${ENVIRONMENT:-dev}"
