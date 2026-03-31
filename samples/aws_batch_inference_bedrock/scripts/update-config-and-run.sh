#!/bin/bash
# update-config-and-run.sh - Quick update: Upload config and submit jobs
# Use this when you've only changed configuration (no code changes)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
AWS_REGION="${AWS_REGION:-us-east-1}"
CONFIG_FILE="${1:-config.json}"
ENVIRONMENT="${2:-dev}"
STACK_NAME="${STACK_NAME:-bedrock-sentiment-stack-${ENVIRONMENT}}"

echo -e "${BLUE}=========================================="
echo "Quick Config Update and Run"
echo -e "==========================================${NC}"
echo "Stack: $STACK_NAME"
echo "Region: $AWS_REGION"
echo "Config: $CONFIG_FILE"
echo "Environment: $ENVIRONMENT"
echo ""

# Validate config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo -e "${RED}❌ Error: Config file '$CONFIG_FILE' not found${NC}"
    echo "Usage: ./scripts/update-config-and-run.sh [config-file] [environment]"
    echo "Example: ./scripts/update-config-and-run.sh config.json dev"
    exit 1
fi

# Validate JSON syntax
if ! jq empty "$CONFIG_FILE" 2>/dev/null; then
    echo -e "${RED}❌ Error: Invalid JSON in config file${NC}"
    exit 1
fi

# Step 1: Get infrastructure details
echo -e "${YELLOW}Step 1/3: Retrieving infrastructure details...${NC}"
PROMPT_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name $STACK_NAME \
  --region $AWS_REGION \
  --query 'Stacks[0].Outputs[?OutputKey==`PromptBucketName`].OutputValue' \
  --output text 2>/dev/null)

JOB_QUEUE=$(aws cloudformation describe-stacks \
  --stack-name $STACK_NAME \
  --region $AWS_REGION \
  --query 'Stacks[0].Outputs[?OutputKey==`JobQueueArn`].OutputValue' \
  --output text 2>/dev/null)

JOB_DEFINITION=$(aws cloudformation describe-stacks \
  --stack-name $STACK_NAME \
  --region $AWS_REGION \
  --query 'Stacks[0].Outputs[?OutputKey==`JobDefinitionArn`].OutputValue' \
  --output text 2>/dev/null)

if [ -z "$PROMPT_BUCKET" ] || [ -z "$JOB_QUEUE" ] || [ -z "$JOB_DEFINITION" ]; then
    echo -e "${RED}❌ Error: Could not retrieve infrastructure details${NC}"
    echo "Make sure the CDK stack '$STACK_NAME' is deployed"
    echo "Run './scripts/deploy-and-run.sh' first for initial deployment"
    exit 1
fi

echo "Prompt Bucket: $PROMPT_BUCKET"
echo "Job Queue: $JOB_QUEUE"
echo "Job Definition: $JOB_DEFINITION"
echo -e "${GREEN}✅ Infrastructure details retrieved${NC}"
echo ""

# Step 2: Upload configuration to S3
echo -e "${YELLOW}Step 2/3: Uploading configuration to S3...${NC}"
CONFIG_S3_PATH="s3://$PROMPT_BUCKET/configs/$ENVIRONMENT/config.json"

# Show config summary
echo "Configuration summary:"
echo "  Input bucket: $(jq -r '.input_s3_bucket' $CONFIG_FILE)"
echo "  Output bucket: $(jq -r '.output_s3_bucket' $CONFIG_FILE)"
echo "  Max articles: $(jq -r '.max_articles // "all"' $CONFIG_FILE)"
echo "  Models: $(jq -r '.models | length' $CONFIG_FILE)"
echo "  Job name: $(jq -r '.job_name' $CONFIG_FILE)"
echo ""

# Upload to S3
aws s3 cp $CONFIG_FILE $CONFIG_S3_PATH
echo "Uploaded to: $CONFIG_S3_PATH"
echo -e "${GREEN}✅ Configuration uploaded${NC}"
echo ""

# Step 3: Submit jobs
echo -e "${YELLOW}Step 3/3: Submitting AWS Batch jobs...${NC}"
export JOB_QUEUE
export JOB_DEFINITION
export CONFIG_S3_URI=$CONFIG_S3_PATH
export LOCAL_CONFIG_FILE=$CONFIG_FILE

./scripts/submit-parallel-jobs.sh

echo ""
echo -e "${GREEN}=========================================="
echo "✅ Config updated and jobs submitted!"
echo -e "==========================================${NC}"
echo ""
echo "Configuration: $CONFIG_S3_PATH"
echo ""
echo "Monitor your jobs:"
echo "  # List running jobs"
echo "  aws batch list-jobs --job-queue $JOB_QUEUE --job-status RUNNING"
echo ""
echo "  # Describe specific job"
echo "  aws batch describe-jobs --jobs <job-id>"
echo ""
echo "  # Watch logs in real-time"
echo "  aws logs tail /aws/batch/bedrock-sentiment-$ENVIRONMENT --follow"
echo ""
echo "  # Check job status summary"
echo "  aws batch list-jobs --job-queue $JOB_QUEUE --job-status RUNNING --query 'jobSummaryList[*].[jobName,status,createdAt]' --output table"
echo ""
echo "To update config again:"
echo "  ./scripts/update-config-and-run.sh $CONFIG_FILE $ENVIRONMENT"
