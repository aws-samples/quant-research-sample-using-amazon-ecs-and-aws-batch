#!/bin/bash
# deploy-and-run.sh - Full deployment: CDK redeploy, Docker rebuild, and job submission
# Use this when you've changed code or infrastructure
# On first run, auto-generates config and uploads example prompts

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
echo "Full Deployment and Run"
echo -e "==========================================${NC}"
echo "Stack: $STACK_NAME"
echo "Region: $AWS_REGION"
echo "Config: $CONFIG_FILE"
echo "Environment: $ENVIRONMENT"
echo ""

# Step 1: Generate unique image tag
echo -e "${YELLOW}Step 1/7: Generating image tag...${NC}"
TIMESTAMP_TAG=$(date +%Y%m%d-%H%M%S)
IMAGE_TAG="${IMAGE_TAG:-$TIMESTAMP_TAG}"
echo "Image tag: $IMAGE_TAG"
echo -e "${GREEN}✅ Image tag generated${NC}"
echo ""

# Step 2: Build Docker image first (before CDK deploy)
echo -e "${YELLOW}Step 2/7: Building Docker image...${NC}"
echo "Building for linux/amd64 (Fargate requirement)..."

# Detect container tool (podman or docker)
if command -v podman &> /dev/null; then
    CONTAINER_TOOL="podman"
elif command -v docker &> /dev/null; then
    CONTAINER_TOOL="docker"
else
    echo -e "${RED}❌ Error: Neither podman nor docker found${NC}"
    exit 1
fi

echo "Using container tool: $CONTAINER_TOOL"

# Build image
$CONTAINER_TOOL build --platform linux/amd64 -t bedrock-sentiment-analyzer:$IMAGE_TAG .

echo -e "${GREEN}✅ Docker image built${NC}"
echo ""

# Step 3: Bootstrap CDK if needed
echo -e "${YELLOW}Step 3/7: Checking CDK bootstrap...${NC}"
cd cdk
npm install
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BOOTSTRAP_STACK=$(aws cloudformation describe-stacks \
  --stack-name CDKToolkit --region $AWS_REGION \
  --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "NONE")

if [ "$BOOTSTRAP_STACK" = "NONE" ]; then
    echo "CDK not bootstrapped in $AWS_REGION. Bootstrapping now..."
    npx cdk bootstrap aws://$ACCOUNT_ID/$AWS_REGION
    echo -e "${GREEN}✅ CDK bootstrapped${NC}"
else
    echo "CDK already bootstrapped (status: $BOOTSTRAP_STACK)"
    echo -e "${GREEN}✅ CDK bootstrap OK${NC}"
fi
echo ""

# Step 4: Deploy CDK infrastructure with image tag
echo -e "${YELLOW}Step 4/7: Deploying CDK infrastructure...${NC}"
echo "Using image tag: $IMAGE_TAG"
echo "Environment: $ENVIRONMENT"
npx cdk deploy \
  --require-approval never \
  --outputs-file outputs.json \
  --context imageTag=$IMAGE_TAG \
  --context environment=$ENVIRONMENT
cd ..
echo -e "${GREEN}✅ CDK deployment complete${NC}"
echo ""

# Step 5: Get infrastructure outputs and generate config
echo -e "${YELLOW}Step 5/7: Generating config from stack outputs...${NC}"
ECR_URI=$(aws cloudformation describe-stacks \
  --stack-name $STACK_NAME \
  --region $AWS_REGION \
  --query 'Stacks[0].Outputs[?OutputKey==`EcrRepositoryUri`].OutputValue' \
  --output text)

PROMPT_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name $STACK_NAME \
  --region $AWS_REGION \
  --query 'Stacks[0].Outputs[?OutputKey==`PromptBucketName`].OutputValue' \
  --output text)

JOB_QUEUE=$(aws cloudformation describe-stacks \
  --stack-name $STACK_NAME \
  --region $AWS_REGION \
  --query 'Stacks[0].Outputs[?OutputKey==`JobQueueArn`].OutputValue' \
  --output text)

JOB_DEFINITION=$(aws cloudformation describe-stacks \
  --stack-name $STACK_NAME \
  --region $AWS_REGION \
  --query 'Stacks[0].Outputs[?OutputKey==`JobDefinitionArn`].OutputValue' \
  --output text)

# Auto-generate config with real bucket names if using example/placeholder config
if grep -q '<input-bucket-name>\|<output-bucket-name>\|<prompts-bucket-name>' "$CONFIG_FILE" 2>/dev/null; then
    echo "Config has placeholder values — generating from stack outputs..."
    ./scripts/generate-config.sh "$ENVIRONMENT" "$CONFIG_FILE"
    CONFIG_FILE="config.json"
    echo -e "${GREEN}✅ Config generated as config.json${NC}"
elif [ ! -f "$CONFIG_FILE" ]; then
    echo "Config file '$CONFIG_FILE' not found — generating from example..."
    ./scripts/generate-config.sh "$ENVIRONMENT"
    CONFIG_FILE="config.json"
    echo -e "${GREEN}✅ Config generated as config.json${NC}"
else
    echo "Using existing config: $CONFIG_FILE"
    echo -e "${GREEN}✅ Config OK${NC}"
fi

echo "ECR URI: $ECR_URI"
echo "Prompt Bucket: $PROMPT_BUCKET"
echo "Job Queue: $JOB_QUEUE"
echo "Job Definition: $JOB_DEFINITION"
echo ""

# Step 6: Push Docker image to ECR and upload prompts
echo -e "${YELLOW}Step 6/7: Pushing Docker image to ECR and uploading prompts...${NC}"

# Login to ECR
aws ecr get-login-password --region $AWS_REGION | \
  $CONTAINER_TOOL login --username AWS --password-stdin $ECR_URI

# Tag and push
$CONTAINER_TOOL tag bedrock-sentiment-analyzer:$IMAGE_TAG $ECR_URI:$IMAGE_TAG
$CONTAINER_TOOL push $ECR_URI:$IMAGE_TAG
echo "Image pushed: $ECR_URI:$IMAGE_TAG"

# Upload example prompts if they don't exist in S3 yet
SYSTEM_PROMPT_EXISTS=$(aws s3 ls "s3://$PROMPT_BUCKET/system-prompt.txt" 2>/dev/null || true)
USER_PROMPT_EXISTS=$(aws s3 ls "s3://$PROMPT_BUCKET/user-prompt.txt" 2>/dev/null || true)

if [ -z "$SYSTEM_PROMPT_EXISTS" ] && [ -f "examples/system-prompt-example.txt" ]; then
    echo "Uploading example system prompt..."
    aws s3 cp examples/system-prompt-example.txt "s3://$PROMPT_BUCKET/system-prompt.txt"
fi
if [ -z "$USER_PROMPT_EXISTS" ] && [ -f "examples/user-prompt-example.txt" ]; then
    echo "Uploading example user prompt..."
    aws s3 cp examples/user-prompt-example.txt "s3://$PROMPT_BUCKET/user-prompt.txt"
fi

echo -e "${GREEN}✅ Docker image pushed and prompts uploaded${NC}"
echo ""

# Step 7: Upload config and submit jobs
echo -e "${YELLOW}Step 7/7: Uploading config and submitting jobs...${NC}"
CONFIG_S3_PATH="s3://$PROMPT_BUCKET/configs/$ENVIRONMENT/config.json"
aws s3 cp $CONFIG_FILE $CONFIG_S3_PATH
echo "Config uploaded to: $CONFIG_S3_PATH"

export JOB_QUEUE
export JOB_DEFINITION
export CONFIG_S3_URI=$CONFIG_S3_PATH
export LOCAL_CONFIG_FILE=$CONFIG_FILE

./scripts/submit-parallel-jobs.sh

echo ""
echo -e "${GREEN}=========================================="
echo "✅ Deployment and job submission complete!"
echo -e "==========================================${NC}"
echo ""
echo "Deployment Summary:"
echo "  Image: $ECR_URI:$IMAGE_TAG"
echo "  Config: $CONFIG_S3_PATH"
echo "  Job Queue: $JOB_QUEUE"
echo ""
echo "Next steps:"
echo "  1. Monitor jobs: aws batch list-jobs --job-queue $JOB_QUEUE --job-status RUNNING"
echo "  2. View logs: aws logs tail /aws/batch/bedrock-sentiment-$ENVIRONMENT --follow"
echo "  3. Check results in S3 output bucket"
echo ""
echo "To update config and rerun without rebuilding:"
echo "  ./scripts/update-config-and-run.sh $CONFIG_FILE $ENVIRONMENT"
echo ""
echo "Note: ECR has immutable tags enabled (security best practice)"
echo "Each deployment uses a unique timestamped tag: $IMAGE_TAG"
