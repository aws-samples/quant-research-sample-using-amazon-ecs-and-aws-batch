#!/bin/bash
# deploy.sh - Deploy infrastructure only (no job submission)
# Use this when you want to deploy first and run jobs later with update-config-and-run.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
AWS_REGION="${AWS_REGION:-us-east-1}"
CONFIG_FILE="${1:-examples/config-example.json}"
ENVIRONMENT="${2:-dev}"
STACK_NAME="${STACK_NAME:-bedrock-sentiment-stack-${ENVIRONMENT}}"

echo -e "${BLUE}=========================================="
echo "Infrastructure Deployment"
echo -e "==========================================${NC}"
echo "Stack: $STACK_NAME"
echo "Region: $AWS_REGION"
echo "Config template: $CONFIG_FILE"
echo "Environment: $ENVIRONMENT"
echo ""

# Step 1: Generate unique image tag
echo -e "${YELLOW}Step 1/6: Generating image tag...${NC}"
TIMESTAMP_TAG=$(date +%Y%m%d-%H%M%S)
IMAGE_TAG="${IMAGE_TAG:-$TIMESTAMP_TAG}"
echo "Image tag: $IMAGE_TAG"
echo -e "${GREEN}✅ Image tag generated${NC}"
echo ""

# Step 2: Build Docker image
echo -e "${YELLOW}Step 2/6: Building Docker image...${NC}"
echo "Building for linux/amd64 (Fargate requirement)..."

if command -v podman &> /dev/null; then
    CONTAINER_TOOL="podman"
elif command -v docker &> /dev/null; then
    CONTAINER_TOOL="docker"
else
    echo -e "${RED}❌ Error: Neither podman nor docker found${NC}"
    exit 1
fi

echo "Using container tool: $CONTAINER_TOOL"
$CONTAINER_TOOL build --platform linux/amd64 -t bedrock-sentiment-analyzer:$IMAGE_TAG .
echo -e "${GREEN}✅ Docker image built${NC}"
echo ""

# Step 3: Bootstrap CDK if needed
echo -e "${YELLOW}Step 3/6: Checking CDK bootstrap...${NC}"
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

# Step 4: Deploy CDK infrastructure
echo -e "${YELLOW}Step 4/6: Deploying CDK infrastructure...${NC}"
npx cdk deploy \
  --require-approval never \
  --outputs-file outputs.json \
  --context imageTag=$IMAGE_TAG \
  --context environment=$ENVIRONMENT
cd ..
echo -e "${GREEN}✅ CDK deployment complete${NC}"
echo ""

# Step 5: Push Docker image to ECR
echo -e "${YELLOW}Step 5/6: Pushing Docker image to ECR...${NC}"
ECR_URI=$(aws cloudformation describe-stacks \
  --stack-name $STACK_NAME --region $AWS_REGION \
  --query 'Stacks[0].Outputs[?OutputKey==`EcrRepositoryUri`].OutputValue' --output text)

aws ecr get-login-password --region $AWS_REGION | \
  $CONTAINER_TOOL login --username AWS --password-stdin $ECR_URI

$CONTAINER_TOOL tag bedrock-sentiment-analyzer:$IMAGE_TAG $ECR_URI:$IMAGE_TAG
$CONTAINER_TOOL push $ECR_URI:$IMAGE_TAG
echo "Image pushed: $ECR_URI:$IMAGE_TAG"
echo -e "${GREEN}✅ Docker image pushed to ECR${NC}"
echo ""

# Step 6: Generate config and upload prompts
echo -e "${YELLOW}Step 6/6: Generating config and uploading prompts...${NC}"
PROMPT_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name $STACK_NAME --region $AWS_REGION \
  --query 'Stacks[0].Outputs[?OutputKey==`PromptBucketName`].OutputValue' --output text)

# Auto-generate config with real bucket names
if [ -f "$CONFIG_FILE" ]; then
    ./scripts/generate-config.sh "$ENVIRONMENT" "$CONFIG_FILE"
else
    ./scripts/generate-config.sh "$ENVIRONMENT"
fi
echo "Generated config.json from stack outputs"

# Upload config to S3
CONFIG_S3_PATH="s3://$PROMPT_BUCKET/configs/$ENVIRONMENT/config.json"
aws s3 cp config.json $CONFIG_S3_PATH
echo "Config uploaded to: $CONFIG_S3_PATH"

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

echo -e "${GREEN}✅ Config and prompts ready${NC}"
echo ""

echo -e "${GREEN}=========================================="
echo "✅ Infrastructure deployment complete!"
echo -e "==========================================${NC}"
echo ""
echo "Deployment Summary:"
echo "  Image: $ECR_URI:$IMAGE_TAG"
echo "  Config: $CONFIG_S3_PATH"
echo "  Stack: $STACK_NAME"
echo ""
echo "To submit jobs:"
echo "  ./scripts/update-config-and-run.sh config.json $ENVIRONMENT"
echo ""
echo "To customize config first:"
echo "  vim config.json"
echo "  ./scripts/update-config-and-run.sh config.json $ENVIRONMENT"
