#!/bin/bash
set -e

# Bedrock Sentiment Analyzer - CDK Deployment Script
# This script deploys the infrastructure with proper validation

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
ENVIRONMENT=${ENVIRONMENT:-dev}
RESOURCE_PREFIX=${RESOURCE_PREFIX:-bedrock-sentiment}
COST_CENTER=${COST_CENTER:-Engineering}
MAX_VCPUS=${MAX_VCPUS:-16}
VPC_CIDR=${VPC_CIDR:-10.0.0.0/16}

# Ensure MAX_VCPUS is a number
if ! [[ "$MAX_VCPUS" =~ ^[0-9]+$ ]]; then
    echo -e "${RED}Error: MAX_VCPUS must be a number${NC}"
    exit 1
fi

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Bedrock Sentiment Analyzer - CDK Deploy${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Validate AWS credentials
echo -e "${YELLOW}Validating AWS credentials...${NC}"
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    echo -e "${RED}Error: AWS credentials not configured${NC}"
    echo "Please run: aws configure"
    exit 1
fi

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=$(aws configure get region || echo "us-east-1")

echo -e "${GREEN}✓ AWS Account: $ACCOUNT_ID${NC}"
echo -e "${GREEN}✓ AWS Region: $REGION${NC}"
echo ""

# Display configuration
echo -e "${YELLOW}Deployment Configuration:${NC}"
echo "  Environment: $ENVIRONMENT"
echo "  Resource Prefix: $RESOURCE_PREFIX"
echo "  Cost Center: $COST_CENTER"
echo "  Max vCPUs: $MAX_VCPUS"
echo "  VPC CIDR: $VPC_CIDR"
echo ""

# Confirm deployment
read -p "Deploy with these settings? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Deployment cancelled${NC}"
    exit 0
fi

# Install dependencies
echo -e "${YELLOW}Installing dependencies...${NC}"
npm install
echo -e "${GREEN}✓ Dependencies installed${NC}"
echo ""

# Bootstrap CDK (if needed)
echo -e "${YELLOW}Checking CDK bootstrap...${NC}"
if ! aws cloudformation describe-stacks --stack-name CDKToolkit > /dev/null 2>&1; then
    echo -e "${YELLOW}Bootstrapping CDK...${NC}"
    cdk bootstrap aws://$ACCOUNT_ID/$REGION
    echo -e "${GREEN}✓ CDK bootstrapped${NC}"
else
    echo -e "${GREEN}✓ CDK already bootstrapped${NC}"
fi
echo ""

# Synthesize CloudFormation template
echo -e "${YELLOW}Synthesizing CloudFormation template...${NC}"
cdk synth \
    --context environment=$ENVIRONMENT \
    --context resourcePrefix=$RESOURCE_PREFIX \
    --context costCenter=$COST_CENTER \
    --context maxVcpus=$MAX_VCPUS \
    --context vpcCidr=$VPC_CIDR \
    > /dev/null

echo -e "${GREEN}✓ Template synthesized${NC}"
echo ""

# Deploy stack
echo -e "${YELLOW}Deploying stack...${NC}"
cdk deploy \
    --context environment=$ENVIRONMENT \
    --context resourcePrefix=$RESOURCE_PREFIX \
    --context costCenter=$COST_CENTER \
    --context maxVcpus=$MAX_VCPUS \
    --context vpcCidr=$VPC_CIDR \
    --require-approval never

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Deployment Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Get stack outputs
STACK_NAME="${RESOURCE_PREFIX}-stack-${ENVIRONMENT}"
echo -e "${YELLOW}Stack Outputs:${NC}"
aws cloudformation describe-stacks \
    --stack-name $STACK_NAME \
    --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' \
    --output table

# Build and push Docker image
echo -e "${YELLOW}Building and pushing Docker image...${NC}"

# Detect available container tool
CONTAINER_TOOL=""
if command -v podman &> /dev/null; then
    CONTAINER_TOOL="podman"
elif command -v docker &> /dev/null; then
    CONTAINER_TOOL="docker"
else
    echo -e "${RED}Error: Neither docker nor podman found${NC}"
    echo "Please install docker or podman to continue"
    exit 1
fi

echo -e "${GREEN}✓ Using $CONTAINER_TOOL${NC}"

# Get ECR repository URI
ECR_URI=$(aws cloudformation describe-stacks \
    --stack-name $STACK_NAME \
    --query 'Stacks[0].Outputs[?OutputKey==`EcrRepositoryUri`].OutputValue' \
    --output text)

if [ -z "$ECR_URI" ]; then
    echo -e "${RED}Error: Could not get ECR repository URI${NC}"
    exit 1
fi

echo "ECR Repository: $ECR_URI"

# Create timestamp-based tag for this deployment
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
IMAGE_TAG="deploy-${TIMESTAMP}"

# Build Docker image (must use AMD64 for Fargate)
echo -e "${YELLOW}Building Docker image...${NC}"
cd ..
$CONTAINER_TOOL build --platform linux/amd64 -t bedrock-sentiment-analyzer:$IMAGE_TAG .
echo -e "${GREEN}✓ Docker image built${NC}"

# Login to ECR
echo -e "${YELLOW}Authenticating with ECR...${NC}"
aws ecr get-login-password --region $REGION | \
    $CONTAINER_TOOL login --username AWS --password-stdin $ECR_URI
echo -e "${GREEN}✓ ECR authentication successful${NC}"

# Tag and push image with both timestamp and latest tags
echo -e "${YELLOW}Tagging and pushing image...${NC}"
$CONTAINER_TOOL tag bedrock-sentiment-analyzer:$IMAGE_TAG $ECR_URI:$IMAGE_TAG
$CONTAINER_TOOL tag bedrock-sentiment-analyzer:$IMAGE_TAG $ECR_URI:latest
$CONTAINER_TOOL push $ECR_URI:$IMAGE_TAG
$CONTAINER_TOOL push $ECR_URI:latest
echo -e "${GREEN}✓ Docker image pushed to ECR (tags: $IMAGE_TAG, latest)${NC}"

# Force update job definition to use new image
echo -e "${YELLOW}Updating job definition to use new image...${NC}"
cdk deploy \
    --context environment=$ENVIRONMENT \
    --context resourcePrefix=$RESOURCE_PREFIX \
    --context costCenter=$COST_CENTER \
    --context maxVcpus=$MAX_VCPUS \
    --context vpcCidr=$VPC_CIDR \
    --require-approval never \
    --force

echo -e "${GREEN}✓ Job definition updated${NC}"

cd cdk

echo ""
echo -e "${YELLOW}Next Steps:${NC}"
echo "1. Upload prompts to S3"
echo "2. Create configuration file"
echo "3. Submit Batch job"
echo ""
echo "See README.md for detailed instructions"
