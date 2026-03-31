#!/bin/bash
# undeploy.sh - Remove all resources created by deploy.sh
# Only deletes resources tracked in the CloudFormation stack outputs
# Usage: ./scripts/undeploy.sh [environment]

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ENVIRONMENT="${1:-dev}"
AWS_REGION="${AWS_REGION:-us-east-1}"
STACK_NAME="${STACK_NAME:-bedrock-sentiment-stack-${ENVIRONMENT}}"

echo -e "${RED}=========================================="
echo "⚠️  DESTROY ALL RESOURCES"
echo -e "==========================================${NC}"
echo "Stack: $STACK_NAME"
echo "Region: $AWS_REGION"
echo ""

# Verify stack exists
if ! aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$AWS_REGION" > /dev/null 2>&1; then
    echo -e "${RED}Stack $STACK_NAME not found. Nothing to undeploy.${NC}"
    exit 1
fi

# Read all resource identifiers from stack outputs
get_output() {
    aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$AWS_REGION" \
      --query "Stacks[0].Outputs[?OutputKey==\`$1\`].OutputValue" --output text 2>/dev/null || true
}

INPUT_BUCKET=$(get_output InputBucketName)
OUTPUT_BUCKET=$(get_output OutputBucketName)
PROMPT_BUCKET=$(get_output PromptBucketName)
ECR_URI=$(get_output EcrRepositoryUri)
JOB_QUEUE=$(get_output JobQueueArn)
TABLE_BUCKET_ARN=$(get_output TableBucketArn)

echo "Resources from stack outputs:"
[ -n "$INPUT_BUCKET" ]    && echo "  Input bucket:  $INPUT_BUCKET"
[ -n "$OUTPUT_BUCKET" ]   && echo "  Output bucket: $OUTPUT_BUCKET"
[ -n "$PROMPT_BUCKET" ]   && echo "  Prompt bucket: $PROMPT_BUCKET"
[ -n "$TABLE_BUCKET_ARN" ] && echo "  Table bucket:  $TABLE_BUCKET_ARN"
[ -n "$ECR_URI" ]         && echo "  ECR repo:      $ECR_URI"
[ -n "$JOB_QUEUE" ]       && echo "  Job queue:     $JOB_QUEUE"
echo ""

echo -e "${RED}This will permanently delete ALL data and infrastructure.${NC}"
read -p "Type 'yes' to confirm: " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    echo "Aborted."
    exit 0
fi

# Helper: empty and delete an S3 bucket (handles versioning)
delete_s3_bucket() {
    local BUCKET="$1"
    if ! aws s3api head-bucket --bucket "$BUCKET" --region "$AWS_REGION" 2>/dev/null; then
        echo "  Bucket $BUCKET not found, skipping"
        return
    fi
    echo "  Emptying $BUCKET..."
    aws s3 rm "s3://$BUCKET" --recursive --region "$AWS_REGION" 2>/dev/null || true
    # Delete versioned objects and delete markers
    for QUERY in '[Versions,DeleteMarkers][].{Key:Key,VersionId:VersionId}'; do
        local OBJECTS
        OBJECTS=$(aws s3api list-object-versions --bucket "$BUCKET" --region "$AWS_REGION" \
          --query "{Objects: $QUERY}" --output json 2>/dev/null)
        if [ "$OBJECTS" != '{"Objects": null}' ] && [ "$OBJECTS" != '{"Objects": []}' ] && [ -n "$OBJECTS" ]; then
            aws s3api delete-objects --bucket "$BUCKET" --region "$AWS_REGION" \
              --delete "$OBJECTS" > /dev/null 2>&1 || true
        fi
    done
    echo "  Deleting bucket $BUCKET..."
    aws s3api delete-bucket --bucket "$BUCKET" --region "$AWS_REGION" 2>/dev/null || true
}

# Step 1: Cancel running Batch jobs
echo ""
echo -e "${YELLOW}Step 1/5: Cancelling running Batch jobs...${NC}"
if [ -n "$JOB_QUEUE" ]; then
    for STATUS in SUBMITTED PENDING RUNNABLE STARTING RUNNING; do
        JOBS=$(aws batch list-jobs --job-queue "$JOB_QUEUE" --job-status "$STATUS" \
          --query 'jobSummaryList[*].jobId' --output text --region "$AWS_REGION" 2>/dev/null || true)
        for JOB_ID in $JOBS; do
            echo "  Cancelling job: $JOB_ID"
            aws batch cancel-job --job-id "$JOB_ID" --reason "Stack teardown" --region "$AWS_REGION" 2>/dev/null || \
            aws batch terminate-job --job-id "$JOB_ID" --reason "Stack teardown" --region "$AWS_REGION" 2>/dev/null || true
        done
    done
fi
echo -e "${GREEN}✅ Batch jobs cancelled${NC}"

# Step 2: Empty and delete S3 buckets (RETAIN policy means CDK won't delete them)
echo ""
echo -e "${YELLOW}Step 2/5: Emptying and deleting S3 buckets...${NC}"
for BUCKET in $INPUT_BUCKET $OUTPUT_BUCKET $PROMPT_BUCKET; do
    [ -n "$BUCKET" ] && delete_s3_bucket "$BUCKET"
done
echo -e "${GREEN}✅ S3 buckets deleted${NC}"

# Step 3: Delete S3 Table Bucket
echo ""
echo -e "${YELLOW}Step 3/5: Deleting S3 Table Bucket...${NC}"
if [ -n "$TABLE_BUCKET_ARN" ]; then
    # Delete tables first
    TABLES=$(aws s3tables list-tables --table-bucket-arn "$TABLE_BUCKET_ARN" --region "$AWS_REGION" \
      --query 'tables[*].[namespace,name]' --output text 2>/dev/null || true)
    if [ -n "$TABLES" ]; then
        while IFS=$'\t' read -r NAMESPACE TABLE_NAME; do
            echo "  Deleting table $NAMESPACE.$TABLE_NAME..."
            aws s3tables delete-table --table-bucket-arn "$TABLE_BUCKET_ARN" \
              --namespace "$NAMESPACE" --name "$TABLE_NAME" --region "$AWS_REGION" 2>/dev/null || true
        done <<< "$TABLES"
    fi
    # Delete namespaces
    NAMESPACES=$(aws s3tables list-namespaces --table-bucket-arn "$TABLE_BUCKET_ARN" --region "$AWS_REGION" \
      --query 'namespaces[*].namespace[0]' --output text 2>/dev/null || true)
    if [ -n "$NAMESPACES" ]; then
        for NS in $NAMESPACES; do
            echo "  Deleting namespace $NS..."
            aws s3tables delete-namespace --table-bucket-arn "$TABLE_BUCKET_ARN" \
              --namespace "$NS" --region "$AWS_REGION" 2>/dev/null || true
        done
    fi
    echo "  Deleting table bucket..."
    aws s3tables delete-table-bucket --table-bucket-arn "$TABLE_BUCKET_ARN" --region "$AWS_REGION" 2>/dev/null || true
else
    echo "  No table bucket found in stack outputs, skipping"
fi
echo -e "${GREEN}✅ S3 Table Bucket deleted${NC}"

# Step 4: Delete ECR repository (RETAIN policy)
echo ""
echo -e "${YELLOW}Step 4/5: Deleting ECR repository...${NC}"
if [ -n "$ECR_URI" ]; then
    ECR_REPO=$(echo "$ECR_URI" | sed 's|.*/||')
    aws ecr delete-repository --repository-name "$ECR_REPO" --region "$AWS_REGION" --force 2>/dev/null || true
else
    echo "  No ECR repository found in stack outputs, skipping"
fi
echo -e "${GREEN}✅ ECR repository deleted${NC}"

# Step 5: Destroy CDK stack
echo ""
echo -e "${YELLOW}Step 5/5: Destroying CDK stack...${NC}"
cd cdk
npx cdk destroy --force --context environment="$ENVIRONMENT"
cd ..
echo -e "${GREEN}✅ CDK stack destroyed${NC}"

echo ""
echo -e "${GREEN}=========================================="
echo "✅ All resources removed!"
echo -e "==========================================${NC}"
