#!/bin/bash
# One-time check of Bedrock model invocation rates

set -e

# Model IDs to monitor
MODELS=(
    "global.anthropic.claude-haiku-4-5-20251001-v1:0"
    "global.anthropic.claude-sonnet-4-20250514-v1:0"
    "global.anthropic.claude-sonnet-4-5-20250929-v1:0"
    "us.anthropic.claude-3-haiku-20240307-v1:0"
    "us.amazon.nova-pro-v1:0"
    "us.meta.llama4-scout-17b-instruct-v1:0"
    "us.meta.llama4-maverick-17b-instruct-v1:0"
)

MODEL_NAMES=(
    "Claude Haiku 4.5"
    "Claude Sonnet 4"
    "Claude Sonnet 4.5"
    "Claude 3 Haiku"
    "Nova Pro v1"
    "Llama4 Scout 17B"
    "Llama4 Maverick 17B"
)

# Time window (last N minutes)
MINUTES=${1:-5}

echo "=========================================="
echo "Bedrock Model Invocation Rates"
echo "=========================================="
echo "Time window: Last $MINUTES minutes"
echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

START_TIME=$(date -u -v-${MINUTES}M +%Y-%m-%dT%H:%M:%S)
END_TIME=$(date -u +%Y-%m-%dT%H:%M:%S)

printf "%-25s | %-15s | %-15s | %-30s\n" "Model" "Avg (req/min)" "Max (req/min)" "Recent Activity"
printf "%s\n" "$(printf '=%.0s' {1..100})"

for i in "${!MODELS[@]}"; do
    MODEL_ID="${MODELS[$i]}"
    MODEL_NAME="${MODEL_NAMES[$i]}"
    
    # Query CloudWatch for invocations per minute
    STATS=$(aws cloudwatch get-metric-statistics \
        --namespace AWS/Bedrock \
        --metric-name Invocations \
        --dimensions Name=ModelId,Value="$MODEL_ID" \
        --start-time "$START_TIME" \
        --end-time "$END_TIME" \
        --period 60 \
        --statistics Sum \
        --query 'Datapoints | sort_by(@, &Timestamp) | [*].Sum' \
        --output text 2>/dev/null)
    
    if [ -z "$STATS" ] || [ "$STATS" = "None" ]; then
        printf "%-25s | %-15s | %-15s | %-30s\n" "$MODEL_NAME" "No data" "-" "-"
    else
        # Calculate average and max
        TOTAL=0
        COUNT=0
        MAX=0
        RECENT=""
        for val in $STATS; do
            TOTAL=$(echo "$TOTAL + $val" | bc)
            COUNT=$((COUNT + 1))
            if (( $(echo "$val > $MAX" | bc -l) )); then
                MAX=$val
            fi
            # Keep last 3 values for recent activity
            if [ $COUNT -gt $(($(echo "$STATS" | wc -w) - 3)) ]; then
                RECENT="$RECENT${val%.0} "
            fi
        done
        
        if [ $COUNT -gt 0 ]; then
            AVG=$(echo "scale=1; $TOTAL / $COUNT" | bc)
            printf "%-25s | %15.1f | %15.0f | %-30s\n" \
                "$MODEL_NAME" "$AVG" "$MAX" "$RECENT"
        else
            printf "%-25s | %-15s | %-15s | %-30s\n" "$MODEL_NAME" "No data" "-" "-"
        fi
    fi
done

echo ""
echo "Note: Run './scripts/check-model-rates.sh 10' to check last 10 minutes"
