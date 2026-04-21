#!/bin/bash
# Monitor Bedrock model invocation rates in CloudWatch

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
MINUTES=${1:-10}

echo "=========================================="
echo "Bedrock Model Invocation Rate Monitor"
echo "=========================================="
echo "Time window: Last $MINUTES minutes"
echo "Refresh: Every 30 seconds (Ctrl+C to stop)"
echo ""

while true; do
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$TIMESTAMP]"
    echo ""
    
    for i in "${!MODELS[@]}"; do
        MODEL_ID="${MODELS[$i]}"
        MODEL_NAME="${MODEL_NAMES[$i]}"
        
        # Get metrics for last N minutes
        START_TIME=$(date -u -v-${MINUTES}M +%Y-%m-%dT%H:%M:%S)
        END_TIME=$(date -u +%Y-%m-%dT%H:%M:%S)
        
        # Query CloudWatch for invocations per minute
        STATS=$(aws cloudwatch get-metric-statistics \
            --namespace AWS/Bedrock \
            --metric-name Invocations \
            --dimensions Name=ModelId,Value="$MODEL_ID" \
            --start-time "$START_TIME" \
            --end-time "$END_TIME" \
            --period 60 \
            --statistics Sum \
            --query 'Datapoints | sort_by(@, &Timestamp)[-5:] | [*].Sum' \
            --output text 2>/dev/null)
        
        if [ -z "$STATS" ] || [ "$STATS" = "None" ]; then
            printf "  %-25s: No data\n" "$MODEL_NAME"
        else
            # Calculate average and max from last 5 minutes
            TOTAL=0
            COUNT=0
            MAX=0
            for val in $STATS; do
                TOTAL=$(echo "$TOTAL + $val" | bc)
                COUNT=$((COUNT + 1))
                if (( $(echo "$val > $MAX" | bc -l) )); then
                    MAX=$val
                fi
            done
            
            if [ $COUNT -gt 0 ]; then
                AVG=$(echo "scale=1; $TOTAL / $COUNT" | bc)
                printf "  %-25s: Avg: %6.1f req/min | Max: %6.0f req/min | Last 5min: %s\n" \
                    "$MODEL_NAME" "$AVG" "$MAX" "$STATS"
            else
                printf "  %-25s: No data\n" "$MODEL_NAME"
            fi
        fi
    done
    
    echo ""
    echo "Waiting 30 seconds for next refresh..."
    echo "=========================================="
    sleep 30
done
