#!/usr/bin/env python3
"""Calculate processing time for sentiment analysis based on rate limits."""

import json
from datetime import timedelta

# Load config
with open('config.json', 'r') as f:
    config = json.load(f)

# Parameters
total_articles = 300000
avg_symbols_per_article = 1.5  # Conservative estimate
components_per_symbol = 3  # content, headline, summary
avg_text_length = 2000  # characters per component (conservative)
chars_per_token = 4  # token estimation
prompt_overhead_tokens = 100  # system + user prompt

# Calculate total tasks
total_tasks = total_articles * avg_symbols_per_article * components_per_symbol
print(f"Total articles: {total_articles:,}")
print(f"Average symbols per article: {avg_symbols_per_article}")
print(f"Components per symbol: {components_per_symbol}")
print(f"Total analysis tasks: {total_tasks:,.0f}")
print()

# Calculate tokens per task
tokens_per_task = (avg_text_length / chars_per_token) + prompt_overhead_tokens
print(f"Average text length: {avg_text_length:,} characters")
print(f"Estimated tokens per task: {tokens_per_task:.0f}")
print()

# Calculate processing time for each model
print("=" * 80)
print("PROCESSING TIME PER MODEL")
print("=" * 80)

for model in config['models']:
    model_name = model['model_name']
    rpm = model['requests_per_minute']
    tpm = model['tokens_per_minute']
    
    # Calculate time based on request rate limit
    time_by_requests_minutes = total_tasks / rpm
    
    # Calculate time based on token rate limit
    total_tokens = total_tasks * tokens_per_task
    time_by_tokens_minutes = total_tokens / tpm
    
    # Bottleneck is the slower of the two
    time_minutes = max(time_by_requests_minutes, time_by_tokens_minutes)
    time_hours = time_minutes / 60
    time_days = time_hours / 24
    
    # Determine bottleneck
    bottleneck = "requests" if time_by_requests_minutes > time_by_tokens_minutes else "tokens"
    
    print(f"\n{model_name}:")
    print(f"  Rate limits: {rpm:,} req/min, {tpm:,} tokens/min")
    print(f"  Time by requests: {time_by_requests_minutes:,.1f} min ({time_by_requests_minutes/60:.1f} hours)")
    print(f"  Time by tokens: {time_by_tokens_minutes:,.1f} min ({time_by_tokens_minutes/60:.1f} hours)")
    print(f"  Bottleneck: {bottleneck}")
    print(f"  Total time: {time_minutes:,.1f} minutes = {time_hours:.1f} hours = {time_days:.2f} days")
    
    # Format as human-readable
    td = timedelta(minutes=time_minutes)
    days = td.days
    hours = td.seconds // 3600
    minutes = (td.seconds % 3600) // 60
    print(f"  Human readable: {days} days, {hours} hours, {minutes} minutes")

print()
print("=" * 80)
print("SUMMARY")
print("=" * 80)

# Find slowest model
slowest_time = 0
slowest_model = ""
for model in config['models']:
    rpm = model['requests_per_minute']
    tpm = model['tokens_per_minute']
    time_by_requests = total_tasks / rpm
    time_by_tokens = (total_tasks * tokens_per_task) / tpm
    time_minutes = max(time_by_requests, time_by_tokens)
    
    if time_minutes > slowest_time:
        slowest_time = time_minutes
        slowest_model = model['model_name']

slowest_hours = slowest_time / 60
slowest_days = slowest_hours / 24

print(f"\nSlowest model: {slowest_model}")
print(f"Processing time: {slowest_time:,.1f} minutes = {slowest_hours:.1f} hours = {slowest_days:.2f} days")

td = timedelta(minutes=slowest_time)
days = td.days
hours = td.seconds // 3600
minutes = (td.seconds % 3600) // 60
print(f"Human readable: {days} days, {hours} hours, {minutes} minutes")

# Check against timeout
timeout_hours = 14 * 24  # 14 days
if slowest_hours > timeout_hours:
    print(f"\n⚠️  WARNING: Processing time ({slowest_hours:.1f}h) exceeds AWS Batch timeout ({timeout_hours}h)")
    print(f"   Consider increasing rate limits or splitting into multiple jobs")
else:
    print(f"\n✅ Processing time is within AWS Batch timeout ({timeout_hours}h)")
    print(f"   Remaining buffer: {timeout_hours - slowest_hours:.1f} hours")

# S3 reading time
s3_reading_minutes = 20  # Conservative estimate for 300K articles
print(f"\nS3 reading time (parallel): ~{s3_reading_minutes} minutes")
print(f"Total end-to-end time: ~{slowest_hours:.1f} hours + {s3_reading_minutes/60:.1f} hours = {slowest_hours + s3_reading_minutes/60:.1f} hours")
