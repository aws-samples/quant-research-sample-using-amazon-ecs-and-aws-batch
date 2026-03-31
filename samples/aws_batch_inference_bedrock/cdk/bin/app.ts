#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { BedrockSentimentAnalyzerStack } from '../lib/bedrock-sentiment-analyzer-stack';
import * as logs from 'aws-cdk-lib/aws-logs';

const app = new cdk.App();

// Get context values or use defaults
const environment = app.node.tryGetContext('environment') || 'dev';
const resourcePrefix = app.node.tryGetContext('resourcePrefix') || 'bedrock-sentiment';
const maxVcpus = Number(app.node.tryGetContext('maxVcpus')) || 16;
const vpcCidr = app.node.tryGetContext('vpcCidr') || '10.0.0.0/16';

// Environment-specific configuration
const envConfig = {
  dev: {
    logRetentionDays: logs.RetentionDays.ONE_WEEK,
    s3LifecycleDays: 30,
    enableBucketVersioning: false,
  },
  staging: {
    logRetentionDays: logs.RetentionDays.TWO_WEEKS,
    s3LifecycleDays: 60,
    enableBucketVersioning: true,
  },
  prod: {
    logRetentionDays: logs.RetentionDays.ONE_MONTH,
    s3LifecycleDays: 90,
    enableBucketVersioning: true,
  },
};

const config = envConfig[environment as keyof typeof envConfig] || envConfig.dev;

new BedrockSentimentAnalyzerStack(app, `${resourcePrefix}-stack-${environment}`, {
  stackName: `${resourcePrefix}-stack-${environment}`,
  description: `Bedrock News Sentiment Analyzer Infrastructure (${environment})`,
  
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1',
  },

  resourcePrefix: `${resourcePrefix}-${environment}`,
  vpcCidr,
  maxVcpus,
  enableBucketVersioning: config.enableBucketVersioning,
  s3LifecycleDays: config.s3LifecycleDays,
  logRetentionDays: config.logRetentionDays,
  enableVpcFlowLogs: true,

  // External S3 buckets to grant read access to (for shared data sources)
  externalInputBuckets: (app.node.tryGetContext('externalInputBuckets') || '').split(',').filter(Boolean),

  // Cross-environment access (set to true if dev/prod need to share resources)
  crossEnvironmentAccess: app.node.tryGetContext('crossEnvironmentAccess') === 'true',
  crossEnvironmentNames: ['dev', 'prod'],

  // Bedrock model ARNs - customize as needed
  // Note: Inference profiles are handled separately in the stack
  bedrockModelArns: [
    'arn:aws:bedrock:*::foundation-model/anthropic.claude-*',
    'arn:aws:bedrock:*::foundation-model/amazon.titan-*',
    'arn:aws:bedrock:*::foundation-model/amazon.nova-*',
    'arn:aws:bedrock:*::foundation-model/meta.llama*',
    'arn:aws:bedrock:*::foundation-model/ai21.jamba-*',
    'arn:aws:bedrock:*::foundation-model/cohere.command-*',
  ],

  tags: {
    Project: 'BedrockSentimentAnalyzer',
    Environment: environment,
    ManagedBy: 'CDK',
    CostCenter: app.node.tryGetContext('costCenter') || 'Engineering',
  },
});

app.synth();
