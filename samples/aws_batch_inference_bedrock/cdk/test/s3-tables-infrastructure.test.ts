import * as cdk from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { BedrockSentimentAnalyzerStack } from '../lib/bedrock-sentiment-analyzer-stack';

describe('S3 Tables Infrastructure Integration', () => {
  test('creates complete S3 Tables infrastructure when enabled', () => {
    const app = new cdk.App({
      context: {
        environment: 'test',
      },
    });
    
    const stack = new BedrockSentimentAnalyzerStack(app, 'TestStack', {
      resourcePrefix: 'test-sentiment',
      enableS3Tables: true,
    });
    
    const template = Template.fromStack(stack);

    // Verify S3 Table Bucket is created
    template.resourceCountIs('AWS::S3Tables::TableBucket', 1);

    // Verify S3 Table is created
    template.resourceCountIs('AWS::S3Tables::Table', 1);

    // Verify Athena Workgroup is created
    template.resourceCountIs('AWS::Athena::WorkGroup', 1);

    // Verify IAM policies include S3 Tables permissions
    const resources = template.toJSON().Resources;
    const policies = Object.values(resources).filter(
      (r: any) => r.Type === 'AWS::IAM::Policy'
    );
    
    expect(policies.length).toBeGreaterThan(0);
    
    // Check that at least one policy has S3 Tables permissions
    const hasS3TablesPermissions = policies.some((policy: any) => {
      const statements = policy.Properties?.PolicyDocument?.Statement || [];
      return statements.some((stmt: any) => 
        stmt.Sid === 'WriteToS3Tables' || 
        stmt.Sid === 'S3TableBucketAccess'
      );
    });
    
    expect(hasS3TablesPermissions).toBe(true);
  });

  test('does not create S3 Tables infrastructure when disabled', () => {
    const app = new cdk.App();
    
    const stack = new BedrockSentimentAnalyzerStack(app, 'TestStack', {
      resourcePrefix: 'test-sentiment',
      enableS3Tables: false,
    });
    
    const template = Template.fromStack(stack);

    // Verify S3 Tables resources are not created
    template.resourceCountIs('AWS::S3Tables::TableBucket', 0);
    template.resourceCountIs('AWS::S3Tables::Table', 0);
    template.resourceCountIs('AWS::Athena::WorkGroup', 0);
  });

  test('exports S3 Tables outputs when enabled', () => {
    const app = new cdk.App({
      context: {
        environment: 'test',
      },
    });
    
    const stack = new BedrockSentimentAnalyzerStack(app, 'TestStack', {
      resourcePrefix: 'test-sentiment',
      enableS3Tables: true,
    });
    
    const template = Template.fromStack(stack);

    // Verify outputs are created
    template.hasOutput('TableBucketArn', {});
    template.hasOutput('SentimentTableName', {});
    template.hasOutput('SentimentTableArn', {});
    template.hasOutput('TableWarehousePath', {});
    template.hasOutput('AthenaWorkgroupName', {});
  });

  test('creates all required base infrastructure', () => {
    const app = new cdk.App();
    
    const stack = new BedrockSentimentAnalyzerStack(app, 'TestStack', {
      resourcePrefix: 'test-sentiment',
    });
    
    const template = Template.fromStack(stack);

    // Verify base infrastructure
    template.resourceCountIs('AWS::S3::Bucket', 3); // input, output, prompt
    template.resourceCountIs('AWS::ECR::Repository', 1);
    template.resourceCountIs('AWS::EC2::VPC', 1);
    template.resourceCountIs('AWS::Batch::ComputeEnvironment', 1);
    template.resourceCountIs('AWS::Batch::JobQueue', 1);
    template.resourceCountIs('AWS::Batch::JobDefinition', 1);
  });
});
