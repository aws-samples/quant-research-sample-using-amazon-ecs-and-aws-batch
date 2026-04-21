import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as batch from 'aws-cdk-lib/aws-batch';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as logs from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import {
  S3TableBucketConstruct,
  S3TableConstruct,
  AthenaWorkgroupConstruct,
  S3TablesIAMPolicyConstruct,
} from './s3-tables-construct';

export interface BedrockSentimentAnalyzerStackProps extends cdk.StackProps {
  /**
   * Name prefix for all resources
   * @default 'bedrock-sentiment'
   */
  readonly resourcePrefix?: string;

  /**
   * VPC CIDR block
   * @default '10.0.0.0/16'
   */
  readonly vpcCidr?: string;

  /**
   * Maximum vCPUs for Batch compute environment
   * @default 16
   */
  readonly maxVcpus?: number;

  /**
   * Enable S3 bucket versioning
   * @default true
   */
  readonly enableBucketVersioning?: boolean;

  /**
   * S3 bucket lifecycle rules in days
   * @default 90
   */
  readonly s3LifecycleDays?: number;

  /**
   * CloudWatch Logs retention in days
   * @default logs.RetentionDays.ONE_MONTH
   */
  readonly logRetentionDays?: logs.RetentionDays;

  /**
   * Bedrock model ARN patterns to allow
   * @default ['arn:aws:bedrock:*::foundation-model/*']
   */
  readonly bedrockModelArns?: string[];

  /**
   * Enable VPC Flow Logs
   * @default true
   */
  readonly enableVpcFlowLogs?: boolean;

  /**
   * External S3 bucket names to grant read access to (e.g., shared data buckets)
   * @default []
   */
  readonly externalInputBuckets?: string[];

  /**
   * Enable cross-environment access (e.g., dev can read prod prompts)
   * @default false
   */
  readonly crossEnvironmentAccess?: boolean;

  /**
   * Environment names for cross-environment access
   * @default ['dev', 'prod']
   */
  readonly crossEnvironmentNames?: string[];

  /**
   * Enable S3 Tables for streaming batch output
   * @default true
   */
  readonly enableS3Tables?: boolean;

  /**
   * S3 Tables snapshot retention in days
   * @default 7
   */
  readonly s3TablesSnapshotRetentionDays?: number;
}

export class BedrockSentimentAnalyzerStack extends cdk.Stack {
  public readonly inputBucket: s3.Bucket;
  public readonly outputBucket: s3.Bucket;
  public readonly promptBucket: s3.Bucket;
  public readonly ecrRepository: ecr.Repository;
  public readonly batchJobQueue: batch.JobQueue;
  public readonly batchJobDefinition: batch.EcsJobDefinition;
  public readonly tableBucket?: S3TableBucketConstruct;
  public readonly sentimentTable?: S3TableConstruct;
  public readonly athenaWorkgroup?: AthenaWorkgroupConstruct;

  constructor(scope: Construct, id: string, props?: BedrockSentimentAnalyzerStackProps) {
    super(scope, id, props);

    const resourcePrefix = props?.resourcePrefix || 'bedrock-sentiment';
    const enableVersioning = props?.enableBucketVersioning ?? true;
    const lifecycleDays = props?.s3LifecycleDays || 90;
    const logRetention = props?.logRetentionDays || logs.RetentionDays.ONE_MONTH;
    const bedrockModelArns = props?.bedrockModelArns || ['arn:aws:bedrock:*::foundation-model/*'];
    const enableVpcFlowLogs = props?.enableVpcFlowLogs ?? true;
    const enableS3Tables = props?.enableS3Tables ?? true;
    const s3TablesSnapshotRetentionDays = props?.s3TablesSnapshotRetentionDays || 7;
    const externalInputBuckets = props?.externalInputBuckets || [];
    const crossEnvironmentAccess = props?.crossEnvironmentAccess ?? false;
    const crossEnvironmentNames = props?.crossEnvironmentNames || ['dev', 'prod'];
    const environment = this.node.tryGetContext('environment') || 'dev';
    const destroyOnRemoval = this.node.tryGetContext('destroy') === 'true';
    const removalPolicy = destroyOnRemoval ? cdk.RemovalPolicy.DESTROY : cdk.RemovalPolicy.RETAIN;
    // Derive base prefix (without environment suffix) for cross-env resource names
    const basePrefix = resourcePrefix.includes(`-${environment}`)
      ? resourcePrefix.replace(`-${environment}`, '')
      : resourcePrefix;

    // Get image tag from context (defaults to 'latest' for backward compatibility)
    // Use: cdk deploy --context imageTag=20250119-143022
    const imageTag = this.node.tryGetContext('imageTag') || 'latest';

    // ========================================
    // S3 Buckets with Security Best Practices
    // ========================================

    // Input bucket for news articles
    this.inputBucket = new s3.Bucket(this, 'InputBucket', {
      bucketName: `${resourcePrefix}-input-${this.account}-${this.region}`,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      versioned: enableVersioning,
      enforceSSL: true,
      lifecycleRules: [
        {
          id: 'DeleteOldArticles',
          enabled: true,
          expiration: cdk.Duration.days(lifecycleDays),
        },
      ],
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      autoDeleteObjects: false,
    });

    // Output bucket for Parquet results
    this.outputBucket = new s3.Bucket(this, 'OutputBucket', {
      bucketName: `${resourcePrefix}-output-${this.account}-${this.region}`,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      versioned: enableVersioning,
      enforceSSL: true,
      lifecycleRules: [
        {
          id: 'TransitionToIA',
          enabled: true,
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(30),
            },
          ],
        },
      ],
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      autoDeleteObjects: false,
    });

    // Prompt bucket for system and user prompts
    this.promptBucket = new s3.Bucket(this, 'PromptBucket', {
      bucketName: `${resourcePrefix}-prompts-${this.account}-${this.region}`,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      versioned: enableVersioning,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      autoDeleteObjects: false,
    });

    // ========================================
    // S3 Tables for Streaming Batch Output
    // ========================================

    if (enableS3Tables) {
      // Create S3 Table Bucket
      this.tableBucket = new S3TableBucketConstruct(this, 'TableBucket', {
        tableBucketName: `${resourcePrefix}-tables-${this.account}-${this.region}`,
        environment: this.node.tryGetContext('environment') || 'dev',
        enableVersioning: enableVersioning,
        transitionToIADays: 30,
      });

      // Create S3 Table for sentiment results
      // Note: S3 Tables only allows lowercase letters, numbers, and underscores in table names
      this.sentimentTable = new S3TableConstruct(this, 'SentimentTable', {
        tableBucket: this.tableBucket.tableBucket,
        namespace: 'default',
        tableName: `sentiment_results_${this.node.tryGetContext('environment') || 'dev'}`,
        environment: this.node.tryGetContext('environment') || 'dev',
        snapshotRetentionDays: s3TablesSnapshotRetentionDays,
      });

      // Create Athena Workgroup for querying S3 Tables
      this.athenaWorkgroup = new AthenaWorkgroupConstruct(this, 'AthenaWorkgroup', {
        workgroupName: `${resourcePrefix}-workgroup`,
        resultsBucket: this.outputBucket,
        resultsPrefix: 'athena-results/',
        environment: this.node.tryGetContext('environment') || 'dev',
      });
    }

    // ========================================
    // ECR Repository for Docker Image
    // ========================================

    this.ecrRepository = new ecr.Repository(this, 'EcrRepository', {
      repositoryName: `${resourcePrefix}-analyzer`,
      imageScanOnPush: true,
      imageTagMutability: ecr.TagMutability.IMMUTABLE,
      lifecycleRules: [
        {
          description: 'Keep last 10 images',
          maxImageCount: 10,
        },
      ],
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    // ========================================
    // VPC with Private Subnets
    // ========================================

    const vpc = new ec2.Vpc(this, 'Vpc', {
      vpcName: `${resourcePrefix}-vpc`,
      ipAddresses: ec2.IpAddresses.cidr(props?.vpcCidr || '10.0.0.0/16'),
      maxAzs: 2,
      natGateways: 1,
      subnetConfiguration: [
        {
          name: 'Public',
          subnetType: ec2.SubnetType.PUBLIC,
          cidrMask: 24,
        },
        {
          name: 'Private',
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
          cidrMask: 24,
        },
      ],
    });

    // VPC Flow Logs for security monitoring
    if (enableVpcFlowLogs) {
      const flowLogGroup = new logs.LogGroup(this, 'VpcFlowLogGroup', {
        logGroupName: `/aws/vpc/${resourcePrefix}`,
        retention: logRetention,
        removalPolicy: cdk.RemovalPolicy.RETAIN,
      });

      new ec2.FlowLog(this, 'VpcFlowLog', {
        resourceType: ec2.FlowLogResourceType.fromVpc(vpc),
        destination: ec2.FlowLogDestination.toCloudWatchLogs(flowLogGroup),
        trafficType: ec2.FlowLogTrafficType.ALL,
      });
    }

    // Security group for Batch compute environment
    const batchSecurityGroup = new ec2.SecurityGroup(this, 'BatchSecurityGroup', {
      vpc,
      description: 'Security group for Batch compute environment',
      allowAllOutbound: true, // Required for pulling ECR images and accessing AWS services
    });

    // ========================================
    // IAM Roles with Least Privilege
    // ========================================

    // Batch Job Execution Role (for ECS task execution)
    const executionRole = new iam.Role(this, 'BatchExecutionRole', {
      roleName: `${resourcePrefix}-batch-execution-role`,
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
      description: 'Execution role for Batch jobs to pull ECR images and write logs',
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonECSTaskExecutionRolePolicy'),
      ],
    });

    // Batch Job Role (for application permissions)
    const jobRole = new iam.Role(this, 'BatchJobRole', {
      roleName: `${resourcePrefix}-batch-job-role`,
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
      description: 'Job role for Batch tasks with least privilege access to S3 and Bedrock',
    });

    // Least privilege S3 permissions
    const inputResources: string[] = [
      this.inputBucket.bucketArn,
      `${this.inputBucket.bucketArn}/*`,
    ];
    // Add external input buckets if provided
    for (const bucket of externalInputBuckets) {
      inputResources.push(`arn:aws:s3:::${bucket}`);
      inputResources.push(`arn:aws:s3:::${bucket}/*`);
    }

    jobRole.addToPolicy(
      new iam.PolicyStatement({
        sid: 'ReadInputArticles',
        effect: iam.Effect.ALLOW,
        actions: ['s3:GetObject', 's3:ListBucket'],
        resources: inputResources,
      })
    );

    const promptResources: string[] = [
      this.promptBucket.bucketArn,
      `${this.promptBucket.bucketArn}/*`,
    ];
    if (crossEnvironmentAccess) {
      for (const env of crossEnvironmentNames) {
        promptResources.push(`arn:aws:s3:::${basePrefix}-${env}-prompts-${this.account}-${this.region}`);
        promptResources.push(`arn:aws:s3:::${basePrefix}-${env}-prompts-${this.account}-${this.region}/*`);
      }
    }

    jobRole.addToPolicy(
      new iam.PolicyStatement({
        sid: 'ReadPrompts',
        effect: iam.Effect.ALLOW,
        actions: ['s3:GetObject', 's3:ListBucket'],
        resources: promptResources,
      })
    );

    const outputResources: string[] = [
      `${this.outputBucket.bucketArn}/*`,
    ];
    if (crossEnvironmentAccess) {
      for (const env of crossEnvironmentNames) {
        outputResources.push(`arn:aws:s3:::${basePrefix}-${env}-output-${this.account}-${this.region}/*`);
      }
    }

    jobRole.addToPolicy(
      new iam.PolicyStatement({
        sid: 'WriteOutputResults',
        effect: iam.Effect.ALLOW,
        actions: ['s3:PutObject', 's3:PutObjectAcl'],
        resources: outputResources,
      })
    );

    // S3 Tables permissions (if enabled)
    if (enableS3Tables && this.tableBucket) {
      const s3TablesResources: string[] = [
        this.tableBucket.tableBucketArn,
        `${this.tableBucket.tableBucketArn}/*`,
      ];
      const s3BucketResources: string[] = [];

      if (crossEnvironmentAccess) {
        for (const env of crossEnvironmentNames) {
          const tableBucketName = `${basePrefix}-${env}-tables-${this.account}-${this.region}`;
          s3TablesResources.push(`arn:aws:s3tables:${this.region}:${this.account}:bucket/${tableBucketName}`);
          s3TablesResources.push(`arn:aws:s3tables:${this.region}:${this.account}:bucket/${tableBucketName}/*`);
          s3BucketResources.push(`arn:aws:s3:::${tableBucketName}`);
          s3BucketResources.push(`arn:aws:s3:::${tableBucketName}/*`);
        }
      }

      jobRole.addToPolicy(
        new iam.PolicyStatement({
          sid: 'S3TablesFullAccess',
          effect: iam.Effect.ALLOW,
          actions: [
            's3tables:GetTableBucket',
            's3tables:ListNamespaces',
            's3tables:GetNamespace',
            's3tables:CreateNamespace',
            's3tables:ListTables',
            's3tables:GetTable',
            's3tables:CreateTable',
            's3tables:GetTableMetadataLocation',
            's3tables:UpdateTableMetadataLocation',
            's3tables:GetTableData',
            's3tables:PutTableData',
            's3tables:UpdateTableMetadata',
          ],
          resources: s3TablesResources,
        })
      );

      // S3 access for table buckets (required for Iceberg operations)
      if (s3BucketResources.length > 0) {
        jobRole.addToPolicy(
          new iam.PolicyStatement({
            sid: 'S3TableBucketAccess',
            effect: iam.Effect.ALLOW,
            actions: [
              's3:PutObject',
              's3:GetObject',
              's3:DeleteObject',
              's3:ListBucket',
            ],
            resources: s3BucketResources,
          })
        );
      }
    }

    // Bedrock permissions with specific model ARNs
    jobRole.addToPolicy(
      new iam.PolicyStatement({
        sid: 'InvokeBedrockModels',
        effect: iam.Effect.ALLOW,
        actions: ['bedrock:InvokeModel'],
        resources: bedrockModelArns,
      })
    );

    // Bedrock permissions for inference profiles (cross-region inference)
    jobRole.addToPolicy(
      new iam.PolicyStatement({
        sid: 'InvokeBedrockInferenceProfiles',
        effect: iam.Effect.ALLOW,
        actions: ['bedrock:InvokeModel'],
        resources: [
          `arn:aws:bedrock:*:${this.account}:inference-profile/*`,
        ],
      })
    );

    // Bedrock permissions for custom model deployments (provisioned throughput, imported models)
    jobRole.addToPolicy(
      new iam.PolicyStatement({
        sid: 'InvokeBedrockCustomModels',
        effect: iam.Effect.ALLOW,
        actions: ['bedrock:InvokeModel'],
        resources: [
          `arn:aws:bedrock:*:${this.account}:custom-model/*`,
          `arn:aws:bedrock:*:${this.account}:custom-model-deployment/*`,
          `arn:aws:bedrock:*:${this.account}:provisioned-model/*`,
        ],
      })
    );

    // CloudWatch Logs permissions
    jobRole.addToPolicy(
      new iam.PolicyStatement({
        sid: 'WriteCloudWatchLogs',
        effect: iam.Effect.ALLOW,
        actions: [
          'logs:CreateLogGroup',
          'logs:CreateLogStream',
          'logs:PutLogEvents',
        ],
        resources: [
          `arn:aws:logs:${this.region}:${this.account}:log-group:/aws/batch/${resourcePrefix}*`,
        ],
      })
    );

    // Batch Service Role
    const batchServiceRole = new iam.Role(this, 'BatchServiceRole', {
      roleName: `${resourcePrefix}-batch-service-role`,
      assumedBy: new iam.ServicePrincipal('batch.amazonaws.com'),
      description: 'Service role for AWS Batch',
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSBatchServiceRole'),
      ],
    });

    // ========================================
    // CloudWatch Log Group
    // ========================================

    const logGroup = new logs.LogGroup(this, 'BatchLogGroup', {
      logGroupName: `/aws/batch/${resourcePrefix}`,
      retention: logRetention,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    // ========================================
    // AWS Batch Compute Environment
    // ========================================

    const computeEnvironment = new batch.FargateComputeEnvironment(this, 'ComputeEnvironment', {
      computeEnvironmentName: `${resourcePrefix}-compute-env`,
      vpc,
      vpcSubnets: {
        subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
      },
      securityGroups: [batchSecurityGroup],
      maxvCpus: props?.maxVcpus || 16,
      serviceRole: batchServiceRole,
    });

    // ========================================
    // AWS Batch Job Queue
    // ========================================

    this.batchJobQueue = new batch.JobQueue(this, 'JobQueue', {
      jobQueueName: `${resourcePrefix}-job-queue`,
      priority: 1,
      computeEnvironments: [
        {
          computeEnvironment,
          order: 1,
        },
      ],
    });

    // ========================================
    // AWS Batch Job Definition
    // ========================================

    this.batchJobDefinition = new batch.EcsJobDefinition(this, 'JobDefinition', {
      jobDefinitionName: `${resourcePrefix}-job-definition`,
      container: new batch.EcsFargateContainerDefinition(this, 'Container', {
        image: ecs.ContainerImage.fromEcrRepository(this.ecrRepository, imageTag),
        cpu: 4,
        memory: cdk.Size.mebibytes(16384),  // 16GB for 265K+ articles with created_at field
        executionRole,
        jobRole,
        logging: ecs.LogDriver.awsLogs({
          streamPrefix: 'batch-job',
          logGroup,
        }),
        environment: {
          AWS_DEFAULT_REGION: this.region,
        },
        // Command will be overridden at job submission time
        command: ['--config', 'config.json'],
      }),
      retryAttempts: 1,
      timeout: cdk.Duration.days(14),  // 14 days (336 hours) - AWS Batch maximum, allows all models to complete with rate limiting
    });

    // ========================================
    // CloudFormation Outputs
    // ========================================

    new cdk.CfnOutput(this, 'InputBucketName', {
      value: this.inputBucket.bucketName,
      description: 'S3 bucket for input news articles',
      exportName: `${resourcePrefix}-input-bucket`,
    });

    new cdk.CfnOutput(this, 'OutputBucketName', {
      value: this.outputBucket.bucketName,
      description: 'S3 bucket for output Parquet results',
      exportName: `${resourcePrefix}-output-bucket`,
    });

    new cdk.CfnOutput(this, 'PromptBucketName', {
      value: this.promptBucket.bucketName,
      description: 'S3 bucket for prompt templates',
      exportName: `${resourcePrefix}-prompt-bucket`,
    });

    new cdk.CfnOutput(this, 'EcrRepositoryUri', {
      value: this.ecrRepository.repositoryUri,
      description: 'ECR repository URI for Docker images',
      exportName: `${resourcePrefix}-ecr-repository-uri`,
    });

    new cdk.CfnOutput(this, 'JobQueueArn', {
      value: this.batchJobQueue.jobQueueArn,
      description: 'AWS Batch job queue ARN',
      exportName: `${resourcePrefix}-job-queue-arn`,
    });

    new cdk.CfnOutput(this, 'ImageTag', {
      value: imageTag,
      description: 'Docker image tag used in job definition',
      exportName: `${resourcePrefix}-image-tag`,
    });

    new cdk.CfnOutput(this, 'JobDefinitionArn', {
      value: this.batchJobDefinition.jobDefinitionArn,
      description: 'AWS Batch job definition ARN',
      exportName: `${resourcePrefix}-job-definition-arn`,
    });

    new cdk.CfnOutput(this, 'VpcId', {
      value: vpc.vpcId,
      description: 'VPC ID',
      exportName: `${resourcePrefix}-vpc-id`,
    });

    // S3 Tables outputs (if enabled)
    if (enableS3Tables && this.tableBucket && this.sentimentTable && this.athenaWorkgroup) {
      new cdk.CfnOutput(this, 'TableBucketArn', {
        value: this.tableBucket.tableBucketArn,
        description: 'S3 Table Bucket ARN',
        exportName: `${resourcePrefix}-table-bucket-arn`,
      });

      new cdk.CfnOutput(this, 'SentimentTableName', {
        value: this.sentimentTable.tableName,
        description: 'S3 Table name for sentiment results',
        exportName: `${resourcePrefix}-sentiment-table-name`,
      });

      new cdk.CfnOutput(this, 'SentimentTableArn', {
        value: this.sentimentTable.tableArn,
        description: 'S3 Table ARN for sentiment results',
        exportName: `${resourcePrefix}-sentiment-table-arn`,
      });

      new cdk.CfnOutput(this, 'TableWarehousePath', {
        value: `s3://${resourcePrefix}-tables-${this.account}-${this.region}/`,
        description: 'S3 Tables warehouse path for PyIceberg',
        exportName: `${resourcePrefix}-table-warehouse-path`,
      });

      new cdk.CfnOutput(this, 'AthenaWorkgroupName', {
        value: this.athenaWorkgroup.workgroupName,
        description: 'Athena workgroup for querying S3 Tables',
        exportName: `${resourcePrefix}-athena-workgroup-name`,
      });
    }

    // ========================================
    // Tags for All Resources
    // ========================================

    cdk.Tags.of(this).add('Project', 'BedrockSentimentAnalyzer');
    cdk.Tags.of(this).add('ManagedBy', 'CDK');
    cdk.Tags.of(this).add('Environment', this.node.tryGetContext('environment') || 'dev');
  }
}
