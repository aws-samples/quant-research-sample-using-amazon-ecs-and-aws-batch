import * as cdk from 'aws-cdk-lib';
import * as s3tables from 'aws-cdk-lib/aws-s3tables';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as athena from 'aws-cdk-lib/aws-athena';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

export interface S3TableBucketProps {
  /**
   * Name for the S3 Table Bucket
   */
  readonly tableBucketName: string;

  /**
   * Environment name (dev, staging, prod)
   */
  readonly environment: string;

  /**
   * Enable versioning for the table bucket
   * @default true
   */
  readonly enableVersioning?: boolean;

  /**
   * Lifecycle transition to Standard-IA after days
   * @default 30
   */
  readonly transitionToIADays?: number;
}

/**
 * Construct for creating an S3 Table Bucket with appropriate configuration
 * for storing Apache Iceberg tables.
 */
export class S3TableBucketConstruct extends Construct {
  public readonly tableBucket: s3tables.CfnTableBucket;
  public readonly tableBucketArn: string;

  constructor(scope: Construct, id: string, props: S3TableBucketProps) {
    super(scope, id);

    const enableVersioning = props.enableVersioning ?? true;
    const transitionToIADays = props.transitionToIADays || 30;

    // Create S3 Table Bucket
    // Note: S3 Tables automatically handle encryption, versioning, and maintenance
    this.tableBucket = new s3tables.CfnTableBucket(this, 'TableBucket', {
      tableBucketName: props.tableBucketName,
    });

    // S3 Tables bucket ARN format: arn:aws:s3tables:region:account:bucket/bucket-name
    this.tableBucketArn = cdk.Stack.of(this).formatArn({
      service: 's3tables',
      resource: 'bucket',
      resourceName: props.tableBucketName,
    });

    // Add tags
    cdk.Tags.of(this.tableBucket).add('Name', props.tableBucketName);
    cdk.Tags.of(this.tableBucket).add('Environment', props.environment);
    cdk.Tags.of(this.tableBucket).add('Purpose', 'S3TablesStorage');
  }
}

export interface S3TableProps {
  /**
   * The S3 Table Bucket to create the table in
   */
  readonly tableBucket: s3tables.CfnTableBucket;

  /**
   * Name for the S3 Table
   */
  readonly tableName: string;

  /**
   * Namespace for the table
   * @default 'default'
   */
  readonly namespace: string;

  /**
   * Environment name (dev, staging, prod)
   */
  readonly environment: string;

  /**
   * Snapshot retention in days
   * @default 7
   */
  readonly snapshotRetentionDays?: number;
}

/**
 * Construct for creating an S3 Table with Apache Iceberg format,
 * configured for sentiment analysis results storage.
 */
export class S3TableConstruct extends Construct {
  public readonly table: s3tables.CfnTable;
  public readonly namespace: s3tables.CfnNamespace;
  public readonly tableArn: string;
  public readonly tableName: string;

  constructor(scope: Construct, id: string, props: S3TableProps) {
    super(scope, id);

    const snapshotRetentionDays = props.snapshotRetentionDays || 7;

    // Create S3 Table with Iceberg format
    // Note: Schema and partitioning will be managed by PyIceberg at runtime
    // This allows for flexible schema evolution and partition management
    const tableBucketArn = cdk.Stack.of(this).formatArn({
      service: 's3tables',
      resource: 'bucket',
      resourceName: props.tableBucket.tableBucketName || 'unknown',
    });

    // Create the namespace first (required before creating tables)
    this.namespace = new s3tables.CfnNamespace(this, 'Namespace', {
      tableBucketArn: tableBucketArn,
      namespace: props.namespace,
    });

    // Ensure namespace is created after bucket
    this.namespace.addDependency(props.tableBucket);

    // Create the table using CloudFormation properties
    // S3 Tables is a new service, so we use minimal configuration
    // Schema and partitioning will be managed by PyIceberg at runtime
    this.table = new s3tables.CfnTable(this, 'Table', {
      tableBucketArn: tableBucketArn,
      namespace: props.namespace,
      tableName: props.tableName,
      openTableFormat: 'ICEBERG',
      // IcebergMetadata with IcebergSchema is required for table creation
      icebergMetadata: {
        icebergSchema: {
          schemaFieldList: [
            { name: 'article_id', type: 'string', required: true },
            { name: 'symbol', type: 'string', required: true },
            { name: 'component', type: 'string', required: true },
            { name: 'model_id', type: 'string', required: true },
            { name: 'sentiment_score', type: 'double', required: false },
            { name: 'model_name', type: 'string', required: true },
            { name: 'timestamp', type: 'timestamp', required: true },
            { name: 'job_name', type: 'string', required: true },
            { name: 'rating_scale_min', type: 'int', required: true },
            { name: 'rating_scale_max', type: 'int', required: true },
            { name: 'created_at', type: 'timestamp', required: false },
          ],
        },
      },
      // Compaction settings
      compaction: {
        status: 'enabled',
        targetFileSizeMb: 512,
      },
      // Snapshot management settings
      snapshotManagement: {
        minSnapshotsToKeep: 3,
        maxSnapshotAgeHours: snapshotRetentionDays * 24,
      },
    } as any); // Use 'as any' to bypass TypeScript checking for new service

    // Ensure table is created after namespace
    this.table.addDependency(this.namespace);

    // S3 Tables table ARN format: arn:aws:s3tables:region:account:bucket/bucket-name/table/table-name
    this.tableArn = cdk.Stack.of(this).formatArn({
      service: 's3tables',
      resource: `bucket/${props.tableBucket.tableBucketName || 'unknown'}/table`,
      resourceName: props.tableName,
    });
    this.tableName = props.tableName;

    // Add tags
    cdk.Tags.of(this.table).add('Name', props.tableName);
    cdk.Tags.of(this.table).add('Environment', props.environment);
    cdk.Tags.of(this.table).add('Format', 'Iceberg');
    cdk.Tags.of(this.table).add('Purpose', 'SentimentResults');
  }
}

export interface AthenaWorkgroupProps {
  /**
   * Name for the Athena workgroup
   */
  readonly workgroupName: string;

  /**
   * S3 bucket for Athena query results
   */
  readonly resultsBucket: s3.IBucket;

  /**
   * Results prefix in the bucket
   * @default 'athena-results/'
   */
  readonly resultsPrefix?: string;

  /**
   * Environment name (dev, staging, prod)
   */
  readonly environment: string;
}

/**
 * Construct for creating an Athena Workgroup configured for querying
 * S3 Tables with Iceberg format support (Athena v3).
 */
export class AthenaWorkgroupConstruct extends Construct {
  public readonly workgroup: athena.CfnWorkGroup;
  public readonly workgroupName: string;

  constructor(scope: Construct, id: string, props: AthenaWorkgroupProps) {
    super(scope, id);

    const resultsPrefix = props.resultsPrefix || 'athena-results/';

    // Create Athena Workgroup with v3 engine for Iceberg support
    this.workgroup = new athena.CfnWorkGroup(this, 'Workgroup', {
      name: props.workgroupName,
      workGroupConfiguration: {
        resultConfiguration: {
          outputLocation: `s3://${props.resultsBucket.bucketName}/${resultsPrefix}`,
          encryptionConfiguration: {
            encryptionOption: 'SSE_S3',
          },
        },
        engineVersion: {
          selectedEngineVersion: 'Athena engine version 3',
        },
        enforceWorkGroupConfiguration: true,
        publishCloudWatchMetricsEnabled: true,
      },
      description: `Athena workgroup for querying S3 Tables in ${props.environment} environment`,
      state: 'ENABLED',
    });

    this.workgroupName = props.workgroupName;

    // Add tags
    cdk.Tags.of(this.workgroup).add('Name', props.workgroupName);
    cdk.Tags.of(this.workgroup).add('Environment', props.environment);
    cdk.Tags.of(this.workgroup).add('Purpose', 'S3TablesQuery');
  }
}

export interface S3TablesIAMPolicyProps {
  /**
   * Input S3 bucket for reading articles
   */
  readonly inputBucket: s3.IBucket;

  /**
   * Input bucket prefix for articles
   * @default ''
   */
  readonly inputPrefix?: string;

  /**
   * Prompt S3 bucket for reading prompt templates
   */
  readonly promptBucket: s3.IBucket;

  /**
   * Prompt template keys to grant access to
   */
  readonly promptKeys: string[];

  /**
   * S3 Table Bucket ARN for write access
   */
  readonly tableBucketArn: string;

  /**
   * Bedrock model ARNs to allow invocation
   */
  readonly bedrockModelArns: string[];

  /**
   * AWS account ID
   */
  readonly accountId: string;

  /**
   * AWS region
   */
  readonly region: string;

  /**
   * Policy name
   */
  readonly policyName: string;
}

/**
 * Construct for creating IAM policies with least privilege access
 * for S3 Tables operations, S3 read/write, and Bedrock invocation.
 */
export class S3TablesIAMPolicyConstruct extends Construct {
  public readonly policy: iam.ManagedPolicy;

  constructor(scope: Construct, id: string, props: S3TablesIAMPolicyProps) {
    super(scope, id);

    const inputPrefix = props.inputPrefix || '';

    this.policy = new iam.ManagedPolicy(this, 'Policy', {
      managedPolicyName: props.policyName,
      description: 'Least privilege policy for S3 Tables sentiment analysis operations',
      statements: [
        // S3 read access for input articles
        new iam.PolicyStatement({
          sid: 'ReadInputArticles',
          effect: iam.Effect.ALLOW,
          actions: ['s3:GetObject', 's3:ListBucket'],
          resources: [
            props.inputBucket.bucketArn,
            inputPrefix
              ? `${props.inputBucket.bucketArn}/${inputPrefix}/*`
              : `${props.inputBucket.bucketArn}/*`,
          ],
        }),

        // S3 read access for prompt templates
        new iam.PolicyStatement({
          sid: 'ReadPromptTemplates',
          effect: iam.Effect.ALLOW,
          actions: ['s3:GetObject'],
          resources: props.promptKeys.map(
            (key) => `${props.promptBucket.bucketArn}/${key}`
          ),
        }),

        // S3 Tables access - full permissions for REST catalog operations
        new iam.PolicyStatement({
          sid: 'S3TablesAccess',
          effect: iam.Effect.ALLOW,
          actions: [
            // Bucket operations (required for REST catalog)
            's3tables:GetTableBucket',
            's3tables:ListNamespaces',
            's3tables:GetNamespace',
            's3tables:CreateNamespace',
            's3tables:ListTables',
            // Table operations
            's3tables:GetTable',
            's3tables:CreateTable',
            's3tables:GetTableMetadataLocation',
            's3tables:UpdateTableMetadataLocation',
            's3tables:GetTableData',
            's3tables:PutTableData',
            's3tables:UpdateTableMetadata',
          ],
          resources: [
            props.tableBucketArn,
            `${props.tableBucketArn}/*`,
          ],
        }),

        // S3 access for table bucket (required for Iceberg operations)
        new iam.PolicyStatement({
          sid: 'S3TableBucketAccess',
          effect: iam.Effect.ALLOW,
          actions: [
            's3:PutObject',
            's3:GetObject',
            's3:DeleteObject',
            's3:ListBucket',
          ],
          resources: [
            // Extract bucket name from ARN and construct S3 bucket ARN
            // S3 Tables bucket ARN format: arn:aws:s3tables:region:account:bucket/bucket-name
            // We need to convert to S3 bucket ARN: arn:aws:s3:::bucket-name
            `arn:aws:s3:::${props.tableBucketArn.split('/').pop()}`,
            `arn:aws:s3:::${props.tableBucketArn.split('/').pop()}/*`,
          ],
        }),

        // Bedrock invoke access for specific models
        new iam.PolicyStatement({
          sid: 'InvokeBedrockModels',
          effect: iam.Effect.ALLOW,
          actions: ['bedrock:InvokeModel'],
          resources: props.bedrockModelArns,
        }),

        // Bedrock inference profiles (cross-region inference)
        new iam.PolicyStatement({
          sid: 'InvokeBedrockInferenceProfiles',
          effect: iam.Effect.ALLOW,
          actions: ['bedrock:InvokeModel'],
          resources: [`arn:aws:bedrock:*:${props.accountId}:inference-profile/*`],
        }),
      ],
    });
  }
}
