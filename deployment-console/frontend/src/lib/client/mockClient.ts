import type {
  AgentTurn,
  BuildSnapshot,
  BuildSummary,
  ConfigOverride,
  LogPage,
  PhaseName,
  ResourceInfo,
  StackInfo,
  ToolCall,
} from "@/lib/client/types";
import { DEFAULT_CONFIG, plannedStacks, stackCount } from "@/lib/defaults";
import { STACK_NS } from "@/lib/resourceStacks";
import { classify, configFor } from "@/lib/intent";
import { useDeploymentStore } from "@/store/useDeploymentStore";

/** How many builds have run this session (drives "recent builds" prose). */
function buildCount(): number {
  return useDeploymentStore.getState().buildHistory.length;
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
let counter = 0;
const id = (p: string) => `${p}-${(counter++).toString(36)}-${Date.now().toString(36)}`;

/**
 * MockClient — scripted, time-compressed replay of the real agent flow.
 * It streams agent prose, emits tool-call chips, and returns an AgentTurn the
 * UI driver turns into Config / Timeline / Components / Logs animations.
 */
export class MockClient {
  /** Whether a build is currently "live" so a status query can answer. */
  private hasPendingBuild = false;
  private lastBuildId?: string;
  private lastConfig: ConfigOverride = DEFAULT_CONFIG;
  /**
   * Stacks deployed this session, accumulated across confirmed deploys so
   * "what's deployed" reflects what the operator actually launched (incl. any
   * components added in a later deploy). Stays undefined until the first deploy,
   * when listStacks() falls back to the offline fixture.
   */
  private deployedStacks?: Map<string, StackInfo>;
  /** Set true once the operator confirms a proposed deploy. */
  awaitingConfirm = false;

  async sendMessage(text: string, onToken: (t: string) => void): Promise<AgentTurn> {
    const intent = classify(text);

    // ---- Confirmation reply: user said "yes/confirm/deploy" while gated ----
    if (this.awaitingConfirm && /\b(yes|confirm|go|deploy|do it|proceed)\b/i.test(text)) {
      this.awaitingConfirm = false;
      const willFail = /fail/i.test(text);
      return this.confirmTurn(onToken, willFail);
    }

    if (intent === "status") return this.statusTurn(onToken);
    if (intent === "deployed") return this.deployedTurn(onToken);
    if (intent === "explain") return this.explainTurn(onToken);

    // ---- Propose + validate a config, then gate on confirm ----
    const cfg = configFor(intent);
    this.lastConfig = cfg;
    const willFail = intent === "fail";

    const intro =
      intent === "gpu"
        ? "Adding a multi-node GPU training environment. Here's the proposed configuration."
        : intent === "full" || intent === "fail"
          ? "Configuring the full platform: CPU + GPU Batch, FSx for Lustre, and the CodePipeline CI/CD stack."
          : "I'll set up a single-node CPU Batch environment. Here's the proposed configuration.";

    await this.stream(onToken, intro + "\n\n");

    const validate: ToolCall = {
      id: id("tc"),
      name: "validate_config",
      input: { parameters: cfg },
      status: "running",
    };
    // The chip is emitted via the turn; the driver flips it to ok after a beat.
    await sleep(700);
    const planned = plannedStacks(cfg);
    validate.status = "ok";
    validate.output = {
      valid: true,
      stack_count: planned.length,
      // The actual CFN stacks this config will create, in deploy order.
      stacks: planned.map((p) => p.name),
      // Echo the validated config so the operator sees exactly what was checked.
      parameters: {
        availability_zone: cfg.availability_zone.name,
        batch_deployment_type: cfg.batch.deployment_type,
        app_with_codepipeline: cfg.app_with_codepipeline,
        app_with_fsx: cfg.app_with_fsx,
        app_with_s3express: cfg.app_with_s3express,
        s3_object_expiration_in_days: cfg.s3.object_expiration_in_days,
        ...(cfg.app_with_fsx
          ? { fsx_storage_capacity_gib: cfg.fsx.storage_capacity_gib, fsx_deployment_type: cfg.fsx.deployment_type }
          : {}),
      },
      warnings:
        cfg.fsx.deployment_type === "SCRATCH_2" && cfg.app_with_fsx
          ? ["SCRATCH_2 FSx is ephemeral — data is lost on filesystem deletion."]
          : [],
    };

    await this.stream(
      onToken,
      `Validation passed — **${stackCount(cfg)} stacks** will deploy. Here's the architecture and the ` +
        "options you can tune. Adjust anything inline, then hit **Deploy** to begin.",
    );

    this.awaitingConfirm = true;
    return {
      text: "",
      toolCalls: [validate],
      proposedConfig: cfg,
      awaitingConfirm: true,
      willFail,
      blocks: [
        { kind: "diagram", caption: "Pipeline clones the repo, CodeBuild runs cdk deploy, and CloudFormation provisions each stack." },
        { kind: "configForm", willFail },
      ],
    };
  }

  /** "Explain the architecture" — a diagram-led answer, no deploy gate. */
  private async explainTurn(onToken: (t: string) => void): Promise<AgentTurn> {
    await this.stream(
      onToken,
      "Here's how the solution fits together. Source lives in **GitHub**; a **CodeBuild** " +
        "project runs `cdk deploy`, which drives **CloudFormation** to provision the network " +
        "(**VPC**), an artifact **S3** bucket, the **ECR** registry, and the **AWS Batch** " +
        "compute environment. Ask me to deploy any variant and I'll propose a config.",
    );
    return {
      text: "",
      toolCalls: [],
      blocks: [
        {
          kind: "diagram",
          full: true,
          caption: "GitHub → CodeBuild (cdk deploy) → CloudFormation → VPC · S3 · ECR · Batch · FSx.",
        },
      ],
    };
  }

  /** "What's already deployed?" — pull CloudFormation + render a live diagram. */
  private async deployedTurn(onToken: (t: string) => void): Promise<AgentTurn> {
    const getStatus: ToolCall = {
      id: id("tc"),
      name: "get_deployment_status",
      input: { query: "list_stacks" },
      status: "running",
    };
    await sleep(500);
    const stacks = await this.listStacks();
    getStatus.status = "ok";
    getStatus.output = { stacks: stacks.length, source: "cloudformation:ListStacks" };

    const builds = buildCount();
    const buildsLine =
      builds > 0
        ? ` I've also run **${builds} build${builds === 1 ? "" : "s"}** this session — load any below to ` +
          "review its timeline, architecture, and logs on the canvas."
        : "";
    await this.stream(
      onToken,
      `I pulled the live stacks from CloudFormation — **${stacks.length} stacks** are deployed in ` +
        `this account. Here's the current architecture with each stack's status.${buildsLine}`,
    );
    const blocks: AgentTurn["blocks"] = [{ kind: "deployed" }];
    if (builds > 0) blocks.push({ kind: "builds" });
    return { text: "", toolCalls: [getStatus], blocks };
  }

  private async confirmTurn(
    onToken: (t: string) => void,
    willFail: boolean,
  ): Promise<AgentTurn> {
    await this.stream(onToken, "Confirmed. Starting the deployment now.\n\n");
    const buildId = `dc-build-${Math.random().toString(36).slice(2, 10)}`;
    this.lastBuildId = buildId;
    this.hasPendingBuild = true;

    // Record what this deploy creates so a later "what's deployed" reflects it.
    // Re-deploying an existing stack reads as UPDATE_COMPLETE; new ones CREATE.
    if (!willFail) this.recordDeploy(this.lastConfig);

    const start: ToolCall = {
      id: id("tc"),
      name: "start_deployment",
      input: { parameters: this.lastConfig, confirm: true },
      output: { buildId, project: "deployment-console-builder", status: "STARTED" },
      status: "ok",
    };

    await this.stream(
      onToken,
      `CodeBuild build \`${buildId}\` started. Switching to the Timeline — I'll report progress as the stacks deploy.`,
    );

    return { text: "", toolCalls: [start], buildId, ignite: true, willFail };
  }

  private async statusTurn(onToken: (t: string) => void): Promise<AgentTurn> {
    const getStatus: ToolCall = {
      id: id("tc"),
      name: "get_deployment_status",
      input: { buildId: this.lastBuildId ?? "(none)" },
      status: "ok",
    };
    const builds = buildCount();
    if (!this.hasPendingBuild || !this.lastBuildId) {
      getStatus.output = { status: "NO_ACTIVE_BUILD", recentBuilds: builds };
      await this.stream(
        onToken,
        builds > 0
          ? `No deployment is running right now. Here are the **${builds} build${builds === 1 ? "" : "s"}** ` +
              "from this session — load any to review it on the canvas."
          : "There's no active deployment right now. Pick a preset to start one.",
      );
      return { text: "", toolCalls: [getStatus], blocks: builds > 0 ? [{ kind: "builds" }] : [] };
    }
    getStatus.output = { buildId: this.lastBuildId, buildStatus: "IN_PROGRESS", currentPhase: "BUILD" };
    await this.stream(
      onToken,
      `Build \`${this.lastBuildId}\` is **IN_PROGRESS**, currently in the BUILD phase running \`cdk deploy\`. ` +
        "See the Timeline for live phase status." +
        (builds > 1 ? " Recent builds are listed below — load any to review it." : ""),
    );
    return { text: "", toolCalls: [getStatus], blocks: builds > 1 ? [{ kind: "builds" }] : [] };
  }

  // The UI driver advances phases itself (so it can animate); these methods
  // exist to satisfy the DeploymentClient interface / Live parity.
  async getStatus(buildId: string): Promise<BuildSnapshot> {
    return {
      buildId,
      buildStatus: "IN_PROGRESS",
      currentPhase: "BUILD" as PhaseName,
      phases: [],
    };
  }

  async getLogs(): Promise<LogPage> {
    // Mock drives the Logs tab from the driver's synthetic lines, not a real stream.
    return { lines: [] };
  }

  async listBuilds(): Promise<BuildSummary[]> {
    // Mock has no external CodeBuild project; the recent-builds block reads the
    // in-memory session history (store.buildHistory) directly.
    return [];
  }

  /** Fold a confirmed deploy's stacks into the session's deployed set. */
  private recordDeploy(cfg: ConfigOverride) {
    const map = this.deployedStacks ?? new Map<string, StackInfo>();
    const stamp = new Date().toISOString().replace(/\.\d+Z$/, "Z");
    for (const p of plannedStacks(cfg)) {
      const name = `${p.name}-${STACK_NS}`;
      const existed = map.has(name);
      map.set(name, {
        name,
        status: existed ? "UPDATE_COMPLETE" : "CREATE_COMPLETE",
        updated: stamp,
      });
    }
    this.deployedStacks = map;
  }

  async listStacks(): Promise<StackInfo[]> {
    await sleep(450);
    // Reflect what was actually deployed this session; fall back to the offline
    // fixture only before the first deploy.
    return this.deployedStacks ? [...this.deployedStacks.values()] : MOCK_STACKS;
  }

  async listResources(stack: string): Promise<ResourceInfo[]> {
    await sleep(450);
    return MOCK_RESOURCES[stack] ?? [];
  }

  /** Stream text token-by-token (word-chunked for a natural cadence). */
  private async stream(onToken: (t: string) => void, text: string) {
    const tokens = text.match(/\S+\s*|\s+/g) ?? [text];
    for (const tok of tokens) {
      onToken(tok);
      await sleep(14 + Math.random() * 26);
    }
  }
}

// --- Offline demo fixtures for the "show real deployed resources" feature ---
// Believable stand-ins so the Components panel works without a live bridge.

const MOCK_STACKS: StackInfo[] = [
  { name: "network-stack-agentpoc", status: "CREATE_COMPLETE", updated: "2026-06-08T14:02:11Z" },
  { name: "s3-storage-stack-agentpoc", status: "CREATE_COMPLETE", updated: "2026-06-08T14:03:48Z" },
  {
    name: "deployment-pipeline-stack-agentpoc",
    status: "UPDATE_COMPLETE",
    updated: "2026-06-08T14:05:30Z",
  },
  {
    name: "batch-job-single-node-with-cpu-stack-agentpoc",
    status: "CREATE_COMPLETE",
    updated: "2026-06-08T14:09:12Z",
  },
];

const MOCK_RESOURCES: Record<string, ResourceInfo[]> = {
  "network-stack-agentpoc": [
    { type: "AWS::EC2::VPC", logicalId: "QuantVpc", physicalId: "vpc-0a1b2c3d4e5f60718", status: "CREATE_COMPLETE" },
    { type: "AWS::EC2::Subnet", logicalId: "PrivateSubnet1", physicalId: "subnet-0aa11bb22cc33dd44", status: "CREATE_COMPLETE" },
    { type: "AWS::EC2::Subnet", logicalId: "PrivateSubnet2", physicalId: "subnet-0ee55ff66gg77hh88", status: "CREATE_COMPLETE" },
    { type: "AWS::EC2::SecurityGroup", logicalId: "BatchSg", physicalId: "sg-0c1d2e3f4a5b6c7d8", status: "CREATE_COMPLETE" },
    { type: "AWS::EC2::VPCEndpoint", logicalId: "S3Endpoint", physicalId: "vpce-05a0b1c2d3e4f5061", status: "CREATE_COMPLETE" },
    { type: "AWS::EC2::VPCEndpoint", logicalId: "EcrApiEndpoint", physicalId: "vpce-0712a3b4c5d6e7f80", status: "CREATE_COMPLETE" },
    { type: "AWS::EC2::VPCEndpoint", logicalId: "EcrDkrEndpoint", physicalId: "vpce-09a8b7c6d5e4f3021", status: "CREATE_COMPLETE" },
  ],
  "s3-storage-stack-agentpoc": [
    { type: "AWS::S3::Bucket", logicalId: "ArtifactBucket", physicalId: "agentpoc-artifacts-0a1b2c3d", status: "CREATE_COMPLETE" },
    { type: "AWS::S3::BucketPolicy", logicalId: "ArtifactBucketPolicy", physicalId: "agentpoc-artifacts-0a1b2c3d", status: "CREATE_COMPLETE" },
  ],
  "deployment-pipeline-stack-agentpoc": [
    { type: "AWS::ECR::Repository", logicalId: "QuantRepo", physicalId: "agentpoc/quant-research", status: "CREATE_COMPLETE" },
    { type: "AWS::CodeBuild::Project", logicalId: "DeployProject", physicalId: "agentpoc-deploy-console-cdk-deploy", status: "CREATE_COMPLETE" },
    { type: "AWS::IAM::Role", logicalId: "CodeBuildRole", physicalId: "agentpoc-codebuild-role", status: "CREATE_COMPLETE" },
  ],
  "batch-job-single-node-with-cpu-stack-agentpoc": [
    { type: "AWS::Batch::ComputeEnvironment", logicalId: "CpuComputeEnv", physicalId: "agentpoc-cpu-ce", status: "CREATE_COMPLETE" },
    { type: "AWS::Batch::JobQueue", logicalId: "CpuJobQueue", physicalId: "agentpoc-cpu-queue", status: "CREATE_COMPLETE" },
    { type: "AWS::Batch::JobDefinition", logicalId: "CpuJobDef", physicalId: "agentpoc-cpu-jobdef:3", status: "CREATE_COMPLETE" },
    { type: "AWS::IAM::Role", logicalId: "BatchExecutionRole", physicalId: "agentpoc-batch-exec-role", status: "CREATE_COMPLETE" },
  ],
  "batch-job-multi-node-with-gpu-stack-agentpoc": [
    { type: "AWS::Batch::ComputeEnvironment", logicalId: "GpuComputeEnv", physicalId: "agentpoc-gpu-ce", status: "CREATE_COMPLETE" },
    { type: "AWS::Batch::JobQueue", logicalId: "GpuJobQueue", physicalId: "agentpoc-gpu-queue", status: "CREATE_COMPLETE" },
    { type: "AWS::Batch::JobDefinition", logicalId: "GpuMultiNodeJobDef", physicalId: "agentpoc-gpu-mnp-jobdef:2", status: "CREATE_COMPLETE" },
    { type: "AWS::IAM::Role", logicalId: "GpuExecutionRole", physicalId: "agentpoc-gpu-exec-role", status: "CREATE_COMPLETE" },
  ],
  "fsx-storage-stack-agentpoc": [
    { type: "AWS::FSx::FileSystem", logicalId: "LustreScratch", physicalId: "fs-0a1b2c3d4e5f60718", status: "CREATE_COMPLETE" },
    { type: "AWS::EC2::SecurityGroup", logicalId: "FsxSg", physicalId: "sg-0fsx1a2b3c4d5e6f7", status: "CREATE_COMPLETE" },
  ],
  "s3-express-one-zone-stack-agentpoc": [
    { type: "AWS::S3Express::DirectoryBucket", logicalId: "ExpressBucket", physicalId: "agentpoc-express--use1-az4--x-s3", status: "CREATE_COMPLETE" },
  ],
};
