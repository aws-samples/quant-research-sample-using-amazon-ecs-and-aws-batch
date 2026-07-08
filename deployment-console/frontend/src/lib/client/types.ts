// Data contract shared by Mock + Live clients.

/** CodeBuild phase identifiers, mirrored from the real agent. */
export type PhaseName =
  | "SUBMITTED"
  | "QUEUED"
  | "PROVISIONING"
  | "DOWNLOAD_SOURCE"
  | "INSTALL"
  | "PRE_BUILD"
  | "BUILD"
  | "POST_BUILD"
  | "COMPLETED";

export type PhaseStatus = "PENDING" | "IN_PROGRESS" | "SUCCEEDED" | "FAILED";

export type BuildStatus = "IN_PROGRESS" | "SUCCEEDED" | "FAILED";

export interface PhaseState {
  name: PhaseName;
  status: PhaseStatus;
  /** Seconds the phase took (filled as it completes). */
  durationSec?: number;
}

export interface BuildSnapshot {
  buildId: string;
  buildStatus: BuildStatus;
  currentPhase: PhaseName;
  phases: PhaseState[];
}

/** Tool call surfaced as a chat chip (validate_config, start_deployment, …). */
export interface ToolCall {
  id: string;
  name: "validate_config" | "start_deployment" | "get_deployment_status";
  input: Record<string, unknown>;
  output?: Record<string, unknown>;
  status: "running" | "ok" | "error";
}

/** Config override — mirrors schema/config.schema.json (partial, agent-relevant slice). */
export interface ConfigOverride {
  availability_zone: { name: string; id?: string };
  app_with_codepipeline: boolean;
  app_with_fsx: boolean;
  app_with_s3express: boolean;
  batch: { deployment_type: "SINGLE_NODE" | "MULTI_NODE" | "ALL" };
  fsx: { storage_capacity_gib: number; deployment_type: string };
  s3: { object_expiration_in_days: number };
}

/** Inline interactive block kinds an agent turn can request the chat to render. */
export type TurnBlock =
  | { kind: "diagram"; caption?: string; full?: boolean }
  | { kind: "configForm"; willFail?: boolean }
  | { kind: "confirm" }
  | { kind: "deployed" }
  | { kind: "builds" };

/** Result of one agent turn. */
export interface AgentTurn {
  text: string;
  toolCalls: ToolCall[];
  /** Discovered when start_deployment runs. */
  buildId?: string;
  /** A proposed config the UI should render in the Config view. */
  proposedConfig?: ConfigOverride;
  /** When true, the chat should render a confirm card before deploying. */
  awaitingConfirm?: boolean;
  /** Marks this turn as the one that kicks off a real build (after confirm). */
  ignite?: boolean;
  /** Force a failure run (demo "fail" intent). */
  willFail?: boolean;
  /** Interactive blocks to render after the message prose (diagram/form/etc). */
  blocks?: TurnBlock[];
}

/** A deployed CloudFormation stack (from GET /api/stacks). */
export interface StackInfo {
  name: string;
  status: string;
  updated?: string;
}

/** A single resource within a stack (from GET /api/resources?stack=…). */
export interface ResourceInfo {
  type: string;
  logicalId: string;
  physicalId: string;
  status: string;
}

/** A CodeBuild build summary (from GET /api/builds). */
export interface BuildSummary {
  buildId: string;
  buildNumber?: number;
  status: BuildStatus | string;
  currentPhase?: string;
  complete?: boolean;
  startedAt?: string;
  finishedAt?: string;
  durationSec?: number;
  phases?: Array<{ phase?: string; status?: string }>;
}

/** A page of a build's CloudWatch log stream. `nextToken` continues/tails it. */
export interface LogPage {
  lines: string[];
  nextToken?: string;
}

export interface DeploymentClient {
  sendMessage(text: string, onToken: (t: string) => void): Promise<AgentTurn>;
  getStatus(buildId: string): Promise<BuildSnapshot>;
  /** Fetch a page of the build's real CodeBuild log output. Pass the prior page's
   *  `nextToken` to tail only new lines. */
  getLogs(buildId: string, nextToken?: string): Promise<LogPage>;
  /** All deployed quant-research CloudFormation stacks. */
  listStacks(): Promise<StackInfo[]>;
  /** Resources for a single stack. */
  listResources(stack: string): Promise<ResourceInfo[]>;
  /** Recent CodeBuild builds for the deploy project (newest first). */
  listBuilds(limit?: number): Promise<BuildSummary[]>;
}
