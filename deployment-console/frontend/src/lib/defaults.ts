import type { ConfigOverride } from "@/lib/client/types";

/** Canonical default config the agent deep-merges overrides onto. */
export const DEFAULT_CONFIG: ConfigOverride = {
  availability_zone: { name: "us-east-1a" },
  app_with_codepipeline: false,
  app_with_fsx: false,
  app_with_s3express: false,
  batch: { deployment_type: "SINGLE_NODE" },
  fsx: { storage_capacity_gib: 1200, deployment_type: "SCRATCH_2" },
  s3: { object_expiration_in_days: 30 },
};

/**
 * One CloudFormation stack the config will deploy. `key` is a stable id for the
 * timeline sub-step map; `name` is the (base) CFN stack name shown to the user;
 * `node` is the topology node it lights up (omitted for nodeless stacks).
 *
 * This is the single source of truth for "what deploys" — the stack count, the
 * timeline sub-steps, the build animation, and the validate_config output all
 * derive from it, so they can never drift apart.
 */
export interface PlannedStack {
  key: string;
  name: string;
  label: string;
  node?: string;
}

/** The ordered set of stacks a given config deploys. */
export function plannedStacks(c: ConfigOverride): PlannedStack[] {
  const steps: PlannedStack[] = [
    { key: "network", name: "network-stack", label: "Network stack (VPC)", node: "vpc" },
    { key: "s3", name: "s3-storage-stack", label: "S3 storage stack", node: "s3" },
  ];
  if (c.app_with_codepipeline)
    steps.push({ key: "pipeline", name: "deployment-pipeline-stack", label: "Pipeline stack (ECR)", node: "ecr" });
  if (c.app_with_s3express)
    steps.push({ key: "s3express", name: "s3-express-one-zone-stack", label: "S3 Express stack" });
  if (c.app_with_fsx)
    steps.push({ key: "fsx", name: "fsx-storage-stack", label: "FSx storage stack", node: "fsx" });
  if (c.batch.deployment_type === "ALL") {
    steps.push({ key: "batchCpu", name: "batch-job-single-node-with-cpu-stack", label: "Batch stack (single-node CPU)", node: "batch" });
    steps.push({ key: "batchGpu", name: "batch-job-multi-node-with-gpu-stack", label: "Batch stack (multi-node GPU)", node: "batch" });
  } else if (c.batch.deployment_type === "MULTI_NODE") {
    steps.push({ key: "batch", name: "batch-job-multi-node-with-gpu-stack", label: "Batch stack (multi-node GPU)", node: "batch" });
  } else {
    steps.push({ key: "batch", name: "batch-job-single-node-with-cpu-stack", label: "Batch stack (single-node CPU)", node: "batch" });
  }
  return steps;
}

/** Derive the number of CloudFormation stacks a config will deploy. */
export function stackCount(c: ConfigOverride): number {
  return plannedStacks(c).length;
}

/**
 * Topology node ids present for a config — the always-on pipeline (github →
 * codebuild → cfn), the always-deployed VPC + S3 + Batch, plus ECR (only with
 * CodePipeline) and FSx (only when enabled). Keeps the diagram showing exactly
 * what the config deploys, so the build animation never lights an absent node.
 */
export function componentIdsFor(c: ConfigOverride): string[] {
  const ids = ["github", "codebuild", "cfn", "vpc", "s3"];
  if (c.app_with_codepipeline) ids.push("ecr");
  ids.push("batch");
  if (c.app_with_fsx) ids.push("fsx");
  return ids;
}
