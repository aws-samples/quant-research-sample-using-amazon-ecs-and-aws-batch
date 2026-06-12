/**
 * Maps Components-topology node ids → their deployed CloudFormation stack(s).
 * Kept separate from the React component so it can be imported without
 * tripping react-refresh's "only export components" rule.
 */

/**
 * The deployed namespace. The stack base names are stable; the namespace
 * suffix is appended to derive each stack name. The deployed app is "agentpoc".
 */
export const STACK_NS = "agentpoc";

/** Map a component node id → its deployed CloudFormation stack name(s). */
export function stacksForNode(id: string): string[] {
  switch (id) {
    case "vpc":
      return [`network-stack-${STACK_NS}`];
    case "s3":
      return [`s3-storage-stack-${STACK_NS}`];
    case "ecr":
      return [`deployment-pipeline-stack-${STACK_NS}`];
    case "batch":
      return [
        `batch-job-single-node-with-cpu-stack-${STACK_NS}`,
        `batch-job-multi-node-with-gpu-stack-${STACK_NS}`,
      ];
    case "fsx":
      return [`fsx-storage-stack-${STACK_NS}`];
    default:
      return [];
  }
}

/**
 * Nodes that expose an info/drill-in affordance on the canvas:
 *  - `cfn` → the full stack browser
 *  - `codebuild` → all builds this session (navigate to each in the console)
 *  - `github` → the source repo
 *  - any node backed by a deployed stack → its resources
 */
export function isResourceNode(id: string): boolean {
  return id === "cfn" || id === "codebuild" || id === "github" || stacksForNode(id).length > 0;
}

/**
 * Reverse map: a deployed stack name → the topology node id it lights up.
 * Tolerant of the namespace suffix and the single-/multi-node batch variants so
 * a returning user's actual CloudFormation stacks map onto the diagram.
 */
export function nodeForStack(stackName: string): string | undefined {
  const n = stackName.toLowerCase();
  if (n.startsWith("network-stack")) return "vpc";
  if (n.startsWith("s3-storage-stack")) return "s3";
  if (n.startsWith("deployment-pipeline-stack")) return "ecr";
  if (n.startsWith("batch-job")) return "batch";
  if (n.startsWith("fsx-storage-stack")) return "fsx";
  return undefined;
}
