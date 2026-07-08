/**
 * Shared architecture topology — the single source of truth for the build
 * graph's node positions, directed edges, and labels. Imported by both the
 * canvas Components panel and the inline chat architecture diagrams so the two
 * always agree on the same layout and wiring.
 *
 * Positions are percentages within a viewBox-ish 100x100 grid (x,y).
 */

export interface TopoNode {
  id: string;
  label: string;
  /** Short caption shown under glyphs in dense (inline) diagrams. */
  detail: string;
  x: number;
  y: number;
}

/** Canonical node layout — github → codebuild → cfn → {vpc,s3,ecr,batch} → fsx. */
export const TOPO_NODES: TopoNode[] = [
  { id: "github", label: "GitHub", detail: "Public source repo", x: 12, y: 18 },
  { id: "codebuild", label: "CodeBuild", detail: "cdk deploy runner", x: 12, y: 52 },
  { id: "cfn", label: "CloudFormation", detail: "Stack orchestration", x: 40, y: 52 },
  { id: "vpc", label: "VPC", detail: "Network stack", x: 74, y: 14 },
  { id: "s3", label: "S3", detail: "Artifact bucket", x: 74, y: 36 },
  { id: "ecr", label: "ECR", detail: "Container registry", x: 74, y: 58 },
  { id: "batch", label: "AWS Batch", detail: "Compute environment", x: 74, y: 80 },
  { id: "fsx", label: "FSx", detail: "Lustre scratch", x: 92, y: 80 },
];

/** Directed edges (source id → target id). */
export const TOPO_EDGES: [string, string][] = [
  ["github", "codebuild"],
  ["codebuild", "cfn"],
  ["cfn", "vpc"],
  ["cfn", "s3"],
  ["cfn", "ecr"],
  ["cfn", "batch"],
  ["batch", "fsx"],
];

const NODE_BY_ID: Record<string, TopoNode> = Object.fromEntries(
  TOPO_NODES.map((n) => [n.id, n]),
);

export function topoNode(id: string): TopoNode | undefined {
  return NODE_BY_ID[id];
}

/** Position lookup (back-compat with the old POS map shape). */
export const TOPO_POS: Record<string, { x: number; y: number }> = Object.fromEntries(
  TOPO_NODES.map((n) => [n.id, { x: n.x, y: n.y }]),
);
