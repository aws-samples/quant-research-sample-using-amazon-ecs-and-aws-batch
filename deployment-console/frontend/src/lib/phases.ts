import type { PhaseName } from "@/lib/client/types";

/** Ordered CodeBuild phases with display labels and relative weights (for the comet/ETA). */
export interface PhaseDef {
  name: PhaseName;
  label: string;
  /** Relative duration weight — BUILD dominates (cdk deploy). */
  weight: number;
}

export const PHASES: PhaseDef[] = [
  { name: "SUBMITTED", label: "Submitted", weight: 1 },
  { name: "QUEUED", label: "Queued", weight: 1.5 },
  { name: "PROVISIONING", label: "Provisioning", weight: 3 },
  { name: "DOWNLOAD_SOURCE", label: "Download Source", weight: 2 },
  { name: "INSTALL", label: "Install", weight: 3 },
  { name: "PRE_BUILD", label: "Pre-build", weight: 2 },
  { name: "BUILD", label: "Build · cdk deploy", weight: 12 },
  { name: "POST_BUILD", label: "Post-build", weight: 2 },
  { name: "COMPLETED", label: "Completed", weight: 0.5 },
];

export const PHASE_LABEL: Record<PhaseName, string> = Object.fromEntries(
  PHASES.map((p) => [p.name, p.label]),
) as Record<PhaseName, string>;

// Nested CDK stack sub-steps under BUILD are now derived per-config from
// `plannedStacks()` in lib/defaults — see BuildState.stacks in the store.
