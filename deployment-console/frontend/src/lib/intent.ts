import type { ConfigOverride } from "@/lib/client/types";
import { DEFAULT_CONFIG } from "@/lib/defaults";

/**
 * Free-text intent detection + the config a deploy intent maps to. Shared by the
 * mock client (which scripts the whole flow) and the live client (which only
 * gets prose back from the agent, so it derives the proposal's config/blocks
 * client-side to keep the UI identical to mock).
 */
export type Intent = "cpu" | "gpu" | "full" | "status" | "deployed" | "explain" | "fail" | "unknown";

export function classify(text: string): Intent {
  const t = text.toLowerCase();
  if (t.includes("fail")) return "fail";

  // "What's already deployed?" — a returning user querying CloudFormation.
  if (
    /\b(deployed|already|exists?|existing|live|provisioned|running)\b/.test(t) &&
    /\b(what|which|show|list|already|currently|status of|tell me)\b/.test(t)
  )
    return "deployed";

  // "Explain / show me the architecture" — answer with a diagram, no deploy.
  if (
    /\b(architecture|diagram|explain|how does|what does|overview|components?|stacks?)\b/.test(t) &&
    !/\b(deploy|provision|launch|spin up|create|build)\b/.test(t)
  )
    return "explain";

  if (t.includes("status") || t.includes("going") || t.includes("how's") || t.includes("hows"))
    return "status";

  // CPU / single-node signals win first — and negated FSx ("no fsx") must NOT
  // be read as a full-platform request.
  const negatedFsx = /\bno\s+(fsx|lustre)\b/.test(t);
  if (t.includes("cpu") || t.includes("single")) return "cpu";
  if (t.includes("gpu") || t.includes("multi") || t.includes("training")) return "gpu";

  // Full platform only when FSx/platform is genuinely requested (not negated).
  if (
    t.includes("full") ||
    t.includes("platform") ||
    ((t.includes("fsx") || t.includes("lustre")) && !negatedFsx)
  )
    return "full";

  if (t.includes("deploy") || t.includes("batch")) return "cpu";
  return "unknown";
}

/** The config a deploy-shaped intent proposes. */
export function configFor(intent: Intent): ConfigOverride {
  switch (intent) {
    case "gpu":
      return { ...DEFAULT_CONFIG, batch: { deployment_type: "MULTI_NODE" } };
    case "full":
    case "fail":
      return {
        ...DEFAULT_CONFIG,
        app_with_codepipeline: true,
        app_with_fsx: true,
        batch: { deployment_type: "ALL" },
      };
    default:
      return { ...DEFAULT_CONFIG };
  }
}

/** Intents that represent a deploy proposal (vs. status/explain/query). */
export function isDeployIntent(intent: Intent): boolean {
  return intent === "cpu" || intent === "gpu" || intent === "full" || intent === "fail";
}
