import type {
  AgentTurn,
  BuildSnapshot,
  BuildStatus,
  BuildSummary,
  LogPage,
  PhaseName,
  PhaseState,
  ResourceInfo,
  StackInfo,
  TurnBlock,
} from "@/lib/client/types";
import { classify, configFor, isDeployIntent } from "@/lib/intent";

/**
 * LiveClient — talks to the real backend bridge (same-origin under /api).
 * The bridge buffers the agent's reply into a single JSON response (NOT SSE) and
 * proxies CodeBuild status. Same DeploymentClient interface as MockClient.
 *
 * Endpoints:
 *   POST /api/message  body {prompt, sessionId?} → {ok, result, sessionId}
 *   GET  /api/status?buildId=  → {ok, buildId, buildStatus, currentPhase, complete, phases:[{phase,status}]}
 *   GET  /api/logs?buildId=&nextToken=  → {ok, lines:[], nextToken}  (real CodeBuild log tail)
 *
 * The agent runs its own tools server-side (including start_deployment, which
 * kicks off a CodeBuild build before the response returns). When a build starts,
 * its id appears in the `result` text — we detect it and set ignite/buildId.
 */

/** Matches a CodeBuild id (project:uuid) or the synthetic dc-build-xx id. */
const BUILD_ID_RE = /(agentpoc-deploy-console-cdk-deploy:[0-9a-f-]+|dc-build-[a-z0-9]+)/i;

/** Word-cadence streaming so the chat still types out the buffered reply. */
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * Hex SHA-256 of a string. Required as the `x-amz-content-sha256` header on POSTs
 * to the OAC-signed Lambda Function URL behind CloudFront — Lambda OAC rejects
 * unsigned payloads, so the browser must supply the body hash itself.
 */
async function sha256Hex(body: string): Promise<string> {
  const bytes = new TextEncoder().encode(body);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/** Map a raw CodeBuild phase string onto the PhaseName enum. */
function toPhaseName(raw: string): PhaseName | undefined {
  const p = (raw || "").toUpperCase();
  switch (p) {
    case "SUBMITTED":
    case "QUEUED":
    case "PROVISIONING":
    case "DOWNLOAD_SOURCE":
    case "INSTALL":
    case "PRE_BUILD":
    case "BUILD":
    case "POST_BUILD":
    case "COMPLETED":
      return p as PhaseName;
    // Phases not in the enum are merged into the nearest meaningful one.
    case "FINALIZING":
    case "UPLOAD_ARTIFACTS":
      return "POST_BUILD";
    case "FINALIZING_FAILED":
      return "POST_BUILD";
    default:
      return undefined;
  }
}

function toPhaseStatus(raw: string): PhaseState["status"] {
  const s = (raw || "").toUpperCase();
  if (s === "SUCCEEDED") return "SUCCEEDED";
  if (s === "FAILED" || s === "FAULT" || s === "TIMED_OUT" || s === "STOPPED") return "FAILED";
  if (s === "IN_PROGRESS") return "IN_PROGRESS";
  return "PENDING";
}

function toBuildStatus(raw: string): BuildStatus {
  const s = (raw || "").toUpperCase();
  if (s === "SUCCEEDED") return "SUCCEEDED";
  if (s === "FAILED" || s === "FAULT" || s === "TIMED_OUT" || s === "STOPPED") return "FAILED";
  return "IN_PROGRESS";
}

interface MessageResponse {
  ok?: boolean;
  result?: string;
  sessionId?: string;
  error?: string;
}

interface StatusResponse {
  ok?: boolean;
  buildId?: string;
  buildStatus?: string;
  currentPhase?: string;
  complete?: boolean;
  phases?: Array<{ phase?: string; status?: string }>;
  error?: string;
}

export class LiveClient {
  /** Conversation memory: persisted across turns so the agent keeps context. */
  private sessionId?: string;

  async sendMessage(text: string, onToken: (t: string) => void): Promise<AgentTurn> {
    const body = JSON.stringify({ prompt: text, sessionId: this.sessionId });
    const res = await fetch("/api/message", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        // Required by the OAC-signed Lambda origin (no unsigned payloads).
        "x-amz-content-sha256": await sha256Hex(body),
      },
      body,
    });
    if (!res.ok) throw new Error(`bridge /api/message failed: ${res.status}`);

    const data = (await res.json()) as MessageResponse;
    if (data.ok === false) {
      throw new Error(data.error || "bridge /api/message returned ok:false");
    }
    const result = data.result ?? "";
    if (data.sessionId) this.sessionId = data.sessionId;

    // Simulate token cadence over the words of the buffered reply.
    await this.stream(onToken, result);

    const match = result.match(BUILD_ID_RE);
    const buildId = match ? match[0] : undefined;

    // A build already started (the agent ran start_deployment) → ignite, no blocks.
    if (buildId) {
      return { text: result, toolCalls: [], buildId, ignite: true };
    }

    // A confirmation message (sent by the inline Deploy button) must NOT re-open
    // a proposal form — we're past that. If no build id came back, the agent
    // likely needs another nudge, but we don't re-prompt the form.
    const isConfirmation = /\bconfirm and deploy\b/i.test(text) || /start_deployment/i.test(text);

    // The real agent returns prose only. To match mock's interactivity, when the
    // user asked to deploy and the agent is asking for confirmation (no build yet),
    // attach the proposed config + an inline diagram + an editable options form
    // with Deploy/Cancel. The form's Deploy sends the (possibly edited) config to
    // the agent, so the form is authoritative over what gets deployed.
    const intent = classify(text);
    if (!isConfirmation && isDeployIntent(intent)) {
      const proposedConfig = configFor(intent);
      return {
        text: result,
        toolCalls: [],
        proposedConfig,
        awaitingConfirm: true,
        willFail: intent === "fail",
        blocks: [
          { kind: "diagram", caption: "Review and tune the options below, then Deploy to start the real CodeBuild run." },
          { kind: "configForm", willFail: intent === "fail" },
        ],
      };
    }

    // Otherwise augment diagram/status-shaped asks (these read real data).
    return { text: result, toolCalls: [], blocks: blocksForPrompt(text) };
  }

  async getStatus(buildId: string): Promise<BuildSnapshot> {
    const res = await fetch(`/api/status?buildId=${encodeURIComponent(buildId)}`);
    if (!res.ok) throw new Error(`bridge /api/status failed: ${res.status}`);
    const data = (await res.json()) as StatusResponse;
    if (data.ok === false) {
      throw new Error(data.error || "bridge /api/status returned ok:false");
    }

    const buildStatus = toBuildStatus(data.buildStatus ?? "IN_PROGRESS");

    // Collapse raw phases into the enum, keeping the strongest status per phase
    // (e.g. FINALIZING + UPLOAD_ARTIFACTS both fold into POST_BUILD).
    const collected = new Map<PhaseName, PhaseState["status"]>();
    for (const p of data.phases ?? []) {
      const name = toPhaseName(p.phase ?? "");
      if (!name) continue;
      const status = toPhaseStatus(p.status ?? "");
      const prev = collected.get(name);
      collected.set(name, mergeStatus(prev, status));
    }
    const phases: PhaseState[] = [...collected.entries()].map(([name, status]) => ({
      name,
      status,
    }));

    const currentPhase = toPhaseName(data.currentPhase ?? "") ?? "BUILD";

    return { buildId: data.buildId ?? buildId, buildStatus, currentPhase, phases };
  }

  async getLogs(buildId: string, nextToken?: string): Promise<LogPage> {
    const qs = new URLSearchParams({ buildId });
    if (nextToken) qs.set("nextToken", nextToken);
    const res = await fetch(`/api/logs?${qs.toString()}`);
    if (!res.ok) throw new Error(`bridge /api/logs failed: ${res.status}`);
    const data = (await res.json()) as {
      ok?: boolean;
      lines?: string[];
      nextToken?: string;
      error?: string;
    };
    if (data.ok === false) throw new Error(data.error || "bridge /api/logs returned ok:false");
    return { lines: data.lines ?? [], nextToken: data.nextToken };
  }

  async listStacks(): Promise<StackInfo[]> {
    const res = await fetch("/api/stacks");
    if (!res.ok) throw new Error(`bridge /api/stacks failed: ${res.status}`);
    const data = (await res.json()) as {
      ok?: boolean;
      stacks?: StackInfo[];
      error?: string;
    };
    if (data.ok === false) throw new Error(data.error || "bridge /api/stacks returned ok:false");
    return data.stacks ?? [];
  }

  async listResources(stack: string): Promise<ResourceInfo[]> {
    const res = await fetch(`/api/resources?stack=${encodeURIComponent(stack)}`);
    if (!res.ok) throw new Error(`bridge /api/resources failed: ${res.status}`);
    const data = (await res.json()) as {
      ok?: boolean;
      resources?: ResourceInfo[];
      error?: string;
    };
    if (data.ok === false) throw new Error(data.error || "bridge /api/resources returned ok:false");
    return data.resources ?? [];
  }

  async listBuilds(limit = 8): Promise<BuildSummary[]> {
    const res = await fetch(`/api/builds?limit=${encodeURIComponent(String(limit))}`);
    if (!res.ok) throw new Error(`bridge /api/builds failed: ${res.status}`);
    const data = (await res.json()) as { ok?: boolean; builds?: BuildSummary[]; error?: string };
    if (data.ok === false) throw new Error(data.error || "bridge /api/builds returned ok:false");
    return data.builds ?? [];
  }

  /** Stream text token-by-token (word-chunked for a natural cadence). */
  private async stream(onToken: (t: string) => void, text: string) {
    if (!text) return;
    const tokens = text.match(/\S+\s*|\s+/g) ?? [text];
    for (const tok of tokens) {
      onToken(tok);
      await sleep(10 + Math.random() * 20);
    }
  }
}

/**
 * Decide which inline block (if any) to attach to a live agent reply, based on
 * the user's prompt. The blocks render real data (the deployed-status block
 * pulls CloudFormation via /api/stacks; the diagram reads the live topology).
 */
function blocksForPrompt(text: string): TurnBlock[] | undefined {
  const t = text.toLowerCase();
  const deployedAsk =
    /\b(deployed|already|existing|live|provisioned|running)\b/.test(t) &&
    /\b(what|which|show|list|currently|already|status of|tell me)\b/.test(t);
  // The RecentBuilds block self-hides when there's no history, so it's safe to
  // always attach it to a "what's deployed" answer.
  if (deployedAsk) return [{ kind: "deployed" }, { kind: "builds" }];

  const statusAsk = /\b(status|going|how'?s|progress|build)\b/.test(t) && !deployedAsk;
  if (statusAsk) return [{ kind: "builds" }];

  const explainAsk =
    /\b(architecture|diagram|explain|overview|components?|stacks?)\b/.test(t) &&
    !/\b(deploy|provision|launch|spin up)\b/.test(t);
  if (explainAsk) return [{ kind: "diagram", full: true, caption: "GitHub → CodeBuild → CloudFormation → VPC · S3 · ECR · Batch · FSx." }];

  return undefined;
}

/** Pick the "furthest along" status when a phase folds in from several raw phases. */
function mergeStatus(
  prev: PhaseState["status"] | undefined,
  next: PhaseState["status"],
): PhaseState["status"] {
  const rank: Record<PhaseState["status"], number> = {
    PENDING: 0,
    IN_PROGRESS: 1,
    SUCCEEDED: 2,
    FAILED: 3,
  };
  if (prev === undefined) return next;
  // FAILED always wins; otherwise take the higher rank.
  if (prev === "FAILED" || next === "FAILED") return "FAILED";
  return rank[next] >= rank[prev] ? next : prev;
}
