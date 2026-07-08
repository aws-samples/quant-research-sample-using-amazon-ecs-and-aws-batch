import type { AgentTurn, PhaseName, PhaseState, ToolCall } from "@/lib/client/types";
import { MockClient } from "@/lib/client/mockClient";
import { LiveClient } from "@/lib/client/liveClient";
import { PHASES } from "@/lib/phases";
import { useDeploymentStore } from "@/store/useDeploymentStore";

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

// One client instance per mode, kept across turns so mock state persists.
const mock = new MockClient();
const live = new LiveClient();

function client() {
  return useDeploymentStore.getState().mode === "live" ? live : mock;
}

/** The active client for the current mode (mock/live). Used by views that
 *  need to read deployed-resource data directly (e.g. the Components panel). */
export function activeClient() {
  return client();
}

/**
 * Load a build onto the canvas from the chat "recent builds" block. If it's a
 * session build (in buildHistory, has a snapshot), restore that snapshot. If
 * it's an external build (real CodeBuild build from a prior session), set up a
 * fresh canvas from its summary and poll live status if it's still in progress.
 */
export function loadBuildOntoCanvas(opts: {
  buildId: string;
  inHistory: boolean;
  status?: string;
  startedAt?: number;
  finishedAt?: number;
}) {
  const store = useDeploymentStore.getState();
  if (opts.inHistory) {
    store.loadBuild(opts.buildId);
    return;
  }
  const status =
    opts.status === "SUCCEEDED" ? "SUCCEEDED" : opts.status === "FAILED" ? "FAILED" : "IN_PROGRESS";
  store.loadExternalBuild(opts.buildId, status, opts.startedAt, opts.finishedAt);
  if (useDeploymentStore.getState().mode !== "live") return;
  // For a still-running external build, animate it live via the same poller (which
  // also tails real logs). For a finished build there's nothing to poll, so fetch
  // its complete CodeBuild log output once and drop it onto the Logs tab.
  if (status === "IN_PROGRESS") {
    void pollBuild(opts.buildId);
  } else {
    void loadFinishedLogs(opts.buildId);
  }
}

/**
 * Fetch a finished build's entire CloudWatch log stream and replace the canvas
 * logs with it. Pages until GetLogEvents stops advancing the forward token
 * (documented end-of-stream behavior). Caps pages so a huge build can't spin.
 */
async function loadFinishedLogs(buildId: string) {
  const store = () => useDeploymentStore.getState();
  try {
    const all: string[] = [];
    let token: string | undefined;
    for (let page = 0; page < 50; page++) {
      const res = await client().getLogs(buildId, token);
      all.push(...res.lines);
      // Same token back ⇒ end of stream. Also stop on an empty, unchanged page.
      if (!res.nextToken || res.nextToken === token) break;
      token = res.nextToken;
    }
    // The operator may have loaded a different build while we paged — don't
    // clobber the canvas if it's moved on.
    if (store().build.buildId !== buildId) return;
    if (all.length === 0) {
      store().appendLogs(["(no log output available for this build)"]);
      return;
    }
    // Replace the "Loaded build … fetching" placeholder with the real output.
    useDeploymentStore.setState({ logs: all });
  } catch (err) {
    store().appendLogs([`Failed to load logs: ${(err as Error).message}`]);
  }
}

/** Send a user turn and drive the resulting animations through the store. */
export async function sendUserTurn(text: string) {
  const s = useDeploymentStore.getState();
  s.addUserMessage(text);

  const agentId = s.startAgentMessage();
  let turn: AgentTurn;
  try {
    turn = await client().sendMessage(text, (t) =>
      useDeploymentStore.getState().appendToken(agentId, t),
    );
  } catch (err) {
    useDeploymentStore
      .getState()
      .appendToken(agentId, `\n\n_Bridge error: ${(err as Error).message}_`);
    useDeploymentStore.getState().finishAgentMessage(agentId, { text: "", toolCalls: [] });
    return;
  }

  // Register tool-call chips.
  for (const tc of turn.toolCalls) useDeploymentStore.getState().upsertToolCall(tc);

  // Apply the proposed config BEFORE finishing the message so any inline diagram
  // block snapshots the freshly-rebuilt (proposed) topology, not the prior one.
  if (turn.proposedConfig) {
    useDeploymentStore.getState().proposeConfig(turn.proposedConfig, Boolean(turn.willFail));
    // "validated" badge animates in shortly after the config cascades.
    setTimeout(() => useDeploymentStore.getState().setValidated(true), 900);
  }

  useDeploymentStore.getState().finishAgentMessage(agentId, turn);

  if (turn.ignite && turn.buildId) {
    useDeploymentStore.getState().igniteBuild(turn.buildId);
    if (useDeploymentStore.getState().mode === "live") {
      void pollBuild(turn.buildId);
    } else {
      void runBuild(turn.buildId, Boolean(turn.willFail));
    }
  }
}

/** Called when the operator clicks Deploy on the inline config form / confirm card. */
export async function confirmDeploy(willFail: boolean) {
  const state = useDeploymentStore.getState();
  if (state.mode === "live") {
    // Send the exact (possibly edited) config so the agent's start_deployment
    // deploys precisely what's on screen — the inline form is authoritative.
    const c = state.config;
    const override = {
      availability_zone: c.availability_zone,
      app_with_codepipeline: c.app_with_codepipeline,
      app_with_fsx: c.app_with_fsx,
      app_with_s3express: c.app_with_s3express,
      batch: c.batch,
      ...(c.app_with_fsx ? { fsx: c.fsx } : {}),
      s3: c.s3,
    };
    const prompt =
      "Yes — confirm and deploy now. Use exactly this configuration override and call " +
      `start_deployment with it (do not ask again): ${JSON.stringify(override)}`;
    await sendUserTurn(prompt);
    return;
  }
  const prompt = willFail ? "Yes, confirm and deploy (fail)" : "Yes, confirm and deploy";
  await sendUserTurn(prompt);
}

// Per-phase dwell times (ms) tuned to *feel* like a real CodeBuild run —
// time-compressed but with believable latency (a real run is minutes). Each is
// jittered ±15% so repeated demos don't look scripted. BUILD is handled
// separately (per-stack). Happy path lands around ~22-26s total.
const PHASE_MS: Partial<Record<PhaseName, number>> = {
  SUBMITTED: 800,
  QUEUED: 1600,
  PROVISIONING: 2800, // spinning up the build container
  DOWNLOAD_SOURCE: 1800, // git clone
  INSTALL: 3000, // pip/uv + node deps
  PRE_BUILD: 2400, // cdk synth
  POST_BUILD: 1600, // collect outputs
};

/** A phase delay with ±15% jitter so the cadence feels organic, not scripted. */
function phaseDelay(name: PhaseName): number {
  const base = PHASE_MS[name] ?? 1500;
  return Math.round(base * (0.85 + Math.random() * 0.3));
}

/**
 * Time-compressed phase progression with live logs, stack sub-steps, and
 * component lighting. ~22-26s on the happy path; fails at BUILD on the fail path.
 */
async function runBuild(buildId: string, willFail: boolean) {
  const store = () => useDeploymentStore.getState();
  const log = (lines: string[]) => store().appendLogs(lines);

  for (const phase of PHASES) {
    if (phase.name === "COMPLETED") break;
    store().setPhase(phase.name, "IN_PROGRESS");
    log([`[${phase.name}] ${phase.label}…`]);

    // Component lighting tied to early phases.
    if (phase.name === "PROVISIONING") store().setComponent("codebuild", "live", "Build environment ready");
    if (phase.name === "DOWNLOAD_SOURCE") {
      store().setComponent("github", "live", "Cloned public repo");
      log(["Cloning https://github.com/aws-samples/quant-research-sample.git", "Source download complete."]);
    }
    if (phase.name === "INSTALL") log(["Installing aws-cdk-lib 2.199.0, dependencies via uv…", "node_modules ready."]);
    if (phase.name === "PRE_BUILD") log(["Merging override → cdk.context.json", "Synthesizing CloudFormation templates…"]);

    if (phase.name === "BUILD") {
      store().setComponent("cfn", "live", "Deploying stacks");
      await runBuildPhase(buildId, willFail);
      if (willFail) return; // runBuildPhase handled the failure
    } else {
      await sleep(phaseDelay(phase.name));
    }

    store().setPhase(phase.name, "SUCCEEDED");
  }

  // COMPLETED + success moment.
  store().setPhase("COMPLETED", "IN_PROGRESS");
  store().setComponent("ecr", "live", "Image pushed");
  log(["[POST_BUILD] Stack outputs collected.", "Deployment SUCCEEDED ✓"]);
  await sleep(700);
  store().setPhase("COMPLETED", "SUCCEEDED");
  store().finishBuild("SUCCEEDED");
}

/** The BUILD phase: nested CDK stack sub-steps + matching component nodes.
 *  Iterates the config-derived planned stacks (build.stacks), so only the
 *  stacks this config actually deploys are created — and only their topology
 *  nodes light up. */
async function runBuildPhase(_buildId: string, willFail: boolean) {
  const store = () => useDeploymentStore.getState();
  const log = (lines: string[]) => store().appendLogs(lines);

  // Each CloudFormation stack takes a believable couple of seconds to create,
  // jittered so the cadence isn't obviously scripted. A short "in progress"
  // dwell precedes the resource-create log so the timeline doesn't snap.
  const stackMs = () => Math.round((2400 + Math.random() * 1200)); // ~2.4-3.6s
  const steps = store().build.stacks;
  for (let i = 0; i < steps.length; i++) {
    const step = steps[i];
    const node = step.node;
    store().setStack(step.key, "creating");
    if (node) store().setComponent(node, "creating");
    log([`cdk deploy → ${step.label}`, `  ${step.name}: CREATE_IN_PROGRESS`]);

    // Fail the demo on the last (batch) stack of the fail path.
    const isLast = i === steps.length - 1;
    if (willFail && isLast) {
      await sleep(2200);
      log([
        `  ${step.name}: CREATE_FAILED`,
        "  ❌ Resource handler returned message: \"Insufficient capacity for instance type in us-east-1a\"",
        "  ROLLBACK_IN_PROGRESS",
        "[BUILD] Phase failed.",
      ]);
      store().setStack(step.key, "planned");
      if (node) store().setComponent(node, "planned");
      store().setPhase("BUILD", "FAILED");
      store().finishBuild("FAILED");
      return;
    }

    await sleep(stackMs());
    store().setStack(step.key, "live");
    if (node) store().setComponent(node, "live", "CREATE_COMPLETE");
    log([`  ${step.name}: CREATE_COMPLETE ✓`]);
  }
}

/**
 * LIVE-mode build progression. Polls the real bridge /api/status every ~3s,
 * translates CodeBuild phase statuses into store phase updates, lights topology
 * components as phases complete, and emits synthetic log lines on transitions.
 * Stops on a terminal build status or after a sane cap.
 */
async function pollBuild(buildId: string) {
  const store = () => useDeploymentStore.getState();
  const log = (lines: string[]) => store().appendLogs(lines);

  const POLL_MS = 3000;
  const MAX_MS = 12 * 60 * 1000; // 12 min cap
  const deadline = Date.now() + MAX_MS;

  // Track which phases / components we've already announced so logs stay terse.
  const seen: Partial<Record<PhaseName, PhaseState["status"]>> = {};
  const lit = new Set<string>();

  const lightComponent = (id: string, status: "creating" | "live", detail?: string) => {
    const key = `${id}:${status}`;
    if (lit.has(key)) return;
    lit.add(key);
    store().setComponent(id, status, detail);
  };

  // Phase → component lighting, mirroring the mock's mapping. Stacks + nodes
  // are config-derived (build.stacks), so only what this config deploys lights.
  const lightForPhase = (phase: PhaseName) => {
    const steps = store().build.stacks;
    switch (phase) {
      case "PROVISIONING":
        lightComponent("codebuild", "live", "Build environment ready");
        break;
      case "DOWNLOAD_SOURCE":
        lightComponent("github", "live", "Cloned public repo");
        break;
      case "BUILD":
        lightComponent("cfn", "live", "Deploying stacks");
        for (const st of steps) {
          store().setStack(st.key, "creating");
          if (st.node) lightComponent(st.node, "creating", "CREATE_IN_PROGRESS");
        }
        break;
      case "COMPLETED":
        for (const c of ["github", "codebuild", "cfn"]) lightComponent(c, "live");
        for (const st of steps) {
          store().setStack(st.key, "live");
          if (st.node) lightComponent(st.node, "live", "CREATE_COMPLETE");
        }
        break;
      default:
        break;
    }
  };

  log([`Tracking CodeBuild \`${buildId}\` — polling status…`]);

  // Tail the real CodeBuild log stream alongside status. The forward token
  // advances as new lines arrive; once it stops changing we're at the stream end.
  let logToken: string | undefined;
  const tailLogs = async () => {
    try {
      const page = await client().getLogs(buildId, logToken);
      if (page.lines.length) log(page.lines);
      logToken = page.nextToken ?? logToken;
    } catch {
      // Logs may not exist yet (pre-PROVISIONING) — ignore and retry next poll.
    }
  };

  for (;;) {
    let snap;
    try {
      snap = await client().getStatus(buildId);
    } catch (err) {
      log([`Status poll error: ${(err as Error).message} — retrying…`]);
      if (Date.now() > deadline) {
        store().finishBuild("FAILED");
        return;
      }
      await sleep(POLL_MS);
      continue;
    }

    // Pull any new real log lines for this iteration.
    await tailLogs();

    // Apply each phase status; emit a log line + lighting on transitions.
    for (const p of snap.phases) {
      if (seen[p.name] === p.status) continue;
      seen[p.name] = p.status;
      store().setPhase(p.name, p.status);
      if (p.status === "IN_PROGRESS") {
        log([`[${p.name}] in progress…`]);
      } else if (p.status === "SUCCEEDED") {
        log([`[${p.name}] succeeded ✓`]);
        lightForPhase(p.name);
      } else if (p.status === "FAILED") {
        log([`[${p.name}] FAILED ✗`]);
      }
    }

    // Ensure the reported current phase is at least marked in progress.
    if (snap.buildStatus === "IN_PROGRESS" && snap.currentPhase) {
      if (!seen[snap.currentPhase]) {
        seen[snap.currentPhase] = "IN_PROGRESS";
        store().setPhase(snap.currentPhase, "IN_PROGRESS");
        lightForPhase(snap.currentPhase);
      }
    }

    if (snap.buildStatus === "SUCCEEDED") {
      await tailLogs(); // flush any trailing lines written after the last poll
      store().setPhase("COMPLETED", "SUCCEEDED");
      lightForPhase("COMPLETED");
      log(["Deployment SUCCEEDED ✓"]);
      store().finishBuild("SUCCEEDED");
      return;
    }
    if (snap.buildStatus === "FAILED") {
      await tailLogs(); // flush the failing tail so the error is visible in Logs
      const cur = snap.currentPhase ?? "BUILD";
      store().setPhase(cur, "FAILED");
      log(["Deployment FAILED ✗"]);
      store().finishBuild("FAILED");
      return;
    }

    if (Date.now() > deadline) {
      log(["Polling cap reached — stopping status updates."]);
      store().finishBuild("FAILED");
      return;
    }
    await sleep(POLL_MS);
  }
}

/** Map a tool name to its chip glyph. */
export function toolGlyph(name: ToolCall["name"]): string {
  switch (name) {
    case "validate_config":
      return "✓";
    case "start_deployment":
      return "▶";
    case "get_deployment_status":
      return "◷";
  }
}
