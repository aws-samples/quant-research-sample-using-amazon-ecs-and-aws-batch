import { create } from "zustand";
import type {
  AgentTurn,
  BuildStatus,
  ConfigOverride,
  PhaseName,
  PhaseState,
  ToolCall,
} from "@/lib/client/types";
import { PHASES } from "@/lib/phases";
import { DEFAULT_CONFIG, componentIdsFor, plannedStacks } from "@/lib/defaults";
import type { PlannedStack } from "@/lib/defaults";
import { TOPO_NODES } from "@/lib/topology";

export type TabKey = "deploy" | "logs";
export type Mode = "mock" | "live";

export type ComponentStatus = "planned" | "creating" | "live";

export interface ComponentNode {
  id: string;
  label: string;
  status: ComponentStatus;
  detail?: string;
}

/**
 * Rich inline chat blocks rendered after a message's prose. They let the agent
 * answer with *interactive* UI — an architecture diagram to explain a solution,
 * an editable config form to gather options, a confirm/cancel button pair, or a
 * live "what's deployed" panel pulled from CloudFormation.
 */
export type MessageBlock =
  | {
      kind: "diagram";
      /** Snapshot of the topology nodes to draw (with per-node status). */
      nodes: ComponentNode[];
      caption?: string;
    }
  | {
      kind: "configForm";
      /** Marks the proposal as the demo fail-path (passed through to deploy). */
      willFail?: boolean;
    }
  | { kind: "confirm" }
  | { kind: "deployed" }
  /** Recent CodeBuild builds with an inline "Load" button per build. */
  | { kind: "builds" };

export interface ChatMessage {
  id: string;
  role: "user" | "agent" | "system";
  text: string;
  /** Streaming flag while tokens arrive. */
  streaming?: boolean;
  toolCallIds?: string[];
  /** Interactive blocks rendered after the message text. */
  blocks?: MessageBlock[];
}

/** Build-graph node ids (topology). */
export const COMPONENT_IDS = TOPO_NODES.map((n) => n.id);

/** Per-batch-type caption for the Batch node. */
function batchDetail(c: ConfigOverride): string {
  return c.batch.deployment_type === "ALL"
    ? "CPU + GPU compute envs"
    : c.batch.deployment_type === "MULTI_NODE"
      ? "Multi-node GPU CE"
      : "Single-node CPU CE";
}

/**
 * The topology nodes a config deploys — derived from `componentIdsFor` so the
 * diagram (and everything the build animation lights) shows exactly the stacks
 * this config will create. No phantom nodes for disabled options.
 */
export function componentsFor(c: ConfigOverride): ComponentNode[] {
  const ids = new Set(componentIdsFor(c));
  return TOPO_NODES.filter((n) => ids.has(n.id)).map((n) => ({
    id: n.id,
    label: n.label,
    status: "planned" as ComponentStatus,
    detail: n.id === "batch" ? batchDetail(c) : n.detail,
  }));
}

/** Config-derived build sub-steps (status starts planned). */
function freshStacks(c: ConfigOverride): BuildStackStep[] {
  return plannedStacks(c).map((s) => ({ ...s, status: "planned" as ComponentStatus }));
}

function freshPhases(): PhaseState[] {
  return PHASES.map((p) => ({ name: p.name, status: "PENDING" as const }));
}

/** A build sub-step = a planned stack plus its live deploy status. */
export interface BuildStackStep extends PlannedStack {
  status: ComponentStatus;
}

export interface BuildState {
  buildId?: string;
  status: BuildStatus | "IDLE";
  currentPhase?: PhaseName;
  phases: PhaseState[];
  startedAt?: number;
  finishedAt?: number;
  /** Nested CDK stack sub-steps (shown under BUILD) — derived from the config. */
  stacks: BuildStackStep[];
}

/**
 * A finished-or-running build, kept in history for the CodeBuild node panel and
 * the chat "recent builds" block. Carries enough of a snapshot (config + final
 * timeline/components/logs) to *reload* the build onto the canvas later.
 */
export interface BuildRecord {
  buildId: string;
  status: BuildStatus | "IDLE";
  startedAt: number;
  finishedAt?: number;
  /** Number of stacks the build deployed (for a quick summary). */
  stackCount: number;
  /** Config used for this build — what the inline summary/diagram restore from. */
  config: ConfigOverride;
  /** Captured at completion so a reload restores the exact end-state canvas. */
  snapshot?: {
    phases: PhaseState[];
    stacks: BuildStackStep[];
    components: ComponentNode[];
    logs: string[];
  };
}

interface DeploymentStore {
  mode: Mode;
  setMode: (m: Mode) => void;

  messages: ChatMessage[];
  toolCalls: Record<string, ToolCall>;
  config: ConfigOverride;
  validated: boolean;
  /** True when the pending proposal is the demo fail-path. */
  pendingWillFail: boolean;
  currentTab: TabKey;
  build: BuildState;
  /** All builds this session, newest first (CodeBuild node "all builds" panel). */
  buildHistory: BuildRecord[];
  logs: string[];
  components: ComponentNode[];
  /** Fired once on success so the confetti burst plays a single time. */
  celebrated: boolean;

  setTab: (t: TabKey) => void;

  addUserMessage: (text: string) => void;
  addSystemMessage: (text: string) => void;
  startAgentMessage: () => string;
  appendToken: (id: string, t: string) => void;
  finishAgentMessage: (id: string, turn: AgentTurn) => void;

  upsertToolCall: (tc: ToolCall) => void;

  proposeConfig: (c: ConfigOverride, willFail: boolean) => void;
  /** Patch the working config from an inline form edit (re-derives topology). */
  patchConfig: (patch: Partial<ConfigOverride>) => void;
  setValidated: (v: boolean) => void;

  // Deployment lifecycle ------------------------------------------------
  igniteBuild: (buildId: string) => void;
  setPhase: (phase: PhaseName, status: PhaseState["status"]) => void;
  setStack: (key: string, status: ComponentStatus) => void;
  setComponent: (id: string, status: ComponentStatus, detail?: string) => void;
  appendLogs: (lines: string[]) => void;
  finishBuild: (status: BuildStatus) => void;
  celebrate: () => void;

  /** Reload a past build's snapshot onto the canvas (timeline/arch/logs/config). */
  loadBuild: (buildId: string) => void;
  /** Load a build that isn't in session history (e.g. a real CodeBuild build from
   *  a prior session) — sets up a fresh canvas the driver then polls live. */
  loadExternalBuild: (buildId: string, status: BuildStatus | "IDLE", startedAt?: number, finishedAt?: number) => void;

  reset: () => void;
}

let seq = 0;
const nextId = (p: string) => `${p}-${Date.now().toString(36)}-${seq++}`;

const WELCOME =
  "Hi — I'm your deployment agent. Describe the infrastructure you want and I'll validate it, then deploy on your confirmation. Try a preset below to see the full flow.";

export const useDeploymentStore = create<DeploymentStore>((set) => ({
  // Default to Live when built with VITE_DEFAULT_MODE=live (the deployed SPA);
  // local dev stays Mock so the offline demo works without a backend.
  mode: (import.meta.env.VITE_DEFAULT_MODE as Mode) === "live" ? "live" : "mock",
  setMode: (m) => set({ mode: m }),

  messages: [{ id: "welcome", role: "agent", text: WELCOME }],
  toolCalls: {},
  config: DEFAULT_CONFIG,
  validated: false,
  pendingWillFail: false,
  currentTab: "deploy",
  build: { status: "IDLE", phases: freshPhases(), stacks: freshStacks(DEFAULT_CONFIG) },
  buildHistory: [],
  logs: [],
  components: componentsFor(DEFAULT_CONFIG),
  celebrated: false,

  setTab: (t) => set({ currentTab: t }),

  addUserMessage: (text) =>
    set((s) => ({ messages: [...s.messages, { id: nextId("u"), role: "user", text }] })),
  addSystemMessage: (text) =>
    set((s) => ({ messages: [...s.messages, { id: nextId("sys"), role: "system", text }] })),

  startAgentMessage: () => {
    const id = nextId("a");
    set((s) => ({
      messages: [...s.messages, { id, role: "agent", text: "", streaming: true }],
    }));
    return id;
  },
  appendToken: (id, t) =>
    set((s) => ({
      messages: s.messages.map((m) => (m.id === id ? { ...m, text: m.text + t } : m)),
    })),
  finishAgentMessage: (id, turn) =>
    set((s) => {
      // Translate the turn's requested blocks into concrete message blocks,
      // snapshotting the (already-rebuilt) component topology for diagrams so
      // each diagram freezes the state it was rendered with.
      const blocks: MessageBlock[] = (turn.blocks ?? []).map((b) => {
        switch (b.kind) {
          case "diagram":
            return {
              kind: "diagram",
              // `full` → the whole reference architecture (all nodes, planned);
              // otherwise snapshot the current (proposed) component set.
              nodes: b.full
                ? TOPO_NODES.map((n) => ({ id: n.id, label: n.label, detail: n.detail, status: "planned" as ComponentStatus }))
                : s.components.map((c) => ({ ...c })),
              caption: b.caption,
            };
          case "configForm":
            return { kind: "configForm", willFail: b.willFail };
          case "confirm":
            return { kind: "confirm" };
          case "deployed":
            return { kind: "deployed" };
          case "builds":
            return { kind: "builds" };
        }
      });
      return {
        messages: s.messages.map((m) =>
          m.id === id
            ? {
                ...m,
                streaming: false,
                toolCallIds: turn.toolCalls.map((tc) => tc.id),
                blocks: blocks.length ? blocks : undefined,
              }
            : m,
        ),
      };
    }),

  upsertToolCall: (tc) => set((s) => ({ toolCalls: { ...s.toolCalls, [tc.id]: tc } })),

  proposeConfig: (c, willFail) =>
    set((s) => ({
      config: c,
      validated: false,
      pendingWillFail: willFail,
      components: rebuildComponents(s.components, c),
    })),
  patchConfig: (patch) =>
    set((s) => {
      const next = { ...s.config, ...patch } as ConfigOverride;
      return { config: next, components: rebuildComponents(s.components, next) };
    }),
  setValidated: (v) => set({ validated: v }),

  igniteBuild: (buildId) =>
    set((s) => {
      const startedAt = Date.now();
      const stackCount = s.build.stacks.length || freshStacks(s.config).length;
      const record: BuildRecord = {
        buildId,
        status: "IN_PROGRESS",
        startedAt,
        stackCount,
        config: { ...s.config },
      };
      return {
        currentTab: "deploy",
        build: {
          buildId,
          status: "IN_PROGRESS",
          currentPhase: "SUBMITTED",
          phases: freshPhases(),
          startedAt,
          // Sub-steps derive from the just-confirmed config.
          stacks: freshStacks(s.config),
        },
        // Prepend to history (newest first); dedupe by buildId on re-ignite.
        buildHistory: [record, ...s.buildHistory.filter((b) => b.buildId !== buildId)],
        logs: [],
        celebrated: false,
        // mark the pipeline-entry components as live/creating
        components: s.components.map((c) =>
          c.id === "github" || c.id === "codebuild" || c.id === "cfn"
            ? { ...c, status: "creating" }
            : c,
        ),
      };
    }),

  setPhase: (phase, status) =>
    set((s) => ({
      build: {
        ...s.build,
        currentPhase: phase,
        phases: s.build.phases.map((p) => (p.name === phase ? { ...p, status } : p)),
      },
    })),

  setStack: (key, status) =>
    set((s) => ({
      build: {
        ...s.build,
        stacks: s.build.stacks.map((st) => (st.key === key ? { ...st, status } : st)),
      },
    })),

  setComponent: (id, status, detail) =>
    set((s) => ({
      components: s.components.map((c) =>
        c.id === id ? { ...c, status, detail: detail ?? c.detail } : c,
      ),
    })),

  appendLogs: (lines) => set((s) => ({ logs: [...s.logs, ...lines] })),

  finishBuild: (status) =>
    set((s) => {
      const finishedAt = Date.now();
      // Snapshot the end-state canvas so this build can be reloaded later.
      const snapshot = {
        phases: s.build.phases.map((p) => ({ ...p })),
        stacks: s.build.stacks.map((st) => ({ ...st })),
        components: s.components.map((c) => ({ ...c })),
        logs: [...s.logs],
      };
      return {
        build: { ...s.build, status, finishedAt },
        buildHistory: s.buildHistory.map((b) =>
          b.buildId === s.build.buildId ? { ...b, status, finishedAt, snapshot } : b,
        ),
      };
    }),

  celebrate: () => set({ celebrated: true }),

  loadBuild: (buildId) =>
    set((s) => {
      const rec = s.buildHistory.find((b) => b.buildId === buildId);
      if (!rec) return {};
      const snap = rec.snapshot;
      return {
        currentTab: "deploy",
        config: { ...rec.config },
        validated: true,
        // Restore the build's timeline/stacks (snapshot if captured, else derive).
        build: {
          buildId: rec.buildId,
          status: rec.status,
          currentPhase: "COMPLETED",
          phases: snap ? snap.phases.map((p) => ({ ...p })) : freshPhases(),
          startedAt: rec.startedAt,
          finishedAt: rec.finishedAt,
          stacks: snap ? snap.stacks.map((st) => ({ ...st })) : freshStacks(rec.config),
        },
        components: snap ? snap.components.map((c) => ({ ...c })) : componentsFor(rec.config),
        logs: snap ? [...snap.logs] : [],
        // Don't replay confetti when merely reviewing a past build.
        celebrated: true,
      };
    }),

  loadExternalBuild: (buildId, status, startedAt, finishedAt) =>
    set((s) => {
      // We don't know the build's config, so show the full reference topology and
      // mark everything live/complete for a finished build (the driver's poll will
      // refine phases). For an in-progress build the driver animates it live.
      const live = status === "SUCCEEDED";
      const compStatus: ComponentStatus = live ? "live" : status === "IN_PROGRESS" ? "creating" : "planned";
      const phases = freshPhases().map((p) =>
        live ? { ...p, status: "SUCCEEDED" as const } : p,
      );
      const stacks = freshStacks(s.config).map((st) => ({
        ...st,
        status: compStatus,
      }));
      return {
        currentTab: "deploy",
        validated: true,
        build: {
          buildId,
          status,
          currentPhase: live ? "COMPLETED" : "BUILD",
          phases,
          startedAt: startedAt ?? Date.now(),
          finishedAt,
          stacks,
        },
        components: componentsFor(s.config).map((c) => ({ ...c, status: compStatus })),
        logs: [`Loaded build ${buildId} from CodeBuild — fetching live status…`],
        celebrated: true,
      };
    }),

  reset: () =>
    set({
      messages: [{ id: "welcome", role: "agent", text: WELCOME }],
      toolCalls: {},
      config: DEFAULT_CONFIG,
      validated: false,
      pendingWillFail: false,
      currentTab: "deploy",
      build: { status: "IDLE", phases: freshPhases(), stacks: freshStacks(DEFAULT_CONFIG) },
      buildHistory: [],
      logs: [],
      components: componentsFor(DEFAULT_CONFIG),
      celebrated: false,
    }),
}));

/**
 * Recompute the component set when config changes. Adds/removes optional nodes
 * (ECR, FSx) so the diagram matches the config exactly, preserving the live
 * status of nodes that persist across the change.
 */
function rebuildComponents(prev: ComponentNode[], c: ConfigOverride): ComponentNode[] {
  const prevById = new Map(prev.map((n) => [n.id, n]));
  return componentsFor(c).map((n) => {
    const existing = prevById.get(n.id);
    // Keep any in-flight/live status; always refresh the (config-derived) detail.
    return existing ? { ...n, status: existing.status } : n;
  });
}
