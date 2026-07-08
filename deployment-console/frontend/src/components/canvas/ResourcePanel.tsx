import { useEffect, useState } from "react";
import { motion, useReducedMotion } from "motion/react";
import { ArrowLeft, Check, Copy, ExternalLink, GitBranch, Hammer, Loader2, X } from "lucide-react";
import type { ResourceInfo, StackInfo } from "@/lib/client/types";
import { activeClient } from "@/lib/driver";
import { useDeploymentStore } from "@/store/useDeploymentStore";
import { stacksForNode } from "@/lib/resourceStacks";
import { cloudFormationUrl, codeBuildUrl, regionFromAz, SOURCE_REPO_URL } from "@/lib/awsConsole";
import { cn } from "@/lib/cn";

// --- Per-stack caches so reopening a panel is instant (no refetch). ---
const resourceCache = new Map<string, ResourceInfo[]>();
let stacksCache: StackInfo[] | undefined;

async function fetchResources(stacks: string[]): Promise<ResourceInfo[]> {
  const lists = await Promise.all(
    stacks.map(async (s) => {
      const hit = resourceCache.get(s);
      if (hit) return hit;
      const rows = await activeClient()
        .listResources(s)
        .catch(() => [] as ResourceInfo[]);
      resourceCache.set(s, rows);
      return rows;
    }),
  );
  return lists.flat();
}

async function fetchStacks(): Promise<StackInfo[]> {
  if (stacksCache) return stacksCache;
  stacksCache = await activeClient().listStacks();
  return stacksCache;
}

/** Header eyebrow text per node kind (demo suffix in mock mode). */
function panelEyebrow(nodeId: string, mode: string): string {
  const demo = mode === "live" ? "" : " · demo";
  if (nodeId === "codebuild") return `Build history${demo}`;
  if (nodeId === "github") return "Source repository";
  return `Deployed resources${demo}`;
}

type Tone = "ok" | "progress" | "error" | "neutral";

function statusTone(status: string): Tone {
  const s = status.toUpperCase();
  if (s.includes("FAIL") || s.includes("ROLLBACK") || s.includes("DELETE")) return "error";
  if (s.includes("PROGRESS")) return "progress";
  if (s.includes("COMPLETE") || s.includes("SUCCEEDED")) return "ok";
  return "neutral";
}

const TONE_DOT: Record<Tone, string> = {
  ok: "bg-aws-green",
  progress: "bg-aws-orange",
  error: "bg-aws-red",
  neutral: "bg-text-lo",
};
const TONE_TEXT: Record<Tone, string> = {
  ok: "text-aws-green",
  progress: "text-aws-orange",
  error: "text-aws-red",
  neutral: "text-text-mid",
};

/** A status dot with the status text revealed on hover (keeps rows calm). */
function StatusDot({ status }: { status: string }) {
  const tone = statusTone(status);
  return (
    <span className="group/st relative flex shrink-0 items-center" title={status}>
      {tone === "progress" ? (
        <span className="relative flex h-2 w-2">
          <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-aws-orange opacity-60" />
          <span className="relative inline-flex h-2 w-2 rounded-full bg-aws-orange" />
        </span>
      ) : (
        <span className={cn("h-2 w-2 rounded-full", TONE_DOT[tone])} />
      )}
    </span>
  );
}

/** Middle-truncate a long id so both the prefix and the meaningful tail show. */
function midTruncate(s: string, head = 16, tail = 12): string {
  if (s.length <= head + tail + 1) return s;
  return `${s.slice(0, head)}…${s.slice(-tail)}`;
}

function CopyButton({ value }: { value: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      aria-label="Copy id"
      title="Copy id"
      onClick={(e) => {
        e.stopPropagation();
        void navigator.clipboard?.writeText(value).then(() => {
          setCopied(true);
          setTimeout(() => setCopied(false), 1200);
        });
      }}
      className="shrink-0 rounded p-1 text-text-lo opacity-0 transition group-hover/row:opacity-100 hover:bg-white/10 hover:text-text-hi"
    >
      {copied ? <Check className="h-3 w-3 text-aws-green" /> : <Copy className="h-3 w-3" />}
    </button>
  );
}

function ResourceRow({ r }: { r: ResourceInfo }) {
  // Short type label, e.g. AWS::EC2::VPCEndpoint → VPCEndpoint.
  const shortType = r.type.split("::").slice(-1)[0] || r.type;
  const tone = statusTone(r.status);
  return (
    <li className="group/row flex items-center gap-2.5 rounded-lg border border-white/5 bg-surface-2/40 px-2.5 py-2 transition-colors hover:border-white/12 hover:bg-surface-2/70">
      <StatusDot status={r.status} />
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-1.5">
          <span className="truncate text-[11.5px] font-medium text-text-hi" title={r.type}>
            {shortType}
          </span>
          <span className="shrink-0 rounded bg-white/5 px-1 py-px font-mono text-[9px] text-text-lo" title={r.logicalId}>
            {r.logicalId}
          </span>
        </div>
        <div
          className="truncate font-mono text-[10px] text-text-mid"
          title={`${r.physicalId}\n${r.status}`}
        >
          {midTruncate(r.physicalId)}
        </div>
      </div>
      <span className={cn("shrink-0 text-[9px] font-medium uppercase tracking-wide", TONE_TEXT[tone])}>
        {tone === "ok" ? "Live" : tone === "progress" ? "Creating" : tone === "error" ? "Failed" : "—"}
      </span>
      <CopyButton value={r.physicalId} />
    </li>
  );
}

/** Resource list with loading / error / empty states. */
function ResourceList({
  stacks,
  emptyLabel,
}: {
  stacks: string[];
  emptyLabel: string;
}) {
  // Seed instantly from cache if every stack is already known.
  const cached = stacks.every((s) => resourceCache.has(s))
    ? stacks.flatMap((s) => resourceCache.get(s) ?? [])
    : undefined;
  const [state, setState] = useState<{
    loading: boolean;
    error?: string;
    rows: ResourceInfo[];
  }>({ loading: cached === undefined, rows: cached ?? [] });

  // Stable dependency: the array identity changes each render, the key doesn't.
  const stacksKey = stacks.join("|");
  useEffect(() => {
    // Already seeded from cache in initial state — nothing to fetch.
    if (stacks.every((s) => resourceCache.has(s))) return;
    let alive = true;
    fetchResources(stacks)
      .then((rows) => {
        if (alive) setState({ loading: false, rows });
      })
      .catch((err: unknown) => {
        if (alive) setState({ loading: false, error: (err as Error).message, rows: [] });
      });
    return () => {
      alive = false;
    };
    // stacksKey captures the (stable) contents of `stacks`.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [stacksKey]);

  if (state.loading) {
    return (
      <div className="flex items-center gap-2 px-1 py-6 text-[11px] text-text-lo">
        <Loader2 className="h-3.5 w-3.5 animate-spin" />
        Loading resources…
      </div>
    );
  }
  if (state.error) {
    return (
      <div className="px-1 py-6 text-[11px] text-aws-red">Could not load resources.</div>
    );
  }
  if (state.rows.length === 0) {
    return <div className="px-1 py-4 text-[11px] text-text-lo">{emptyLabel}</div>;
  }
  return (
    <ul className="flex flex-col gap-1.5">
      {state.rows.map((r) => (
        <ResourceRow key={`${r.logicalId}-${r.physicalId}`} r={r} />
      ))}
    </ul>
  );
}

/** A single stack's header (name + CloudFormation deep-link) over its resources. */
function StackSection({ stack, region }: { stack: string; region: string }) {
  return (
    <div className="mb-2.5 last:mb-0">
      <a
        href={cloudFormationUrl(region, stack)}
        target="_blank"
        rel="noreferrer"
        title="Open this stack in the CloudFormation console"
        className="group/sh mb-1.5 flex items-center gap-1.5 rounded-md px-0.5 py-0.5 text-[10px] text-text-mid transition-colors hover:text-aws-blue"
      >
        <span className="truncate font-mono" title={stack}>
          {stack}
        </span>
        <ExternalLink className="h-3 w-3 shrink-0 text-text-lo group-hover/sh:text-aws-blue" />
      </a>
      <ResourceList stacks={[stack]} emptyLabel="No resources / not deployed yet." />
    </div>
  );
}

/** Per-node view: label + each backing stack (linked) with its resources grouped. */
function NodeResources({ nodeId, label }: { nodeId: string; label: string }) {
  const stacks = stacksForNode(nodeId);
  const region = useDeploymentStore((s) => regionFromAz(s.config.availability_zone.name));
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="mb-2.5 shrink-0 text-[13px] font-semibold text-text-hi">{label}</div>
      <div className="min-h-0 flex-1 overflow-y-auto pr-1">
        {stacks.length === 0 ? (
          <div className="px-1 py-6 text-[11px] text-text-lo">No resources / not deployed yet.</div>
        ) : (
          stacks.map((s) => <StackSection key={s} stack={s} region={region} />)
        )}
      </div>
    </div>
  );
}

/** cfn view: full stack list, drilling into a selected stack's resources. */
function StackBrowser() {
  const region = useDeploymentStore((s) => regionFromAz(s.config.availability_zone.name));
  const [state, setState] = useState<{
    loading: boolean;
    error?: string;
    stacks: StackInfo[];
  }>(
    stacksCache
      ? { loading: false, stacks: stacksCache }
      : { loading: true, stacks: [] },
  );
  const [selected, setSelected] = useState<StackInfo | null>(null);

  useEffect(() => {
    if (stacksCache) return;
    let alive = true;
    fetchStacks()
      .then((stacks) => {
        if (alive) setState({ loading: false, stacks });
      })
      .catch((err: unknown) => {
        if (alive) setState({ loading: false, error: (err as Error).message, stacks: [] });
      });
    return () => {
      alive = false;
    };
  }, []);

  if (selected) {
    return (
      <div className="flex min-h-0 flex-1 flex-col">
        <button
          type="button"
          onClick={() => setSelected(null)}
          className="mb-2 flex shrink-0 items-center gap-1 text-[11px] text-text-mid hover:text-text-hi"
        >
          <ArrowLeft className="h-3 w-3" /> All stacks
        </button>
        <a
          href={cloudFormationUrl(region, selected.name)}
          target="_blank"
          rel="noreferrer"
          title="Open this stack in the CloudFormation console"
          className="group/sh mb-2 flex shrink-0 items-center gap-1.5 text-[10px] text-text-mid transition-colors hover:text-aws-blue"
        >
          <span className="truncate font-mono" title={selected.name}>
            {selected.name}
          </span>
          <ExternalLink className="h-3 w-3 shrink-0 text-text-lo group-hover/sh:text-aws-blue" />
        </a>
        <div className="min-h-0 flex-1 overflow-y-auto pr-1">
          <ResourceList stacks={[selected.name]} emptyLabel="No resources reported." />
        </div>
      </div>
    );
  }

  if (state.loading) {
    return (
      <div className="flex items-center gap-2 px-1 py-6 text-[11px] text-text-lo">
        <Loader2 className="h-3.5 w-3.5 animate-spin" />
        Loading stacks…
      </div>
    );
  }
  if (state.error) {
    return <div className="px-1 py-6 text-[11px] text-aws-red">Could not load stacks.</div>;
  }
  if (state.stacks.length === 0) {
    return <div className="px-1 py-6 text-[11px] text-text-lo">No deployed stacks found.</div>;
  }

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="mb-2 shrink-0 text-[13px] font-semibold text-text-hi">
        Deployed stacks
        <span className="ml-1 text-[11px] font-normal text-text-lo">({state.stacks.length})</span>
      </div>
      <ul className="min-h-0 flex-1 space-y-1.5 overflow-y-auto pr-1">
        {state.stacks.map((s) => (
          <li key={s.name}>
            <button
              type="button"
              onClick={() => setSelected(s)}
              className="group/row flex w-full items-center gap-2.5 rounded-lg border border-white/5 bg-surface-2/40 px-2.5 py-2 text-left transition-colors hover:border-white/15 hover:bg-surface-2/70"
            >
              <StatusDot status={s.status} />
              <span className="min-w-0 flex-1 truncate font-mono text-[10.5px] text-text-hi" title={s.name}>
                {s.name}
              </span>
              <ArrowLeft className="h-3 w-3 shrink-0 rotate-180 text-text-lo transition-colors group-hover/row:text-text-mid" />
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}

function fmtDuration(start: number, end?: number): string {
  const sec = Math.max(0, Math.round(((end ?? Date.now()) - start) / 1000));
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return `${m}:${s.toString().padStart(2, "0")}`;
}

/** codebuild view: every build this session, each linking to the CodeBuild console. */
function BuildsBrowser() {
  const region = useDeploymentStore((s) => regionFromAz(s.config.availability_zone.name));
  const history = useDeploymentStore((s) => s.buildHistory);

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="mb-2 shrink-0 text-[13px] font-semibold text-text-hi">
        CodeBuild
        <span className="ml-1 text-[11px] font-normal text-text-lo">
          ({history.length} build{history.length === 1 ? "" : "s"})
        </span>
      </div>
      {history.length === 0 ? (
        <div className="px-1 py-6 text-[11px] text-text-lo">
          No builds yet — confirm a deployment to run one.
        </div>
      ) : (
        <ul className="min-h-0 flex-1 space-y-1.5 overflow-y-auto pr-1">
          {history.map((b) => {
            const tone = b.status === "SUCCEEDED" ? "ok" : b.status === "FAILED" ? "error" : "progress";
            return (
              <li key={b.buildId}>
                <a
                  href={codeBuildUrl(region, b.buildId)}
                  target="_blank"
                  rel="noreferrer"
                  title="Open this build in the CodeBuild console"
                  className="group/row flex items-center gap-2.5 rounded-lg border border-white/5 bg-surface-2/40 px-2.5 py-2 transition-colors hover:border-aws-blue/40 hover:bg-surface-2/70"
                >
                  <StatusDot status={b.status} />
                  <div className="min-w-0 flex-1">
                    <div className="truncate font-mono text-[10.5px] text-text-hi" title={b.buildId}>
                      {b.buildId}
                    </div>
                    <div className="text-[10px] text-text-lo">
                      {b.stackCount} stack{b.stackCount === 1 ? "" : "s"} · {fmtDuration(b.startedAt, b.finishedAt)}
                      {b.status === "IN_PROGRESS" && " · running"}
                    </div>
                  </div>
                  <span className={cn("shrink-0 text-[9px] font-medium uppercase tracking-wide", TONE_TEXT[tone])}>
                    {b.status === "SUCCEEDED" ? "Done" : b.status === "FAILED" ? "Failed" : "Running"}
                  </span>
                  <ExternalLink className="h-3 w-3 shrink-0 text-text-lo group-hover/row:text-aws-blue" />
                </a>
              </li>
            );
          })}
        </ul>
      )}
      <a
        href={codeBuildUrl(region, undefined)}
        target="_blank"
        rel="noreferrer"
        className="mt-2 flex shrink-0 items-center justify-center gap-1.5 rounded-lg border border-white/10 py-1.5 text-[11px] text-text-mid transition-colors hover:border-aws-blue/40 hover:text-aws-blue"
      >
        Open CodeBuild project <ExternalLink className="h-3 w-3" />
      </a>
    </div>
  );
}

/** github view: the source repo CodeBuild clones, linking out to GitHub. */
function GithubInfo() {
  const branch = "main";
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="mb-2 shrink-0 text-[13px] font-semibold text-text-hi">Source repository</div>
      <a
        href={SOURCE_REPO_URL}
        target="_blank"
        rel="noreferrer"
        title="Open the source repository on GitHub"
        className="group/row flex items-start gap-2.5 rounded-lg border border-white/5 bg-surface-2/40 px-3 py-2.5 transition-colors hover:border-aws-blue/40 hover:bg-surface-2/70"
      >
        <GitBranch className="mt-0.5 h-4 w-4 shrink-0 text-aws-teal" />
        <div className="min-w-0 flex-1">
          <div className="truncate text-[11.5px] font-medium text-text-hi">
            aws-samples/quant-research-sample-using-amazon-ecs-and-aws-batch
          </div>
          <div className="mt-0.5 truncate font-mono text-[10px] text-text-lo">
            {SOURCE_REPO_URL.replace("https://", "")} · {branch}
          </div>
        </div>
        <ExternalLink className="mt-0.5 h-3.5 w-3.5 shrink-0 text-text-lo group-hover/row:text-aws-blue" />
      </a>
      <p className="mt-2 px-0.5 text-[10.5px] leading-relaxed text-text-mid">
        CodeBuild clones this public repository and runs <span className="font-mono text-text-hi">cdk deploy</span>{" "}
        against it to provision the stacks. The deployment config is the only thing the agent changes — never the
        repo's code.
      </p>
    </div>
  );
}

/**
 * Floating, dismissible panel anchored within the Components area (NOT a
 * full-screen modal). Content depends on the node: the cfn node shows the full
 * stack browser, codebuild shows all builds, github shows the source repo, and
 * any stack-backed node shows its deployed resources grouped by stack.
 */
export function ResourcePanel({
  nodeId,
  label,
  onClose,
}: {
  nodeId: string;
  label: string;
  onClose: () => void;
}) {
  const reduce = useReducedMotion();
  const mode = useDeploymentStore((s) => s.mode);

  // Close on Escape.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <>
      {/* Click-outside scrim — transparent so topology stays visible. */}
      <div className="absolute inset-0 z-20" onClick={onClose} />
      <motion.div
        role="dialog"
        aria-label={`${label} deployed resources`}
        className="absolute right-3 top-3 z-30 flex max-h-[calc(100%-1.5rem)] w-[320px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-anchor/95 shadow-2xl backdrop-blur"
        initial={reduce ? { opacity: 0 } : { opacity: 0, x: 16, scale: 0.97 }}
        animate={{ opacity: 1, x: 0, scale: 1 }}
        exit={reduce ? { opacity: 0 } : { opacity: 0, x: 16, scale: 0.97 }}
        transition={{ type: "spring", stiffness: 320, damping: 26 }}
      >
        <div className="flex shrink-0 items-center justify-between border-b border-white/8 px-3 py-2.5">
          <span className="flex items-center gap-1.5 text-[9.5px] font-semibold uppercase tracking-widest text-aws-teal">
            {nodeId === "codebuild" ? (
              <Hammer className="h-3 w-3" />
            ) : nodeId === "github" ? (
              <GitBranch className="h-3 w-3" />
            ) : (
              <span className="h-1.5 w-1.5 rounded-full bg-aws-teal shadow-[0_0_6px_var(--color-aws-teal)]" />
            )}
            {panelEyebrow(nodeId, mode)}
          </span>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close"
            className="rounded-md p-1 text-text-lo transition-colors hover:bg-white/10 hover:text-text-hi"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
        <div className="flex min-h-0 flex-1 flex-col p-3">
          {nodeId === "cfn" ? (
            <StackBrowser />
          ) : nodeId === "codebuild" ? (
            <BuildsBrowser />
          ) : nodeId === "github" ? (
            <GithubInfo />
          ) : (
            <NodeResources nodeId={nodeId} label={label} />
          )}
        </div>
      </motion.div>
    </>
  );
}
