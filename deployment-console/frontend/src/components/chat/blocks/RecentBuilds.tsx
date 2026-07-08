import { useEffect, useState } from "react";
import { motion } from "motion/react";
import { CheckCircle2, ExternalLink, Hammer, Loader2, PlayCircle, ScrollText, XCircle } from "lucide-react";
import { useDeploymentStore } from "@/store/useDeploymentStore";
import type { BuildRecord } from "@/store/useDeploymentStore";
import type { BuildSummary } from "@/lib/client/types";
import { plannedStacks } from "@/lib/defaults";
import { activeClient, loadBuildOntoCanvas } from "@/lib/driver";
import { codeBuildUrl, regionFromAz } from "@/lib/awsConsole";
import { cn } from "@/lib/cn";

/** Show at most this many recent builds inline. */
const MAX = 6;

/** A unified display model for both session builds and real CodeBuild builds. */
interface BuildView {
  buildId: string;
  status: "SUCCEEDED" | "FAILED" | "IN_PROGRESS" | "IDLE" | string;
  startedAt?: number;
  finishedAt?: number;
  /** Stack-type chips (session builds know their config; real builds may not). */
  chips: string[];
  stackCount?: number;
  /** True if this build is in session history (has a loadable snapshot). */
  inHistory: boolean;
}

function fmtDuration(start?: number, end?: number): string {
  if (!start) return "—";
  const sec = Math.max(0, Math.round(((end ?? start) - start) / 1000));
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return `${m}:${s.toString().padStart(2, "0")}`;
}

/** "just now" / "3m ago" — coarse relative time from a timestamp. */
function relTime(ts?: number): string {
  if (!ts) return "";
  const diff = Math.max(0, Date.now() - ts);
  const min = Math.floor(diff / 60000);
  if (min < 1) return "just now";
  if (min < 60) return `${min}m ago`;
  const h = Math.floor(min / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

const TONE = {
  ok: { Icon: CheckCircle2, chip: "bg-aws-green/15 text-aws-green", text: "text-aws-green", label: "Succeeded" },
  error: { Icon: XCircle, chip: "bg-aws-red/15 text-aws-red", text: "text-aws-red", label: "Failed" },
  progress: { Icon: Loader2, chip: "bg-aws-orange/15 text-aws-orange", text: "text-aws-orange", label: "Running" },
} as const;

function toneFor(status: string) {
  if (status === "SUCCEEDED") return TONE.ok;
  if (status === "FAILED" || status === "FAULT" || status === "TIMED_OUT" || status === "STOPPED")
    return TONE.error;
  return TONE.progress;
}

/** Short stack-type chips summarising what a session build deployed. */
function chipsForConfig(config: BuildRecord["config"]): string[] {
  return plannedStacks(config).map((p) => {
    if (p.node === "vpc") return "VPC";
    if (p.node === "s3") return "S3";
    if (p.node === "ecr") return "Pipeline";
    if (p.node === "fsx") return "FSx";
    if (p.key === "s3express") return "S3X";
    if (p.key === "batchGpu" || p.name.includes("gpu")) return "GPU";
    if (p.key === "batchCpu" || p.name.includes("cpu")) return "CPU";
    return p.label;
  });
}

function fromHistory(r: BuildRecord): BuildView {
  return {
    buildId: r.buildId,
    status: r.status,
    startedAt: r.startedAt,
    finishedAt: r.finishedAt,
    chips: chipsForConfig(r.config),
    stackCount: r.stackCount,
    inHistory: true,
  };
}

function fromSummary(b: BuildSummary): BuildView {
  return {
    buildId: b.buildId,
    status: b.status,
    startedAt: b.startedAt ? Date.parse(b.startedAt) : undefined,
    finishedAt: b.finishedAt ? Date.parse(b.finishedAt) : undefined,
    chips: [],
    inHistory: false,
  };
}

/**
 * Inline "recent builds" block for the chat — surfaced on "what's deployed" /
 * "check status". In **live** mode it fetches the REAL CodeBuild builds for the
 * deploy project (so builds from prior sessions show too) and merges them with
 * this session's history (session records win — they carry a loadable snapshot
 * and config chips). Each build can be **loaded** onto the canvas.
 */
export function RecentBuilds() {
  const history = useDeploymentStore((s) => s.buildHistory);
  const mode = useDeploymentStore((s) => s.mode);
  const activeBuildId = useDeploymentStore((s) => s.build.buildId);
  const region = useDeploymentStore((s) => regionFromAz(s.config.availability_zone.name));

  const [remote, setRemote] = useState<{ loading: boolean; builds: BuildSummary[]; error?: string }>(
    { loading: mode === "live", builds: [] },
  );

  useEffect(() => {
    if (mode !== "live") return;
    let alive = true;
    activeClient()
      .listBuilds(MAX)
      .then((builds) => alive && setRemote({ loading: false, builds }))
      .catch((err: unknown) => alive && setRemote({ loading: false, builds: [], error: (err as Error).message }));
    return () => {
      alive = false;
    };
  }, [mode]);

  // Merge: session history first (loadable snapshots), then real builds not
  // already represented in history. Dedupe by buildId.
  const seen = new Set<string>();
  const merged: BuildView[] = [];
  for (const r of history) {
    if (seen.has(r.buildId)) continue;
    seen.add(r.buildId);
    merged.push(fromHistory(r));
  }
  for (const b of remote.builds) {
    if (seen.has(b.buildId)) continue;
    seen.add(b.buildId);
    merged.push(fromSummary(b));
  }
  // Sort newest-first by start time (undefined last).
  merged.sort((a, z) => (z.startedAt ?? 0) - (a.startedAt ?? 0));
  const builds = merged.slice(0, MAX);

  if (remote.loading && builds.length === 0) {
    return (
      <div className="flex items-center gap-2 rounded-2xl border border-white/10 bg-surface-0/60 px-3.5 py-3 text-[11.5px] text-text-mid backdrop-blur-sm">
        <Loader2 className="h-3.5 w-3.5 animate-spin" /> Loading builds from CodeBuild…
      </div>
    );
  }

  if (builds.length === 0) {
    return (
      <div className="rounded-2xl border border-white/10 bg-surface-0/60 px-3.5 py-3 text-[11.5px] text-text-mid backdrop-blur-sm">
        No builds found{remote.error ? ` (${remote.error})` : ""}. Describe a deployment and confirm it to start one.
      </div>
    );
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 8, scale: 0.98 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      transition={{ type: "spring", stiffness: 260, damping: 24 }}
      className="overflow-hidden rounded-2xl border border-white/10 bg-surface-0/60 backdrop-blur-sm"
    >
      <div className="flex items-center gap-1.5 border-b border-white/8 px-3.5 py-2 text-[10.5px] font-semibold uppercase tracking-widest text-aws-teal">
        <Hammer className="h-3.5 w-3.5" />
        Recent builds
        <span className="ml-auto font-normal normal-case tracking-normal text-text-lo">
          {mode === "live" ? "from CodeBuild" : "this session"}
        </span>
      </div>

      <div className="flex flex-col gap-2 p-2.5">
        {builds.map((b, i) => (
          <BuildCard
            key={b.buildId}
            build={b}
            index={i}
            region={region}
            active={b.buildId === activeBuildId}
          />
        ))}
      </div>
    </motion.div>
  );
}

function BuildCard({
  build: b,
  index,
  region,
  active,
}: {
  build: BuildView;
  index: number;
  region: string;
  active: boolean;
}) {
  const tone = toneFor(b.status);
  const running = b.status === "IN_PROGRESS";
  const setTab = useDeploymentStore((s) => s.setTab);

  return (
    <motion.div
      initial={{ opacity: 0, y: 6 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.05, type: "spring", stiffness: 300, damping: 26 }}
      className={cn(
        "rounded-xl border p-2.5 transition-colors",
        active
          ? "border-aws-teal/40 bg-aws-teal/[0.06]"
          : "border-white/8 bg-surface-1/50 hover:border-white/15",
      )}
    >
      <div className="flex items-center gap-2.5">
        <span className={cn("grid h-8 w-8 shrink-0 place-items-center rounded-lg", tone.chip)}>
          <tone.Icon className={cn("h-4 w-4", running && "animate-spin")} strokeWidth={2.5} />
        </span>

        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-1.5">
            <span className="truncate font-mono text-[11.5px] text-text-hi" title={b.buildId}>
              {b.buildId}
            </span>
            {active && (
              <span className="shrink-0 rounded bg-aws-teal/15 px-1 py-px text-[8.5px] font-semibold uppercase tracking-wide text-aws-teal">
                on canvas
              </span>
            )}
          </div>
          <div className="mt-0.5 flex flex-wrap items-center gap-1.5 text-[10px] text-text-lo">
            <span className={cn("font-medium", tone.text)}>{tone.label}</span>
            {b.stackCount != null && (
              <>
                <span>·</span>
                <span>{b.stackCount} stacks</span>
              </>
            )}
            <span>·</span>
            <span className="tabular-nums">{fmtDuration(b.startedAt, b.finishedAt)}</span>
            {relTime(b.startedAt) && (
              <>
                <span>·</span>
                <span>{relTime(b.startedAt)}</span>
              </>
            )}
          </div>
        </div>
      </div>

      {b.chips.length > 0 && (
        <div className="mt-2 flex flex-wrap items-center gap-1">
          {b.chips.map((c, idx) => (
            <span
              key={`${c}-${idx}`}
              className="rounded-md border border-white/8 bg-white/[0.03] px-1.5 py-0.5 text-[9.5px] font-medium text-text-mid"
            >
              {c}
            </span>
          ))}
        </div>
      )}

      <div className="mt-2.5 flex items-center gap-2">
        <button
          type="button"
          onClick={() =>
            loadBuildOntoCanvas({
              buildId: b.buildId,
              inHistory: b.inHistory,
              status: b.status,
              startedAt: b.startedAt,
              finishedAt: b.finishedAt,
            })
          }
          disabled={active}
          className={cn(
            "flex flex-1 items-center justify-center gap-1.5 rounded-lg px-3 py-1.5 text-[12px] font-semibold transition-colors",
            active
              ? "cursor-default border border-aws-teal/30 text-aws-teal"
              : "bg-aws-orange text-anchor hover:bg-aws-orange-2",
          )}
        >
          <PlayCircle className="h-3.5 w-3.5" />
          {active ? "On canvas" : "Load on canvas"}
        </button>
        <button
          type="button"
          title="Load this build and view its execution logs"
          onClick={() => {
            // Load the build onto the canvas if it isn't already, then jump to Logs.
            if (!active) {
              loadBuildOntoCanvas({
                buildId: b.buildId,
                inHistory: b.inHistory,
                status: b.status,
                startedAt: b.startedAt,
                finishedAt: b.finishedAt,
              });
            }
            setTab("logs");
          }}
          className="flex shrink-0 items-center gap-1 rounded-lg border border-white/10 px-2.5 py-1.5 text-[11.5px] font-medium text-text-mid transition-colors hover:border-aws-orange/40 hover:text-aws-orange"
        >
          <ScrollText className="h-3.5 w-3.5" /> Logs
        </button>
        <a
          href={codeBuildUrl(region, b.buildId)}
          target="_blank"
          rel="noreferrer"
          title="Open in the CodeBuild console"
          className="flex shrink-0 items-center gap-1 rounded-lg border border-white/10 px-2.5 py-1.5 text-[11.5px] font-medium text-text-mid transition-colors hover:border-aws-blue/40 hover:text-aws-blue"
        >
          Console <ExternalLink className="h-3 w-3" />
        </a>
      </div>
    </motion.div>
  );
}
