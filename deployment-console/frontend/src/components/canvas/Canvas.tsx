import { useEffect, useRef, useState } from "react";
import { AnimatePresence, motion, useReducedMotion } from "motion/react";
import { PartyPopper, X } from "lucide-react";
import { useDeploymentStore } from "@/store/useDeploymentStore";
import type { TabKey } from "@/store/useDeploymentStore";
import { useElapsed } from "@/lib/useElapsed";
import { DeployView } from "@/components/canvas/DeployView";
import { Logs } from "@/components/canvas/Logs";
import { Confetti } from "@/components/fx/Confetti";
import { cn } from "@/lib/cn";

const TABS: { key: TabKey; label: string }[] = [
  { key: "deploy", label: "Deploy" },
  { key: "logs", label: "Logs" },
];

function fmt(sec: number): string {
  const m = Math.floor(sec / 60);
  const s = Math.floor(sec % 60);
  return `${m}:${s.toString().padStart(2, "0")}`;
}

export function Canvas() {
  const tab = useDeploymentStore((s) => s.currentTab);
  const setTab = useDeploymentStore((s) => s.setTab);
  const build = useDeploymentStore((s) => s.build);
  const celebrated = useDeploymentStore((s) => s.celebrated);
  const celebrate = useDeploymentStore((s) => s.celebrate);
  const reduce = useReducedMotion();

  // Deploy-ignition shockwave: keyed on buildId so it replays each deploy.
  const igniteKey = build.startedAt ?? 0;
  const justIgnited = useRef(0);
  useEffect(() => {
    if (build.startedAt) justIgnited.current = build.startedAt;
  }, [build.startedAt]);

  const succeeded = build.status === "SUCCEEDED";
  const failed = build.status === "FAILED";
  const elapsed = useElapsed(build.startedAt, build.finishedAt);

  // Fire confetti exactly once on success.
  useEffect(() => {
    if (succeeded && !celebrated) celebrate();
  }, [succeeded, celebrated, celebrate]);

  // Hero badge auto-dismisses a few seconds after a build settles; a new build
  // (new finishedAt) re-shows it. We track the *dismissed build's* finishedAt
  // so a new build's badge shows without a synchronous setState reset.
  const [dismissedAt, setDismissedAt] = useState<number | undefined>(undefined);
  useEffect(() => {
    if (!build.finishedAt) return;
    const id = setTimeout(() => setDismissedAt(build.finishedAt), 6000);
    return () => clearTimeout(id);
  }, [build.finishedAt]);
  const showHero = (succeeded || failed) && dismissedAt !== build.finishedAt;

  return (
    <div className="relative flex h-full min-h-0 flex-col rounded-2xl border border-white/8 bg-surface-0/50 backdrop-blur-sm">
      {/* Tab bar */}
      <div className="flex items-center gap-1 border-b border-white/8 px-3 py-2">
        {TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={cn(
              "relative rounded-lg px-3 py-1.5 text-[13px] font-medium transition-colors",
              tab === t.key ? "text-text-hi" : "text-text-lo hover:text-text-mid",
            )}
          >
            {tab === t.key && (
              <motion.span
                layoutId="tab-pill"
                className="absolute inset-0 rounded-lg bg-white/8"
                transition={{ type: "spring", stiffness: 380, damping: 30 }}
              />
            )}
            <span className="relative z-10">{t.label}</span>
          </button>
        ))}
        <StatusBadge status={build.status} />
      </div>

      {/* Body */}
      <div className="relative min-h-0 flex-1 p-4">
        <AnimatePresence mode="wait">
          <motion.div
            key={tab}
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -8 }}
            transition={{ duration: 0.2 }}
            className="h-full"
          >
            {tab === "deploy" && <DeployView />}
            {tab === "logs" && <Logs />}
          </motion.div>
        </AnimatePresence>

        {/* Deploy-ignition shockwave */}
        {!reduce && build.status === "IN_PROGRESS" && (
          <motion.div
            key={igniteKey}
            className="pointer-events-none absolute inset-0 rounded-2xl"
            initial={{ opacity: 0.6, scale: 0.6 }}
            animate={{ opacity: 0, scale: 1.05 }}
            transition={{ duration: 0.8, ease: "easeOut" }}
            style={{ background: "radial-gradient(circle at 30% 30%, rgba(255,153,0,0.4), transparent 60%)" }}
          />
        )}
      </div>

      <Confetti fire={succeeded} />

      {/* Success / failure hero badge — auto-dismisses, or close manually. */}
      <AnimatePresence>
        {showHero && (
          <motion.div
            initial={{ opacity: 0, y: 16, scale: 0.9 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 16 }}
            transition={{ type: "spring", stiffness: 280, damping: 22 }}
            className={cn(
              "absolute bottom-4 left-1/2 flex -translate-x-1/2 items-center gap-2.5 rounded-2xl border py-2.5 pl-4 pr-2.5 shadow-2xl backdrop-blur-md",
              succeeded
                ? "border-aws-green/50 bg-aws-green/15"
                : "border-aws-red/50 bg-aws-red/15",
            )}
          >
            {succeeded ? (
              <PartyPopper size={18} className="text-aws-green" />
            ) : (
              <span className="text-aws-red">⚠</span>
            )}
            <div className="leading-tight">
              <div
                className={cn(
                  "text-[13px] font-semibold",
                  succeeded ? "text-aws-green" : "text-aws-red",
                )}
              >
                {succeeded ? "Deployment complete" : "Deployment failed"}
              </div>
              <div className="text-[11px] text-text-mid">
                {succeeded
                  ? `All stacks live · ${fmt(elapsed)}`
                  : "BUILD phase failed · see Logs"}
              </div>
            </div>
            <button
              type="button"
              onClick={() => setDismissedAt(build.finishedAt)}
              aria-label="Dismiss"
              className="ml-1 grid h-6 w-6 shrink-0 place-items-center rounded-lg text-text-lo transition-colors hover:bg-white/10 hover:text-text-hi"
            >
              <X size={14} />
            </button>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

function StatusBadge({ status }: { status: ReturnType<typeof useDeploymentStore.getState>["build"]["status"] }) {
  if (status === "IDLE") return null;
  const map = {
    IN_PROGRESS: { t: "in progress", c: "text-aws-orange bg-aws-orange/10 border-aws-orange/30" },
    SUCCEEDED: { t: "succeeded", c: "text-aws-green bg-aws-green/10 border-aws-green/30" },
    FAILED: { t: "failed", c: "text-aws-red bg-aws-red/10 border-aws-red/30" },
  } as const;
  const m = map[status];
  return (
    <span className={cn("ml-auto rounded-full border px-2.5 py-0.5 text-[11px] font-medium", m.c)}>
      {m.t}
    </span>
  );
}
