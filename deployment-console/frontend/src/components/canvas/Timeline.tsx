import { AnimatePresence, motion, useReducedMotion } from "motion/react";
import { Check, ExternalLink, X } from "lucide-react";
import { useDeploymentStore } from "@/store/useDeploymentStore";
import { PHASES } from "@/lib/phases";
import type { PhaseState } from "@/lib/client/types";
import { useElapsed } from "@/lib/useElapsed";
import { codeBuildUrl, regionFromAz } from "@/lib/awsConsole";
import { cn } from "@/lib/cn";

// Rail geometry — shared by the node discs and the connector segments so they
// always line up. Node center sits at RAIL_CENTER px from the row's left edge.
const RAIL_CENTER = 15; // px
const NODE = 22; // px disc diameter
const NODE_TOP = 4; // px from row top → node vertical center = NODE_TOP + NODE/2 = 15

function fmt(sec: number): string {
  const m = Math.floor(sec / 60);
  const s = Math.floor(sec % 60);
  return `${m}:${s.toString().padStart(2, "0")}`;
}

export function Timeline() {
  const build = useDeploymentStore((s) => s.build);
  const az = useDeploymentStore((s) => s.config.availability_zone.name);
  const elapsed = useElapsed(build.startedAt, build.finishedAt);
  const region = regionFromAz(az);

  if (build.status === "IDLE")
    return (
      <div className="grid h-full place-items-center text-center text-sm text-text-lo">
        <div>
          <div className="text-text-mid">No deployment yet</div>
          <div className="mt-1 text-xs">Confirm a deploy to watch the CodeBuild journey here.</div>
        </div>
      </div>
    );

  return (
    <div className="flex h-full flex-col">
      <div className="mb-4 flex items-center justify-between">
        <a
          href={codeBuildUrl(region, build.buildId)}
          target="_blank"
          rel="noreferrer"
          title="Open this build in the CodeBuild console"
          className="group inline-flex items-center gap-1.5 text-sm font-medium text-text-mid transition-colors hover:text-text-hi"
        >
          Build{" "}
          <span className="font-mono text-aws-blue underline decoration-aws-blue/40 underline-offset-2 group-hover:decoration-aws-blue">
            {build.buildId}
          </span>
          <ExternalLink className="h-3 w-3 text-text-lo group-hover:text-aws-blue" />
        </a>
        <div className="flex items-center gap-2 font-mono text-sm">
          <span
            className={cn(
              "tabular-nums",
              build.status === "FAILED" ? "text-aws-red" : "text-aws-orange",
            )}
          >
            {fmt(elapsed)}
          </span>
          {build.status === "IN_PROGRESS" && <span className="text-text-lo">~28s est.</span>}
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto pr-1">
        <ol>
          {PHASES.map((def, i) => {
            const ph = build.phases.find((p) => p.name === def.name)!;
            return (
              <Step
                key={def.name}
                label={def.label}
                ph={ph}
                isBuild={def.name === "BUILD"}
                isLast={i === PHASES.length - 1}
              />
            );
          })}
        </ol>
      </div>
    </div>
  );
}

function Step({
  label,
  ph,
  isBuild,
  isLast,
}: {
  label: string;
  ph: PhaseState;
  isBuild: boolean;
  isLast: boolean;
}) {
  const stacks = useDeploymentStore((s) => s.build.stacks);
  const reduce = useReducedMotion();
  const failed = ph.status === "FAILED";
  const done = ph.status === "SUCCEEDED";
  const active = ph.status === "IN_PROGRESS";

  return (
    <li className="relative pb-2.5 pl-9 last:pb-0">
      {/* Connector segment: node-center of THIS step → node-center of the NEXT.
          Anchored to RAIL_CENTER so it's always perfectly under the disc, and it
          tucks beneath the next opaque node disc (never crosses a marker center). */}
      {!isLast && (
        <span
          className="absolute w-[2px] -translate-x-1/2 overflow-hidden rounded bg-white/10"
          style={{ left: RAIL_CENTER, top: NODE_TOP + NODE / 2, bottom: -(NODE_TOP + NODE / 2) }}
        >
          <motion.span
            className="absolute inset-0 origin-top rounded bg-gradient-to-b from-aws-orange to-aws-teal"
            initial={{ scaleY: 0 }}
            animate={{ scaleY: done ? 1 : active ? 0.5 : 0 }}
            transition={reduce ? { duration: 0 } : { type: "spring", stiffness: 90, damping: 18 }}
            style={done ? { boxShadow: "0 0 10px rgba(1,168,141,0.45)" } : undefined}
          />
        </span>
      )}

      <Node status={ph.status} />

      <motion.div
        animate={failed && !reduce ? { x: [0, -4, 4, -3, 3, 0] } : undefined}
        transition={{ duration: 0.4 }}
        className={cn(
          "rounded-lg px-2 py-1.5 text-[13px] leading-[14px]",
          ph.status === "PENDING" && "text-text-lo",
          active && "bg-aws-orange/10 font-medium text-aws-orange",
          done && "text-text-mid",
          failed && "bg-aws-red/10 font-medium text-aws-red",
        )}
      >
        {label}
      </motion.div>

      {/* Nested CDK stack sub-steps under BUILD */}
      <AnimatePresence>
        {isBuild && (active || done || failed) && (
          <motion.ul
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
            className="ml-1 mt-1 space-y-1 border-l border-white/10 pl-3"
          >
            {stacks.map((st, i) => (
              <motion.li
                key={st.key}
                initial={{ opacity: 0, x: -6 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: i * 0.05 }}
                className="flex items-center gap-2 text-[12px]"
              >
                <span
                  className={cn(
                    "h-1.5 w-1.5 rounded-full",
                    st.status === "planned" && "bg-white/20",
                    st.status === "creating" && "bg-aws-orange",
                    st.status === "live" && "bg-aws-green",
                  )}
                />
                <span
                  className={cn(
                    st.status === "planned" && "text-text-lo",
                    st.status === "creating" && "text-aws-orange",
                    st.status === "live" && "text-text-mid",
                  )}
                >
                  {st.label}
                </span>
              </motion.li>
            ))}
          </motion.ul>
        )}
      </AnimatePresence>
    </li>
  );
}

/**
 * The step marker — an OPAQUE disc so the connector segment is cleanly hidden
 * behind it (no bleed-through). Pending dot → spinning ring (active) → spring
 * check / X. Center is fixed at RAIL_CENTER so every marker aligns with the line.
 */
function Node({ status }: { status: PhaseState["status"] }) {
  return (
    <span
      className="absolute z-10 grid -translate-x-1/2 place-items-center"
      style={{ left: RAIL_CENTER, top: NODE_TOP, height: NODE, width: NODE }}
    >
      {status === "PENDING" && (
        <span className="grid h-full w-full place-items-center rounded-full border border-white/12 bg-surface-1">
          <span className="h-2 w-2 rounded-full bg-white/25" />
        </span>
      )}

      {status === "IN_PROGRESS" && (
        <span className="grid h-full w-full place-items-center rounded-full bg-surface-1">
          <motion.span
            className="h-full w-full rounded-full border-2 border-aws-orange border-t-transparent"
            animate={{ rotate: 360 }}
            transition={{ duration: 0.9, repeat: Infinity, ease: "linear" }}
            style={{ boxShadow: "0 0 10px rgba(255,153,0,0.5)" }}
          />
        </span>
      )}

      {status === "SUCCEEDED" && (
        <motion.span
          initial={{ scale: 0 }}
          animate={{ scale: 1 }}
          transition={{ type: "spring", stiffness: 500, damping: 18 }}
          className="grid h-full w-full place-items-center rounded-full bg-aws-green text-anchor"
        >
          <Check size={13} strokeWidth={3} />
        </motion.span>
      )}

      {status === "FAILED" && (
        <motion.span
          initial={{ scale: 0 }}
          animate={{ scale: 1 }}
          transition={{ type: "spring", stiffness: 500, damping: 18 }}
          className="grid h-full w-full place-items-center rounded-full bg-aws-red text-white"
        >
          <X size={13} strokeWidth={3} />
        </motion.span>
      )}
    </span>
  );
}
