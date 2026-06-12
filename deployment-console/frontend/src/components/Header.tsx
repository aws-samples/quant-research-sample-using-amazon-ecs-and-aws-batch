import { motion, useReducedMotion } from "motion/react";
import { useDeploymentStore } from "@/store/useDeploymentStore";
import { cn } from "@/lib/cn";

/** Animated diamond logo: the ◆ draws itself on entrance. */
function Logo() {
  const reduce = useReducedMotion();
  return (
    <svg width="26" height="26" viewBox="0 0 24 24" className="shrink-0">
      <motion.path
        d="M12 2 L22 12 L12 22 L2 12 Z"
        fill="none"
        stroke="var(--color-aws-orange)"
        strokeWidth={2}
        strokeLinejoin="round"
        initial={reduce ? { pathLength: 1 } : { pathLength: 0 }}
        animate={{ pathLength: 1 }}
        transition={{ duration: 1.1, ease: "easeInOut", delay: 0.15 }}
      />
      <motion.path
        d="M12 7 L17 12 L12 17 L7 12 Z"
        fill="var(--color-aws-orange)"
        initial={reduce ? { opacity: 1, scale: 1 } : { opacity: 0, scale: 0.2 }}
        animate={{ opacity: 1, scale: 1 }}
        transition={{ type: "spring", stiffness: 220, damping: 16, delay: 1.0 }}
        style={{ transformOrigin: "12px 12px" }}
      />
    </svg>
  );
}

export function Header() {
  const mode = useDeploymentStore((s) => s.mode);
  const setMode = useDeploymentStore((s) => s.setMode);

  return (
    <motion.header
      initial={{ opacity: 0, y: -16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ type: "spring", stiffness: 180, damping: 22 }}
      className="flex items-center justify-between border-b border-white/8 bg-surface-0/70 px-5 py-3 backdrop-blur-md"
    >
      <div className="flex items-center gap-2.5">
        <Logo />
        <div className="leading-tight">
          <div className="text-[15px] font-semibold tracking-tight text-text-hi">
            Deployment Console
          </div>
          <div className="text-[11px] text-text-lo">AgentCore · CodeBuild · CDK</div>
        </div>
      </div>

      <div className="flex items-center gap-4">
        <ModeToggle mode={mode} onChange={setMode} />
        <span
          className="h-2.5 w-2.5 rounded-full bg-aws-teal shadow-[0_0_10px_2px_rgba(1,168,141,0.6)]"
          title="AWS dark theme"
        />
      </div>
    </motion.header>
  );
}

function ModeToggle({
  mode,
  onChange,
}: {
  mode: "mock" | "live";
  onChange: (m: "mock" | "live") => void;
}) {
  return (
    <div className="flex items-center rounded-full border border-white/10 bg-surface-1 p-0.5 text-xs font-medium">
      {(["mock", "live"] as const).map((m) => (
        <button
          key={m}
          onClick={() => onChange(m)}
          className={cn(
            "relative rounded-full px-3 py-1 capitalize transition-colors",
            mode === m ? "text-anchor" : "text-text-mid hover:text-text-hi",
          )}
        >
          {mode === m && (
            <motion.span
              layoutId="mode-pill"
              className="absolute inset-0 rounded-full bg-aws-orange"
              transition={{ type: "spring", stiffness: 380, damping: 30 }}
            />
          )}
          <span className="relative z-10">{m}</span>
        </button>
      ))}
    </div>
  );
}
