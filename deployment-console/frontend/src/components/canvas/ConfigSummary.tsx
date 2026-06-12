import { useState } from "react";
import { AnimatePresence, motion } from "motion/react";
import { Check, ChevronDown, SlidersHorizontal } from "lucide-react";
import { useDeploymentStore } from "@/store/useDeploymentStore";
import { stackCount } from "@/lib/defaults";
import { ConfigDetail } from "@/components/canvas/ConfigView";
import { cn } from "@/lib/cn";

/**
 * Compact, collapsible configuration bar shown at the top of the Architecture
 * pane (the Config tab was merged into Deploy). Collapsed: a single line with
 * the deployment type, AZ, stack count, enabled options as chips, and the
 * validated badge. Expanded: the full ConfigDetail.
 */
export function ConfigSummary() {
  const config = useDeploymentStore((s) => s.config);
  const validated = useDeploymentStore((s) => s.validated);
  const [open, setOpen] = useState(false);
  const n = stackCount(config);

  const chips = [
    config.app_with_fsx && "FSx",
    config.app_with_codepipeline && "CodePipeline",
    config.app_with_s3express && "S3 Express",
  ].filter(Boolean) as string[];

  return (
    <div className="mb-3 shrink-0 overflow-hidden rounded-xl border border-white/8 bg-surface-1/50">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        aria-expanded={open}
        className="flex w-full items-center gap-2.5 px-3 py-2 text-left transition-colors hover:bg-white/[0.03]"
      >
        <SlidersHorizontal className="h-3.5 w-3.5 shrink-0 text-aws-teal" />
        <span className="shrink-0 text-[10.5px] font-semibold uppercase tracking-widest text-aws-teal">
          Config
        </span>

        {/* Inline summary */}
        <span className="flex min-w-0 flex-1 items-center gap-2 text-[12px] text-text-mid">
          <span className="font-mono text-text-hi">{config.batch.deployment_type}</span>
          <span className="text-text-lo">·</span>
          <span className="font-mono text-text-hi">{config.availability_zone.name}</span>
          <span className="text-text-lo">·</span>
          <span>
            <span className="font-semibold text-aws-orange">{n}</span> stacks
          </span>
          {chips.length > 0 && (
            <span className="hidden items-center gap-1 sm:flex">
              {chips.map((c) => (
                <span
                  key={c}
                  className="rounded-full border border-aws-teal/30 bg-aws-teal/10 px-1.5 py-px text-[10px] font-medium text-aws-teal"
                >
                  {c}
                </span>
              ))}
            </span>
          )}
        </span>

        {validated && (
          <span className="flex shrink-0 items-center gap-1 rounded-full border border-aws-green/40 bg-aws-green/10 px-2 py-0.5 text-[10.5px] font-medium text-aws-green">
            <Check size={11} strokeWidth={3} /> validated
          </span>
        )}
        <ChevronDown
          className={cn(
            "h-4 w-4 shrink-0 text-text-lo transition-transform",
            open && "rotate-180",
          )}
        />
      </button>

      <AnimatePresence initial={false}>
        {open && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="overflow-hidden border-t border-white/8"
          >
            <div className="p-3">
              <ConfigDetail />
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
