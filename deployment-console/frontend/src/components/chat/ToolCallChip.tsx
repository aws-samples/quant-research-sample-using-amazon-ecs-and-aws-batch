import { useState } from "react";
import { AnimatePresence, motion } from "motion/react";
import { ChevronRight } from "lucide-react";
import type { ToolCall } from "@/lib/client/types";
import { toolGlyph } from "@/lib/driver";
import { cn } from "@/lib/cn";

const STATUS_STYLE: Record<ToolCall["status"], string> = {
  running: "border-aws-orange/40 text-aws-orange",
  ok: "border-aws-green/40 text-aws-green",
  error: "border-aws-red/40 text-aws-red",
};

export function ToolCallChip({ tc }: { tc: ToolCall }) {
  const [open, setOpen] = useState(false);
  const result = tc.output;

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.95, y: 6 }}
      animate={{ opacity: 1, scale: 1, y: 0 }}
      transition={{ type: "spring", stiffness: 300, damping: 22 }}
      className={cn(
        "overflow-hidden rounded-xl border bg-surface-0/60 font-mono text-[12px]",
        STATUS_STYLE[tc.status],
      )}
    >
      <button
        onClick={() => result && setOpen((o) => !o)}
        className="flex w-full items-center gap-2 px-3 py-2 text-left"
      >
        <span className="shrink-0">{toolGlyph(tc.name)}</span>
        <span className="font-medium">{tc.name}</span>
        {tc.status === "running" && (
          <motion.span
            className="ml-1 h-1.5 w-1.5 rounded-full bg-aws-orange"
            animate={{ opacity: [1, 0.2, 1] }}
            transition={{ duration: 0.8, repeat: Infinity }}
          />
        )}
        <span className="ml-auto flex items-center gap-1 text-[10px] uppercase opacity-70">
          {tc.status}
          {result && (
            <motion.span animate={{ rotate: open ? 90 : 0 }}>
              <ChevronRight size={13} />
            </motion.span>
          )}
        </span>
      </button>

      {/* Running shimmer */}
      {tc.status === "running" && (
        <motion.div
          className="h-0.5 bg-gradient-to-r from-transparent via-aws-orange to-transparent"
          animate={{ x: ["-100%", "100%"] }}
          transition={{ duration: 1.1, repeat: Infinity, ease: "linear" }}
        />
      )}

      <AnimatePresence initial={false}>
        {open && result && (
          <motion.pre
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            className="overflow-x-auto border-t border-white/8 bg-black/30 px-3 py-2 text-[11px] text-text-mid"
          >
            {JSON.stringify(result, null, 2)}
          </motion.pre>
        )}
      </AnimatePresence>
    </motion.div>
  );
}
