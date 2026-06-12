import { useEffect, useRef, useState } from "react";
import { AnimatePresence, motion } from "motion/react";
import { ArrowDown } from "lucide-react";
import { useDeploymentStore } from "@/store/useDeploymentStore";
import { cn } from "@/lib/cn";

function lineClass(line: string): string {
  if (/CREATE_FAILED|❌|failed|Insufficient|ROLLBACK/i.test(line)) return "text-aws-red";
  if (/SUCCEEDED|CREATE_COMPLETE|✓|complete/i.test(line)) return "text-aws-green";
  if (/^\[[A-Z_]+\]/.test(line)) return "text-aws-orange font-medium";
  return "text-text-mid";
}

export function Logs() {
  const logs = useDeploymentStore((s) => s.logs);
  const ref = useRef<HTMLDivElement>(null);
  const [pinned, setPinned] = useState(true);

  // Auto-scroll only when pinned to bottom. On a FAILED line, jump to it.
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const lastIsError = /FAILED|❌/i.test(logs[logs.length - 1] ?? "");
    if (pinned || lastIsError) el.scrollTo({ top: el.scrollHeight, behavior: "smooth" });
  }, [logs, pinned]);

  const onScroll = () => {
    const el = ref.current;
    if (!el) return;
    setPinned(el.scrollHeight - el.scrollTop - el.clientHeight < 24);
  };

  return (
    <div className="relative h-full">
      <div
        ref={ref}
        onScroll={onScroll}
        className="h-full overflow-y-auto rounded-xl border border-white/8 bg-black/40 p-3 font-mono text-[12px] leading-relaxed"
      >
        {logs.length === 0 ? (
          <div className="text-text-lo">// logs stream here during deployment</div>
        ) : (
          logs.map((line, i) => (
            <motion.div
              key={i}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ duration: 0.18 }}
              className={cn("whitespace-pre-wrap", lineClass(line))}
            >
              <span className="select-none text-aws-orange/60">$ </span>
              {line}
            </motion.div>
          ))
        )}
      </div>

      <AnimatePresence>
        {!pinned && (
          <motion.button
            initial={{ opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: 6 }}
            onClick={() => {
              ref.current?.scrollTo({ top: ref.current.scrollHeight, behavior: "smooth" });
              setPinned(true);
            }}
            className="absolute bottom-3 left-1/2 flex -translate-x-1/2 items-center gap-1 rounded-full border border-white/12 bg-surface-2 px-3 py-1 text-[11px] font-medium text-text-hi shadow-lg"
          >
            <ArrowDown size={12} /> Jump to latest
          </motion.button>
        )}
      </AnimatePresence>
    </div>
  );
}
