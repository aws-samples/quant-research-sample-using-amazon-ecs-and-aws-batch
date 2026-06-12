import { motion } from "motion/react";
import { PRESETS } from "@/lib/presets";

/** Quick-chip presets above the composer. Clicking sends the prompt. */
export function PresetChips({
  onPick,
  disabled,
}: {
  onPick: (prompt: string) => void;
  disabled: boolean;
}) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {PRESETS.map((p, i) => (
        <motion.button
          key={p.label}
          initial={{ opacity: 0, y: 6 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.05 * i, type: "spring", stiffness: 300, damping: 24 }}
          whileHover={{ y: -1 }}
          whileTap={{ scale: 0.96 }}
          disabled={disabled}
          onClick={() => onPick(p.prompt)}
          className="rounded-full border border-white/10 bg-surface-1 px-2.5 py-1 text-[11.5px] font-medium text-text-mid transition-colors hover:border-aws-orange/40 hover:text-text-hi disabled:opacity-40"
        >
          {p.label}
        </motion.button>
      ))}
    </div>
  );
}
