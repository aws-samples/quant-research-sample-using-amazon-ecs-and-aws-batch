import { useLayoutEffect, useRef, useState } from "react";
import { motion } from "motion/react";
import { ArrowUp } from "lucide-react";
import { PresetChips } from "@/components/chat/PresetChips";

const MAX_H = 220; // px — grow until here, then scroll

/** Textarea + send. Enter sends, Shift+Enter newlines. The input auto-grows with
 *  content (typing, pasting, or newlines) up to MAX_H, then scrolls. */
export function Composer({
  onSend,
  busy,
}: {
  onSend: (text: string) => void;
  busy: boolean;
}) {
  const [value, setValue] = useState("");
  const ref = useRef<HTMLTextAreaElement>(null);

  // Resize to fit content on every value change (covers type / paste / newline / clear).
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = `${Math.min(el.scrollHeight, MAX_H)}px`;
  }, [value]);

  const send = (text: string) => {
    const t = text.trim();
    if (!t || busy) return;
    onSend(t);
    setValue("");
  };

  return (
    <div className="flex flex-col gap-2 border-t border-white/8 bg-surface-0/60 p-3 backdrop-blur-md">
      <PresetChips onPick={send} disabled={busy} />
      <div className="flex items-end gap-2 rounded-xl border border-white/10 bg-surface-1 px-3.5 py-2.5 focus-within:border-aws-orange/50">
        <textarea
          ref={ref}
          value={value}
          rows={1}
          placeholder={busy ? "Agent is working…" : "Describe the infrastructure to deploy…"}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              send(value);
            }
          }}
          className="flex-1 resize-none overflow-y-auto bg-transparent text-[15px] leading-relaxed text-text-hi placeholder:text-text-lo focus:outline-none"
        />
        <motion.button
          whileTap={{ scale: 0.9 }}
          disabled={busy || !value.trim()}
          onClick={() => send(value)}
          className="grid h-9 w-9 shrink-0 place-items-center rounded-lg bg-aws-orange text-anchor transition-colors hover:bg-aws-orange-2 disabled:opacity-30"
          aria-label="Send"
        >
          <ArrowUp size={17} strokeWidth={2.5} />
        </motion.button>
      </div>
    </div>
  );
}
