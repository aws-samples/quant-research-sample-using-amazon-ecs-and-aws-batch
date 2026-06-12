import { useCallback, useRef, useState } from "react";
import { motion } from "motion/react";
import { GradientBackdrop } from "@/components/fx/GradientBackdrop";
import { Header } from "@/components/Header";
import { ChatPane } from "@/components/chat/ChatPane";
import { Canvas } from "@/components/canvas/Canvas";

// Chat pane width as a % of the split row. Clamped so the canvas keeps >=60%
// (chat can grow to 40%) and never shrinks below a usable minimum.
const CHAT_MIN = 24;
const CHAT_MAX = 40;
const CHAT_DEFAULT = 30;

export default function App() {
  const [chatPct, setChatPct] = useState(CHAT_DEFAULT);
  const rowRef = useRef<HTMLDivElement>(null);
  const dragging = useRef(false);

  const onPointerMove = useCallback((e: PointerEvent) => {
    if (!dragging.current || !rowRef.current) return;
    const rect = rowRef.current.getBoundingClientRect();
    const pct = ((e.clientX - rect.left) / rect.width) * 100;
    setChatPct(Math.min(CHAT_MAX, Math.max(CHAT_MIN, pct)));
  }, []);

  const stopDrag = useCallback(() => {
    dragging.current = false;
    document.body.style.cursor = "";
    document.body.style.userSelect = "";
    window.removeEventListener("pointermove", onPointerMove);
    window.removeEventListener("pointerup", stopDrag);
  }, [onPointerMove]);

  const startDrag = useCallback(() => {
    dragging.current = true;
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
    window.addEventListener("pointermove", onPointerMove);
    window.addEventListener("pointerup", stopDrag);
  }, [onPointerMove, stopDrag]);

  return (
    <div className="flex h-full flex-col">
      <GradientBackdrop />
      <Header />

      {/* Split shell. Below lg the canvas stacks under the chat (no resize). */}
      <div
        ref={rowRef}
        className="flex min-h-0 flex-1 flex-col gap-3 p-3 lg:flex-row lg:gap-0"
      >
        <motion.aside
          initial={{ opacity: 0, x: -20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ type: "spring", stiffness: 180, damping: 24, delay: 0.1 }}
          style={{ ["--chat" as string]: `${chatPct}%` }}
          className="flex min-h-0 flex-col overflow-hidden rounded-2xl border border-white/8 bg-surface-0/50 backdrop-blur-sm lg:w-[var(--chat)] lg:shrink-0"
        >
          <ChatPane />
        </motion.aside>

        {/* Drag handle — only interactive at lg+ where the row is horizontal. */}
        <div
          onPointerDown={startDrag}
          onDoubleClick={() => setChatPct(CHAT_DEFAULT)}
          title="Drag to resize · double-click to reset"
          className="group hidden shrink-0 cursor-col-resize items-center justify-center px-1.5 lg:flex"
        >
          <div className="h-16 w-1 rounded-full bg-white/12 transition-colors group-hover:bg-aws-orange/60" />
        </div>

        <motion.main
          initial={{ opacity: 0, x: 20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ type: "spring", stiffness: 180, damping: 24, delay: 0.18 }}
          className="min-h-0 min-w-0 flex-1"
        >
          <Canvas />
        </motion.main>
      </div>
    </div>
  );
}
