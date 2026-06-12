import { motion } from "motion/react";
import type { ReactNode } from "react";
import type { ChatMessage } from "@/store/useDeploymentStore";
import { Markdown } from "@/components/chat/Markdown";
import { cn } from "@/lib/cn";

export function Message({ msg, children }: { msg: ChatMessage; children?: ReactNode }) {
  const isUser = msg.role === "user";
  const isSystem = msg.role === "system";
  // Rich blocks (diagrams, forms) need room — let those messages span the pane.
  const wide = !isUser && (msg.blocks?.length ?? 0) > 0;

  if (isSystem)
    return (
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        className="mx-auto text-center text-[11px] uppercase tracking-wide text-text-lo"
      >
        {msg.text}
      </motion.div>
    );

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ type: "spring", stiffness: 260, damping: 24 }}
      className={cn("flex w-full", isUser ? "justify-end" : "justify-start")}
    >
      <div className={cn("flex flex-col gap-2", wide ? "w-full" : "max-w-[88%]", isUser && "items-end")}>
        <div
          className={cn(
            "rounded-2xl px-3.5 py-2.5 text-[13.5px] leading-relaxed",
            isUser
              ? "rounded-br-sm bg-aws-blue/90 text-white"
              : "rounded-bl-sm border border-white/8 bg-surface-1 text-text-mid",
          )}
        >
          {!isUser && (
            <div className="mb-1 flex items-center gap-1.5 text-[11px] font-medium text-aws-teal">
              <span className="inline-block h-1.5 w-1.5 rounded-full bg-aws-teal" />
              Agent
            </div>
          )}
          <div>
            {isUser ? (
              // User text is plain (they typed it) — preserve newlines, no markdown.
              <span className="whitespace-pre-wrap">{msg.text}</span>
            ) : (
              <Markdown text={msg.text} />
            )}
            {msg.streaming && (
              <motion.span
                className="ml-0.5 inline-block h-3.5 w-1.5 translate-y-0.5 bg-aws-teal"
                animate={{ opacity: [1, 0.2, 1] }}
                transition={{ duration: 0.9, repeat: Infinity }}
              />
            )}
          </div>
        </div>
        {children}
      </div>
    </motion.div>
  );
}
