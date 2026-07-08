import { useEffect, useRef } from "react";
import { AnimatePresence } from "motion/react";
import { useDeploymentStore } from "@/store/useDeploymentStore";
import { sendUserTurn } from "@/lib/driver";
import { Message } from "@/components/chat/Message";
import { ToolCallChip } from "@/components/chat/ToolCallChip";
import { ConfirmCard } from "@/components/chat/ConfirmCard";
import { Composer } from "@/components/chat/Composer";
import { InlineDiagram } from "@/components/chat/blocks/InlineDiagram";
import { InlineConfigForm } from "@/components/chat/blocks/InlineConfigForm";
import { DeployedStatus } from "@/components/chat/blocks/DeployedStatus";
import { RecentBuilds } from "@/components/chat/blocks/RecentBuilds";

export function ChatPane() {
  const messages = useDeploymentStore((s) => s.messages);
  const toolCalls = useDeploymentStore((s) => s.toolCalls);
  const scrollRef = useRef<HTMLDivElement>(null);

  // A turn is "in flight" while the last agent message is still streaming.
  const busy = messages.some((m) => m.streaming);

  useEffect(() => {
    const el = scrollRef.current;
    if (el) el.scrollTo({ top: el.scrollHeight, behavior: "smooth" });
  }, [messages, toolCalls]);

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div ref={scrollRef} className="flex-1 space-y-4 overflow-y-auto px-4 py-5">
        <AnimatePresence initial={false}>
          {messages.map((m) => (
            <Message key={m.id} msg={m}>
              {m.toolCallIds?.map((tid) =>
                toolCalls[tid] ? <ToolCallChip key={tid} tc={toolCalls[tid]} /> : null,
              )}
              {m.blocks?.map((b, i) => {
                switch (b.kind) {
                  case "diagram":
                    return <InlineDiagram key={`b${i}`} nodes={b.nodes} caption={b.caption} />;
                  case "configForm":
                    return <InlineConfigForm key={`b${i}`} willFail={b.willFail} />;
                  case "confirm":
                    return <ConfirmCard key={`b${i}`} />;
                  case "deployed":
                    return <DeployedStatus key={`b${i}`} />;
                  case "builds":
                    return <RecentBuilds key={`b${i}`} />;
                }
              })}
            </Message>
          ))}
        </AnimatePresence>
      </div>
      <Composer onSend={(t) => void sendUserTurn(t)} busy={busy} />
    </div>
  );
}
