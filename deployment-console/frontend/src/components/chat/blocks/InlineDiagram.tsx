import { motion } from "motion/react";
import { Workflow } from "lucide-react";
import type { ComponentNode } from "@/store/useDeploymentStore";
import { ArchitectureDiagram, DiagramLegend } from "@/components/canvas/ArchitectureDiagram";

/**
 * An architecture diagram rendered inline in an agent message — a compact,
 * non-interactive card that *explains* a solution visually instead of in prose.
 * Height is fixed so the flow layout (percentage-positioned) has room to breathe.
 */
export function InlineDiagram({
  nodes,
  caption,
}: {
  nodes: ComponentNode[];
  caption?: string;
}) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 8, scale: 0.98 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      transition={{ type: "spring", stiffness: 260, damping: 24 }}
      className="overflow-hidden rounded-2xl border border-white/10 bg-surface-0/60 backdrop-blur-sm"
    >
      <div className="flex items-center gap-1.5 border-b border-white/8 px-3 py-2 text-[10.5px] font-semibold uppercase tracking-widest text-aws-teal">
        <Workflow className="h-3.5 w-3.5" />
        Architecture
        <span className="ml-auto">
          <DiagramLegend show={[...new Set(nodes.map((n) => n.status))]} />
        </span>
      </div>
      <div className="relative h-[230px] w-full px-2 py-3">
        <ArchitectureDiagram nodes={nodes} compact interactive={false} />
      </div>
      {caption && (
        <div className="border-t border-white/8 px-3 py-2 text-[11.5px] leading-relaxed text-text-mid">
          {caption}
        </div>
      )}
    </motion.div>
  );
}
