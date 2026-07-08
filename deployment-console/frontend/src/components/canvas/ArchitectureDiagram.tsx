import { useState } from "react";
import { AnimatePresence, motion, useReducedMotion } from "motion/react";
import { ExternalLink, Info } from "lucide-react";
import type { ComponentNode, ComponentStatus } from "@/store/useDeploymentStore";
import { ServiceGlyph } from "@/components/canvas/ServiceGlyph";
import { ResourcePanel } from "@/components/canvas/ResourcePanel";
import { isResourceNode } from "@/lib/resourceStacks";
import { serviceConsoleUrl } from "@/lib/awsConsole";
import { TOPO_EDGES, TOPO_POS } from "@/lib/topology";
import { cn } from "@/lib/cn";

function edgeColor(a: ComponentNode | undefined, b: ComponentNode | undefined): string {
  if (a?.status === "live" && b?.status === "live") return "var(--color-aws-green)";
  if (b?.status === "creating" || a?.status === "creating") return "var(--color-aws-orange)";
  return "rgba(255,255,255,0.12)";
}

/**
 * Reusable animated AWS architecture flow diagram.
 *
 *  - `compact` (chat): smaller glyphs, tighter type, no resource drill-in. Used
 *    inline inside agent messages to *explain* the solution.
 *  - default (canvas): full size with hover tooltips + resource drill-in panel.
 *
 * Edges animate their draw-in; a "creating" node pulses; edges recolor as the
 * topology lights up (planned → creating → live). One source of truth for the
 * layout lives in `lib/topology.ts`.
 */
export function ArchitectureDiagram({
  nodes,
  compact = false,
  interactive = true,
  region,
  selectedId,
  onSelect,
}: {
  nodes: ComponentNode[];
  compact?: boolean;
  interactive?: boolean;
  /** When set, nodes link out to their AWS console page (region-scoped). */
  region?: string;
  /** Controlled selection — clicking a node toggles it (for an external panel). */
  selectedId?: string | null;
  onSelect?: (id: string | null) => void;
}) {
  const byId = Object.fromEntries(nodes.map((c) => [c.id, c]));
  const reduce = useReducedMotion();
  const [hover, setHover] = useState<string | null>(null);
  const [openId, setOpenId] = useState<string | null>(null);

  const present = new Set(nodes.map((c) => c.id));
  const edges = TOPO_EDGES.filter(([a, b]) => present.has(a) && present.has(b));
  const openNode = nodes.find((c) => c.id === openId) ?? null;
  const glyphSize = compact ? 30 : 36;

  return (
    <div className="relative h-full w-full">
      <svg className="absolute inset-0 h-full w-full" preserveAspectRatio="none">
        {edges.map(([a, b], i) => {
          const pa = TOPO_POS[a];
          const pb = TOPO_POS[b];
          return (
            <motion.line
              key={`${a}-${b}`}
              x1={`${pa.x}%`}
              y1={`${pa.y}%`}
              x2={`${pb.x}%`}
              y2={`${pb.y}%`}
              stroke={edgeColor(byId[a], byId[b])}
              strokeWidth={compact ? 1.25 : 1.5}
              initial={reduce ? { pathLength: 1 } : { pathLength: 0 }}
              animate={{ pathLength: 1 }}
              transition={{ delay: 0.3 + i * 0.08, duration: 0.6, ease: "easeInOut" }}
            />
          );
        })}
      </svg>

      {nodes.map((c, i) => (
        <DiagramNode
          key={c.id}
          node={c}
          index={i}
          glyphSize={glyphSize}
          compact={compact}
          interactive={interactive}
          region={region}
          selected={selectedId === c.id}
          onSelect={onSelect}
          hovered={hover === c.id}
          onHover={setHover}
          onOpen={setOpenId}
        />
      ))}

      <AnimatePresence>
        {openNode && (
          <ResourcePanel
            key={openNode.id}
            nodeId={openNode.id}
            label={openNode.label}
            onClose={() => setOpenId(null)}
          />
        )}
      </AnimatePresence>
    </div>
  );
}

function DiagramNode({
  node,
  index,
  glyphSize,
  compact,
  interactive,
  region,
  selected,
  onSelect,
  hovered,
  onHover,
  onOpen,
}: {
  node: ComponentNode;
  index: number;
  glyphSize: number;
  compact: boolean;
  interactive: boolean;
  region?: string;
  selected?: boolean;
  onSelect?: (id: string | null) => void;
  hovered: boolean;
  onHover: (id: string | null) => void;
  onOpen: (id: string) => void;
}) {
  const pos = TOPO_POS[node.id] ?? { x: 50, y: 50 };
  const reduce = useReducedMotion();
  const hasResources = interactive && !compact && isResourceNode(node.id);
  const consoleUrl = region ? serviceConsoleUrl(node.id, region) : null;
  // The glyph is clickable when it can select (chat) or open the console.
  const selectable = Boolean(onSelect);
  const clickable = selectable || Boolean(consoleUrl);

  const handleGlyphClick = () => {
    if (selectable) {
      onSelect!(selected ? null : node.id);
    } else if (consoleUrl) {
      window.open(consoleUrl, "_blank", "noopener,noreferrer");
    }
  };

  return (
    <motion.div
      className="absolute flex -translate-x-1/2 -translate-y-1/2 flex-col items-center"
      style={{ left: `${pos.x}%`, top: `${pos.y}%` }}
      initial={{ opacity: 0, scale: 0.4 }}
      animate={{ opacity: 1, scale: 1 }}
      transition={{ delay: index * 0.07, type: "spring", stiffness: 260, damping: 18 }}
      onMouseEnter={() => onHover(node.id)}
      onMouseLeave={() => onHover(null)}
    >
      <div className="relative">
        {node.status === "creating" && !reduce && (
          <motion.span
            className="absolute -inset-1 rounded-xl"
            style={{ boxShadow: "0 0 0 2px var(--color-aws-orange)" }}
            animate={{ opacity: [0.7, 0.1, 0.7], scale: [1, 1.18, 1] }}
            transition={{ duration: 1.4, repeat: Infinity }}
          />
        )}
        {/* Selection ring (chat click-to-inspect). */}
        {selected && (
          <span
            className="absolute -inset-1 rounded-xl ring-2 ring-aws-teal"
            style={{ boxShadow: "0 0 12px rgba(1,168,141,0.5)" }}
          />
        )}
        {clickable ? (
          <button
            type="button"
            onClick={handleGlyphClick}
            aria-label={
              selectable
                ? `${selected ? "Hide" : "Show"} resources for ${node.label}`
                : `Open ${node.label} in the AWS console`
            }
            title={
              selectable
                ? "Click to view resources"
                : `Open ${node.label} in the AWS console`
            }
            className="block rounded-lg transition-transform hover:scale-105 focus:outline-none focus-visible:ring-2 focus-visible:ring-aws-teal"
          >
            <ServiceGlyph id={node.id} status={node.status} size={glyphSize} />
          </button>
        ) : (
          <ServiceGlyph id={node.id} status={node.status} size={glyphSize} />
        )}
        {/* Console-link badge: shown whenever a region is set and the glyph's
            own click does something else (selection on chat, or resource
            drill-in on the canvas) — so the console stays one click away.
            Sits top-left when the Info drill-in badge occupies top-right. */}
        {consoleUrl && (selectable || hasResources) && (
          <a
            href={consoleUrl}
            target="_blank"
            rel="noreferrer"
            onClick={(e) => e.stopPropagation()}
            aria-label={`Open ${node.label} in the AWS console`}
            title="Open in AWS console"
            className={cn(
              "absolute z-10 grid h-4 w-4 place-items-center rounded-full",
              "border border-white/15 bg-anchor text-aws-blue shadow-md transition-colors",
              "hover:border-aws-blue hover:text-aws-blue/90",
              hasResources ? "-left-1.5 -top-1.5" : "-right-1.5 -top-1.5",
            )}
          >
            <ExternalLink className="h-2.5 w-2.5" />
          </a>
        )}
        {hasResources && (
          <button
            type="button"
            aria-label={`Show deployed resources for ${node.label}`}
            title="Show deployed resources"
            onClick={(e) => {
              e.stopPropagation();
              onOpen(node.id);
            }}
            className={cn(
              "absolute -right-1.5 -top-1.5 z-10 grid h-4 w-4 place-items-center rounded-full",
              "border border-white/15 bg-anchor text-aws-teal shadow-md transition-colors",
              "hover:border-aws-teal hover:text-aws-teal/90",
            )}
          >
            <Info className="h-2.5 w-2.5" />
          </button>
        )}
      </div>
      <span
        className={cn(
          "mt-1 font-medium",
          compact ? "text-[9.5px]" : "text-[10.5px]",
          node.status === "planned" ? "text-text-lo" : "text-text-mid",
        )}
      >
        {node.label}
      </span>

      {hovered && node.detail && (
        <motion.div
          initial={{ opacity: 0, y: 4 }}
          animate={{ opacity: 1, y: 0 }}
          className="absolute top-full z-20 mt-1 whitespace-nowrap rounded-md border border-white/10 bg-anchor px-2 py-1 text-[10.5px] text-text-mid shadow-lg"
        >
          {node.detail}
        </motion.div>
      )}
    </motion.div>
  );
}

/** Status → label/tone for the small diagram legend. */
const LEGEND: { status: ComponentStatus; label: string; dot: string }[] = [
  { status: "planned", label: "Planned", dot: "bg-white/25" },
  { status: "creating", label: "Deploying", dot: "bg-aws-orange" },
  { status: "live", label: "Live", dot: "bg-aws-green" },
];

/** Compact legend strip for inline diagrams. */
export function DiagramLegend({ show }: { show?: ComponentStatus[] }) {
  const items = show ? LEGEND.filter((l) => show.includes(l.status)) : LEGEND;
  return (
    <div className="flex items-center justify-center gap-3">
      {items.map((l) => (
        <span key={l.status} className="flex items-center gap-1 text-[9.5px] text-text-lo">
          <span className={cn("h-1.5 w-1.5 rounded-full", l.dot)} />
          {l.label}
        </span>
      ))}
    </div>
  );
}
