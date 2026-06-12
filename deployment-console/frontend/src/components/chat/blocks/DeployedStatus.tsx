import { useEffect, useMemo, useState } from "react";
import { AnimatePresence, motion } from "motion/react";
import { ExternalLink, Loader2, MousePointerClick, ServerCog } from "lucide-react";
import { useDeploymentStore } from "@/store/useDeploymentStore";
import type { ComponentNode } from "@/store/useDeploymentStore";
import type { ResourceInfo, StackInfo } from "@/lib/client/types";
import { activeClient } from "@/lib/driver";
import { TOPO_NODES, topoNode } from "@/lib/topology";
import { nodeForStack, stacksForNode } from "@/lib/resourceStacks";
import { cloudFormationUrl, regionFromAz, serviceConsoleUrl } from "@/lib/awsConsole";
import { ArchitectureDiagram, DiagramLegend } from "@/components/canvas/ArchitectureDiagram";
import { cn } from "@/lib/cn";

function statusTone(status: string): string {
  const s = status.toUpperCase();
  if (s.includes("FAIL") || s.includes("ROLLBACK") || s.includes("DELETE")) return "text-aws-red";
  if (s.includes("PROGRESS")) return "text-aws-orange";
  if (s.includes("COMPLETE")) return "text-aws-green";
  return "text-text-mid";
}

/** A CloudFormation status → topology status (live / creating / planned). */
function topoStatus(cfnStatus: string): ComponentNode["status"] {
  const s = cfnStatus.toUpperCase();
  if (s.includes("PROGRESS")) return "creating";
  if (s.includes("COMPLETE") && !s.includes("DELETE")) return "live";
  return "planned"; // failed / rolled-back / deleted read as not-live
}

/**
 * Build a diagram-node set from the deployed stacks. The always-present pipeline
 * nodes (github → codebuild → cfn) read as live whenever *any* stack exists
 * (something was deployed by the pipeline); leaf nodes reflect their stack's
 * real CloudFormation status. Nodes with no backing stack stay "planned".
 */
function nodesFromStacks(stacks: StackInfo[]): ComponentNode[] {
  const status = new Map<string, ComponentNode["status"]>();
  let any = false;
  for (const st of stacks) {
    const node = nodeForStack(st.name);
    if (!node) continue;
    any = true;
    const ts = topoStatus(st.status);
    // Keep the "furthest along" status if multiple stacks map to one node.
    const prev = status.get(node);
    if (prev !== "live") status.set(node, ts === "live" ? "live" : prev ?? ts);
  }
  const pipeline = any ? "live" : "planned";
  return TOPO_NODES.filter((n) => n.id !== "fsx" || status.has("fsx")).map((n) => {
    const base: ComponentNode = { id: n.id, label: n.label, detail: n.detail, status: "planned" };
    if (n.id === "github" || n.id === "codebuild" || n.id === "cfn") {
      return { ...base, status: pipeline };
    }
    return { ...base, status: status.get(n.id) ?? "planned" };
  });
}

/**
 * Inline "what's already deployed" panel for a returning user. Pulls the live
 * CloudFormation stacks (via the active mock/live client), paints the
 * architecture diagram with each stack's real status, and lists the stacks
 * with their CloudFormation statuses.
 */
export function DeployedStatus() {
  const region = useDeploymentStore((s) => regionFromAz(s.config.availability_zone.name));
  const [state, setState] = useState<{
    loading: boolean;
    error?: string;
    stacks: StackInfo[];
  }>({ loading: true, stacks: [] });
  const [selected, setSelected] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    activeClient()
      .listStacks()
      .then((stacks) => alive && setState({ loading: false, stacks }))
      .catch((err: unknown) => alive && setState({ loading: false, error: (err as Error).message, stacks: [] }));
    return () => {
      alive = false;
    };
  }, []);

  const nodes = nodesFromStacks(state.stacks);
  // Selectable nodes = those backed by at least one deployed stack.
  const deployedNames = new Set(state.stacks.map((s) => s.name));
  const selectableIds = useMemo(
    () => new Set(nodes.filter((n) => stacksForNode(n.id).some((s) => deployedNames.has(s))).map((n) => n.id)),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [state.stacks],
  );

  // The deployed stacks that map to the selected node (for the resource table).
  const selectedStacks = selected
    ? state.stacks.filter((s) => stacksForNode(selected).includes(s.name))
    : [];

  return (
    <motion.div
      initial={{ opacity: 0, y: 8, scale: 0.98 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      transition={{ type: "spring", stiffness: 260, damping: 24 }}
      className="overflow-hidden rounded-2xl border border-white/10 bg-surface-0/60 backdrop-blur-sm"
    >
      <div className="flex items-center gap-1.5 border-b border-white/8 px-3 py-2 text-[10.5px] font-semibold uppercase tracking-widest text-aws-teal">
        <ServerCog className="h-3.5 w-3.5" />
        Currently deployed
        {!state.loading && !state.error && (
          <span className="ml-auto">
            <DiagramLegend show={["live", "creating"]} />
          </span>
        )}
      </div>

      {state.loading ? (
        <div className="flex items-center gap-2 px-3 py-8 text-[11.5px] text-text-lo">
          <Loader2 className="h-3.5 w-3.5 animate-spin" />
          Querying CloudFormation…
        </div>
      ) : state.error ? (
        <div className="px-3 py-8 text-[11.5px] text-aws-red">
          Could not reach CloudFormation: {state.error}
        </div>
      ) : state.stacks.length === 0 ? (
        <div className="px-3 py-8 text-[11.5px] text-text-mid">
          No deployed stacks found — nothing is live in this account yet.
        </div>
      ) : (
        <>
          <div className="relative h-[230px] w-full px-2 py-3">
            <ArchitectureDiagram
              nodes={nodes}
              compact
              region={region}
              selectedId={selected}
              onSelect={(id) => setSelected(id && selectableIds.has(id) ? id : null)}
            />
          </div>
          <div className="flex items-center gap-1.5 border-t border-white/8 px-3 pt-2 text-[10px] text-text-lo">
            <MousePointerClick className="h-3 w-3" />
            Click a service to inspect its resources · click the ↗ badge to open the AWS console
          </div>

          {/* Click-to-inspect resource table for the selected node. */}
          <AnimatePresence initial={false}>
            {selected && (
              <ResourceTable
                key={selected}
                nodeId={selected}
                stacks={selectedStacks}
                region={region}
                onClose={() => setSelected(null)}
              />
            )}
          </AnimatePresence>

          {/* Stack list — each row links to its CloudFormation stack. */}
          <ul className="space-y-1 border-t border-white/8 p-3">
            {state.stacks.map((s) => (
              <li key={s.name}>
                <a
                  href={cloudFormationUrl(region, s.name)}
                  target="_blank"
                  rel="noreferrer"
                  title="Open this stack in the CloudFormation console"
                  className="group flex items-center gap-2 rounded-md border border-white/5 bg-surface-2/40 px-2.5 py-1.5 transition-colors hover:border-aws-blue/40 hover:bg-surface-2/70"
                >
                  <span className="min-w-0 flex-1 truncate font-mono text-[10.5px] text-text-hi" title={s.name}>
                    {s.name}
                  </span>
                  <ExternalLink className="h-3 w-3 shrink-0 text-text-lo group-hover:text-aws-blue" />
                  <span
                    className={cn(
                      "shrink-0 rounded-full border border-white/10 bg-white/5 px-2 py-0.5 text-[9.5px] font-medium uppercase tracking-wide",
                      statusTone(s.status),
                    )}
                  >
                    {s.status}
                  </span>
                </a>
              </li>
            ))}
          </ul>
        </>
      )}
    </motion.div>
  );
}

/**
 * Inline, collapsible resource table for a clicked architecture node. Fetches
 * the deployed resources for that node's stack(s) and renders them as a table,
 * with each stack header linking to the CloudFormation console.
 */
function ResourceTable({
  nodeId,
  stacks,
  region,
  onClose,
}: {
  nodeId: string;
  stacks: StackInfo[];
  region: string;
  onClose: () => void;
}) {
  const label = topoNode(nodeId)?.label ?? nodeId;
  const consoleUrl = serviceConsoleUrl(nodeId, region);
  const [rows, setRows] = useState<{ loading: boolean; error?: string; items: ResourceInfo[] }>({
    loading: true,
    items: [],
  });

  const stackNames = stacks.map((s) => s.name).join("|");
  useEffect(() => {
    let alive = true;
    const names = stacks.map((s) => s.name);
    Promise.all(names.map((n) => activeClient().listResources(n).catch(() => [] as ResourceInfo[])))
      .then((lists) => alive && setRows({ loading: false, items: lists.flat() }))
      .catch((err: unknown) => alive && setRows({ loading: false, error: (err as Error).message, items: [] }));
    return () => {
      alive = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [stackNames]);

  return (
    <motion.div
      initial={{ opacity: 0, height: 0 }}
      animate={{ opacity: 1, height: "auto" }}
      exit={{ opacity: 0, height: 0 }}
      className="overflow-hidden border-t border-white/8 bg-surface-1/40"
    >
      <div className="flex items-center gap-2 px-3 pb-1 pt-2.5">
        <span className="text-[11.5px] font-semibold text-text-hi">{label}</span>
        <span className="text-[10px] text-text-lo">
          {rows.items.length > 0 && `${rows.items.length} resources`}
        </span>
        {consoleUrl && (
          <a
            href={consoleUrl}
            target="_blank"
            rel="noreferrer"
            className="flex items-center gap-1 text-[10px] text-aws-blue hover:underline"
          >
            Open console <ExternalLink className="h-2.5 w-2.5" />
          </a>
        )}
        <button
          type="button"
          onClick={onClose}
          className="ml-auto rounded px-1.5 py-0.5 text-[10px] text-text-lo hover:bg-white/10 hover:text-text-hi"
        >
          Close
        </button>
      </div>

      {rows.loading ? (
        <div className="flex items-center gap-2 px-3 py-4 text-[11px] text-text-lo">
          <Loader2 className="h-3.5 w-3.5 animate-spin" /> Loading resources…
        </div>
      ) : rows.error ? (
        <div className="px-3 py-4 text-[11px] text-aws-red">Could not load resources.</div>
      ) : rows.items.length === 0 ? (
        <div className="px-3 py-4 text-[11px] text-text-lo">No resources reported for this service.</div>
      ) : (
        <div className="max-h-[220px] overflow-y-auto px-3 pb-3">
          <table className="w-full border-collapse text-left text-[11px]">
            <thead>
              <tr className="text-[9.5px] uppercase tracking-wide text-text-lo">
                <th className="py-1 pr-2 font-semibold">Type</th>
                <th className="py-1 pr-2 font-semibold">Physical ID</th>
                <th className="py-1 font-semibold">Status</th>
              </tr>
            </thead>
            <tbody>
              {rows.items.map((r) => (
                <tr key={`${r.logicalId}-${r.physicalId}`} className="border-t border-white/5">
                  <td className="py-1 pr-2 align-top text-text-hi" title={r.type}>
                    {r.type.split("::").slice(-1)[0] || r.type}
                  </td>
                  <td className="py-1 pr-2 align-top">
                    <span className="font-mono text-[10px] text-text-mid" title={`${r.physicalId} (${r.logicalId})`}>
                      {r.physicalId}
                    </span>
                  </td>
                  <td className="py-1 align-top">
                    <span className={cn("text-[9.5px] font-medium uppercase", statusTone(r.status))}>
                      {r.status}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </motion.div>
  );
}
