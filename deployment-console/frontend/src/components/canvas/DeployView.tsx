import { GitBranch, Workflow } from "lucide-react";
import { useDeploymentStore } from "@/store/useDeploymentStore";
import { Timeline } from "@/components/canvas/Timeline";
import { Components } from "@/components/canvas/Components";
import { ConfigSummary } from "@/components/canvas/ConfigSummary";
import { DiagramLegend } from "@/components/canvas/ArchitectureDiagram";

/**
 * The merged Deploy view — Timeline (left) and the live architecture diagram
 * (right) shown side-by-side so the operator watches stacks deploy on the
 * timeline AND light up on the diagram simultaneously. Stacks vertically below
 * lg so it stays usable in a narrow canvas.
 */
export function DeployView() {
  const hasFsx = useDeploymentStore((s) => s.components.some((c) => c.id === "fsx"));
  return (
    <div className="flex h-full min-h-0 flex-col gap-3 lg:flex-row lg:gap-4">
      {/* Timeline rail */}
      <section className="flex min-h-0 flex-col lg:w-[clamp(210px,34%,300px)] lg:shrink-0">
        <PaneHeading icon={<GitBranch className="h-3.5 w-3.5" />} label="Timeline" />
        <div className="min-h-0 flex-1">
          <Timeline />
        </div>
      </section>

      {/* Vertical divider (lg+) */}
      <div className="hidden w-px shrink-0 bg-white/8 lg:block" />

      {/* Config summary + live architecture diagram */}
      <section className="flex min-h-0 flex-1 flex-col">
        <ConfigSummary />
        <PaneHeading
          icon={<Workflow className="h-3.5 w-3.5" />}
          label="Architecture"
          aside={<DiagramLegend />}
        />
        <div className="relative min-h-0 flex-1">
          <Components key={hasFsx ? "fsx" : "nofsx"} />
        </div>
      </section>
    </div>
  );
}

function PaneHeading({
  icon,
  label,
  aside,
}: {
  icon: React.ReactNode;
  label: string;
  aside?: React.ReactNode;
}) {
  return (
    <div className="mb-2 flex shrink-0 items-center justify-between">
      <div className="flex items-center gap-1.5 text-[10.5px] font-semibold uppercase tracking-widest text-aws-teal">
        {icon}
        {label}
      </div>
      {aside}
    </div>
  );
}
