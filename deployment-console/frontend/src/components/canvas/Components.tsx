import { useDeploymentStore } from "@/store/useDeploymentStore";
import { ArchitectureDiagram } from "@/components/canvas/ArchitectureDiagram";
import { regionFromAz } from "@/lib/awsConsole";

/**
 * Canvas "Components" panel — the full interactive architecture diagram with
 * hover tooltips, per-node resource drill-in, and AWS-console deep-links.
 * Layout/wiring live in `lib/topology.ts`; the diagram is the shared
 * ArchitectureDiagram.
 */
export function Components() {
  const components = useDeploymentStore((s) => s.components);
  const region = useDeploymentStore((s) => regionFromAz(s.config.availability_zone.name));
  return <ArchitectureDiagram nodes={components} region={region} />;
}
