import type { ComponentStatus } from "@/store/useDeploymentStore";

/** Per-service AWS-style square glyph: gradient tile + a drawn mark + initials. */
const GLYPH: Record<string, { from: string; to: string; mark: "git" | "build" | "stack" | "net" | "bucket" | "registry" | "compute" | "fs" }> = {
  github: { from: "#3b4453", to: "#232f3e", mark: "git" },
  codebuild: { from: "#2074D5", to: "#0d4ea8", mark: "build" },
  cfn: { from: "#E7157B", to: "#a30d57", mark: "stack" },
  vpc: { from: "#8C4FFF", to: "#5b2bb5", mark: "net" },
  s3: { from: "#1B660F", to: "#0f4008", mark: "bucket" },
  ecr: { from: "#E7157B", to: "#a30d57", mark: "registry" },
  batch: { from: "#ED7100", to: "#b45600", mark: "compute" },
  fsx: { from: "#1B660F", to: "#0f4008", mark: "fs" },
};

const STATUS_RING: Record<ComponentStatus, string> = {
  planned: "rgba(255,255,255,0.18)",
  creating: "var(--color-aws-orange)",
  live: "var(--color-aws-green)",
};

function Mark({ kind }: { kind: string }) {
  const s = { fill: "none", stroke: "white", strokeWidth: 1.6, strokeLinecap: "round" as const, strokeLinejoin: "round" as const };
  switch (kind) {
    case "git":
      return <path d="M12 16 v8 m0 0 a3 3 0 1 0 0.01 0 M12 16 a3 3 0 1 0 0.01 0 M24 18 a3 3 0 1 0 0.01 0 M24 21 v3 a4 4 0 0 1-4 4 h-2" {...s} />;
    case "build":
      return <path d="M14 14 l-5 4 5 4 M22 14 l5 4-5 4 M19 12 l-2 12" {...s} />;
    case "stack":
      return <path d="M9 14 l9-4 9 4-9 4z M9 19 l9 4 9-4 M9 24 l9 4 9-4" {...s} />;
    case "net":
      return <path d="M18 10 a8 8 0 1 0 0.01 0 M10 18 h16 M18 10 c-4 4-4 12 0 16 M18 10 c4 4 4 12 0 16" {...s} />;
    case "bucket":
      return <path d="M10 13 h16 l-1.5 12 a2 2 0 0 1-2 2 h-9 a2 2 0 0 1-2-2z M10 13 a8 2 0 0 0 16 0" {...s} />;
    case "registry":
      return <path d="M10 22 h4v4h-4z M16 22 h4v4h-4z M22 22 h-1 M13 16 h10 v2 h-10z M11 22 v-2 h14 v2" {...s} />;
    case "compute":
      return <path d="M13 13 h10 v10 h-10z M15 10 v3 M19 10 v3 M23 10 v3 M15 23 v3 M19 23 v3 M10 15 h3 M10 19 h3 M23 15 h3 M23 19 h3" {...s} />;
    case "fs":
      return <path d="M11 12 h9 l4 4 v9 a1 1 0 0 1-1 1 h-12 a1 1 0 0 1-1-1z M12 20 h12 M12 24 h12" {...s} />;
    default:
      return null;
  }
}

export function ServiceGlyph({
  id,
  status,
  size = 36,
}: {
  id: string;
  status: ComponentStatus;
  size?: number;
}) {
  const g = GLYPH[id] ?? GLYPH.cfn;
  const gid = `grad-${id}`;
  return (
    <svg width={size} height={size} viewBox="0 0 36 36">
      <defs>
        <linearGradient id={gid} x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor={g.from} />
          <stop offset="100%" stopColor={g.to} />
        </linearGradient>
      </defs>
      <rect
        x="2"
        y="2"
        width="32"
        height="32"
        rx="7"
        fill={`url(#${gid})`}
        stroke={STATUS_RING[status]}
        strokeWidth="2"
        opacity={status === "planned" ? 0.55 : 1}
      />
      <Mark kind={g.mark} />
    </svg>
  );
}
