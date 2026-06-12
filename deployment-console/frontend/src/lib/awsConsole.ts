/**
 * AWS console deep-link helpers. Console URLs don't carry an account id (the
 * console resolves it from the signed-in session), so a region is all we need —
 * derived from the deployment's availability zone.
 *
 * The public GitHub source repo the pipeline clones (shown on the `github` node).
 */
export const SOURCE_REPO_URL =
  "https://github.com/aws-samples/quant-research-sample-using-amazon-ecs-and-aws-batch";

/** us-east-1a → us-east-1. Falls back to us-east-1 if the AZ looks odd. */
export function regionFromAz(az: string | undefined): string {
  if (!az) return "us-east-1";
  // Strip a single trailing AZ letter (…-1a, …-1b); local zones keep their id.
  const m = az.match(/^([a-z]{2}-[a-z]+-\d+)[a-z]$/);
  return m ? m[1] : az;
}

const home = (region: string) => `https://${region}.console.aws.amazon.com`;

/** CloudFormation stacks list (optionally focused on one stack). */
export function cloudFormationUrl(region: string, stackName?: string): string {
  const base = `${home(region)}/cloudformation/home?region=${region}#/stacks`;
  if (!stackName) return base;
  // The console accepts a filtering text query against the stacks list.
  return `${base}?filteringText=${encodeURIComponent(stackName)}&filteringStatus=active`;
}

/**
 * CodeBuild build detail. A real build id is `project:uuid`; we link straight to
 * its build page. The mock's synthetic `dc-build-xxx` id has no real build, so
 * we fall back to the CodeBuild projects list.
 */
export function codeBuildUrl(region: string, buildId: string | undefined): string {
  const projects = `${home(region)}/codesuite/codebuild/projects?region=${region}`;
  if (!buildId) return projects;
  const sep = buildId.indexOf(":");
  if (sep === -1) return projects; // synthetic/mock id — no real build to open
  const project = buildId.slice(0, sep);
  return `${home(region)}/codesuite/codebuild/projects/${encodeURIComponent(
    project,
  )}/build/${encodeURIComponent(buildId)}/?region=${region}`;
}

/**
 * Per-service console landing page for an architecture node. Returns null for
 * nodes with no meaningful console page (the source repo links out to GitHub
 * via SOURCE_REPO_URL instead).
 */
export function serviceConsoleUrl(nodeId: string, region: string): string | null {
  const h = home(region);
  switch (nodeId) {
    case "github":
      return SOURCE_REPO_URL;
    case "codebuild":
      return `${h}/codesuite/codebuild/projects?region=${region}`;
    case "cfn":
      return cloudFormationUrl(region);
    case "vpc":
      return `${h}/vpcconsole/home?region=${region}#vpcs:`;
    case "s3":
      // S3 console is global (no region path), but accepts a region query.
      return `https://s3.console.aws.amazon.com/s3/buckets?region=${region}`;
    case "ecr":
      return `${h}/ecr/repositories?region=${region}`;
    case "batch":
      return `${h}/batch/home?region=${region}#dashboard`;
    case "fsx":
      return `${h}/fsx/home?region=${region}#file-systems`;
    default:
      return null;
  }
}
