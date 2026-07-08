/** Quick-chip presets shown above the composer. Clicking sends the prompt. */
export interface Preset {
  label: string;
  prompt: string;
}

export const PRESETS: Preset[] = [
  {
    label: "Deploy CPU batch",
    prompt: "Deploy a single-node CPU batch environment, no FSx.",
  },
  {
    label: "Add GPU training",
    prompt: "Add a multi-node GPU training environment to the deployment.",
  },
  {
    label: "Full platform + FSx",
    prompt: "Deploy the full platform with FSx for Lustre and CodePipeline.",
  },
  {
    label: "Explain architecture",
    prompt: "Explain the solution architecture and its components.",
  },
  {
    label: "What's deployed?",
    prompt: "What is already deployed in this account?",
  },
  {
    label: "Check status",
    prompt: "How's the deployment going?",
  },
];
