# UI / UX Design Spec — Deployment Console

> **STATUS: ✅ BUILT (2026-06-09).** Implemented in `frontend/`, builds clean
> (`npm run build`), verified via Playwright with zero console errors. All six signature
> animation moments confirmed on screen — see `design/screenshots/`. Default Mock mode
> tells the full story (~28s); Live mode wired behind the header toggle (needs the agent
> runtime re-created + the `frontend/bridge/` proxy running).
>
> The authoritative brief for the frontend. Build target: a **beautiful, smooth,
> professional, AWS-themed** chat-driven deployment console with tasteful, high-impact
> animations. Concise over cluttered. This spec is the contract subagents build against.

## 0. Product in one sentence
An operator talks to an agent in a chat pane; on the right, a **live deployment canvas**
animates the configuration forming, the deployment progressing through phases, the
components lighting up, and logs streaming — turning an invisible `cdk deploy` into a
cinematic, legible experience.

## 1. Tech stack (decided)
- **Vite + React 19 + TypeScript**
- **Tailwind CSS v4** for styling + design tokens
- **Framer Motion** (`motion`) — the primary animation engine (layout, springs, orchestration)
- **shadcn/ui** primitives (Radix under the hood) for accessible, clean components
- **lucide-react** icons; **@number-flow/react** for animated numbers (or a small custom counter)
- **canvas-confetti** for the success moment (used sparingly, once)
- State: lightweight **zustand** store. No router needed (single view).
- Data layer abstraction: a `DeploymentClient` interface with **two implementations** —
  `MockClient` (default, scripted realistic replay) and `LiveClient` (calls the local
  bridge → AgentCore). A toggle in the header switches modes.

## 2. AWS theme (color system)
Use official AWS palette. Define as CSS variables / Tailwind tokens:

| Token | Hex | Use |
|---|---|---|
| `--aws-squid-ink` | `#232F3E` | primary dark surface / text on light |
| `--aws-anchor` | `#161E2D` | deepest background |
| `--aws-orange` | `#FF9900` | primary accent / CTA / active state |
| `--aws-orange-2` | `#EC7211` | hover/pressed orange |
| `--aws-blue` | `#2074D5` / `#0972D3` | links, info, secondary accent |
| `--aws-teal` | `#01A88D` | AI/agent accent (AgentCore domain color) |
| `--aws-green` | `#1D8102` / `#2BC253` | success |
| `--aws-red` | `#D13212` | error/failure |
| `--aws-slate` | `#5F6B7A` | muted text/borders |
| surfaces | `#0F1B2D`→`#1A2535` gradient | dark-mode canvas |

**Default to a dark theme** (looks premium, makes orange/teal pop). Provide a light theme
token set too, but ship dark as default. Backgrounds: subtle radial/linear gradients +
faint grid or noise texture — never flat black.

## 3. Layout — split shell
```
┌──────────────────────────────────────────────────────────────┐
│  HEADER: ◆ logo "Deployment Console"   [mode: Mock|Live]  ◐    │
├───────────────────────────┬──────────────────────────────────┤
│  CHAT PANE (380–460px)    │  DEPLOYMENT CANVAS (flex-1)        │
│  • message stream         │  Tabs: Timeline · Components ·     │
│  • agent/tool/user bubbles│        Config · Logs               │
│  • tool-call chips        │  (animated content per tab)        │
│  • composer + presets     │                                    │
└───────────────────────────┴──────────────────────────────────┘
```
Responsive: below ~900px, canvas collapses into a bottom sheet / tab below chat.

## 4. The four canvas views (where the "wow" lives)

### 4a. Timeline (default)
The CodeBuild + CFN journey as an **animated vertical stepper**:
`Submitted → Queued → Provisioning → Download Source → Install → Pre-build → Build (cdk deploy) → Post-build → Done`.
- Each step: pending (dim) → active (orange pulse + animated spinner ring) → done (green check, spring pop).
- A progress "comet"/gradient line travels down the connector as phases complete.
- Live elapsed timer with animated digits. ETA hint.
- On the BUILD step, expand to show the **stack sub-progress** (network → s3 → pipeline → batch) as nested mini-steps.

### 4b. Components (topology)
An **animated architecture graph** that builds itself as the config is decided and as
resources come up:
- Nodes: GitHub → CodeBuild → CloudFormation → {VPC, S3, ECR, Batch (+FSx if toggled)}.
- Nodes fade/scale in with stagger; edges draw with animated `pathLength`.
- A node is grey (planned) → orange (creating, pulsing) → teal/green (live).
- Use official-style AWS service glyphs (simple SVG squares with the service initial/icon
  in domain colors) — clean, not clip-art.
- Hover a node → tooltip with the resource detail (ARN/id from outputs).

### 4c. Config
The `parameters` object as a **beautiful diff/spec card**, not raw JSON:
- Toggles (deployment_type, FSx, S3 Express, CodePipeline) as animated pills/switches.
- When the agent changes config, the changed field **highlights and counts/morphs** to the new value.
- Show "X stacks will deploy" derived from toggles, updating with a number-flow animation.
- A subtle "validated ✓" badge animates in when validate passes.

### 4d. Logs
A **terminal-style** streaming panel:
- Monospace, dark, AWS-orange prompt accents.
- Lines stream in with a fast typewriter/opacity cascade (batched, not per-char laggy).
- Auto-scroll with a "jump to latest" pill when scrolled up.
- Phase headers as dividers. Errors in red, success in green.

## 5. Chat pane details
- **Message types:** user, agent (markdown), tool-call chip (e.g. `⚙ validate_config` →
  expandable result), system/status.
- Tool-call chips animate: collapsed pill → expand to show structured result; a small
  "running" shimmer while in-flight.
- **Composer:** textarea + send; **preset quick-chips** above it ("Deploy CPU batch",
  "Add GPU training", "Full platform + FSx", "Check status"). Clicking fills/sends.
- Agent messages stream token-by-token (SSE in live mode; simulated in mock).
- Confirmation moments (before deploy) render a distinct **confirm card** with Deploy/Cancel.

## 6. Signature animation moments (make these excellent)
1. **App entrance** — header + panes stagger in; logo draws.
2. **Config forming** — when the agent proposes a config, fields cascade into the Config card.
3. **Deploy ignition** — pressing/confirming Deploy triggers a satisfying transition: the
   canvas auto-switches to Timeline, the first step ignites, a soft orange shockwave.
4. **Phase completion** — spring check-pops + the comet advancing.
5. **Success** — final step completes → components all turn green → **one** confetti burst
   (AWS orange/teal/green), a "Deployment complete" hero badge, elapsed time.
6. **Failure** — red pulse, the failed step shakes subtly, log auto-jumps to the error.

Animation principles: spring physics (not linear), 150–400ms, stagger 40–80ms, respect
`prefers-reduced-motion` (degrade to fades). 60fps — animate transform/opacity only.

## 7. Data contract (matches the validated agent)
The `DeploymentClient` interface (both Mock + Live implement):
```ts
interface DeploymentClient {
  sendMessage(text: string, onToken: (t: string) => void): Promise<AgentTurn>;
  // AgentTurn includes: text, toolCalls[] ({name, input, output}), and any buildId discovered
  getStatus(buildId: string): Promise<BuildStatus>; // {buildStatus, currentPhase, phases[]}
  getLogs(buildId: string): Promise<string[]>;
}
```
- **Tool names** mirror the agent: `validate_config`, `start_deployment`, `get_deployment_status`.
- **Phases** mirror CodeBuild: SUBMITTED, QUEUED, PROVISIONING, DOWNLOAD_SOURCE, INSTALL,
  PRE_BUILD, BUILD, POST_BUILD, COMPLETED + status SUCCEEDED/FAILED/IN_PROGRESS.
- **Config override** shape mirrors `schema/config.schema.json` (deployment_type, app_with_fsx, etc.).

### MockClient (default — ship this working)
Scripted, realistic, time-compressed replay so the whole UX is demoable offline:
- Understands a few intents (deploy CPU / add GPU / full+FSx / check status).
- Streams agent text, emits tool-call chips, drives phase progression on a timer
  (~total 25–40s, phases weighted realistically), then success (or a failure script).

### LiveClient (wire it, document it)
Calls a **local bridge** (see §8). Same interface. Behind the mode toggle.

## 8. Live bridge (thin, optional for the POC demo)
A tiny **Node/Express** server in `frontend/bridge/` (or `deployment-console/bridge/`):
- Holds AWS creds via the local profile (`dialseny-burner-1`) — **never in the browser**.
- `POST /api/message` → `bedrock-agentcore.invoke_agent_runtime` (SSE passthrough).
- `GET /api/status?buildId=` → `codebuild.batch_get_builds`.
- CORS to the Vite dev origin. Reads `AGENT_RUNTIME_ARN` from env.
- Documented as the path to flip Mock→Live once the agent runtime is re-created
  (it was torn down after validation; re-create via `agent/` + `platform-infra/`).

## 9. Quality bar / definition of done
- `npm run build` passes clean (TS strict, no errors).
- Runs with `npm run dev`; **Mock mode tells the full story** end-to-end with all six
  signature moments visible.
- Looks genuinely premium: cohesive AWS dark theme, consistent spacing/typography
  (Inter or system; mono for logs), no layout jank, 60fps animations.
- Accessible: keyboard send, focus states, `prefers-reduced-motion` honored, AA contrast.
- Concise: no dead UI, no lorem, no clutter. Every element earns its place.

## 10. File layout (target)
```
frontend/
  index.html
  package.json  vite.config.ts  tsconfig.json  tailwind config
  src/
    main.tsx  App.tsx
    theme/aws-tokens.css
    store/useDeploymentStore.ts
    lib/client/{types.ts, mockClient.ts, liveClient.ts}
    lib/phases.ts   lib/presets.ts
    components/
      Header.tsx
      chat/{ChatPane.tsx, Message.tsx, ToolCallChip.tsx, Composer.tsx, ConfirmCard.tsx, PresetChips.tsx}
      canvas/{Canvas.tsx, Timeline.tsx, Components.tsx, ConfigView.tsx, Logs.tsx}
      fx/{Confetti.tsx, AnimatedNumber.tsx, GradientBackdrop.tsx}
  bridge/  (Express live proxy, optional)
  README.md  (run instructions + Mock/Live)
```

Keep components small and focused. Comment only where intent isn't obvious.
