# Deployment Console — Frontend

A chat-driven, AWS-themed deployment console. An operator talks to an agent on the
left; a live deployment canvas on the right animates the config forming, the CodeBuild
phases progressing, the architecture lighting up, and logs streaming.

Built per `../design/UI_DESIGN_SPEC.md`, mirroring the validated agent in
`../design/VALIDATION_REPORT.md`.

## Run

```bash
npm install        # already installed in the scaffold
npm run dev        # http://localhost:5173
npm run build      # tsc -b && vite build (passes clean)
```

## Mock vs Live (header toggle)

- **Mock** (default): fully offline, scripted, time-compressed replay (~28s) that tells
  the whole story — validate → confirm → deploy → phases → success (or failure). Try the
  preset chips: *Deploy CPU batch*, *Add GPU training*, *Full platform + FSx*,
  *Check status*. Include the word **fail** in a prompt to see the failure moment.
- **Live**: `LiveClient` calls a local bridge at `/api/*` (proxied by Vite to `:8787`).

### Live bridge (optional)

`bridge/server.mjs` is a thin Node proxy that holds AWS creds (never the browser) and
calls AgentCore + CodeBuild. The AgentCore runtime was torn down after validation —
re-create it via `../agent/` + `../platform-infra/`, then:

```bash
export AGENT_RUNTIME_ARN=arn:aws:bedrock-agentcore:us-east-1:ACCOUNT:runtime/...
export AWS_REGION=us-east-1
node bridge/server.mjs      # needs @aws-sdk/client-bedrock-agentcore + client-codebuild + client-cloudwatch-logs
```

Endpoints: `POST /api/message` (SSE), `GET /api/status?buildId=`, `GET /api/logs?buildId=`.

## Layout

- `lib/client/` — `DeploymentClient` interface + `MockClient` (scripted) + `LiveClient` (bridge).
- `lib/driver.ts` — orchestrates a turn: streams tokens, emits tool chips, drives phase
  progression / stack sub-steps / component lighting / logs on timers.
- `store/useDeploymentStore.ts` — zustand state for the whole flow.
- `components/chat/` — chat pane, messages, tool-call chips, confirm card, composer, presets.
- `components/canvas/` — Timeline, Components topology, Config, Logs + tab host.
- `components/fx/` — Confetti, AnimatedNumber, GradientBackdrop.

Animations honor `prefers-reduced-motion` (degrade to fades).
