// Live bridge — thin Node proxy holding AWS creds (never the browser).
// Run: AGENT_RUNTIME_ARN=... node bridge/server.mjs
// Requires the AWS SDK v3 packages installed in this dir (not bundled with the
// frontend). This is the documented path to flip Mock → Live once the AgentCore
// runtime is re-created (it was torn down after validation; rebuild via agent/ +
// platform-infra/). The frontend's LiveClient calls /api/* through the Vite proxy.

import http from "node:http";
import { randomUUID } from "node:crypto";

const PORT = process.env.BRIDGE_PORT ? Number(process.env.BRIDGE_PORT) : 8787;
const REGION = process.env.AWS_REGION || "us-east-1";
const AGENT_RUNTIME_ARN = process.env.AGENT_RUNTIME_ARN; // required for live invokes

// Lazy import so the file parses even without the SDK installed.
async function awsClients() {
  const { BedrockAgentCoreClient, InvokeAgentRuntimeCommand } = await import(
    "@aws-sdk/client-bedrock-agentcore"
  );
  const { CodeBuildClient, BatchGetBuildsCommand } = await import("@aws-sdk/client-codebuild");
  const { CloudWatchLogsClient, GetLogEventsCommand } = await import(
    "@aws-sdk/client-cloudwatch-logs"
  );
  return {
    agentcore: new BedrockAgentCoreClient({ region: REGION }),
    codebuild: new CodeBuildClient({ region: REGION }),
    logs: new CloudWatchLogsClient({ region: REGION }),
    InvokeAgentRuntimeCommand,
    BatchGetBuildsCommand,
    GetLogEventsCommand,
  };
}

function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
}

function send(res, code, obj) {
  cors(res);
  res.writeHead(code, { "Content-Type": "application/json" });
  res.end(JSON.stringify(obj));
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  if (req.method === "OPTIONS") {
    cors(res);
    res.writeHead(204);
    return res.end();
  }

  try {
    if (req.method === "POST" && url.pathname === "/api/message") {
      const body = await readJson(req);
      if (!AGENT_RUNTIME_ARN) return send(res, 500, { error: "AGENT_RUNTIME_ARN not set" });
      const { agentcore, InvokeAgentRuntimeCommand } = await awsClients();
      cors(res);
      res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache" });
      const out = await agentcore.send(
        new InvokeAgentRuntimeCommand({
          agentRuntimeArn: AGENT_RUNTIME_ARN,
          runtimeSessionId: `dc-bridge-${randomUUID()}`,
          payload: new TextEncoder().encode(JSON.stringify({ prompt: body.text })),
        }),
      );
      // Stream SSE chunks as token events.
      for await (const chunk of out.response ?? []) {
        const text = new TextDecoder().decode(chunk);
        res.write(`data: ${JSON.stringify({ type: "token", token: text })}\n\n`);
      }
      res.write(`data: ${JSON.stringify({ type: "done" })}\n\n`);
      return res.end();
    }

    if (req.method === "GET" && url.pathname === "/api/status") {
      const buildId = url.searchParams.get("buildId");
      if (!buildId) return send(res, 400, { error: "buildId required" });
      const { codebuild, BatchGetBuildsCommand } = await awsClients();
      const data = await codebuild.send(new BatchGetBuildsCommand({ ids: [buildId] }));
      const b = data.builds?.[0];
      return send(res, 200, {
        buildId,
        buildStatus: b?.buildStatus ?? "IN_PROGRESS",
        currentPhase: b?.currentPhase ?? "BUILD",
        phases: (b?.phases ?? []).map((p) => ({
          name: p.phaseType,
          status: p.phaseStatus === "SUCCEEDED" ? "SUCCEEDED" : p.phaseStatus === "FAILED" ? "FAILED" : "IN_PROGRESS",
          durationSec: p.durationInSeconds,
        })),
      });
    }

    if (req.method === "GET" && url.pathname === "/api/logs") {
      const buildId = url.searchParams.get("buildId");
      if (!buildId) return send(res, 400, { error: "buildId required" });
      const { codebuild, logs, BatchGetBuildsCommand, GetLogEventsCommand } = await awsClients();
      const data = await codebuild.send(new BatchGetBuildsCommand({ ids: [buildId] }));
      const loc = data.builds?.[0]?.logs ?? {};
      const group = loc.groupName;
      const stream = loc.streamName;
      const prevToken = url.searchParams.get("nextToken") ?? undefined;
      // Log location only exists once the build reaches PROVISIONING — until then,
      // return empty (not an error) so the poller keeps trying.
      if (!group || !stream) return send(res, 200, { lines: [], nextToken: prevToken });
      const ev = await logs.send(
        new GetLogEventsCommand({
          logGroupName: group,
          logStreamName: stream,
          startFromHead: true,
          limit: 1000,
          nextToken: prevToken,
        }),
      );
      const lines = (ev.events ?? []).map((e) => (e.message ?? "").replace(/\n$/, ""));
      return send(res, 200, { lines, nextToken: ev.nextForwardToken });
    }

    return send(res, 404, { error: "not found" });
  } catch (err) {
    return send(res, 500, { error: String(err?.message ?? err) });
  }
});

function readJson(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (c) => (data += c));
    req.on("end", () => {
      try {
        resolve(data ? JSON.parse(data) : {});
      } catch (e) {
        reject(e);
      }
    });
    req.on("error", reject);
  });
}

server.listen(PORT, () => {
  console.log(`[bridge] listening on http://localhost:${PORT} (region ${REGION})`);
  if (!AGENT_RUNTIME_ARN) console.log("[bridge] AGENT_RUNTIME_ARN unset — /api/message will 500 until provided");
});
