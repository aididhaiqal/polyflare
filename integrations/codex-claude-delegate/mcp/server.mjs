#!/usr/bin/env node
// An MCP tool that delegates a task to Claude, so a Codex agent can get a second opinion from a
// different model family without a wrapper agent standing in between.
//
// Why a tool and not a subagent: a Codex subagent is itself a model, so delegating through one
// means paying a model to type a shell command and relay the answer. A tool is called directly by
// whichever agent wants it — no driver model, no relay, no paraphrase of the delegate's judgment.
//
// Transport is stdio JSON-RPC (newline-delimited), implemented directly so this has no npm
// dependencies to install, pin, or audit.

import { spawn } from "node:child_process";
import { createInterface } from "node:readline";

/** Where `claude` sends its traffic. Pointing at PolyFlare gives the delegate the whole pool:
 * multi-seat balancing, per-model cap steering, session affinity, cooldowns and usage telemetry.
 * Override with POLYFLARE_URL to talk to a different gateway (or unset to go direct to Anthropic). */
const BASE_URL = process.env.POLYFLARE_URL ?? "https://ultraflux.tail6de914.ts.net";

/** Default model. Deliberately Fable rather than Opus: PolyFlare's default translation routes match
 * the substrings `opus`/`sonnet`/`haiku` and would silently reroute those to a Codex model, so a
 * request that LOOKS like Opus can come back as GPT. `claude-fable-5` matches no route and reaches
 * Claude natively, and it draws on its own per-model weekly cap instead of competing with the rest. */
const DEFAULT_MODEL = "claude-fable-5";

/** Models known to be intercepted by the default translation routes — see DEFAULT_MODEL. */
const INTERCEPTED = ["opus", "sonnet", "haiku"];

/** Read-only by default so a delegate can never edit the caller's working tree. Two agents writing
 * to one checkout is the failure mode this avoids; ask for edit tools explicitly if you want them. */
const DEFAULT_TOOLS = ["Read", "Grep", "Glob"];

/** A delegation carries Claude Code's full prompt+tool preamble (~130k tokens), so it is inherently
 * a slow, substantial call. Generous but bounded, so a hung subprocess cannot wedge the agent. */
const TIMEOUT_MS = Number(process.env.CLAUDE_DELEGATE_TIMEOUT_MS ?? 600_000);

const TOOL = {
  name: "ask_claude",
  description:
    "Delegate a substantial analysis task to Claude (a different model family) and return its " +
    "verbatim answer. Use for independent review, architecture critique, subtle correctness or " +
    "concurrency bugs, security analysis, and second opinions on work this agent produced — " +
    "cases where judgement from outside this model family is the point. Each call carries a large " +
    "fixed prompt cost, so send one substantial, self-contained request rather than several small " +
    "ones, and do not use it for tasks this agent can already do. The delegate starts with NO " +
    "shared context: restate the question in full, including the paths it should read.",
  inputSchema: {
    type: "object",
    properties: {
      task: {
        type: "string",
        description:
          "The complete, self-contained request. Include the file paths to read and what to look " +
          "for — the delegate cannot see this conversation.",
      },
      cwd: {
        type: "string",
        description:
          "Absolute path the delegate runs in, so relative paths in `task` resolve. Defaults to " +
          "the server's working directory, which is usually not the caller's project.",
      },
      model: {
        type: "string",
        description:
          `Claude model. Defaults to ${DEFAULT_MODEL}. Note that models whose name contains ` +
          `opus/sonnet/haiku may be intercepted by gateway translation routes and answered by a ` +
          `different provider; the result reports which model actually replied.`,
      },
      allowed_tools: {
        type: "array",
        items: { type: "string" },
        description:
          `Tools the delegate may use. Defaults to ${DEFAULT_TOOLS.join(", ")} (read-only). Add ` +
          `Edit/Write only when you intend the delegate to change files.`,
      },
    },
    required: ["task"],
  },
};

function runClaude({ task, cwd, model, allowedTools }) {
  return new Promise((resolve) => {
    const args = ["-p", task, "--model", model, "--allowedTools", allowedTools.join(" ")];
    const child = spawn("claude", args, {
      cwd: cwd || process.cwd(),
      env: { ...process.env, ANTHROPIC_BASE_URL: BASE_URL },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let out = "";
    let err = "";
    child.stdout.on("data", (d) => (out += d));
    child.stderr.on("data", (d) => (err += d));
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      resolve({ ok: false, text: `delegation timed out after ${TIMEOUT_MS}ms` });
    }, TIMEOUT_MS);
    child.on("error", (e) => {
      clearTimeout(timer);
      resolve({ ok: false, text: `could not run \`claude\`: ${e.message}` });
    });
    child.on("close", (code) => {
      clearTimeout(timer);
      if (code === 0 && out.trim()) resolve({ ok: true, text: out.trim() });
      // Surface the delegate's own error rather than inventing an answer: a caller that silently
      // falls back to its own analysis gets same-family review while believing otherwise.
      else resolve({ ok: false, text: (err.trim() || out.trim() || `claude exited ${code}`) });
    });
  });
}

async function callTool(args) {
  const task = typeof args?.task === "string" ? args.task.trim() : "";
  if (!task) return { ok: false, text: "`task` is required and must be a non-empty string." };
  const model = typeof args?.model === "string" && args.model.trim() ? args.model.trim() : DEFAULT_MODEL;
  const allowedTools = Array.isArray(args?.allowed_tools) && args.allowed_tools.length
    ? args.allowed_tools.map(String)
    : DEFAULT_TOOLS;

  const result = await runClaude({ task, cwd: args?.cwd, model, allowedTools });
  if (!result.ok) return result;

  // Name the model that was ASKED for, plus a warning when it is one the gateway may have
  // rerouted — so a substituted answer can never be mistaken for the model you requested.
  const risky = INTERCEPTED.some((m) => model.toLowerCase().includes(m));
  const header = risky
    ? `[delegated to ${model} — this name matches a gateway translation route, so verify the ` +
      `answer came from Claude and not a substituted provider]`
    : `[delegated to ${model}]`;
  return { ok: true, text: `${header}\n\n${result.text}` };
}

function send(msg) {
  process.stdout.write(JSON.stringify(msg) + "\n");
}

const rl = createInterface({ input: process.stdin });
rl.on("line", async (line) => {
  const text = line.trim();
  if (!text) return;
  let req;
  try {
    req = JSON.parse(text);
  } catch {
    return; // malformed frame: ignore rather than crash the server
  }
  const { id, method, params } = req;
  const reply = (result) => id !== undefined && send({ jsonrpc: "2.0", id, result });

  switch (method) {
    case "initialize":
      reply({
        // Echo the client's protocol version when it names one, so this stays compatible as the
        // spec moves rather than pinning a version the client may not speak.
        protocolVersion: params?.protocolVersion ?? "2025-06-18",
        capabilities: { tools: {} },
        serverInfo: { name: "claude-delegate", version: "1.0.0" },
      });
      break;
    case "tools/list":
      reply({ tools: [TOOL] });
      break;
    case "tools/call": {
      if (params?.name !== TOOL.name) {
        reply({ content: [{ type: "text", text: `unknown tool: ${params?.name}` }], isError: true });
        break;
      }
      const r = await callTool(params?.arguments);
      reply({ content: [{ type: "text", text: r.text }], isError: !r.ok });
      break;
    }
    case "ping":
      reply({});
      break;
    default:
      // Notifications (no id) need no reply; unknown requests get a proper JSON-RPC error.
      if (id !== undefined) {
        send({ jsonrpc: "2.0", id, error: { code: -32601, message: `method not found: ${method}` } });
      }
  }
});
