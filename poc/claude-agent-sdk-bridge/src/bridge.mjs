import { query } from "@anthropic-ai/claude-agent-sdk";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

const DEFAULT_TIMEOUT_MS = 30_000;
const DUMMY_OAUTH_TOKEN = "polyflare-poc-dummy-oauth-token";

function textContent(content, fieldName) {
  if (typeof content === "string") {
    return content;
  }

  if (Array.isArray(content)) {
    return content
      .map((part) => {
        if (
          part &&
          typeof part === "object" &&
          (part.type === "text" || part.type === "input_text") &&
          typeof part.text === "string"
        ) {
          return part.text;
        }
        throw new Error(`${fieldName} contains an unsupported non-text part`);
      })
      .join("");
  }

  throw new Error(`${fieldName} must be text`);
}

export function translateOpenAIChatRequest(body) {
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw new Error("request body must be an object");
  }
  if (!Array.isArray(body.messages) || body.messages.length === 0) {
    throw new Error("messages must be a non-empty array");
  }
  if (body.stream === true) {
    throw new Error("streaming is outside this POC");
  }
  if (Array.isArray(body.tools) && body.tools.length > 0) {
    throw new Error("external tool bridging is outside this POC");
  }

  const systemParts = [];
  const userMessages = [];

  for (const [index, message] of body.messages.entries()) {
    if (!message || typeof message !== "object") {
      throw new Error(`messages[${index}] must be an object`);
    }

    if (message.role === "system" || message.role === "developer") {
      systemParts.push(textContent(message.content, `messages[${index}].content`));
      continue;
    }

    if (message.role === "user") {
      userMessages.push(textContent(message.content, `messages[${index}].content`));
      continue;
    }

    throw new Error(
      `messages[${index}] role ${JSON.stringify(message.role)} requires session replay, which is outside this POC`,
    );
  }

  if (userMessages.length !== 1) {
    throw new Error("this POC accepts exactly one user message");
  }

  return {
    model:
      typeof body.model === "string" && body.model.length > 0
        ? body.model
        : "claude-sonnet-4-6",
    prompt: userMessages[0],
    systemPrompt:
      systemParts.length > 0
        ? systemParts.join("\n\n")
        : "You are a concise, helpful assistant.",
  };
}

function isolatedChildEnv(upstreamUrl) {
  const childEnv = {};
  for (const key of [
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "PATH",
    "SHELL",
    "TERM",
    "TMP",
    "TMPDIR",
    "TEMP",
    "TZ",
  ]) {
    const value = process.env[key];
    if (typeof value === "string") {
      childEnv[key] = value;
    }
  }

  return {
    ...childEnv,
    ANTHROPIC_BASE_URL: upstreamUrl,
    CLAUDE_CODE_OAUTH_TOKEN: DUMMY_OAUTH_TOKEN,
    CLAUDE_AGENT_SDK_CLIENT_APP: "polyflare-poc/0.1.0",
    CLAUDE_CODE_ENTRYPOINT: "sdk-ts",
    CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: "1",
    DISABLE_AUTOUPDATER: "1",
    DISABLE_BUG_COMMAND: "1",
    DISABLE_ERROR_REPORTING: "1",
    DISABLE_TELEMETRY: "1",
    NO_PROXY: "127.0.0.1,localhost",
    no_proxy: "127.0.0.1,localhost",
  };
}

function validatedLoopbackOrigin(value) {
  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error("the POC upstream must be a valid URL");
  }

  const port = Number(parsed.port);
  if (
    parsed.protocol !== "http:" ||
    parsed.hostname !== "127.0.0.1" ||
    parsed.username !== "" ||
    parsed.password !== "" ||
    parsed.port === "" ||
    !Number.isInteger(port) ||
    port < 1 ||
    port > 65_535 ||
    parsed.pathname !== "/" ||
    parsed.search !== "" ||
    parsed.hash !== ""
  ) {
    throw new Error(
      "the POC upstream must be an explicit http://127.0.0.1:<port> origin without credentials, path, query, or fragment",
    );
  }

  return parsed.origin;
}

function assistantText(message) {
  if (message?.type !== "assistant" || !Array.isArray(message.message?.content)) {
    return "";
  }
  return message.message.content
    .filter((block) => block?.type === "text" && typeof block.text === "string")
    .map((block) => block.text)
    .join("");
}

export async function bridgeOpenAIChatRequest(
  body,
  {
    claudeExecutable,
    upstreamUrl,
    timeoutMs = DEFAULT_TIMEOUT_MS,
    scratchParent = tmpdir(),
  } = {},
) {
  if (!claudeExecutable) {
    throw new Error("claudeExecutable is required");
  }
  const loopbackOrigin = validatedLoopbackOrigin(upstreamUrl);

  const translated = translateOpenAIChatRequest(body);
  const scratchRoot = await mkdtemp(
    path.join(scratchParent, "polyflare-agent-sdk-poc-"),
  );
  const configDir = path.join(scratchRoot, "claude-config");
  const workspaceDir = path.join(scratchRoot, "workspace");
  await Promise.all([
    mkdir(configDir, { recursive: true }),
    mkdir(workspaceDir, { recursive: true }),
  ]);
  const abortController = new AbortController();
  const timeout = setTimeout(() => abortController.abort(), timeoutMs);

  let outputText = "";
  let usage;
  let sessionId;
  let queryHandle;

  try {
    queryHandle = query({
      prompt: translated.prompt,
      options: {
        abortController,
        cwd: workspaceDir,
        env: {
          ...isolatedChildEnv(loopbackOrigin),
          CLAUDE_CONFIG_DIR: configDir,
        },
        includePartialMessages: false,
        maxTurns: 1,
        model: translated.model,
        pathToClaudeCodeExecutable: claudeExecutable,
        permissionMode: "dontAsk",
        persistSession: false,
        settingSources: [],
        systemPrompt: translated.systemPrompt,
        tools: [],
      },
    });

    for await (const message of queryHandle) {
      if (typeof message?.session_id === "string") {
        sessionId = message.session_id;
      }
      if (message?.type === "assistant") {
        outputText += assistantText(message);
        usage = message.message?.usage ?? usage;
      }
      if (message?.type === "result") {
        usage = message.usage ?? usage;
        if (message.is_error) {
          throw new Error(
            `Claude Agent SDK failed with ${message.subtype ?? "an unknown result"}`,
          );
        }
      }
    }
  } finally {
    clearTimeout(timeout);
    queryHandle?.close();
    await rm(scratchRoot, { recursive: true, force: true });
  }

  if (!outputText) {
    throw new Error("Claude Agent SDK completed without assistant text");
  }

  return {
    id: `chatcmpl_${sessionId ?? "poc"}`,
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model: translated.model,
    choices: [
      {
        index: 0,
        message: {
          role: "assistant",
          content: outputText,
        },
        finish_reason: "stop",
      },
    ],
    usage: usage
      ? {
          prompt_tokens: usage.input_tokens ?? 0,
          completion_tokens: usage.output_tokens ?? 0,
          total_tokens:
            (usage.input_tokens ?? 0) + (usage.output_tokens ?? 0),
        }
      : undefined,
  };
}
