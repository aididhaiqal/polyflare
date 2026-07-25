import { createHash } from "node:crypto";
import { createServer } from "node:http";

const MAX_BODY_BYTES = 2 * 1024 * 1024;

function hashText(value) {
  return createHash("sha256").update(value).digest("hex");
}

function summarizeContent(content) {
  const blocks = typeof content === "string" ? [{ type: "text", text: content }] : content;
  if (!Array.isArray(blocks)) {
    return [{ type: typeof content }];
  }

  return blocks.map((block) => {
    if (!block || typeof block !== "object") {
      return { type: typeof block };
    }
    if (typeof block.text === "string") {
      return {
        type: block.type ?? "text",
        textBytes: Buffer.byteLength(block.text),
        textSha256: hashText(block.text),
      };
    }
    return { type: block.type ?? "object" };
  });
}

function summarizeBody(body) {
  const system = Array.isArray(body.system) ? body.system : [];
  const messages = Array.isArray(body.messages) ? body.messages : [];
  const tools = Array.isArray(body.tools) ? body.tools : [];

  return {
    topLevelKeys: Object.keys(body).sort(),
    model: body.model,
    stream: body.stream,
    system: system.map((block) => summarizeContent([block])[0]),
    messages: messages.map((message) => ({
      role: message?.role,
      content: summarizeContent(message?.content),
    })),
    toolNames: tools
      .map((tool) => tool?.name)
      .filter((name) => typeof name === "string"),
    thinkingType: body.thinking?.type,
    metadataKeys:
      body.metadata && typeof body.metadata === "object"
        ? Object.keys(body.metadata).sort()
        : [],
  };
}

function summarizeHeaders(headers) {
  const summary = {};
  for (const [key, value] of Object.entries(headers)) {
    const normalized = key.toLowerCase();
    if (normalized === "authorization") {
      summary.authorization = value?.startsWith("Bearer ")
        ? "<redacted-bearer>"
        : "<redacted>";
      continue;
    }
    if (
      normalized === "accept" ||
      normalized === "anthropic-beta" ||
      normalized === "anthropic-dangerous-direct-browser-access" ||
      normalized === "anthropic-version" ||
      normalized === "content-type" ||
      normalized === "user-agent" ||
      normalized === "x-app" ||
      normalized === "x-client-app" ||
      normalized === "x-claude-code-session-id" ||
      normalized === "x-client-request-id" ||
      normalized.startsWith("x-stainless-")
    ) {
      summary[normalized] = value;
    }
  }
  return summary;
}

function sseEvent(event, data) {
  return `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
}

function fakeMessageStream(model, responseText) {
  return [
    sseEvent("message_start", {
      type: "message_start",
      message: {
        id: "msg_polyflare_poc",
        type: "message",
        role: "assistant",
        model,
        content: [],
        stop_reason: null,
        stop_sequence: null,
        usage: {
          input_tokens: 7,
          cache_creation_input_tokens: 0,
          cache_read_input_tokens: 0,
          output_tokens: 0,
        },
      },
    }),
    sseEvent("content_block_start", {
      type: "content_block_start",
      index: 0,
      content_block: { type: "text", text: "" },
    }),
    sseEvent("content_block_delta", {
      type: "content_block_delta",
      index: 0,
      delta: { type: "text_delta", text: responseText },
    }),
    sseEvent("content_block_stop", {
      type: "content_block_stop",
      index: 0,
    }),
    sseEvent("message_delta", {
      type: "message_delta",
      delta: { stop_reason: "end_turn", stop_sequence: null },
      usage: { output_tokens: 4 },
    }),
    sseEvent("message_stop", { type: "message_stop" }),
  ].join("");
}

async function requestBody(request) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > MAX_BODY_BYTES) {
      throw new Error("request body exceeds POC limit");
    }
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf8");
}

export async function startFakeAnthropic({
  holdResponse = false,
  responseText = "POC bridge response",
} = {}) {
  const captures = [];
  let releaseHeldResponse;
  const heldResponse = new Promise((resolve) => {
    releaseHeldResponse = resolve;
  });
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, "http://127.0.0.1");
      if (request.method !== "POST" || url.pathname !== "/v1/messages") {
        response.writeHead(404, { "content-type": "application/json" });
        response.end(JSON.stringify({ error: { message: "not found" } }));
        return;
      }

      const body = JSON.parse(await requestBody(request));
      captures.push({
        method: request.method,
        path: `${url.pathname}${url.search}`,
        headerNames: Object.keys(request.headers).sort(),
        headers: summarizeHeaders(request.headers),
        body: summarizeBody(body),
      });

      if (holdResponse) {
        await heldResponse;
      }

      response.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
      });
      response.end(fakeMessageStream(body.model, responseText));
    } catch (error) {
      response.writeHead(400, { "content-type": "application/json" });
      response.end(
        JSON.stringify({
          error: {
            type: "invalid_request_error",
            message: error instanceof Error ? error.message : String(error),
          },
        }),
      );
    }
  });

  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });

  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("fake Anthropic server did not bind a TCP port");
  }

  return {
    baseUrl: `http://127.0.0.1:${address.port}`,
    captures,
    close: () => {
      releaseHeldResponse();
      return new Promise((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    },
  };
}
