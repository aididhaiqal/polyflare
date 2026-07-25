import assert from "node:assert/strict";
import { access, mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import {
  bridgeOpenAIChatRequest,
  translateOpenAIChatRequest,
} from "../src/bridge.mjs";
import { startFakeAnthropic } from "../src/fake-anthropic.mjs";

test("translates one system message and one user message", () => {
  assert.deepEqual(
    translateOpenAIChatRequest({
      model: "claude-sonnet-4-6",
      messages: [
        { role: "system", content: "Be concise." },
        { role: "user", content: "Hello." },
      ],
    }),
    {
      model: "claude-sonnet-4-6",
      prompt: "Hello.",
      systemPrompt: "Be concise.",
    },
  );
});

test("rejects histories and tools that need a stateful bridge", () => {
  assert.throws(
    () =>
      translateOpenAIChatRequest({
        messages: [
          { role: "user", content: "Hello." },
          { role: "assistant", content: "Hi." },
        ],
      }),
    /session replay/,
  );
  assert.throws(
    () =>
      translateOpenAIChatRequest({
        messages: [{ role: "user", content: "Hello." }],
        tools: [{ type: "function", function: { name: "lookup" } }],
      }),
    /tool bridging/,
  );
});

test("rejects loopback-looking URLs whose actual host is external", async () => {
  await assert.rejects(
    bridgeOpenAIChatRequest(
      {
        messages: [{ role: "user", content: "Hello." }],
      },
      {
        claudeExecutable: "/unused/for-validation",
        upstreamUrl: "http://127.0.0.1:65535@192.0.2.1/",
      },
    ),
    /explicit http:\/\/127\.0\.0\.1/,
  );
});

test("routes a non-native message through the actual Claude Agent SDK runtime", async (t) => {
  const claudeExecutable = process.env.CLAUDE_EXECUTABLE;
  if (!claudeExecutable) {
    t.skip("CLAUDE_EXECUTABLE is not set");
    return;
  }
  await access(claudeExecutable);

  const fake = await startFakeAnthropic();
  try {
    const response = await bridgeOpenAIChatRequest(
      {
        model: "claude-sonnet-4-6",
        messages: [
          { role: "system", content: "Be concise." },
          { role: "user", content: "Local POC message." },
        ],
      },
      {
        claudeExecutable,
        upstreamUrl: fake.baseUrl,
      },
    );

    assert.equal(
      response.choices[0].message.content,
      "POC bridge response",
    );
    assert.equal(response.usage.completion_tokens, 4);
    assert.equal(fake.captures.length, 1);

    const [capture] = fake.captures;
    assert.equal(capture.method, "POST");
    assert.equal(capture.path, "/v1/messages?beta=true");
    assert.equal(capture.headers.authorization, "<redacted-bearer>");
    assert.match(
      capture.headers["user-agent"],
      /^claude-cli\/2\.1\.218 \(external, sdk-ts, agent-sdk\/0\.3\.218,/,
    );
    assert.equal(capture.headers["x-client-app"], "polyflare-poc/0.1.0");
    assert.equal(
      capture.headers["x-stainless-package-version"],
      "0.94.0",
    );
    assert.match(capture.headers["anthropic-beta"], /oauth-2025-04-20/);
    assert.equal(capture.body.stream, true);
    assert.deepEqual(capture.body.toolNames, []);
    assert.ok(capture.body.system.length >= 2);
    assert.deepEqual(
      capture.body.messages.map((message) => message.role),
      ["user"],
    );
  } finally {
    await fake.close();
  }
});

test("aborts a stalled SDK request and removes its scratch directory", async (t) => {
  const claudeExecutable = process.env.CLAUDE_EXECUTABLE;
  if (!claudeExecutable) {
    t.skip("CLAUDE_EXECUTABLE is not set");
    return;
  }
  await access(claudeExecutable);

  const scratchParent = await mkdtemp(
    path.join(tmpdir(), "polyflare-agent-sdk-poc-test-"),
  );
  const fake = await startFakeAnthropic({ holdResponse: true });
  try {
    await assert.rejects(
      bridgeOpenAIChatRequest(
        {
          messages: [{ role: "user", content: "Local timeout probe." }],
        },
        {
          claudeExecutable,
          upstreamUrl: fake.baseUrl,
          timeoutMs: 500,
          scratchParent,
        },
      ),
    );
    assert.deepEqual(await readdir(scratchParent), []);
  } finally {
    await fake.close();
    await rm(scratchParent, { recursive: true, force: true });
  }
});
