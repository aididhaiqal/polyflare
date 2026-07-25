# Claude Agent SDK bridge POC

This isolated experiment tests one narrow question: can an OpenAI-style,
single-turn chat request be translated into the official Claude Agent SDK and
sent by the actual Claude Code runtime?

It does not call Anthropic. The runner supplies a dummy OAuth token, points
Claude Code at a loopback-only fake Anthropic endpoint, disables nonessential
traffic, uses an empty temporary configuration/workspace, disables tools, and
deletes the temporary state after the request. The capture redacts
authorization and stores only hashes and lengths for message text.

## Run

Install only the JavaScript SDK and peer dependencies; the POC deliberately
omits the SDK's large optional native package because it uses an explicitly
selected local Claude binary:

```sh
npm install --ignore-scripts --omit=optional
CLAUDE_EXECUTABLE=/absolute/path/to/claude npm test
CLAUDE_EXECUTABLE=/absolute/path/to/claude npm run poc
```

`npm run poc` writes the sanitized result to `capture.json`, which is ignored
by Git.

## Deliberate limits

- One system/developer prompt plus exactly one user message.
- Non-streaming OpenAI-style output only.
- No assistant-history replay, session resume, images, or external tools.
- No real OAuth, upstream acceptance, account selection, or refresh testing.
- No claim that the SDK's native attestation remains usable with an
  operator-managed subscription account.

The next experiment, if this POC succeeds, is a persistent per-account worker
that maps a PolyFlare conversation to one Agent SDK session. Tool calling would
require an MCP bridge that suspends the SDK tool handler while the downstream
client executes the tool and later supplies its result.
