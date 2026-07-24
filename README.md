# PolyFlare

PolyFlare is a self-hosted, multi-account and multi-provider gateway for AI coding clients. It
presents OpenAI Responses-compatible and Anthropic Messages-compatible endpoints, selects a healthy
backend for each request, preserves conversation continuity, and records content-safe operational
telemetry in an embedded SQLite store.

The project is designed for a single operator or team using accounts and API credentials they own.
Its default local setup is deliberately simple: start one Rust binary, open the embedded dashboard,
add accounts or providers, and point a compatible client at `http://127.0.0.1:8080`.

## What PolyFlare provides

- Multiple OAuth-backed Codex accounts with live quota, health, cooldown, and routing state.
- Named pools, with an account able to belong to more than one pool.
- Capacity-aware session placement and configurable routing strategies.
- Durable ownership for anchored conversations so a response ID never moves to the wrong account.
- Native downstream WebSocket support, upstream WebSocket support, and HTTP/SSE fallback.
- Bounded failover, starvation recovery, stream deadlines, and admission control.
- Native Anthropic Messages routing plus selected Messages-to-Responses translation aliases.
- Generic OpenAI Responses-compatible custom providers with credential pools and model catalogs.
- Provider-aware request history, usage, cost, TTFT, latency, throughput, sessions, and reports.
- An embedded dashboard with live SSE updates and polling fallback.
- Prometheus metrics and structured, content-safe process logs.
- Encrypted storage for OAuth tokens and custom-provider API keys.

## How requests flow

```text
Codex / Responses client ─┐
                          ├─> PolyFlare ingress
Claude / Messages client ─┘       │
                                  ├─ authenticate caller when client keys are enabled
                                  ├─ resolve model, provider, pool, session, and capability
                                  ├─ enforce continuity ownership and admission limits
                                  ├─ select an eligible account or provider credential
                                  ├─ relay by WebSocket or HTTP/SSE
                                  └─ observe terminal usage and update health + telemetry
```

A request first resolves its protocol and target:

- `POST /responses` uses the native Responses path. A configured custom-model slug is resolved
  before the built-in account fleet.
- `POST /v1/messages` uses an Anthropic account for an ordinary Anthropic model. Recognized Claude
  family aliases are translated to their mapped Codex target and translated back to Anthropic
  response events.
- `/{pool}/responses` and `/{pool}/v1/messages` apply the same behavior within one named pool.
  Custom models are intentionally root-scoped and are not resolved through a named account pool.

Selection is performed only after hard constraints have been applied. Provider, pool membership,
model support, manual pause, health, quota, capability requirements, and durable conversation
ownership cannot be bypassed by a routing strategy.

## Accounts, pools, and routing

OAuth-backed accounts are long-lived routing targets. PolyFlare refreshes Codex usage data in the
background, tracks request-driven health, persists rate-limit cooldowns across restarts, and keeps
runtime selection state separate from durable account configuration.

An account may be a member of multiple pools. Pools are routing scopes rather than separate copies
of an account:

```text
/responses                  all eligible Codex accounts
/work/responses             accounts in the "work" pool
/research/responses         accounts in the "research" pool
/work/models                safe model catalog for the "work" fleet
```

The default strategy is `capacity_weighted`. It applies deterministic weighted rendezvous to a new
session so a main session and its subagents prefer the same account, while remaining quota,
in-flight pressure, routing policy, and health influence the weight. Once continuity ownership is
established, ownership is stronger than preference: an anchored turn stays on its owning account
or waits/fails safely.

Available strategies are:

| Strategy | Intended behavior |
|---|---|
| `capacity_weighted` | Prefer available capacity while keeping a session family stable. |
| `usage_weighted` | Weight selection using observed usage. |
| `round_robin` | Distribute new work deterministically across the eligible set. |
| `fill_first` | Concentrate work on the warmest eligible account before moving on. |
| `sequential_drain` | Drain eligible accounts in a stable sequence. |
| `cache_affinity_tier` | Combine session affinity with reasoning-tier-aware capacity steering. |

Set one global strategy or override selected pools:

```sh
POLYFLARE_ROUTING_STRATEGY=capacity_weighted \
POLYFLARE_POOL_STRATEGY="work=cache_affinity_tier,batch=sequential_drain" \
cargo run --bin polyflare -- serve
```

Accounts also have a routing policy:

- `normal` participates normally.
- `burn_first` is preferred before neutral accounts.
- `preserve` is held back while less protected capacity is available.

## Continuity and response-anchor safety

Codex conversations can carry `previous_response_id`, turn state, session identifiers, and
connection-local incremental state. Treating those values as freely movable request metadata can
break a conversation when the next request lands on a different account or socket.

PolyFlare maintains a content-free continuity state machine:

1. It derives a stable session key from bounded client metadata.
2. It records which account owns a completed response anchor.
3. It pins later anchored turns to that owner.
4. It commits a new anchor only after `response.completed`.
5. It never fails over after response bytes have become visible to the client.
6. If a recoverable pre-output attempt loses its locally generated anchor, it can resend the full
   materialized request under a watchdog rather than silently reusing an invalid response ID.

Failed, incomplete, cancelled, and transport-lost turns do not become successful anchors. The
continuity tables store hashes, state, response IDs, timestamps, and ownership—not prompts,
messages, tool results, or generated text.

## WebSockets, SSE, and idle behavior

Client-facing Responses WebSockets are enabled by default on `GET /responses` and
`GET /{pool}/responses`. PolyFlare completes the upstream selection and upgrade before returning
`101 Switching Protocols`, then relays frames while retaining account and pool ownership.

Between turns, an upstream WebSocket is parked rather than treated as a stalled generation:

- Empty keepalive pings default to every 30 seconds.
- The parked connection has a 25-minute absolute idle budget.
- Ping writes are bounded and never start after the absolute deadline.
- A genuine upstream death or idle-budget expiry closes both legs. This is intentional: the client
  must establish a fresh connection rather than believe connection-scoped anchor state survived.

During an active turn, the normal stream idle deadline applies. Hidden redials and replays are
bounded, preserve the original account, and are allowed only before client-visible progress and
only while the upstream handshake contract remains compatible.

WebSocket behavior is managed on the dashboard **Settings** page. Transport construction happens
at startup, so these values are saved immediately and marked as pending until PolyFlare restarts:

| Setting | Default | Meaning |
|---|---:|---|
| `client_websocket_enabled` | `true` | Accept client-facing Responses WebSockets. Disabling it returns `426` so compatible clients can use HTTP/SSE. |
| `http_requests_use_upstream_websocket` | `false` | For an HTTP `POST /responses`, convert only the Codex upstream leg from HTTP/SSE to WebSocket. This does not control the client-facing relay. |
| `http_upstream_websocket_ping` | `false` | Send client-initiated pings during silent active turns only on WebSockets created for HTTP ingress. |
| `websocket_idle_ping_secs` | `30` | Parked relay ping cadence; `0` disables it and positive values clamp to 5–300 seconds. |
| `websocket_idle_budget_secs` | `1500` | Parked relay lifetime, clamped to 60–86400 seconds. |

`stream_idle_timeout` is a separate live setting for active response silence. It defaults to 300
seconds, accepts `0` to disable the deadline, and clamps to 3600 seconds.

The former `POLYFLARE_WS_DOWNSTREAM`, `POLYFLARE_WS_UPSTREAM`,
`POLYFLARE_WS_CLIENT_PING`, and `POLYFLARE_WS_IDLE_*` environment variables remain deprecated
bootstrap aliases. Dashboard values take precedence after they have been saved.

HTTP/SSE remains available for Responses requests and is the native transport for Messages and
generic custom providers. Dashboard live updates use a separate SSE stream and automatically fall
back to polling if that stream is unavailable.

## Providers and models

PolyFlare distinguishes two kinds of routing target:

- **Accounts** are subscription/OAuth identities with provider-specific continuity and quota state.
- **Credentials** belong to an operator-defined custom provider and are selected within that
  provider using health, routing weight, concurrency, and retry policy.

### Built-in protocol paths

The built-in Codex path preserves the client’s surviving request headers and raw JSON bytes,
replaces only selected-account authentication and identity, and supports HTTP/SSE or WebSocket
transport.

The built-in Anthropic path accepts `/v1/messages`, sends Anthropic-native HTTP requests through
Anthropic accounts, and supports streaming and non-streaming responses. Selected Claude family
names can also be translated to Responses requests and routed through Codex models. These aliases
are routing rules only; they are intentionally hidden from model-discovery pickers.

### Generic Responses-compatible providers

The Providers dashboard page can register any service that accepts:

```text
POST {base_url}/responses
Authorization: Bearer <provider credential>
Content-Type: application/json
Accept: text/event-stream
```

Each provider controls:

- slug and display name;
- base URL;
- enabled state;
- stateless Responses behavior;
- connect and stream-idle timeouts;
- retry count;
- provider-wide concurrency.

Each provider may have multiple credentials. Credentials have independent labels, routing weights,
concurrency limits, enabled state, health, and cooldown. Pre-stream `429` and `5xx` responses may
rotate to another credential within the configured retry bound. Authentication failures mark the
credential as requiring attention.

Each model maps a stable public model slug to an upstream model slug and can declare context size,
output limit, tool/vision/search/reasoning capabilities, pricing, and whether it appears in Codex
or generic OpenAI model discovery. Catalog visibility does not disable an explicitly addressed
route.

For stateless providers, PolyFlare removes `previous_response_id` and sends the materialized
request. This prevents account-scoped Responses anchors from leaking into a provider that does not
share that state. Custom-provider traffic is still represented in Requests, Sessions, Reports,
usage, cost, and Prometheus metrics using the provider slug and credential target.

### Example: Fugu Ultra

Use **Dashboard → Providers → Add provider**:

| Field | Example |
|---|---|
| Slug | `sakana` |
| Display name | `Sakana AI` |
| Base URL | `https://api.sakana.ai/v1` |
| Stateless Responses | enabled |

Add a credential using the Sakana API key, then add a model:

| Field | Example |
|---|---|
| Public model | `fugu-ultra` |
| Upstream model | `fugu-ultra-v1.1` |
| Display name | `Fugu Ultra` |
| Context window | `1000000` |

Use the provider’s current documentation as the authority for the upstream model slug and
capabilities. After saving, use **Test** on the provider card. A client can then request
`model = "fugu-ultra"` through the normal root `/responses` endpoint.

## Model discovery

PolyFlare serves model catalogs at:

- `GET /models`
- `GET /{pool}/models`
- `GET /backend-api/codex/models`
- `GET /v1/models`

Native fleet catalogs are intersections: a root or pool catalog advertises a native model only
when every eligible account in that scope supports it. This avoids selecting a model that a later
account cannot serve. Scope ETags are deterministic virtual identities and are reused across model
responses, HTTP response metadata, and WebSocket handshakes.

At startup, active root and pool scopes receive a bounded two-second warmup. Slow discovery
continues in the background, and stale or static floor data remains available when an upstream is
unavailable. Custom models are merged according to their explicit visibility settings.

## Dashboard

The dashboard is embedded in the server binary:

```text
http://127.0.0.1:8080/dashboard
```

Its pages cover:

- **Overview** — fleet status, top metrics, quota and capacity maps, request-volume analysis, and
  recent requests.
- **Accounts** — add Codex accounts, assign nicknames, pause/resume, set routing policy and
  capabilities, inspect trends, and manage multiple pool memberships.
- **Pools** — create routing groups and inspect their account/capacity composition.
- **Providers** — create Responses-compatible providers, credentials, and models; test and
  enable/disable each layer.
- **Requests** — paginated request history with request ID, session, account/credential, model,
  provider, priority/service tier, transport, TTFT, latency, throughput, tokens, cost, and terminal
  protocol outcome.
- **Sessions** — group main and subagent activity by content-free session key.
- **Reports** — time-series totals and account/model/provider breakdowns.
- **Settings** — live runtime settings, restart-only configuration, and the browser-local choice
  to display quota as remaining or used.
- **Keys** — create and revoke caller API keys.
- **Live logs** — content-safe server events delivered over SSE.

When PolyFlare is bound to a parsed loopback address and `POLYFLARE_ADMIN_TOKEN` is unset, dashboard
API access is local and tokenless. If an admin token is configured, the dashboard login stores it
in that browser and sends it as a bearer token. On a non-loopback bind, an unset admin token
disables the management API rather than exposing it.

## Install and run

Requirements:

- A current stable Rust toolchain.
- Git access for the pinned WebSocket dependencies.
- Node.js and npm only when rebuilding or testing the dashboard source.

Build the server:

```sh
git clone <your-polyflare-repository>
cd polyflare
cargo build --release --bin polyflare
```

Start it:

```sh
./target/release/polyflare serve
```

For development:

```sh
cargo run --bin polyflare -- serve
```

The default bind is `127.0.0.1:8080`. The default data directory is
`$HOME/.polyflare`, containing:

```text
store.db    SQLite accounts, routing state, settings, usage, and request telemetry
key         local 32-byte encryption key for stored upstream secrets
```

Back up both files together. A database backup without its key cannot decrypt account tokens or
custom-provider credentials.

## Add a Codex account

From another terminal:

```sh
cargo run --bin polyflare -- accounts login
```

The command prints an OAuth URL and listens for the loopback callback. To ask it to open the
browser automatically:

```sh
cargo run --bin polyflare -- accounts login --open
```

Optionally assign the account to a pool during onboarding:

```sh
cargo run --bin polyflare -- accounts login --pool work
```

The dashboard can run the same onboarding flow without stopping the server. Logging in an existing
identity refreshes its encrypted tokens in place. Account and pool changes invalidate the
in-process cache, so the running server sees them without a restart.

## Configure a client

A Responses-compatible client should use the PolyFlare origin as its base URL, without an added
`/v1` suffix:

```toml
model_provider = "polyflare"

[model_providers.polyflare]
name = "PolyFlare"
base_url = "http://127.0.0.1:8080"
wire_api = "responses"
experimental_bearer_token = "local-placeholder"
```

On the default keyless loopback setup, the placeholder is accepted because caller-key enforcement
is off. Once a PolyFlare client API key exists, use the generated raw key instead.

To target a pool, include the pool in the provider base URL:

```toml
base_url = "http://127.0.0.1:8080/work"
```

The repository also includes `scripts/codex-polyflare`, which creates an isolated client
configuration for local development:

```sh
scripts/codex-polyflare "explain this repository"
scripts/codex-polyflare --model fugu-ultra "review this change"
```

## Authentication and network posture

Dashboard administration and proxied model traffic use separate credentials:

- `POLYFLARE_ADMIN_TOKEN` protects `/api/*` and `/metrics`.
- PolyFlare client API keys protect model and control endpoints. Create one with the dashboard or
  `polyflare keys create`.
- Upstream OAuth tokens and provider API keys authenticate PolyFlare to upstream services.

Create and manage caller keys:

```sh
polyflare keys create --label laptop
polyflare keys list
polyflare keys revoke --id <key-id>
```

The raw key is shown once. Only its SHA-256 hash and display prefix are stored.

Startup posture is fail-safe:

| State at startup | Behavior |
|---|---|
| No client keys, loopback bind | Proxy endpoints are available without a PolyFlare key. |
| One or more client keys | A valid key is required on every bind. |
| No keys, non-loopback bind | Startup is refused. |
| No keys, non-loopback bind, `POLYFLARE_ALLOW_UNAUTHENTICATED_REMOTE=1` | Starts with a prominent warning; anyone who can connect can spend upstream quota. |

If PolyFlare sits behind a reverse proxy, create a client key even when PolyFlare itself listens on
loopback. The process cannot infer the reverse proxy’s real remote callers.

## Configuration reference

All values are read at startup unless the Settings page marks a field as live. Persisted live
settings override their environment-derived startup values.

### Core and security

| Variable | Default | Notes |
|---|---|---|
| `POLYFLARE_BIND` | `127.0.0.1:8080` | Listener socket address. |
| `POLYFLARE_DATA_DIR` | `$HOME/.polyflare` | Directory for `store.db` and `key`. |
| `POLYFLARE_ADMIN_TOKEN` | unset | Bearer token for dashboard APIs and `/metrics`; local loopback dashboard is tokenless when unset. |
| `POLYFLARE_ALLOW_UNAUTHENTICATED_REMOTE` | unset | Only exact `1` allows a non-loopback, keyless proxy. |
| `POLYFLARE_UPSTREAM_URL` | production Codex endpoint | Shared Codex account base URL. |
| `POLYFLARE_ANTHROPIC_UPSTREAM_URL` | `https://api.anthropic.com` | Shared Anthropic account base URL. |
| `POLYFLARE_AUTH_URL` | `https://auth.openai.com` | OAuth authority used for Codex login and refresh. |
| `POLYFLARE_CAPTURE_FINGERPRINT` | unset | Append content-safe structural request fingerprints to a JSONL file. |
| `RUST_LOG` | `info` | Standard tracing filter, for example `polyflare_server=debug,info`. |

### Routing and recovery

| Variable | Default | Notes |
|---|---:|---|
| `POLYFLARE_ROUTING_STRATEGY` | `capacity_weighted` | Global strategy name. |
| `POLYFLARE_POOL_STRATEGY` | unset | Comma-separated `pool=strategy` overrides. |
| `POLYFLARE_POOL_CAPABILITIES` | unset | Comma-separated `pool:capability` requirements; currently `security_work`. |
| `POLYFLARE_MAX_ACCOUNT_ATTEMPTS` | `3` | Total bounded upstream attempts; minimum 1. |
| `POLYFLARE_WATCHDOG_SECS` | `30` | Continuity watchdog duration. |
| `POLYFLARE_STARVATION_WAIT_BUDGET_SECS` | `60` | Bounded wait for recovering capacity; `0` disables, maximum 300. |
| `POLYFLARE_STARVATION_HEARTBEAT_SECS` | `10` | Keepalive cadence while waiting, clamped to the wait budget. |
| `POLYFLARE_STARVATION_WAKE_JITTER_MS` | `0` | Spreads concurrent recovery wakeups; maximum 30000. |
| `POLYFLARE_INFLIGHT_PENALTY_PCT` | `2.5` | Soft per-request pressure penalty; `0` disables, maximum 50. |
| `POLYFLARE_SOFT_DRAIN_ENABLED` | `true` | Prefer healthy capacity over draining accounts. |

### Admission limits

Zero disables an individual limit.

| Variable | Default | Meaning |
|---|---:|---|
| `POLYFLARE_ADMISSION_GLOBAL_INFLIGHT` | `256` | Process-wide active request count. |
| `POLYFLARE_ADMISSION_ACCOUNT_INFLIGHT` | `4` | Active requests per account. |
| `POLYFLARE_ADMISSION_GLOBAL_PRESSURE` | `1024` | Process-wide weighted request pressure. |
| `POLYFLARE_ADMISSION_ACCOUNT_PRESSURE` | `16` | Weighted pressure per account. |
| `POLYFLARE_ADMISSION_GLOBAL_OPEN_WS` | `128` | Process-wide open downstream WebSockets. |
| `POLYFLARE_ADMISSION_ACCOUNT_OPEN_WS` | `8` | Open downstream WebSockets per account. |
| `POLYFLARE_ADMISSION_OWNER_RECOVERY_RESERVE` | `1` | Per-account count reserved for pinned recovery. |
| `POLYFLARE_ADMISSION_OWNER_RECOVERY_PRESSURE_RESERVE` | `4` | Per-account pressure reserved for pinned recovery. |
| `POLYFLARE_ADMISSION_WAIT_TIMEOUT_MS` | `10000` | How long a pinned owner waits for admission. |

### Catalog, telemetry, and retention

| Variable | Default | Notes |
|---|---:|---|
| `POLYFLARE_MODEL_CATALOG_ENABLED` | `true` | Enable live per-account model discovery. |
| `POLYFLARE_MODEL_CATALOG_TTL_SECS` | `3600` | Catalog refresh TTL, clamped to 60–86400 seconds. |
| `POLYFLARE_LIVE_LOGS` | `true` | Dashboard SSE updates; set `0` for polling only. |
| `POLYFLARE_REQUEST_LOG_RETENTION_DAYS` | `0` | Age-prune request history; `0` disables, maximum 3650 days. |
| `POLYFLARE_USAGE_HISTORY_RETENTION_DAYS` | `0` | Age-prune usage samples while retaining the latest window sample; `0` disables. |

WebSocket variables are listed in [WebSockets, SSE, and idle behavior](#websockets-sse-and-idle-behavior).

## API surface

### Model traffic

| Method and path | Purpose |
|---|---|
| `POST /responses` | Root Responses routing, including custom models. |
| `GET /responses` | Responses WebSocket upgrade when enabled. |
| `POST /{pool}/responses` | Pool-scoped Responses routing. |
| `GET /{pool}/responses` | Pool-scoped Responses WebSocket upgrade. |
| `POST /v1/messages` | Root Anthropic Messages routing or alias translation. |
| `POST /{pool}/v1/messages` | Pool-scoped Messages routing or alias translation. |
| `POST /responses/compact` | Account-aware compaction. |
| `POST /{pool}/responses/compact` | Pool-scoped compaction. |
| `POST /images/generations` | Account-aware image generation forwarding. |
| `POST /images/edits` | Account-aware image edit forwarding. |
| `POST /alpha/search` | Forward explicitly enabled standalone search. |
| `POST /memories/trace_summarize` | Account-aware memory summarization. |

Current client control routes for goals and agent identity keys are also forwarded through the same
selected-account authentication and health machinery. See
[`docs/CODEX-RS-COMPATIBILITY.md`](docs/CODEX-RS-COMPATIBILITY.md) for the maintained compatibility
matrix and invariants.

### Operations

| Method and path | Purpose |
|---|---|
| `GET /models`, `GET /v1/models` | Root model discovery. |
| `GET /{pool}/models` | Pool-safe model discovery. |
| `GET /metrics` | Prometheus exposition, admin-gated. |
| `GET /dashboard` | Embedded operator dashboard. |
| `/api/*` | Admin-gated account, pool, provider, request, report, settings, and key APIs. |
| `GET /api/logs/stream` | Dashboard SSE stream for logs and request invalidation. |

## Observability

Every request receives a random content-free request ID. Request rows can include:

- provider and account or provider-credential target;
- session key and subagent/main-agent classification;
- public and upstream model;
- downstream and upstream transport;
- requested and actual service tier;
- terminal protocol outcome;
- TTFT, total latency, post-TTFT output throughput;
- input, cached-input, cache-write-input, output, reasoning-output, upstream-reported total, and
  orchestration token counts;
- computed cost when pricing is known.

Usage follows the Responses contract used by Codex. Cached input is a subset of input, and
reasoning output is a subset of output, so neither is added to the API total a second time.
PolyFlare keeps the upstream-reported total as its own fact and derives separately:

- **API total** — upstream `total_tokens`, then a legacy compatibility total, then a complete
  `input_tokens + output_tokens` pair when older data lacks an upstream total;
- **uncached input** — `input_tokens - cached_input_tokens`, clamped at zero;
- **visible output** — `output_tokens - reasoning_tokens`, clamped at zero;
- **effective Codex usage** — uncached input plus all output;
- **cache-hit rate** — cached input divided by input, not by total tokens.

New terminal observations are marked with their usage schema, upstream source, and final status.
Migrated request history remains usable but is labeled `legacy`; PolyFlare does not invent
historical cache-write counts or upstream totals that were never recorded.

Throughput is output tokens divided by the generation window
`(duration_ms - ttft_ms)`, not total request duration. Terminal protocol outcomes take precedence
over an initial HTTP `200`, so a stream that later fails is not reported as a success.

Process logs and the live log bus deliberately exclude request and response bodies, bearer tokens,
API keys, and free-form upstream errors. The Prometheus endpoint exposes bounded dimensions for
routing, health, failover, rate limits, admission leases, relays, and upstream requests.

## Storage and privacy

PolyFlare uses SQLite with forward-only embedded migrations. The database stores account identity,
pool memberships, encrypted credentials, quota samples, continuity ownership, runtime settings,
cooldowns, client-key hashes, and request telemetry.

OAuth tokens and custom-provider API keys are encrypted with XChaCha20-Poly1305 using a random
nonce per value. Decrypted secret wrappers are zeroized on drop. The local encryption key is
created once in the data directory and must be protected with the same care as the upstream
credentials.

Conversation content is not written to the request log, continuity tables, metrics, or live log
stream. Requests and streamed responses necessarily pass through process memory while being
relayed.

## Operational behavior

- Account configuration and pool changes become visible without restart.
- Live settings are persisted and override environment defaults on the next boot.
- Codex usage is refreshed every ten minutes, with immediate coalesced refreshes after capacity
  failures.
- Request and audit telemetry uses a bounded background writer so a slow SQLite write does not
  backpressure model output. Non-critical telemetry may be dropped if that queue is full.
- Model catalog startup work has a two-second readiness budget and continues in the background.
- Ctrl-C and SIGTERM stop accepting new work, allow active responses up to ten seconds to drain,
  then flush queued store writes.
- Request and usage retention are opt-in. Continuity cleanup runs independently for stale state.
- The current store/cache model is single-process. Multiple PolyFlare processes must not be treated
  as a coordinated distributed fleet.

## Development

Rust checks:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Dashboard checks:

```sh
cd crates/polyflare-server/dashboard
npm test
npm run build
```

The dashboard build writes to `crates/polyflare-server/dashboard/dist`, which is embedded in the
Rust binary. Rebuild it before producing a binary after dashboard source changes.

The workspace is organized as:

| Crate | Responsibility |
|---|---|
| `polyflare-core` | Provider-neutral types, traits, request context, and selection strategies. |
| `polyflare-codex` | Codex HTTP/WebSocket transport, OAuth, request identity, and incremental-turn handling. |
| `polyflare-anthropic` | Anthropic transport and Messages/Responses translation. |
| `polyflare-store` | SQLite repositories, migrations, encrypted secrets, and telemetry persistence. |
| `polyflare-testkit` | Scriptable HTTP and WebSocket upstreams for integration tests. |
| `polyflare-server` | Axum ingress, routing, recovery, management APIs, metrics, and the embedded dashboard. |

## Scope and limitations

- PolyFlare is intended for credentials and accounts you are authorized to use.
- Generic custom providers currently use the Responses HTTP/SSE contract. Provider-specific
  protocols require an adapter rather than only a dashboard entry.
- Custom-provider models are root-scoped; named pools scope built-in account fleets.
- Native Anthropic account onboarding and refresh are not exposed by the current dashboard flow.
- Realtime call creation and its account-matched sideband WebSocket are intentionally not exposed.
- The server is designed as one process with one local SQLite store, not an active-active cluster.
- Provider behavior and model capabilities change; validate a custom provider with its test action
  and current upstream documentation before relying on it.

## Responsible use

Only connect accounts and credentials you own or are explicitly authorized to operate. You are
responsible for provider terms, rate limits, billing, access control, and any network exposure of
your PolyFlare instance.

## License

[MIT](LICENSE) © 2026 aididhaiqal
