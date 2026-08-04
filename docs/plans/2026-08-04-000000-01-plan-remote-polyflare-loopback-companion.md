# Remote PolyFlare loopback companion

**Goal:** Let Codex use remote-control and other ChatGPT backend routes when PolyFlare runs on
another tailnet host, while Codex still sees the allowlisted local origin
`http://localhost:8000/backend-api`.

**Scope boundary:** This companion is only for remotely hosted PolyFlare. It is unnecessary when
PolyFlare already runs on the Codex machine. This work builds and packages the companion but does
not install it, start a persistent service, change Codex configuration, or restart live PolyFlare.

**Why planning is required:** This is a credential-bearing HTTP/WebSocket forwarding boundary and
cross-platform persistent-service entry point. A mistake could expose authentication material,
forward traffic to the wrong origin, or interfere with the production PolyFlare listener.

### Outcome 1: Explicit, fail-closed network boundary

- Work: Add a standalone Rust crate whose CLI requires an HTTPS remote origin and only accepts a
  loopback listen address. Reject local upstreams, URL paths, credentials, query strings, and
  fragments. Disable upstream redirects and never select or fall back to another provider.
- Verify: focused configuration tests cover accepted remote HTTPS origins and rejection of unsafe
  listeners/upstreams.

### Outcome 2: Streaming HTTP/SSE forwarding

- Work: Forward every non-health request to the configured origin with its original method,
  path/query, end-to-end headers, and streaming body. Relay response status, safe end-to-end
  headers, and bytes without buffering, preserving SSE timing. Remove hop-by-hop headers including
  names nominated by `Connection`.
- Verify: integration tests cover exact target path/query, request/response streaming, SSE framing,
  headers, and fail-closed upstream errors.

### Outcome 3: Transparent WebSocket forwarding

- Work: Establish the configured WSS upstream before accepting the downstream upgrade, preserve
  negotiated subprotocols, and relay text, binary, ping, pong, and close messages bidirectionally.
  Do not replay or automatically reconnect an interrupted WebSocket session; clients reconnect at
  their protocol boundary, avoiding unsafe frame replay.
- Verify: integration tests cover upgrade, bidirectional data, and upstream-handshake failure.

### Outcome 4: Content-safe operation and health

- Work: Expose a namespaced local health endpoint. Log only structural lifecycle data; never log
  request/response bodies, WebSocket frames, authorization/cookie headers, URLs with query strings,
  or credentials.
- Verify: focused tests and source review enforce the health contract and sensitive-data boundary.

### Outcome 5: Opt-in service packaging

- Work: Add documented macOS LaunchAgent and Windows install/uninstall entry points that require an
  explicit remote origin. Templates use loopback `127.0.0.1:8000`, keep secrets out of arguments,
  and remain inert until the user installs them.
- Verify: shell/PowerShell syntax checks where available and documentation review. Do not load,
  enable, or start either service in this task.

### Outcome 6: Release-quality verification

- Work: Review the authenticated forwarding boundary, then run formatting, lint, focused tests,
  workspace tests, diff checks, and a release build. Perform live proxy checks only against an
  ephemeral loopback port and a controlled test upstream, never the installed `:8000` listener.
- Verify: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace`, `git diff --check`, and
  `cargo build --release -p polyflare-loopback`.
