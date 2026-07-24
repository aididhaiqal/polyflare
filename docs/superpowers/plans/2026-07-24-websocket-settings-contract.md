# WebSocket settings contract cleanup

**Goal:** Replace the ambiguous WebSocket flags with clearly named, persisted dashboard settings while preserving existing deployments through deprecated environment aliases.
**Why planning is required:** This changes a public configuration contract and startup overlay behavior across the server API, persistence layer, dashboard, and documentation.
**Acceptance:** The Settings page can edit every WebSocket transport and idle-policy setting; restart-required values are persisted and applied on the next boot; effective versus pending values are unambiguous; the HTTP-to-upstream-WebSocket option is named for exactly what it does; existing `POLYFLARE_WS_*` launches continue to resolve identically as compatibility fallbacks; relevant Rust and dashboard tests pass.

### Outcome 1: Clear canonical names and compatibility
- Work: Rename the server-facing `ws_upstream` concept to `http_requests_use_upstream_websocket`, name the client relay and executor-ping controls by their actual scopes, and support the old environment and persisted keys only as deprecated fallbacks. Update startup and README terminology without changing defaults.
- Risks/open questions: Canonical settings must win over aliases deterministically, and no old setting may silently override a newly saved value.
- Verify: `cargo test -p polyflare-server config runtime_settings settings`

### Outcome 2: Persisted restart-required transport settings
- Work: Extend the settings metadata and PATCH path so client WebSocket enablement, HTTP-request upstream WebSocket conversion, and its active-turn ping policy are editable but marked restart-required. Apply their persisted values to `ServeConfig` before executors and routes are constructed. Return effective and configured values so the dashboard can show when a restart is pending.
- Risks/open questions: A write must never imply that the running router/executor already changed; invalid multi-key PATCH requests remain all-or-nothing.
- Verify: `cargo test -p polyflare-server --test settings_api`

### Outcome 3: Relay idle policy under Settings
- Work: Expose parked-relay ping cadence and idle budget as persisted restart-required settings with bounds matching the existing environment parser. Group them with the other WebSocket controls and provide explicit reader-facing labels/descriptions.
- Risks/open questions: `0` must continue to disable parked pings, while idle-budget clamps and defaults remain unchanged.
- Verify: `cargo test -p polyflare-server config --test settings_api`

### Outcome 4: Dashboard and documentation
- Work: Render an editable WebSocket section with clear descriptions and restart-pending state, keep fixed configuration read-only, and rewrite the README table around canonical settings rather than unexplained flags.
- Verify: `npm test -- --run && npm run build`
