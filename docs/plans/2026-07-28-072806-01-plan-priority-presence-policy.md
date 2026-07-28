# Priority presence policy

**Goal:** Let an administrator control Responses priority service tier globally, by daily active
hours, and per session while keeping unattended work on standard service.
**Why planning is required:** This adds a persisted routing policy, changes request bytes on both
HTTP/SSE and WebSocket transports, and exposes new authenticated admin mutations.
**Acceptance:** Existing behavior remains byte-preserving by default. A session override takes
precedence over the overall mode; overall force modes take precedence over the schedule; scheduled
active hours use priority and inactive hours use standard. A genuinely new main session may receive
one bounded priority-presence window without continued requests renewing it; subagents never create
presence. Policy decisions apply before both native Codex and custom-provider routing, are reflected
as the effective service tier in content-free telemetry, and are configurable from Settings and
Sessions. No live-store access, deployment, restart, commit, merge, or push is performed.

### Outcome 1: Persist and resolve priority policy
- Work: Store the overall policy and per-session overrides through the existing settings store;
  add an in-memory resolver with precedence, wraparound schedule handling, fixed UTC offset, and a
  bounded first-main-session presence window. Use request-log history to distinguish a genuinely
  new session after process restart without storing content.
- Risks/open questions: Concurrent first requests must converge on the same bounded presence
  outcome; malformed persisted policy must fail back to passthrough.
- Verify: `cargo test -p polyflare-server priority_policy`

### Outcome 2: Enforce one policy across transports and targets
- Work: Rewrite only valid Responses request objects/`response.create` frames when the resolver
  chooses priority or standard. Preserve exact bytes in passthrough mode. Apply before native Codex,
  custom target capability selection, HTTP/SSE execution, and WebSocket forwarding/replay so all
  retries retain one effective decision.
- Risks/open questions: Never log request content, never mutate non-Responses frames, and never let
  a subagent create or renew interactive presence.
- Verify: `cargo test -p polyflare-server --test custom_provider && cargo test -p polyflare-server --test ws_downstream_relay`

### Outcome 3: Add authenticated admin controls
- Work: Add read/write endpoints for the global policy and per-session override, expose effective
  policy state in the session read model, add an overall card to Settings, and add per-session
  `Inherit`/`Priority`/`Standard` controls to Sessions.
- Risks/open questions: Session identifiers remain one-way hashes; the API must reject unknown
  modes, invalid schedules, excessive presence windows, and invalid session keys.
- Verify: `cargo test -p polyflare-server --test settings_api --test read_api && npm test && npm run build`

### Outcome 4: Completion evidence
- Work: Review the full change for precedence, presence non-renewal, HTTP/WS parity, custom-target
  capability behavior, telemetry accuracy, content safety, persistence, and UI/API consistency.
- Verify: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && git diff --check`
