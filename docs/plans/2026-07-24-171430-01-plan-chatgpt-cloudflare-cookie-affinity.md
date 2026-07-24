# ChatGPT Cloudflare cookie affinity

**Goal:** Preserve the Cloudflare infrastructure cookies needed by ChatGPT backend passthrough without retaining any account, session, authentication, or non-ChatGPT cookies.

**Why planning is required:** This changes the process-shared HTTP client at an authentication-adjacent reverse-proxy boundary and therefore has security and cross-route consequences.

**Acceptance:** PolyFlare's shared Codex HTTP client retains and replays only the documented Cloudflare infrastructure cookie allowlist for HTTPS ChatGPT first-party hosts; it rejects all other cookie names, hosts, and schemes; existing Codex execution and ChatGPT gateway callers reuse the same client and connection pool; focused gateway tests and the full Rust workspace remain green.

### Outcome 1: Restricted affinity-cookie store

- Work: Add a process-global `reqwest::cookie::CookieStore` in `polyflare-codex` with exact ChatGPT host and HTTPS checks, a fixed Cloudflare service-cookie allowlist, and defense-in-depth output filtering.
- Risks/open questions: A process-global store is safe only while it cannot retain user-specific cookies. Subdomain matching must not accept suffix-confusion hosts.
- Verify: `cargo test -p polyflare-codex chatgpt_cloudflare_cookies`

### Outcome 2: Shared-client integration

- Work: Attach the restricted store inside `polyflare_codex::build_client` so the existing Codex executor and ChatGPT backend passthrough share cookie state and pooled connections without route-specific buffering or database work.
- Verify: `cargo test -p polyflare-codex --lib && cargo test -p polyflare-server --test chatgpt_backend_gateway`

### Outcome 3: Security and compatibility gate

- Work: Verify formatting, warnings, all workspace tests, and the final diff; review the cookie name, host, scheme, logging, and shared-client boundaries for credential leakage or unintended persistence.
- Verify: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && git diff --check`
