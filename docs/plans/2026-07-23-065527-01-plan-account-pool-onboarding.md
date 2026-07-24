# Dashboard account and routing-group onboarding

**Goal:** Let a local/admin operator onboard a Codex account through the dashboard and create a usable routing group with initial account membership, without exposing OAuth secrets to the browser.

**Why planning is required:** OAuth is security-sensitive, replay-sensitive credential work spanning durable storage, authenticated APIs, account persistence, and browser UI.

**Acceptance:** The existing dirty checkout is preserved; all new write endpoints remain behind the existing admin/loopback gate; OAuth verifier material is encrypted at rest and never returned or logged; callback state, expiry, and one-shot consumption are enforced; pool slugs are canonical and server-validated; routing-group creation requires at least one valid account and assigns all selected accounts atomically; failed operations do not partially mutate accounts; existing CLI login and routing behavior remain compatible; focused backend and frontend coverage, full dashboard build, relevant Rust tests, live desktop/mobile browser checks, console inspection, and a final diff/review gate pass before completion.

### Outcome 1: Durable Codex OAuth onboarding contract
- Work: Add an embedded migration and repository for expiring onboarding flows; expose authenticated start, status, and pasted-callback completion endpoints; reuse the existing PKCE/OAuth client and encrypted token store; insert a new account or refresh the matching ChatGPT identity without returning token material.
- Risks/open questions: The registered callback remains fixed at `localhost:1455`; the first browser slice must clearly support copying the final redirect URL when no listener owns that port. Terminal CLI login remains supported independently.
- Verify: `cargo test -p polyflare-server --test account_onboarding && cargo test -p polyflare-store onboarding`

### Outcome 2: Accounts onboarding UI
- Work: Add a visible Accounts-header and empty-state action using the existing PolyFlare design system; guide the operator through opening/copying the authorize URL, pasting the callback URL, observing pending/completed/error state, and optionally choosing an initial routing group; keep mutation and errors local to the dialog and invalidate account/pool/overview queries on completion.
- Risks/open questions: Browser storage must never contain access, refresh, ID tokens, or the PKCE verifier; UI copy must not imply automatic callback capture when only paste completion is guaranteed.
- Verify: `cd crates/polyflare-server/dashboard && npm test && npm run build`

### Outcome 3: Validated routing-group creation
- Work: Canonicalize and validate pool slugs in every dashboard/API write path; add authenticated atomic creation-by-assignment for a slug plus one or more existing account IDs; add a Pools-page action with explicit initial account selection and honest tag-model language; refresh account, pool, and overview data after success.
- Risks/open questions: This intentionally creates a usable routing tag, not an empty durable pool entity; rename/delete/per-pool persisted strategy remain outside this tag-model slice and must not be implied by the UI.
- Verify: `cargo test -p polyflare-server --test write_api && cargo test -p polyflare-server --test pool_routing`

### Outcome 4: Integrated security and UX verification
- Work: Rebuild the embedded dashboard, run the focused API/store/UI suites, verify loopback and bearer authorization behavior, inspect live Accounts/Pools flows at desktop and mobile widths, confirm no console errors or overflow, inspect the final diff for secret/log leakage and unrelated changes, and obtain a bounded independent review.
- Verify: `cargo test -p polyflare-server --test dashboard_api && cargo build -p polyflare-server --bin polyflare && git diff --check`
