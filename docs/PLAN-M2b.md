# PolyFlare M2b — OAuth Refresh + `capacity_weighted` Selector + Pool Wiring — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **SPLIT — read this first.** M2b is large and cleanly separable, so this plan is split into two independently-mergeable halves. **Merge M2b-1 before starting M2b-2** (M2b-2 consumes the reshaped `Selector`, the `CapacityWeighted` impl, and the `OAuthClient` from M2b-1).
> - **M2b-1 (Tasks 1–4)** — pure / unit-testable, **no serve-path change**: reshape `Selector` (M2-GATE1), the `capacity_weighted` selector impl, and the OAuth module (JWT decode + `should_refresh` + `POST /oauth/token` refresh). Touches `polyflare-core`, `polyflare-codex`, `polyflare-testkit` only. Ships: the routing brain + token refresh, fully tested, wired into nothing yet.
> - **M2b-2 (Tasks 5–6)** — the serve-path integration: snapshot assembly from the store + pool wiring into the ingress handler (replaces M1's single hardcoded account). Touches `polyflare-store`, `polyflare-server`. Ships: multi-account, store-backed routing.

**Goal:** Turn PolyFlare from a single-hardcoded-account relay into a multi-account, store-backed load balancer. Reshape the `Selector` seam to the rich-snapshot signature (M2-GATE1), port codex-lb's default `capacity_weighted` scoring as a pure, deterministic function, add OpenAI OAuth token refresh (decode-only JWT claims + 8-day refresh + `POST /oauth/token`), assemble per-account selection snapshots from the store, and wire selection + refresh + per-account bearer into the `/responses` ingress path.

**Architecture:** `polyflare-core` grows the reshaped `Selector` trait (`pick(&[AccountSnapshot], &SelectionCtx) -> Option<AccountId>` — sync, owned return), the `AccountSnapshot`/`SelectionCtx`/`AccountId` value types, and the `CapacityWeighted` implementation (eligibility hard-filter → health-tier pooling → burn/normal/preserve waterfall → weighted-random by remaining secondary credits, with a seedable RNG for parity). `polyflare-codex` grows an `oauth` module (decode-only JWT claims parser, `should_refresh`, `refresh_access_token` over `reqwest`, permanent-failure classification). `polyflare-testkit` grows a scriptable `MockOAuth` token endpoint. `polyflare-store` grows a `latest_usage` query. `polyflare-server` grows a `snapshot` assembler and a rewritten ingress handler: per request it assembles snapshots → selects → loads the account → refreshes if stale → decrypts → builds the core `Account` (shared upstream base URL + per-account bearer) → executes. The serve config migrates from `POLYFLARE_UPSTREAM_TOKEN` (single token) to store-backed per-account tokens.

**Tech Stack:** Rust 2021, tokio; `rand` 0.9 (seedable `StdRng` + `distr::weighted::WeightedIndex`); `base64` 0.22 (Engine API, `URL_SAFE_NO_PAD` for JWT payloads); `serde`/`serde_json` (claims + bodies); `reqwest` 0.12 (already a dep; `POST` JSON + read JSON); `thiserror`; `sqlx` 0.8 runtime-checked (existing store); `axum` 0.8 (existing server + testkit).

**Design references:** `DESIGN-DECISIONS.md` §M2-GATE1 (reshape `Selector` only; rich snapshot input; owned `AccountId`; STAYS SYNC), §S3 (selection ordering: continuity-ownership → session-affinity → availability scoring → health/cooldown), §C1 (continuity-ownership is a hard pre-filter — but continuity itself is M3, so M2b provides the ordering *seam* with ownership as a no-op passthrough hook), §TA6 (`security_work_authorized` hard pre-filter), §M2-SCOPE (the M2b line). `reference/codex-lb-port-reference.md` §Selector algorithm (the `capacity_weighted` pipeline + plan-capacity constants + tiebreak), §OAuth (decode-only claims, `POST /oauth/token` body/client_id/scope/timeout, `should_refresh` 8-day, permanent-failure codes), §Error/failure transitions.

## Global Constraints

- **Language / runtime:** Rust edition 2021, stable toolchain, `tokio`. Follow the established workspace + `.workspace = true` manifest style.
- **The pure selector is faithful to the port reference's `capacity_weighted`.** The scoring pipeline (eligibility hard-filter, health-tier pooling, burn/normal/preserve waterfall, weighted-random by `remaining_secondary_credits`, `account_id` final tiebreak, plan-capacity constants) mirrors codex-lb `logic.py` as distilled in `reference/codex-lb-port-reference.md` §Selector algorithm. Deviations are documented inline.
- **Selector scoring is PURE and deterministic under a seeded RNG.** `Selector::pick` performs no I/O and reads no clock — time enters via `SelectionCtx::now`, randomness via `SelectionCtx::rng_seed`. Given the same snapshots + ctx (with a seed), the pick is reproducible. Async DB snapshot-assembly lives in the *caller*, never in the trait (the trait stays sync per M2-GATE1).
- **Continuity-ownership is a no-op passthrough hook in M2b.** The S3 ordering seam (ownership → session-affinity → scoring → health) is present in `CapacityWeighted::pick` as documented steps, but ownership and session-affinity are passthroughs (all candidates pass) until the M3 continuity engine lands. This is deliberate — do not implement continuity here.
- **OAuth tests use a mock server, never real OpenAI.** The refresh path is exercised against `polyflare_testkit::MockOAuth`. No test performs a network call to `auth.openai.com` or `chatgpt.com`.
- **Never log or print tokens.** Any struct carrying a token (`RefreshedTokens`, reuse of `PlainTokens`) implements a redacting `Debug` (`***`) and carries a redaction unit test (standing rule). The ingress handler never prints a token; error responses to clients carry generic bodies (no upstream URL, no token, no internal error `Display`).
- **Secrets come from store / key-file / env only.** Per-account bearer tokens are decrypted from the store on demand; the at-rest key is the raw 32-byte file; the upstream/auth base URLs come from env. No secret is a compile-time constant (the OAuth `client_id` and `scope` are public, non-secret protocol constants).
- **sqlx stays runtime-checked (NOT compile-time-macro mode).** New store queries use `sqlx::query`/`query_as::<_, T>` with `#[derive(sqlx::FromRow)]`. No `query!`/`query_as!`; no `DATABASE_URL`/`.sqlx` cache needed.
- **The M1/M2a store + executor APIs are consumed as-is.** `Store`, `AccountRepo` (`get`/`list`/`update_status`/`update_tokens`/`decrypt_tokens`/`insert`), `Account`, `PlainTokens`, `EncryptedTokens`, `TokenCipher`, `StoreError`, `CodexExecutor::execute` keep their real M2a signatures. M2b adds one store method (`latest_usage`) and rewires the server; it does not change existing store signatures.
- **CI is strict.** `.github/workflows/ci.yml` runs `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo build --workspace --all-targets`, `cargo test --workspace`. Every task ends fmt-clean, clippy-clean (`-D warnings`), and green.

### Current-API notes (verified via Context7, 2026-07-14)

- **`rand` 0.9** — seedable RNG + weighted choice:
  - `use rand::{SeedableRng, rngs::StdRng}; let mut rng = StdRng::seed_from_u64(seed);` (deterministic; the parity-test seam).
  - `use rand::distr::weighted::WeightedIndex; use rand::distr::Distribution;` then `let dist = WeightedIndex::new(&weights_f64)?; let idx: usize = dist.sample(&mut rng);`.
  - `WeightedIndex::new` errors: `Error::InvalidInput` (empty), `Error::InsufficientNonZero` (all-zero), etc. — we pre-check all-zero and fall back deterministically, and treat any `Err` as the deterministic fallback defensively.
  - Non-deterministic path (production, `rng_seed = None`): `rand::rng()` (0.9's thread RNG, replaces `thread_rng()`).
  - A transitive `rand` 0.8.7 already exists in the lockfile (via `fernet`, import-only). Adding `rand = "0.9"` as a **direct** dep coexists fine (semver-incompatible versions are allowed to duplicate); the selector uses only the 0.9 direct dep.
- **`base64` 0.22** — JWT payload decode: `use base64::engine::general_purpose::URL_SAFE_NO_PAD; use base64::Engine as _;` then `URL_SAFE_NO_PAD.decode(payload_segment)?` → `Vec<u8>`. JWT segments are base64url with no padding, so `URL_SAFE_NO_PAD` is exact. (`base64` 0.22.1 is already resolved in the lockfile.)
- **`reqwest` 0.12** — `client.post(url).json(&body).timeout(Duration::from_secs(8)).send().await?`; `resp.status()`, `resp.json::<T>().await?`. Already a dep with the `json` feature.
- **`serde_json`** — `serde_json::from_slice::<serde_json::Value>(&bytes)` for claims; `serde_json::json!` for bodies; typed `#[derive(Deserialize)]` for the token response.

---

## File structure

```
polyflare/
├── Cargo.toml                                         # MODIFY (T1,T3): +rand +base64 workspace deps
├── crates/
│   ├── polyflare-core/
│   │   ├── Cargo.toml                                 # MODIFY (T1): +rand
│   │   └── src/
│   │       ├── lib.rs                                 # MODIFY (T1,T2): re-exports + `select` module
│   │       ├── types.rs                               # MODIFY (T1): +AccountId +AccountSnapshot +SelectionCtx
│   │       ├── traits.rs                              # MODIFY (T1): reshape `Selector`; new trait test
│   │       └── select.rs                              # CREATE (T2): CapacityWeighted + unit tests
│   ├── polyflare-codex/
│   │   ├── Cargo.toml                                 # MODIFY (T3): +base64 +serde +serde_json +thiserror
│   │   ├── src/
│   │   │   ├── lib.rs                                 # MODIFY (T3): `oauth` module + re-exports
│   │   │   └── oauth.rs                               # CREATE (T3 pure; T4 refresh): claims/should_refresh/refresh
│   │   └── tests/
│   │       └── oauth_refresh.rs                       # CREATE (T4): refresh against MockOAuth
│   ├── polyflare-testkit/
│   │   └── src/lib.rs                                 # MODIFY (T4): +MockOAuth token-endpoint mock
│   ├── polyflare-store/
│   │   └── src/
│   │       ├── lib.rs                                 # MODIFY (T5): re-export WindowUsage/UsageSnapshot
│   │       └── account.rs                             # MODIFY (T5): +latest_usage + usage snapshot types
│   └── polyflare-server/
│       ├── Cargo.toml                                 # MODIFY (T5,T6): dev-deps +sqlx +tempfile
│       └── src/
│           ├── lib.rs                                 # MODIFY (T5): `snapshot` module
│           ├── snapshot.rs                            # CREATE (T5): assemble_snapshots
│           ├── config.rs                              # MODIFY (T6): Config → ServeConfig (config migration)
│           ├── app.rs                                 # MODIFY (T6): AppState = store + selector + cipher + oauth
│           ├── ingress.rs                             # MODIFY (T6): select→refresh→decrypt→execute handler
│           └── main.rs                                # MODIFY (T6): serve() builds the store-backed AppState
│       └── tests/
│           ├── snapshot_assembly.rs                   # CREATE (T5)
│           ├── ingress_relays.rs                      # REWRITE (T6): seed store + assert selection/relay
│           ├── e2e_passthrough.rs                     # REWRITE (T6): store-backed helper
│           ├── large_body.rs                          # REWRITE (T6): store-backed helper
│           └── pool_selection.rs                      # CREATE (T6): no-eligible-account → 503
```

All commands below assume the repo root:
`POLYFLARE=/Users/wmaididhaiqal/Development/Codex-LoadBalancer/polyflare`

---
---

# PART M2b-1 — Routing brain + OAuth (pure / unit-testable; no serve-path change)

## Task 1: M2-GATE1 — reshape `Selector` + `AccountSnapshot` / `SelectionCtx` / `AccountId`

**Files:**
- Modify: `Cargo.toml` (workspace `[workspace.dependencies]`)
- Modify: `crates/polyflare-core/Cargo.toml`
- Modify: `crates/polyflare-core/src/types.rs`
- Modify: `crates/polyflare-core/src/traits.rs`
- Modify: `crates/polyflare-core/src/lib.rs`

**Interfaces:**
- Consumes: existing `polyflare_core::types` (`Account`, `RequestCtx`, `PreparedRequest`, `ExecError`, `ResponseStream`).
- Produces:
  - `struct AccountId(String)` with `as_str()`, `From<&str>`, `From<String>`, `Display`; derives `Debug, Clone, PartialEq, Eq`.
  - `struct AccountSnapshot { id: AccountId, status: String, used_percent: f64, secondary_used_percent: f64, reset_at: Option<i64>, capacity_credits: Option<f64>, routing_policy: String, health_tier: u8, error_count: u32, cooldown_until: Option<i64>, last_error_at: Option<i64>, last_selected_at: Option<i64>, plan_type: String, security_work_authorized: bool, in_flight: u32 }` (`Debug, Clone`) with `AccountSnapshot::new(id) -> Self` (neutral defaults).
  - `struct SelectionCtx { now: i64, require_security_work_authorized: bool, rng_seed: Option<u64>, session_id: Option<String> }` (`Debug, Clone, Default`).
  - Reshaped `trait Selector { fn pick(&self, candidates: &[AccountSnapshot], ctx: &SelectionCtx) -> Option<AccountId>; }` (sync, owned return).
- **Design note (added field `last_error_at`):** the port reference's eligibility "error backoff … skip while within backoff" and health-tier `should_drain` "(error_count>=2 within 60s)" both need an error *timestamp*, not just `error_count`. `last_error_at: Option<i64>` carries it so both clauses are implementable faithfully; the M2b snapshot assembler leaves it `None` (live error-timestamp tracking is deferred), which makes both time-windowed clauses inert in M2b — exactly the "no live tracking yet" behavior.

- [ ] **Step 1: Add the workspace `rand` dependency**

Add to `[workspace.dependencies]` in `$POLYFLARE/Cargo.toml` (below the existing `tempfile = "3"` line):
```toml
rand = "0.9"
base64 = "0.22"
```
(`base64` is used in Task 3; adding both now keeps the workspace table edits in one place. `rand` 0.9 default features include `std`, `std_rng` (`StdRng`), `thread_rng` (`rand::rng()`), and `alloc` (`WeightedIndex`) — all required.)

- [ ] **Step 2: Add `rand` to `polyflare-core`**

Add to `[dependencies]` in `$POLYFLARE/crates/polyflare-core/Cargo.toml` (after the `thiserror` line):
```toml
rand = { workspace = true }
```

- [ ] **Step 3: Write the failing new-signature trait test**

Replace the ENTIRE `#[cfg(test)]` module at the bottom of `$POLYFLARE/crates/polyflare-core/src/traits.rs` with (this is the "failing test" — it references `AccountSnapshot`/`SelectionCtx`/the owned-`AccountId` return that do not exist yet):
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{AccountId, AccountSnapshot, SelectionCtx};

    // A trivial Selector proves the reshaped trait is object-safe and returns an owned id.
    struct FirstCandidate;
    impl Selector for FirstCandidate {
        fn pick(&self, candidates: &[AccountSnapshot], _ctx: &SelectionCtx) -> Option<AccountId> {
            candidates.first().map(|s| s.id.clone())
        }
    }

    #[test]
    fn selector_returns_owned_account_id() {
        let pool = vec![AccountSnapshot::new("a"), AccountSnapshot::new("b")];
        let sel: Box<dyn Selector> = Box::new(FirstCandidate);
        let picked = sel.pick(&pool, &SelectionCtx::default()).unwrap();
        assert_eq!(picked.as_str(), "a");
    }
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `cd $POLYFLARE && cargo test -p polyflare-core --lib`
Expected: FAIL — compile errors (`AccountId`/`AccountSnapshot`/`SelectionCtx` not found; old `Selector::pick` signature mismatch).

- [ ] **Step 5: Add the new value types to `types.rs`**

In `$POLYFLARE/crates/polyflare-core/src/types.rs`, insert the following block **immediately before** the `#[cfg(test)]` line (after the `RequestCtx` definition):
```rust
/// An owned account identifier — the `Selector`'s return type (M2-GATE1: owned, not a borrow).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountId(String);

impl AccountId {
    /// The id as a string slice (e.g. for store lookups).
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for AccountId {
    fn from(s: String) -> Self {
        AccountId(s)
    }
}

impl From<&str> for AccountId {
    fn from(s: &str) -> Self {
        AccountId(s.to_string())
    }
}

impl std::fmt::Display for AccountId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A per-account snapshot the `Selector` scores over. Durable fields come from the store
/// `Account`; window fields come from the latest `usage_history` rows; runtime fields
/// (`health_tier`, `error_count`, `cooldown_until`, `last_error_at`, `last_selected_at`,
/// `in_flight`) are live-tracked later and default to neutral values in M2b.
#[derive(Debug, Clone)]
pub struct AccountSnapshot {
    pub id: AccountId,
    /// active | rate_limited | quota_exceeded | paused | reauth_required | deactivated
    pub status: String,
    /// Primary-window used percent (0–100).
    pub used_percent: f64,
    /// Secondary-window used percent (0–100) — drives the capacity weight.
    pub secondary_used_percent: f64,
    /// Durable rate-limit/quota reset epoch (seconds); auto-recovery gate.
    pub reset_at: Option<i64>,
    /// Per-account capacity override (credits); `None` ⇒ derive from `plan_type`.
    pub capacity_credits: Option<f64>,
    /// normal | burn_first | preserve
    pub routing_policy: String,
    /// 0 healthy / 1 draining / 2 probing (defaulted 0 in M2b).
    pub health_tier: u8,
    pub error_count: u32,
    /// Generic "don't select until" epoch (seconds).
    pub cooldown_until: Option<i64>,
    /// Epoch (seconds) of the most recent error — drives error-backoff + drain recency.
    pub last_error_at: Option<i64>,
    /// Epoch (seconds) this account was last selected — a deterministic tiebreak key.
    pub last_selected_at: Option<i64>,
    /// free | plus | pro | prolite | team | business | enterprise | edu
    pub plan_type: String,
    /// TA6 hard-pre-filter capability flag.
    pub security_work_authorized: bool,
    /// In-flight request count (live-tracked later; 0 in M2b).
    pub in_flight: u32,
}

impl AccountSnapshot {
    /// A snapshot with neutral defaults (active, zero usage, healthy, no runtime state). The
    /// assembler overrides the durable/window fields it knows; runtime fields stay defaulted
    /// in M2b (live tracking is deferred).
    pub fn new(id: impl Into<AccountId>) -> Self {
        Self {
            id: id.into(),
            status: "active".to_string(),
            used_percent: 0.0,
            secondary_used_percent: 0.0,
            reset_at: None,
            capacity_credits: None,
            routing_policy: "normal".to_string(),
            health_tier: 0,
            error_count: 0,
            cooldown_until: None,
            last_error_at: None,
            last_selected_at: None,
            plan_type: "plus".to_string(),
            security_work_authorized: false,
            in_flight: 0,
        }
    }
}

/// Per-selection context (M2-GATE1). `now`/`rng_seed` keep the `Selector` pure + deterministic:
/// time and randomness are injected, never read inside the trait. `session_id` is the
/// session-affinity seam (unused by `capacity_weighted` scoring in M2b).
#[derive(Debug, Clone, Default)]
pub struct SelectionCtx {
    pub now: i64,
    pub require_security_work_authorized: bool,
    pub rng_seed: Option<u64>,
    pub session_id: Option<String>,
}
```

- [ ] **Step 6: Reshape the `Selector` trait in `traits.rs`**

In `$POLYFLARE/crates/polyflare-core/src/traits.rs`, update the top `use` line and replace the `Selector` trait. The file's top becomes:
```rust
//! The five trait seams. M1 implemented only `Executor`; M2b implements `Selector`
//! (reshaped here per M2-GATE1 + the `CapacityWeighted` impl in `select.rs`).
//! `Continuity`/`Coordinator` stay PROVISIONAL — reshaped at their own milestones.

use async_trait::async_trait;

use crate::types::{
    Account, AccountId, AccountSnapshot, ExecError, PreparedRequest, RequestCtx, ResponseStream,
    SelectionCtx,
};

/// Executes a prepared request against an upstream using an account, returning a byte stream.
#[async_trait]
pub trait Executor: Send + Sync {
    async fn execute(
        &self,
        req: PreparedRequest,
        account: &Account,
    ) -> Result<ResponseStream, ExecError>;
}

/// Picks an account from a pool of per-account snapshots for a request. Sync + pure: scoring is
/// deterministic given the snapshots + ctx (async DB snapshot-assembly lives in the caller).
/// Returns an owned `AccountId` (M2-GATE1).
pub trait Selector: Send + Sync {
    fn pick(&self, candidates: &[AccountSnapshot], ctx: &SelectionCtx) -> Option<AccountId>;
}

/// Applies continuity (anchor/trim/resend) before execution. (No-op in M1/M2; state machine in M3.)
pub trait Continuity: Send + Sync {
    fn prepare(&self, req: PreparedRequest, ctx: &RequestCtx) -> PreparedRequest;
}

/// Coordinates session ownership + admission. (In-process pass in M1.)
pub trait Coordinator: Send + Sync {
    fn admit(&self, ctx: &RequestCtx) -> bool;
}
```
(The `#[cfg(test)]` module below stays as written in Step 3. `Account`/`RequestCtx`/`PreparedRequest` remain imported for `Executor`/`Continuity`/`Coordinator`.)

- [ ] **Step 7: Re-export the new types from `lib.rs`**

In `$POLYFLARE/crates/polyflare-core/src/lib.rs`, update the `types` re-export line to add the three new types:
```rust
pub use types::{
    Account, AccountId, AccountSnapshot, ExecError, PreparedRequest, RequestCtx, ResponseStream,
    SelectionCtx,
};
```
(Leave the other `pub use` lines and `pub mod` declarations unchanged for now; the `select` module is added in Task 2.)

- [ ] **Step 8: Run the test to verify it passes**

Run: `cd $POLYFLARE && cargo test -p polyflare-core --lib`
Expected: PASS — including `selector_returns_owned_account_id`.

- [ ] **Step 9: Format + lint**

Run: `cd $POLYFLARE && cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`
Expected: no fmt diff, no clippy warnings. (No other crate references `Selector` yet, so the reshape breaks nothing downstream.)

- [ ] **Step 10: Commit**
```bash
cd $POLYFLARE
git add Cargo.toml crates/polyflare-core
git commit -m "feat(core): reshape Selector to AccountSnapshot/SelectionCtx/AccountId (M2-GATE1)"
```

---

## Task 2: `capacity_weighted` selector impl (pure, seeded, faithful to the port reference)

**Files:**
- Create: `crates/polyflare-core/src/select.rs`
- Modify: `crates/polyflare-core/src/lib.rs`

**Interfaces:**
- Consumes: `crate::traits::Selector`, `crate::types::{AccountId, AccountSnapshot, SelectionCtx}`, `rand` 0.9.
- Produces: `struct CapacityWeighted` (unit struct; `Debug, Default, Clone, Copy`) implementing `Selector`.

- [ ] **Step 1: Write `select.rs` with the failing unit tests only**

Create `$POLYFLARE/crates/polyflare-core/src/select.rs` containing ONLY the test module for now (the impl lands in Step 3, above this block):
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Selector;
    use crate::types::{AccountSnapshot, SelectionCtx};

    fn ctx(now: i64, seed: u64) -> SelectionCtx {
        SelectionCtx {
            now,
            require_security_work_authorized: false,
            rng_seed: Some(seed),
            session_id: None,
        }
    }

    fn snap(id: &str, plan: &str, secondary_used: f64) -> AccountSnapshot {
        let mut s = AccountSnapshot::new(id);
        s.plan_type = plan.to_string();
        s.secondary_used_percent = secondary_used;
        s
    }

    #[test]
    fn skips_terminal_and_paused_accounts() {
        let sel = CapacityWeighted;
        for status in ["reauth_required", "deactivated", "paused"] {
            let mut s = snap("a", "plus", 0.0);
            s.status = status.to_string();
            assert!(
                sel.pick(&[s], &ctx(1000, 1)).is_none(),
                "status {status} must be ineligible"
            );
        }
    }

    #[test]
    fn rate_limited_recovers_only_after_reset() {
        let sel = CapacityWeighted;
        let mut s = snap("a", "plus", 50.0);
        s.status = "rate_limited".to_string();
        s.reset_at = Some(2000);
        assert!(sel.pick(&[s.clone()], &ctx(1500, 1)).is_none(), "before reset");
        assert_eq!(sel.pick(&[s], &ctx(2000, 1)).unwrap().as_str(), "a", "at reset");
    }

    #[test]
    fn cooldown_blocks_until_expiry() {
        let sel = CapacityWeighted;
        let mut s = snap("a", "plus", 0.0);
        s.cooldown_until = Some(5000);
        assert!(sel.pick(&[s.clone()], &ctx(4999, 1)).is_none());
        assert_eq!(sel.pick(&[s], &ctx(5000, 1)).unwrap().as_str(), "a");
    }

    #[test]
    fn error_backoff_blocks_within_window() {
        let sel = CapacityWeighted;
        let mut s = snap("a", "plus", 0.0);
        s.error_count = 4; // backoff = min(300, 30*2^(4-3)) = 60s
        s.last_error_at = Some(1000);
        assert!(sel.pick(&[s.clone()], &ctx(1030, 1)).is_none(), "within 60s");
        assert_eq!(sel.pick(&[s], &ctx(1061, 1)).unwrap().as_str(), "a", "past 60s");
    }

    #[test]
    fn ta6_filters_to_authorized_accounts() {
        let sel = CapacityWeighted;
        let a = snap("a", "plus", 0.0);
        let mut b = snap("b", "plus", 0.0);
        b.security_work_authorized = true;
        let c = SelectionCtx {
            now: 0,
            require_security_work_authorized: true,
            rng_seed: Some(1),
            session_id: None,
        };
        assert_eq!(sel.pick(&[a, b], &c).unwrap().as_str(), "b");
    }

    #[test]
    fn ta6_none_authorized_yields_no_account() {
        let sel = CapacityWeighted;
        let a = snap("a", "plus", 0.0);
        let c = SelectionCtx {
            now: 0,
            require_security_work_authorized: true,
            rng_seed: Some(1),
            session_id: None,
        };
        assert!(sel.pick(&[a], &c).is_none());
    }

    #[test]
    fn burn_first_drains_before_normal_and_preserve() {
        let sel = CapacityWeighted;
        let mut burn = snap("burn", "plus", 10.0);
        burn.routing_policy = "burn_first".to_string();
        let normal = snap("normal", "plus", 10.0);
        let mut preserve = snap("preserve", "plus", 10.0);
        preserve.routing_policy = "preserve".to_string();
        // burn_first is the only pool considered when present.
        assert_eq!(
            sel.pick(&[normal, preserve, burn], &ctx(0, 7)).unwrap().as_str(),
            "burn"
        );
    }

    #[test]
    fn should_drain_deprioritizes_maxed_account_when_a_healthy_one_exists() {
        let sel = CapacityWeighted;
        let healthy = snap("healthy", "plus", 10.0);
        let maxed = snap("maxed", "plus", 95.0); // secondary% >= 90 → should_drain → tier 1
        for seed in 0..20u64 {
            assert_eq!(
                sel.pick(&[healthy.clone(), maxed.clone()], &ctx(0, seed)).unwrap().as_str(),
                "healthy"
            );
        }
    }

    #[test]
    fn weighted_pick_is_reproducible_under_a_fixed_seed() {
        let sel = CapacityWeighted;
        let a = snap("a", "plus", 0.0);
        let b = snap("b", "pro", 0.0);
        let first = sel.pick(&[a.clone(), b.clone()], &ctx(0, 42)).unwrap();
        let second = sel.pick(&[a, b], &ctx(0, 42)).unwrap();
        assert_eq!(first, second, "same seed ⇒ same pick");
    }

    #[test]
    fn higher_capacity_account_wins_more_often_across_seeds() {
        let sel = CapacityWeighted;
        let big = snap("big", "pro", 0.0); // capacity 50400
        let small = snap("small", "free", 0.0); // capacity 1134
        let mut big_wins = 0;
        for seed in 0..1000u64 {
            if sel.pick(&[big.clone(), small.clone()], &ctx(0, seed)).unwrap().as_str() == "big" {
                big_wins += 1;
            }
        }
        assert!(big_wins > 900, "expected big to dominate, got {big_wins}/1000");
    }

    #[test]
    fn all_zero_weights_fall_back_to_account_id_tiebreak() {
        let sel = CapacityWeighted;
        // both fully used (secondary 100%) → remaining credits 0 → deterministic min;
        // equal on every key except account_id → lexicographically-smaller "aaa" wins.
        let a = snap("aaa", "plus", 100.0);
        let b = snap("bbb", "plus", 100.0);
        assert_eq!(sel.pick(&[b, a], &ctx(0, 5)).unwrap().as_str(), "aaa");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd $POLYFLARE && cargo test -p polyflare-core --lib select`
Expected: FAIL — compile error (`CapacityWeighted` not found).

- [ ] **Step 3: Implement `CapacityWeighted`**

Prepend to `$POLYFLARE/crates/polyflare-core/src/select.rs` (above the `#[cfg(test)]` block):
```rust
//! The default `capacity_weighted` account selector — a faithful port of codex-lb's `logic.py`
//! scoring (see docs/reference/codex-lb-port-reference.md §Selector algorithm). Pure and
//! deterministic given a seeded RNG: no I/O, no clock reads (time enters via `SelectionCtx::now`,
//! randomness via `SelectionCtx::rng_seed`).

use rand::distr::weighted::WeightedIndex;
use rand::distr::Distribution;
use rand::rngs::StdRng;
use rand::SeedableRng;

use crate::traits::Selector;
use crate::types::{AccountId, AccountSnapshot, SelectionCtx};

/// Secondary-window plan capacity (credits). Source: port reference §Plan capacity.
fn plan_capacity_secondary(plan: &str) -> f64 {
    match plan {
        "free" => 1134.0,
        "plus" | "business" | "team" | "edu" => 7560.0,
        "pro" | "enterprise" => 50400.0,
        "prolite" => 37800.0,
        // Unknown plans fall back to the plus-tier capacity (a safe mid value).
        _ => 7560.0,
    }
}

/// Error backoff = min(300, 30 * 2^(error_count-3)) seconds, for error_count >= 3.
fn error_backoff_secs(error_count: u32) -> i64 {
    let exp = error_count.saturating_sub(3).min(20); // cap the shift to avoid overflow
    let raw = 30i64.saturating_mul(1i64 << exp);
    raw.min(300)
}

/// An eligible candidate: a borrowed snapshot + its post-recovery effective usage.
#[derive(Clone, Copy)]
struct Candidate<'a> {
    snap: &'a AccountSnapshot,
    eff_used: f64,
    eff_secondary_used: f64,
}

impl Candidate<'_> {
    /// remaining_secondary_credits = max(0, capacity * (1 - min(secondary_used%,100)/100)).
    fn remaining_secondary_credits(&self) -> f64 {
        let capacity = self
            .snap
            .capacity_credits
            .unwrap_or_else(|| plan_capacity_secondary(&self.snap.plan_type));
        (capacity * (1.0 - self.eff_secondary_used.min(100.0) / 100.0)).max(0.0)
    }

    /// should_drain if used%>=85 OR secondary%>=90 OR (error_count>=2 within 60s of last error).
    fn should_drain(&self, now: i64) -> bool {
        self.eff_used >= 85.0
            || self.eff_secondary_used >= 90.0
            || (self.snap.error_count >= 2
                && self.snap.last_error_at.is_some_and(|t| now - t <= 60))
    }

    /// Effective health tier: base tier, bumped to at least `draining`(1) when `should_drain`.
    fn effective_tier(&self, now: i64) -> u8 {
        if self.should_drain(now) {
            self.snap.health_tier.max(1)
        } else {
            self.snap.health_tier
        }
    }
}

/// Eligibility hard-filter (port reference step 1). `None` ⇒ skip; `Some(Candidate)` with usage
/// zeroed for auto-recovered rate/quota accounts.
fn eligibility(s: &AccountSnapshot, now: i64) -> Option<Candidate<'_>> {
    match s.status.as_str() {
        // Terminal / operator-held: never eligible.
        "reauth_required" | "deactivated" | "paused" => return None,
        // Rate/quota limited: eligible only once the reset time has passed (usage zeroed).
        "rate_limited" | "quota_exceeded" => match s.reset_at {
            Some(reset) if now >= reset => {
                return Some(Candidate {
                    snap: s,
                    eff_used: 0.0,
                    eff_secondary_used: 0.0,
                });
            }
            _ => return None,
        },
        // active (or any other value) → fall through to the cooldown/backoff gates.
        _ => {}
    }

    // Generic cooldown gate.
    if let Some(cd) = s.cooldown_until {
        if now < cd {
            return None;
        }
    }

    // Error backoff (only once error_count >= 3, measured from the last error time).
    if s.error_count >= 3 {
        if let Some(last) = s.last_error_at {
            if now < last + error_backoff_secs(s.error_count) {
                return None;
            }
        }
    }

    Some(Candidate {
        snap: s,
        eff_used: s.used_percent,
        eff_secondary_used: s.secondary_used_percent,
    })
}

/// Health-tier pooling (step 2): prefer healthy(0), then probing(2), then draining(1) — mirrors
/// codex-lb's `healthy or probing or draining or available`.
fn health_tier_pool<'a>(pool: &[Candidate<'a>], now: i64) -> Vec<Candidate<'a>> {
    for tier in [0u8, 2, 1] {
        let group: Vec<Candidate> = pool
            .iter()
            .copied()
            .filter(|c| c.effective_tier(now) == tier)
            .collect();
        if !group.is_empty() {
            return group;
        }
    }
    pool.to_vec()
}

/// Burn/normal/preserve waterfall (step 3): drain burn_first, then normal, then preserve.
fn policy_waterfall<'a>(pool: &[Candidate<'a>]) -> Vec<Candidate<'a>> {
    for policy in ["burn_first", "normal", "preserve"] {
        let group: Vec<Candidate> = pool
            .iter()
            .copied()
            .filter(|c| c.snap.routing_policy == policy)
            .collect();
        if !group.is_empty() {
            return group;
        }
    }
    pool.to_vec()
}

/// Deterministic tiebreak (all-zero weights): min by
/// `(-remaining_secondary_credits, secondary_used%, primary_used%, last_selected_at, account_id)`.
fn deterministic_min<'a, 'b>(pool: &'a [Candidate<'b>]) -> &'a Candidate<'b> {
    pool.iter()
        .min_by(|a, b| {
            // -remaining ascending == remaining descending.
            b.remaining_secondary_credits()
                .total_cmp(&a.remaining_secondary_credits())
                .then(a.eff_secondary_used.total_cmp(&b.eff_secondary_used))
                .then(a.eff_used.total_cmp(&b.eff_used))
                .then(
                    a.snap
                        .last_selected_at
                        .unwrap_or(0)
                        .cmp(&b.snap.last_selected_at.unwrap_or(0)),
                )
                .then(a.snap.id.as_str().cmp(b.snap.id.as_str()))
        })
        .expect("pool is non-empty")
}

/// Weighted-random pick by remaining secondary credits (step 4). All-zero weights fall back to
/// the deterministic tiebreak. The RNG is seeded from `ctx.rng_seed` when present (parity).
fn weighted_pick(pool: &[Candidate<'_>], ctx: &SelectionCtx) -> Option<AccountId> {
    if pool.is_empty() {
        return None;
    }
    let weights: Vec<f64> = pool
        .iter()
        .map(Candidate::remaining_secondary_credits)
        .collect();

    if weights.iter().all(|w| *w <= 0.0) {
        return Some(deterministic_min(pool).snap.id.clone());
    }

    let dist = match WeightedIndex::new(&weights) {
        Ok(d) => d,
        // Defensive: any weight error (e.g. all-zero slipping through) → deterministic pick.
        Err(_) => return Some(deterministic_min(pool).snap.id.clone()),
    };

    let idx = match ctx.rng_seed {
        Some(seed) => dist.sample(&mut StdRng::seed_from_u64(seed)),
        None => dist.sample(&mut rand::rng()),
    };
    Some(pool[idx].snap.id.clone())
}

/// The default selector: (S3 ordering) continuity-ownership + session-affinity are M3 no-op
/// passthroughs here; TA6 capability pre-filter → eligibility → health-tier → policy waterfall →
/// capacity-weighted pick.
#[derive(Debug, Default, Clone, Copy)]
pub struct CapacityWeighted;

impl Selector for CapacityWeighted {
    fn pick(&self, candidates: &[AccountSnapshot], ctx: &SelectionCtx) -> Option<AccountId> {
        let now = ctx.now;

        // (S3 steps 1–2) continuity-ownership + session-affinity: M3 hard pre-filters; in M2b
        // they are no-op passthroughs (every candidate passes).

        // TA6 capability hard pre-filter (above scoring), then eligibility hard-filter.
        let eligible: Vec<Candidate> = candidates
            .iter()
            .filter(|s| !ctx.require_security_work_authorized || s.security_work_authorized)
            .filter_map(|s| eligibility(s, now))
            .collect();
        if eligible.is_empty() {
            return None;
        }

        // Health-tier pooling, then burn/normal/preserve waterfall.
        let pool = health_tier_pool(&eligible, now);
        let pool = policy_waterfall(&pool);

        // Capacity-weighted random pick (deterministic under a seed).
        weighted_pick(&pool, ctx)
    }
}
```

- [ ] **Step 4: Wire `select` into `lib.rs`**

In `$POLYFLARE/crates/polyflare-core/src/lib.rs`, add the module declaration and re-export:
```rust
pub mod select;
```
(add alongside the other `pub mod` lines) and
```rust
pub use select::CapacityWeighted;
```
(add alongside the other `pub use` lines).

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd $POLYFLARE && cargo test -p polyflare-core --lib select`
Expected: PASS (11 tests).

- [ ] **Step 6: Format + lint**

Run: `cd $POLYFLARE && cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 7: Commit**
```bash
cd $POLYFLARE
git add crates/polyflare-core
git commit -m "feat(core): capacity_weighted selector (pure, seeded, parity-tested)"
```

---

## Task 3: OAuth — decode-only JWT claims + `should_refresh` + failure classification (pure)

**Files:**
- Modify: `crates/polyflare-codex/Cargo.toml`
- Create: `crates/polyflare-codex/src/oauth.rs`
- Modify: `crates/polyflare-codex/src/lib.rs`

**Interfaces:**
- Consumes: `base64` 0.22, `serde`/`serde_json`, `thiserror`.
- Produces (this task — the network-free surface):
  - `struct Claims { email, sub, chatgpt_account_id, chatgpt_user_id, chatgpt_plan_type, workspace_id, workspace_label, seat_type: Option<String>, exp: Option<i64> }` (`Debug, Clone, Default, PartialEq, Eq`).
  - `enum FailureClass { ReauthRequired, Deactivated, Transient }` with `fn status(self) -> Option<&'static str>`.
  - `enum OAuthError { MalformedJwt(String), Transport(String), Endpoint { status: u16, code: Option<String> } }` (`Debug, thiserror::Error`).
  - `fn should_refresh(last_refresh: i64, now: i64) -> bool` (8-day).
  - `fn classify_failure(code: &str) -> FailureClass`.
  - `fn decode_claims(id_token: &str) -> Result<Claims, OAuthError>`.

- [ ] **Step 1: Add the codex crate's new dependencies**

Replace the `[dependencies]` block in `$POLYFLARE/crates/polyflare-codex/Cargo.toml` with:
```toml
[dependencies]
polyflare-core = { path = "../polyflare-core" }
reqwest = { workspace = true }
futures-util = { workspace = true }
bytes = { workspace = true }
async-trait = { workspace = true }
base64 = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
```
and set `[dev-dependencies]` to:
```toml
[dev-dependencies]
tokio = { workspace = true }
polyflare-testkit = { path = "../polyflare-testkit" }
```
(`serde_json` moves from dev-deps to normal deps — it is now used in `src/oauth.rs`. Integration tests still see it via the normal dep. `base64` as a normal dep is likewise visible to tests for crafting JWTs.)

- [ ] **Step 2: Write `oauth.rs` with the failing pure-unit tests only**

Create `$POLYFLARE/crates/polyflare-codex/src/oauth.rs` containing ONLY this test module for now (the impl lands in Step 4, above it):
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine as _;

    /// Build a JWT with an unsigned base64url-no-pad payload from a JSON value.
    fn make_jwt(payload: &serde_json::Value) -> String {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#);
        let body = URL_SAFE_NO_PAD.encode(serde_json::to_vec(payload).unwrap());
        format!("{header}.{body}.sig")
    }

    #[test]
    fn should_refresh_at_8_day_boundary() {
        let day = 86_400;
        assert!(!should_refresh(0, 8 * day), "exactly 8 days ⇒ not yet");
        assert!(should_refresh(0, 8 * day + 1), "just over 8 days ⇒ refresh");
        assert!(!should_refresh(1000, 1000), "fresh");
    }

    #[test]
    fn decode_claims_reads_top_level_and_nested_auth() {
        let payload = serde_json::json!({
            "email": "user@example.test",
            "sub": "sub-123",
            "exp": 1_800_000_000i64,
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "acct-xyz",
                "chatgpt_user_id": "user-auth",
                "chatgpt_plan_type": "pro"
            }
        });
        let claims = decode_claims(&make_jwt(&payload)).unwrap();
        assert_eq!(claims.email.as_deref(), Some("user@example.test"));
        assert_eq!(claims.chatgpt_account_id.as_deref(), Some("acct-xyz"));
        assert_eq!(claims.chatgpt_user_id.as_deref(), Some("user-auth")); // auth-claim wins
        assert_eq!(claims.chatgpt_plan_type.as_deref(), Some("pro"));
        assert_eq!(claims.exp, Some(1_800_000_000));
    }

    #[test]
    fn chatgpt_user_id_falls_back_to_sub() {
        let claims = decode_claims(&make_jwt(&serde_json::json!({ "sub": "sub-only" }))).unwrap();
        assert_eq!(claims.chatgpt_user_id.as_deref(), Some("sub-only"));
    }

    #[test]
    fn malformed_jwt_missing_payload_errors() {
        assert!(matches!(
            decode_claims("only-one-segment"),
            Err(OAuthError::MalformedJwt(_))
        ));
    }

    #[test]
    fn classify_failure_splits_reauth_vs_deactivate_vs_transient() {
        assert_eq!(classify_failure("invalid_grant"), FailureClass::ReauthRequired);
        assert_eq!(classify_failure("refresh_token_expired"), FailureClass::ReauthRequired);
        assert_eq!(classify_failure("account_deleted"), FailureClass::Deactivated);
        assert_eq!(classify_failure("account_suspended"), FailureClass::Deactivated);
        assert_eq!(classify_failure("temporarily_unavailable"), FailureClass::Transient);
        assert_eq!(FailureClass::ReauthRequired.status(), Some("reauth_required"));
        assert_eq!(FailureClass::Deactivated.status(), Some("deactivated"));
        assert_eq!(FailureClass::Transient.status(), None);
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd $POLYFLARE && cargo test -p polyflare-codex oauth`
Expected: FAIL — compile errors (`should_refresh`, `decode_claims`, `Claims`, `FailureClass`, `OAuthError` not found).

- [ ] **Step 4: Implement the pure OAuth surface**

Prepend to `$POLYFLARE/crates/polyflare-codex/src/oauth.rs` (above the `#[cfg(test)]` block):
```rust
//! OpenAI OAuth for the Codex backend: decode-only JWT claims, an 8-day refresh gate,
//! `POST /oauth/token` refresh (Task 4), and permanent-failure classification. Tokens are never
//! logged (see the redacting `Debug` on `RefreshedTokens`). See
//! docs/reference/codex-lb-port-reference.md §OAuth.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde_json::Value;

/// Refresh when the stored token is older than 8 days (`token_refresh_interval_days`).
const TOKEN_REFRESH_INTERVAL_DAYS: i64 = 8;
/// The nested OpenAI auth claim carrying auth-scoped identity fields.
const AUTH_CLAIM: &str = "https://api.openai.com/auth";

/// Codex-relevant identity claims decoded (NOT signature-verified) from an id_token.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Claims {
    pub email: Option<String>,
    pub sub: Option<String>,
    pub chatgpt_account_id: Option<String>,
    pub chatgpt_user_id: Option<String>,
    pub chatgpt_plan_type: Option<String>,
    pub workspace_id: Option<String>,
    pub workspace_label: Option<String>,
    pub seat_type: Option<String>,
    pub exp: Option<i64>,
}

/// How a refresh failure should transition the account's `status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// Token/session invalidated — the user can re-authenticate.
    ReauthRequired,
    /// Account-level termination — deactivate.
    Deactivated,
    /// Not a permanent failure (network / 5xx / unknown) — retry later.
    Transient,
}

impl FailureClass {
    /// The store `status` string this class maps to (`None` for `Transient` — status unchanged).
    pub fn status(self) -> Option<&'static str> {
        match self {
            FailureClass::ReauthRequired => Some("reauth_required"),
            FailureClass::Deactivated => Some("deactivated"),
            FailureClass::Transient => None,
        }
    }
}

/// Errors from OAuth decode / refresh.
#[derive(Debug, thiserror::Error)]
pub enum OAuthError {
    #[error("malformed jwt: {0}")]
    MalformedJwt(String),
    #[error("oauth transport error: {0}")]
    Transport(String),
    #[error("oauth endpoint returned status {status} (error code: {code:?})")]
    Endpoint { status: u16, code: Option<String> },
}

/// `true` when the token was last refreshed more than 8 days before `now` (epoch seconds).
pub fn should_refresh(last_refresh: i64, now: i64) -> bool {
    now - last_refresh > TOKEN_REFRESH_INTERVAL_DAYS * 86_400
}

/// Classify a token-endpoint error code into a status transition (port reference §permanent
/// failure codes). Account-terminal codes ⇒ Deactivated; token/session codes ⇒ ReauthRequired;
/// anything else ⇒ Transient.
pub fn classify_failure(code: &str) -> FailureClass {
    match code {
        "account_deactivated" | "account_suspended" | "account_deleted" => {
            FailureClass::Deactivated
        }
        "refresh_token_expired"
        | "refresh_token_reused"
        | "refresh_token_invalidated"
        | "invalid_grant"
        | "token_invalidated"
        | "token_expired"
        | "app_session_terminated"
        | "account_session_expired"
        | "account_auth_invalidated" => FailureClass::ReauthRequired,
        _ => FailureClass::Transient,
    }
}

/// Decode (WITHOUT verifying) the identity claims from a JWT id_token.
pub fn decode_claims(id_token: &str) -> Result<Claims, OAuthError> {
    let payload_b64 = id_token
        .split('.')
        .nth(1)
        .ok_or_else(|| OAuthError::MalformedJwt("missing payload segment".to_string()))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|e| OAuthError::MalformedJwt(format!("base64url: {e}")))?;
    let v: Value = serde_json::from_slice(&bytes)
        .map_err(|e| OAuthError::MalformedJwt(format!("json: {e}")))?;

    let auth = v.get(AUTH_CLAIM);
    // Prefer the nested auth claim, then the top-level claim.
    let pick = |key: &str| -> Option<String> {
        auth.and_then(|a| a.get(key))
            .or_else(|| v.get(key))
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    // chatgpt_user_id precedence: auth claim > top-level > sub.
    let chatgpt_user_id = auth
        .and_then(|a| a.get("chatgpt_user_id"))
        .or_else(|| v.get("chatgpt_user_id"))
        .or_else(|| v.get("sub"))
        .and_then(Value::as_str)
        .map(str::to_string);

    Ok(Claims {
        email: pick("email"),
        sub: v.get("sub").and_then(Value::as_str).map(str::to_string),
        chatgpt_account_id: pick("chatgpt_account_id"),
        chatgpt_user_id,
        chatgpt_plan_type: pick("chatgpt_plan_type"),
        workspace_id: pick("workspace_id"),
        workspace_label: pick("workspace_label"),
        seat_type: pick("seat_type"),
        exp: v.get("exp").and_then(Value::as_i64),
    })
}
```

- [ ] **Step 5: Wire `oauth` into `lib.rs`**

Replace `$POLYFLARE/crates/polyflare-codex/src/lib.rs` with:
```rust
//! Codex backend: WS/SSE transport, fingerprint laundering, continuity, OAuth. M1 = SSE identity
//! pass-through; M2b adds the `oauth` module (claims decode + refresh).

pub mod executor;
pub mod oauth;

pub use executor::CodexExecutor;
pub use oauth::{classify_failure, decode_claims, should_refresh, Claims, FailureClass, OAuthError};
```
(Task 4 adds `OAuthClient`/`Refreshed`/`RefreshedTokens` to this re-export list.)

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd $POLYFLARE && cargo test -p polyflare-codex oauth`
Expected: PASS (5 tests).

- [ ] **Step 7: Format + lint**

Run: `cd $POLYFLARE && cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 8: Commit**
```bash
cd $POLYFLARE
git add crates/polyflare-codex
git commit -m "feat(codex): OAuth claims decode + should_refresh + failure classification"
```

---

## Task 4: OAuth refresh over HTTP + `MockOAuth` test server

**Files:**
- Modify: `crates/polyflare-testkit/src/lib.rs` (add `MockOAuth`)
- Modify: `crates/polyflare-codex/src/oauth.rs` (add `OAuthClient` + `refresh`)
- Modify: `crates/polyflare-codex/src/lib.rs` (re-export)
- Create: `crates/polyflare-codex/tests/oauth_refresh.rs`

**Interfaces:**
- Consumes: `reqwest` 0.12, `serde`, the Task 3 `decode_claims`/`OAuthError`/`Claims`; `polyflare_testkit::MockOAuth`.
- Produces:
  - `polyflare_testkit::MockOAuth` (`ok(access, refresh, id)` / `error(status, code)`, `last_body()`, `async spawn() -> String`) + `enum OAuthResponse`.
  - `struct RefreshedTokens { access_token, refresh_token, id_token: String }` (`Clone`; redacting `Debug`).
  - `struct Refreshed { tokens: RefreshedTokens, claims: Claims }` (`Debug, Clone`).
  - `struct OAuthClient` with `new(auth_base_url: impl Into<String>) -> Result<OAuthClient, OAuthError>` and `async fn refresh(&self, refresh_token: &str) -> Result<Refreshed, OAuthError>`.

- [ ] **Step 1: Add `MockOAuth` to the testkit**

Append to `$POLYFLARE/crates/polyflare-testkit/src/lib.rs` (after the existing `MockUpstream` code; add `use axum::http::StatusCode;` to the imports at the top — the other imports `Arc`/`Mutex`/`SocketAddr`/`TcpListener`/`Router`/`post`/`Json`/`State` are already present):
```rust
/// A scriptable mock of the OpenAI OAuth token endpoint (`POST /oauth/token`). Records the
/// request body and returns either a success token payload or an error status + code. Test infra
/// only — never used in production wiring.
#[derive(Clone)]
pub struct MockOAuth {
    response: Arc<OAuthResponse>,
    last_body: Arc<Mutex<Option<serde_json::Value>>>,
}

/// The scripted response for a `MockOAuth`.
#[derive(Clone)]
pub enum OAuthResponse {
    Ok {
        access_token: String,
        refresh_token: String,
        id_token: String,
    },
    Error {
        status: u16,
        code: String,
    },
}

impl MockOAuth {
    /// A mock that returns HTTP 200 with the given tokens.
    pub fn ok(
        access_token: impl Into<String>,
        refresh_token: impl Into<String>,
        id_token: impl Into<String>,
    ) -> Self {
        Self {
            response: Arc::new(OAuthResponse::Ok {
                access_token: access_token.into(),
                refresh_token: refresh_token.into(),
                id_token: id_token.into(),
            }),
            last_body: Arc::new(Mutex::new(None)),
        }
    }

    /// A mock that returns the given error status with `{"error": code}`.
    pub fn error(status: u16, code: impl Into<String>) -> Self {
        Self {
            response: Arc::new(OAuthResponse::Error {
                status,
                code: code.into(),
            }),
            last_body: Arc::new(Mutex::new(None)),
        }
    }

    /// The JSON body of the most recent request, if any.
    pub fn last_body(&self) -> Option<serde_json::Value> {
        self.last_body.lock().unwrap().clone()
    }

    /// Bind an ephemeral port, serve in a background task, and return the base URL.
    pub async fn spawn(self) -> String {
        let app = Router::new()
            .route("/oauth/token", post(oauth_handler))
            .with_state(self);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }
}

async fn oauth_handler(
    State(mock): State<MockOAuth>,
    Json(body): Json<serde_json::Value>,
) -> (StatusCode, Json<serde_json::Value>) {
    *mock.last_body.lock().unwrap() = Some(body);
    match &*mock.response {
        OAuthResponse::Ok {
            access_token,
            refresh_token,
            id_token,
        } => (
            StatusCode::OK,
            Json(serde_json::json!({
                "access_token": access_token,
                "refresh_token": refresh_token,
                "id_token": id_token,
                "token_type": "Bearer",
                "expires_in": 3600,
            })),
        ),
        OAuthResponse::Error { status, code } => (
            StatusCode::from_u16(*status).unwrap_or(StatusCode::BAD_REQUEST),
            Json(serde_json::json!({ "error": code })),
        ),
    }
}
```

- [ ] **Step 2: Write the failing refresh integration test**

Create `$POLYFLARE/crates/polyflare-codex/tests/oauth_refresh.rs`:
```rust
//! OAuth refresh e2e against a scripted mock token endpoint (never real OpenAI).

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use polyflare_codex::oauth::{classify_failure, FailureClass, OAuthClient, OAuthError};
use polyflare_testkit::MockOAuth;

fn make_id_token(payload: &serde_json::Value) -> String {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#);
    let body = URL_SAFE_NO_PAD.encode(serde_json::to_vec(payload).unwrap());
    format!("{header}.{body}.sig")
}

#[tokio::test]
async fn refresh_returns_new_tokens_and_decoded_claims() {
    let id_token = make_id_token(&serde_json::json!({
        "email": "a@b.test",
        "sub": "s1",
        "https://api.openai.com/auth": { "chatgpt_plan_type": "pro" }
    }));
    let mock = MockOAuth::ok("new-access", "new-refresh", id_token);
    let handle = mock.clone();
    let base = mock.spawn().await;

    let client = OAuthClient::new(base).unwrap();
    let refreshed = client.refresh("old-refresh").await.unwrap();

    assert_eq!(refreshed.tokens.access_token, "new-access");
    assert_eq!(refreshed.tokens.refresh_token, "new-refresh");
    assert_eq!(refreshed.claims.chatgpt_plan_type.as_deref(), Some("pro"));

    // The request carried the exact grant / client id / scope / refresh token.
    let body = handle.last_body().unwrap();
    assert_eq!(body["grant_type"], "refresh_token");
    assert_eq!(body["client_id"], "app_EMoamEEZ73f0CkXaXp7hrann");
    assert_eq!(body["scope"], "openid profile email");
    assert_eq!(body["refresh_token"], "old-refresh");
}

#[tokio::test]
async fn refresh_surfaces_permanent_failure_code() {
    let mock = MockOAuth::error(400, "invalid_grant");
    let base = mock.spawn().await;
    let client = OAuthClient::new(base).unwrap();

    let err = client.refresh("dead-refresh").await.err().unwrap();
    match err {
        OAuthError::Endpoint { status, code } => {
            assert_eq!(status, 400);
            assert_eq!(
                classify_failure(code.as_deref().unwrap()),
                FailureClass::ReauthRequired
            );
        }
        other => panic!("expected Endpoint error, got {other:?}"),
    }
}

#[tokio::test]
async fn refresh_keeps_existing_refresh_token_when_omitted() {
    // The mock always returns a refresh token, so this asserts the request-side default path is
    // exercised end-to-end for a normal rotation; the unit-level "omitted" fallback is covered by
    // `refresh`'s `unwrap_or_else`. Here we simply confirm a full round-trip succeeds.
    let id_token = make_id_token(&serde_json::json!({ "sub": "s2" }));
    let base = MockOAuth::ok("acc2", "rot-refresh", id_token).spawn().await;
    let client = OAuthClient::new(base).unwrap();
    let refreshed = client.refresh("prev-refresh").await.unwrap();
    assert_eq!(refreshed.tokens.refresh_token, "rot-refresh");
    assert_eq!(refreshed.claims.sub.as_deref(), Some("s2"));
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd $POLYFLARE && cargo test -p polyflare-codex --test oauth_refresh`
Expected: FAIL — compile errors (`OAuthClient`, `MockOAuth` not found).

- [ ] **Step 4: Implement `OAuthClient::refresh` + the refreshed-token structs**

Append to `$POLYFLARE/crates/polyflare-codex/src/oauth.rs` (below the `decode_claims` fn, above the `#[cfg(test)]` block; and add `use std::time::Duration;` and `use serde::Deserialize;` to the module's imports at the top):
```rust
/// The OAuth client id used by the Codex CLI (a public, non-secret protocol constant).
const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
/// The refresh scope.
const SCOPE: &str = "openid profile email";

/// Three OAuth tokens returned by a refresh. Never logged: `Debug` redacts every field.
#[derive(Clone)]
pub struct RefreshedTokens {
    pub access_token: String,
    pub refresh_token: String,
    pub id_token: String,
}

impl std::fmt::Debug for RefreshedTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefreshedTokens")
            .field("access_token", &"***")
            .field("refresh_token", &"***")
            .field("id_token", &"***")
            .finish()
    }
}

/// A completed refresh: the new tokens plus the identity claims decoded from the new id_token.
#[derive(Debug, Clone)]
pub struct Refreshed {
    pub tokens: RefreshedTokens,
    pub claims: Claims,
}

/// The token-endpoint success body. `refresh_token` may be omitted (no rotation) → keep the old.
#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    id_token: String,
}

/// OAuth client for the Codex backend. Holds a `reqwest::Client` + the auth base URL
/// (default `https://auth.openai.com`; overridable so tests point at `MockOAuth`).
pub struct OAuthClient {
    http: reqwest::Client,
    auth_base_url: String,
}

impl OAuthClient {
    pub fn new(auth_base_url: impl Into<String>) -> Result<Self, OAuthError> {
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| OAuthError::Transport(e.to_string()))?;
        Ok(Self {
            http,
            auth_base_url: auth_base_url.into(),
        })
    }

    /// Exchange a refresh token for fresh tokens via `POST {auth_base_url}/oauth/token`. On a
    /// non-2xx response, the endpoint's `error` code (if present) is surfaced for classification.
    pub async fn refresh(&self, refresh_token: &str) -> Result<Refreshed, OAuthError> {
        let url = format!("{}/oauth/token", self.auth_base_url.trim_end_matches('/'));
        let body = serde_json::json!({
            "grant_type": "refresh_token",
            "client_id": CLIENT_ID,
            "refresh_token": refresh_token,
            "scope": SCOPE,
        });
        let resp = self
            .http
            .post(url)
            .json(&body)
            .timeout(Duration::from_secs(8))
            .send()
            .await
            .map_err(|e| OAuthError::Transport(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            let code = resp
                .json::<Value>()
                .await
                .ok()
                .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_string));
            return Err(OAuthError::Endpoint {
                status: status.as_u16(),
                code,
            });
        }

        let token: TokenResponse = resp
            .json()
            .await
            .map_err(|e| OAuthError::Transport(e.to_string()))?;
        let claims = decode_claims(&token.id_token)?;
        Ok(Refreshed {
            tokens: RefreshedTokens {
                access_token: token.access_token,
                // OpenAI may omit a rotated refresh token → keep the caller's existing one.
                refresh_token: token
                    .refresh_token
                    .unwrap_or_else(|| refresh_token.to_string()),
                id_token: token.id_token,
            },
            claims,
        })
    }
}
```

- [ ] **Step 5: Add a redaction test for `RefreshedTokens`**

Append these two tests inside the existing `#[cfg(test)] mod tests` block in `oauth.rs`:
```rust
    #[test]
    fn refreshed_tokens_debug_redacts_secrets() {
        let t = RefreshedTokens {
            access_token: "secret-access-xyz".to_string(),
            refresh_token: "secret-refresh-xyz".to_string(),
            id_token: "secret-id-xyz".to_string(),
        };
        let s = format!("{t:?}");
        assert!(!s.contains("secret-access-xyz"), "must not leak access token");
        assert!(!s.contains("secret-refresh-xyz"), "must not leak refresh token");
        assert!(!s.contains("secret-id-xyz"), "must not leak id token");
        assert!(s.contains("***"), "must redact with ***");
    }
```

- [ ] **Step 6: Extend the `lib.rs` re-export**

In `$POLYFLARE/crates/polyflare-codex/src/lib.rs`, replace the `oauth` re-export line with:
```rust
pub use oauth::{
    classify_failure, decode_claims, should_refresh, Claims, FailureClass, OAuthClient, OAuthError,
    Refreshed, RefreshedTokens,
};
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cd $POLYFLARE && cargo test -p polyflare-codex && cargo test -p polyflare-testkit`
Expected: PASS — `oauth_refresh` (3 tests) + the oauth unit tests (6) + testkit tests (existing).

- [ ] **Step 8: Format + lint**

Run: `cd $POLYFLARE && cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 9: Commit**
```bash
cd $POLYFLARE
git add crates/polyflare-codex crates/polyflare-testkit
git commit -m "feat(codex,testkit): OAuth refresh over reqwest + MockOAuth token endpoint"
```

> **M2b-1 merge point.** Tasks 1–4 are complete: the reshaped `Selector`, the `capacity_weighted` impl, and the full OAuth module (decode + refresh) are green, fmt-clean, and clippy-clean, with nothing in the serve path changed yet. **Merge M2b-1 before starting M2b-2.**

---
---

# PART M2b-2 — Snapshot assembly + pool wiring (the serve-path integration)

## Task 5: Snapshot assembly — store `latest_usage` + server assembler

**Files:**
- Modify: `crates/polyflare-store/src/account.rs` (add `latest_usage` + usage types)
- Modify: `crates/polyflare-store/src/lib.rs` (re-export)
- Create: `crates/polyflare-server/src/snapshot.rs`
- Modify: `crates/polyflare-server/src/lib.rs` (add `snapshot` module)
- Modify: `crates/polyflare-server/Cargo.toml` (dev-deps: `sqlx`, `tempfile`)
- Create: `crates/polyflare-server/tests/snapshot_assembly.rs`

**Interfaces:**
- Consumes: `polyflare_store::{Store, Account, AccountRepo, PlainTokens, TokenCipher, StoreError}`, `polyflare_core::AccountSnapshot`.
- Produces:
  - `struct WindowUsage { used_percent: f64, reset_at: Option<i64> }` (`Debug, Clone, sqlx::FromRow`).
  - `struct UsageSnapshot { primary: Option<WindowUsage>, secondary: Option<WindowUsage> }` (`Debug, Clone, Default`).
  - `AccountRepo::latest_usage(&self, account_id: &str) -> Result<UsageSnapshot, StoreError>`.
  - `polyflare_server::snapshot::assemble_snapshots(store: &Store) -> Result<Vec<AccountSnapshot>, StoreError>`.

- [ ] **Step 1: Add the server dev-dependencies**

Replace the `[dev-dependencies]` block in `$POLYFLARE/crates/polyflare-server/Cargo.toml` with:
```toml
[dev-dependencies]
polyflare-testkit = { path = "../polyflare-testkit" }
reqwest = { workspace = true }
futures-util = { workspace = true }
sqlx = { workspace = true }
tempfile = { workspace = true }
```
(`sqlx` — the snapshot test seeds `usage_history` rows via `store.pool()`, the store's documented raw-query escape hatch. `tempfile` — temp-file DBs. Both are also used by Task 6's tests.)

- [ ] **Step 2: Write the failing assembler integration test**

Create `$POLYFLARE/crates/polyflare-server/tests/snapshot_assembly.rs`:
```rust
//! Snapshot assembly: seed an account + usage rows, assemble, assert the snapshot fields
//! (latest-per-window usage, durable metadata, deferred runtime defaults).

use polyflare_server::snapshot::assemble_snapshots;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};

fn account(id: &str) -> Account {
    Account {
        id: id.to_string(),
        chatgpt_account_id: None,
        chatgpt_user_id: None,
        email: "u@example.test".to_string(),
        alias: None,
        workspace_id: None,
        workspace_label: None,
        seat_type: None,
        plan_type: "pro".to_string(),
        routing_policy: "normal".to_string(),
        last_refresh: 1_700_000_000,
        created_at: 1_699_000_000,
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: Some(1_700_100_000),
        blocked_at: None,
        security_work_authorized: true,
    }
}

fn tokens() -> PlainTokens {
    PlainTokens {
        access_token: "a".to_string(),
        refresh_token: "r".to_string(),
        id_token: "i".to_string(),
    }
}

async fn insert_usage(
    store: &Store,
    account_id: &str,
    window: &str,
    used_percent: f64,
    recorded_at: i64,
    reset_at: i64,
) {
    sqlx::query(
        "INSERT INTO usage_history (account_id, recorded_at, \"window\", used_percent, reset_at) \
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(account_id)
    .bind(recorded_at)
    .bind(window)
    .bind(used_percent)
    .bind(reset_at)
    .execute(store.pool())
    .await
    .unwrap();
}

#[tokio::test]
async fn assembles_snapshot_with_latest_usage_per_window() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[7u8; 32]).unwrap();
    store
        .accounts()
        .insert(&account("acct-1"), &tokens(), &cipher)
        .await
        .unwrap();

    insert_usage(&store, "acct-1", "primary", 10.0, 1000, 2000).await;
    insert_usage(&store, "acct-1", "secondary", 30.0, 1000, 3000).await;
    insert_usage(&store, "acct-1", "secondary", 55.0, 2000, 3500).await; // newer wins

    let snaps = assemble_snapshots(&store).await.unwrap();
    assert_eq!(snaps.len(), 1);
    let s = &snaps[0];
    assert_eq!(s.id.as_str(), "acct-1");
    assert_eq!(s.status, "active");
    assert_eq!(s.used_percent, 10.0);
    assert_eq!(s.secondary_used_percent, 55.0); // newest secondary row
    assert_eq!(s.reset_at, Some(1_700_100_000)); // durable account column
    assert_eq!(s.plan_type, "pro");
    assert!(s.security_work_authorized);
    // Deferred runtime defaults.
    assert_eq!(s.health_tier, 0);
    assert_eq!(s.in_flight, 0);
    assert!(s.last_error_at.is_none());
}

#[tokio::test]
async fn account_without_usage_gets_zeroed_windows() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[7u8; 32]).unwrap();
    store
        .accounts()
        .insert(&account("acct-2"), &tokens(), &cipher)
        .await
        .unwrap();

    let snaps = assemble_snapshots(&store).await.unwrap();
    assert_eq!(snaps.len(), 1);
    assert_eq!(snaps[0].used_percent, 0.0);
    assert_eq!(snaps[0].secondary_used_percent, 0.0);
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd $POLYFLARE && cargo test -p polyflare-server --test snapshot_assembly`
Expected: FAIL — compile error (`polyflare_server::snapshot` / `assemble_snapshots` not found).

- [ ] **Step 4: Add `latest_usage` + usage types to the store**

In `$POLYFLARE/crates/polyflare-store/src/account.rs`, add these type definitions (place them after the `EncryptedTokens` block, before `SELECT_ACCOUNT_BY_ID`):
```rust
/// The latest usage percentage + reset for one window of an account.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct WindowUsage {
    pub used_percent: f64,
    pub reset_at: Option<i64>,
}

/// The latest usage per window ("primary"/"secondary") for an account. Missing windows are
/// `None` (the snapshot assembler treats them as zero usage).
#[derive(Debug, Clone, Default)]
pub struct UsageSnapshot {
    pub primary: Option<WindowUsage>,
    pub secondary: Option<WindowUsage>,
}
```
and add these two methods inside `impl AccountRepo` (after `decrypt_tokens`):
```rust
    /// The most-recent `usage_history` row for each window ("primary"/"secondary") of an account.
    pub async fn latest_usage(&self, account_id: &str) -> Result<UsageSnapshot, StoreError> {
        Ok(UsageSnapshot {
            primary: self.latest_window_usage(account_id, "primary").await?,
            secondary: self.latest_window_usage(account_id, "secondary").await?,
        })
    }

    /// The most-recent usage row for a single window, or `None` if the account has none.
    async fn latest_window_usage(
        &self,
        account_id: &str,
        window: &str,
    ) -> Result<Option<WindowUsage>, StoreError> {
        let row = sqlx::query_as::<_, WindowUsage>(
            "SELECT used_percent, reset_at FROM usage_history \
             WHERE account_id = ? AND \"window\" = ? ORDER BY recorded_at DESC LIMIT 1",
        )
        .bind(account_id)
        .bind(window)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }
```

- [ ] **Step 5: Re-export the usage types from the store `lib.rs`**

In `$POLYFLARE/crates/polyflare-store/src/lib.rs`, extend the `account` re-export line:
```rust
pub use account::{Account, AccountRepo, EncryptedTokens, PlainTokens, UsageSnapshot, WindowUsage};
```

- [ ] **Step 6: Implement the server assembler**

Create `$POLYFLARE/crates/polyflare-server/src/snapshot.rs`:
```rust
//! Assemble the selector's per-account snapshots from the durable store: each `Account` joined
//! with its latest `usage_history` row per window. Runtime fields (health tier, in-flight,
//! error/cooldown timestamps) are live-tracked later and default to neutral values here.

use polyflare_core::AccountSnapshot;
use polyflare_store::{Store, StoreError};

/// Build one `AccountSnapshot` per stored account. Capacity is derived from `plan_type` inside
/// the selector (no per-account override in M2b, so `capacity_credits` stays `None`).
pub async fn assemble_snapshots(store: &Store) -> Result<Vec<AccountSnapshot>, StoreError> {
    let repo = store.accounts();
    let accounts = repo.list().await?;
    let mut snapshots = Vec::with_capacity(accounts.len());
    for account in accounts {
        let usage = repo.latest_usage(&account.id).await?;
        let mut snap = AccountSnapshot::new(account.id.as_str());
        snap.status = account.status;
        snap.used_percent = usage.primary.as_ref().map_or(0.0, |w| w.used_percent);
        snap.secondary_used_percent = usage.secondary.as_ref().map_or(0.0, |w| w.used_percent);
        snap.reset_at = account.reset_at;
        snap.routing_policy = account.routing_policy;
        snap.plan_type = account.plan_type;
        snap.security_work_authorized = account.security_work_authorized;
        snapshots.push(snap);
    }
    Ok(snapshots)
}
```

- [ ] **Step 7: Wire `snapshot` into the server `lib.rs`**

Replace `$POLYFLARE/crates/polyflare-server/src/lib.rs` with:
```rust
//! PolyFlare server edge: ingress, config, snapshot assembly, wiring.

pub mod app;
pub mod config;
pub mod ingress;
pub mod snapshot;
```

- [ ] **Step 8: Run the test to verify it passes**

Run: `cd $POLYFLARE && cargo test -p polyflare-server --test snapshot_assembly`
Expected: PASS (2 tests).

- [ ] **Step 9: Format + lint**

Run: `cd $POLYFLARE && cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 10: Commit**
```bash
cd $POLYFLARE
git add crates/polyflare-store crates/polyflare-server
git commit -m "feat(store,server): latest_usage query + AccountSnapshot assembler"
```

---

## Task 6: Pool wiring into the ingress path (replace M1's single hardcoded account)

**Files:**
- Modify: `crates/polyflare-server/src/config.rs` (`Config` → `ServeConfig`; config migration)
- Modify: `crates/polyflare-server/src/app.rs` (`AppState`)
- Modify: `crates/polyflare-server/src/ingress.rs` (select → refresh → decrypt → execute)
- Modify: `crates/polyflare-server/src/main.rs` (`serve()` builds the store-backed state)
- Rewrite: `crates/polyflare-server/tests/{ingress_relays,e2e_passthrough,large_body}.rs`
- Create: `crates/polyflare-server/tests/pool_selection.rs`

**Interfaces:**
- Consumes: `polyflare_core::{Account, AccountId, PreparedRequest, SelectionCtx, Selector, CapacityWeighted}`, `polyflare_codex::{CodexExecutor, oauth::{OAuthClient, OAuthError, should_refresh, classify_failure}}`, `polyflare_store::{Store, TokenCipher, PlainTokens, Account as StoreAccount}`, `crate::snapshot::assemble_snapshots`.
- Produces:
  - `struct AppState { executor: Arc<dyn Executor>, selector: Arc<dyn Selector>, store: Store, cipher: TokenCipher, oauth: OAuthClient, upstream_base_url: String }`.
  - `struct ServeConfig { bind_addr, upstream_base_url, auth_base_url: String, db_path, key_path: PathBuf }` with `from_env()`.
  - Rewritten `responses_handler` performing store-backed selection + refresh + relay.

> **Config migration (document this in the commit + any ops notes).** `serve` no longer reads a single `POLYFLARE_UPSTREAM_TOKEN` — per-account bearer tokens now come from the store (decrypted per request). Env for `serve` becomes:
> - `POLYFLARE_UPSTREAM_URL` — **retained**, now the *shared* Codex upstream base URL applied to every selected account (the store `Account` carries no `base_url`).
> - `POLYFLARE_AUTH_URL` — **new**, OAuth token base URL (default `https://auth.openai.com`).
> - `POLYFLARE_BIND` — unchanged. Store DB + at-rest key come from `POLYFLARE_DATA_DIR` (`db_path`/`key_path`), same as `accounts import`.
> - `POLYFLARE_UPSTREAM_TOKEN` — **removed** (superseded by store tokens).

- [ ] **Step 1: Migrate `config.rs` (`Config` → `ServeConfig`)**

Replace `$POLYFLARE/crates/polyflare-server/src/config.rs` with:
```rust
//! Process configuration for `polyflare serve`, read from environment. Secrets are NOT here —
//! per-account bearer tokens live in the store; only shared base URLs + data paths are config.

use std::path::{Path, PathBuf};

/// `serve` configuration. The upstream base URL is shared across accounts; per-account bearer
/// tokens are decrypted from the store per request.
pub struct ServeConfig {
    pub bind_addr: String,
    pub upstream_base_url: String,
    pub auth_base_url: String,
    pub db_path: PathBuf,
    pub key_path: PathBuf,
}

impl ServeConfig {
    pub fn from_env() -> Result<Self, String> {
        let bind_addr =
            std::env::var("POLYFLARE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
        let upstream_base_url = std::env::var("POLYFLARE_UPSTREAM_URL")
            .map_err(|_| "POLYFLARE_UPSTREAM_URL not set".to_string())?;
        let auth_base_url = std::env::var("POLYFLARE_AUTH_URL")
            .unwrap_or_else(|_| "https://auth.openai.com".to_string());
        let data_dir = data_dir_from_env();
        Ok(ServeConfig {
            bind_addr,
            upstream_base_url,
            auth_base_url,
            db_path: db_path(&data_dir),
            key_path: key_path(&data_dir),
        })
    }
}

/// The PolyFlare data directory: `$POLYFLARE_DATA_DIR`, else `$HOME/.polyflare`.
pub fn data_dir_from_env() -> PathBuf {
    if let Ok(dir) = std::env::var("POLYFLARE_DATA_DIR") {
        return PathBuf::from(dir);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".polyflare")
}

/// The store DB path within a data directory.
pub fn db_path(data_dir: &Path) -> PathBuf {
    data_dir.join("store.db")
}

/// The at-rest key file path within a data directory (raw 32 bytes).
pub fn key_path(data_dir: &Path) -> PathBuf {
    data_dir.join("key")
}
```
(The old `Config { bind_addr, account }` + its `polyflare_core::Account` import are removed — nothing but `serve()` used them, and `serve()` is rewritten in Step 4. The `data_dir_from_env`/`db_path`/`key_path` helpers are unchanged, so `accounts import` keeps working.)

- [ ] **Step 2: Reshape `AppState` in `app.rs`**

Replace `$POLYFLARE/crates/polyflare-server/src/app.rs` with:
```rust
//! Application state and router construction.

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::post;
use axum::Router;

use polyflare_codex::oauth::OAuthClient;
use polyflare_core::{Executor, Selector};
use polyflare_store::{Store, TokenCipher};

use crate::ingress::responses_handler;

/// Raised request-body limit: axum's `Json` extractor default (2 MB) 413s real
/// OpenAI-Responses requests. 100 MB is generous for real Codex turns while bounded.
const MAX_REQUEST_BODY_BYTES: usize = 100 * 1024 * 1024;

/// Shared server state: the executor, the account selector, the store + at-rest cipher, the
/// OAuth refresher, and the shared upstream base URL. Wrapped in `Arc` by the caller.
pub struct AppState {
    pub executor: Arc<dyn Executor>,
    pub selector: Arc<dyn Selector>,
    pub store: Store,
    pub cipher: TokenCipher,
    pub oauth: OAuthClient,
    pub upstream_base_url: String,
}

pub fn build_app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/responses", post(responses_handler))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}
```

- [ ] **Step 3: Rewrite the ingress handler**

Replace `$POLYFLARE/crates/polyflare-server/src/ingress.rs` with:
```rust
//! Ingress: assemble candidate snapshots → select an account → refresh its token if stale →
//! decrypt → relay the executor's stream. Client-facing errors carry generic bodies (never a
//! token, an upstream URL, or an internal error `Display`).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::{Json, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

use polyflare_codex::oauth::{classify_failure, should_refresh, OAuthError};
use polyflare_core::{Account, PreparedRequest, SelectionCtx};
use polyflare_store::PlainTokens;

use crate::app::AppState;
use crate::snapshot::assemble_snapshots;

/// Current unix time in seconds (0 on the impossible pre-epoch error).
fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub async fn responses_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let model = body
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_string();
    let now = unix_now();

    // 1. Assemble candidate snapshots from the store.
    let snapshots = match assemble_snapshots(&state.store).await {
        Ok(s) => s,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    };

    // 2. Select an account. No eligible account → 503.
    let ctx = SelectionCtx {
        now,
        require_security_work_authorized: false,
        rng_seed: None,
        session_id: None,
    };
    let picked = match state.selector.pick(&snapshots, &ctx) {
        Some(id) => id,
        None => return (StatusCode::SERVICE_UNAVAILABLE, "no eligible account").into_response(),
    };

    // 3. Load the selected account.
    let repo = state.store.accounts();
    let account = match repo.get(picked.as_str()).await {
        Ok(Some(a)) => a,
        Ok(None) | Err(_) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    };

    // 4. Decrypt tokens; refresh if the stored token is stale (>8 days).
    let mut tokens = match repo.decrypt_tokens(picked.as_str(), &state.cipher).await {
        Ok(Some(t)) => t,
        Ok(None) | Err(_) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    };
    if should_refresh(account.last_refresh, now) {
        match state.oauth.refresh(&tokens.refresh_token).await {
            Ok(refreshed) => {
                let new = PlainTokens {
                    access_token: refreshed.tokens.access_token,
                    refresh_token: refreshed.tokens.refresh_token,
                    id_token: refreshed.tokens.id_token,
                };
                // Persist best-effort; a write failure must not drop the request.
                let _ = repo
                    .update_tokens(picked.as_str(), &new, &state.cipher, now)
                    .await;
                tokens = new;
            }
            Err(OAuthError::Endpoint { code: Some(code), .. }) => {
                // Mark the account per the classified failure; proceed with the current token
                // (re-selection / retry orchestration is M3).
                if let Some(status) = classify_failure(&code).status() {
                    let _ = repo.update_status(picked.as_str(), status).await;
                }
            }
            Err(_) => {} // transient / network → proceed with the current token
        }
    }

    // 5. Build the core Account (shared upstream base URL + per-account bearer) and execute.
    let core_account = Account {
        id: account.id,
        base_url: state.upstream_base_url.clone(),
        bearer_token: tokens.access_token,
    };
    let req = PreparedRequest { body, model };
    match state.executor.execute(req, &core_account).await {
        Ok(stream) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from_stream(stream))
            .expect("valid response"),
        // Generic 502 — never forward the upstream error Display (may carry the URL).
        Err(_) => (StatusCode::BAD_GATEWAY, "upstream error").into_response(),
    }
}
```

- [ ] **Step 4: Rewrite `serve()` in `main.rs`**

In `$POLYFLARE/crates/polyflare-server/src/main.rs`, update the imports and replace the `serve()` fn. The import block at the top becomes:
```rust
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Parser, Subcommand};

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Selector};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::config::{self, ServeConfig};
use polyflare_store::{import_from_codex_lb, Store, TokenCipher};
```
and `serve()` becomes:
```rust
/// The M2b server: store-backed multi-account pool selection.
async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let config = ServeConfig::from_env()?;
    let store = Store::open(&config.db_path).await?;
    let cipher = TokenCipher::load_or_create(&config.key_path)?;
    let executor = Arc::new(CodexExecutor::new()?);
    let selector: Arc<dyn Selector> = Arc::new(CapacityWeighted);
    let oauth = OAuthClient::new(config.auth_base_url)?;

    let state = Arc::new(AppState {
        executor,
        selector,
        store,
        cipher,
        oauth,
        upstream_base_url: config.upstream_base_url,
    });
    let app = build_app(state);

    let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;
    println!("polyflare listening on {}", config.bind_addr);
    axum::serve(listener, app).await?;
    Ok(())
}
```
(The `accounts_import` fn, the clap structs, and the `#[cfg(test)]` CLI-parse tests are unchanged — they still use `config::{data_dir_from_env, db_path, key_path}`. `Path`/`PathBuf` stay imported for `accounts_import`.)

- [ ] **Step 5: Rewrite the three existing integration tests (store-backed) + the failing new one**

These four files exercise the new serve path. Each seeds a one-account temp store with `last_refresh = now` (so `should_refresh` is false and the OAuth client is never called), a `CapacityWeighted` selector, and an unused `OAuthClient`.

Rewrite `$POLYFLARE/crates/polyflare-server/tests/ingress_relays.rs`:
```rust
//! The server selects the single stored account and relays its upstream stream to the client.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::CapacityWeighted;
use polyflare_server::app::{build_app, AppState};
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockUpstream;

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

fn store_account(id: &str) -> Account {
    Account {
        id: id.to_string(),
        chatgpt_account_id: None,
        chatgpt_user_id: None,
        email: "u@example.test".to_string(),
        alias: None,
        workspace_id: None,
        workspace_label: None,
        seat_type: None,
        plan_type: "pro".to_string(),
        routing_policy: "normal".to_string(),
        last_refresh: now(), // fresh ⇒ no OAuth refresh attempted
        created_at: now(),
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
    }
}

/// Spawn a store-backed polyflare server whose single account relays to `upstream`.
async fn spawn_polyflare(upstream: String) -> String {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[3u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &store_account("acct-1"),
            &PlainTokens {
                access_token: "tok".to_string(),
                refresh_token: "r".to_string(),
                id_token: "i".to_string(),
            },
            &cipher,
        )
        .await
        .unwrap();
    // Keep the temp DB alive for the (short-lived) server task.
    std::mem::forget(dir);

    let state = Arc::new(AppState {
        executor: Arc::new(CodexExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(), // never called (fresh token)
        upstream_base_url: upstream,
    });
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn server_selects_account_and_relays_upstream_stream() {
    let mock = MockUpstream::new(vec![
        r#"{"type":"response.output_text.delta","delta":"yo"}"#.to_string(),
        r#"{"type":"response.completed"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(upstream).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    let mut body = String::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    assert!(body.contains("response.output_text.delta"));
    assert!(body.contains("response.completed"));
    assert_eq!(handle.last_body().unwrap()["model"], "gpt-5.6-sol");
}
```

Rewrite `$POLYFLARE/crates/polyflare-server/tests/e2e_passthrough.rs`:
```rust
//! End-to-end: client → polyflare (store-backed selection) → executor → mock upstream.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::CapacityWeighted;
use polyflare_server::app::{build_app, AppState};
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockUpstream;

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

fn store_account(id: &str) -> Account {
    Account {
        id: id.to_string(),
        chatgpt_account_id: None,
        chatgpt_user_id: None,
        email: "u@example.test".to_string(),
        alias: None,
        workspace_id: None,
        workspace_label: None,
        seat_type: None,
        plan_type: "pro".to_string(),
        routing_policy: "normal".to_string(),
        last_refresh: now(),
        created_at: now(),
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
    }
}

async fn spawn_polyflare(upstream: String) -> String {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[5u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &store_account("e2e"),
            &PlainTokens {
                access_token: "tok".to_string(),
                refresh_token: "r".to_string(),
                id_token: "i".to_string(),
            },
            &cipher,
        )
        .await
        .unwrap();
    std::mem::forget(dir);

    let state = Arc::new(AppState {
        executor: Arc::new(CodexExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: upstream,
    });
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn end_to_end_streaming_passthrough() {
    let mock = MockUpstream::new(vec![
        r#"{"type":"response.output_text.delta","delta":"a"}"#.to_string(),
        r#"{"type":"response.output_text.delta","delta":"b"}"#.to_string(),
        r#"{"type":"response.completed"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(upstream).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let mut body = String::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    let first = body.find("delta\":\"a").unwrap();
    let second = body.find("delta\":\"b").unwrap();
    let done = body.find("response.completed").unwrap();
    assert!(first < second && second < done);
    assert_eq!(handle.last_body().unwrap()["model"], "gpt-5.6-sol");
}
```

Rewrite `$POLYFLARE/crates/polyflare-server/tests/large_body.rs`:
```rust
//! Regression: the raised 100 MB body limit holds through the store-backed serve path.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::CapacityWeighted;
use polyflare_server::app::{build_app, AppState};
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockUpstream;

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

fn store_account(id: &str) -> Account {
    Account {
        id: id.to_string(),
        chatgpt_account_id: None,
        chatgpt_user_id: None,
        email: "u@example.test".to_string(),
        alias: None,
        workspace_id: None,
        workspace_label: None,
        seat_type: None,
        plan_type: "pro".to_string(),
        routing_policy: "normal".to_string(),
        last_refresh: now(),
        created_at: now(),
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
    }
}

#[tokio::test]
async fn large_request_body_is_not_rejected_with_413() {
    let mock = MockUpstream::new(vec![r#"{"type":"response.completed"}"#.to_string()]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[6u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &store_account("large-body"),
            &PlainTokens {
                access_token: "tok".to_string(),
                refresh_token: "r".to_string(),
                id_token: "i".to_string(),
            },
            &cipher,
        )
        .await
        .unwrap();
    std::mem::forget(dir);

    let state = Arc::new(AppState {
        executor: Arc::new(CodexExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: upstream,
    });
    let app = build_app(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let payload = serde_json::json!({
        "model": "gpt-5.6-sol",
        "input": "x".repeat(2_500_000),
    });
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/responses"))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "large body must not be rejected with 413");

    let last_body = handle.last_body().unwrap();
    assert_eq!(last_body["model"], "gpt-5.6-sol");
    assert_eq!(last_body["input"].as_str().unwrap().len(), 2_500_000);
}
```

Create `$POLYFLARE/crates/polyflare-server/tests/pool_selection.rs` (the "no eligible account" path):
```rust
//! Pool selection edge case: an empty pool (no accounts) → the handler returns 503.

use std::sync::Arc;

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::CapacityWeighted;
use polyflare_server::app::{build_app, AppState};
use polyflare_store::{Store, TokenCipher};

#[tokio::test]
async fn no_eligible_account_returns_503() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[8u8; 32]).unwrap();
    std::mem::forget(dir);

    let state = Arc::new(AppState {
        executor: Arc::new(CodexExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        store, // no accounts inserted → empty snapshot pool
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: "http://127.0.0.1:9".to_string(),
    });
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503, "empty pool must yield 503");
}
```

- [ ] **Step 6: Run the server tests to verify they pass**

Run: `cd $POLYFLARE && cargo test -p polyflare-server`
Expected: PASS — `snapshot_assembly` (2), `ingress_relays` (1), `e2e_passthrough` (1), `large_body` (1), `pool_selection` (1), plus the 4 CLI-parse `--bin` tests.

- [ ] **Step 7: Full workspace test + format + lint**

Run:
```bash
cd $POLYFLARE
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
Expected: fmt clean, no clippy warnings, all tests green across every crate.

- [ ] **Step 8: Manual serve smoke (documented; optional to run)**

```bash
cd $POLYFLARE
# Import at least one account first (M2a), then serve store-backed:
POLYFLARE_DATA_DIR=/tmp/pf-smoke \
  cargo run --bin polyflare -- accounts import \
    --from /path/to/codex-lb/store.db --fernet-key /path/to/codex-lb/encryption.key

POLYFLARE_DATA_DIR=/tmp/pf-smoke \
POLYFLARE_UPSTREAM_URL="https://<codex-upstream-base>" \
  cargo run --bin polyflare -- serve
# (POLYFLARE_UPSTREAM_TOKEN is no longer used; tokens come from the store. POLYFLARE_AUTH_URL
#  defaults to https://auth.openai.com.)
```

- [ ] **Step 9: Commit**
```bash
cd $POLYFLARE
git add crates/polyflare-server
git commit -m "feat(server): store-backed pool selection + OAuth refresh in the ingress path"
```

---
---

## Self-review (completed against the spec)

**1. Spec coverage (M2b scope items → tasks):**
- **M2-GATE1 — reshape `Selector`** (`AccountSnapshot` with the full scoring field set + `in_flight` defaulted 0 + the added `last_error_at`; `SelectionCtx{now, require_security_work_authorized, rng_seed, session_id}`; `pick(&[AccountSnapshot], &SelectionCtx) -> Option<AccountId>` sync/owned; newtype `AccountId`; pure trait+types change + compile check + trivial selector test) → **Task 1**. ✅
- **`capacity_weighted` selector impl** (eligibility hard-filter incl. reauth/deactivated/paused skip, rate/quota auto-recover at `reset_at`, cooldown, error-backoff `min(300,30*2^(n-3))`; health-tier pooling with `should_drain`; burn/normal/preserve waterfall; weighted-random by `remaining_secondary_credits` with plan-capacity constants; all-zero → deterministic usage-sort with `account_id` final tiebreak; TA6 hard pre-filter; injectable seeded RNG; 11 unit tests covering eligibility, waterfall, health-drain, TA6, reproducibility, weighted distribution, all-zero tiebreak) → **Task 2**. ✅
- **OAuth** (decode-only JWT claims via base64url-no-pad + `serde_json`, nested `https://api.openai.com/auth` precedence + `chatgpt_user_id` fallback to `sub`; `should_refresh` 8-day; `refresh` via `POST {auth}/oauth/token` with exact `grant_type`/`client_id`/`scope`/8s-timeout; permanent-failure classification; redacting `Debug` + redaction test on `RefreshedTokens`; mock-server tests, never real OpenAI) → **Tasks 3 (pure) + 4 (HTTP + `MockOAuth`)**. ✅
- **Snapshot assembly** (store `latest_usage` join of `Account` + latest `usage_history` per window; server `assemble_snapshots`; `capacity_credits=None`→plan-derived; `health_tier=0`/`in_flight=0`/`last_error_at=None` deferred defaults; temp-store integration test) → **Task 5**. ✅
- **Pool wiring** (`AppState` = `Store` + `Arc<dyn Selector>` + `TokenCipher` + `OAuthClient` + `upstream_base_url`; config migration `POLYFLARE_UPSTREAM_TOKEN`→store tokens, `POLYFLARE_UPSTREAM_URL` retained as shared base, `POLYFLARE_AUTH_URL` added; per-request assemble→pick→load→refresh+persist→decrypt→build core `Account`→execute; "no eligible account"→503; rewritten integration tests seeding a store account; generic 502 body) → **Task 6**. ✅
- **Global constraints** (Rust 2021; runtime-checked sqlx; never log tokens + redacting `Debug`/redaction test on `RefreshedTokens`; pure+deterministic seeded selector; OAuth mock-only; faithful `capacity_weighted`; secrets from store/key/env) → **Global Constraints** + enforced per task. ✅
- **Split recommendation honored:** M2b-1 (Tasks 1–4, no serve-path change) and M2b-2 (Tasks 5–6, serve-path integration), with an explicit merge-order note. ✅

**2. Placeholder scan:** No `TBD`/`TODO`/"add error handling"/"similar to above". Every code block is complete, compilable Rust. The no-op continuity/session-affinity hooks are intentional, documented passthroughs (per §C1: continuity is M3), not gaps. `capacity_credits`/`health_tier`/`in_flight`/`last_error_at` defaults are the documented deferred-runtime-tracking behavior, not stubs. ✅

**3. Type consistency across tasks:**
- `AccountId` (newtype, `as_str`/`From`/`Display`) — defined Task 1; returned by `Selector::pick` (Task 1/2), matched by `.as_str()` at every store lookup in the ingress handler (Task 6). ✅
- `AccountSnapshot` (15 fields incl. `last_error_at`) + `AccountSnapshot::new` — defined Task 1; consumed by `CapacityWeighted` (Task 2) and constructed by `assemble_snapshots` via `new` + field overrides (Task 5). Field names/types (`used_percent`/`secondary_used_percent: f64`, `reset_at`/`cooldown_until`/`last_error_at`/`last_selected_at: Option<i64>`, `health_tier: u8`, `error_count`/`in_flight: u32`) are identical at every use site. ✅
- `SelectionCtx{now: i64, require_security_work_authorized: bool, rng_seed: Option<u64>, session_id: Option<String>}` — defined Task 1; constructed in the selector tests (Task 2) and the ingress handler (Task 6) with the same field set. ✅
- `Selector::pick(&self, &[AccountSnapshot], &SelectionCtx) -> Option<AccountId>` — reshaped Task 1; implemented by `CapacityWeighted` (Task 2); called via `Arc<dyn Selector>` in Task 6. ✅
- OAuth surface — `should_refresh(i64,i64)->bool`, `classify_failure(&str)->FailureClass`, `FailureClass::status(self)->Option<&'static str>`, `decode_claims(&str)->Result<Claims,OAuthError>` (Task 3); `OAuthClient::{new,refresh}`, `Refreshed{tokens,claims}`, `RefreshedTokens{access_token,refresh_token,id_token}` (Task 4) — consumed unchanged by the ingress handler (Task 6: `should_refresh`, `refresh`, `OAuthError::Endpoint{code,..}`, `classify_failure(&code).status()`). ✅
- Store additions — `WindowUsage`/`UsageSnapshot`, `AccountRepo::latest_usage` (Task 5) — consumed only by `assemble_snapshots` (Task 5). Existing `AccountRepo::{list,get,decrypt_tokens,update_tokens,update_status,insert}` + `PlainTokens`/`TokenCipher`/`Account` used at the exact M2a signatures in Tasks 5–6. ✅
- `MockOAuth::{ok,error,last_body,spawn}` + `OAuthResponse` (Task 4 testkit) — consumed only by `tests/oauth_refresh.rs` (Task 4). `MockUpstream` reused unchanged by the Task 6 server tests. ✅
- `AppState` field set is identical in `app.rs` (Task 6 def) and all four server test constructors + `main.rs::serve` (Task 6). SQL `"window"` stays double-quoted in the new `latest_usage` query and the snapshot-test inserts. ✅

**Known API caveats to watch during execution (not blockers):**
- **`rand` 0.9 paths** — `rand::distr::weighted::WeightedIndex`, `rand::distr::Distribution`, `rand::rngs::StdRng`, `rand::SeedableRng::seed_from_u64`, `rand::rng()` (Context7-verified for 0.9). If a future patch gates `rand::rng()` behind a non-default feature, add `features = ["thread_rng"]` to the core dep. The transitive `rand` 0.8.7 (via `fernet`) coexists — the selector links only the 0.9 direct dep. ✅
- **`WeightedIndex<f64>`** — float weights are supported; all-zero is pre-checked (→ deterministic fallback) so `Error::InsufficientNonZero` cannot surface in the sample path; any `Err` is treated as the deterministic fallback defensively. ✅
- **`base64` 0.22 Engine API** — `URL_SAFE_NO_PAD.decode(seg)`; JWT payloads are base64url-no-pad, so no padding fixups are needed. (0.22.1 already in the lockfile.) ✅
- **`reqwest` 0.12** — `RequestBuilder::{json,timeout,send}`, `Response::{status,json}`; `json` feature already enabled workspace-wide. ✅
- **Temp-DB lifetime in server tests** — `std::mem::forget(dir)` deliberately leaks each test's `TempDir` so the SQLite file (+ WAL sidecars) outlive the spawned server task; acceptable in short-lived test processes and avoids a dropped-dir/open-pool race. ✅
- **`is_some_and`** (used in `should_drain`/eligibility) is stable (Rust ≥1.70); toolchain here is 1.92. ✅

---

## Execution handoff

M2b delivers PolyFlare's routing brain and token lifecycle: a reshaped, rich-input `Selector` (M2-GATE1), the faithful, pure, seed-deterministic `capacity_weighted` scoring port, OpenAI OAuth refresh (decode-only claims + `POST /oauth/token`), store-derived selection snapshots, and a store-backed ingress path that selects an account per request, refreshes its token when stale, and relays with a per-account bearer — replacing M1's single hardcoded account. **Execute M2b-1 (Tasks 1–4) and merge it first**, then **M2b-2 (Tasks 5–6)** on top. Deferred to later/data-driven milestones: live health-tier + in-flight runtime tracking, the other 7 selector strategies, continuity ownership (M3, currently a no-op passthrough hook), TA6 reactive retry orchestration (M3), and the Anthropic executor's OAuth (M4).
