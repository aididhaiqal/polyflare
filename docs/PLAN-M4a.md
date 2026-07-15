# PolyFlare M4a — Anthropic Backend Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give PolyFlare a second, independently-routable backend pool — Anthropic (Claude Max/Pro OAuth subscription) accounts — reachable **natively** by a Claude client speaking the Anthropic Messages API (`POST /v1/messages`), with no cross-provider translation involved. This is the foundation M4b's cross-translator (Anthropic→Codex, the headline "Claude Code drives Sol" feature) plugs into; it is **not** built here (SPEC-M4 Q6).

**Architecture:** A `provider` column discriminates accounts (`codex` | `anthropic`) in the existing minimal-additive store schema. `AppState` gains a second executor (`anthropic_executor`) and a second per-provider upstream base URL, selected via a `Provider`-keyed helper. Each ingress path (`/responses` for Codex, the new `/v1/messages` for Anthropic) assembles the full account-snapshot pool then **narrows it to its own provider** before calling `Selector::pick` — since M4a ships no cross-format translator, an ingress path may never select an account whose format it can't speak. The Anthropic path reuses the existing `NoopContinuity` (added in M3 for exactly this) and the existing watchdog wrapper, so its `WatchdogArm` is always `Disarmed` and the wedge machinery never arms. `AnthropicExecutor` mirrors `CodexExecutor`'s M1 shape verbatim: HTTP `POST {base}/v1/messages`, `bearer_auth`, the required `anthropic-version` header, `bytes_stream` → `ResponseStream`, non-2xx → `ExecError`.

**Tech Stack:** Rust 2021, tokio, axum 0.8, reqwest 0.12 (streaming), futures 0.3, sqlx 0.8 (runtime-checked, SQLite), serde/serde_json, thiserror, async-trait.

## Global Constraints

*Every task's requirements implicitly include this section. Values are copied verbatim from SPEC-M4 + the standing project gates.*

- **Rust edition 2021**, workspace resolver `2`. New code compiles clean under `cargo clippy --workspace --all-targets -- -D warnings`.
- **sqlx is runtime-checked only**: `sqlx::query` / `sqlx::query_as::<_, T>` / `#[derive(sqlx::FromRow)]`. NO `sqlx::query!`/`query_as!` compile-time macros, NO `DATABASE_URL` needed to build. Quote `"window"`-style SQLite keywords. **Migrations are forward-only** — this plan adds `0003_provider.sql`; `0001_accounts_and_usage.sql` and `0002_continuity.sql` are never edited.
- **Account model = Claude Max/Pro OAuth *subscription* accounts, NOT platform API-key accounts** (SPEC-M4 U5, resolved). This is load-bearing: auth uses `bearer_auth` (`Authorization: Bearer`), never `x-api-key`; and the rate-limit signal set is the ccflare-style subscription surface (`out_of_credits`/`extra_usage`/window-reset/24h-clamp), never the platform `anthropic-ratelimit-*` headers (SPEC-M4 §3.5a).
- **Store is minimal-additive** (SPEC-M4 U3): one new column, `provider TEXT NOT NULL DEFAULT 'codex'`. All existing Codex-only columns are already nullable (verified against `0001_accounts_and_usage.sql` — `chatgpt_account_id`, `chatgpt_user_id`, `workspace_id`, `workspace_label`, `seat_type` carry no `NOT NULL`), so no other schema change is needed.
- **Streaming stays non-buffering.** `ResponseStream` chunks are forwarded unchanged; no client byte is ever synthesized or delayed by buffering a full body.
- **Redacting `Debug` + a redaction test on every secret-bearing type.** Any new type that carries a token, bearer credential, or request body must never print it via `{:?}` (mirrors `Account`/`PreparedRequest`/`RefreshedTokens` today).
- **Real-wire-shape fixtures.** Test bodies/SSE events use the doc-verified Anthropic shapes from SPEC-M4 §3.5/§3.5a (confirmed independently via the Anthropic TypeScript SDK source during this planning pass — see Task 6), never invented shapes.
- **Client-facing errors carry generic bodies** — never a token, URL, or internal `Display` (existing `ingress.rs` convention; applies identically to the new `/v1/messages` handler).
- **No cross-provider routing in M4a.** An ingress path may only ever select an account whose `provider` matches its own wire format — this is the concrete meaning of SPEC-M4 Q6's "M4a reaches the Anthropic pool natively" (the translator that would make cross-provider routing safe is M4b, not built here).
- **Gates before EVERY commit:** `cargo fmt --all -- --check` → `cargo clippy --workspace --all-targets -- -D warnings` → `cargo test --workspace`, all green.

---

## Task ordering note (deviation from SPEC-M4 §6's suggested order)

SPEC-M4 §6 sketches the order Provider → Store → Dispatch → Ingress → Executor → Rate-limit. This plan builds **the `AnthropicExecutor` (Task 3) before wiring dispatch (Task 4)**: `AppState` must hold a real, concrete `anthropic_executor: Arc<dyn Executor>` the moment its struct shape changes (the whole workspace — `main.rs` plus nine existing test files — must keep compiling after every task), and there is no honest placeholder executor to plug in otherwise. Building the leaf executor first, then wiring dispatch on top of two already-real executors, avoids an artificial stub.

## Spec gaps hit while planning (flagging, not silently resolving)

1. **Per-provider upstream base URL isn't in SPEC-M4's "minimal additive" framing.** `AppState.upstream_base_url` is a single shared field (Codex's ChatGPT-backend URL). Anthropic needs its own (`https://api.anthropic.com`, distinct host). Task 4 adds `anthropic_upstream_base_url` to `ServeConfig`/`AppState` as a sibling field — additive, but not something SPEC-M4 called out explicitly.
2. **SPEC-M4 §3.7 says TA6 routes into "the neutral `NeedsCapability` error the retry loop already understands"** — no `NeedsCapability` type or generic capability-retry loop exists anywhere in the current codebase (confirmed by search); only the existing hard **pre-filter** (`AccountSnapshot.security_work_authorized` + `SelectionCtx.require_security_work_authorized` in `select.rs`) exists. Task 8 is scoped down accordingly: it builds the classification function only (real, tested now) and flags the `NeedsCapability`-wiring gap explicitly rather than inventing a retry-loop mechanism that isn't there.
3. **Anthropic OAuth refresh cannot reuse `AppState.oauth`** (that `OAuthClient` is hardwired to the Codex `auth.openai.com` token endpoint + Codex `CLIENT_ID`). Task 4's `resolve_core_account` explicitly skips the refresh-on-stale check for `Provider::Anthropic` accounts (documented as a known M4a limitation, closed by Task 7).

---

## File Structure

**New files**

- `crates/polyflare-core/src/provider.rs` — `Provider` enum + `Display`/`FromStr`. (Task 1)
- `crates/polyflare-store/migrations/0003_provider.sql` — the `provider` column. (Task 2)
- `crates/polyflare-anthropic/src/executor.rs` — `AnthropicExecutor`. (Task 3)
- `crates/polyflare-anthropic/tests/executor_stream.rs` — mock-upstream executor test. (Task 3)
- `crates/polyflare-server/tests/provider_dispatch.rs` — dispatch/filter integration tests. (Task 4)
- `crates/polyflare-server/tests/messages_ingress.rs` — `/v1/messages` integration tests. (Task 5)
- `crates/polyflare-anthropic/src/errors.rs` — rate-limit/error classification module. (Task 6)
- `crates/polyflare-anthropic/src/oauth.rs` — VERIFY-gated OAuth scaffold. (Task 7)

**Modified files**

- `crates/polyflare-core/src/lib.rs` — export `Provider`. (Task 1)
- `crates/polyflare-core/src/types.rs` — `AccountSnapshot.provider` field. (Task 4)
- `crates/polyflare-store/src/account.rs` — `Account.provider` field + `AccountRepo` SELECT/INSERT. (Task 2)
- `crates/polyflare-store/src/import.rs` — imported (codex-lb) accounts hardcode `provider: "codex"`. (Task 2)
- `crates/polyflare-store/tests/account_repo.rs` — fixture + new provider round-trip tests. (Task 2)
- 9 server-crate test fixtures (`tests/{ingress_relays,large_body,snapshot_assembly,refresh_path,wedge_regression,no_anchor_failover,ownership,e2e_passthrough,signal_client}.rs`) — add `provider` field to their local `Account` literal. (Task 2); rename `executor` → `codex_executor` + add `anthropic_executor`/`anthropic_upstream_base_url` to their `AppState` literal. (Task 4)
- `crates/polyflare-server/tests/pool_selection.rs` — same `AppState` literal update. (Task 4)
- `crates/polyflare-testkit/src/lib.rs` — `MockUpstream` also serves `POST /v1/messages`. (Task 3)
- `crates/polyflare-anthropic/Cargo.toml` — dependencies (built up across Tasks 3/6/7).
- `crates/polyflare-anthropic/src/lib.rs` — module exports (built up across Tasks 3/6/7).
- `crates/polyflare-server/Cargo.toml` — add `polyflare-anthropic` dependency. (Task 4)
- `crates/polyflare-server/src/app.rs` — `AppState` fields + `executor_for`/`upstream_base_url_for`. (Task 4); register `/v1/messages`. (Task 5)
- `crates/polyflare-server/src/config.rs` — `anthropic_upstream_base_url`. (Task 4)
- `crates/polyflare-server/src/main.rs` — build + wire both executors. (Task 4)
- `crates/polyflare-server/src/ingress.rs` — `resolve_core_account` returns `(Account, Provider)` + filters + dispatch. (Task 4); `messages_handler`. (Task 5)
- `crates/polyflare-server/src/snapshot.rs` — populate `provider` + `filter_by_provider`. (Task 4)

---

## Task 1: `Provider` enum in `polyflare-core`

**Files:**
- Create: `crates/polyflare-core/src/provider.rs`
- Modify: `crates/polyflare-core/src/lib.rs`

**Interfaces:**
- Produces: `pub enum Provider { Codex, Anthropic }` with `impl Display for Provider` (→ `"codex"`/`"anthropic"`) and `impl FromStr for Provider` (`Err = UnknownProvider`); `Provider` is `Debug + Clone + Copy + PartialEq + Eq + Hash`. No secret data — **no redaction needed** (unlike `Account`/`PreparedRequest`, `Provider` carries no token or user content).

- [ ] **Step 1: Write the failing test**

Create `crates/polyflare-core/src/provider.rs`:

```rust
//! The backend provider an account belongs to — decides which executor + backend wire `Format`
//! services a request. Carries no secret data, so (unlike `Account`/`PreparedRequest`) it needs no
//! redacting `Debug`.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_display_and_from_str() {
        assert_eq!(Provider::Codex.to_string(), "codex");
        assert_eq!(Provider::Anthropic.to_string(), "anthropic");
        assert_eq!("codex".parse::<Provider>().unwrap(), Provider::Codex);
        assert_eq!("anthropic".parse::<Provider>().unwrap(), Provider::Anthropic);
    }

    #[test]
    fn unknown_provider_string_is_rejected() {
        let err = "bogus".parse::<Provider>().unwrap_err();
        assert_eq!(err.0, "bogus");
    }
}
```

Register the module and re-export in `crates/polyflare-core/src/lib.rs`:

```rust
pub mod continuity;
pub mod format;
pub mod provider;
pub mod select;
pub mod traits;
pub mod translate;
pub mod types;

pub use continuity::NoopContinuity;
pub use format::Format;
pub use provider::Provider;
pub use select::CapacityWeighted;
```

(leave the rest of `lib.rs` unchanged)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p polyflare-core provider`
Expected: FAIL to compile — `cannot find type \`Provider\` in this scope` (and `UnknownProvider`/`.0` unresolved).

- [ ] **Step 3: Write minimal implementation**

Above the `#[cfg(test)]` block in `crates/polyflare-core/src/provider.rs`, add:

```rust
use std::fmt;
use std::str::FromStr;

/// Which backend pool an account belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Provider {
    /// The Codex (OpenAI-Responses) backend pool.
    Codex,
    /// The Anthropic (Messages) backend pool.
    Anthropic,
}

impl fmt::Display for Provider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Provider::Codex => "codex",
            Provider::Anthropic => "anthropic",
        })
    }
}

/// Returned when a stored `provider` column value doesn't match a known provider.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown provider: {0}")]
pub struct UnknownProvider(pub String);

impl FromStr for Provider {
    type Err = UnknownProvider;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "codex" => Ok(Provider::Codex),
            "anthropic" => Ok(Provider::Anthropic),
            other => Err(UnknownProvider(other.to_string())),
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p polyflare-core provider`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/polyflare-core/src/provider.rs crates/polyflare-core/src/lib.rs
git commit -m "feat(m4a): Provider enum with codex/anthropic TEXT round-trip"
```

---

## Task 2: Store — `provider` column + `Account.provider` + `AccountRepo`

**Files:**
- Create: `crates/polyflare-store/migrations/0003_provider.sql`
- Modify: `crates/polyflare-store/src/account.rs`
- Modify: `crates/polyflare-store/src/import.rs`
- Modify: `crates/polyflare-store/tests/account_repo.rs`
- Modify (mechanical, one field added to each local `Account` literal): `crates/polyflare-server/tests/ingress_relays.rs:41`, `large_body.rs:40`, `snapshot_assembly.rs:25`, `refresh_path.rs:52`, `wedge_regression.rs:85`, `no_anchor_failover.rs:56`, `ownership.rs:53`, `e2e_passthrough.rs:41`, `signal_client.rs:41`

**Interfaces:**
- Consumes: nothing new from Task 1 (the store crate deliberately does NOT depend on `polyflare-core` — `Account.provider` stays a plain `String`, matching the existing convention for `status`/`plan_type`/`routing_policy`; the typed `Provider` enum is applied one layer up, in Task 4's `AccountSnapshot`).
- Produces: `polyflare_store::Account.provider: String` (round-trips through `AccountRepo::insert`/`insert_encrypted`/`get`/`list`).

- [ ] **Step 1: Write the failing test**

In `crates/polyflare-store/tests/account_repo.rs`, add `provider: "codex".to_string(),` to the end of `sample_account()`'s literal (making the file compile against the field this task adds), then append a new test:

```rust
#[tokio::test]
async fn provider_round_trips_and_legacy_rows_default_to_codex() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[11u8; 32]).unwrap();
    let repo = store.accounts();

    // A fresh Anthropic account round-trips its provider through insert/get.
    let mut anthropic = sample_account("anthropic-1");
    anthropic.provider = "anthropic".to_string();
    repo.insert(&anthropic, &sample_tokens(), &cipher)
        .await
        .unwrap();
    assert_eq!(
        repo.get("anthropic-1").await.unwrap().unwrap().provider,
        "anthropic"
    );

    // A legacy row written the way pre-M4a code would (no `provider` column mentioned at all)
    // must default to 'codex' via the migration's column default — the real regression this
    // migration protects against.
    sqlx::query(
        "INSERT INTO accounts (id, email, plan_type, routing_policy, access_token_enc, \
         refresh_token_enc, id_token_enc, last_refresh, created_at, status, \
         security_work_authorized) VALUES ('legacy-1', 'legacy@example.test', 'pro', 'normal', \
         x'00', x'00', x'00', 0, 0, 'active', 0)",
    )
    .execute(store.pool())
    .await
    .unwrap();
    assert_eq!(repo.get("legacy-1").await.unwrap().unwrap().provider, "codex");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p polyflare-store`
Expected: FAIL to compile — `Account` has no field `provider` (in both `account_repo.rs` and the test above), and `sqlx::query(...)` against `store.pool()` would additionally 500 at the SQL level once it compiles (no `provider` column exists yet).

- [ ] **Step 3: Write the migration**

Create `crates/polyflare-store/migrations/0003_provider.sql`:

```sql
-- PolyFlare M4a: provider discriminator on accounts. Forward-only; existing (pre-M4a) rows
-- default to 'codex' since every account created before this migration is Codex-shaped. New
-- 'anthropic' rows populate the neutral columns (id, email, plan_type, routing_policy, the three
-- token columns, security_work_authorized) and leave the already-nullable Codex-only columns
-- (chatgpt_account_id, chatgpt_user_id, workspace_id, workspace_label, seat_type) NULL.

ALTER TABLE accounts ADD COLUMN provider TEXT NOT NULL DEFAULT 'codex';
```

- [ ] **Step 4: Update `Account` + `AccountRepo` in `crates/polyflare-store/src/account.rs`**

Add the field to the struct (after `security_work_authorized`):

```rust
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Account {
    pub id: String,
    pub chatgpt_account_id: Option<String>,
    pub chatgpt_user_id: Option<String>,
    pub email: String,
    pub alias: Option<String>,
    pub workspace_id: Option<String>,
    pub workspace_label: Option<String>,
    pub seat_type: Option<String>,
    pub plan_type: String,
    pub routing_policy: String,
    pub last_refresh: i64,
    pub created_at: i64,
    pub status: String,
    pub deactivation_reason: Option<String>,
    pub reset_at: Option<i64>,
    pub blocked_at: Option<i64>,
    pub security_work_authorized: bool,
    /// 'codex' | 'anthropic' — which backend pool this account belongs to.
    pub provider: String,
}
```

Update the two SELECT constants (append `, provider`):

```rust
const SELECT_ACCOUNT_BY_ID: &str = "SELECT id, chatgpt_account_id, chatgpt_user_id, email, \
    alias, workspace_id, workspace_label, seat_type, plan_type, routing_policy, last_refresh, \
    created_at, status, deactivation_reason, reset_at, blocked_at, security_work_authorized, \
    provider FROM accounts WHERE id = ?";

const SELECT_ALL_ACCOUNTS: &str = "SELECT id, chatgpt_account_id, chatgpt_user_id, email, \
    alias, workspace_id, workspace_label, seat_type, plan_type, routing_policy, last_refresh, \
    created_at, status, deactivation_reason, reset_at, blocked_at, security_work_authorized, \
    provider FROM accounts ORDER BY id";
```

Update `insert_encrypted` (add the column + bind):

```rust
    pub async fn insert_encrypted(
        &self,
        account: &Account,
        enc: &EncryptedTokens,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO accounts (\
                id, chatgpt_account_id, chatgpt_user_id, email, alias, \
                workspace_id, workspace_label, seat_type, plan_type, routing_policy, \
                access_token_enc, refresh_token_enc, id_token_enc, \
                last_refresh, created_at, status, deactivation_reason, \
                reset_at, blocked_at, security_work_authorized, provider\
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(account.id.as_str())
        .bind(account.chatgpt_account_id.as_deref())
        .bind(account.chatgpt_user_id.as_deref())
        .bind(account.email.as_str())
        .bind(account.alias.as_deref())
        .bind(account.workspace_id.as_deref())
        .bind(account.workspace_label.as_deref())
        .bind(account.seat_type.as_deref())
        .bind(account.plan_type.as_str())
        .bind(account.routing_policy.as_str())
        .bind(enc.access_token_enc.as_slice())
        .bind(enc.refresh_token_enc.as_slice())
        .bind(enc.id_token_enc.as_slice())
        .bind(account.last_refresh)
        .bind(account.created_at)
        .bind(account.status.as_str())
        .bind(account.deactivation_reason.as_deref())
        .bind(account.reset_at)
        .bind(account.blocked_at)
        .bind(account.security_work_authorized)
        .bind(account.provider.as_str())
        .execute(&self.pool)
        .await?;
        Ok(())
    }
```

(the `VALUES` placeholder count goes from 19 `?` to 20 — count carefully: `id..security_work_authorized` is 19 columns, `+provider` = 20)

- [ ] **Step 5: Fix the importer — `crates/polyflare-store/src/import.rs`**

Codex-lb accounts are always Codex-origin — hardcode it. In the `Account { ... }` construction:

```rust
        let account = Account {
            id: src.id,
            chatgpt_account_id: src.chatgpt_account_id,
            chatgpt_user_id: src.chatgpt_user_id,
            email: src.email,
            alias: src.alias,
            workspace_id: src.workspace_id,
            workspace_label: src.workspace_label,
            seat_type: src.seat_type,
            plan_type: src.plan_type,
            routing_policy: src.routing_policy,
            last_refresh,
            created_at,
            status: src.status,
            deactivation_reason: src.deactivation_reason,
            reset_at: src.reset_at,
            blocked_at: src.blocked_at,
            security_work_authorized: src.security_work_authorized,
            provider: "codex".to_string(),
        };
```

And the destination `INSERT OR IGNORE` (add the column + bind, mirroring Step 4's placeholder-count change):

```rust
        let result = sqlx::query(
            "INSERT OR IGNORE INTO accounts (\
                id, chatgpt_account_id, chatgpt_user_id, email, alias, \
                workspace_id, workspace_label, seat_type, plan_type, routing_policy, \
                access_token_enc, refresh_token_enc, id_token_enc, \
                last_refresh, created_at, status, deactivation_reason, \
                reset_at, blocked_at, security_work_authorized, provider\
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(account.id.as_str())
        .bind(account.chatgpt_account_id.as_deref())
        .bind(account.chatgpt_user_id.as_deref())
        .bind(account.email.as_str())
        .bind(account.alias.as_deref())
        .bind(account.workspace_id.as_deref())
        .bind(account.workspace_label.as_deref())
        .bind(account.seat_type.as_deref())
        .bind(account.plan_type.as_str())
        .bind(account.routing_policy.as_str())
        .bind(enc.access_token_enc.as_slice())
        .bind(enc.refresh_token_enc.as_slice())
        .bind(enc.id_token_enc.as_slice())
        .bind(account.last_refresh)
        .bind(account.created_at)
        .bind(account.status.as_str())
        .bind(account.deactivation_reason.as_deref())
        .bind(account.reset_at)
        .bind(account.blocked_at)
        .bind(account.security_work_authorized)
        .bind(account.provider.as_str())
        .execute(&mut *tx)
        .await?;
```

- [ ] **Step 6: Fix the nine server-crate test fixtures (mechanical, one line each)**

Each of these files defines a local `Account { ... security_work_authorized: <bool>, }` literal ending at the line noted. Add `provider: "codex".to_string(),` as the line immediately after `security_work_authorized: <bool>,` in each:

- `crates/polyflare-server/tests/ingress_relays.rs:41`
- `crates/polyflare-server/tests/large_body.rs:40`
- `crates/polyflare-server/tests/snapshot_assembly.rs:25`
- `crates/polyflare-server/tests/refresh_path.rs:52`
- `crates/polyflare-server/tests/wedge_regression.rs:85`
- `crates/polyflare-server/tests/no_anchor_failover.rs:56`
- `crates/polyflare-server/tests/ownership.rs:53`
- `crates/polyflare-server/tests/e2e_passthrough.rs:41`
- `crates/polyflare-server/tests/signal_client.rs:41`

Example (identical pattern in all nine — `ingress_relays.rs:41` shown):

```rust
        blocked_at: None,
        security_work_authorized: false,
        provider: "codex".to_string(),
    }
}
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `cargo test --workspace`
Expected: PASS — every crate compiles and all existing + the two new provider tests are green.

- [ ] **Step 8: Commit**

```bash
git add crates/polyflare-store/migrations/0003_provider.sql \
        crates/polyflare-store/src/account.rs \
        crates/polyflare-store/src/import.rs \
        crates/polyflare-store/tests/account_repo.rs \
        crates/polyflare-server/tests/ingress_relays.rs \
        crates/polyflare-server/tests/large_body.rs \
        crates/polyflare-server/tests/snapshot_assembly.rs \
        crates/polyflare-server/tests/refresh_path.rs \
        crates/polyflare-server/tests/wedge_regression.rs \
        crates/polyflare-server/tests/no_anchor_failover.rs \
        crates/polyflare-server/tests/ownership.rs \
        crates/polyflare-server/tests/e2e_passthrough.rs \
        crates/polyflare-server/tests/signal_client.rs
git commit -m "feat(m4a): 0003_provider migration + Account.provider round-trip"
```

---

## Task 3: `AnthropicExecutor` in `polyflare-anthropic`

**Files:**
- Modify: `crates/polyflare-anthropic/Cargo.toml`
- Modify: `crates/polyflare-anthropic/src/lib.rs`
- Create: `crates/polyflare-anthropic/src/executor.rs`
- Create: `crates/polyflare-anthropic/tests/executor_stream.rs`
- Modify: `crates/polyflare-testkit/src/lib.rs`

**Interfaces:**
- Consumes: `polyflare_core::{Account, ExecError, Executor, PreparedRequest, ResponseStream}` (unchanged trait, from `crates/polyflare-core/src/traits.rs`/`types.rs`).
- Produces: `pub struct AnthropicExecutor` implementing `Executor`; `AnthropicExecutor::new() -> Result<Self, ExecError>`.

- [ ] **Step 1: Add the `/v1/messages` route to the shared mock upstream**

`MockUpstream`'s handler already ignores the path (it just records the body/headers and streams by `mode`); only the router needs the extra route. In `crates/polyflare-testkit/src/lib.rs`, update `spawn`:

```rust
    /// Bind an ephemeral port, serve in a background task, and return the base URL.
    pub async fn spawn(self) -> String {
        let app = Router::new()
            .route("/responses", post(handler))
            .route("/v1/messages", post(handler))
            // Match the raised polyflare-server body limit so large-body e2e tests
            // don't 413 against the mock upstream itself. Test infra only.
            .layer(DefaultBodyLimit::max(100 * 1024 * 1024))
            .with_state(self);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }
```

- [ ] **Step 2: Write the failing test**

Update `crates/polyflare-anthropic/Cargo.toml`:

```toml
[package]
name = "polyflare-anthropic"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
polyflare-core = { path = "../polyflare-core" }
reqwest = { workspace = true }
futures-util = { workspace = true }
bytes = { workspace = true }
async-trait = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
tokio = { workspace = true }
polyflare-testkit = { path = "../polyflare-testkit" }
```

Create `crates/polyflare-anthropic/tests/executor_stream.rs`:

```rust
use futures_util::StreamExt;
use polyflare_anthropic::AnthropicExecutor;
use polyflare_core::{Account, Executor, PreparedRequest};
use polyflare_testkit::MockUpstream;

#[tokio::test]
async fn executor_streams_upstream_events_and_forwards_body() {
    let mock = MockUpstream::new(vec![
        r#"{"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}"#.to_string(),
        r#"{"type":"message_stop"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let base = mock.spawn().await;

    let executor = AnthropicExecutor::new().unwrap();
    let account = Account {
        id: "test".into(),
        base_url: base,
        bearer_token: "test-token".into(),
    };
    let req = PreparedRequest {
        body: serde_json::json!({
            "model": "claude-opus-4",
            "messages": [{"role": "user", "content": "hi"}]
        }),
        model: "claude-opus-4".into(),
    };

    let mut stream = executor.execute(req, &account).await.unwrap();
    let mut collected = String::new();
    while let Some(chunk) = stream.next().await {
        collected.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }

    assert!(collected.contains("content_block_delta"));
    assert!(collected.contains("message_stop"));
    assert_eq!(handle.last_body().unwrap()["model"], "claude-opus-4");
    assert_eq!(handle.last_authorization().unwrap(), "Bearer test-token");
}

#[tokio::test]
async fn executor_surfaces_upstream_error_status() {
    // No route for this path on the mock → 404 → ExecError::Upstream.
    let base = MockUpstream::new(vec![]).spawn().await;
    let executor = AnthropicExecutor::new().unwrap();
    let account = Account {
        id: "test".into(),
        base_url: format!("{base}/nonexistent-base"),
        bearer_token: "t".into(),
    };
    let req = PreparedRequest {
        body: serde_json::json!({"model": "m"}),
        model: "m".into(),
    };
    let err = executor.execute(req, &account).await.err().unwrap();
    assert!(matches!(err, polyflare_core::ExecError::Upstream(_)));
}
```

Update `crates/polyflare-anthropic/src/lib.rs`:

```rust
//! Anthropic backend: HTTP executor (M4a), rate-limit/error classification (M4a), OAuth (M4a,
//! VERIFY-gated). Byte-parity fingerprinting + the cross-format translator are M4b/M5.

pub mod executor;

pub use executor::AnthropicExecutor;
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p polyflare-anthropic`
Expected: FAIL to compile — `unresolved import polyflare_anthropic::executor` / `AnthropicExecutor` not found.

- [ ] **Step 4: Write minimal implementation**

Create `crates/polyflare-anthropic/src/executor.rs`:

```rust
//! Anthropic backend executor: HTTP `POST /v1/messages`, subscription-OAuth bearer auth, the
//! required `anthropic-version` header, SSE byte-stream pass-through. Mirrors `CodexExecutor`'s
//! M1 shape; byte-parity fingerprinting is M5, not here.

use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;

use polyflare_core::{Account, ExecError, Executor, PreparedRequest, ResponseStream};

/// The Anthropic Messages API version this executor speaks. Every request must carry this header
/// (doc-verified against the Anthropic TypeScript SDK: `'anthropic-version': '2023-06-01'` is sent
/// on every request in `src/client.ts`).
const ANTHROPIC_VERSION: &str = "2023-06-01";

pub struct AnthropicExecutor {
    client: reqwest::Client,
}

impl AnthropicExecutor {
    pub fn new() -> Result<Self, ExecError> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| ExecError::Upstream(e.to_string()))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl Executor for AnthropicExecutor {
    async fn execute(
        &self,
        req: PreparedRequest,
        account: &Account,
    ) -> Result<ResponseStream, ExecError> {
        let url = format!("{}/v1/messages", account.base_url.trim_end_matches('/'));
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&account.bearer_token)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .json(&req.body)
            .send()
            .await
            .map_err(|e| ExecError::Upstream(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(ExecError::Upstream(format!("status {}", resp.status())));
        }

        let stream = resp
            .bytes_stream()
            .map(|chunk| chunk.map_err(|e| ExecError::Stream(e.to_string())));

        Ok(Box::pin(stream))
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test -p polyflare-anthropic`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add crates/polyflare-anthropic/Cargo.toml crates/polyflare-anthropic/src/lib.rs \
        crates/polyflare-anthropic/src/executor.rs crates/polyflare-anthropic/tests/executor_stream.rs \
        crates/polyflare-testkit/src/lib.rs
git commit -m "feat(m4a): AnthropicExecutor (mirrors CodexExecutor's HTTP+bearer+SSE shape)"
```

---

## Task 4: Executor dispatch — `AppState` + `executor_for` + wire into `/responses`

**Files:**
- Modify: `crates/polyflare-core/src/types.rs`
- Modify: `crates/polyflare-server/Cargo.toml`
- Modify: `crates/polyflare-server/src/app.rs`
- Modify: `crates/polyflare-server/src/config.rs`
- Modify: `crates/polyflare-server/src/main.rs`
- Modify: `crates/polyflare-server/src/snapshot.rs`
- Modify: `crates/polyflare-server/src/ingress.rs`
- Modify: `crates/polyflare-server/tests/{ingress_relays,large_body,snapshot_assembly is test-only,refresh_path,wedge_regression,no_anchor_failover,ownership,e2e_passthrough,signal_client,pool_selection}.rs`
- Create: `crates/polyflare-server/tests/provider_dispatch.rs`

**Interfaces:**
- Consumes: `Provider` (Task 1), `Account.provider: String` (Task 2), `AnthropicExecutor::new() -> Result<Self, ExecError>` (Task 3).
- Produces: `AccountSnapshot.provider: Provider`; `polyflare_server::snapshot::filter_by_provider(snapshots: &[AccountSnapshot], provider: Provider) -> Vec<AccountSnapshot>`; `AppState::executor_for(&self, provider: Provider) -> &Arc<dyn Executor>`; `AppState::upstream_base_url_for(&self, provider: Provider) -> &str`; `resolve_core_account(..) -> Result<(Account, Provider), Response>` (was `Result<Account, Response>`).

- [ ] **Step 1: Write the failing test — `AccountSnapshot.provider`**

In `crates/polyflare-core/src/types.rs`, add to the existing `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn new_snapshot_defaults_to_codex_provider() {
        let snap = AccountSnapshot::new("a");
        assert_eq!(snap.provider, Provider::Codex);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p polyflare-core`
Expected: FAIL to compile — `AccountSnapshot` has no field `provider`, and `Provider` is unresolved in `types.rs`.

- [ ] **Step 3: Implement — add `provider` to `AccountSnapshot`**

At the top of `crates/polyflare-core/src/types.rs`, add the import:

```rust
use crate::provider::Provider;
```

Add the field to the struct (after `in_flight`):

```rust
    /// In-flight request count (live-tracked later; 0 in M2b).
    pub in_flight: u32,
    /// Which backend pool this account belongs to — selects the executor + backend wire `Format`.
    pub provider: Provider,
}
```

Update `AccountSnapshot::new`:

```rust
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
            provider: Provider::Codex,
        }
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p polyflare-core`
Expected: PASS.

- [ ] **Step 5: Write the failing test — `filter_by_provider` + snapshot population**

In `crates/polyflare-server/tests/snapshot_assembly.rs`, add:

```rust
#[tokio::test]
async fn assemble_snapshots_populates_provider_and_filter_narrows_by_it() {
    use polyflare_core::Provider;
    use polyflare_server::snapshot::filter_by_provider;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[9u8; 32]).unwrap();

    let mut anthro = account("anthropic-1");
    anthro.provider = "anthropic".to_string();
    store
        .accounts()
        .insert(&anthro, &tokens(), &cipher)
        .await
        .unwrap();
    store
        .accounts()
        .insert(&account("codex-1"), &tokens(), &cipher)
        .await
        .unwrap();

    let snaps = assemble_snapshots(&store).await.unwrap();
    assert_eq!(snaps.len(), 2);

    let codex_only = filter_by_provider(&snaps, Provider::Codex);
    assert_eq!(codex_only.len(), 1);
    assert_eq!(codex_only[0].id.as_str(), "codex-1");

    let anthropic_only = filter_by_provider(&snaps, Provider::Anthropic);
    assert_eq!(anthropic_only.len(), 1);
    assert_eq!(anthropic_only[0].id.as_str(), "anthropic-1");
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `cargo test -p polyflare-server --test snapshot_assembly`
Expected: FAIL to compile — `filter_by_provider` doesn't exist in `polyflare_server::snapshot`.

- [ ] **Step 7: Implement `filter_by_provider` + populate `provider` in `crates/polyflare-server/src/snapshot.rs`**

```rust
//! Assemble the selector's per-account snapshots from the durable store: each `Account` joined
//! with its latest `usage_history` row per window. Runtime fields (health tier, in-flight,
//! error/cooldown timestamps) are live-tracked later and default to neutral values here.

use std::str::FromStr;

use polyflare_core::{AccountSnapshot, Provider};
use polyflare_store::{Store, StoreError};

/// Build one `AccountSnapshot` per stored account. Capacity is derived from `plan_type` inside
/// the selector (no per-account override in M2b, so `capacity_credits` stays `None`).
///
/// Candidate order is the account `list()` order (`ORDER BY id` — deterministic, stable across
/// calls). The selector samples over this input order for seed-reproducible picks (same input
/// order + same seed ⇒ same pick), so callers must not reorder the returned `Vec` before passing
/// it to the selector.
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
        // Defensive default: the `provider` column is NOT NULL with a DB-level default, and only
        // this crate's `AccountRepo` ever writes it, so an unparseable value means data written
        // outside the app's control — fail safe to `Codex` rather than dropping the account.
        snap.provider = Provider::from_str(&account.provider).unwrap_or(Provider::Codex);
        snapshots.push(snap);
    }
    Ok(snapshots)
}

/// Narrow candidates to one provider's pool. M4a has no cross-format translator (that's M4b), so
/// each ingress path must call this before `Selector::pick` — a request can only ever be routed to
/// an account whose provider matches the ingress path's own wire format.
pub fn filter_by_provider(
    snapshots: &[AccountSnapshot],
    provider: Provider,
) -> Vec<AccountSnapshot> {
    snapshots
        .iter()
        .filter(|s| s.provider == provider)
        .cloned()
        .collect()
}
```

- [ ] **Step 8: Run test to verify it passes**

Run: `cargo test -p polyflare-server --test snapshot_assembly`
Expected: PASS (4 tests in that file).

- [ ] **Step 9: Write the failing integration test — dispatch wired end-to-end**

Add `polyflare-anthropic` to `crates/polyflare-server/Cargo.toml`'s `[dependencies]`:

```toml
[dependencies]
polyflare-core = { path = "../polyflare-core" }
polyflare-codex = { path = "../polyflare-codex" }
polyflare-anthropic = { path = "../polyflare-anthropic" }
polyflare-store = { path = "../polyflare-store" }
axum = { workspace = true }
tokio = { workspace = true }
serde_json = { workspace = true }
clap = { workspace = true }
futures-core = { workspace = true }
futures-util = { workspace = true }
bytes = { workspace = true }
async-trait = { workspace = true }
sha2 = { workspace = true }
hex = { workspace = true }
thiserror = { workspace = true }
```

Create `crates/polyflare-server/tests/provider_dispatch.rs`:

```rust
//! Provider dispatch: `/responses` must never select — nor execute against — an Anthropic-
//! provider account. M4a has no cross-format translator yet (that's M4b), so a mixed pool must
//! stay strictly partitioned by provider at the ingress boundary.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity, Executor};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockUpstream;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn account(id: &str, provider: &str) -> Account {
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
        provider: provider.to_string(),
    }
}

fn tokens() -> PlainTokens {
    PlainTokens {
        access_token: "tok".to_string(),
        refresh_token: "r".to_string(),
        id_token: "i".to_string(),
    }
}

async fn spawn_polyflare(store: Store, upstream: String) -> String {
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    let codex_executor: Arc<dyn Executor> = Arc::new(CodexExecutor::new().unwrap());
    let anthropic_executor: Arc<dyn Executor> =
        Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap());

    let state = Arc::new(AppState {
        codex_executor,
        anthropic_executor,
        selector: Arc::new(CapacityWeighted),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: upstream,
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
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
async fn responses_returns_503_when_pool_has_only_an_anthropic_account() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[14u8; 32]).unwrap();
    store
        .accounts()
        .insert(&account("anthropic-1", "anthropic"), &tokens(), &cipher)
        .await
        .unwrap();
    std::mem::forget(dir);

    let mock = MockUpstream::new(vec![]);
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "an anthropic-only pool must not serve /responses"
    );
}

#[tokio::test]
async fn responses_routes_only_to_the_codex_account_in_a_mixed_pool() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[15u8; 32]).unwrap();
    store
        .accounts()
        .insert(&account("anthropic-1", "anthropic"), &tokens(), &cipher)
        .await
        .unwrap();
    store
        .accounts()
        .insert(&account("codex-1", "codex"), &tokens(), &cipher)
        .await
        .unwrap();
    std::mem::forget(dir);

    let mock = MockUpstream::new(vec![
        r#"{"type":"response.output_text.delta","delta":"hi"}"#.to_string(),
        r#"{"type":"response.completed"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(handle.request_count(), 1, "exactly one upstream call");
}
```

- [ ] **Step 10: Run test to verify it fails**

Run: `cargo test -p polyflare-server --test provider_dispatch`
Expected: FAIL to compile — `AppState` has no field `codex_executor`/`anthropic_executor`/`anthropic_upstream_base_url`, and `polyflare_anthropic` is not yet a dependency of `polyflare-server`.

- [ ] **Step 11: Implement — `AppState` dispatch fields + helpers in `crates/polyflare-server/src/app.rs`**

```rust
//! Application state and router construction.

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::post;
use axum::Router;

use polyflare_codex::oauth::OAuthClient;
use polyflare_core::{Continuity, Executor, Provider, Selector};
use polyflare_store::{Store, TokenCipher};

use crate::ingress::responses_handler;

/// Raised request-body limit: axum's `Json` extractor default (2 MB) 413s real
/// OpenAI-Responses requests. 100 MB is generous for real Codex turns while bounded.
const MAX_REQUEST_BODY_BYTES: usize = 100 * 1024 * 1024;

/// Shared server state: the per-provider executors, the account selector, the continuity engine,
/// the store + at-rest cipher, the OAuth refresher, and the per-provider upstream base URLs.
/// Wrapped in `Arc` by the caller.
pub struct AppState {
    pub codex_executor: Arc<dyn Executor>,
    pub anthropic_executor: Arc<dyn Executor>,
    pub selector: Arc<dyn Selector>,
    pub continuity: Arc<dyn Continuity>,
    pub store: Store,
    pub cipher: TokenCipher,
    pub oauth: OAuthClient,
    pub upstream_base_url: String,
    pub anthropic_upstream_base_url: String,
}

impl AppState {
    /// The executor that serves `provider`'s pool.
    pub fn executor_for(&self, provider: Provider) -> &Arc<dyn Executor> {
        match provider {
            Provider::Codex => &self.codex_executor,
            Provider::Anthropic => &self.anthropic_executor,
        }
    }

    /// The upstream base URL for `provider`'s pool.
    pub fn upstream_base_url_for(&self, provider: Provider) -> &str {
        match provider {
            Provider::Codex => &self.upstream_base_url,
            Provider::Anthropic => &self.anthropic_upstream_base_url,
        }
    }
}

pub fn build_app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/responses", post(responses_handler))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}
```

(the `/v1/messages` route is registered in Task 5, once `messages_handler` exists — `build_app` here is otherwise unchanged from before this task)

- [ ] **Step 12: Add `anthropic_upstream_base_url` to `crates/polyflare-server/src/config.rs`**

```rust
/// `serve` configuration. The upstream base URL is shared across accounts; per-account bearer
/// tokens are decrypted from the store per request.
pub struct ServeConfig {
    pub bind_addr: String,
    pub upstream_base_url: String,
    pub anthropic_upstream_base_url: String,
    pub auth_base_url: String,
    pub db_path: PathBuf,
    pub key_path: PathBuf,
    pub continuity_watchdog: Duration,
}

impl ServeConfig {
    pub fn from_env() -> Result<Self, String> {
        let bind_addr =
            std::env::var("POLYFLARE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
        let upstream_base_url = std::env::var("POLYFLARE_UPSTREAM_URL")
            .map_err(|_| "POLYFLARE_UPSTREAM_URL not set".to_string())?;
        let anthropic_upstream_base_url = std::env::var("POLYFLARE_ANTHROPIC_UPSTREAM_URL")
            .unwrap_or_else(|_| "https://api.anthropic.com".to_string());
        let auth_base_url = std::env::var("POLYFLARE_AUTH_URL")
            .unwrap_or_else(|_| "https://auth.openai.com".to_string());
        let data_dir = data_dir_from_env();
        let continuity_watchdog = std::env::var("POLYFLARE_WATCHDOG_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(30));
        Ok(ServeConfig {
            bind_addr,
            upstream_base_url,
            anthropic_upstream_base_url,
            auth_base_url,
            db_path: db_path(&data_dir),
            key_path: key_path(&data_dir),
            continuity_watchdog,
        })
    }
}
```

- [ ] **Step 13: Wire both executors in `crates/polyflare-server/src/main.rs`**

Update the imports:

```rust
use polyflare_anthropic::AnthropicExecutor;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity, Executor, Selector};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::config::{self, ServeConfig};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{import_from_codex_lb, Store, TokenCipher};
```

Update `serve()`:

```rust
async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let config = ServeConfig::from_env()?;
    let store = Store::open(&config.db_path).await?;
    let cipher = TokenCipher::load_or_create(&config.key_path)?;
    let codex_executor: Arc<dyn Executor> = Arc::new(CodexExecutor::new()?);
    let anthropic_executor: Arc<dyn Executor> = Arc::new(AnthropicExecutor::new()?);
    let selector: Arc<dyn Selector> = Arc::new(CapacityWeighted);
    let oauth = OAuthClient::new(config.auth_base_url)?;
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        config.continuity_watchdog,
    ));

    let state = Arc::new(AppState {
        codex_executor,
        anthropic_executor,
        selector,
        continuity,
        store,
        cipher,
        oauth,
        upstream_base_url: config.upstream_base_url,
        anthropic_upstream_base_url: config.anthropic_upstream_base_url,
    });
    let app = build_app(state);

    let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;
    println!("polyflare listening on {}", config.bind_addr);
    axum::serve(listener, app).await?;
    Ok(())
}
```

- [ ] **Step 14: Update `resolve_core_account` + `responses_handler` in `crates/polyflare-server/src/ingress.rs`**

Update the imports:

```rust
use polyflare_codex::oauth::{classify_failure, should_refresh, OAuthError};
use polyflare_core::{
    Account, AccountId, ContinuityDirective, Prepared, PreparedRequest, Provider, RecoveryPlan,
    RequestCtx, ResponseStream, SelectionCtx,
};
use polyflare_store::PlainTokens;

use crate::app::AppState;
use crate::session_key::derive_request_ctx;
use crate::snapshot::{assemble_snapshots, filter_by_provider};
use crate::watchdog::{
    apply_ownership, execute_recovery, execute_with_watchdog, signal_client_stream, RouteDecision,
};
```

Replace `resolve_core_account` (it now returns the account's `Provider` too, and skips the Codex-only refresh check for Anthropic accounts — that OAuth client doesn't exist until Task 7):

```rust
/// Load + decrypt + refresh-if-stale the selected account, returning the core `Account` to execute
/// with plus its `Provider`, or a ready client-facing error `Response`.
async fn resolve_core_account(
    state: &AppState,
    picked: &AccountId,
    now: i64,
) -> Result<(Account, Provider), Response> {
    let repo = state.store.accounts();
    let account = match repo.get(picked.as_str()).await {
        Ok(Some(a)) => a,
        Ok(None) | Err(_) => return Err(internal_error()),
    };
    let provider: Provider = match account.provider.parse() {
        Ok(p) => p,
        Err(_) => return Err(internal_error()),
    };
    let mut tokens = match repo.decrypt_tokens(picked.as_str(), &state.cipher).await {
        Ok(Some(t)) => t,
        Ok(None) | Err(_) => return Err(internal_error()),
    };
    // Refresh-on-stale is Codex-specific (the only OAuth client AppState holds today); Anthropic
    // subscription-OAuth refresh is Task 7 (VERIFY-gated — no confirmed endpoint/client_id yet).
    // An Anthropic account's stored access_token is used as-is until Task 7 lands.
    if provider == Provider::Codex && should_refresh(account.last_refresh, now) {
        match state.oauth.refresh(&tokens.refresh_token).await {
            Ok(refreshed) => {
                let new = PlainTokens {
                    access_token: refreshed.tokens.access_token,
                    refresh_token: refreshed.tokens.refresh_token,
                    id_token: refreshed.tokens.id_token,
                };
                let _ = repo
                    .update_tokens(picked.as_str(), &new, &state.cipher, now)
                    .await;
                tokens = new;
            }
            Err(OAuthError::Endpoint {
                code: Some(code), ..
            }) => {
                if let Some(status) = classify_failure(&code).status() {
                    let _ = repo.update_status(picked.as_str(), status).await;
                }
                return Err(account_unavailable());
            }
            Err(OAuthError::Endpoint { code: None, .. }) | Err(OAuthError::MalformedJwt(_)) => {
                let _ = repo.update_status(picked.as_str(), "reauth_required").await;
                return Err(account_unavailable());
            }
            Err(OAuthError::Transport(_)) => {}
        }
    }
    Ok((
        Account {
            id: account.id,
            base_url: state.upstream_base_url_for(provider).to_string(),
            bearer_token: tokens.access_token,
        },
        provider,
    ))
}
```

Update `responses_handler`: filter to `Provider::Codex` right after assembling snapshots, and update all three `resolve_core_account` call sites to destructure `(account, provider)` and dispatch via `state.executor_for(provider)`:

```rust
pub async fn responses_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let model = body
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_string();
    let now = unix_now();

    // C3: derive continuity ctx from headers + body.
    let ctx: RequestCtx = derive_request_ctx(&headers, &body);
    let req = PreparedRequest { body, model };

    // C4: prepare (resolve owner + arm + recovery plan).
    let prepared = match state.continuity.prepare(req, &ctx).await {
        Ok(p) => p,
        Err(_) => return internal_error(),
    };

    let snapshots = match assemble_snapshots(&state.store).await {
        Ok(s) => s,
        Err(_) => return internal_error(),
    };
    // M4a has no cross-format translator (that's M4b): `/responses` may only ever pick a
    // Codex-provider account.
    let snapshots = filter_by_provider(&snapshots, Provider::Codex);
    let sel_ctx = SelectionCtx {
        now,
        require_security_work_authorized: false,
        rng_seed: None,
        session_id: ctx.session_id.clone(),
    };
    let session_key = prepared.directive.session_key.clone();

    // C5: ownership pre-filter.
    match apply_ownership(
        &prepared.directive,
        &snapshots,
        state.selector.as_ref(),
        &sel_ctx,
    ) {
        RouteDecision::Route(id) => {
            let (account, provider) = match resolve_core_account(&state, &id, now).await {
                Ok(a) => a,
                Err(r) => return r,
            };
            match execute_with_watchdog(
                state.executor_for(provider).as_ref(),
                state.continuity.clone(),
                prepared,
                &account,
                id,
                ctx,
            )
            .await
            {
                Ok(stream) => stream_response(stream),
                Err(_) => (StatusCode::BAD_GATEWAY, "upstream error").into_response(),
            }
        }
        RouteDecision::Recover => {
            // Owner pinned but ineligible: recover on a freshly-selected account (full pool), or
            // signal the client if the input is a bare tail.
            match prepared.directive.recovery {
                RecoveryPlan::ResendFull { anchorless_req } => {
                    let fresh = match state.selector.pick(&snapshots, &sel_ctx) {
                        Some(id) => id,
                        None => return no_eligible(),
                    };
                    let (account, provider) = match resolve_core_account(&state, &fresh, now).await
                    {
                        Ok(a) => a,
                        Err(r) => return r,
                    };
                    match execute_recovery(
                        state.executor_for(provider).as_ref(),
                        state.continuity.clone(),
                        anchorless_req,
                        &account,
                        fresh,
                        ctx,
                        session_key,
                    )
                    .await
                    {
                        Ok(stream) => stream_response(stream),
                        Err(_) => (StatusCode::BAD_GATEWAY, "upstream error").into_response(),
                    }
                }
                RecoveryPlan::SignalClient => {
                    let owner = prepared
                        .directive
                        .pin_account
                        .clone()
                        .unwrap_or_else(|| AccountId::from("unknown"));
                    let stream =
                        signal_client_stream(state.continuity.clone(), ctx, owner, session_key)
                            .await;
                    stream_response(stream)
                }
                RecoveryPlan::None => {
                    // No anchor ⇒ this request is self-sufficient (nothing to resume), so a
                    // pinned-but-ineligible owner (cooldown / rate-limited / reauth_required /
                    // a stale Soft session-row pin) is NOT fatal: fail over to any eligible
                    // account from the FULL candidate pool, ignoring the pin, and relay as a
                    // normal (Disarmed) request. `prepared.req` is still owned here — only
                    // `directive.recovery` was moved by the outer match.
                    match state.selector.pick(&snapshots, &sel_ctx) {
                        Some(fresh) => {
                            let (account, provider) =
                                match resolve_core_account(&state, &fresh, now).await {
                                    Ok(a) => a,
                                    Err(r) => return r,
                                };
                            let fallback = Prepared {
                                req: prepared.req,
                                directive: ContinuityDirective {
                                    pin_account: None,
                                    watchdog: prepared.directive.watchdog,
                                    recovery: RecoveryPlan::None,
                                    session_key: prepared.directive.session_key.clone(),
                                },
                            };
                            match execute_with_watchdog(
                                state.executor_for(provider).as_ref(),
                                state.continuity.clone(),
                                fallback,
                                &account,
                                fresh,
                                ctx,
                            )
                            .await
                            {
                                Ok(stream) => stream_response(stream),
                                Err(_) => {
                                    (StatusCode::BAD_GATEWAY, "upstream error").into_response()
                                }
                            }
                        }
                        None => no_eligible(),
                    }
                }
            }
        }
        RouteDecision::NoEligibleAccount => no_eligible(),
    }
}
```

- [ ] **Step 15: Fix the ten remaining `AppState { executor: ..., upstream_base_url: ... }` construction sites**

For each of `tests/{no_anchor_failover,wedge_regression,ownership,e2e_passthrough,refresh_path,signal_client}.rs` plus `tests/pool_selection.rs`, `tests/ingress_relays.rs`, and `tests/large_body.rs` (nine files total): replace the `executor: Arc::new(CodexExecutor::new().unwrap()),` line with two lines, and add one line after `upstream_base_url: <expr>,`. Using `ingress_relays.rs` (`crates/polyflare-server/tests/ingress_relays.rs:70-78`) as the concrete example — every other file follows the identical pattern:

```rust
    let state = Arc::new(AppState {
        codex_executor: Arc::new(CodexExecutor::new().unwrap()),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(), // never called (fresh token)
        upstream_base_url: upstream,
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
    });
```

Apply the same two-part edit (rename `executor` → `codex_executor` + add `anthropic_executor` right after it; add `anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),` right after the existing `upstream_base_url: ...,` line) in:
- `crates/polyflare-server/tests/large_body.rs`
- `crates/polyflare-server/tests/no_anchor_failover.rs:101,107`
- `crates/polyflare-server/tests/wedge_regression.rs:50,56`
- `crates/polyflare-server/tests/ownership.rs:97,103`
- `crates/polyflare-server/tests/e2e_passthrough.rs:69,75`
- `crates/polyflare-server/tests/refresh_path.rs:86,92`
- `crates/polyflare-server/tests/signal_client.rs:72,78`
- `crates/polyflare-server/tests/pool_selection.rs:25,31`

- [ ] **Step 16: Run tests to verify they pass**

Run: `cargo test --workspace`
Expected: PASS — every existing test still green, plus the two new `provider_dispatch.rs` tests.

- [ ] **Step 17: Commit**

```bash
git add crates/polyflare-core/src/types.rs \
        crates/polyflare-server/Cargo.toml crates/polyflare-server/src/app.rs \
        crates/polyflare-server/src/config.rs crates/polyflare-server/src/main.rs \
        crates/polyflare-server/src/snapshot.rs crates/polyflare-server/src/ingress.rs \
        crates/polyflare-server/tests/provider_dispatch.rs \
        crates/polyflare-server/tests/snapshot_assembly.rs \
        crates/polyflare-server/tests/large_body.rs \
        crates/polyflare-server/tests/no_anchor_failover.rs \
        crates/polyflare-server/tests/wedge_regression.rs \
        crates/polyflare-server/tests/ownership.rs \
        crates/polyflare-server/tests/e2e_passthrough.rs \
        crates/polyflare-server/tests/refresh_path.rs \
        crates/polyflare-server/tests/signal_client.rs \
        crates/polyflare-server/tests/pool_selection.rs \
        crates/polyflare-server/tests/ingress_relays.rs
git commit -m "feat(m4a): AppState per-provider executor dispatch, wired into /responses"
```

---

## Task 5: Anthropic-Messages ingress decode path (`POST /v1/messages`)

**Files:**
- Modify: `crates/polyflare-server/src/ingress.rs`
- Modify: `crates/polyflare-server/src/app.rs`
- Create: `crates/polyflare-server/tests/messages_ingress.rs`

**Interfaces:**
- Consumes: `filter_by_provider`, `AppState::executor_for`/`upstream_base_url_for`, `resolve_core_account` (all Task 4); `polyflare_core::NoopContinuity` (existing, from M3).
- Produces: `pub async fn messages_handler(State<Arc<AppState>>, HeaderMap, Json<Value>) -> Response`, routed at `POST /v1/messages`.

- [ ] **Step 1: Write the failing test**

Create `crates/polyflare-server/tests/messages_ingress.rs`:

```rust
//! The native Anthropic-Messages ingress path: `/v1/messages` selects only Anthropic-provider
//! accounts and relays through `AnthropicExecutor`; continuity is a no-op (SPEC-M4 §3.7 — no
//! `previous_response_id`-style anchor exists for this backend, so the watchdog never arms).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity, Executor};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockUpstream;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn anthropic_account(id: &str) -> Account {
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
        provider: "anthropic".to_string(),
    }
}

fn tokens() -> PlainTokens {
    PlainTokens {
        access_token: "tok".to_string(),
        refresh_token: "r".to_string(),
        id_token: "i".to_string(),
    }
}

async fn spawn_polyflare(store: Store, anthropic_upstream: String) -> String {
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    let codex_executor: Arc<dyn Executor> = Arc::new(CodexExecutor::new().unwrap());
    let anthropic_executor: Arc<dyn Executor> =
        Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap());

    let state = Arc::new(AppState {
        codex_executor,
        anthropic_executor,
        selector: Arc::new(CapacityWeighted),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: "http://127.0.0.1:9".to_string(),
        anthropic_upstream_base_url: anthropic_upstream,
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
async fn messages_relays_to_the_anthropic_executor() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[22u8; 32]).unwrap();
    store
        .accounts()
        .insert(&anthropic_account("anthropic-1"), &tokens(), &cipher)
        .await
        .unwrap();
    std::mem::forget(dir);

    let mock = MockUpstream::new(vec![
        r#"{"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}"#.to_string(),
        r#"{"type":"message_stop"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(store, upstream).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/v1/messages"))
        .json(&serde_json::json!({
            "model": "claude-opus-4",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let mut body = String::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    assert!(body.contains("content_block_delta"));
    assert!(body.contains("message_stop"));
    assert_eq!(handle.last_body().unwrap()["model"], "claude-opus-4");
}

#[tokio::test]
async fn messages_returns_503_when_pool_has_no_anthropic_account() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);

    let pf = spawn_polyflare(store, "http://127.0.0.1:9".to_string()).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/v1/messages"))
        .json(&serde_json::json!({"model": "claude-opus-4", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p polyflare-server --test messages_ingress`
Expected: FAIL at runtime — `messages_relays_to_the_anthropic_executor` gets HTTP 404 (no `/v1/messages` route registered yet), not 200.

- [ ] **Step 3: Implement `messages_handler` in `crates/polyflare-server/src/ingress.rs`**

Add to the imports:

```rust
use polyflare_core::{
    Account, AccountId, Continuity, ContinuityDirective, NoopContinuity, Prepared, PreparedRequest,
    Provider, RecoveryPlan, RequestCtx, ResponseStream, SelectionCtx,
};
```

(this replaces the Task-4 import list — `Continuity` and `NoopContinuity` are newly added)

Append the handler:

```rust
/// The native Anthropic-Messages ingress path: `POST /v1/messages`. Continuity is a no-op here
/// (SPEC-M4 §3.7: the Anthropic backend has no `previous_response_id`-style anchor), so every
/// request is `Disarmed` and `execute_with_watchdog`'s Disarmed branch just relays — the wedge
/// machinery never arms.
pub async fn messages_handler(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let model = body
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_string();
    let now = unix_now();
    let req = PreparedRequest { body, model };
    let ctx = RequestCtx::default();

    let prepared = match NoopContinuity.prepare(req, &ctx).await {
        Ok(p) => p,
        Err(_) => return internal_error(),
    };

    let snapshots = match assemble_snapshots(&state.store).await {
        Ok(s) => s,
        Err(_) => return internal_error(),
    };
    let snapshots = filter_by_provider(&snapshots, Provider::Anthropic);
    let sel_ctx = SelectionCtx {
        now,
        require_security_work_authorized: false,
        rng_seed: None,
        session_id: None,
    };
    let picked = match state.selector.pick(&snapshots, &sel_ctx) {
        Some(id) => id,
        None => return no_eligible(),
    };
    let (account, provider) = match resolve_core_account(&state, &picked, now).await {
        Ok(a) => a,
        Err(r) => return r,
    };

    match execute_with_watchdog(
        state.executor_for(provider).as_ref(),
        Arc::new(NoopContinuity) as Arc<dyn Continuity>,
        prepared,
        &account,
        picked,
        ctx,
    )
    .await
    {
        Ok(stream) => stream_response(stream),
        Err(_) => (StatusCode::BAD_GATEWAY, "upstream error").into_response(),
    }
}
```

- [ ] **Step 4: Register the route in `crates/polyflare-server/src/app.rs`**

```rust
use crate::ingress::{messages_handler, responses_handler};
```

```rust
pub fn build_app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/responses", post(responses_handler))
        .route("/v1/messages", post(messages_handler))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test -p polyflare-server --test messages_ingress`
Expected: PASS (2 tests).

- [ ] **Step 6: Run the full workspace suite**

Run: `cargo test --workspace`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/polyflare-server/src/ingress.rs crates/polyflare-server/src/app.rs \
        crates/polyflare-server/tests/messages_ingress.rs
git commit -m "feat(m4a): native Anthropic-Messages ingress (POST /v1/messages)"
```

---

## Task 6: Anthropic rate-limit / error classification module

**Files:**
- Modify: `crates/polyflare-anthropic/Cargo.toml`
- Modify: `crates/polyflare-anthropic/src/lib.rs`
- Create: `crates/polyflare-anthropic/src/errors.rs`

**Interfaces:**
- Produces: `AnthropicErrorType` (enum: `InvalidRequest | Authentication | Permission | NotFound | RequestTooLarge | RateLimit | Api | Overloaded | Unknown`) + `AnthropicErrorType::from_wire(&str) -> Self`; `AnthropicErrorBody { error: AnthropicErrorDetail, request_id: Option<String> }` (serde `Deserialize`) + `.classified() -> AnthropicErrorType`; `StatusClass` (enum) + `classify_status(u16) -> StatusClass`; `parse_retry_after_secs(&str) -> Option<Duration>`.

This task's shapes are doc-verified two ways: SPEC-M4 §3.5a (independent doc research) and, during this planning pass, directly against the Anthropic TypeScript SDK source via Context7 (`/anthropics/anthropic-sdk-typescript`) — confirming the `{"type":"error","error":{"type","message"},"request_id"}` body shape, the `error.type` vocabulary (`invalid_request_error`/`authentication_error`/`permission_error`/`not_found_error`/`rate_limit_error`/`api_error`, plus the doc-confirmed `overloaded_error` for HTTP 529), and that `Retry-After` is sent as a plain integer-seconds value in practice (the HTTP-date form is valid per RFC 7231 but not implemented here — YAGNI until a live capture ever shows it).

- [ ] **Step 1: Write the failing test**

Add `serde` to `crates/polyflare-anthropic/Cargo.toml`'s `[dependencies]`:

```toml
[dependencies]
polyflare-core = { path = "../polyflare-core" }
reqwest = { workspace = true }
futures-util = { workspace = true }
bytes = { workspace = true }
async-trait = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
```

Create `crates/polyflare-anthropic/src/errors.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_known_error_types() {
        assert_eq!(
            AnthropicErrorType::from_wire("rate_limit_error"),
            AnthropicErrorType::RateLimit
        );
        assert_eq!(
            AnthropicErrorType::from_wire("overloaded_error"),
            AnthropicErrorType::Overloaded
        );
        assert_eq!(
            AnthropicErrorType::from_wire("permission_error"),
            AnthropicErrorType::Permission
        );
        assert_eq!(
            AnthropicErrorType::from_wire("something_new"),
            AnthropicErrorType::Unknown
        );
    }

    #[test]
    fn classifies_known_statuses() {
        assert_eq!(classify_status(429), StatusClass::RateLimited);
        assert_eq!(classify_status(529), StatusClass::Overloaded);
        assert_eq!(classify_status(401), StatusClass::Authentication);
        assert_eq!(classify_status(403), StatusClass::Permission);
        assert_eq!(classify_status(504), StatusClass::ServerError);
        assert_eq!(classify_status(200), StatusClass::Unclassified);
    }

    #[test]
    fn parses_the_doc_verified_error_body_shape() {
        let body: AnthropicErrorBody = serde_json::from_str(
            r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"},"request_id":"req_1"}"#,
        )
        .unwrap();
        assert_eq!(body.classified(), AnthropicErrorType::RateLimit);
        assert_eq!(body.error.message, "slow down");
        assert_eq!(body.request_id.as_deref(), Some("req_1"));
    }

    #[test]
    fn retry_after_parses_plain_seconds() {
        assert_eq!(
            parse_retry_after_secs("30"),
            Some(std::time::Duration::from_secs(30))
        );
        assert_eq!(
            parse_retry_after_secs("  7 "),
            Some(std::time::Duration::from_secs(7))
        );
        assert_eq!(parse_retry_after_secs("not-a-number"), None);
    }
}
```

Register the module in `crates/polyflare-anthropic/src/lib.rs`:

```rust
pub mod errors;
pub mod executor;

pub use errors::{
    classify_status, parse_retry_after_secs, AnthropicErrorBody, AnthropicErrorDetail,
    AnthropicErrorType, StatusClass,
};
pub use executor::AnthropicExecutor;
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p polyflare-anthropic errors`
Expected: FAIL to compile — none of `AnthropicErrorType`/`classify_status`/`AnthropicErrorBody`/`parse_retry_after_secs`/`StatusClass` exist yet.

- [ ] **Step 3: Write minimal implementation**

Above the `#[cfg(test)]` block in `crates/polyflare-anthropic/src/errors.rs`:

```rust
//! Anthropic error/rate-limit classification: the doc-verified surface shared by both the
//! platform API-key and Claude subscription-OAuth pools (SPEC-M4 §3.5a). Header-based rate-limit
//! signals (`anthropic-ratelimit-*`) are API-key-only and NOT read here; the ccflare-style
//! subscription signal set is VERIFY-gated (see Task 7 in `docs/PLAN-M4a.md`).

use std::time::Duration;

/// The Anthropic error-response `error.type` vocabulary that is doc-verified and shared by both
/// account surfaces (SPEC-M4 §3.5a). Reused verbatim for mid-stream SSE `error` events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnthropicErrorType {
    InvalidRequest,
    Authentication,
    Permission,
    NotFound,
    RequestTooLarge,
    RateLimit,
    Api,
    Overloaded,
    /// Any `error.type` string not in the doc-verified set above.
    Unknown,
}

impl AnthropicErrorType {
    pub fn from_wire(s: &str) -> Self {
        match s {
            "invalid_request_error" => Self::InvalidRequest,
            "authentication_error" => Self::Authentication,
            "permission_error" => Self::Permission,
            "not_found_error" => Self::NotFound,
            "request_too_large" => Self::RequestTooLarge,
            "rate_limit_error" => Self::RateLimit,
            "api_error" => Self::Api,
            "overloaded_error" => Self::Overloaded,
            _ => Self::Unknown,
        }
    }
}

/// The always-present Anthropic error-response envelope: `{"type":"error","error":{"type",
/// "message"},"request_id"}` (SPEC-M4 §3.5a; doc-verified against the Anthropic TypeScript SDK's
/// error-extraction code, `src/core/error.ts`).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct AnthropicErrorBody {
    pub error: AnthropicErrorDetail,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct AnthropicErrorDetail {
    #[serde(rename = "type")]
    pub error_type: String,
    pub message: String,
}

impl AnthropicErrorBody {
    /// The classified `error.type`.
    pub fn classified(&self) -> AnthropicErrorType {
        AnthropicErrorType::from_wire(&self.error.error_type)
    }
}

/// Classify an HTTP status into the doc-verified bucket (SPEC-M4 §3.5a): 429 is rate-limiting,
/// 529 is Anthropic's confirmed-real overload status, 401/403/404/413/500..504 map to their
/// standard meaning. Anything else is `Unclassified`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusClass {
    RateLimited,
    Overloaded,
    Authentication,
    Permission,
    NotFound,
    RequestTooLarge,
    ServerError,
    Unclassified,
}

pub fn classify_status(status: u16) -> StatusClass {
    match status {
        429 => StatusClass::RateLimited,
        529 => StatusClass::Overloaded,
        401 => StatusClass::Authentication,
        403 => StatusClass::Permission,
        404 => StatusClass::NotFound,
        413 => StatusClass::RequestTooLarge,
        500..=504 => StatusClass::ServerError,
        _ => StatusClass::Unclassified,
    }
}

/// Parse the `Retry-After` header as a plain integer number of seconds — the form Anthropic's API
/// sends in practice. The HTTP-date form is valid per RFC 7231 but not implemented here (YAGNI —
/// revisit if a live capture ever shows it).
pub fn parse_retry_after_secs(value: &str) -> Option<Duration> {
    value.trim().parse::<u64>().ok().map(Duration::from_secs)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p polyflare-anthropic errors`
Expected: PASS (4 tests).

- [ ] **Step 5: Run the full workspace suite**

Run: `cargo test --workspace`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/polyflare-anthropic/Cargo.toml crates/polyflare-anthropic/src/lib.rs \
        crates/polyflare-anthropic/src/errors.rs
git commit -m "feat(m4a): Anthropic error/rate-limit classification (429/529/error-body/retry-after)"
```

---

## Task 7: Anthropic subscription OAuth (client_id, endpoints, token exchange)

**⚠️ VERIFY-AT-IMPL (needs runtime capture):** the real Claude Max/Pro subscription OAuth `client_id`, the authorize/token endpoint host + path, the `scope` string, and the ccflare-style subscription rate-limit signal set (`out_of_credits` / `extra_usage` / windowed reset / 24h-clamp) must be captured from a **live Claude Max/Pro OAuth login** before this task can be implemented for real — exactly as `polyflare-codex/src/oauth.rs`'s `CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"` and `SCOPE = "openid profile email"` were pinned only after reading the real Codex CLI's OAuth flow (`docs/reference/codex-lb-port-reference.md` §OAuth). **No equivalent reference document exists yet for Anthropic subscription OAuth** — this is a genuine gap, not an oversight.

**Files (once verified):**
- Create: `crates/polyflare-anthropic/src/oauth.rs`
- Modify: `crates/polyflare-anthropic/src/lib.rs` (export)
- Modify: `crates/polyflare-anthropic/Cargo.toml` (add `base64`)

**What's structurally known now (safe to note, not yet code):** the shape mirrors `polyflare_codex::oauth` — `OAuthClient::new(auth_base_url: impl Into<String>) -> Result<Self, OAuthError>`, `async fn refresh(&self, refresh_token: &str) -> Result<Refreshed, OAuthError>`, a `FailureClass` enum (`ReauthRequired | Deactivated | Transient`) mapping refresh-endpoint error codes to `AccountRepo::update_status` transitions, and — non-negotiably, per the Global Constraints — a redacting `Debug` on any new token-carrying struct plus a redaction test, exactly like `RefreshedTokens` in `polyflare-codex`.

**What's UNKNOWN — placeholders only, not to be treated as real values:**
- `CLIENT_ID` — placeholder `"VERIFY_CLAUDE_OAUTH_CLIENT_ID"`. Capture from a live Claude Code / Claude Max OAuth login (packet capture, or reading the Claude Code CLI's own OAuth constants if its source is available).
- The auth host — placeholder `"https://console.anthropic.com"` or `"https://claude.ai"` (VERIFY which host actually issues subscription OAuth tokens; they may differ).
- The token endpoint path — placeholder `/oauth/token` (mirrors Codex's shape; VERIFY against a live flow — Anthropic's path may differ).
- `SCOPE` — placeholder `"VERIFY_SCOPE_STRING"`.
- The subscription rate-limit signal set — VERIFY the exact response shape ccflare's `out_of_credits`/`extra_usage`/window-reset/24h-clamp names take on a real rate-limited Claude Max OAuth account (SPEC-M4 §3.5a + §7 risk list — better-ccflare's implementation is a starting reference, not a confirmed-correct port target).

**Why this can't be TDD'd today:** every one of the unknowns above is either a literal wire constant (client_id/scope/path) or a live response shape — inventing them and writing "passing" tests against invented values would test nothing real and risk the placeholders being mistaken for facts later. The step below defers actual test-writing until the capture happens.

- [ ] **Step 1 (blocked until capture): Capture the real values**

Perform (or obtain from someone who has performed) a live Claude Max/Pro OAuth login; record the exact `client_id`, auth host, token endpoint path, `scope`, and a captured 429/rate-limited response body from a real subscription-OAuth account. Write the findings into a new `docs/reference/anthropic-oauth-reference.md` (mirroring the existing `docs/reference/codex-lb-port-reference.md` §OAuth format) before proceeding.

- [ ] **Step 2 (once captured): Write the failing test**

Mirror `crates/polyflare-codex/tests/oauth_refresh.rs` exactly — a `MockOAuth`-driven refresh test asserting the request body's `grant_type`/`client_id`/`scope`/`refresh_token` against the now-confirmed real values, plus a `classify_failure`-equivalent test table once the real permanent-failure codes (if any exist for Anthropic — VERIFY, do not assume Codex's set applies) are known.

- [ ] **Step 3 (once captured): Implement `OAuthClient::refresh`** against the confirmed real endpoint, with a redacting `Debug` + redaction test on the token struct (non-negotiable per Global Constraints).

- [ ] **Step 4: Run to green, then commit** following the same TDD/commit pattern as Tasks 1–6.

---

## Task 8: TA6 Anthropic capability — classify a rejection toward `NeedsCapability`

**⚠️ VERIFY-AT-IMPL (partially executable now — see split below).** SPEC-M4 §3.7 (research-resolved): Anthropic exposes no documented per-account/org "approved" or entitlement flag in any API response, header, or error, so the capability flag stays **operator-set** (`Account.security_work_authorized`, exactly like codex-lb's flag and the existing hard pre-filter in `crates/polyflare-core/src/select.rs`) — never derived from the API. TA6's only Anthropic-specific job is the **reactive** half: recognizing that an error response looks like a capability/policy rejection.

**Spec-vs-codebase gap (flagged, not silently resolved):** SPEC-M4 §3.7 describes routing a rejection into "the neutral `NeedsCapability` error the retry loop already understands" — **no `NeedsCapability` type or generic capability-retry loop exists anywhere in the current codebase** (verified by search across all crates). Only the existing **hard pre-filter** exists (`SelectionCtx.require_security_work_authorized` + `AccountSnapshot.security_work_authorized`, enforced in `CapacityWeighted::pick`). Wiring a classified rejection into a generic retry-and-reselect loop is blocked on that mechanism being designed and built — likely its own cross-cutting milestone, not M4a. This task is scoped down to what's real and buildable today: the classifier.

**Files:**
- Modify: `crates/polyflare-anthropic/src/errors.rs` (uses `AnthropicErrorType` from Task 6)

**Interfaces:**
- Consumes: `AnthropicErrorType` (Task 6).
- Produces: `pub fn is_capability_rejection(error_type: AnthropicErrorType) -> bool`.

- [ ] **Step 1: Write the failing test**

Append to the `#[cfg(test)] mod tests` block in `crates/polyflare-anthropic/src/errors.rs`:

```rust
    #[test]
    fn permission_error_is_treated_as_a_capability_rejection() {
        assert!(is_capability_rejection(AnthropicErrorType::Permission));
        assert!(!is_capability_rejection(AnthropicErrorType::RateLimit));
        assert!(!is_capability_rejection(AnthropicErrorType::Authentication));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p polyflare-anthropic errors`
Expected: FAIL to compile — `is_capability_rejection` doesn't exist.

- [ ] **Step 3: Write minimal implementation**

Add to `crates/polyflare-anthropic/src/errors.rs` (above the test module):

```rust
/// TA6 (SPEC-M4 §3.7): the reactive half of Anthropic capability detection. Does this error
/// response look like a capability/policy rejection (as opposed to an ordinary transient or auth
/// error), so the caller can exclude the account and retry elsewhere?
///
/// ⚠️ VERIFY-AT-IMPL: `permission_error` is the only doc-verified `error.type` that plausibly maps
/// to a capability rejection, but Anthropic's docs don't distinguish "you lack entitlement X" from
/// "malformed/expired credential" within that single error type — both surface as
/// `permission_error`. A live-captured example of a genuine policy/capability rejection (message
/// text, or a distinguishing sub-field) is needed to refine this beyond "any permission_error is
/// capability-shaped", which today conflates the two. Wiring this signal into a generic
/// capability-retry loop additionally depends on a `NeedsCapability` mechanism that does not exist
/// in the codebase yet (see the Task 8 gap note in `docs/PLAN-M4a.md`).
pub fn is_capability_rejection(error_type: AnthropicErrorType) -> bool {
    matches!(error_type, AnthropicErrorType::Permission)
}
```

Add `is_capability_rejection` to the `pub use errors::{...}` list in `crates/polyflare-anthropic/src/lib.rs`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p polyflare-anthropic errors`
Expected: PASS (5 tests in that file).

- [ ] **Step 5: Run the full workspace suite**

Run: `cargo test --workspace`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/polyflare-anthropic/src/errors.rs crates/polyflare-anthropic/src/lib.rs
git commit -m "feat(m4a): classify permission_error as a TA6 capability rejection (VERIFY-gated wiring)"
```

**Remaining VERIFY-AT-IMPL work (not executable now):** (a) refine `is_capability_rejection` beyond the blunt "any `permission_error`" heuristic once a real rejection is captured; (b) design and build the `NeedsCapability` mechanism + retry-and-reselect loop SPEC-M4 assumes exists (a gap in the current codebase, not just in Anthropic support) and wire this classifier into it.

---

## Self-Review

**1. Spec coverage** (against SPEC-M4 §3.1–§3.7, §4 Q6, §6 for the M4a slice):
- §3.2 per-provider executor dispatch → Task 4.
- §3.3 store provider modeling → Task 2 (+ `Provider` enum, Task 1).
- §3.5a rate-limit/error module (doc-verified shared parts) → Task 6.
- §3.6 (out of scope for M4a — model-alias/payload-override is M4b) → correctly excluded.
- §3.7 TA6 → Task 8 (scoped to what's real; gap flagged).
- §4 Q6 "M4a: store + dispatch + Anthropic-Messages ingress + Anthropic HTTP executor + rate-limit + TA6 detection + Anthropic OAuth" → Tasks 1–8 cover every listed item; §4 Q6's "your headline feature" (M4b, the cross-translator) is correctly NOT built here.
- §6 testing strategy: "Dispatch — an Anthropic-provider account routes to the Anthropic executor; a Codex account to Codex" → Task 4's `provider_dispatch.rs`. "Anthropic executor — non-2xx → ExecError" → Task 3's `executor_surfaces_upstream_error_status`. "Rate-limit classification unit tests" → Task 6. Golden-replay/model-alias/e2e-as-Sol testing strategy items are M4b, correctly excluded.

**2. Placeholder scan:** Tasks 1–6 and 8 contain complete, real code in every step — no "add error handling"/"TBD" patterns. Task 7 intentionally contains placeholder constants, but each is explicitly labeled `VERIFY_...` and paired with exactly what must be captured to replace it, per the top-level instruction's carve-out for verification-gated tasks.

**3. Type consistency check across tasks:**
- `Provider` (Task 1: `Codex`/`Anthropic`, `Display`/`FromStr`) is used identically in `AccountSnapshot.provider` (Task 4), `filter_by_provider` (Task 4), `AppState::executor_for`/`upstream_base_url_for` (Task 4), `resolve_core_account`'s return type (Task 4), and `messages_handler` (Task 5) — no drift.
- `resolve_core_account(..) -> Result<(Account, Provider), Response>` (redefined in Task 4) is called with the same destructuring `let (account, provider) = ...` at all four call sites across Tasks 4–5.
- `AppState` field names (`codex_executor`, `anthropic_executor`, `upstream_base_url`, `anthropic_upstream_base_url`) introduced in Task 4 are used identically in every test file touched by Tasks 4–5 (`provider_dispatch.rs`, `messages_ingress.rs`, and the nine mechanically-updated fixtures).
- `AnthropicExecutor::new() -> Result<Self, ExecError>` (Task 3) is the exact constructor called in Tasks 4, 5.
- `AnthropicErrorType`/`classify_status`/`AnthropicErrorBody`/`parse_retry_after_secs` (Task 6) and `is_capability_rejection` (Task 8) share one module (`errors.rs`) and one `pub use` list — no cross-task duplication.
