//! D17 Task 2: soft session→owner affinity account resolution for CONTROL requests
//! (`thread/goal/*`, `agent-identities/jwks`, `memories/trace_summarize` — the D17 minimal
//! control-endpoint set; see `docs/superpowers/plans/2026-07-18-d17-control-endpoints.md`).
//!
//! Unlike `/responses`'s HARD `previous_response_id` anchor (`crate::watchdog::apply_ownership`,
//! which narrows to the pinned owner and RECOVERS — never falls back to a different account — when
//! that owner turns out ineligible), a control request has no such anchor. Binding it to the
//! conversation's owner here is a SOFT, best-effort optimization: a request that carries no session
//! header, or whose owner happens to be unavailable right now, ALWAYS falls through to normal
//! (any-eligible) selection — the exact same machinery `/responses` uses when unowned. Over-binding
//! this into a hard pin-or-fail is the primary risk the D17 scoping study flagged (Global
//! Constraints: "SOFT affinity — do NOT over-bind (inviolable for correctness)").

use axum::http::HeaderMap;
use axum::response::Response;
use polyflare_core::{Account, AccountId, Provider, SelectionCtx};

use crate::app::AppState;
use crate::ingress::{internal_error, no_eligible, resolve_core_account};
use crate::session_key::header_session_key;
use crate::snapshot::filter_by_provider_and_pool;

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Resolve which account a control request should be forwarded to, then materialize it (decrypt +
/// refresh-if-stale, via `crate::ingress::resolve_core_account` — unchanged from `/responses`).
///
/// 1. Derive a session key from the request's headers ONLY
///    (`crate::session_key::header_session_key` — the SAME Hard-strength derivation
///    `session_key::parse_inbound` uses for `x-codex-turn-state`/`session_id`/`x-session-id`;
///    control carries no body to fall back into a content-derived soft key from, and doesn't need
///    one for this purpose — an absent session header should read as "no affinity signal", not
///    manufacture one).
/// 2. If a session key was derived AND its continuity session row names an owner
///    (`ContinuityRepo::get_session(..).owning_account_id` — the same read-only primitive
///    `CodexContinuity::prepare` uses) AND that owner is currently ELIGIBLE (appears pickable in
///    the overlaid + provider/pool-filtered snapshot — checked by narrowing candidates to just the
///    owner and running it through the SAME selector, mirroring `watchdog::apply_ownership`'s
///    narrow-then-pick shape) ⇒ use the owner (soft affinity hit).
/// 3. OTHERWISE (no session header, no owner on record, or the owner is currently ineligible —
///    benched/cooled-down/inactive/absent from the pool) ⇒ fall through to the SAME any-eligible
///    selection `/responses` uses when unowned: `account_cache.snapshots()` →
///    `filter_by_provider_and_pool(Codex, None)` → `runtime.overlay` → `selector.pick`. **This
///    fallback is INVIOLABLE** — a control call must never be stranded merely because its owner
///    happens to be unavailable right now (contrast `apply_ownership`'s `RouteDecision::Recover`,
///    which is correct for `/responses`'s hard anchor but would be over-binding here).
/// 4. `resolve_core_account` the chosen id.
///
/// No eligible account at all (neither the owner nor any fallback candidate) ⇒ a clean 503
/// (`crate::ingress::no_eligible`, byte-identical to `/responses`'s empty-pool response).
pub async fn resolve_control_account(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(Account, AccountId), Response> {
    let now = unix_now();

    let snapshots = match state.account_cache.snapshots(&state.store).await {
        Ok(s) => s,
        Err(_) => return Err(internal_error()),
    };
    // D17 is scoped to Codex control endpoints only (the plan's entire subject); control requests
    // are never pool-scoped today (no `/{pool}/…` control route exists), so `pool = None` — the
    // same "select over ALL accounts" behavior the bare `/responses` path uses when unowned.
    let mut snapshots = filter_by_provider_and_pool(&snapshots, Provider::Codex, None);
    state.runtime.overlay(&mut snapshots, now);
    let selector = state.selector_for(None);
    let sel_ctx = SelectionCtx {
        now,
        ..Default::default()
    };

    // Step 1: header-only session key (no body — control has none to derive a soft key from).
    let session_key = header_session_key(headers, None);

    // Step 2: soft owner lookup — a read-only session-row fetch, no write, no watchdog arm, no
    // recovery plan (control has none of those concepts; this is deliberately NOT
    // `Continuity::prepare`, which would also mutate the session row's state).
    let owner: Option<AccountId> = match session_key.as_ref() {
        Some(sk) => match state.store.continuity().get_session(&sk.value).await {
            Ok(Some(row)) => row.owning_account_id.map(AccountId::from),
            Ok(None) | Err(_) => None,
        },
        None => None,
    };

    // Step 2 (eligibility) + Step 3 (inviolable fallback).
    let picked = match owner {
        Some(owner_id) => {
            let narrowed: Vec<_> = snapshots
                .iter()
                .filter(|s| s.id == owner_id)
                .cloned()
                .collect();
            match selector.pick(&narrowed, &sel_ctx) {
                // The owner is present in the eligible pool and the selector accepted it (the
                // narrowed candidate list has exactly one member, so a `Some` here can only ever
                // be that owner) — soft affinity hit.
                Some(id) => id,
                // Owner absent from the pool entirely, or present but currently ineligible
                // (benched/cooled-down/inactive/wrong-provider) ⇒ NEVER stranded: fall through to
                // the same any-eligible selection an unowned request would get.
                None => match selector.pick(&snapshots, &sel_ctx) {
                    Some(id) => id,
                    None => return Err(no_eligible()),
                },
            }
        }
        None => match selector.pick(&snapshots, &sel_ctx) {
            Some(id) => id,
            None => return Err(no_eligible()),
        },
    };

    let (account, _provider) = resolve_core_account(state, &picked, now).await?;
    Ok((account, picked))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use axum::http::HeaderName;
    use polyflare_codex::oauth::OAuthClient;
    use polyflare_codex::CodexExecutor;
    use polyflare_core::{Continuity, RoundRobin, Selector};
    use polyflare_store::{Account as StoreAccount, PlainTokens, Store, TokenCipher};

    use crate::continuity::CodexContinuity;

    fn now() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    fn hdr(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        h
    }

    fn account(id: &str) -> StoreAccount {
        StoreAccount {
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
            provider: "codex".to_string(),
            pool: None,
        }
    }

    async fn seed_account(store: &Store, cipher: &TokenCipher, id: &str, token: &str) {
        store
            .accounts()
            .insert(
                &account(id),
                &PlainTokens {
                    access_token: token.into(),
                    refresh_token: "r".into(),
                    id_token: "i".into(),
                },
                cipher,
            )
            .await
            .unwrap();
    }

    /// Builds a full `AppState` for these tests, mirroring `tests/ownership.rs`'s construction
    /// pattern exactly. `Store` is NOT `Clone`, so (matching that existing pattern) callers reach
    /// the store/cipher back out via `state.store`/`state.cipher`, never a separately-held copy.
    async fn build_state(selector: Arc<dyn Selector>) -> Arc<AppState> {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("s.db")).await.unwrap();
        std::mem::forget(dir);
        let cipher = TokenCipher::from_key_bytes(&[9u8; 32]).unwrap();
        let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
            store.continuity(),
            Duration::from_secs(30),
        ));
        Arc::new(AppState {
            enforce_client_keys: false,
            codex_executor: Arc::new(CodexExecutor::new().unwrap()),
            control_client: polyflare_codex::build_client().expect("build control_client"),
            anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
            selector,
            pool_selectors: Default::default(),
            continuity,
            store,
            cipher,
            oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
            upstream_base_url: "http://127.0.0.1:9".to_string(),
            anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
            refresh_locks: Default::default(),
            capture_fingerprint_path: None,
            codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
            account_cache: Arc::new(crate::account_cache::AccountCache::new()),
            token_cache: Default::default(),
            admin_token: None,
            live_logs: false,
            log_bus: crate::log_bus::LogBus::new(1000),
            max_account_attempts: 3,
            failover_metrics: crate::observability::FailoverMetrics::new(),
            starvation_wait_budget: Duration::from_secs(60),
            starvation_heartbeat: Duration::from_secs(10),
            starvation_metrics: crate::observability::StarvationMetrics::new(),
            stream_idle_timeout: Duration::from_secs(300),
            runtime: Default::default(),
        })
    }

    /// (a) A control request carrying a session header whose session has a KNOWN, ELIGIBLE owner
    /// resolves to that OWNER — even though the REAL `RoundRobin` selector, unpinned, would prefer
    /// a DIFFERENT account (both accounts are fresh/never-selected, so `RoundRobin`'s tiebreak
    /// falls to account id ascending, i.e. "A" — see `no_session_header_falls_back_to_normal_
    /// selection` below, which proves that fact directly). Owner is deliberately seeded as "B" —
    /// the NON-default pick — so this test cannot pass by coincidence.
    #[tokio::test]
    async fn session_header_with_eligible_owner_resolves_to_owner() {
        let state = build_state(Arc::new(RoundRobin)).await;
        seed_account(&state.store, &state.cipher, "A", "tokA").await;
        seed_account(&state.store, &state.cipher, "B", "tokB").await;
        // Seed the continuity session row (under the SAME key `header_session_key` derives for
        // this exact header) naming "B" as the owner.
        let now = now();
        let headers = hdr(&[("x-codex-turn-state", "ts-owned")]);
        let sk = header_session_key(&headers, None).unwrap();
        state
            .store
            .continuity()
            .ensure_session(&sk.value, "hard", now)
            .await
            .unwrap();
        state
            .store
            .continuity()
            .record_completion(&sk.value, "hard", "B", "resp_owned", "fp", 1, now)
            .await
            .unwrap();

        let (_account, picked) = resolve_control_account(&state, &headers).await.unwrap();
        assert_eq!(
            picked,
            AccountId::from("B"),
            "resolved to the session's owner (B), not RoundRobin's default tiebreak pick (A)"
        );
    }

    /// (b) A control request with NO session header resolves to a selected (any-eligible) account
    /// via normal selection — it must NOT error. Also establishes the baseline fact
    /// `session_header_with_eligible_owner_resolves_to_owner` depends on: `RoundRobin`, unpinned,
    /// over two fresh (never-selected) accounts, deterministically ties to the lexicographically
    /// smaller account id — "A".
    #[tokio::test]
    async fn no_session_header_falls_back_to_normal_selection() {
        let state = build_state(Arc::new(RoundRobin)).await;
        seed_account(&state.store, &state.cipher, "A", "tokA").await;
        seed_account(&state.store, &state.cipher, "B", "tokB").await;

        let headers = hdr(&[]);
        let (_account, picked) = resolve_control_account(&state, &headers)
            .await
            .expect("must not error when no session header is present");
        assert_eq!(
            picked,
            AccountId::from("A"),
            "RoundRobin's any-eligible tiebreak choice, proving normal selection ran"
        );
    }

    /// (c) A session header whose owner is INELIGIBLE (benched via a rate-limit cooldown — the
    /// SAME runtime API the request-failure path uses, not a test-only backdoor) falls back to
    /// ANOTHER eligible account — never stranded, and never the benched owner. This is the central
    /// inviolable: over-binding to an unavailable owner (à la `/responses`'s hard anchor recovery)
    /// would be the bug.
    #[tokio::test]
    async fn ineligible_owner_falls_back_to_another_eligible_account() {
        let state = build_state(Arc::new(RoundRobin)).await;
        seed_account(&state.store, &state.cipher, "A", "tokA").await;
        seed_account(&state.store, &state.cipher, "B", "tokB").await;

        let headers = hdr(&[("x-codex-turn-state", "ts-benched")]);
        let sk = header_session_key(&headers, None).unwrap();
        let now = now();
        state
            .store
            .continuity()
            .ensure_session(&sk.value, "hard", now)
            .await
            .unwrap();
        state
            .store
            .continuity()
            .record_completion(&sk.value, "hard", "B", "resp_benched", "fp", 1, now)
            .await
            .unwrap();
        // Bench the owner "B" (a real cooldown — `RuntimeStates::overlay` applies `cooldown_until`
        // onto the snapshot at selection time, and `select.rs`'s real eligibility gate rejects it
        // regardless of the account's durable `status`).
        state
            .runtime
            .record_rate_limit(&AccountId::from("B"), Some(3600), now);

        let (_account, picked) = resolve_control_account(&state, &headers)
            .await
            .expect("an ineligible owner must fall back, never 503 or hang");
        assert_ne!(
            picked,
            AccountId::from("B"),
            "must NOT return the benched owner"
        );
        assert_eq!(
            picked,
            AccountId::from("A"),
            "falls back to the other eligible account"
        );
    }

    /// (d) No eligible account at all (empty pool) ⇒ a clean 503, matching `/responses`'s
    /// `no_eligible()` — never a panic, never a hang.
    #[tokio::test]
    async fn no_eligible_account_at_all_yields_503() {
        let state = build_state(Arc::new(RoundRobin)).await;
        // No accounts seeded at all.
        let headers = hdr(&[]);
        let err = resolve_control_account(&state, &headers)
            .await
            .expect_err("empty pool must error, not panic");
        assert_eq!(err.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Regression: an owner recorded for a DIFFERENT session key must never leak into a request
    /// whose own session key resolves to no owner (a distinct, never-seen key, NOT the same as "no
    /// header at all" — proves the lookup is keyed correctly, not just "any session row exists").
    /// The seeded owner is "B" (the non-default pick) so a leak would be distinguishable from the
    /// genuine fallback result ("A").
    #[tokio::test]
    async fn unrelated_sessions_owner_never_leaks() {
        let state = build_state(Arc::new(RoundRobin)).await;
        seed_account(&state.store, &state.cipher, "A", "tokA").await;
        seed_account(&state.store, &state.cipher, "B", "tokB").await;

        let owned_headers = hdr(&[("x-codex-turn-state", "ts-other")]);
        let owned_key = header_session_key(&owned_headers, None).unwrap();
        let now = now();
        state
            .store
            .continuity()
            .ensure_session(&owned_key.value, "hard", now)
            .await
            .unwrap();
        state
            .store
            .continuity()
            .record_completion(&owned_key.value, "hard", "B", "resp_other", "fp", 1, now)
            .await
            .unwrap();

        // A DIFFERENT session header — its row was never created, so `get_session` returns `None`.
        let fresh_headers = hdr(&[("x-codex-turn-state", "ts-fresh-unseen")]);
        let (_account, picked) = resolve_control_account(&state, &fresh_headers)
            .await
            .unwrap();
        assert_eq!(
            picked,
            AccountId::from("A"),
            "an unrelated/unseen session key must fall back to normal selection, not B's ownership"
        );
    }
}
