//! WS-downstream relay Task 3: resolve + pin the conversation's OWNER account.
//!
//! One downstream WS = one conversation, pinned to ONE upstream account for its life (design §4).
//! At CONNECT time [`resolve_owner`] answers the single question "which account owns this
//! conversation?":
//! - if the conversation already has a pinned owner (recorded by the continuity engine on a prior
//!   turn's `response.completed`) AND that owner is still ELIGIBLE (present in the overlaid,
//!   provider/pool-filtered snapshots — i.e. not deactivated / in-cooldown / quota-exhausted),
//!   REUSE it — sticky (the client's `previous_response_id` anchor resumes on the same account, so
//!   no cross-account wedge);
//! - otherwise (no pin on record, or the pin is currently ineligible) SELECT a healthy Codex
//!   account via the existing selector — that account becomes the owner (the ownership map is
//!   written LATER, by Task 6's content-free `response.completed` sniff, NOT here).
//!
//! **Reuse, don't reinvent (design §8 wedge-sacred).** This is *exactly* the owner-affine
//! resolution the codex HTTP control/compact endpoints already perform — `get_session().owning_
//! account_id` → reuse-if-eligible → else `filter_by_provider_and_pool(Codex) → runtime.overlay →
//! Selector::pick` — so [`resolve_owner`] DELEGATES to that one implementation
//! ([`crate::control::resolve_owner_affine_account`]) rather than growing a second, parallel
//! selection path. The "hardness" of WS pinning (never move mid-connection except on durable
//! exhaustion) is Tasks 4-6's reconnect-vs-move logic; the initial *resolve* is precisely the
//! reuse-eligible-owner-else-select shape that function already encodes.
//!
//! **Content-free (inviolable):** only the account **id** (a non-secret, as in ingress logs) is
//! ever surfaced; the `session_key` value and the account bearer are never logged here.

use polyflare_core::{Account, SessionKey};

use crate::app::AppState;

/// Why the conversation's owner account could not be resolved. Kept intentionally small — the WS
/// relay only needs to know whether to close the downstream socket because no account is available
/// (`NoEligibleAccount`, a clean upstream-capacity condition) or because a lower-level lookup
/// failed (`Internal`).
// A seam held for Tasks 4-6: the accept handler wires `resolve_owner` (and matches on this error)
// once the real pump replaces `relay_stub`. Not yet referenced from non-test code.
#[allow(dead_code)]
#[derive(Debug, thiserror::Error)]
pub(crate) enum RelayError {
    /// No Codex account is currently eligible to own the conversation (neither the pinned owner nor
    /// any selectable fallback) — the WS-relay analogue of the HTTP path's `503 no eligible
    /// account`.
    #[error("no eligible Codex account to own the conversation")]
    NoEligibleAccount,
    /// A lower-level lookup failed (e.g. the snapshot/account read) — the analogue of the HTTP
    /// path's generic `500`.
    #[error("internal error resolving the conversation owner")]
    Internal,
}

/// Resolve — and thereby pin, in memory, for this connection's life — the upstream Codex account
/// that owns the conversation identified by `session_key`.
///
/// Returns the full [`Account`] (not just an id) because Task 4's upstream WS dial needs it. The
/// pin is held by the CALLER (the relay task holds the returned `Account`); this function does NOT
/// write the ownership map — that is Task 6's job, on the first `response.completed`.
///
/// Delegates to [`crate::control::resolve_owner_affine_account`] with `pool = None` (the Phase-1
/// MVP is unpooled, mirroring the bare `/responses` path) — the SAME continuity `get_session` +
/// `filter_by_provider_and_pool(Codex)` + `Selector::pick` engine the HTTP path uses. Its
/// client-facing error `Response` is mapped back into a [`RelayError`] by status (`503 → NoEligible
/// Account`, anything else → `Internal`).
// A seam held for Tasks 4-6 (the pump calls this on connect); covered by this module's unit tests
// now, wired into `responses_ws_handler` when the real pump replaces `relay_stub`.
#[allow(dead_code)]
pub(crate) async fn resolve_owner(
    state: &AppState,
    session_key: &SessionKey,
) -> Result<Account, RelayError> {
    // `pool = None`: Phase-1 MVP is unpooled (design §4), mirroring the bare `/responses` path.
    // The returned `AccountId` is discarded — the caller pins the full `Account`; the ownership map
    // is written later by Task 6 (`observe`), not here.
    match crate::control::resolve_owner_affine_account(state, Some(session_key), None).await {
        Ok((account, _id)) => Ok(account),
        // Map the shared engine's client-facing error `Response` back into a relay error by status:
        // `503` is `no_eligible()` (no owner AND no selectable fallback); anything else is the
        // generic `internal_error()` (e.g. a snapshot/account read failure).
        Err(resp) => match resp.status() {
            axum::http::StatusCode::SERVICE_UNAVAILABLE => Err(RelayError::NoEligibleAccount),
            _ => Err(RelayError::Internal),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use polyflare_codex::oauth::OAuthClient;
    use polyflare_codex::CodexExecutor;
    use polyflare_core::{AccountId, Continuity, KeyStrength, RoundRobin, Selector, SessionKey};
    use polyflare_store::{Account as StoreAccount, PlainTokens, Store, TokenCipher};

    use crate::continuity::CodexContinuity;

    fn now() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    /// A Hard-strength session key with a caller-chosen value (the WS path derives this value from
    /// the handshake headers; here we set it directly so a matching continuity row can be seeded).
    fn session_key(value: &str) -> SessionKey {
        SessionKey {
            value: value.to_string(),
            strength: KeyStrength::Hard,
        }
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

    /// Seed a continuity session row (under `key`) that pins `owner` — the state Task 6 would have
    /// written on a prior turn's `response.completed`.
    async fn pin_owner(store: &Store, key: &SessionKey, owner: &str) {
        let now = now();
        store
            .continuity()
            .ensure_session(&key.value, "hard", now)
            .await
            .unwrap();
        store
            .continuity()
            .record_completion(&key.value, "hard", owner, "resp_prior", "fp", 1, now)
            .await
            .unwrap();
    }

    /// Full `AppState` for these unit tests, mirroring `crate::control`'s test construction exactly
    /// (`Store` is not `Clone`, so the store/cipher are reached back out via `state`).
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
            ws_downstream: true,
            log_bus: crate::log_bus::LogBus::new(1000),
            max_account_attempts: 3,
            failover_metrics: crate::observability::FailoverMetrics::new(),
            health_tier_metrics: crate::observability::HealthTierMetrics::new(),
            starvation_wait_budget: Duration::from_secs(60),
            starvation_heartbeat: Duration::from_secs(10),
            wake_jitter_ms: 0,
            starvation_metrics: crate::observability::StarvationMetrics::new(),
            stream_idle_timeout: Duration::from_secs(300),
            soft_drain_enabled: true,
            request_log_retention_days: 0,
            usage_history_retention_days: 0,
            runtime: Default::default(),
            inflight_penalty_pct: 2.5,
            lease_metrics: crate::observability::LeaseMetrics::new(),
            upstream_request_metrics: crate::observability::UpstreamRequestMetrics::new(),
            rate_limit_metrics: crate::observability::RateLimitMetrics::new(),
            model_catalog: crate::model_catalog::floor_only_model_catalog(),
        })
    }

    /// A conversation whose continuity row already pins an ELIGIBLE owner reuses that owner —
    /// sticky. Owner is deliberately "B" (the NON-default pick: `RoundRobin`, unpinned, ties to the
    /// lexicographically smaller id "A" — see `resolve_owner_selects_when_unpinned`), so this cannot
    /// pass by coincidence of the selector's own tiebreak.
    #[tokio::test]
    async fn resolve_owner_reuses_the_pinned_owner() {
        let state = build_state(Arc::new(RoundRobin)).await;
        seed_account(&state.store, &state.cipher, "A", "tokA").await;
        seed_account(&state.store, &state.cipher, "B", "tokB").await;
        let key = session_key("conv-owned");
        pin_owner(&state.store, &key, "B").await;

        let account = resolve_owner(&state, &key)
            .await
            .expect("pinned owner reused");
        assert_eq!(
            account.id, "B",
            "must reuse the conversation's pinned owner (B), not RoundRobin's default (A)"
        );
    }

    /// A conversation with NO pin on record selects a healthy Codex account (any eligible one) — it
    /// must not error. `RoundRobin` over two fresh accounts ties to "A".
    #[tokio::test]
    async fn resolve_owner_selects_when_unpinned() {
        let state = build_state(Arc::new(RoundRobin)).await;
        seed_account(&state.store, &state.cipher, "A", "tokA").await;
        seed_account(&state.store, &state.cipher, "B", "tokB").await;
        let key = session_key("conv-fresh-unseen");

        let account = resolve_owner(&state, &key)
            .await
            .expect("an unpinned conversation must select a healthy account, not error");
        assert_eq!(
            account.id, "A",
            "unpinned selection ran (RoundRobin's any-eligible tiebreak)"
        );
    }

    /// A pin whose owner is currently INELIGIBLE (benched via a real rate-limit cooldown — the SAME
    /// runtime API the failure path uses) re-selects a DIFFERENT eligible account, never the
    /// benched owner. This is the wedge-avoidance inviolable: a connection is never stranded because
    /// its pinned owner happens to be unavailable right now.
    #[tokio::test]
    async fn resolve_owner_reselects_when_pinned_owner_ineligible() {
        let state = build_state(Arc::new(RoundRobin)).await;
        seed_account(&state.store, &state.cipher, "A", "tokA").await;
        seed_account(&state.store, &state.cipher, "B", "tokB").await;
        let key = session_key("conv-owner-benched");
        pin_owner(&state.store, &key, "A").await;
        // Bench the pinned owner "A" (a real cooldown; `RuntimeStates::overlay` applies it at
        // selection time and `select.rs`'s eligibility gate rejects it).
        state.runtime.record_rate_limit(
            &AccountId::from("A"),
            Some(3600),
            now(),
            &state.rate_limit_metrics,
        );

        let account = resolve_owner(&state, &key)
            .await
            .expect("an ineligible pin must re-select, never strand the connection");
        assert_ne!(account.id, "A", "must NOT return the benched pinned owner");
        assert_eq!(account.id, "B", "re-selects the other eligible account");
    }
}
