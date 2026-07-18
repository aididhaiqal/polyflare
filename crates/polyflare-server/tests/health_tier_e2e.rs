//! B8 Task 4 — the INTEGRATION crux: prove the health-tier soft-drain WRITE side
//! (`RuntimeStates` funnel/poller → `overlay` → `AccountSnapshot`) actually reaches the READ side
//! (`select.rs`'s already-built `health_tier_pool`), so a live-tracked DRAINING account is really
//! de-preferred in production selection — through the real `AppState.runtime` / `AppState.selector`
//! path, not a unit re-test of `select.rs`.
//!
//! This drives selection the exact way `crate::ingress` does: overlay the live runtime state onto a
//! (cloned) snapshot slice, then call the production default `Selector::pick`. The two DIFFER only
//! in the health tier the runtime wrote, so any pick difference is attributable to the soft-drain
//! machinery and nothing else.
//!
//! It also bakes in the plan's Global-Constraint checks: the disable lever (`soft_drain_enabled`
//! false ⇒ the poller resets the tier to 0 end-to-end ⇒ selection not skewed), the pigeonhole
//! guarantee (a fully-drained pool still serves someone), and the ownership no-op (health-tier
//! bucketing on a 1-candidate slice never changes the pick), plus a content-free assertion on the
//! emitted `HealthTierSignal` through the real `AppState` log bus + metrics.

use std::sync::Arc;
use std::time::Duration;

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{AccountId, AccountSnapshot, CapacityWeighted, Continuity, SelectionCtx};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::observability::emit_health_tier_signal;
use polyflare_store::{Store, TokenCipher};

/// A real `AppState` (no HTTP upstream needed — this exercises the in-memory selection path only),
/// parameterized on the `POLYFLARE_SOFT_DRAIN_ENABLED` disable lever. Building the real `AppState`
/// and calling `build_app` proves the new `health_tier_metrics` field is wired at every
/// construction site and the router still assembles.
async fn state(soft_drain_enabled: bool) -> Arc<AppState> {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[9u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    std::mem::forget(dir);
    let state = Arc::new(AppState {
        enforce_client_keys: false,
        codex_executor: Arc::new(CodexExecutor::new().unwrap()),
        control_client: polyflare_codex::build_client().expect("build control_client"),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9".to_string()).unwrap(),
        upstream_base_url: "http://127.0.0.1:9".to_string(),
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        runtime: Default::default(),
        admin_token: None,
        live_logs: true,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        max_account_attempts: 3,
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        starvation_wait_budget: Duration::from_secs(60),
        starvation_heartbeat: Duration::from_secs(10),
        wake_jitter_ms: 0,
        inflight_penalty_pct: 2.5,
        lease_metrics: polyflare_server::observability::LeaseMetrics::new(),

        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        stream_idle_timeout: Duration::from_secs(300),
        soft_drain_enabled,
        request_log_retention_days: 0,
        usage_history_retention_days: 0,
    });
    // Prove the router still builds with the new field present (no route churn).
    let _app = build_app(state.clone());
    state
}

/// A neutral, eligible `plus`-tier snapshot with zero usage — two of these are indistinguishable to
/// the capacity-weighted selector, so the ONLY thing that can separate them in a pick is the health
/// tier the runtime overlays.
fn eligible_snap(id: &str) -> AccountSnapshot {
    let mut s = AccountSnapshot::new(id);
    s.plan_type = "plus".to_string();
    s.status = "active".to_string();
    s
}

fn ctx(now: i64, seed: u64) -> SelectionCtx {
    SelectionCtx {
        now,
        require_security_work_authorized: false,
        rng_seed: Some(seed),
        session_id: None,
        tier: None,
        inflight_penalty_pct: 0.0,
    }
}

/// THE CORE PROPERTY: with soft-drain ON, an account the runtime funnel drove to DRAINING is
/// de-preferred by the REAL selector in favor of a HEALTHY peer of otherwise-identical standing.
/// This is the `select.rs` unit test's property, but proven through the live
/// runtime→overlay→snapshot→`Selector::pick` chain.
#[tokio::test]
async fn draining_account_is_depreferred_in_live_selection() {
    let state = state(true).await;
    let now = 1_000_000;
    let a = AccountId::from("acct-a"); // acct-b is only ever referenced by its snapshot id string.

    // Drive `acct-a` to DRAINING via the ERROR funnel (the fast path): two transient errors within
    // the 60s window ⇒ error-flapping ⇒ HEALTHY→DRAINING. `acct-b` is never touched ⇒ stays HEALTHY.
    assert_eq!(
        state.runtime.record_transient_error(&a, now),
        None,
        "the FIRST error alone does not reach the drain threshold"
    );
    let t = state
        .runtime
        .record_transient_error(&a, now)
        .expect("the SECOND error within 60s crosses the error-drain threshold");
    assert_eq!((t.from, t.to, t.reason), (0, 1, "error_drain"));

    // Overlay the live runtime onto a fresh snapshot slice — EXACTLY what ingress does before pick.
    let mut snaps = vec![eligible_snap("acct-a"), eligible_snap("acct-b")];
    state.runtime.overlay(&mut snaps, now);
    assert_eq!(snaps[0].health_tier, 1, "write side: acct-a is DRAINING");
    assert_eq!(snaps[1].health_tier, 0, "write side: acct-b is HEALTHY");
    assert!(
        snaps[0].error_count < 3,
        "acct-a must stay ELIGIBLE (below the backoff gate) — soft-drain is a PREFERENCE, not a \
         hard exclusion; the de-preference must come from the health tier, not benching"
    );

    // Read side: the HEALTHY peer wins every seed (health_tier_pool buckets healthy-first).
    for seed in 0..64u64 {
        let picked = state.selector.pick(&snaps, &ctx(now, seed)).unwrap();
        assert_eq!(
            picked.as_str(),
            "acct-b",
            "seed {seed}: the DRAINING account must be de-preferred vs the HEALTHY one"
        );
    }
}

/// Disable lever (`POLYFLARE_SOFT_DRAIN_ENABLED=0`): the poller resets a drained account's tier to
/// HEALTHY end-to-end, so selection is no longer skewed by the health tier — today's exact pre-B8
/// behavior (clean rollback). NB: the disable lever gates the POLLER (`evaluate_with_usage`), which
/// is the authoritative tier owner; the error funnel is not flag-gated (it can transiently drain),
/// but the very next poller cycle forces the tier back to 0 while disabled — which is what this
/// asserts end-to-end.
#[tokio::test]
async fn disable_lever_resets_tier_and_unskews_selection() {
    let state = state(false).await;
    assert!(!state.soft_drain_enabled);
    let now = 2_000_000;
    let a = AccountId::from("acct-a");

    // A transient-error drain still happens in the funnel (not flag-gated)...
    state.runtime.record_transient_error(&a, now);
    state.runtime.record_transient_error(&a, now);
    let mut mid = vec![eligible_snap("acct-a")];
    state.runtime.overlay(&mut mid, now);
    assert_eq!(
        mid[0].health_tier, 1,
        "funnel drained acct-a (not flag-gated)"
    );

    // ...but a poller cycle with the lever OFF forces the tier back to HEALTHY + emits a
    // `disabled_reset` transition — the codex-lb disable path, end-to-end.
    let reset = state
        .runtime
        .evaluate_with_usage(&a, Some(10.0), None, false, state.soft_drain_enabled, now)
        .expect("the poller reset from DRAINING→HEALTHY is a real transition");
    assert_eq!(
        (reset.from, reset.to, reset.reason),
        (1, 0, "disabled_reset")
    );

    let mut snaps = vec![eligible_snap("acct-a"), eligible_snap("acct-b")];
    state.runtime.overlay(&mut snaps, now);
    assert_eq!(
        snaps[0].health_tier, 0,
        "disable lever: the PERSISTED soft-drain tier is 0 end-to-end (through overlay) — the \
         codex-lb rollback behavior"
    );

    // A subsequent successful completion clears the transient error state, so the recovered account
    // carries NO independent drain signal at all. (`select.rs`'s `effective_tier` folds its own
    // usage/error `should_drain` on top of the persisted tier — a separate, always-on flapping guard
    // that the disable lever deliberately does NOT touch — so the residual `error_count` must clear
    // for the account to compete on truly equal footing.)
    state.runtime.record_success(&a);
    let mut snaps = vec![eligible_snap("acct-a"), eligible_snap("acct-b")];
    state.runtime.overlay(&mut snaps, now);

    // Both accounts are now tier 0 with no drain signal ⇒ health_tier_pool is a single-bucket no-op
    // ⇒ selection reverts to pure capacity weighting over EQUAL accounts ⇒ acct-a is NOT skewed out
    // (it wins a fair share of seeds), exactly today's pre-B8 behavior.
    let a_wins = (0..64u64)
        .filter(|&seed| {
            state
                .selector
                .pick(&snaps, &ctx(now, seed))
                .unwrap()
                .as_str()
                == "acct-a"
        })
        .count();
    assert!(
        a_wins > 0,
        "with the lever off + errors cleared, the formerly-draining account is not skewed out of \
         selection (got {a_wins}/64 wins)"
    );
}

/// Global constraint (pigeonhole): `health_tier_pool` can never empty the pool — a fully-drained
/// pool still serves its least-bad member. Drive BOTH accounts to DRAINING and assert a pick still
/// resolves.
#[tokio::test]
async fn fully_drained_pool_still_selects_someone() {
    let state = state(true).await;
    let now = 3_000_000;
    for id in ["acct-a", "acct-b"] {
        let a = AccountId::from(id);
        state.runtime.record_transient_error(&a, now);
        state.runtime.record_transient_error(&a, now);
    }
    let mut snaps = vec![eligible_snap("acct-a"), eligible_snap("acct-b")];
    state.runtime.overlay(&mut snaps, now);
    assert_eq!(snaps[0].health_tier, 1);
    assert_eq!(snaps[1].health_tier, 1);
    assert!(
        state.selector.pick(&snaps, &ctx(now, 0)).is_some(),
        "a fully-drained pool must still select someone (pigeonhole)"
    );
}

/// Global constraint (ownership still wins): continuity ownership narrows the candidate slice to the
/// single owner BEFORE `pick` (in the ingress, `apply_ownership`), so health-tier bucketing operates
/// on a 1-candidate slice — a strict no-op. Proven here: a lone DRAINING candidate is still picked
/// (health-tier never overrides an owned pick).
#[tokio::test]
async fn ownership_narrowed_draining_candidate_is_still_picked() {
    let state = state(true).await;
    let now = 4_000_000;
    let a = AccountId::from("acct-a");
    state.runtime.record_transient_error(&a, now);
    state.runtime.record_transient_error(&a, now);
    // The post-ownership slice is exactly one account (the owner), even though it is DRAINING.
    let mut owned = vec![eligible_snap("acct-a")];
    state.runtime.overlay(&mut owned, now);
    assert_eq!(owned[0].health_tier, 1, "the owner is DRAINING");
    assert_eq!(
        state.selector.pick(&owned, &ctx(now, 7)).unwrap().as_str(),
        "acct-a",
        "health-tier bucketing is a no-op on a 1-candidate slice — the owned pick still wins"
    );
}

/// The emitted `HealthTierSignal`, routed through the REAL `AppState` log bus + metrics (the exact
/// handles `crate::ingress::record_failure` and `crate::usage_refresh::refresh_account` pass to
/// `emit_health_tier_signal`), is content-free: only the account id, the two tier NUMBERS, and a
/// fixed reason label — never a body/token/usage percentage — and the metrics counter advances once
/// per real transition.
#[tokio::test]
async fn health_tier_signal_is_content_free_through_appstate() {
    let state = state(true).await;
    let now = 5_000_000;
    let a = AccountId::from("acct-a");

    state.runtime.record_transient_error(&a, now);
    let t = state
        .runtime
        .record_transient_error(&a, now)
        .expect("second error is a real transition");

    assert_eq!(state.health_tier_metrics.total(), 0, "no emit yet");
    emit_health_tier_signal(
        &state.log_bus,
        &state.health_tier_metrics,
        a.as_str(),
        t.from,
        t.to,
        t.reason,
    );
    assert_eq!(
        state.health_tier_metrics.total(),
        1,
        "the metrics counter advances exactly once per transition"
    );

    // The published event is in the ring buffer's backfill (published before we subscribe).
    let (backfill, _rx) = state.log_bus.subscribe();
    let ev = backfill
        .iter()
        .find(|e| e.kind == "health_tier")
        .expect("the health-tier event was published to the live log bus");
    assert_eq!(ev.account.as_deref(), Some("acct-a"));
    assert_eq!(ev.model, None);
    assert_eq!(ev.status, None);
    assert!(ev.message.contains("reason=error_drain"));
    assert!(ev.message.contains("from=0"));
    assert!(ev.message.contains("to=1"));
    for forbidden in [
        "bearer", "token", "sess_", "session", "percent", "used", "body", "content", "delta",
    ] {
        assert!(
            !ev.message.to_lowercase().contains(forbidden),
            "forbidden content `{forbidden}` leaked into health-tier log event: {}",
            ev.message
        );
    }
}
