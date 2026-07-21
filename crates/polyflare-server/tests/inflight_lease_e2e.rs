//! C9 Task 4 — the concurrent-load INTEGRATION crux: prove the in-flight soft-penalty WRITE side
//! (`RuntimeStates::acquire_in_flight` → `overlay` → `AccountSnapshot.in_flight` → `select.rs`'s
//! penalty fold, Tasks 1-3) actually reaches production selection end-to-end, AND that the
//! content-free `LeaseMetrics` counters (`AppState.lease_metrics`, this task) move correctly on
//! both acquire and release — through the real `AppState.runtime` / `AppState.selector` path, not
//! a unit re-test of `select.rs`'s C9 Task 3 tests (which hand-set `AccountSnapshot.in_flight`
//! directly, never touching `RuntimeStates`) or `runtime_state.rs`'s C9 Task 1 unit tests (which
//! never touch `select.rs`). Mirrors `health_tier_e2e.rs`'s "live runtime→overlay→snapshot→
//! `Selector::pick` chain" integration pattern exactly, for the in-flight lease instead of the
//! health tier.
//!
//! This drives selection the exact way `crate::ingress` does: acquire real `InFlightGuard`s on one
//! account (holding several concurrent leases, simulating a burst of concurrent requests landing on
//! the same account before the selector had a chance to spread them), overlay the live runtime
//! state onto a snapshot slice, then call the production default `Selector::pick`. Releasing the
//! guards re-proves C9 Task 1-2's leak-proof guarantee at this integration level too, and proves the
//! soft penalty tracks LIVE state (an account is not permanently branded by past load).

use std::sync::Arc;
use std::time::Duration;

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{AccountId, AccountSnapshot, CapacityWeighted, Continuity, SelectionCtx};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::runtime_settings::{RuntimeSettings, RuntimeSettingsFields};
use polyflare_store::{Store, TokenCipher};

/// A real `AppState` (no HTTP upstream needed — this exercises the in-memory selection path only,
/// exactly like `health_tier_e2e.rs`'s `state()`).
async fn state() -> Arc<AppState> {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[11u8; 32]).unwrap();
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
        runtime_settings: Arc::new(RuntimeSettings::new_from_fields(RuntimeSettingsFields {
            max_account_attempts: 3,
            starvation_wait_budget: Duration::from_secs(60),
            starvation_heartbeat: Duration::from_secs(10),
            wake_jitter_ms: 0,
            stream_idle_timeout: Duration::from_secs(300),
            inflight_penalty_pct: 2.5,
            soft_drain_enabled: true,
            request_log_retention_days: 0,
            usage_history_retention_days: 0,
            live_logs: false,
        })),
        ws_downstream: false,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        lease_metrics: polyflare_server::observability::LeaseMetrics::new(),
        upstream_request_metrics: polyflare_server::observability::UpstreamRequestMetrics::new(),
        rate_limit_metrics: polyflare_server::observability::RateLimitMetrics::new(),
        relay_metrics: polyflare_server::observability::RelayMetrics::new(),
        model_catalog: polyflare_server::model_catalog::floor_only_model_catalog(),
        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
    });
    // Prove the router still builds with the new field present (no route churn).
    let _app = build_app(state.clone());
    state
}

/// Two otherwise-identical eligible accounts at the SAME secondary usage (79%, under the 90%
/// should_drain line — mirrors `select.rs`'s C9 Task 3
/// `less_busy_account_wins_the_weighted_pick_more_often_than_a_high_inflight_peer` shape exactly)
/// so any pick skew is attributable ONLY to the live in-flight penalty, nothing else.
fn snap(id: &str) -> AccountSnapshot {
    let mut s = AccountSnapshot::new(id);
    s.plan_type = "pro".to_string();
    s.secondary_used_percent = 79.0;
    s
}

fn ctx(now: i64, seed: u64) -> SelectionCtx {
    SelectionCtx {
        now,
        require_security_work_authorized: false,
        rng_seed: Some(seed),
        session_id: None,
        tier: None,
        // The production default (`POLYFLARE_INFLIGHT_PENALTY_PCT` unset ⇒ 2.5).
        inflight_penalty_pct: 2.5,
    }
}

/// THE CORE PROPERTY (end-to-end soft penalty): a burst of concurrent requests that landed on
/// "busy" (4 real `InFlightGuard` leases, held — not a hand-set `AccountSnapshot.in_flight`) is
/// de-preferred by the REAL selector in favor of an otherwise-identical "idle" peer. Proven through
/// the live acquire→overlay→snapshot→`Selector::pick` chain — `select.rs`'s C9 Task 3 test proves
/// the identical scoring math in isolation; this proves the WRITE side (`acquire_in_flight`)
/// actually reaches it through production `AppState` wiring. Also asserts the content-free
/// `LeaseMetrics.acquired` counter advanced exactly once per lease.
#[tokio::test]
async fn busy_account_holding_several_leases_is_depreferred_in_live_selection() {
    let state = state().await;
    let now = 1_000_000;
    let busy = AccountId::from("busy");

    assert_eq!(state.lease_metrics.acquired(), 0, "no leases acquired yet");
    assert_eq!(state.lease_metrics.released(), 0, "no leases released yet");

    // Simulate 4 concurrent in-flight requests landing on "busy" — a real acquire through the same
    // `state.runtime.acquire_in_flight(id, now, &state.lease_metrics)` call shape every
    // `crate::ingress` streaming selection site uses, not a hand-set field.
    let guards: Vec<_> = (0..4)
        .map(|_| {
            state
                .runtime
                .acquire_in_flight(&busy, now, &state.lease_metrics)
        })
        .collect();

    assert_eq!(
        state.lease_metrics.acquired(),
        4,
        "the acquire counter bumped exactly once per acquire_in_flight call"
    );
    assert_eq!(
        state.lease_metrics.released(),
        0,
        "nothing released while the guards are still held"
    );

    // Overlay the live runtime onto a fresh snapshot slice — EXACTLY what ingress does before pick.
    let mut snaps = vec![snap("idle"), snap("busy")];
    state.runtime.overlay(&mut snaps, now);
    assert_eq!(snaps[0].in_flight, 0, "write side: idle carries no lease");
    assert_eq!(
        snaps[1].in_flight, 4,
        "write side: busy carries 4 live leases"
    );

    // Read side: the idle peer wins a strong majority of seeds — the same statistical bar
    // `select.rs`'s `less_busy_account_wins_the_weighted_pick_more_often_than_a_high_inflight_peer`
    // test uses for the identical in_flight=4/penalty=2.5 shape, now proven through live wiring
    // (real `RuntimeStates`/`overlay`/`AppState.selector`, not hand-set snapshot fields).
    let mut idle_wins = 0;
    for seed in 0..1000u64 {
        if state
            .selector
            .pick(&snaps, &ctx(now, seed))
            .unwrap()
            .as_str()
            == "idle"
        {
            idle_wins += 1;
        }
    }
    assert!(
        idle_wins > 580,
        "the less-busy account should win more often through live selection, got {idle_wins}/1000"
    );

    // Cleanup: release every held lease so this test doesn't leak into later assertions.
    drop(guards);
}

/// Releasing the leases (a) restores `in_flight` to 0 (re-proving C9 Task 1-2's leak-proof
/// guarantee at THIS integration level, not just `watchdog.rs`'s stream-lifetime unit tests), (b)
/// bumps the content-free `released` counter exactly once per guard, and (c) selection reverts to
/// an even split once the accounts are balanced again — the soft penalty tracks LIVE state, it does
/// not permanently brand an account for past load.
#[tokio::test]
async fn releasing_the_leases_restores_balance_and_bumps_released_metrics_leak_proof() {
    let state = state().await;
    let now = 2_000_000;
    let busy = AccountId::from("busy");

    let guards: Vec<_> = (0..4)
        .map(|_| {
            state
                .runtime
                .acquire_in_flight(&busy, now, &state.lease_metrics)
        })
        .collect();
    assert_eq!(state.lease_metrics.acquired(), 4);

    let mut mid = vec![snap("idle"), snap("busy")];
    state.runtime.overlay(&mut mid, now);
    assert_eq!(mid[1].in_flight, 4, "leases held before release");
    assert_eq!(
        state.lease_metrics.current(),
        4,
        "derived in-flight total matches the held guards"
    );

    // The disconnect/completion/failover-reselect analog at this level: drop every guard.
    drop(guards);

    assert_eq!(
        state.lease_metrics.released(),
        4,
        "one release bump per dropped guard — leak-proof, not just at watchdog.rs's stream level"
    );
    assert_eq!(
        state.lease_metrics.current(),
        0,
        "acquired == released ⇒ 0 derived in-flight"
    );

    let mut after = vec![snap("idle"), snap("busy")];
    state.runtime.overlay(&mut after, now);
    assert_eq!(
        after[1].in_flight, 0,
        "in_flight returns to 0 once every lease is released — no leak"
    );

    // Balance restored: with no live in_flight signal on either side, the pick distribution is
    // statistically EVEN again — the account is not permanently branded by its past load.
    let mut busy_wins = 0;
    for seed in 0..1000u64 {
        if state
            .selector
            .pick(&after, &ctx(now, seed))
            .unwrap()
            .as_str()
            == "busy"
        {
            busy_wins += 1;
        }
    }
    assert!(
        (400..600).contains(&busy_wins),
        "with leases released, selection is unskewed again, got busy={busy_wins}/1000"
    );
}

/// Global constraint (pigeonhole, re-asserted at this integration level): even when EVERY account
/// in the pool is carrying live leases, the soft penalty can never empty the eligible pool — a
/// fully-busy pool still selects its least-busy member. `select.rs`'s C9 Task 3
/// `fully_busy_pool_still_selects_someone` proves the same property with hand-set snapshots; this
/// proves it through real `acquire_in_flight` writes.
#[tokio::test]
async fn fully_busy_pool_still_selects_someone_through_live_selection() {
    let state = state().await;
    let now = 3_000_000;
    let a = AccountId::from("a");
    let b = AccountId::from("b");

    // Both accounts carry a large number of live leases — enough to clamp eff_secondary_used to
    // 100 for both (all weights zero ⇒ deterministic tiebreak), never an empty pool.
    let mut guards = Vec::new();
    for _ in 0..20 {
        guards.push(
            state
                .runtime
                .acquire_in_flight(&a, now, &state.lease_metrics),
        );
        guards.push(
            state
                .runtime
                .acquire_in_flight(&b, now, &state.lease_metrics),
        );
    }

    let mut snaps = vec![snap("a"), snap("b")];
    state.runtime.overlay(&mut snaps, now);
    assert_eq!(snaps[0].in_flight, 20);
    assert_eq!(snaps[1].in_flight, 20);

    assert!(
        state.selector.pick(&snaps, &ctx(now, 1)).is_some(),
        "a fully-busy pool must still select someone, never empty (soft penalty, not a hard cap)"
    );
}
