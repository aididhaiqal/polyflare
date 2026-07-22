//! S3(a) regression fixture from the 2026-07-22 codex-lb `continuity_owner_conflict` field
//! incident (`docs/incidents/2026-07-22-codex-lb-continuity-conflict.md`): a conversation
//! anchored on account A whose session-affinity row drifted to account B must ROUTE TO A with no
//! error (affinity is never ownership), the stale row must be corrected, and a repeated identical
//! follow-up must succeed — never the codex-lb signature of the same terminal error twice in a
//! row. Also locks directive 3's deletability guarantee: affinity state can be wiped
//! mid-conversation without breaking the conversation.
//!
//! PolyFlare has no in-memory bridge pin — owner resolution is store-backed on every turn — so
//! the incident's "idle eviction destroyed the pin" precondition is inherently true here; these
//! tests exercise exactly the persistent-source re-resolution the incident went through.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{AccountId, AccountSnapshot, Continuity, SelectionCtx, Selector};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::runtime_settings::{RuntimeSettings, RuntimeSettingsFields};
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockUpstream;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Prefers "B" when present (the unpinned choice), else the first candidate — so any break in the
/// ownership pre-filter routes to B and trips the `tokA` assertions. Skips non-`active` accounts
/// (the one eligibility gate these tests need: a `paused` pinned owner must yield `pick == None`
/// so `apply_ownership` returns `Recover`, as every real selector's eligibility filter does).
struct PreferB;
impl Selector for PreferB {
    fn pick(&self, candidates: &[AccountSnapshot], _ctx: &SelectionCtx) -> Option<AccountId> {
        let eligible: Vec<&AccountSnapshot> =
            candidates.iter().filter(|s| s.status == "active").collect();
        if let Some(b) = eligible.iter().find(|s| s.id.as_str() == "B") {
            return Some(b.id.clone());
        }
        eligible.first().map(|s| s.id.clone())
    }

    fn name(&self) -> &'static str {
        "prefer_b"
    }
}

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

async fn drain(resp: reqwest::Response) -> (reqwest::StatusCode, String) {
    let status = resp.status();
    let mut body = String::new();
    let mut s = resp.bytes_stream();
    while let Some(chunk) = s.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    (status, body)
}

/// Build the app over an existing store + mock upstream and serve it, returning the base URL and
/// the shared state (for store access mid-test).
async fn serve(store: Store, upstream: String, watchdog: Duration) -> (String, Arc<AppState>) {
    let continuity: Arc<dyn Continuity> =
        Arc::new(CodexContinuity::new(store.continuity(), watchdog));
    let state = Arc::new(AppState {
        enforce_client_keys: false,
        codex_executor: Arc::new(CodexExecutor::new().unwrap()),
        control_client: polyflare_codex::build_client().expect("build control_client"),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
        selector: Arc::new(PreferB),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher: TokenCipher::from_key_bytes(&[7u8; 32]).unwrap(),
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: upstream,
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: std::sync::Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: std::sync::Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        admin_token: None,
        runtime_settings: Arc::new(RuntimeSettings::new_from_fields(RuntimeSettingsFields {
            max_account_attempts: 3,
            starvation_wait_budget: std::time::Duration::from_secs(60),
            starvation_heartbeat: std::time::Duration::from_secs(10),
            wake_jitter_ms: 0,
            stream_idle_timeout: std::time::Duration::from_secs(300),
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
        runtime: Default::default(),
    });
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), state)
}

/// Open a store seeded with account A ("tokA") only; B is inserted later per test.
async fn store_with_a() -> Store {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);
    let cipher = TokenCipher::from_key_bytes(&[7u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &account("A"),
            &PlainTokens {
                access_token: "tokA".into(),
                refresh_token: "r".into(),
                id_token: "i".into(),
            },
            &cipher,
        )
        .await
        .unwrap();
    store
}

async fn insert_b(state: &AppState) {
    state
        .store
        .accounts()
        .insert(
            &account("B"),
            &PlainTokens {
                access_token: "tokB".into(),
                refresh_token: "r".into(),
                id_token: "i".into(),
            },
            &state.cipher,
        )
        .await
        .unwrap();
}

/// The incident's stale-drift injection: point the (single) session-affinity row at B, the way
/// codex-lb's TTL-less sticky rows drifted via capacity-routed first turns.
async fn drift_affinity_to_b(state: &AppState) {
    let n = sqlx::query("UPDATE continuity_sessions SET owning_account_id = 'B'")
        .execute(state.store.pool())
        .await
        .unwrap()
        .rows_affected();
    assert_eq!(n, 1, "exactly the one fixture session row drifted");
}

async fn session_owner(state: &AppState) -> Option<String> {
    let row: Option<(Option<String>,)> =
        sqlx::query_as("SELECT owning_account_id FROM continuity_sessions LIMIT 1")
            .fetch_optional(state.store.pool())
            .await
            .unwrap();
    row.and_then(|(o,)| o)
}

/// Turn 1 (anchorless, shared `session_id` header) — lands on A (the only account), records
/// `resp_1 → A` in the anchor map and owner A on the session row.
async fn anchor_turn_one(client: &reqwest::Client, pf: &str) {
    let r1 = client
        .post(format!("{pf}/responses"))
        .header("session_id", "fixture-session")
        .json(&serde_json::json!({"model":"m","input":"hi"}))
        .send()
        .await
        .unwrap();
    let (status, body) = drain(r1).await;
    assert_eq!(status, 200);
    assert!(body.contains("resp_1"), "turn 1 anchored as resp_1");
}

/// The incident follow-up: the same logical turn, carrying the A-owned anchor + full history
/// (multi-item input ⇒ `is_full_resend`, exactly the Codex CLI's store:false shape).
fn follow_up(client: &reqwest::Client, pf: &str) -> reqwest::RequestBuilder {
    client
        .post(format!("{pf}/responses"))
        .header("session_id", "fixture-session")
        .json(&serde_json::json!({
            "model": "m",
            "previous_response_id": "resp_1",
            "input": [
                {"role": "user", "content": "turn one"},
                {"role": "assistant", "content": "reply one"},
                {"role": "user", "content": "turn two"}
            ]
        }))
}

/// Directives 1 + 5 — the incident trace verbatim: anchored on A, affinity drifted to B, the
/// follow-up carrying the A-owned anchor MUST route to A (no error), the stale row MUST be
/// corrected to A, and the identical follow-up repeated MUST succeed again. codex-lb returned the
/// same terminal 503 for this state on every retry.
#[tokio::test]
async fn stale_affinity_never_overrides_anchor_owner_and_is_corrected() {
    let mock = MockUpstream::with_ids(vec![
        r#"{"type":"response.output_text.delta","delta":"x"}"#.to_string()
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let (pf, state) = serve(store_with_a().await, upstream, Duration::from_secs(30)).await;

    let client = reqwest::Client::new();
    anchor_turn_one(&client, &pf).await;
    insert_b(&state).await;
    drift_affinity_to_b(&state).await;
    assert_eq!(
        session_owner(&state).await.as_deref(),
        Some("B"),
        "precondition: the affinity row claims B"
    );

    // Follow-up 1: anchor map (A) beats stale affinity (B) AND the PreferB unpinned choice.
    let (status, body) = drain(follow_up(&client, &pf).send().await.unwrap()).await;
    assert_eq!(
        status, 200,
        "the disagreement is not a client-visible error"
    );
    assert!(body.contains("response.completed"));
    assert_eq!(
        handle.last_authorization().as_deref(),
        Some("Bearer tokA"),
        "routed to the anchor owner, not the affinity hint"
    );

    // The stale affinity entry is corrected by the completed turn (observe runs at stream end —
    // poll briefly rather than racing it).
    let mut corrected = false;
    for _ in 0..20 {
        if session_owner(&state).await.as_deref() == Some("A") {
            corrected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(corrected, "stale affinity entry overwritten back to A");

    // The client's blind retry of the SAME logical turn: must succeed again (the codex-lb
    // signature was the same error twice in a row — assert the strictly stronger property).
    let (status2, body2) = drain(follow_up(&client, &pf).send().await.unwrap()).await;
    assert_eq!(
        status2, 200,
        "identical follow-up converges, never error-loops"
    );
    assert!(body2.contains("response.completed"));
    assert_eq!(handle.last_authorization().as_deref(), Some("Bearer tokA"));
}

/// Directive 2 — a REAL disagreement the router must resolve: the anchor owner A is hard-blocked
/// (paused) while the stale affinity still claims B. Expected: Recover (strip anchor → reselect →
/// full resend on B → re-home), and the client's verbatim retry with the ORIGINAL anchor also
/// succeeds — the disagreement never becomes a terminal error loop.
#[tokio::test]
async fn blocked_owner_with_stale_affinity_recovers_and_converges() {
    let mock = MockUpstream::with_ids(vec![
        r#"{"type":"response.output_text.delta","delta":"x"}"#.to_string()
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let (pf, state) = serve(store_with_a().await, upstream, Duration::from_secs(30)).await;

    let client = reqwest::Client::new();
    anchor_turn_one(&client, &pf).await;
    insert_b(&state).await;
    drift_affinity_to_b(&state).await;
    // Hard-block the anchor owner (paused ⇒ HardBlocked under every strategy).
    state
        .store
        .accounts()
        .update_status("A", "paused")
        .await
        .unwrap();

    // Follow-up 1: pinned owner ineligible ⇒ Recover — anchorless full resend lands on B.
    let before = handle.request_count();
    let (status, body) = drain(follow_up(&client, &pf).send().await.unwrap()).await;
    assert_eq!(
        status, 200,
        "blocked-owner disagreement recovers, never 5xx-loops"
    );
    assert!(body.contains("response.completed"));
    assert_eq!(
        handle.last_authorization().as_deref(),
        Some("Bearer tokB"),
        "recovery reselected the eligible account"
    );
    let bodies = handle.bodies();
    let recovery_body = &bodies[before..][0];
    assert!(
        recovery_body.get("previous_response_id").is_none(),
        "recovery stripped the A-owned anchor before resending"
    );

    // The blind retry carries the ORIGINAL resp_1 anchor (still mapped to the blocked A):
    // the router must converge again — recover again, succeed again. NOT the same error twice.
    let (status2, body2) = drain(follow_up(&client, &pf).send().await.unwrap()).await;
    assert_eq!(status2, 200, "verbatim client retry converges");
    assert!(body2.contains("response.completed"));

    // record_recovery re-homed the session to B.
    let mut rehomed = false;
    for _ in 0..20 {
        if session_owner(&state).await.as_deref() == Some("B") {
            rehomed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(rehomed, "recovery re-homed session ownership to B");
}

/// Directive 3 (surgical wipe) — the codex-lb remedy analog: clearing every affinity hint
/// (`owning_account_id = NULL`) mid-conversation must not move the conversation — ownership lives
/// in the anchor map and survives.
#[tokio::test]
async fn ownership_survives_affinity_data_wipe() {
    let mock = MockUpstream::with_ids(vec![
        r#"{"type":"response.output_text.delta","delta":"x"}"#.to_string()
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let (pf, state) = serve(store_with_a().await, upstream, Duration::from_secs(30)).await;

    let client = reqwest::Client::new();
    anchor_turn_one(&client, &pf).await;
    insert_b(&state).await;
    sqlx::query("UPDATE continuity_sessions SET owning_account_id = NULL")
        .execute(state.store.pool())
        .await
        .unwrap();

    let (status, body) = drain(follow_up(&client, &pf).send().await.unwrap()).await;
    assert_eq!(status, 200);
    assert!(body.contains("response.completed"));
    assert_eq!(
        handle.last_authorization().as_deref(),
        Some("Bearer tokA"),
        "anchor-map ownership survives the affinity wipe"
    );
}

/// Directive 3 (hard wipe) — `DELETE FROM continuity_sessions` mid-conversation (which CASCADEs
/// the anchor map away too, per migration 0002). The conversation must STILL survive: the
/// follow-up degrades to an unowned pick; if the upstream can't resume the anchor there, the
/// armed-watchdog `ResendFull` recovery completes the turn. Never a wedge, never an error loop —
/// affinity state is deletable at ANY moment.
#[tokio::test]
async fn conversation_survives_full_continuity_wipe() {
    // silent_on_anchor: any anchored request stalls (the account doesn't hold the anchor);
    // anchorless requests serve normally — forcing the recovery path wherever the pick lands.
    let mock = MockUpstream::silent_on_anchor(vec![
        r#"{"type":"response.output_text.delta","delta":"x"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    // Short watchdog (as in wedge_regression) so the silent attempt recovers fast.
    let (pf, state) = serve(store_with_a().await, upstream, Duration::from_millis(150)).await;

    let client = reqwest::Client::new();
    anchor_turn_one(&client, &pf).await;
    insert_b(&state).await;
    let deleted = sqlx::query("DELETE FROM continuity_sessions")
        .execute(state.store.pool())
        .await
        .unwrap()
        .rows_affected();
    assert_eq!(
        deleted, 1,
        "the whole affinity store wiped mid-conversation"
    );

    let (status, body) = tokio::time::timeout(Duration::from_secs(5), async {
        drain(follow_up(&client, &pf).send().await.unwrap()).await
    })
    .await
    .expect("follow-up completes within bound (no wedge after a full wipe)");
    assert_eq!(status, 200);
    assert!(body.contains("response.completed"));
    let last = handle
        .last_body()
        .expect("upstream saw the recovery resend");
    assert!(
        last.get("previous_response_id").is_none(),
        "recovery stripped the unresumable anchor"
    );
}
