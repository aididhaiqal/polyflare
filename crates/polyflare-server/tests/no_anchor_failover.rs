//! No-anchor pin fail-over (M3-core review fix): a no-anchor request whose recorded owner (from
//! the session row, not an anchor) has since become ineligible must fail OVER to another eligible
//! account — never a 500. `RecoveryPlan::None` only ever arises for anchor-less requests, so there
//! is nothing to resume; the pin is best-effort, unlike an anchor-bearing pin (whose recovery is
//! `ResendFull`/`SignalClient` and stays hard/authoritative).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{AccountId, AccountSnapshot, Continuity, SelectionCtx, Selector};
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

/// Mirrors the real `CapacityWeighted` terminal-status gate (reauth_required is never eligible),
/// then deterministically picks the first eligible candidate in input order.
struct ExcludeReauth;
impl Selector for ExcludeReauth {
    fn pick(&self, candidates: &[AccountSnapshot], _ctx: &SelectionCtx) -> Option<AccountId> {
        candidates
            .iter()
            .find(|s| s.status != "reauth_required")
            .map(|s| s.id.clone())
    }

    fn name(&self) -> &'static str {
        "exclude_reauth"
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

async fn drain(resp: reqwest::Response) -> String {
    let mut body = String::new();
    let mut s = resp.bytes_stream();
    while let Some(chunk) = s.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    body
}

#[tokio::test]
async fn no_anchor_request_on_ineligible_owner_fails_over_not_500() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[11u8; 32]).unwrap();
    // Turn 1: only account A exists, so the no-anchor pick lands on A and its completion
    // records A as the session's owning account (via the session row, not an anchor).
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
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));

    let mock = MockUpstream::with_ids(vec![
        r#"{"type":"response.output_text.delta","delta":"x"}"#.to_string()
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;

    let state = Arc::new(AppState {
        enforce_client_keys: false,
        codex_executor: Arc::new(CodexExecutor::new().unwrap()),
        control_client: polyflare_codex::build_client().expect("build control_client"),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
        selector: Arc::new(ExcludeReauth),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher: TokenCipher::from_key_bytes(&[11u8; 32]).unwrap(),
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: upstream,
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: std::sync::Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: std::sync::Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        admin_token: None,
        live_logs: false,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        max_account_attempts: 3,
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        starvation_wait_budget: std::time::Duration::from_secs(60),
        starvation_heartbeat: std::time::Duration::from_secs(10),
        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        stream_idle_timeout: std::time::Duration::from_secs(300),
        soft_drain_enabled: true,
        runtime: Default::default(),
    });
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let pf = format!("http://{addr}");

    let client = reqwest::Client::new();
    // Turn 1: no anchor, stable session_id header. Lands on A; observe() records A as owner on
    // the session row (NOT via the anchor map, since there's no previous_response_id at all).
    let r1 = client
        .post(format!("{pf}/responses"))
        .header("session_id", "sess-failover")
        .json(&serde_json::json!({"model":"m","input":"turn one"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);
    let _ = drain(r1).await;

    // Insert account B (eligible), then make the recorded owner A ineligible.
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
    state
        .store
        .accounts()
        .update_status("A", "reauth_required")
        .await
        .unwrap();

    // Turn 2: SAME session_id, still no anchor. `prepare` resolves pin_account = Some(A) from
    // the session row (no anchor to look up), so watchdog stays Disarmed and recovery is
    // `RecoveryPlan::None`. `apply_ownership` narrows to {A}, which the selector rejects (A is
    // reauth_required) ⇒ `RouteDecision::Recover`. Before the fix this hit `RecoveryPlan::None
    // => internal_error()` (500). After the fix it must re-select over the FULL pool and relay
    // on B instead.
    let r2 = client
        .post(format!("{pf}/responses"))
        .header("session_id", "sess-failover")
        .json(&serde_json::json!({"model":"m","input":"turn two"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r2.status(),
        200,
        "no-anchor request on an ineligible owner must fail over, not 500"
    );
    let body2 = drain(r2).await;
    assert!(
        body2.contains("response.completed"),
        "relayed normally on the fallback account: {body2}"
    );
    assert_eq!(
        handle.last_authorization().as_deref(),
        Some("Bearer tokB"),
        "turn 2 failed over to B, not the ineligible pinned owner A"
    );
    std::mem::forget(dir);
}
