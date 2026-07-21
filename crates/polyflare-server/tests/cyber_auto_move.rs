//! TA6(b) Task 2: the reactive move trigger. Task 1 (`cyber_policy_detection.rs`) proved a streamed
//! `cyber_policy` rejection surfaces as `WatchdogError::CapabilityRejection` on the Armed path,
//! BEFORE any content is relayed. This suite drives the FULL ingress (`responses_handler_impl`'s
//! `RouteDecision::Route` branch) end-to-end: an owner rejects `cyber_policy` ⇒ the ingress must
//! reuse the EXACT `ResendFull`/`execute_recovery`/`record_recovery` machinery
//! `RouteDecision::Recover` already uses to reselect onto a `security_work_authorized` account,
//! relay ITS clean stream to the client, and re-home ownership — or, if no capability-holder
//! exists, refuse cleanly (the security floor) rather than ever retrying unfiltered.

use std::convert::Infallible;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::{Json, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use futures_util::stream;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::runtime_settings::{RuntimeSettings, RuntimeSettingsFields};
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn account(id: &str, security_work_authorized: bool) -> Account {
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
        last_refresh: i64::MAX / 2, // never triggers a refresh
        created_at: 1,
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized,
        provider: "codex".to_string(),
        pool: None,
    }
}

fn tokens(access_token: &str) -> PlainTokens {
    PlainTokens {
        access_token: access_token.to_string(),
        refresh_token: "r".into(),
        id_token: "i".into(),
    }
}

/// What the mock upstream does when the CURRENT OWNER (matched by bearer token) sends an
/// ANCHOR-bearing (`previous_response_id`-carrying) request — the "second turn on the pinned
/// owner" case this suite is about. Any OTHER request (no anchor, or a different token — i.e. the
/// anchor-stripped resend to a reselected account) always gets a normal `response.completed` SSE.
#[derive(Clone, Copy)]
enum OwnerAnchorBehavior {
    /// A `response.failed` `cyber_policy` frame on a 200-OK stream (Task 1's wire truth).
    CyberPolicy,
    /// A bare non-2xx failure with no parseable error code — the ordinary transient-failure
    /// regression case (test c).
    Plain500,
}

#[derive(Clone)]
struct CyberMock {
    owner_token: String,
    behavior: OwnerAnchorBehavior,
    counter: Arc<AtomicU32>,
    /// Every request's `Authorization` header value, in order — lets tests assert exactly which
    /// account each attempt targeted (and how many attempts were made at all).
    tokens_seen: Arc<Mutex<Vec<String>>>,
}

async fn cyber_handler(
    State(mock): State<CyberMock>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    mock.tokens_seen.lock().unwrap().push(auth.clone());

    let has_anchor = body.get("previous_response_id").is_some();
    let is_owner = auth == format!("Bearer {}", mock.owner_token);

    if has_anchor && is_owner {
        return match mock.behavior {
            OwnerAnchorBehavior::CyberPolicy => {
                // Content-safety mirror of `cyber_policy_detection.rs`'s fixture: code only matters,
                // the message is never asserted on (and must never reach the client).
                let frame = r#"{"type":"response.failed","response":{"id":"resp_fatal_cyber","status":"failed","error":{"code":"cyber_policy","message":"classified — must never leak"}}}"#;
                let s = stream::once(async move {
                    Ok::<Bytes, Infallible>(Bytes::from(format!("data: {frame}\n\n")))
                });
                Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "text/event-stream")
                    .body(Body::from_stream(s))
                    .unwrap()
            }
            OwnerAnchorBehavior::Plain500 => {
                (StatusCode::INTERNAL_SERVER_ERROR, "boom").into_response()
            }
        };
    }

    // Normal completion: turn 1 (no anchor, any account), or the anchor-stripped resend to a
    // reselected account (no `previous_response_id` — Task 2's `ResendFull` shape).
    let n = mock.counter.fetch_add(1, Ordering::SeqCst) + 1;
    let id = format!("resp_{n}");
    let created = format!(r#"{{"type":"response.created","response":{{"id":"{id}"}}}}"#);
    let completed = format!(r#"{{"type":"response.completed","response":{{"id":"{id}"}}}}"#);
    let s = stream::iter(vec![
        Ok::<Bytes, Infallible>(Bytes::from(format!("data: {created}\n\n"))),
        Ok(Bytes::from(format!("data: {completed}\n\n"))),
    ]);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .body(Body::from_stream(s))
        .unwrap()
}

async fn spawn_cyber_mock(
    owner_token: String,
    behavior: OwnerAnchorBehavior,
) -> (String, CyberMock) {
    let mock = CyberMock {
        owner_token,
        behavior,
        counter: Arc::new(AtomicU32::new(0)),
        tokens_seen: Arc::new(Mutex::new(Vec::new())),
    };
    let app = Router::new()
        .route("/responses", post(cyber_handler))
        .with_state(mock.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), mock)
}

async fn spawn_app(
    store: Store,
    cipher: TokenCipher,
    upstream_url: String,
) -> (String, Arc<AppState>) {
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
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
        upstream_base_url: upstream_url,
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
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
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), state)
}

/// Pulls the first `"id":"..."` value out of an SSE body — used to carry turn 1's emitted
/// `response.id` into turn 2's `previous_response_id`, exactly as a real client would.
fn extract_id(body: &str) -> Option<String> {
    let idx = body.find("\"id\":\"")?;
    let rest = &body[idx + "\"id\":\"".len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// (a) A cyber-capable account EXISTS: the owner's `cyber_policy` rejection reroutes to it, the
/// client sees a CLEAN stream (never the rejection), and ownership re-homes via `record_recovery`.
#[tokio::test]
async fn owner_rejects_cyber_policy_reroutes_to_capable_account_and_rehomes_ownership() {
    let owner_token = "owner-tok".to_string();
    let capable_token = "capable-tok".to_string();
    let (upstream, mock) =
        spawn_cyber_mock(owner_token.clone(), OwnerAnchorBehavior::CyberPolicy).await;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[7u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &account("owner-acct", false),
            &tokens(&owner_token),
            &cipher,
        )
        .await
        .unwrap();

    let (pf, state) = spawn_app(store, cipher, upstream).await;
    let client = reqwest::Client::new();
    let session_header = "sess-cyber-a";

    // Turn 1: fresh (no anchor), only "owner-acct" exists ⇒ deterministically lands there,
    // establishing ownership + the anchor map (no rng-dependent selection).
    let r1 = client
        .post(format!("{pf}/responses"))
        .header("session_id", session_header)
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": [{"a": 1}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);
    let body1 = r1.text().await.unwrap();
    let anchor_id = extract_id(&body1).expect("turn 1 emitted a response id");

    // Register the cyber-capable account — present for turn 2's RESELECT, but was absent for turn
    // 1's pick (so turn 1's landing on the owner is deterministic, not a race).
    state
        .store
        .accounts()
        .insert(
            &account("capable-acct", true),
            &tokens(&capable_token),
            &state.cipher,
        )
        .await
        .unwrap();

    // Turn 2: anchored to the owner, full-resend shape (>=2 input items ⇒ RecoveryPlan::ResendFull
    // per `CodexContinuity::prepare`). The owner rejects with `cyber_policy` on this attempt.
    let r2 = client
        .post(format!("{pf}/responses"))
        .header("session_id", session_header)
        .json(&serde_json::json!({
            "model": "gpt-5.6-sol",
            "previous_response_id": anchor_id,
            "input": [{"a": 1}, {"b": 2}],
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        r2.status(),
        200,
        "the client gets the capable account's clean stream, not an error"
    );
    let body2 = r2.text().await.unwrap();
    assert!(
        !body2.contains("cyber_policy"),
        "the rejection itself must never reach the client: {body2}"
    );
    assert!(
        body2.contains("response.completed"),
        "a clean completed stream relayed: {body2}"
    );

    // Exactly 3 upstream attempts: turn1 (owner) + turn2's rejected owner attempt + the resend to
    // the capable account. The SECOND attempt targeted the owner (as expected, and rejected); the
    // THIRD (the actual retry) targeted the capability-holder — never an unfiltered account.
    let tokens_seen = mock.tokens_seen.lock().unwrap().clone();
    assert_eq!(
        tokens_seen.len(),
        3,
        "turn1 + rejected-owner attempt + capable resend: {tokens_seen:?}"
    );
    assert_eq!(tokens_seen[1], format!("Bearer {owner_token}"));
    assert_eq!(
        tokens_seen[2],
        format!("Bearer {capable_token}"),
        "the resend targeted the security_work_authorized account, not the rejecting owner"
    );

    // Ownership re-homed via `record_recovery` (the SAME machinery `RouteDecision::Recover` uses).
    let session_key =
        polyflare_server::session_key::sha256_hex(format!("session:{session_header}").as_bytes());
    let row = state
        .store
        .continuity()
        .get_session(&session_key)
        .await
        .unwrap()
        .expect("session row exists");
    assert_eq!(
        row.owning_account_id.as_deref(),
        Some("capable-acct"),
        "ownership re-homed to the capable account"
    );
}

/// (b) SECURITY FLOOR: no capability-holding account exists ⇒ a clear, distinct error to the
/// client (NOT the generic 502), and NO attempt is ever made on a non-authorized account. This is
/// the inviolable invariant: cyber work is never served on an account that isn't authorized for it.
#[tokio::test]
async fn no_capable_account_yields_a_clear_error_and_never_retries_unfiltered() {
    let owner_token = "owner-only-tok".to_string();
    let (upstream, mock) =
        spawn_cyber_mock(owner_token.clone(), OwnerAnchorBehavior::CyberPolicy).await;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[9u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &account("owner-only", false),
            &tokens(&owner_token),
            &cipher,
        )
        .await
        .unwrap();

    let (pf, state) = spawn_app(store, cipher, upstream).await;
    let client = reqwest::Client::new();
    let session_header = "sess-cyber-b";

    let r1 = client
        .post(format!("{pf}/responses"))
        .header("session_id", session_header)
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": [{"a": 1}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);
    let body1 = r1.text().await.unwrap();
    let anchor_id = extract_id(&body1).expect("turn 1 emitted a response id");

    // No capability-holding account is EVER inserted — the owner is the only account that exists.
    let r2 = client
        .post(format!("{pf}/responses"))
        .header("session_id", session_header)
        .json(&serde_json::json!({
            "model": "gpt-5.6-sol",
            "previous_response_id": anchor_id,
            "input": [{"a": 1}, {"b": 2}],
        }))
        .send()
        .await
        .unwrap();

    assert_ne!(
        r2.status(),
        StatusCode::BAD_GATEWAY,
        "the security floor is a DISTINCT error, not the generic 502 an upstream failure gets"
    );
    assert!(
        r2.status().is_client_error() || r2.status() == StatusCode::SERVICE_UNAVAILABLE,
        "expected a clean 4xx/503-style refusal, got {}",
        r2.status()
    );
    let body2 = r2.text().await.unwrap();
    assert!(
        body2.to_lowercase().contains("security") || body2.to_lowercase().contains("authorized"),
        "body should clearly state no authorized account is available: {body2}"
    );

    // THE INVARIANT: exactly 2 upstream calls total (turn1 + the rejected owner attempt on turn2)
    // — no third call anywhere, i.e. no retry on the (only, non-authorized) owner or anyone else.
    let tokens_seen = mock.tokens_seen.lock().unwrap().clone();
    assert_eq!(
        tokens_seen.len(),
        2,
        "no unfiltered retry may ever be attempted: {tokens_seen:?}"
    );

    // The account itself is untouched by this rejection (a capability rejection is not an
    // account-health signal — no cooldown/error-count bump, no durable status change).
    let stored = state
        .store
        .accounts()
        .get("owner-only")
        .await
        .unwrap()
        .expect("account still exists");
    assert_eq!(stored.status, "active");
}

/// (c) Regression: a NON-cyber failure (a plain transient upstream error) on the SAME owner+anchor
/// call site must still route through the pre-existing `record_failure` + 502 path exactly as
/// before — proving the new branch triggers ONLY on `WatchdogError::CapabilityRejection`, never on
/// any other `WatchdogError` variant.
#[tokio::test]
async fn non_cyber_failure_still_routes_to_record_failure_and_502() {
    let owner_token = "owner-plain-tok".to_string();
    let (upstream, mock) =
        spawn_cyber_mock(owner_token.clone(), OwnerAnchorBehavior::Plain500).await;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &account("owner-plain", false),
            &tokens(&owner_token),
            &cipher,
        )
        .await
        .unwrap();

    let (pf, state) = spawn_app(store, cipher, upstream).await;
    let client = reqwest::Client::new();
    let session_header = "sess-cyber-c";

    let r1 = client
        .post(format!("{pf}/responses"))
        .header("session_id", session_header)
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": [{"a": 1}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);
    let body1 = r1.text().await.unwrap();
    let anchor_id = extract_id(&body1).expect("turn 1 emitted a response id");

    let r2 = client
        .post(format!("{pf}/responses"))
        .header("session_id", session_header)
        .json(&serde_json::json!({
            "model": "gpt-5.6-sol",
            "previous_response_id": anchor_id,
            "input": [{"a": 1}, {"b": 2}],
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        r2.status(),
        502,
        "a plain (non-cyber) upstream failure surfaces exactly as before: the generic 502"
    );

    // Exactly 2 attempts: turn1 + the failed owner attempt — the new branch must NOT have
    // triggered any reselect/resend for a non-CapabilityRejection error.
    let tokens_seen = mock.tokens_seen.lock().unwrap().clone();
    assert_eq!(
        tokens_seen.len(),
        2,
        "no cyber reroute for a plain failure: {tokens_seen:?}"
    );

    // `record_failure`'s ordinary transient-error bookkeeping still fires (unchanged behavior).
    let mut snaps = vec![polyflare_core::AccountSnapshot::new("owner-plain")];
    state.runtime.overlay(&mut snaps, now());
    assert_eq!(
        snaps[0].error_count, 1,
        "the pre-existing record_failure transient-error path still bumps error_count"
    );
}
