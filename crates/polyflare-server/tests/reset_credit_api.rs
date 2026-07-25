mod support;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use polyflare_store::{Account, PlainTokens, ResetCredit};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[derive(Clone, Debug)]
struct ConsumeCall {
    account_id: String,
    body: serde_json::Value,
}

#[derive(Clone)]
struct UpstreamCapture {
    calls: Arc<Mutex<Vec<ConsumeCall>>>,
    consume_delay_ms: Arc<AtomicU64>,
    usage_percent_bits: Arc<AtomicU64>,
    credit_available: Arc<AtomicBool>,
}

impl Default for UpstreamCapture {
    fn default() -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            consume_delay_ms: Arc::new(AtomicU64::new(0)),
            usage_percent_bits: Arc::new(AtomicU64::new(90.0_f64.to_bits())),
            credit_available: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl UpstreamCapture {
    fn calls(&self) -> Vec<ConsumeCall> {
        self.calls.lock().unwrap().clone()
    }

    fn set_consume_delay(&self, milliseconds: u64) {
        self.consume_delay_ms.store(milliseconds, Ordering::Relaxed);
    }

    fn set_usage_percent(&self, value: f64) {
        self.usage_percent_bits
            .store(value.to_bits(), Ordering::Relaxed);
    }

    fn set_credit_available(&self, available: bool) {
        self.credit_available.store(available, Ordering::Relaxed);
    }
}

async fn reset_upstream(
    State(capture): State<UpstreamCapture>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let account_id = headers
        .get("chatgpt-account-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    match uri.path() {
        "/backend-api/wham/usage" => axum::Json(serde_json::json!({
            "rate_limit": {
                "primary_window": {
                    "used_percent": f64::from_bits(
                        capture.usage_percent_bits.load(Ordering::Relaxed)
                    ),
                    "reset_at": now() + 5 * 24 * 3_600,
                    "limit_window_seconds": 604_800
                },
                "secondary_window": null
            }
        }))
        .into_response(),
        "/backend-api/wham/rate-limit-reset-credits" => {
            let available = capture.credit_available.load(Ordering::Relaxed);
            axum::Json(serde_json::json!({
                "available_count": if available { 1 } else { 0 },
                "credits": if available {
                    vec![serde_json::json!({
                        "id": format!("credit-{account_id}"),
                        "reset_type": "rate_limit_reset",
                        "status": "available",
                        "granted_at": "2026-07-01T00:00:00Z",
                        "expires_at": "2026-08-01T00:00:00Z"
                    })]
                } else {
                    Vec::new()
                }
            }))
            .into_response()
        }
        "/backend-api/wham/rate-limit-reset-credits/consume" => {
            let body = serde_json::from_slice(&body).unwrap();
            capture.calls.lock().unwrap().push(ConsumeCall {
                account_id: account_id.clone(),
                body,
            });
            let delay = capture.consume_delay_ms.load(Ordering::Relaxed);
            if delay > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            }
            if account_id == "workspace-failing" {
                (
                    StatusCode::BAD_GATEWAY,
                    axum::Json(serde_json::json!({
                        "error": {"code": "provider_failure"}
                    })),
                )
                    .into_response()
            } else if account_id == "workspace-empty" {
                (
                    StatusCode::CONFLICT,
                    axum::Json(serde_json::json!({
                        "error": {"code": "no_credit"}
                    })),
                )
                    .into_response()
            } else {
                axum::Json(serde_json::json!({
                    "code": "reset",
                    "windows_reset": 2,
                    "credit": {
                        "id": format!("credit-{account_id}"),
                        "status": "redeemed",
                        "redeemed_at": "2026-07-25T02:00:00Z"
                    }
                }))
                .into_response()
            }
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn spawn_upstream() -> (String, UpstreamCapture) {
    let capture = UpstreamCapture::default();
    let app = Router::new()
        .route("/backend-api/{*path}", any(reset_upstream))
        .with_state(capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}/backend-api/codex"), capture)
}

fn reset_account(id: &str, workspace: &str) -> Account {
    let mut account = support::account(id);
    account.chatgpt_account_id = Some(workspace.to_string());
    account.email = format!("{id}@example.test");
    account
}

async fn seed_reset_account(
    state: &polyflare_server::app::AppState,
    account: Account,
    insert: bool,
) {
    if insert {
        state
            .store
            .accounts()
            .insert(
                &account,
                &PlainTokens {
                    access_token: "access".to_string(),
                    refresh_token: "refresh".to_string(),
                    id_token: "id".to_string(),
                },
                &state.cipher,
            )
            .await
            .unwrap();
    } else {
        sqlx::query("UPDATE accounts SET chatgpt_account_id = ?, email = ? WHERE id = ?")
            .bind(account.chatgpt_account_id.as_deref())
            .bind(&account.email)
            .bind(&account.id)
            .execute(state.store.pool())
            .await
            .unwrap();
    }
    state
        .store
        .accounts()
        .insert_usage_window(
            &account.id,
            "secondary",
            90.0,
            Some(now() + 5 * 24 * 3_600),
            Some(10_080),
            now(),
        )
        .await
        .unwrap();
    state
        .store
        .reset_credits()
        .replace_snapshot(
            &account.id,
            1,
            now(),
            &[ResetCredit {
                account_id: account.id.clone(),
                credit_id: format!("credit-{}", account.chatgpt_account_id.as_deref().unwrap()),
                reset_type: Some("rate_limit_reset".to_string()),
                status: Some("available".to_string()),
                granted_at: Some(now() - 60),
                expires_at: Some(now() + 7 * 24 * 3_600),
                title: None,
                description: None,
                redeem_started_at: None,
                redeemed_at: None,
            }],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn dashboard_plan_is_admin_gated_and_redeem_payloads_are_validated() {
    let (upstream, _capture) = spawn_upstream().await;
    let (base, state) = support::spawn(upstream).await;
    seed_reset_account(&state, reset_account("acct-1", "workspace-1"), false).await;
    let client = reqwest::Client::new();

    let unauthenticated = client
        .get(format!("{base}/api/reset-credits/plan"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let plan = client
        .get(format!("{base}/api/reset-credits/plan"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(plan.status(), StatusCode::OK);
    let plan: serde_json::Value = plan.json().await.unwrap();
    assert_eq!(plan["total_credits"], 1);
    assert_eq!(plan["candidates"][0]["account_id"], "acct-1");

    let blank_account_request = client
        .post(format!("{base}/api/accounts/acct-1/reset-credit"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({"redeem_request_id": "  "}))
        .send()
        .await
        .unwrap();
    assert_eq!(blank_account_request.status(), StatusCode::BAD_REQUEST);

    let duplicate_fleet = client
        .post(format!("{base}/api/reset-credits/redeem"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "redeem_request_id": "fleet-1",
            "account_ids": ["acct-1", "acct-1"]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(duplicate_fleet.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn native_consume_preserves_safe_upstream_outcomes_and_replays_them() {
    let (upstream, capture) = spawn_upstream().await;
    let (base, state) = support::spawn(upstream).await;
    seed_reset_account(&state, reset_account("acct-1", "workspace-empty"), false).await;
    let client = reqwest::Client::new();
    let body = serde_json::json!({"redeem_request_id": "native-stable-request"});

    for _ in 0..2 {
        let response = client
            .post(format!("{base}/api/codex/rate-limit-reset-credits/consume"))
            .header("authorization", "Bearer local-codex")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap(),
            serde_json::json!({"code": "no_credit", "windows_reset": 0})
        );
    }

    assert_eq!(
        capture.calls().len(),
        1,
        "the safe terminal outcome must be stored in the idempotency ledger"
    );
}

#[tokio::test]
async fn native_local_no_credit_remains_terminal_if_a_credit_appears_later() {
    let (upstream, capture) = spawn_upstream().await;
    capture.set_credit_available(false);
    let (base, state) = support::spawn(upstream).await;
    seed_reset_account(&state, reset_account("acct-1", "workspace-1"), false).await;
    let client = reqwest::Client::new();
    let body = serde_json::json!({"redeem_request_id": "native-local-empty"});

    let first = client
        .post(format!("{base}/api/codex/rate-limit-reset-credits/consume"))
        .header("authorization", "Bearer local-codex")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(
        first.json::<serde_json::Value>().await.unwrap(),
        serde_json::json!({"code": "no_credit", "windows_reset": 0})
    );

    capture.set_credit_available(true);
    let retry = client
        .post(format!("{base}/api/codex/rate-limit-reset-credits/consume"))
        .header("authorization", "Bearer local-codex")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(retry.status(), StatusCode::OK);
    assert_eq!(
        retry.json::<serde_json::Value>().await.unwrap(),
        serde_json::json!({"code": "no_credit", "windows_reset": 0})
    );
    assert!(
        capture.calls().is_empty(),
        "a later credit must not be spent by replaying an already-terminal native request"
    );
}

#[tokio::test]
async fn explicit_native_local_no_credit_replays_with_the_same_opaque_credit() {
    let (upstream, capture) = spawn_upstream().await;
    let (base, state) = support::spawn(upstream).await;
    seed_reset_account(&state, reset_account("acct-1", "workspace-1"), false).await;
    let client = reqwest::Client::new();
    let listed = client
        .get(format!("{base}/api/codex/rate-limit-reset-credits"))
        .header("authorization", "Bearer local-codex")
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    let opaque_credit = listed["credits"][0]["id"].as_str().unwrap();
    capture.set_credit_available(false);
    let body = serde_json::json!({
        "redeem_request_id": "native-explicit-local-empty",
        "credit_id": opaque_credit
    });

    for _ in 0..2 {
        let response = client
            .post(format!("{base}/api/codex/rate-limit-reset-credits/consume"))
            .header("authorization", "Bearer local-codex")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap(),
            serde_json::json!({"code": "no_credit", "windows_reset": 0})
        );
    }
    assert!(capture.calls().is_empty());
}

#[tokio::test]
async fn fleet_redeem_is_sequential_returns_partials_and_replays_completed_accounts() {
    let (upstream, capture) = spawn_upstream().await;
    let (base, state) = support::spawn(upstream).await;
    seed_reset_account(&state, reset_account("acct-1", "workspace-1"), false).await;
    seed_reset_account(&state, reset_account("acct-2", "workspace-failing"), true).await;
    let client = reqwest::Client::new();
    let request = serde_json::json!({
        "redeem_request_id": "fleet-stable-request",
        "account_ids": ["acct-1", "acct-2"]
    });

    let first = client
        .post(format!("{base}/api/reset-credits/redeem"))
        .header("authorization", "Bearer secret")
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first: serde_json::Value = first.json().await.unwrap();
    assert_eq!(first["results"].as_array().unwrap().len(), 1);
    assert_eq!(first["results"][0]["account_id"], "acct-1");
    assert_eq!(first["errors"].as_array().unwrap().len(), 1);
    assert_eq!(first["errors"][0]["account_id"], "acct-2");

    let first_calls = capture.calls();
    assert_eq!(
        first_calls
            .iter()
            .map(|call| call.account_id.as_str())
            .collect::<Vec<_>>(),
        ["workspace-1", "workspace-failing"]
    );

    let replay = client
        .post(format!("{base}/api/reset-credits/redeem"))
        .header("authorization", "Bearer secret")
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    let replay: serde_json::Value = replay.json().await.unwrap();
    assert_eq!(replay["results"][0]["account_id"], "acct-1");
    assert_eq!(replay["errors"][0]["account_id"], "acct-2");

    let all_calls = capture.calls();
    assert_eq!(
        all_calls
            .iter()
            .map(|call| call.account_id.as_str())
            .collect::<Vec<_>>(),
        ["workspace-1", "workspace-failing", "workspace-failing"],
        "the completed first account must replay from the ledger instead of spending again"
    );
    assert_eq!(
        all_calls[1].body["redeem_request_id"], all_calls[2].body["redeem_request_id"],
        "a retry of the failed account must preserve its deterministic upstream idempotency key"
    );
}

#[tokio::test]
async fn concurrent_duplicate_redeems_make_one_upstream_consume_call() {
    let (upstream, capture) = spawn_upstream().await;
    capture.set_consume_delay(250);
    let (base, state) = support::spawn(upstream).await;
    seed_reset_account(&state, reset_account("acct-1", "workspace-1"), false).await;
    let client = reqwest::Client::new();
    let url = format!("{base}/api/accounts/acct-1/reset-credit");
    let body = serde_json::json!({"redeem_request_id": "concurrent-stable-request"});

    let first = client
        .post(&url)
        .header("authorization", "Bearer secret")
        .json(&body)
        .send();
    let second = client
        .post(&url)
        .header("authorization", "Bearer secret")
        .json(&body)
        .send();
    let (first, second) = tokio::join!(first, second);

    assert_eq!(first.unwrap().status(), StatusCode::OK);
    assert_eq!(second.unwrap().status(), StatusCode::OK);
    assert_eq!(
        capture.calls().len(),
        1,
        "the waiter must replay the completed ledger after acquiring the lease"
    );
}

#[tokio::test]
async fn completed_redemption_replays_after_its_account_is_deleted() {
    let (upstream, capture) = spawn_upstream().await;
    let (base, state) = support::spawn(upstream).await;
    seed_reset_account(&state, reset_account("acct-1", "workspace-1"), false).await;
    let client = reqwest::Client::new();
    let url = format!("{base}/api/accounts/acct-1/reset-credit");
    let body = serde_json::json!({"redeem_request_id": "delete-safe-terminal"});

    let first = client
        .post(&url)
        .header("authorization", "Bearer secret")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first_body = first.json::<serde_json::Value>().await.unwrap();

    assert!(state
        .store
        .accounts()
        .delete("acct-1", false)
        .await
        .unwrap());

    let replay = client
        .post(&url)
        .header("authorization", "Bearer secret")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(
        replay.json::<serde_json::Value>().await.unwrap(),
        first_body
    );
    assert_eq!(
        capture.calls().len(),
        1,
        "account deletion must not erase a terminal redemption outcome"
    );
}

#[tokio::test]
async fn in_flight_account_deletion_cannot_erase_the_terminal_result() {
    let (upstream, capture) = spawn_upstream().await;
    capture.set_consume_delay(500);
    let (base, state) = support::spawn(upstream).await;
    seed_reset_account(&state, reset_account("acct-1", "workspace-1"), false).await;
    let client = reqwest::Client::new();
    let url = format!("{base}/api/accounts/acct-1/reset-credit");
    let body = serde_json::json!({"redeem_request_id": "delete-during-consume"});

    let request_client = client.clone();
    let request_url = url.clone();
    let request_body = body.clone();
    let in_flight = tokio::spawn(async move {
        request_client
            .post(request_url)
            .header("authorization", "Bearer secret")
            .json(&request_body)
            .send()
            .await
            .unwrap()
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while capture.calls().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the upstream consume should start");
    assert!(state
        .store
        .accounts()
        .delete("acct-1", false)
        .await
        .unwrap());

    let first = in_flight.await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first_body = first.json::<serde_json::Value>().await.unwrap();
    let replay = client
        .post(&url)
        .header("authorization", "Bearer secret")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(
        replay.json::<serde_json::Value>().await.unwrap(),
        first_body
    );
    assert_eq!(capture.calls().len(), 1);
}

#[tokio::test]
async fn native_terminal_result_replays_after_its_account_is_deleted() {
    let (upstream, capture) = spawn_upstream().await;
    let (base, state) = support::spawn(upstream).await;
    seed_reset_account(&state, reset_account("acct-1", "workspace-1"), false).await;
    let client = reqwest::Client::new();
    let url = format!("{base}/api/codex/rate-limit-reset-credits/consume");
    let body = serde_json::json!({"redeem_request_id": "native-delete-safe"});

    let first = client
        .post(&url)
        .header("authorization", "Bearer local-codex")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first_body = first.json::<serde_json::Value>().await.unwrap();
    assert!(state
        .store
        .accounts()
        .delete("acct-1", false)
        .await
        .unwrap());

    let replay = client
        .post(&url)
        .header("authorization", "Bearer local-codex")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(
        replay.json::<serde_json::Value>().await.unwrap(),
        first_body
    );
    assert_eq!(capture.calls().len(), 1);
}

#[tokio::test]
async fn named_pool_terminal_replay_survives_account_deletion_but_rejects_another_scope() {
    let (upstream, capture) = spawn_upstream().await;
    let (base, state) = support::spawn(upstream).await;
    seed_reset_account(&state, reset_account("acct-1", "workspace-1"), false).await;
    sqlx::query("INSERT INTO account_pool_memberships (account_id, pool) VALUES (?, ?)")
        .bind("acct-1")
        .bind("alpha")
        .execute(state.store.pool())
        .await
        .unwrap();
    let client = reqwest::Client::new();
    let body = serde_json::json!({"redeem_request_id": "native-pool-delete-safe"});
    let pool_url = format!("{base}/alpha/backend-api/wham/rate-limit-reset-credits/consume");

    let first = client
        .post(&pool_url)
        .header("authorization", "Bearer local-codex")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first_body = first.json::<serde_json::Value>().await.unwrap();
    assert!(state
        .store
        .accounts()
        .delete("acct-1", false)
        .await
        .unwrap());

    let replay = client
        .post(&pool_url)
        .header("authorization", "Bearer local-codex")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(
        replay.json::<serde_json::Value>().await.unwrap(),
        first_body
    );

    let cross_scope = client
        .post(format!("{base}/api/codex/rate-limit-reset-credits/consume"))
        .header("authorization", "Bearer local-codex")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(cross_scope.status(), StatusCode::CONFLICT);
    assert_eq!(capture.calls().len(), 1);
}

#[tokio::test]
async fn recommended_count_includes_expiry_actions() {
    let (upstream, _capture) = spawn_upstream().await;
    let (base, state) = support::spawn(upstream).await;
    seed_reset_account(&state, reset_account("acct-1", "workspace-1"), false).await;
    seed_reset_account(&state, reset_account("acct-2", "workspace-2"), true).await;
    sqlx::query("UPDATE reset_credits SET expires_at = ? WHERE account_id = 'acct-2'")
        .bind(now() + 24 * 3_600)
        .execute(state.store.pool())
        .await
        .unwrap();

    let plan = reqwest::Client::new()
        .get(format!("{base}/api/reset-credits/plan"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(plan.status(), StatusCode::OK);
    let plan = plan.json::<serde_json::Value>().await.unwrap();
    assert_eq!(plan["recommended_now"], 2);
    assert_eq!(
        plan["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|candidate| matches!(
                candidate["recommendation"].as_str(),
                Some("redeem_now" | "redeem_before_expiry")
            ))
            .count(),
        2
    );
}

#[tokio::test]
async fn native_retry_stays_on_its_original_account_after_ranking_changes() {
    let (upstream, capture) = spawn_upstream().await;
    let (base, state) = support::spawn(upstream).await;
    seed_reset_account(&state, reset_account("acct-1", "workspace-failing"), false).await;
    seed_reset_account(&state, reset_account("acct-2", "workspace-2"), true).await;
    let client = reqwest::Client::new();
    let body = serde_json::json!({"redeem_request_id": "native-rerank-request"});

    let first = client
        .post(format!("{base}/api/codex/rate-limit-reset-credits/consume"))
        .header("authorization", "Bearer local-codex")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::SERVICE_UNAVAILABLE);

    state
        .store
        .accounts()
        .insert_usage_window(
            "acct-1",
            "secondary",
            10.0,
            Some(now() + 5 * 24 * 3_600),
            Some(10_080),
            now(),
        )
        .await
        .unwrap();
    state
        .store
        .accounts()
        .insert_usage_window(
            "acct-2",
            "secondary",
            99.0,
            Some(now() + 5 * 24 * 3_600),
            Some(10_080),
            now(),
        )
        .await
        .unwrap();

    let retry = client
        .post(format!("{base}/api/codex/rate-limit-reset-credits/consume"))
        .header("authorization", "Bearer local-codex")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(retry.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        capture
            .calls()
            .iter()
            .map(|call| call.account_id.as_str())
            .collect::<Vec<_>>(),
        ["workspace-failing", "workspace-failing"],
        "the durable native pin must prevent a retry from switching to the newly higher-ranked account"
    );
}

#[tokio::test]
async fn legacy_native_pin_with_unknown_scope_uses_pool_membership_for_exact_retry() {
    let (upstream, capture) = spawn_upstream().await;
    let (base, state) = support::spawn(upstream).await;
    seed_reset_account(&state, reset_account("acct-1", "workspace-failing"), false).await;
    sqlx::query("INSERT INTO account_pool_memberships (account_id, pool) VALUES (?, ?)")
        .bind("acct-1")
        .bind("alpha")
        .execute(state.store.pool())
        .await
        .unwrap();
    state
        .store
        .reset_credits()
        .pin_native_request(
            "legacy-native-retry",
            "acct-1",
            None,
            "alpha",
            now(),
            86_400,
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE reset_credit_native_requests SET pool_scope = NULL \
         WHERE redeem_request_id = 'legacy-native-retry'",
    )
    .execute(state.store.pool())
    .await
    .unwrap();

    let response = reqwest::Client::new()
        .post(format!(
            "{base}/alpha/backend-api/wham/rate-limit-reset-credits/consume"
        ))
        .header("authorization", "Bearer local-codex")
        .json(&serde_json::json!({"redeem_request_id": "legacy-native-retry"}))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(capture.calls().len(), 1);
    assert_eq!(capture.calls()[0].account_id, "workspace-failing");
}

#[tokio::test]
async fn native_list_omits_stale_or_paused_account_credits() {
    let (upstream, _capture) = spawn_upstream().await;
    let (base, state) = support::spawn(upstream).await;
    seed_reset_account(&state, reset_account("acct-1", "workspace-1"), false).await;
    seed_reset_account(&state, reset_account("acct-2", "workspace-2"), true).await;
    seed_reset_account(&state, reset_account("acct-3", "workspace-3"), true).await;
    sqlx::query("UPDATE accounts SET status = 'paused' WHERE id = 'acct-2'")
        .execute(state.store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE reset_credit_snapshots SET fetched_at = ? WHERE account_id = 'acct-3'")
        .bind(now() - 1_000)
        .execute(state.store.pool())
        .await
        .unwrap();
    let client = reqwest::Client::new();

    let native = client
        .get(format!("{base}/api/codex/rate-limit-reset-credits"))
        .header("authorization", "Bearer local-codex")
        .send()
        .await
        .unwrap();
    assert_eq!(native.status(), StatusCode::OK);
    let native: serde_json::Value = native.json().await.unwrap();
    assert_eq!(native["available_count"], 1);
    assert_eq!(native["credits"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn pooled_native_routes_never_expose_or_pin_another_pools_credit() {
    let (upstream, _capture) = spawn_upstream().await;
    let (base, state) = support::spawn(upstream).await;
    seed_reset_account(&state, reset_account("acct-1", "workspace-1"), false).await;
    seed_reset_account(&state, reset_account("acct-2", "workspace-2"), true).await;
    for (account_id, pool) in [("acct-1", "alpha"), ("acct-2", "beta")] {
        sqlx::query("UPDATE accounts SET pool = ? WHERE id = ?")
            .bind(pool)
            .bind(account_id)
            .execute(state.store.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO account_pool_memberships (account_id, pool) VALUES (?, ?)")
            .bind(account_id)
            .bind(pool)
            .execute(state.store.pool())
            .await
            .unwrap();
    }
    let client = reqwest::Client::new();
    let beta = client
        .get(format!(
            "{base}/beta/backend-api/wham/rate-limit-reset-credits"
        ))
        .header("authorization", "Bearer local-codex")
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(beta["available_count"], 1);
    let beta_credit = beta["credits"][0]["id"].as_str().unwrap();

    let forbidden = client
        .post(format!(
            "{base}/alpha/backend-api/wham/rate-limit-reset-credits/consume"
        ))
        .header("authorization", "Bearer local-codex")
        .json(&serde_json::json!({
            "redeem_request_id": "cross-pool-credit",
            "credit_id": beta_credit
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);
    assert!(state
        .store
        .reset_credits()
        .get_native_request("cross-pool-credit", now(), 86_400)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn recommended_only_redeem_rechecks_live_usage_and_refuses_a_stale_plan() {
    let (upstream, capture) = spawn_upstream().await;
    let (base, state) = support::spawn(upstream).await;
    seed_reset_account(&state, reset_account("acct-1", "workspace-1"), false).await;
    sqlx::query(
        "UPDATE usage_history SET recorded_at = ? WHERE account_id = 'acct-1' AND \"window\" = 'secondary'",
    )
    .bind(now() - 10)
    .execute(state.store.pool())
    .await
    .unwrap();
    capture.set_usage_percent(1.0);
    let client = reqwest::Client::new();

    let response = client
        .post(format!("{base}/api/accounts/acct-1/reset-credit"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "redeem_request_id": "recommended-stale-request",
            "require_recommended": true
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"]["code"], "reset_credit_recommendation_changed");
    assert!(
        capture.calls().is_empty(),
        "a recommendation that changed after authoritative refresh must not spend a credit"
    );
}

#[tokio::test]
async fn fleet_request_id_rejects_a_changed_account_selection() {
    let (upstream, capture) = spawn_upstream().await;
    let (base, state) = support::spawn(upstream).await;
    seed_reset_account(&state, reset_account("acct-1", "workspace-1"), false).await;
    seed_reset_account(&state, reset_account("acct-2", "workspace-2"), true).await;
    let client = reqwest::Client::new();

    let first = client
        .post(format!("{base}/api/reset-credits/redeem"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "redeem_request_id": "fleet-selection-pin",
            "account_ids": ["acct-1"]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let changed = client
        .post(format!("{base}/api/reset-credits/redeem"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "redeem_request_id": "fleet-selection-pin",
            "account_ids": ["acct-1", "acct-2"]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(changed.status(), StatusCode::CONFLICT);
    assert_eq!(
        capture
            .calls()
            .iter()
            .map(|call| call.account_id.as_str())
            .collect::<Vec<_>>(),
        ["workspace-1"],
        "changing a parent fleet request must not spend a newly appended account"
    );
}
