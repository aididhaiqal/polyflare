//! Dashboard read API: `/api/accounts` surfaces per-account usage windows + reset times (the
//! "see the reset time" goal), `/api/pools` aggregates accounts by pool, `/api/requests` pages the
//! request log. Asserts shape + that NO secret (token) is present in any response body.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{AccountId, CapacityWeighted, Continuity, Executor};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::runtime_settings::{RuntimeSettings, RuntimeSettingsFields};
use polyflare_store::{Account, PlainTokens, RequestLogRecord, Store, TokenCipher};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn account(id: &str, email: &str, pool: Option<&str>) -> Account {
    Account {
        id: id.to_string(),
        chatgpt_account_id: None,
        chatgpt_user_id: None,
        email: email.to_string(),
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
        reset_at: Some(1_783_900_000),
        blocked_at: None,
        security_work_authorized: false,
        provider: "codex".to_string(),
        pool: pool.map(str::to_string),
    }
}

fn tokens() -> PlainTokens {
    PlainTokens {
        access_token: "SECRET-ACCESS-TOKEN".to_string(),
        refresh_token: "SECRET-REFRESH".to_string(),
        id_token: "SECRET-ID".to_string(),
    }
}

async fn seed_store() -> Store {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    let repo = store.accounts();
    repo.insert(
        &account("codex-a", "a@example.test", Some("team-a")),
        &tokens(),
        &cipher,
    )
    .await
    .unwrap();
    repo.insert(
        &account("codex-b", "b@example.test", None),
        &tokens(),
        &cipher,
    )
    .await
    .unwrap();
    // Only codex-a gets a weekly usage window (5h/primary absent, as upstream currently behaves).
    repo.insert_usage_window(
        "codex-a",
        "secondary",
        73.5,
        Some(1_783_900_000),
        Some(10080),
        now(),
    )
    .await
    .unwrap();
    // One request-log row so /api/requests has something to page.
    store
        .request_log()
        .insert(&RequestLogRecord {
            requested_at: now(),
            provider: "codex".to_string(),
            method: "POST".to_string(),
            path: "/responses".to_string(),
            aliased: false,
            status: 200,
            duration_ms: 12,
            account_id: None,
            target_kind: None,
            provider_credential_id: None,
            model: None,
            upstream_model: None,
            upstream_transport: None,
            reasoning_effort: None,
            service_tier: None,
            transport: None,
            ttft_ms: None,
            total_tokens: None,
            cached_tokens: None,
            subagent: None,
            request_id: None,
            session_key: None,
            input_tokens: None,
            output_tokens: None,
            cached_input_tokens: None,
            reasoning_tokens: None,
            orchestration_input_tokens: None,
            orchestration_output_tokens: None,
            orchestration_cached_input_tokens: None,
            cost_usd: None,
            latency_first_token_ms: None,
            protocol_outcome: None,
        })
        .await
        .unwrap();
    // One owned + one unowned continuity session so /api/sessions returns real rows.
    // Without this the content-safety SECRET-loop below would hit an empty /api/sessions
    // and pass vacuously; the owned row (owning_account_id = the real "codex-a") makes it
    // surface a joined owner_email, giving the leak assertion real teeth for this endpoint.
    let continuity = store.continuity();
    continuity
        .record_completion(
            "sk-seed-owned",
            "hard",
            "codex-a",
            "resp-seed",
            "fp-seed",
            1,
            now(),
        )
        .await
        .unwrap();
    continuity
        .ensure_session("sk-seed-unowned", "soft", now())
        .await
        .unwrap();
    std::mem::forget(dir);
    store
}

async fn spawn(store: Store) -> String {
    spawn_with_state(store).await.0
}

/// Same as `spawn`, but also hands back the `AppState` so a test can reach into live-only state
/// (e.g. `state.runtime.record_rate_limit`) that isn't reachable through the HTTP surface.
async fn spawn_with_state(store: Store) -> (String, Arc<AppState>) {
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    let state = Arc::new(AppState {
        enforce_client_keys: false,
        codex_executor: Arc::new(CodexExecutor::new().unwrap()) as Arc<dyn Executor>,
        control_client: polyflare_codex::build_client().expect("build control_client"),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap())
            as Arc<dyn Executor>,
        selector: Arc::new(CapacityWeighted),
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
        account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        admin_token: Some("secret".to_string()),
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
            live_logs: true,
        })),
        ws_downstream: false,
        ws_relay_idle: polyflare_server::ws_relay::WsRelayIdlePolicy::default(),
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

#[tokio::test]
async fn accounts_endpoint_surfaces_usage_windows_and_reset_times() {
    let pf = spawn(seed_store().await).await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{pf}/api/accounts"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    // Accounts are listed by id, so codex-a is first.
    let a = &arr[0];
    assert_eq!(a["id"], "codex-a");
    assert_eq!(a["pool"], "team-a");
    assert_eq!(a["reset_at"], 1_783_900_000_i64);
    // The seeded window is a 10080-min (weekly) window, freshly recorded → it resolves to `weekly`
    // (by duration, not slot) and is not stale. No 5h-duration window exists → `five_hour` null.
    assert_eq!(a["weekly"]["used_percent"], 73.5);
    assert_eq!(a["weekly"]["reset_at"], 1_783_900_000_i64);
    assert_eq!(
        a["weekly"]["stale"], false,
        "freshly recorded → live, not stale"
    );
    assert!(
        a["five_hour"].is_null(),
        "no 5h-duration window → null, not blocked"
    );

    // codex-b is unpooled and has no usage window yet.
    let b = &arr[1];
    assert_eq!(b["id"], "codex-b");
    assert!(b["pool"].is_null());
    assert!(b["weekly"].is_null());
    assert!(b["five_hour"].is_null());
}

#[tokio::test]
async fn accounts_endpoint_carries_provider_pool_usage_token_health_and_request_count() {
    // Task 7: /api/accounts must additionally surface provider/pool (already present, re-asserted
    // here for the new shape), an adaptive per-window `usage` array (`{window, used_percent,
    // reset_at}`), a `token_health` object derived from the stored access token's JWT `exp` (NEVER
    // the token itself), and a rolling-24h `request_count_24h`.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    let repo = store.accounts();
    // Task 4b: /api/accounts (list) must ALSO carry `alias` + `security_work_authorized` — the
    // per-account detail view already carries both, the list didn't. codex-c gets a non-null alias
    // + security_work_authorized=true; codex-alias-null (below) keeps the account()-builder default
    // (alias: None, security_work_authorized: false) to prove the null/false case round-trips too.
    let mut acct_c = account("codex-c", "c@example.test", Some("team-c"));
    acct_c.alias = Some("Casey".to_string());
    acct_c.security_work_authorized = true;
    repo.insert(&acct_c, &tokens(), &cipher).await.unwrap();
    repo.insert(
        &account("codex-alias-null", "null@example.test", None),
        &tokens(),
        &cipher,
    )
    .await
    .unwrap();
    repo.insert_usage_window(
        "codex-c",
        "secondary",
        12.5,
        Some(1_900_000_000),
        Some(10080),
        now(),
    )
    .await
    .unwrap();
    store
        .request_log()
        .insert(&RequestLogRecord {
            requested_at: now(),
            provider: "codex".to_string(),
            method: "POST".to_string(),
            path: "/responses".to_string(),
            aliased: false,
            status: 200,
            duration_ms: 10,
            account_id: Some("codex-c".to_string()),
            target_kind: Some("account".to_string()),
            provider_credential_id: None,
            model: None,
            upstream_model: None,
            upstream_transport: None,
            reasoning_effort: None,
            service_tier: None,
            transport: None,
            ttft_ms: None,
            total_tokens: None,
            cached_tokens: None,
            subagent: None,
            request_id: None,
            session_key: None,
            input_tokens: None,
            output_tokens: None,
            cached_input_tokens: None,
            reasoning_tokens: None,
            orchestration_input_tokens: None,
            orchestration_output_tokens: None,
            orchestration_cached_input_tokens: None,
            cost_usd: None,
            latency_first_token_ms: None,
            protocol_outcome: None,
        })
        .await
        .unwrap();
    std::mem::forget(dir);

    let pf = spawn(store).await;
    let body: serde_json::Value = reqwest::Client::new()
        .get(format!("{pf}/api/accounts"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let arr = body.as_array().unwrap();
    let a = arr
        .iter()
        .find(|a| a["id"] == "codex-c")
        .expect("codex-c present");

    assert_eq!(a["provider"], "codex");
    assert_eq!(a["pool"], "team-c");

    let usage = a["usage"].as_array().expect("usage is an array");
    assert!(
        !usage.is_empty(),
        "usage must carry at least the seeded weekly window"
    );
    assert!(
        usage
            .iter()
            .any(|w| w["window"] == "weekly" && w["used_percent"] == 12.5),
        "usage: {usage:?}"
    );

    let token_health = &a["token_health"];
    assert!(token_health.is_object(), "token_health: {token_health:?}");
    assert!(
        token_health["access_state"].is_string(),
        "token_health.access_state must be a string: {token_health:?}"
    );
    // "SECRET-ACCESS-TOKEN" isn't a JWT → exp can't be decoded → access_state is "missing", and
    // access_expires_at is null. Also the whole point of this test: the raw token never appears.
    assert_eq!(token_health["access_state"], "missing");
    assert!(token_health["access_expires_at"].is_null());

    assert_eq!(a["request_count_24h"], 1);

    // Task 4b: alias + security_work_authorized on the LIST row.
    assert_eq!(a["alias"], "Casey");
    assert_eq!(a["security_work_authorized"], true);

    let null_alias = arr
        .iter()
        .find(|a| a["id"] == "codex-alias-null")
        .expect("codex-alias-null present");
    assert!(
        null_alias["alias"].is_null(),
        "no alias seeded -> null, not omitted: {null_alias:?}"
    );
    assert_eq!(null_alias["security_work_authorized"], false);
}

#[tokio::test]
async fn promo_shape_resolves_weekly_from_primary_slot_and_flags_stale() {
    // The real-world shape the live API surfaced: during the no-5h-limit promo, upstream writes the
    // weekly window into the PRIMARY slot (fresh) and leaves an OLD weekly in the secondary slot.
    // The API must resolve `weekly` from the fresh primary (by duration, not slot), mark nothing
    // live-but-stale, and report NO 5h window. A second account whose only weekly is old must be
    // surfaced but flagged stale.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    let repo = store.accounts();
    repo.insert(
        &account("promo-1", "p@example.test", None),
        &tokens(),
        &cipher,
    )
    .await
    .unwrap();
    repo.insert(
        &account("stale-2", "s@example.test", None),
        &tokens(),
        &cipher,
    )
    .await
    .unwrap();
    let fresh = now();
    let old = now() - 300_000; // ~3.5 days ago → stale
                               // promo-1: fresh weekly in the primary slot, older weekly left in the secondary slot.
    repo.insert_usage_window(
        "promo-1",
        "primary",
        44.0,
        Some(1_900_000_000),
        Some(10080),
        fresh,
    )
    .await
    .unwrap();
    repo.insert_usage_window(
        "promo-1",
        "secondary",
        55.0,
        Some(1_800_000_000),
        Some(10080),
        old,
    )
    .await
    .unwrap();
    // stale-2: only an old weekly, never refreshed live.
    repo.insert_usage_window(
        "stale-2",
        "secondary",
        30.0,
        Some(1_800_000_000),
        Some(10080),
        old,
    )
    .await
    .unwrap();
    std::mem::forget(dir);

    let pf = spawn(store).await;
    let body: serde_json::Value = reqwest::Client::new()
        .get(format!("{pf}/api/accounts"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let by_id = |id: &str| -> serde_json::Value {
        body.as_array()
            .unwrap()
            .iter()
            .find(|a| a["id"] == id)
            .cloned()
            .unwrap()
    };

    let promo = by_id("promo-1");
    assert!(promo["five_hour"].is_null(), "no 5h-duration window → null");
    assert_eq!(
        promo["weekly"]["used_percent"], 44.0,
        "fresh primary-slot weekly wins the stale one"
    );
    assert_eq!(promo["weekly"]["stale"], false);

    let stale = by_id("stale-2");
    assert_eq!(
        stale["weekly"]["used_percent"], 30.0,
        "last-known value still surfaced"
    );
    assert_eq!(
        stale["weekly"]["stale"], true,
        "but flagged stale — must not read as live"
    );
}

#[tokio::test]
async fn no_secret_token_is_ever_present_in_a_read_response() {
    let pf = spawn(seed_store().await).await;
    let client = reqwest::Client::new();
    for path in [
        "/api/accounts",
        "/api/pools",
        "/api/requests",
        "/api/sessions",
    ] {
        let text = client
            .get(format!("{pf}{path}"))
            .header("authorization", "Bearer secret")
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(
            !text.contains("SECRET"),
            "{path} response leaked a token: {text}"
        );
    }
}

#[tokio::test]
async fn pools_endpoint_aggregates_named_and_unpooled_groups() {
    let pf = spawn(seed_store().await).await;
    let client = reqwest::Client::new();
    let body: serde_json::Value = client
        .get(format!("{pf}/api/pools"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let arr = body.as_array().unwrap();
    // Named pool "team-a" first, unpooled (null) group last.
    assert_eq!(arr[0]["pool"], "team-a");
    assert_eq!(arr[0]["accounts"], 1);
    assert_eq!(arr[0]["active"], 1);
    assert!(arr[1]["pool"].is_null(), "unpooled group last");
    assert_eq!(arr[1]["accounts"], 1);
}

#[tokio::test]
async fn pools_endpoint_carries_available_usage_percent_and_strategy() {
    // Task 10: /api/pools must additionally surface, per pool, `available` (eligible-right-now
    // count, i.e. active AND not currently cooled down by the live runtime overlay),
    // `usage_percent` (mean `used_percent` across the pool's accounts), and `strategy` (the
    // pool's configured routing-selector name).
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    let repo = store.accounts();
    repo.insert(
        &account("codex-d1", "d1@example.test", Some("default")),
        &tokens(),
        &cipher,
    )
    .await
    .unwrap();
    repo.insert(
        &account("codex-d2", "d2@example.test", Some("default")),
        &tokens(),
        &cipher,
    )
    .await
    .unwrap();
    std::mem::forget(dir);

    let (pf, state) = spawn_with_state(store).await;

    // Cool codex-d2 down via the live runtime overlay (same mechanism `/api/overview` reads through
    // `RuntimeStates::overlay`) — it stays durably `active` but is not selectable right now.
    state.runtime.record_rate_limit(
        &AccountId::from("codex-d2"),
        None,
        now(),
        &state.rate_limit_metrics,
    );

    let body: serde_json::Value = reqwest::Client::new()
        .get(format!("{pf}/api/pools"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let arr = body.as_array().unwrap();
    let default_pool = arr
        .iter()
        .find(|p| p["pool"] == "default")
        .expect("default pool present");

    assert_eq!(default_pool["accounts"], 2);
    assert_eq!(
        default_pool["available"], 1,
        "only codex-d1 is eligible right now: {default_pool:?}"
    );
    assert!(
        default_pool["usage_percent"].is_number(),
        "usage_percent must be numeric: {default_pool:?}"
    );
    assert!(
        default_pool["strategy"].is_string()
            && !default_pool["strategy"].as_str().unwrap().is_empty(),
        "strategy must be a non-empty string: {default_pool:?}"
    );
}

#[tokio::test]
async fn requests_endpoint_pages_the_log() {
    let store = seed_store().await;
    sqlx::query(
        "UPDATE request_log SET session_key = 'hashed-request-session', \
         protocol_outcome = 'incomplete'",
    )
    .execute(store.pool())
    .await
    .unwrap();
    let pf = spawn(store).await;
    let client = reqwest::Client::new();
    let body: serde_json::Value = client
        .get(format!("{pf}/api/requests?limit=10"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["total"], 1);
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["path"], "/responses");
    assert_eq!(rows[0]["status"], 200);
    assert_eq!(rows[0]["duration_ms"], 12);
    assert_eq!(rows[0]["session_key"], "hashed-request-session");
    assert_eq!(rows[0]["protocol_outcome"], "incomplete");

    let exact: serde_json::Value = client
        .get(format!(
            "{pf}/api/requests?session_key=hashed-request-session"
        ))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(exact["total"], 1);
    assert_eq!(exact["rows"][0]["session_key"], "hashed-request-session");
}

#[tokio::test]
async fn requests_endpoint_preserves_imported_outcome_when_http_status_is_unknown() {
    let store = seed_store().await;
    sqlx::query(
        "UPDATE request_log SET status = 0, outcome = 'success', error_code = NULL, \
         transport = 'websocket'",
    )
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO request_log \
         (requested_at, provider, method, path, aliased, status, duration_ms, outcome, error_code, transport) \
         VALUES (?, 'codex', 'POST', '/responses', 0, 0, 25, 'error', 'stream_incomplete', 'websocket')",
    )
    .bind(now())
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO request_log \
         (requested_at, provider, method, path, aliased, status, duration_ms, outcome, error_code, transport) \
         VALUES (?, 'codex', 'POST', '/responses', 0, 0, 30, 'error', \
                 'free form upstream failure: do not expose', 'websocket')",
    )
    .bind(now())
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO request_log \
         (requested_at, provider, method, path, aliased, status, duration_ms, outcome, error_code, transport) \
         VALUES (?, 'codex', 'POST', '/responses', 0, 0, 35, \
                 'arbitrary imported outcome', 'also arbitrary', 'websocket')",
    )
    .bind(now())
    .execute(store.pool())
    .await
    .unwrap();

    let pf = spawn(store).await;
    let body: serde_json::Value = reqwest::Client::new()
        .get(format!("{pf}/api/requests?limit=10"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let rows = body["rows"].as_array().unwrap();
    let success = rows.iter().find(|row| row["outcome"] == "success").unwrap();
    let failure = rows.iter().find(|row| row["duration_ms"] == 25).unwrap();

    assert_eq!(
        success["status"], 0,
        "legacy row keeps its no-HTTP-status sentinel"
    );
    assert!(success["error_code"].is_null());
    assert_eq!(success["transport"], "websocket");
    assert_eq!(failure["status"], 0);
    assert_eq!(failure["error_code"], "stream_incomplete");
    let bounded_failure = rows
        .iter()
        .find(|row| row["error_code"] == "legacy_error")
        .expect("unknown imported error codes must collapse to a bounded fallback");
    assert_eq!(bounded_failure["outcome"], "error");
    assert!(
        !body.to_string().contains("free form upstream failure"),
        "arbitrary imported error strings must never cross the API boundary"
    );
    let unknown_outcome = rows
        .iter()
        .find(|row| row["duration_ms"] == 35)
        .expect("hostile-outcome fixture is returned as a request row");
    assert!(unknown_outcome["outcome"].is_null());
    assert!(unknown_outcome["error_code"].is_null());
    assert!(!body.to_string().contains("arbitrary imported"));

    let client = reqwest::Client::new();
    for (status_class, expected_outcome) in [("success", "success"), ("error", "error")] {
        let filtered: serde_json::Value = client
            .get(format!("{pf}/api/requests?status_class={status_class}"))
            .header("authorization", "Bearer secret")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let expected_total = if status_class == "success" { 1 } else { 2 };
        assert_eq!(
            filtered["total"], expected_total,
            "{status_class} filter must classify only canonical legacy outcomes"
        );
        assert_eq!(filtered["rows"][0]["outcome"], expected_outcome);
    }

    let overview: serde_json::Value = client
        .get(format!("{pf}/api/overview"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(overview["kpis"]["requests"], 4);
    assert_eq!(overview["kpis"]["success"], 1);
    assert_eq!(overview["kpis"]["errors"], 2);
    let recent_errors = overview["recent_errors"].as_array().unwrap();
    assert_eq!(recent_errors.len(), 2);
    assert!(recent_errors
        .iter()
        .any(|row| row["error_code"] == "stream_incomplete"));
    assert!(recent_errors
        .iter()
        .any(|row| row["error_code"] == "legacy_error"));
    assert!(!overview.to_string().contains("free form upstream failure"));

    let series: serde_json::Value = client
        .get(format!("{pf}/api/overview/series"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let buckets = series["buckets"].as_array().unwrap();
    assert_eq!(
        buckets
            .iter()
            .map(|b| b["requests"].as_i64().unwrap())
            .sum::<i64>(),
        4
    );
    assert_eq!(
        buckets
            .iter()
            .map(|b| b["errors"].as_i64().unwrap())
            .sum::<i64>(),
        2
    );

    let reports: serde_json::Value = client
        .get(format!("{pf}/api/reports?range=24h&dimension=provider"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(reports["totals"]["requests"], 4);
    assert_eq!(reports["totals"]["errors"], 2);
}

/// Seeds 3 rows (2 codex/200 with metrics, 1 anthropic/500) so filters + the content-free metric
/// columns + the derived `tps` can be exercised end to end, unauthenticated (auth lands later).
async fn seed_store_for_filters() -> Store {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let repo = store.request_log();
    repo.insert(&RequestLogRecord {
        requested_at: now(),
        provider: "codex".to_string(),
        method: "POST".to_string(),
        path: "/responses".to_string(),
        aliased: false,
        status: 200,
        duration_ms: 2000,
        account_id: Some("acct-1".to_string()),
        target_kind: Some("account".to_string()),
        provider_credential_id: None,
        model: Some("gpt-5.6-sol".to_string()),
        upstream_model: None,
        upstream_transport: None,
        reasoning_effort: Some("high".to_string()),
        service_tier: Some("priority".to_string()),
        transport: Some("http".to_string()),
        ttft_ms: Some(500),
        total_tokens: Some(3000),
        cached_tokens: Some(1000),
        subagent: Some("review".to_string()),
        request_id: None,
        session_key: None,
        input_tokens: None,
        output_tokens: Some(300),
        cached_input_tokens: None,
        reasoning_tokens: None,
        orchestration_input_tokens: None,
        orchestration_output_tokens: None,
        orchestration_cached_input_tokens: None,
        cost_usd: None,
        latency_first_token_ms: None,
        protocol_outcome: None,
    })
    .await
    .unwrap();
    repo.insert(&RequestLogRecord {
        requested_at: now(),
        provider: "codex".to_string(),
        method: "POST".to_string(),
        path: "/responses".to_string(),
        aliased: false,
        status: 200,
        duration_ms: 1500,
        account_id: Some("acct-2".to_string()),
        target_kind: Some("account".to_string()),
        provider_credential_id: None,
        model: Some("gpt-5.6-sol".to_string()),
        upstream_model: None,
        upstream_transport: None,
        reasoning_effort: None,
        service_tier: None,
        transport: Some("http".to_string()),
        ttft_ms: None,
        total_tokens: None,
        cached_tokens: None,
        subagent: None,
        request_id: None,
        session_key: None,
        input_tokens: None,
        output_tokens: None,
        cached_input_tokens: None,
        reasoning_tokens: None,
        orchestration_input_tokens: None,
        orchestration_output_tokens: None,
        orchestration_cached_input_tokens: None,
        cost_usd: None,
        latency_first_token_ms: None,
        protocol_outcome: None,
    })
    .await
    .unwrap();
    repo.insert(&RequestLogRecord {
        requested_at: now(),
        provider: "anthropic".to_string(),
        method: "POST".to_string(),
        path: "/v1/messages".to_string(),
        aliased: false,
        status: 500,
        duration_ms: 300,
        account_id: Some("acct-3".to_string()),
        target_kind: Some("account".to_string()),
        provider_credential_id: None,
        model: Some("claude-x".to_string()),
        upstream_model: None,
        upstream_transport: None,
        reasoning_effort: None,
        service_tier: None,
        transport: Some("sse".to_string()),
        ttft_ms: None,
        total_tokens: None,
        cached_tokens: None,
        subagent: None,
        request_id: None,
        session_key: None,
        input_tokens: None,
        output_tokens: None,
        cached_input_tokens: None,
        reasoning_tokens: None,
        orchestration_input_tokens: None,
        orchestration_output_tokens: None,
        orchestration_cached_input_tokens: None,
        cost_usd: None,
        latency_first_token_ms: None,
        protocol_outcome: None,
    })
    .await
    .unwrap();
    std::mem::forget(dir);
    store
}

#[tokio::test]
async fn requests_endpoint_filters_by_provider_and_carries_content_free_metrics() {
    let pf = spawn(seed_store_for_filters().await).await;
    let client = reqwest::Client::new();
    let body: serde_json::Value = client
        .get(format!("{pf}/api/requests?provider=codex&limit=10"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["total"], 2, "only the 2 codex rows count");
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r["provider"] == "codex"));

    // Generation throughput uses completion tokens only: duration_ms=2000, ttft_ms=500 →
    // generation window = 1500ms = 1.5s; output_tokens=300 → tps = 200.0. Prompt tokens are
    // excluded because they were processed before the first output token.
    let full = rows
        .iter()
        .find(|r| r["account_id"] == "acct-1")
        .expect("acct-1 row present");
    assert_eq!(full["model"], "gpt-5.6-sol");
    assert_eq!(full["reasoning_effort"], "high");
    assert_eq!(full["service_tier"], "priority");
    assert_eq!(full["transport"], "http");
    assert_eq!(full["ttft_ms"], 500);
    assert_eq!(full["total_tokens"], 3000);
    assert_eq!(full["cached_tokens"], 1000);
    assert_eq!(full["tps"], 200.0);
    assert_eq!(
        full["subagent"], "review",
        "Task 3: read API surfaces the sub-agent label"
    );

    // The row missing TTFT/output tokens gets no derived tps.
    let partial = rows
        .iter()
        .find(|r| r["account_id"] == "acct-2")
        .expect("acct-2 row present");
    assert!(partial["tps"].is_null());
    assert!(partial["ttft_ms"].is_null());
    assert!(
        partial["subagent"].is_null(),
        "the main agent (no x-openai-subagent header) round-trips as null, not a placeholder"
    );
}

/// A minimal content-free `request_log` row for the overview KPI test: only the fields the
/// overview aggregation reads (`status`, `total_tokens`, `duration_ms`) are set; everything else
/// (`account_id`, `model`, `reasoning_effort`, `service_tier`, `transport`, `ttft_ms`,
/// `cached_tokens`, `subagent`) is `None` — those aren't exercised by `/api/overview`'s KPI tile.
fn req_row(status: u16, total_tokens: i64) -> RequestLogRecord {
    RequestLogRecord {
        requested_at: now(),
        provider: "codex".to_string(),
        method: "POST".to_string(),
        path: "/responses".to_string(),
        aliased: false,
        status,
        duration_ms: 100,
        account_id: None,
        target_kind: None,
        provider_credential_id: None,
        model: None,
        upstream_model: None,
        upstream_transport: None,
        reasoning_effort: None,
        service_tier: None,
        transport: None,
        ttft_ms: None,
        total_tokens: Some(total_tokens),
        cached_tokens: None,
        subagent: None,
        request_id: None,
        session_key: None,
        input_tokens: None,
        output_tokens: None,
        cached_input_tokens: None,
        reasoning_tokens: None,
        orchestration_input_tokens: None,
        orchestration_output_tokens: None,
        orchestration_cached_input_tokens: None,
        cost_usd: None,
        latency_first_token_ms: None,
        protocol_outcome: None,
    }
}

#[tokio::test]
async fn overview_reports_kpis_and_recent_errors_from_request_log() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let repo = store.request_log();
    for (status, tokens) in [(200, 1000), (200, 2000), (429, 0)] {
        repo.insert(&req_row(status, tokens)).await.unwrap();
    }
    std::mem::forget(dir);

    let pf = spawn(store).await;
    let v: serde_json::Value = reqwest::Client::new()
        .get(format!("{pf}/api/overview"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(v["kpis"]["requests"], 3);
    assert_eq!(v["kpis"]["success"], 2);
    assert_eq!(v["kpis"]["errors"], 1);
    let success_rate = v["kpis"]["success_rate"].as_f64().unwrap();
    assert!(
        (success_rate - (2.0 / 3.0)).abs() < 0.001,
        "expected ~0.667, got {success_rate}"
    );
    assert_eq!(v["kpis"]["total_tokens"], 3000);

    let recent_errors = v["recent_errors"].as_array().unwrap();
    assert!(
        !recent_errors.is_empty(),
        "the 429 row must surface in recent_errors"
    );
    assert_eq!(recent_errors[0]["status"], 429);

    // Shape smoke-check for the other top-level fields (no accounts seeded in this test → empty).
    assert!(v["pools"].as_array().unwrap().is_empty());
    assert_eq!(v["accounts_available"], 0);
    assert!(v["quota"].as_array().unwrap().is_empty());
    assert_eq!(v["admission"]["waiters"], 0);
    assert_eq!(v["admission"]["waits_total"], 0);
    assert_eq!(v["admission"]["timeouts_total"], 0);
    assert_eq!(v["admission"]["owner_recovery_total"], 0);
    assert_eq!(v["admission"]["avg_wait_ms"], 0.0);
    assert_eq!(v["admission"]["in_flight_pressure"], 0);
    assert_eq!(v["admission"]["calibration_ratio"], 1.0);
    assert_eq!(v["admission"]["calibration_samples"], 0);
}

#[tokio::test]
async fn overview_reports_pools_and_quota_from_seeded_accounts() {
    // `seed_store()` seeds two active codex accounts: codex-a in pool "team-a" with a 73.5%-used
    // weekly window (no 5h window reported), and codex-b unpooled with no usage window at all.
    let pf = spawn(seed_store().await).await;
    let v: serde_json::Value = reqwest::Client::new()
        .get(format!("{pf}/api/overview"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    // Both accounts are active with no runtime cooldown → both available.
    assert_eq!(v["accounts_available"], 2);

    let pools = v["pools"].as_array().unwrap();
    let team_a = pools
        .iter()
        .find(|p| p["pool"] == "team-a")
        .expect("team-a present");
    assert_eq!(team_a["accounts"], 1);
    assert_eq!(team_a["available"], 1);
    let unpooled = pools
        .iter()
        .find(|p| p["pool"].is_null())
        .expect("unpooled group present");
    assert_eq!(unpooled["accounts"], 1);
    assert_eq!(unpooled["available"], 1);

    // Single provider ("codex"): five_hour has no reported window on either account → remaining
    // 100%; weekly is the worst case across the two accounts — codex-a's 73.5%-used window
    // (26.5% remaining) beats codex-b's no-window default (100% remaining).
    let quota = v["quota"].as_array().unwrap();
    assert_eq!(quota.len(), 1);
    assert_eq!(quota[0]["provider"], "codex");
    assert_eq!(quota[0]["five_hour"], 100.0);
    assert_eq!(quota[0]["weekly"], 26.5);
}

/// Same content-free shape as [`req_row`], but with an explicit `requested_at` so the series test
/// can control which hourly bucket a row lands in.
fn req_row_at(requested_at: i64, status: u16, total_tokens: i64) -> RequestLogRecord {
    RequestLogRecord {
        requested_at,
        provider: "codex".to_string(),
        method: "POST".to_string(),
        path: "/responses".to_string(),
        aliased: false,
        status,
        duration_ms: 100,
        account_id: None,
        target_kind: None,
        provider_credential_id: None,
        model: None,
        upstream_model: None,
        upstream_transport: None,
        reasoning_effort: None,
        service_tier: None,
        transport: None,
        ttft_ms: None,
        total_tokens: Some(total_tokens),
        cached_tokens: None,
        subagent: None,
        request_id: None,
        session_key: None,
        input_tokens: None,
        output_tokens: None,
        cached_input_tokens: None,
        reasoning_tokens: None,
        orchestration_input_tokens: None,
        orchestration_output_tokens: None,
        orchestration_cached_input_tokens: None,
        cost_usd: None,
        latency_first_token_ms: None,
        protocol_outcome: None,
    }
}

/// `GET /api/overview/series`: hourly buckets over the rolling 24h window, ascending by `ts`, with
/// EVERY bucket in the grid present — including the ones with no rows, zero-filled rather than
/// missing (see `read_api.rs::overview_series_handler`'s doc comment for where that zero-fill lives).
#[tokio::test]
async fn overview_series_reports_hourly_buckets_zero_filled_over_the_24h_window() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let repo = store.request_log();

    let insert_ts = now();
    for (status, tokens) in [(200, 1000), (200, 2000), (500, 500)] {
        repo.insert(&req_row_at(insert_ts, status, tokens))
            .await
            .unwrap();
    }
    std::mem::forget(dir);

    let pf = spawn(store).await;
    let v: serde_json::Value = reqwest::Client::new()
        .get(format!("{pf}/api/overview/series"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(v["bucket_secs"], 3600);
    let buckets = v["buckets"].as_array().unwrap();
    assert!(
        buckets.len() >= 24,
        "hourly buckets across (at least) a 24h window, got {}",
        buckets.len()
    );

    // Ascending order by ts.
    let tss: Vec<i64> = buckets.iter().map(|b| b["ts"].as_i64().unwrap()).collect();
    let mut sorted = tss.clone();
    sorted.sort();
    assert_eq!(tss, sorted, "buckets must be ascending by ts");

    // The bucket our 3 rows landed in carries the real rollup.
    let expected_bucket_ts = (insert_ts / 3600) * 3600;
    let populated = buckets
        .iter()
        .find(|b| b["ts"] == expected_bucket_ts)
        .expect("the bucket our rows landed in must be present");
    assert_eq!(populated["requests"], 3);
    assert_eq!(populated["errors"], 1);
    assert_eq!(populated["total_tokens"], 3500);
    let avg = populated["avg_latency_ms"].as_f64().unwrap();
    assert!(
        (avg - 100.0).abs() < 0.001,
        "all 3 rows use duration_ms=100"
    );

    // Every other bucket in the grid is zero-filled, not absent.
    let others: Vec<_> = buckets
        .iter()
        .filter(|b| b["ts"] != expected_bucket_ts)
        .collect();
    assert!(!others.is_empty());
    for b in others {
        assert_eq!(b["requests"], 0);
        assert_eq!(b["errors"], 0);
        assert_eq!(b["total_tokens"], 0);
        assert_eq!(b["avg_latency_ms"], 0.0);
    }
}

#[tokio::test]
async fn account_detail_returns_identity_status_quota_and_token_status_and_404s_for_unknown() {
    // Task 8: GET /api/accounts/{id} — the per-account detail view. Seed an account with a non-default
    // routing_policy + security_work_authorized=true + a usage window, and assert all three surface
    // (plus a non-empty quota_windows + a secret-safe token_status), then assert an unknown id 404s.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    let repo = store.accounts();
    let mut acct = account("acct-1", "acct1@example.test", Some("team-a"));
    acct.routing_policy = "burn_first".to_string();
    acct.security_work_authorized = true;
    repo.insert(&acct, &tokens(), &cipher).await.unwrap();
    repo.insert_usage_window(
        "acct-1",
        "secondary",
        40.0,
        Some(1_900_000_000),
        Some(10080),
        now(),
    )
    .await
    .unwrap();
    std::mem::forget(dir);

    let pf = spawn(store).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{pf}/api/accounts/acct-1"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(body["identity"]["id"], "acct-1");
    assert_eq!(body["identity"]["email"], "acct1@example.test");
    assert_eq!(body["identity"]["pool"], "team-a");
    assert_eq!(body["identity"]["provider"], "codex");
    assert_eq!(body["status"], "active");
    assert_eq!(body["routing_policy"], "burn_first");
    assert_eq!(body["security_work_authorized"], true);

    let quota = body["quota_windows"].as_array().unwrap();
    assert!(!quota.is_empty(), "quota_windows: {quota:?}");
    assert!(
        quota
            .iter()
            .any(|w| w["window"] == "weekly" && w["used_percent"] == 40.0),
        "quota_windows: {quota:?}"
    );

    let token_status = &body["token_status"];
    assert!(
        token_status["access_state"].is_string(),
        "token_status: {token_status:?}"
    );
    // "SECRET-ACCESS-TOKEN" isn't a JWT → exp can't be decoded → "missing", and the raw token must
    // never appear anywhere in the body.
    assert_eq!(token_status["access_state"], "missing");
    assert!(!body.to_string().contains("SECRET"));

    assert_eq!(body["request_totals"]["request_count"], 0);
    assert_eq!(body["request_totals"]["total_tokens"], 0);

    let missing = client
        .get(format!("{pf}/api/accounts/does-not-exist"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
}

/// Task 7: `total_tokens` is NULL on every imported/backfilled row (only the 0005
/// `input_tokens`/`output_tokens` columns are populated) — `request_totals.total_tokens` must fall
/// back to `input_tokens + output_tokens` per row so the lifetime total isn't just 0.
/// `reasoning_tokens` is set on the imported-shaped row (to a value that would double-count the
/// total if wrongly added) so a wrong implementation shows up as a wrong sum, not a
/// coincidentally-passing test.
#[tokio::test]
async fn account_detail_request_totals_fall_back_to_input_plus_output_tokens() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &account("acct-1", "acct1@example.test", None),
            &tokens(),
            &cipher,
        )
        .await
        .unwrap();

    let log = store.request_log();
    // A native row: total_tokens already populated — must not be disturbed by the fallback.
    log.insert(&RequestLogRecord {
        requested_at: now(),
        provider: "codex".to_string(),
        method: "POST".to_string(),
        path: "/responses".to_string(),
        aliased: false,
        status: 200,
        duration_ms: 100,
        account_id: Some("acct-1".to_string()),
        target_kind: Some("account".to_string()),
        provider_credential_id: None,
        model: None,
        upstream_model: None,
        upstream_transport: None,
        reasoning_effort: None,
        service_tier: None,
        transport: None,
        ttft_ms: None,
        total_tokens: Some(1000),
        cached_tokens: None,
        subagent: None,
        request_id: None,
        session_key: None,
        input_tokens: None,
        output_tokens: None,
        cached_input_tokens: None,
        reasoning_tokens: None,
        orchestration_input_tokens: None,
        orchestration_output_tokens: None,
        orchestration_cached_input_tokens: None,
        cost_usd: None,
        latency_first_token_ms: None,
        protocol_outcome: None,
    })
    .await
    .unwrap();
    // An imported-shaped row: total_tokens is NULL, only the 0005 input/output columns are set.
    log.insert(&RequestLogRecord {
        requested_at: now(),
        provider: "codex".to_string(),
        method: "POST".to_string(),
        path: "/responses".to_string(),
        aliased: false,
        status: 200,
        duration_ms: 100,
        account_id: Some("acct-1".to_string()),
        target_kind: Some("account".to_string()),
        provider_credential_id: None,
        model: None,
        upstream_model: None,
        upstream_transport: None,
        reasoning_effort: None,
        service_tier: None,
        transport: None,
        ttft_ms: None,
        total_tokens: None,
        cached_tokens: None,
        subagent: None,
        request_id: None,
        session_key: None,
        input_tokens: Some(1200),
        output_tokens: Some(300),
        cached_input_tokens: None,
        reasoning_tokens: Some(150),
        orchestration_input_tokens: None,
        orchestration_output_tokens: None,
        orchestration_cached_input_tokens: None,
        cost_usd: None,
        latency_first_token_ms: None,
        protocol_outcome: None,
    })
    .await
    .unwrap();
    std::mem::forget(dir);

    let pf = spawn(store).await;
    let body: serde_json::Value = reqwest::Client::new()
        .get(format!("{pf}/api/accounts/acct-1"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["request_totals"]["request_count"], 2);
    assert_eq!(
        body["request_totals"]["total_tokens"], 2500,
        "1000 (native) + (1200 input + 300 output), NOT + the imported row's reasoning_tokens"
    );
}

/// Task 7: same fallback, but for `/api/requests`' per-row `total_tokens`/`tps` — an
/// imported-shaped row (native `total_tokens`/`ttft_ms` absent, 0005 columns present) must still
/// surface a real `total_tokens`, effective TTFT, and derived `tps`. Throughput uses output tokens
/// over the post-first-token generation window; input tokens are excluded.
#[tokio::test]
async fn requests_endpoint_falls_back_to_input_output_tokens_and_latency_first_token_ms() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let repo = store.request_log();
    repo.insert(&RequestLogRecord {
        requested_at: now(),
        provider: "codex".to_string(),
        method: "POST".to_string(),
        path: "/responses".to_string(),
        aliased: false,
        status: 200,
        duration_ms: 1300,
        account_id: Some("acct-1".to_string()),
        target_kind: Some("account".to_string()),
        provider_credential_id: None,
        model: None,
        upstream_model: None,
        upstream_transport: None,
        reasoning_effort: None,
        service_tier: None,
        transport: None,
        ttft_ms: None,
        total_tokens: None,
        cached_tokens: None,
        subagent: None,
        request_id: None,
        session_key: None,
        input_tokens: Some(1500),
        output_tokens: Some(500),
        cached_input_tokens: None,
        reasoning_tokens: Some(200), // subset of output_tokens — must NOT be added to the total
        orchestration_input_tokens: None,
        orchestration_output_tokens: None,
        orchestration_cached_input_tokens: None,
        cost_usd: None,
        latency_first_token_ms: Some(300),
        protocol_outcome: None,
    })
    .await
    .unwrap();
    std::mem::forget(dir);

    let pf = spawn(store).await;
    let body: serde_json::Value = reqwest::Client::new()
        .get(format!("{pf}/api/requests?limit=10"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]["total_tokens"], 2000,
        "1500 input + 500 output, NOT + reasoning_tokens(200)"
    );
    // duration_ms(1300) - latency_first_token_ms(300) = 1000ms = 1.0s;
    // output_tokens(500) / 1.0s = 500.0 tokens/sec.
    assert_eq!(rows[0]["tps"], 500.0);
    assert_eq!(
        rows[0]["ttft_ms"], 300,
        "the API should surface the effective imported/backfilled TTFT"
    );
}

#[tokio::test]
async fn account_trends_returns_seeded_history_split_by_window() {
    // Task 9: GET /api/accounts/{id}/trends — a 7-day per-window usage series derived from
    // `usage_history`. Seed 3 rows for acct-1 (2 primary, 1 secondary) across distinct
    // timestamps and assert the response splits them into `primary`/`secondary` point arrays.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    let repo = store.accounts();
    repo.insert(
        &account("acct-1", "acct1@example.test", None),
        &tokens(),
        &cipher,
    )
    .await
    .unwrap();
    let t0 = now() - 3 * 3600;
    let t1 = now() - 2 * 3600;
    let t2 = now() - 3600;
    repo.insert_usage_window("acct-1", "primary", 12.0, None, None, t0)
        .await
        .unwrap();
    repo.insert_usage_window("acct-1", "secondary", 40.0, None, None, t1)
        .await
        .unwrap();
    repo.insert_usage_window("acct-1", "primary", 15.5, None, None, t2)
        .await
        .unwrap();
    std::mem::forget(dir);

    let pf = spawn(store).await;
    let resp = reqwest::Client::new()
        .get(format!("{pf}/api/accounts/acct-1/trends"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(body["account_id"], "acct-1");
    let primary = body["primary"].as_array().expect("primary is an array");
    assert_eq!(primary.len(), 2, "primary: {primary:?}");
    assert_eq!(primary[0]["t"], t0);
    assert_eq!(primary[0]["v"], 12.0);
    assert_eq!(primary[1]["t"], t2);
    assert_eq!(primary[1]["v"], 15.5);
    for p in primary {
        let v = p["v"].as_f64().unwrap();
        assert!((0.0..=100.0).contains(&v), "v out of range: {v}");
    }

    let secondary = body["secondary"].as_array().expect("secondary is an array");
    assert_eq!(secondary.len(), 1, "secondary: {secondary:?}");
    assert_eq!(secondary[0]["t"], t1);
    assert_eq!(secondary[0]["v"], 40.0);
}

#[tokio::test]
async fn account_trends_returns_empty_series_for_account_with_no_history() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &account("acct-quiet", "quiet@example.test", None),
            &tokens(),
            &cipher,
        )
        .await
        .unwrap();
    std::mem::forget(dir);

    let pf = spawn(store).await;
    let resp = reqwest::Client::new()
        .get(format!("{pf}/api/accounts/acct-quiet/trends"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "no history is still a 200, not 404");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["primary"].as_array().unwrap().is_empty());
    assert!(body["secondary"].as_array().unwrap().is_empty());
}

/// TA6(c) Task 2: `GET /api/sessions` — content-free session→account affinity view. Seeds two
/// accounts and three sessions (one owned by each account, one never-owned/`fresh`) and asserts
/// the `{total, rows}` envelope, the LEFT-JOINed `owner_email`, and that the NULL-owner row
/// survives (serializes `owner_email: null`, not dropped — proves LEFT not INNER).
#[tokio::test]
async fn sessions_endpoint_surfaces_owner_email_and_keeps_null_owner_rows() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    let repo = store.accounts();
    repo.insert(
        &account("sess-a", "sess-a@example.test", None),
        &tokens(),
        &cipher,
    )
    .await
    .unwrap();
    repo.insert(
        &account("sess-b", "sess-b@example.test", None),
        &tokens(),
        &cipher,
    )
    .await
    .unwrap();

    let continuity = store.continuity();
    let t0 = now() - 300;
    let t1 = now() - 200;
    let t2 = now() - 100;
    continuity
        .record_completion("sk-owned-a", "hard", "sess-a", "resp-a", "fp-a", 1, t0)
        .await
        .unwrap();
    continuity
        .record_completion("sk-owned-b", "soft", "sess-b", "resp-b", "fp-b", 1, t1)
        .await
        .unwrap();
    // Fresh session, never completed a turn -> owning_account_id stays NULL.
    continuity
        .ensure_session("sk-unowned", "soft", t2)
        .await
        .unwrap();
    std::mem::forget(dir);

    let pf = spawn(store).await;
    let body: serde_json::Value = reqwest::Client::new()
        .get(format!("{pf}/api/sessions"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["total"], 3, "body: {body}");
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3, "rows: {rows:?}");

    // Ordered by last_activity_at DESC: sk-unowned (t2) first, then sk-owned-b (t1), then
    // sk-owned-a (t0).
    assert_eq!(rows[0]["session_key"], "sk-unowned");
    assert!(
        rows[0]["owner_email"].is_null(),
        "unowned session must serialize owner_email: null, not be dropped: {:?}",
        rows[0]
    );
    assert!(rows[0]["owning_account_id"].is_null());
    assert_eq!(rows[0]["state"], "fresh");
    assert_eq!(rows[0]["key_strength"], "soft");

    let by_key = |key: &str| -> serde_json::Value {
        rows.iter()
            .find(|r| r["session_key"] == key)
            .cloned()
            .unwrap_or_else(|| panic!("{key} not found in rows: {rows:?}"))
    };

    let owned_a = by_key("sk-owned-a");
    assert_eq!(owned_a["owning_account_id"], "sess-a");
    assert_eq!(owned_a["owner_email"], "sess-a@example.test");
    assert_eq!(owned_a["state"], "anchored");
    assert_eq!(owned_a["key_strength"], "hard");

    let owned_b = by_key("sk-owned-b");
    assert_eq!(owned_b["owning_account_id"], "sess-b");
    assert_eq!(owned_b["owner_email"], "sess-b@example.test");
    assert_eq!(owned_b["state"], "anchored");

    // Every row must carry the timestamp fields too.
    for row in rows {
        assert!(row["created_at"].is_i64());
        assert!(row["updated_at"].is_i64());
        assert!(row["last_activity_at"].is_i64());
    }

    let exact: serde_json::Value = reqwest::Client::new()
        .get(format!("{pf}/api/sessions?session_key=sk-owned-a"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(exact["total"], 1);
    assert_eq!(exact["rows"][0]["session_key"], "sk-owned-a");
    assert_eq!(exact["rows"][0]["owner_email"], "sess-a@example.test");
}

/// `limit`/`offset` are honored (like `/api/requests`) and clamped: `limit=0` clamps up to 1
/// (never an empty-by-limit page when rows exist), `limit=5000` clamps down to the 1000 max.
#[tokio::test]
async fn sessions_endpoint_honors_pagination_and_clamps_limit() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);

    let continuity = store.continuity();
    let base = now();
    for i in 0..3 {
        continuity
            .ensure_session(&format!("sk-page-{i}"), "soft", base + i)
            .await
            .unwrap();
    }

    let pf = spawn(store).await;
    let client = reqwest::Client::new();

    // limit=2 -> exactly 2 rows, total still reports the full 3.
    let body: serde_json::Value = client
        .get(format!("{pf}/api/sessions?limit=2"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["total"], 3);
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);

    // offset=2 -> the 3rd (last-activity-oldest) row only.
    let body: serde_json::Value = client
        .get(format!("{pf}/api/sessions?limit=2&offset=2"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);

    // limit=0 clamps up to 1, not an empty page.
    let body: serde_json::Value = client
        .get(format!("{pf}/api/sessions?limit=0"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);

    // limit=5000 clamps down to MAX_LIMIT (1000) -> still just returns all 3 available rows.
    let body: serde_json::Value = client
        .get(format!("{pf}/api/sessions?limit=5000"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["rows"].as_array().unwrap().len(), 3);
}

/// `/api/sessions` sits behind the SAME `require_admin` gate as `/api/accounts` — a keyless
/// request must be rejected exactly like the other `/api/*` routes, never open.
#[tokio::test]
async fn sessions_endpoint_is_behind_the_admin_gate() {
    let pf = spawn(seed_store().await).await;
    let client = reqwest::Client::new();

    let gated = client
        .get(format!("{pf}/api/accounts"))
        .send()
        .await
        .unwrap();
    let sessions = client
        .get(format!("{pf}/api/sessions"))
        .send()
        .await
        .unwrap();

    assert_ne!(sessions.status(), 200, "must not be open: {sessions:?}");
    assert_eq!(
        sessions.status(),
        gated.status(),
        "must be gated identically to /api/accounts"
    );
}

#[tokio::test]
async fn requests_endpoint_filters_by_status_class() {
    let pf = spawn(seed_store_for_filters().await).await;
    let client = reqwest::Client::new();
    let body: serde_json::Value = client
        .get(format!("{pf}/api/requests?status_class=error&limit=10"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["total"], 1);
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["status"], 500);
    assert_eq!(rows[0]["provider"], "anthropic");
}

/// A `RequestLogRecord` shaped for the `/api/reports` endpoint test: like `req_row_at` but carries
/// a `model` + `cost_usd` so `dimension=model` has real per-model rows to break down, and
/// `cached_tokens` so `totals.cache_hit_rate` has a non-trivial value to assert on.
fn report_endpoint_row(
    requested_at: i64,
    model: &str,
    status: u16,
    total_tokens: i64,
    cached_tokens: i64,
    cost_usd: f64,
) -> RequestLogRecord {
    RequestLogRecord {
        requested_at,
        provider: "codex".to_string(),
        method: "POST".to_string(),
        path: "/responses".to_string(),
        aliased: false,
        status,
        duration_ms: 100,
        account_id: None,
        target_kind: None,
        provider_credential_id: None,
        model: Some(model.to_string()),
        upstream_model: None,
        upstream_transport: None,
        reasoning_effort: None,
        service_tier: None,
        transport: None,
        ttft_ms: None,
        total_tokens: Some(total_tokens),
        cached_tokens: Some(cached_tokens),
        subagent: None,
        request_id: None,
        session_key: None,
        input_tokens: None,
        output_tokens: None,
        cached_input_tokens: None,
        reasoning_tokens: None,
        orchestration_input_tokens: None,
        orchestration_output_tokens: None,
        orchestration_cached_input_tokens: None,
        cost_usd: Some(cost_usd),
        latency_first_token_ms: None,
        protocol_outcome: None,
    }
}

/// `GET /api/reports?range=7d&dimension=model`: 200 with a zero-filled ascending `time_series`,
/// a per-model `breakdown` (ordered by cost desc, matching `reports_breakdown`'s own order), and
/// `totals` carrying the derived `error_rate`/`cache_hit_rate`. `?range=bogus` is a 400 (an
/// explicit-but-unknown value, NOT defaulted); the endpoint sits behind the same admin gate as
/// every other `/api/*` read, so a keyless request is a 401.
#[tokio::test]
async fn reports_endpoint_assembles_zero_filled_series_breakdown_and_totals() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let repo = store.request_log();

    let insert_ts = now();
    // 2x model-a (one error), 1x model-b — model-a costs more, so it must sort first in the
    // cost-desc breakdown.
    repo.insert(&report_endpoint_row(
        insert_ts, "model-a", 200, 1000, 400, 1.5,
    ))
    .await
    .unwrap();
    repo.insert(&report_endpoint_row(
        insert_ts, "model-a", 500, 2000, 800, 2.5,
    ))
    .await
    .unwrap();
    repo.insert(&report_endpoint_row(
        insert_ts, "model-b", 200, 500, 250, 0.5,
    ))
    .await
    .unwrap();
    std::mem::forget(dir);

    let pf = spawn(store).await;
    let client = reqwest::Client::new();

    // Keyless -> 401 (same admin gate as every other /api/* read).
    let unauth = client
        .get(format!("{pf}/api/reports?range=7d&dimension=model"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth.status(), reqwest::StatusCode::UNAUTHORIZED);

    // Explicit-but-unknown range -> 400 (not silently defaulted).
    let bad_range = client
        .get(format!("{pf}/api/reports?range=bogus"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(bad_range.status(), reqwest::StatusCode::BAD_REQUEST);

    // Explicit-but-unknown dimension -> 400.
    let bad_dimension = client
        .get(format!("{pf}/api/reports?dimension=bogus"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(bad_dimension.status(), reqwest::StatusCode::BAD_REQUEST);

    // Happy path.
    let v: serde_json::Value = client
        .get(format!("{pf}/api/reports?range=7d&dimension=model"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    // totals: 3 requests, 1 error (the 500), cost summed, tokens summed w/ derived rates.
    assert_eq!(v["totals"]["requests"], 3);
    assert_eq!(v["totals"]["errors"], 1);
    assert_eq!(v["totals"]["tokens"], 3500);
    assert_eq!(v["totals"]["cached_tokens"], 1450);
    let cost = v["totals"]["cost_usd"].as_f64().unwrap();
    assert!((cost - 4.5).abs() < 0.001, "cost_usd: {cost}");
    let error_rate = v["totals"]["error_rate"].as_f64().unwrap();
    assert!(
        (error_rate - (1.0 / 3.0)).abs() < 0.001,
        "error_rate: {error_rate}"
    );
    let cache_hit_rate = v["totals"]["cache_hit_rate"].as_f64().unwrap();
    assert!(
        (cache_hit_rate - (1450.0 / 3500.0)).abs() < 0.001,
        "cache_hit_rate: {cache_hit_rate}"
    );

    // breakdown: per-model, ordered by summed cost_usd descending — model-a (4.0) before
    // model-b (0.5).
    let breakdown = v["breakdown"].as_array().unwrap();
    assert_eq!(breakdown.len(), 2);
    assert_eq!(breakdown[0]["key"], "model-a");
    assert_eq!(breakdown[0]["requests"], 2);
    assert_eq!(breakdown[0]["errors"], 1);
    assert_eq!(breakdown[1]["key"], "model-b");
    assert_eq!(breakdown[1]["requests"], 1);

    // time_series: ascending by ts, zero-filled across the whole 7d/daily grid — including the
    // bucket our 3 rows landed in (real rollup) and every other (empty) bucket.
    let buckets = v["time_series"].as_array().unwrap();
    assert!(
        buckets.len() >= 7,
        "daily buckets across (at least) a 7d window, got {}",
        buckets.len()
    );
    let tss: Vec<i64> = buckets.iter().map(|b| b["ts"].as_i64().unwrap()).collect();
    let mut sorted = tss.clone();
    sorted.sort();
    assert_eq!(tss, sorted, "time_series must be ascending by ts");

    let bucket_secs = 24 * 3600;
    let expected_bucket_ts = (insert_ts / bucket_secs) * bucket_secs;
    let populated = buckets
        .iter()
        .find(|b| b["ts"] == expected_bucket_ts)
        .expect("the bucket our rows landed in must be present");
    assert_eq!(populated["requests"], 3);
    assert_eq!(populated["errors"], 1);

    let empty_buckets = buckets.iter().filter(|b| b["ts"] != expected_bucket_ts);
    for b in empty_buckets {
        assert_eq!(b["requests"], 0, "non-populated bucket must be zero-filled");
        assert_eq!(b["errors"], 0);
        assert_eq!(b["tokens"], 0);
    }
}
