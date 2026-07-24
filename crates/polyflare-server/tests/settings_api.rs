//! Settings subsystem Task 5: `GET`/`PATCH /api/settings`. Asserts the PATCH validate+clamp+
//! persist+live contract (a clamped value — not the raw request value — persists to the `settings`
//! table AND is immediately visible via both the live `GET` and `RuntimeSettings`'s own getters),
//! the cross-field `starvation_heartbeat ≤ starvation_wait_budget` ordering within one PATCH body,
//! strict per-field JSON-kind coercion (400 on a wrong-shaped value), a non-live key's hard 400
//! (never reaching `RuntimeSettings::set`), the admin gate (401 keyless), and `GET`'s full field
//! list with the correct `class` per field — including `admin_token` never carrying a value.

mod support;
use support::spawn;

#[tokio::test]
async fn patch_max_account_attempts_applies_live_and_persists() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await;
    let c = reqwest::Client::new();

    let resp = c
        .patch(format!("{pf}/api/settings"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "max_account_attempts": 7 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body, serde_json::json!({ "ok": true }));

    // Live: the atomic holder reflects it immediately.
    assert_eq!(state.runtime_settings.max_account_attempts(), 7);

    // GET reflects it too.
    let get: serde_json::Value = c
        .get(format!("{pf}/api/settings"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let field = find_field(&get, "max_account_attempts");
    assert_eq!(field["value"], "7");
    assert_eq!(field["class"], "live");

    // Persisted to the store.
    let all = state.store.settings().get_all().await.unwrap();
    assert_eq!(
        all.get("max_account_attempts").map(String::as_str),
        Some("7")
    );
}

#[tokio::test]
async fn patch_inflight_penalty_pct_clamps_to_fifty_and_persists_the_clamped_value() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await;
    let c = reqwest::Client::new();

    let resp = c
        .patch(format!("{pf}/api/settings"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "inflight_penalty_pct": 99 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Live: clamped to 50, not the raw 99.
    assert_eq!(state.runtime_settings.inflight_penalty_pct(), 50.0);

    // Persisted value is the CLAMPED canonical string, not "99".
    let all = state.store.settings().get_all().await.unwrap();
    assert_eq!(
        all.get("inflight_penalty_pct").map(String::as_str),
        Some("50")
    );
}

#[tokio::test]
async fn patch_starvation_heartbeat_above_budget_clamps_to_the_current_budget() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await; // support::spawn seeds starvation_wait_budget = 60s
    let c = reqwest::Client::new();

    let resp = c
        .patch(format!("{pf}/api/settings"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "starvation_heartbeat": 99999 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    assert_eq!(
        state.runtime_settings.starvation_heartbeat(),
        std::time::Duration::from_secs(60)
    );
    let all = state.store.settings().get_all().await.unwrap();
    assert_eq!(
        all.get("starvation_heartbeat").map(String::as_str),
        Some("60")
    );
}

#[tokio::test]
async fn patch_one_body_applies_budget_before_heartbeat_so_heartbeat_clamps_against_the_incoming_budget(
) {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await; // seeded budget = 60s
    let c = reqwest::Client::new();

    // In one PATCH: lower the budget to 5s AND ask for a heartbeat of 20s. If heartbeat clamped
    // against the STALE pre-PATCH budget (60s) it would pass through as 20; clamped against the
    // INCOMING 5s budget it must clamp down to 5.
    let resp = c
        .patch(format!("{pf}/api/settings"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "starvation_heartbeat": 20, "starvation_wait_budget": 5 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    assert_eq!(
        state.runtime_settings.starvation_wait_budget(),
        std::time::Duration::from_secs(5)
    );
    assert_eq!(
        state.runtime_settings.starvation_heartbeat(),
        std::time::Duration::from_secs(5),
        "heartbeat must clamp against the INCOMING budget from the same PATCH body, not the stale pre-PATCH one"
    );
}

#[tokio::test]
async fn patch_non_live_key_is_400_and_applies_nothing() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await;
    let c = reqwest::Client::new();

    let resp = c
        .patch(format!("{pf}/api/settings"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "bind_addr": "x" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    assert!(
        state.store.settings().get_all().await.unwrap().is_empty(),
        "a rejected patch must not persist anything"
    );
}

#[tokio::test]
async fn patch_wrong_kind_value_is_400() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await;
    let c = reqwest::Client::new();

    // A string where a bool is expected.
    let resp = c
        .patch(format!("{pf}/api/settings"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "live_logs": "notabool" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // Untouched: still whatever support::spawn seeded (true).
    assert!(state.runtime_settings.live_logs());
    assert!(!state
        .store
        .settings()
        .get_all()
        .await
        .unwrap()
        .contains_key("live_logs"));
}

#[tokio::test]
async fn patch_a_mixed_valid_and_unknown_key_body_applies_nothing() {
    // Validate-before-apply: a body containing one valid live key and one unknown key must reject
    // the WHOLE patch, not partially apply the valid one.
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await;
    let c = reqwest::Client::new();

    let resp = c
        .patch(format!("{pf}/api/settings"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "max_account_attempts": 9, "bind_addr": "x" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    assert_ne!(state.runtime_settings.max_account_attempts(), 9);
}

#[tokio::test]
async fn patch_without_admin_token_is_401() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn(up).await;
    let c = reqwest::Client::new();

    let resp = c
        .patch(format!("{pf}/api/settings"))
        .json(&serde_json::json!({ "max_account_attempts": 7 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn get_without_admin_token_is_401() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn(up).await;
    let c = reqwest::Client::new();

    let resp = c.get(format!("{pf}/api/settings")).send().await.unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn get_returns_every_field_with_the_correct_class() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn(up).await;
    let c = reqwest::Client::new();

    let body: serde_json::Value = c
        .get(format!("{pf}/api/settings"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let fields = body["fields"].as_array().unwrap();

    let live_keys = [
        "max_account_attempts",
        "starvation_wait_budget",
        "starvation_heartbeat",
        "wake_jitter_ms",
        "stream_idle_timeout",
        "inflight_penalty_pct",
        "soft_drain_enabled",
        "request_log_retention_days",
        "usage_history_retention_days",
        "live_logs",
    ];
    for key in live_keys {
        let f = find_field(&body, key);
        assert_eq!(f["class"], "live", "{key} must be class=live");
        assert!(
            f["value"].is_string(),
            "{key} (a live field) must carry a current value"
        );
    }

    let restart_only_keys = [
        "routing_strategy",
        "pool_strategies",
        "model_catalog_ttl_secs",
        "model_catalog_enabled",
        "client_websocket_enabled",
        "http_requests_use_upstream_websocket",
        "http_upstream_websocket_ping",
        "websocket_idle_ping_secs",
        "websocket_idle_budget_secs",
        "continuity_watchdog",
    ];
    for key in restart_only_keys {
        assert_eq!(
            find_field(&body, key)["class"],
            "restart-only",
            "{key} must be class=restart-only"
        );
    }
    assert_eq!(
        find_field(&body, "client_websocket_enabled")["default"],
        "true",
        "the dashboard must describe downstream WS as the production default"
    );
    assert_eq!(
        find_field(&body, "http_requests_use_upstream_websocket")["label"],
        "Use an upstream WebSocket for HTTP requests"
    );

    let fixed_keys = [
        "bind_addr",
        "db_path",
        "key_path",
        "upstream_base_url",
        "anthropic_upstream_base_url",
        "auth_base_url",
        "admin_token",
        "capture_fingerprint_path",
        "allow_unauthenticated_remote",
    ];
    for key in fixed_keys {
        assert_eq!(
            find_field(&body, key)["class"],
            "fixed",
            "{key} must be class=fixed"
        );
    }

    // admin_token is NEVER returned as a value, even though the dashboard is reachable (so it IS
    // set) — content-safety, not merely "unset".
    assert!(
        find_field(&body, "admin_token")["value"].is_null(),
        "admin_token must never carry a value"
    );

    assert_eq!(
        fields.len(),
        live_keys.len() + restart_only_keys.len() + fixed_keys.len()
    );
}

#[tokio::test]
async fn websocket_settings_persist_for_restart_and_report_pending_state() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await;
    let c = reqwest::Client::new();

    let resp = c
        .patch(format!("{pf}/api/settings"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({
            "http_requests_use_upstream_websocket": true,
            "websocket_idle_ping_secs": 1,
            "websocket_idle_budget_secs": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let persisted = state.store.settings().get_all().await.unwrap();
    assert_eq!(
        persisted
            .get("http_requests_use_upstream_websocket")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        persisted
            .get("websocket_idle_ping_secs")
            .map(String::as_str),
        Some("5")
    );
    assert_eq!(
        persisted
            .get("websocket_idle_budget_secs")
            .map(String::as_str),
        Some("60")
    );

    let body: serde_json::Value = c
        .get(format!("{pf}/api/settings"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let transport = find_field(&body, "http_requests_use_upstream_websocket");
    assert_eq!(transport["value"], "false");
    assert_eq!(transport["configured_value"], "true");
    assert_eq!(transport["pending_restart"], true);

    let ping = find_field(&body, "websocket_idle_ping_secs");
    assert_eq!(ping["value"], "30");
    assert_eq!(ping["configured_value"], "5");
    assert_eq!(ping["pending_restart"], true);
}

#[tokio::test]
async fn legacy_restart_values_are_canonicalized_before_pending_comparison() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await;
    state
        .store
        .settings()
        .set("ws_downstream", "1", 1)
        .await
        .unwrap();

    let body: serde_json::Value = reqwest::Client::new()
        .get(format!("{pf}/api/settings"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let field = find_field(&body, "client_websocket_enabled");
    assert_eq!(field["value"], "true");
    assert_eq!(field["configured_value"], "true");
    assert_eq!(field["pending_restart"], false);
}

#[tokio::test]
async fn get_starvation_heartbeat_max_tracks_the_live_current_budget() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await; // seeded budget = 60s
    let c = reqwest::Client::new();

    let before: serde_json::Value = c
        .get(format!("{pf}/api/settings"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(find_field(&before, "starvation_heartbeat")["max"], 60.0);

    // Lower the budget live; the heartbeat's advertised max must track it.
    c.patch(format!("{pf}/api/settings"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "starvation_wait_budget": 30 }))
        .send()
        .await
        .unwrap();

    let after: serde_json::Value = c
        .get(format!("{pf}/api/settings"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(find_field(&after, "starvation_heartbeat")["max"], 30.0);
    let _ = state; // silence unused warning if the assertions above are trimmed later
}

fn find_field<'a>(body: &'a serde_json::Value, key: &str) -> &'a serde_json::Value {
    body["fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["key"] == key)
        .unwrap_or_else(|| panic!("no field named {key} in GET /api/settings response"))
}
