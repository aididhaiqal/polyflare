//! C11 Task 2 — `GET /metrics`: admin-gated Prometheus scrape endpoint integration e2e. Drives the
//! REAL `build_app` router (`support::spawn`/`spawn_without_admin_token`, the same harness
//! `dashboard_api.rs` uses) rather than unit-testing `crate::metrics::render_prometheus_text`
//! directly (Task 1 already covers that pure renderer) — this file proves the HTTP wiring: the
//! route is admin-gated, returns the right content-type, reflects live `AppState` counters, and
//! (the content-safety crux) never leaks the store's email/token fields through the
//! `AccountSnapshot`-only mapping into `AccountMetric`.

mod support;

use std::time::{SystemTime, UNIX_EPOCH};

use polyflare_core::AccountId;
use polyflare_store::{Account, PlainTokens};
use support::{spawn, spawn_without_admin_token};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// (a) `GET /metrics` WITH the admin Bearer returns `200`, the Prometheus content-type, and a body
/// containing a process counter plus (the seeded `acct-1` account) a per-account gauge line.
#[tokio::test]
async fn metrics_with_admin_bearer_returns_prometheus_body() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn(up).await; // admin_token = Some("secret"); seeds "acct-1"

    let r = reqwest::Client::new()
        .get(format!("{pf}/metrics"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();

    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["content-type"], "text/plain; version=0.0.4");
    let body = r.text().await.unwrap();
    assert!(
        body.contains("polyflare_failover_total"),
        "missing process counter: {body}"
    );
    assert!(
        body.contains("polyflare_account_inflight{account_id=\"acct-1\""),
        "missing per-account gauge line for the seeded account: {body}"
    );
    assert!(
        body.contains("polyflare_admission_waiters{work=\"request\",scope=\"owner\"} 0"),
        "missing admission pressure family: {body}"
    );
    assert!(
        body.contains("polyflare_account_inflight_pressure{account_id=\"acct-1\""),
        "missing weighted account-pressure gauge: {body}"
    );
    assert!(
        body.contains("polyflare_request_pressure_calibration_ratio 1"),
        "missing request-pressure calibration gauge: {body}"
    );
}

/// (b) A KEYLESS `GET /metrics` is rejected exactly the same way a keyless `GET /api/accounts`
/// is — same status, not 200 — proving `/metrics` inherits the SAME `require_admin` gate as the
/// rest of the dashboard API.
#[tokio::test]
async fn metrics_keyless_is_rejected_same_as_keyless_api_accounts() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn(up).await;
    let c = reqwest::Client::new();

    let metrics_resp = c.get(format!("{pf}/metrics")).send().await.unwrap();
    let accounts_resp = c.get(format!("{pf}/api/accounts")).send().await.unwrap();

    assert_ne!(
        metrics_resp.status(),
        200,
        "keyless /metrics must not be 200"
    );
    assert_eq!(
        metrics_resp.status(),
        accounts_resp.status(),
        "keyless /metrics must be rejected identically to keyless /api/accounts"
    );

    // With no configured token on a loopback bind, the same local-open posture applies uniformly
    // to `/metrics` and the dashboard API.
    let up2 = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf2, _state2) = spawn_without_admin_token(up2).await;
    let disabled_resp = c.get(format!("{pf2}/metrics")).send().await.unwrap();
    assert_eq!(
        disabled_resp.status(),
        200,
        "tokenless loopback /metrics should open with the rest of the local dashboard API"
    );
}

/// (c) CONTENT-SAFETY: seed an account with a known email + a SECRET token, hit `/metrics` with
/// the admin key, and assert the body contains NEITHER — proving the account→label mapping goes
/// through the opaque `AccountSnapshot` (which has no email/token field) and never reaches the
/// store's account row for those fields.
#[tokio::test]
async fn metrics_body_never_contains_seeded_email_or_secret_token() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await;

    let leak_account = Account {
        id: "acct-leak-check".to_string(),
        chatgpt_account_id: None,
        chatgpt_user_id: None,
        email: "leaktest-9f3a@example.test".to_string(),
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
    };
    state
        .store
        .accounts()
        .insert(
            &leak_account,
            &PlainTokens {
                access_token: "SECRET-ACCESS-SHOULD-NOT-LEAK".to_string(),
                refresh_token: "SECRET-REFRESH-SHOULD-NOT-LEAK".to_string(),
                id_token: "SECRET-ID-SHOULD-NOT-LEAK".to_string(),
            },
            &state.cipher,
        )
        .await
        .unwrap();

    // C11b: populate the two new counter families keyed by the SAME opaque leak-check account id
    // (and a fixed rate-limit type) — proves the `upstream_request_metrics`/`rate_limit_metrics` →
    // `MetricsSnapshot` → `render_prometheus_text` pipeline is exactly as leak-proof as the
    // pre-existing per-account gauge families: the maps only ever hold what `record()` is handed
    // (an opaque account id, a numeric status, a fixed `&'static str` type), never a store row, so
    // there is structurally nothing here that could carry the email/token seeded above.
    state
        .upstream_request_metrics
        .record(Some("acct-leak-check"), 200);
    state.rate_limit_metrics.record("upstream");

    let r = reqwest::Client::new()
        .get(format!("{pf}/metrics"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let body = r.text().await.unwrap();

    assert!(
        !body.contains("leaktest-9f3a@example.test"),
        "body must never contain the seeded email: {body}"
    );
    assert!(
        !body.contains("SECRET-ACCESS-SHOULD-NOT-LEAK"),
        "body must never contain the seeded access token: {body}"
    );
    assert!(
        !body.contains("SECRET-REFRESH-SHOULD-NOT-LEAK"),
        "body must never contain the seeded refresh token: {body}"
    );
    assert!(
        !body.contains("SECRET-ID-SHOULD-NOT-LEAK"),
        "body must never contain the seeded id token: {body}"
    );
    // The account's OPAQUE id IS expected to appear — that's the whole point of the label.
    assert!(
        body.contains("account_id=\"acct-leak-check\""),
        "the opaque account id should still be present as the label: {body}"
    );

    // C11b: the two new counter families also carry only the opaque id / fixed type, never the
    // seeded email or token.
    assert!(
        body.contains(
            "polyflare_upstream_requests_total{provider=\"codex\",target_kind=\"account\",target_id=\"acct-leak-check\",status=\"200\"} 1"
        ),
        "missing the seeded upstream_requests_total line: {body}"
    );
    assert!(
        body.contains("polyflare_rate_limit_hits_total{type=\"upstream\"} 1"),
        "missing the seeded rate_limit_hits_total line: {body}"
    );
}

/// (d) Counters reflect LIVE state: acquiring a real `InFlightGuard` via
/// `state.runtime.acquire_in_flight` bumps both `polyflare_lease_acquired_total` and the seeded
/// account's `polyflare_account_inflight` gauge in the very next scrape — proving the handler reads
/// live `AppState`, not a stale/frozen snapshot.
#[tokio::test]
async fn metrics_reflect_live_lease_acquire_state() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await; // seeds "acct-1"
    let c = reqwest::Client::new();

    let before = c
        .get(format!("{pf}/metrics"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(before.contains("polyflare_lease_acquired_total 0"));
    assert!(before.contains("polyflare_account_inflight{account_id=\"acct-1\",status=\"active\",provider=\"codex\",pool=\"\"} 0"));

    let id = AccountId::from("acct-1");
    let guard = state
        .runtime
        .acquire_in_flight(&id, now(), &state.lease_metrics);

    let after = c
        .get(format!("{pf}/metrics"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        after.contains("polyflare_lease_acquired_total 1"),
        "lease_acquired_total should reflect the live acquire: {after}"
    );
    assert!(
        after.contains("polyflare_account_inflight{account_id=\"acct-1\",status=\"active\",provider=\"codex\",pool=\"\"} 1"),
        "the account's inflight gauge should reflect the live guard: {after}"
    );

    drop(guard);
}

/// (e) C11b: driving a REAL successful `/responses` request through the admin-gated harness bumps
/// `polyflare_upstream_requests_total{provider="codex",target_kind="account",target_id="acct-1",status="200"}`,
/// visible on the very next
/// admin-keyed `/metrics` scrape — proving the handler reads `state.upstream_request_metrics`
/// live, exactly like the existing lease/account gauge families above.
#[tokio::test]
async fn metrics_reflects_a_live_successful_responses_request_as_upstream_requests_total() {
    let up = polyflare_testkit::MockUpstream::new(vec![
        r#"{"type":"response.output_text.delta","delta":"hi"}"#.to_string(),
        r#"{"type":"response.completed","response":{"usage":{"input_tokens":100,"output_tokens":20}}}"#
            .to_string(),
    ])
    .spawn()
    .await;
    let (pf, _state) = spawn(up).await; // seeds "acct-1"
    let c = reqwest::Client::new();

    let resp = c
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "m", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let _ = resp.bytes().await.unwrap();

    let body = c
        .get(format!("{pf}/metrics"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert!(body.contains("# TYPE polyflare_upstream_requests_total counter"));
    assert!(
        body.contains(
            "polyflare_upstream_requests_total{provider=\"codex\",target_kind=\"account\",target_id=\"acct-1\",status=\"200\"} 1"
        ),
        "the driven /responses request should be visible as an upstream_requests_total sample: {body}"
    );
    assert!(
        body.contains("polyflare_request_pressure_samples_total 1"),
        "terminal usage must calibrate future request pressure: {body}"
    );
    assert!(
        body.contains("polyflare_request_pressure_equivalent_tokens_total 180"),
        "the calibration must use authoritative terminal compute-equivalent tokens: {body}"
    );
}

/// (f) C11b: driving a REAL upstream 429 through `/responses` (the only account 429s, so the
/// client sees a generic `502`, mirroring `tests/failure_routing.rs`'s
/// `a_429_cools_the_account_down_and_benches_it_next_request`) bumps
/// `polyflare_rate_limit_hits_total{type="backoff"}` — `MockUpstream::error_status` never sets a
/// `Retry-After` header, so `RuntimeStates::record_rate_limit` takes the computed-backoff branch —
/// visible on the very next admin-keyed `/metrics` scrape.
#[tokio::test]
async fn metrics_reflects_a_live_429_as_rate_limit_hits_total() {
    let up = polyflare_testkit::MockUpstream::error_status(
        429,
        r#"{"error":{"message":"rate limited"}}"#,
    )
    .spawn()
    .await;
    let (pf, _state) = spawn(up).await; // seeds "acct-1"
    let c = reqwest::Client::new();

    let resp = c
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "m", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        429,
        "the only account 429ing with no failover target preserves the actionable upstream status"
    );

    let body = c
        .get(format!("{pf}/metrics"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert!(body.contains("# TYPE polyflare_rate_limit_hits_total counter"));
    assert!(
        body.contains("polyflare_rate_limit_hits_total{type=\"backoff\"} 1"),
        "the driven 429 should be visible as a rate_limit_hits_total sample: {body}"
    );
}
