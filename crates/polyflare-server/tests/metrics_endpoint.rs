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

    // The admin-disabled posture (no POLYFLARE_ADMIN_TOKEN) also applies uniformly: 503, inherited
    // from the same `require_admin` gate `/api/whoami` already proves this for in `dashboard_api.rs`.
    let up2 = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf2, _state2) = spawn_without_admin_token(up2).await;
    let disabled_resp = c
        .get(format!("{pf2}/metrics"))
        .header("authorization", "Bearer whatever")
        .send()
        .await
        .unwrap();
    assert_eq!(
        disabled_resp.status(),
        503,
        "no POLYFLARE_ADMIN_TOKEN configured ⇒ /metrics disabled, same as the rest of the dashboard API"
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
