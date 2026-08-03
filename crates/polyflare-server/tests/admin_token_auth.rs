//! The stored admin token (`polyflare admin-token set`) against the REAL router.
//!
//! `dashboard_api.rs` covers the environment-variable token. This file covers the store-backed one
//! and, more importantly, the interaction that makes it safe: storing a token must close the
//! tokenless loopback bypass on a server that is already running, without a restart. The bypass
//! marker is installed once at startup from the bind address, so nothing about it can notice a
//! token written afterwards — only the per-request check in `auth::local_bypass_available` can.

mod support;
use polyflare_server::admin_token;
use support::{spawn_remote_without_admin_token, spawn_without_admin_token};

/// Install a token the way the CLI does, and hand back the plaintext.
async fn install_token(state: &polyflare_server::app::AppState) -> String {
    let token = admin_token::generate();
    state
        .store
        .admin_token()
        .set(&token.hash, &token.prefix, 1_700_000_000)
        .await
        .unwrap();
    token.raw
}

/// The headline property: a token set against a LIVE server takes effect on the next request. If
/// this regressed, `admin-token set` would silently leave the dashboard wide open until a restart.
#[tokio::test]
async fn storing_a_token_closes_the_loopback_bypass_without_a_restart() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn_without_admin_token(up).await;
    let c = reqwest::Client::new();

    let before = c.get(format!("{pf}/api/whoami")).send().await.unwrap();
    assert_eq!(before.status(), 200, "tokenless loopback starts open");

    let raw = install_token(&state).await;

    let after = c.get(format!("{pf}/api/whoami")).send().await.unwrap();
    assert_eq!(
        after.status(),
        401,
        "the same request must stop being admitted the moment a token exists"
    );

    let with_token = c
        .get(format!("{pf}/api/whoami"))
        .header("authorization", format!("Bearer {raw}"))
        .send()
        .await
        .unwrap();
    assert_eq!(with_token.status(), 200, "the stored token authenticates");
}

#[tokio::test]
async fn a_wrong_token_is_rejected_and_the_hash_itself_is_not_a_credential() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn_without_admin_token(up).await;
    let raw = install_token(&state).await;
    let c = reqwest::Client::new();

    let wrong = c
        .get(format!("{pf}/api/whoami"))
        .header("authorization", "Bearer pfa_not-the-token")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), 401);

    // Presenting the stored hash must not work: the whole point of hashing is that a database
    // reader gains nothing they can replay.
    let stored_hash = state
        .store
        .admin_token()
        .get()
        .await
        .unwrap()
        .unwrap()
        .token_hash;
    let as_hash = c
        .get(format!("{pf}/api/whoami"))
        .header("authorization", format!("Bearer {stored_hash}"))
        .send()
        .await
        .unwrap();
    assert_eq!(as_hash.status(), 401, "the hash must not authenticate");

    // The real token still does — the rejections above are not just a broken gate.
    let ok = c
        .get(format!("{pf}/api/whoami"))
        .header("authorization", format!("Bearer {raw}"))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);
}

#[tokio::test]
async fn rotating_the_token_revokes_the_previous_one_immediately() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn_without_admin_token(up).await;
    let first = install_token(&state).await;
    let second = install_token(&state).await;
    let c = reqwest::Client::new();

    let old = c
        .get(format!("{pf}/api/whoami"))
        .header("authorization", format!("Bearer {first}"))
        .send()
        .await
        .unwrap();
    assert_eq!(old.status(), 401, "the rotated-out token must stop working");

    let new = c
        .get(format!("{pf}/api/whoami"))
        .header("authorization", format!("Bearer {second}"))
        .send()
        .await
        .unwrap();
    assert_eq!(new.status(), 200);
}

/// Clearing the token restores the previous posture — that is the documented escape hatch when a
/// token is lost, so it has to actually work.
#[tokio::test]
async fn clearing_the_token_reopens_the_loopback_bypass() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn_without_admin_token(up).await;
    let raw = install_token(&state).await;
    let c = reqwest::Client::new();

    assert_eq!(
        c.get(format!("{pf}/api/whoami"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );

    assert!(state.store.admin_token().clear().await.unwrap());

    assert_eq!(
        c.get(format!("{pf}/api/whoami"))
            .send()
            .await
            .unwrap()
            .status(),
        200,
        "with no credential left, a loopback bind is open again"
    );
    let stale = c
        .get(format!("{pf}/api/whoami"))
        .header("authorization", format!("Bearer {raw}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        stale.status(),
        200,
        "the bypass admits this, but on its own merits — the cleared token is not what let it in"
    );
}

/// A non-loopback deployment has no bypass to close, so a stored token is the whole difference
/// between "disabled" (503) and "sign in" (401 / 200).
#[tokio::test]
async fn a_stored_token_enables_a_non_loopback_dashboard() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn_remote_without_admin_token(up).await;
    let c = reqwest::Client::new();

    let disabled = c.get(format!("{pf}/api/whoami")).send().await.unwrap();
    assert_eq!(disabled.status(), 503, "nothing configured yet");

    let raw = install_token(&state).await;

    let unauthenticated = c.get(format!("{pf}/api/whoami")).send().await.unwrap();
    assert_eq!(
        unauthenticated.status(),
        401,
        "sign-in is now possible, so this is 401 (go sign in), not 503 (nothing configured)"
    );

    let ok = c
        .get(format!("{pf}/api/whoami"))
        .header("authorization", format!("Bearer {raw}"))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);
}

/// `/metrics` shares the same gate, so it must move with the token rather than staying open.
#[tokio::test]
async fn metrics_follows_the_stored_token() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn_without_admin_token(up).await;
    let c = reqwest::Client::new();

    assert_eq!(
        c.get(format!("{pf}/metrics"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let raw = install_token(&state).await;
    assert_eq!(
        c.get(format!("{pf}/metrics"))
            .send()
            .await
            .unwrap()
            .status(),
        401,
        "/metrics must not stay open once a token is configured"
    );
    assert_eq!(
        c.get(format!("{pf}/metrics"))
            .header("authorization", format!("Bearer {raw}"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
}

/// The dashboard needs to state whether a token exists; it must never learn what it is.
#[tokio::test]
async fn capabilities_reports_presence_and_never_the_token() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn_without_admin_token(up).await;
    let c = reqwest::Client::new();

    let before: serde_json::Value = c
        .get(format!("{pf}/api/capabilities"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(before["admin_token_configured"], false);

    let raw = install_token(&state).await;
    let body = c
        .get(format!("{pf}/api/capabilities"))
        .header("authorization", format!("Bearer {raw}"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let after: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(after["admin_token_configured"], true);
    assert!(
        !body.contains(&raw),
        "the response must never carry the token itself"
    );
    let stored_hash = state
        .store
        .admin_token()
        .get()
        .await
        .unwrap()
        .unwrap()
        .token_hash;
    assert!(!body.contains(&stored_hash), "nor its hash");
}
