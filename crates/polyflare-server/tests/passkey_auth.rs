//! Passkey sign-in: the auth-posture transition, the public/gated route split, and session
//! validation. The WebAuthn ceremony itself needs a real authenticator, so these tests exercise
//! everything around it — which is where the security-relevant decisions live.

mod support;

use std::time::{SystemTime, UNIX_EPOCH};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn sha256_hex(raw: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    hex::encode(hasher.finalize())
}

/// Registering a passkey must WITHDRAW the tokenless loopback bypass. This is the entire security
/// point of the feature: until a passkey exists any local process can reach every /api route
/// (credential export included), and afterwards it cannot.
#[tokio::test]
async fn registering_a_passkey_closes_the_local_bypass() {
    let (pf, app_state) =
        support::spawn_without_admin_token("http://127.0.0.1:9".to_string()).await;
    let client = reqwest::Client::new();

    // Before: the loopback bypass admits an unauthenticated caller.
    let before = client
        .get(format!("{pf}/api/accounts"))
        .send()
        .await
        .unwrap();
    assert_eq!(before.status(), 200, "loopback is open before any passkey");

    app_state
        .store
        .passkeys()
        .insert("pk-1", "cred-1", "{}", "Touch ID", now())
        .await
        .unwrap();

    // After: the same request is refused, and refused as 401 (sign in) not 503 (nothing set up).
    let after = client
        .get(format!("{pf}/api/accounts"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        after.status(),
        401,
        "a registered passkey must close the tokenless bypass"
    );
}

/// A live session token authenticates; an expired or unknown one does not.
#[tokio::test]
async fn session_tokens_authenticate_only_while_live() {
    let (pf, app_state) =
        support::spawn_without_admin_token("http://127.0.0.1:9".to_string()).await;
    let repo = app_state.store.passkeys();
    repo.insert("pk-1", "cred-1", "{}", "Touch ID", now())
        .await
        .unwrap();
    let client = reqwest::Client::new();

    repo.create_session(&sha256_hex("live-token"), "pk-1", now(), now() + 3_600)
        .await
        .unwrap();
    repo.create_session(
        &sha256_hex("stale-token"),
        "pk-1",
        now() - 7_200,
        now() - 3_600,
    )
    .await
    .unwrap();

    let ok = client
        .get(format!("{pf}/api/accounts"))
        .header("authorization", "Bearer live-token")
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200, "a live session authenticates");

    for bad in ["stale-token", "never-issued"] {
        let response = client
            .get(format!("{pf}/api/accounts"))
            .header("authorization", format!("Bearer {bad}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 401, "{bad} must not authenticate");
    }
}

/// Revoking a passkey must end the sessions it minted — otherwise "remove the lost device" would
/// leave that device signed in.
#[tokio::test]
async fn deleting_a_passkey_revokes_its_sessions() {
    let (pf, app_state) =
        support::spawn_without_admin_token("http://127.0.0.1:9".to_string()).await;
    let repo = app_state.store.passkeys();
    repo.insert("pk-1", "cred-1", "{}", "Touch ID", now())
        .await
        .unwrap();
    repo.insert("pk-2", "cred-2", "{}", "Phone", now())
        .await
        .unwrap();
    repo.create_session(&sha256_hex("tok"), "pk-1", now(), now() + 3_600)
        .await
        .unwrap();
    let client = reqwest::Client::new();

    assert_eq!(
        client
            .get(format!("{pf}/api/accounts"))
            .header("authorization", "Bearer tok")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );

    let deleted = client
        .delete(format!("{pf}/api/auth/passkeys/pk-1"))
        .header("authorization", "Bearer tok")
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 200);

    assert_eq!(
        client
            .get(format!("{pf}/api/accounts"))
            .header("authorization", "Bearer tok")
            .send()
            .await
            .unwrap()
            .status(),
        401,
        "the revoked passkey's session must stop working"
    );
}

/// Deleting the LAST passkey with no admin token configured would silently reopen the loopback
/// bypass, so it is refused rather than quietly downgrading the dashboard's posture.
#[tokio::test]
async fn the_last_passkey_cannot_be_removed_without_a_break_glass_token() {
    let (pf, app_state) =
        support::spawn_without_admin_token("http://127.0.0.1:9".to_string()).await;
    let repo = app_state.store.passkeys();
    repo.insert("pk-only", "cred-1", "{}", "Touch ID", now())
        .await
        .unwrap();
    repo.create_session(&sha256_hex("tok"), "pk-only", now(), now() + 3_600)
        .await
        .unwrap();

    let response = reqwest::Client::new()
        .delete(format!("{pf}/api/auth/passkeys/pk-only"))
        .header("authorization", "Bearer tok")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 409);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"], "last_passkey_needs_admin_token");
    assert!(
        app_state.store.passkeys().any_registered().await.unwrap(),
        "the passkey must survive the refused delete"
    );
}

/// The sign-in surface must be reachable WITHOUT credentials — a login path behind the gate it
/// opens is unusable — while registration must NOT be.
#[tokio::test]
async fn sign_in_routes_are_public_but_registration_is_gated() {
    let (pf, app_state) =
        support::spawn_without_admin_token("http://127.0.0.1:9".to_string()).await;
    app_state
        .store
        .passkeys()
        .insert("pk-1", "cred-1", "{}", "Touch ID", now())
        .await
        .unwrap();
    let client = reqwest::Client::new();

    // Public: status reports posture to an unauthenticated caller.
    let status = client
        .get(format!("{pf}/api/auth/status"))
        .send()
        .await
        .unwrap();
    assert_eq!(status.status(), 200);
    let body: serde_json::Value = status.json().await.unwrap();
    assert_eq!(body["passkey_registered"], true);
    assert_eq!(
        body["authenticated"], false,
        "the bypass is closed, so an anonymous caller is not authenticated"
    );

    // Public: login/start is reachable unauthenticated (it is the way in).
    let login = client
        .post(format!("{pf}/api/auth/passkey/login/start"))
        .send()
        .await
        .unwrap();
    assert_ne!(
        login.status(),
        401,
        "login/start must not require the credential it issues"
    );

    // Gated: registration from an unauthenticated caller must be refused, or anyone who reached
    // the port could enrol their own authenticator and own the dashboard.
    for path in [
        "/api/auth/passkey/register/start",
        "/api/auth/passkey/register/finish",
    ] {
        let response = client.post(format!("{pf}{path}")).send().await.unwrap();
        assert_eq!(response.status(), 401, "{path} must be admin-gated");
    }
    let list = client
        .get(format!("{pf}/api/auth/passkeys"))
        .send()
        .await
        .unwrap();
    assert_eq!(list.status(), 401, "the passkey list must be admin-gated");
}

/// The admin token remains a break-glass path after passkeys exist, so losing every authenticator
/// is recoverable.
#[tokio::test]
async fn the_admin_token_still_authenticates_after_passkeys_exist() {
    let (pf, app_state) = support::spawn("http://127.0.0.1:9".to_string()).await;
    app_state
        .store
        .passkeys()
        .insert("pk-1", "cred-1", "{}", "Touch ID", now())
        .await
        .unwrap();
    let client = reqwest::Client::new();

    let with_token = client
        .get(format!("{pf}/api/accounts"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(
        with_token.status(),
        200,
        "break-glass token must still work"
    );

    let without = client
        .get(format!("{pf}/api/accounts"))
        .send()
        .await
        .unwrap();
    assert_eq!(without.status(), 401);
}
