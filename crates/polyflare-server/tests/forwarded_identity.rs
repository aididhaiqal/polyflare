//! Operator attribution from a reverse proxy's identity header.
//!
//! The security property under test is narrow and important: the header is believed ONLY when the
//! listener is loopback-bound, because that is the one arrangement where the sole path to the
//! server is a local proxy that sets it. On any other bind a remote caller could set it themselves
//! and forge an audit entry naming someone else.

mod support;

use polyflare_server::identity::{actor_label, forwarded_identity};

fn headers(login: Option<&str>) -> axum::http::HeaderMap {
    let mut headers = axum::http::HeaderMap::new();
    if let Some(login) = login {
        headers.insert(
            "tailscale-user-login",
            axum::http::HeaderValue::from_str(login).unwrap(),
        );
    }
    headers
}

#[test]
fn a_non_loopback_bind_refuses_to_believe_the_header() {
    let forged = headers(Some("attacker@example.test"));
    assert_eq!(
        forwarded_identity(&forged, false),
        None,
        "a remote caller must not be able to name an actor"
    );
    assert_eq!(actor_label(&forged, false), "unattributed");
}

/// Behind a local proxy the identity is used, and its absence is reported truthfully rather than
/// guessed at.
#[test]
fn a_loopback_bind_attributes_the_action() {
    let real = headers(Some("sam@example.test"));
    assert_eq!(actor_label(&real, true), "sam@example.test");
    assert_eq!(actor_label(&headers(None), true), "unattributed");
}

/// The audited operations still work when no proxy is in front — attribution is additive, never a
/// precondition for the operation itself.
#[tokio::test]
async fn an_audited_operation_succeeds_without_any_forwarded_identity() {
    let (pf, app_state) = support::spawn("http://127.0.0.1:9".to_string()).await;
    sqlx::query("UPDATE accounts SET chatgpt_account_id = 'chatgpt-1' WHERE id = 'acct-1'")
        .execute(app_state.store.pool())
        .await
        .unwrap();

    let response = reqwest::Client::new()
        .post(format!("{pf}/api/accounts/acct-1/export-auth"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "export must not require a proxy identity to work"
    );
}

/// A caller-supplied header reaching a non-loopback deployment must not appear in the audit trail.
/// The test harness resolves its posture as non-loopback, so this is the forgeable case.
#[tokio::test]
async fn a_client_supplied_identity_is_ignored_on_a_forgeable_deployment() {
    let (pf, app_state) = support::spawn("http://127.0.0.1:9".to_string()).await;
    assert!(
        !app_state.trust_forwarded_identity,
        "the harness must model a deployment where the header is forgeable"
    );
    sqlx::query("UPDATE accounts SET chatgpt_account_id = 'chatgpt-1' WHERE id = 'acct-1'")
        .execute(app_state.store.pool())
        .await
        .unwrap();

    let response = reqwest::Client::new()
        .post(format!("{pf}/api/accounts/acct-1/export-auth"))
        .header("authorization", "Bearer secret")
        .header("tailscale-user-login", "attacker@example.test")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    // The operation is allowed (the bearer authorised it); what must NOT happen is the forged
    // name being recorded as the actor.
    assert_eq!(
        actor_label(
            &{
                let mut h = axum::http::HeaderMap::new();
                h.insert(
                    "tailscale-user-login",
                    axum::http::HeaderValue::from_static("attacker@example.test"),
                );
                h
            },
            app_state.trust_forwarded_identity
        ),
        "unattributed"
    );
}
