//! `GET /api/replica/accounts` — the credential feed a replica pulls from its master.
//!
//! This endpoint hands out live OAuth refresh tokens, so the tests that matter most are the ones
//! about who can reach it and what leaks when they cannot.

mod support;

use polyflare_store::PlainTokens;

#[tokio::test]
async fn replica_accounts_requires_admin_auth_and_leaks_nothing_when_refused() {
    let (pf, app_state) = support::spawn("http://127.0.0.1:9".to_string()).await;
    app_state
        .store
        .accounts()
        .update_tokens(
            "acct-1",
            &PlainTokens {
                access_token: "access-secret".into(),
                refresh_token: "refresh-secret".into(),
                id_token: "id-secret".into(),
            },
            &app_state.cipher,
            1_700_000_000,
        )
        .await
        .unwrap();

    let unauthorized = reqwest::Client::new()
        .get(format!("{pf}/api/replica/accounts"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), 401);

    // A refusal must not carry the thing it refused. This route serves refresh tokens, and a
    // refresh token in an error body is the whole account.
    let body = unauthorized.text().await.unwrap();
    assert!(!body.contains("secret"), "401 body must not leak: {body}");
}

#[tokio::test]
async fn replica_accounts_serves_named_fields_and_current_credentials() {
    let (pf, app_state) = support::spawn("http://127.0.0.1:9".to_string()).await;
    app_state
        .store
        .accounts()
        .update_tokens(
            "acct-1",
            &PlainTokens {
                access_token: "access-xyz".into(),
                refresh_token: "refresh-xyz".into(),
                id_token: "id-xyz".into(),
            },
            &app_state.cipher,
            1_700_000_000,
        )
        .await
        .unwrap();

    let response = reqwest::Client::new()
        .get(format!("{pf}/api/replica/accounts"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    // Credentials must not sit in any cache between master and replica.
    let cache_control = response
        .headers()
        .get("cache-control")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        cache_control.contains("no-store"),
        "cache-control: {cache_control}"
    );

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["contract"], 1);
    assert!(body["generated_at"].as_i64().unwrap() > 1_700_000_000);

    let account = body["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["id"] == "acct-1")
        .expect("acct-1 present");

    // Decrypted, because the replica must present these upstream itself.
    assert_eq!(account["access_token"], "access-xyz");
    assert_eq!(account["refresh_token"], "refresh-xyz");
    assert_eq!(account["id_token"], "id-xyz");
    assert_eq!(account["last_refresh"], 1_700_000_000);

    // The point of this endpoint over `.dump`: fields are NAMED, so a column reorder on either
    // side cannot shift a value into the wrong slot.
    for field in [
        "id",
        "provider",
        "email",
        "plan_type",
        "routing_policy",
        "status",
        "auth_mode",
    ] {
        assert!(
            account.get(field).is_some(),
            "contract field `{field}` missing from {account}"
        );
    }
}

#[tokio::test]
async fn replica_accounts_never_ships_continuity_anchors() {
    // Anchors are owned by one account on ONE node. Replicating them would have the replica resume
    // a conversation the master is mid-turn on — exactly the cross-node confusion the anchor
    // machinery exists to prevent. This asserts the omission is deliberate, not incidental.
    let (pf, _) = support::spawn("http://127.0.0.1:9".to_string()).await;
    let body: serde_json::Value = reqwest::Client::new()
        .get(format!("{pf}/api/replica/accounts"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let raw = body.to_string();
    assert!(
        !raw.contains("continuity"),
        "continuity state must never flow master -> replica: {raw}"
    );
}
