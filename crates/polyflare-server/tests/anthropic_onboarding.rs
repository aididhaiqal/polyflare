//! Adding a Claude (Anthropic) account through the dashboard onboarding API.
//!
//! Until this existed the flow was Codex-only: `account_onboarding.rs` hardcoded `provider:
//! "codex"` on every path, and `account_onboarding_flows` had never held a single Anthropic row.
//! The Claude branding, the `claude` account filter and the token-refresh wiring were all present
//! — everything except a way to add one.
//!
//! These tests exercise the API surface and the flow bookkeeping. They stop short of a full token
//! exchange: the Anthropic OAuth client's endpoints are allowlisted to real Anthropic hosts and
//! only overridable through a `#[cfg(test)]` constructor inside its own crate, deliberately so a
//! configuration mistake cannot redirect an authorization code elsewhere. That boundary is worth
//! more than the coverage it costs here.

mod support;
use support::spawn;

async fn start_anthropic(
    client: &reqwest::Client,
    pf: &str,
    body: serde_json::Value,
) -> (reqwest::StatusCode, serde_json::Value) {
    let response = client
        .post(format!("{pf}/api/account-onboarding/codex"))
        .header("authorization", "Bearer secret")
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let value: serde_json::Value = response.json().await.unwrap_or(serde_json::json!({}));
    (status, value)
}

/// The headline: an Anthropic flow can be started at all, and it is recorded as Anthropic rather
/// than silently becoming a Codex flow.
#[tokio::test]
async fn an_anthropic_flow_starts_and_is_recorded_as_anthropic() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await;
    let client = reqwest::Client::new();

    let (status, body) =
        start_anthropic(&client, &pf, serde_json::json!({"provider":"anthropic"})).await;
    assert_eq!(status, 200, "body: {body}");

    let flow_id = body["flow_id"].as_str().expect("a flow id").to_string();
    assert_eq!(body["provider"], "anthropic");
    assert_eq!(
        body["completion"], "paste_code",
        "Anthropic has no loopback redirect, so the operator brings a code back by hand"
    );

    let authorize = body["authorize_url"].as_str().expect("an authorize url");
    assert!(
        authorize.starts_with("https://claude.com/cai/oauth/authorize"),
        "must use the reviewed contract endpoint, got {authorize}"
    );
    assert!(
        authorize.contains("code_challenge="),
        "PKCE is not optional"
    );
    assert!(
        authorize.contains("scope=user%3Ainference") || authorize.contains("scope=user:inference"),
        "only the inference scope is requested, got {authorize}"
    );

    let flow = state
        .store
        .onboarding()
        .get(&flow_id)
        .await
        .unwrap()
        .expect("the flow was persisted");
    assert_eq!(flow.provider, "anthropic");
    assert_eq!(flow.status, "pending");
    assert_eq!(
        flow.redirect_uri.as_deref(),
        Some("https://platform.claude.com/oauth/code/callback"),
        "the redirect is replayed verbatim at exchange, so it is recorded per flow"
    );
    // Ciphertext, not the verifier: the authorize URL carries the CHALLENGE, and a database
    // holding the verifier in the clear beside it would let a reader complete the exchange.
    let challenge = authorize
        .split("code_challenge=")
        .nth(1)
        .and_then(|rest| rest.split('&').next())
        .expect("a challenge in the authorize url");
    assert!(!flow.verifier_enc.is_empty());
    assert!(
        !String::from_utf8_lossy(&flow.verifier_enc).contains(challenge),
        "the stored verifier must not be readable next to its own challenge"
    );
}

/// The response states whether the client registration is verified, so the dialog can explain a
/// rejection instead of blaming the operator's credentials.
#[tokio::test]
async fn the_start_response_reports_client_registration_provenance() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn(up).await;
    let client = reqwest::Client::new();

    let (_, body) =
        start_anthropic(&client, &pf, serde_json::json!({"provider":"anthropic"})).await;
    assert!(
        body["client_id_verified"].is_boolean(),
        "presence is the contract; the value tracks oauth_contract's provenance"
    );
}

/// A default-provider request must keep behaving exactly as before this existed.
#[tokio::test]
async fn omitting_the_provider_still_starts_a_codex_flow() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await;
    let client = reqwest::Client::new();

    let (status, body) = start_anthropic(&client, &pf, serde_json::json!({})).await;
    assert_eq!(status, 200);
    let flow_id = body["flow_id"].as_str().unwrap();
    let flow = state
        .store
        .onboarding()
        .get(flow_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(flow.provider, "codex");
}

#[tokio::test]
async fn an_unknown_provider_is_refused_rather_than_defaulted() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn(up).await;
    let client = reqwest::Client::new();

    let (status, _) = start_anthropic(&client, &pf, serde_json::json!({"provider":"gemini"})).await;
    assert_eq!(
        status, 400,
        "silently onboarding a Codex account for an unrecognised provider would be worse than \
         refusing"
    );
}

/// Device-code onboarding is a Codex capability; Anthropic's contract has no device endpoint.
#[tokio::test]
async fn the_device_flow_refuses_anthropic_instead_of_pretending() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn(up).await;

    let response = reqwest::Client::new()
        .post(format!("{pf}/api/account-onboarding/codex/device"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({"provider":"anthropic"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"], "device_flow_unsupported_for_provider");
}

/// A targeted re-auth must name an account of the same provider — signing into Claude cannot
/// repair a Codex seat, and the mismatch is worth catching before a whole login is spent.
#[tokio::test]
async fn a_cross_provider_reauth_target_is_refused_at_start() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn(up).await;
    let client = reqwest::Client::new();

    // `acct-1` is the harness's seeded CODEX account.
    let (status, _) = start_anthropic(
        &client,
        &pf,
        serde_json::json!({"provider":"anthropic","account_id":"acct-1"}),
    )
    .await;
    assert_eq!(
        status, 404,
        "a codex seat is not an anthropic re-auth target"
    );
}

/// An empty paste must not consume the flow — the operator should be able to try again.
#[tokio::test]
async fn an_empty_code_is_rejected_without_consuming_the_flow() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await;
    let client = reqwest::Client::new();

    let (_, body) =
        start_anthropic(&client, &pf, serde_json::json!({"provider":"anthropic"})).await;
    let flow_id = body["flow_id"].as_str().unwrap().to_string();

    let response = client
        .post(format!("{pf}/api/account-onboarding/{flow_id}/callback"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({"callback_url": "   "}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);

    let flow = state
        .store
        .onboarding()
        .get(&flow_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        flow.status, "pending",
        "a bad paste must leave the flow usable for another attempt"
    );
}

/// The CSRF check: a state value that came back and does not match must stop the exchange.
#[tokio::test]
async fn a_mismatched_state_is_refused() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn(up).await;
    let client = reqwest::Client::new();

    let (_, body) =
        start_anthropic(&client, &pf, serde_json::json!({"provider":"anthropic"})).await;
    let flow_id = body["flow_id"].as_str().unwrap().to_string();

    let response = client
        .post(format!("{pf}/api/account-onboarding/{flow_id}/callback"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({"callback_url": "some-code#not-the-right-state"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"], "state_mismatch");
}
