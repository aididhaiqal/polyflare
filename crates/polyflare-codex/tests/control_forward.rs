//! D17 Task 1: failing-first tests for the generic parameterized-path UNARY control forward
//! primitive (`polyflare_codex::control_forward`). Exercises it against a live
//! `polyflare_testkit::MockControlUpstream`, asserting real values (path, headers, status, body,
//! header filtering) rather than just "did it not panic".

use bytes::Bytes;
use polyflare_core::Account;
use polyflare_testkit::MockControlUpstream;

/// Builds an `Account` whose `base_url` has the SAME shape PolyFlare actually configures in
/// production — already ending in `/codex` (see `control_forward.rs`'s module doc: the real
/// default is `https://chatgpt.com/backend-api/codex`). `mock_base` is the mock's bare
/// `http://host:port`; `MockControlUpstream` serves `/codex/*path` and `/wham/*path` directly off
/// its root (mirroring the plan's literal `{base}/codex/<path>` spec), so `base_url` here is
/// `{mock_base}/codex` — the `/backend-api`-stripping normalization itself is covered separately
/// by `control_url`'s own unit tests against the real `.../backend-api/codex` literal.
fn account_for(mock_base: &str) -> Account {
    Account {
        id: "acct-1".to_string(),
        base_url: format!("{mock_base}/codex"),
        bearer_token: "the-account-bearer-token".to_string(),
        chatgpt_account_id: Some("chatgpt-acct-42".to_string()),
    }
}

#[tokio::test]
async fn post_forwards_to_codex_path_with_bearer_and_account_id() {
    let mock = MockControlUpstream::new(200, r#"{"status":"summarized"}"#);
    let handle = mock.clone();
    let base = mock.spawn().await;
    let account = account_for(&base);
    let client = reqwest::Client::new();

    let resp = polyflare_codex::control_forward(
        &client,
        &account,
        "memories/trace_summarize",
        reqwest::Method::POST,
        &[],
        Some(Bytes::from(r#"{"trace":"sentinel-body"}"#)),
    )
    .await
    .expect("control_forward should succeed against a live mock");

    assert_eq!(resp.status, 200);
    assert_eq!(resp.body.as_ref(), br#"{"status":"summarized"}"#);

    let recorded = handle
        .last_request()
        .expect("mock should have recorded a request");
    assert_eq!(recorded.method, "POST");
    assert_eq!(recorded.path, "/codex/memories/trace_summarize");
    assert_eq!(
        recorded
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok()),
        Some("Bearer the-account-bearer-token")
    );
    assert_eq!(
        recorded
            .headers
            .get("chatgpt-account-id")
            .and_then(|v| v.to_str().ok()),
        Some("chatgpt-acct-42")
    );
    assert_eq!(recorded.body.as_ref(), br#"{"trace":"sentinel-body"}"#);
}

#[tokio::test]
async fn response_headers_are_filtered_to_the_allow_set() {
    let mock = MockControlUpstream::new(200, "{}")
        .with_header("etag", "W/\"abc123\"")
        .with_header("x-internal-secret", "shh-do-not-leak")
        .with_header("set-cookie", "session=leaked");
    let base = mock.spawn().await;
    let account = account_for(&base);
    let client = reqwest::Client::new();

    let resp = polyflare_codex::control_forward(
        &client,
        &account,
        "memories/trace_summarize",
        reqwest::Method::POST,
        &[],
        Some(Bytes::from("{}")),
    )
    .await
    .expect("control_forward should succeed");

    let names: Vec<&str> = resp.headers.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        names.contains(&"etag"),
        "allow-listed header `etag` must survive filtering: {names:?}"
    );
    assert!(
        !names.contains(&"x-internal-secret"),
        "non-allow-listed header `x-internal-secret` must be dropped: {names:?}"
    );
    assert!(
        !names.contains(&"set-cookie"),
        "non-allow-listed header `set-cookie` must be dropped: {names:?}"
    );
}

#[tokio::test]
async fn wham_path_joins_without_a_codex_segment() {
    let mock = MockControlUpstream::new(200, r#"{"keys":[]}"#);
    let handle = mock.clone();
    let base = mock.spawn().await;
    let account = account_for(&base);
    let client = reqwest::Client::new();

    let resp = polyflare_codex::control_forward(
        &client,
        &account,
        "wham/agent-identities/jwks",
        reqwest::Method::GET,
        &[],
        None,
    )
    .await
    .expect("control_forward should succeed for a wham path");

    assert_eq!(resp.status, 200);
    let recorded = handle.last_request().expect("recorded request");
    assert_eq!(recorded.path, "/wham/agent-identities/jwks");
    assert!(
        !recorded.path.contains("/codex/"),
        "wham path must not be nested under /codex/: {}",
        recorded.path
    );
}

#[tokio::test]
async fn get_with_no_body_works() {
    let mock = MockControlUpstream::new(200, r#"{"goal":null}"#);
    let handle = mock.clone();
    let base = mock.spawn().await;
    let account = account_for(&base);
    let client = reqwest::Client::new();

    let resp = polyflare_codex::control_forward(
        &client,
        &account,
        "thread/goal/get",
        reqwest::Method::GET,
        &[],
        None,
    )
    .await
    .expect("GET with no body should succeed");

    assert_eq!(resp.status, 200);
    assert_eq!(resp.body.as_ref(), br#"{"goal":null}"#);
    let recorded = handle.last_request().expect("recorded request");
    assert_eq!(recorded.method, "GET");
    assert!(recorded.body.is_empty());
}

#[tokio::test]
async fn transport_failure_returns_a_typed_error_not_a_panic() {
    // A syntactically valid but non-resolving host — the same pattern
    // `polyflare-server/src/watchdog.rs`'s own tests use for an unreachable base_url.
    let account = Account {
        id: "acct-down".to_string(),
        base_url: "http://unused.invalid/codex".to_string(),
        bearer_token: "tok".to_string(),
        chatgpt_account_id: None,
    };
    let client = reqwest::Client::new();

    let result = polyflare_codex::control_forward(
        &client,
        &account,
        "memories/trace_summarize",
        reqwest::Method::POST,
        &[],
        Some(Bytes::from("{}")),
    )
    .await;

    assert!(
        matches!(result, Err(polyflare_codex::ControlError::Transport(_))),
        "expected a Transport error, got: {result:?}"
    );
}
