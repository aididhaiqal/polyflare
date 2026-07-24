//! Dashboard auth gate: a configured `POLYFLARE_ADMIN_TOKEN` protects every `/api/*` route.
//! Without one, loopback-bound deployments are open for zero-config local use while non-loopback
//! deployments remain disabled.

mod support;
use futures_util::StreamExt;
use polyflare_server::log_bus::LogEvent;
use support::{
    spawn, spawn_live_logs, spawn_remote_without_admin_token, spawn_without_admin_token,
};

#[tokio::test]
async fn whoami_requires_admin_token() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn(up).await; // admin_token = Some("secret")
    let c = reqwest::Client::new();

    let no_tok = c.get(format!("{pf}/api/whoami")).send().await.unwrap();
    assert_eq!(no_tok.status(), 401);

    let ok = c
        .get(format!("{pf}/api/whoami"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);
}

#[tokio::test]
async fn capabilities_reports_live_logs_flag() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _s) = spawn(up).await; // spawn sets live_logs = true for tests
    let c = reqwest::Client::new();

    let no_tok = c
        .get(format!("{pf}/api/capabilities"))
        .send()
        .await
        .unwrap();
    assert_eq!(no_tok.status(), 401, "must be behind admin auth");

    let r = c
        .get(format!("{pf}/api/capabilities"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let v: serde_json::Value = r.json().await.unwrap();
    assert_eq!(v["live_logs"], true);
}

#[tokio::test]
async fn whoami_is_open_without_a_token_on_loopback() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn_without_admin_token(up).await; // admin_token = None
    let c = reqwest::Client::new();

    let resp = c.get(format!("{pf}/api/whoami")).send().await.unwrap();
    assert_eq!(
        resp.status(),
        200,
        "loopback dashboard should open directly"
    );
}

#[tokio::test]
async fn whoami_stays_disabled_without_a_token_on_non_loopback_bind() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn_remote_without_admin_token(up).await;

    let resp = reqwest::Client::new()
        .get(format!("{pf}/api/whoami"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "remote management must never open implicitly"
    );
}

#[tokio::test]
async fn overview_series_requires_admin_token() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn(up).await; // admin_token = Some("secret")
    let c = reqwest::Client::new();

    let no_tok = c
        .get(format!("{pf}/api/overview/series"))
        .send()
        .await
        .unwrap();
    assert_eq!(no_tok.status(), 401);

    let ok = c
        .get(format!("{pf}/api/overview/series"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);
}

#[tokio::test]
async fn logs_stream_200_and_streams_backfill_when_flag_on() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await; // spawn sets live_logs = true for tests

    // Publish before connecting so this event lands in the backfill snapshot.
    state.log_bus.publish(LogEvent::info("test", "hello"));

    let r = reqwest::Client::new()
        .get(format!("{pf}/api/logs/stream"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["content-type"], "text/event-stream");

    let mut stream = r.bytes_stream();
    let chunk = stream.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&chunk).contains("hello"));
}

#[tokio::test]
async fn logs_stream_is_404_when_flag_off() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn_live_logs(up, false).await;

    let r = reqwest::Client::new()
        .get(format!("{pf}/api/logs/stream"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404, "POLYFLARE_LIVE_LOGS off ⇒ 404");
}

/// Task 12: drives one real `/responses` request through the app (against a `MockUpstream`
/// returning a `response.completed` event) and asserts the persisted `request_log` row — read back
/// via the auth'd `/api/requests` endpoint — carries the content-free `account_id`/`model` metrics
/// the ingress chokepoint (`crate::ingress`) is supposed to fill in from data it already has (the
/// selected account + the requested model), not `None`.
#[tokio::test]
async fn responses_request_persists_account_and_model_metrics() {
    let mock =
        polyflare_testkit::MockUpstream::new(vec![r#"{"type":"response.completed"}"#.to_string()]);
    let upstream = mock.spawn().await;
    let (pf, state) = spawn(upstream).await; // seeds active account "acct-1", admin_token = Some("secret")
    let (_backfill, mut live_logs) = state.log_bus.subscribe();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/responses"))
        .header("session_id", "dashboard-session-a")
        .header("x-openai-subagent", "review")
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Drain the SSE response to completion — the persist happens after the handler returns the
    // stream, so the client must finish reading it for the persist operation to have been queued.
    let mut stream = resp.bytes_stream();
    while stream.next().await.is_some() {}

    // The persist is handled by the bounded background writer, so poll
    // `/api/requests` a few times rather than assuming the row is already there.
    let mut rows = Vec::new();
    for _ in 0..20 {
        let r = client
            .get(format!("{pf}/api/requests?limit=1"))
            .header("authorization", "Bearer secret")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        let v: serde_json::Value = r.json().await.unwrap();
        if let Some(arr) = v["rows"].as_array() {
            if !arr.is_empty() {
                rows = arr.clone();
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert!(!rows.is_empty(), "expected a persisted request_log row");
    assert_eq!(rows[0]["account_id"], "acct-1");
    assert_eq!(rows[0]["model"], "gpt-5.6-sol");
    assert!(
        rows[0]["request_id"]
            .as_str()
            .is_some_and(|request_id| request_id.len() == 32),
        "dashboard API should expose the generated 128-bit correlation id"
    );
    let request_id = rows[0]["request_id"].as_str().unwrap();
    let session_key = rows[0]["session_key"]
        .as_str()
        .expect("request row should expose the derived session hash");
    assert_eq!(session_key.len(), 64);
    assert_eq!(rows[0]["subagent"], "review");
    let live_event = tokio::time::timeout(std::time::Duration::from_secs(1), live_logs.recv())
        .await
        .expect("request event should reach the live log bus")
        .expect("live log channel should stay open");
    assert_eq!(
        live_event.request_id.as_deref(),
        Some(request_id),
        "live logs and durable request history must expose the same correlation id"
    );
    assert_eq!(live_event.session_key.as_deref(), Some(session_key));
    assert_eq!(live_event.subagent.as_deref(), Some("review"));
    assert_eq!(
        rows[0]["transport"], "sse",
        "a streamed HTTP response should be identified as SSE in dashboard telemetry"
    );
}
