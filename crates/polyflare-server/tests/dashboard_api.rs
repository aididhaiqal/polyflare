//! Dashboard auth gate: every `/api/*` route sits behind `POLYFLARE_ADMIN_TOKEN`
//! (`Authorization: Bearer <token>`). No token configured ⇒ the dashboard API is disabled (503),
//! not silently open.

mod support;
use futures_util::StreamExt;
use polyflare_server::log_bus::LogEvent;
use support::{spawn, spawn_live_logs, spawn_without_admin_token};

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
async fn whoami_is_503_when_dashboard_disabled() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn_without_admin_token(up).await; // admin_token = None
    let c = reqwest::Client::new();

    let resp = c
        .get(format!("{pf}/api/whoami"))
        .header("authorization", "Bearer whatever")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "no POLYFLARE_ADMIN_TOKEN configured ⇒ dashboard disabled"
    );
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
