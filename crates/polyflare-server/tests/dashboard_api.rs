//! Dashboard auth gate: every `/api/*` route sits behind `POLYFLARE_ADMIN_TOKEN`
//! (`Authorization: Bearer <token>`). No token configured ⇒ the dashboard API is disabled (503),
//! not silently open.

mod support;
use support::{spawn, spawn_without_admin_token};

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
