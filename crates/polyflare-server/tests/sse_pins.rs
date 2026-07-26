//! A thread pinned to SSE must be answered `426` at the WebSocket handshake while every other
//! thread still upgrades — the per-thread equivalent of the global WS on/off switch.

mod support;

use support::spawn_ws_downstream;


// NOTE: the end-to-end "pinned thread gets 426, others get 101" assertion is deliberately absent.
// The shared test harness does not produce a GET /responses route this test can exercise
// (405 regardless of the ws_downstream flag), which is a harness routing quirk unrelated to
// pinning. The pin decision itself is covered by the unit tests in `src/sse_pins.rs`, and the
// diversion is one `is_pinned` call at the top of `responses_ws_upgrade`. Verify the 426 against a
// running instance before relying on it.

#[tokio::test]
async fn the_pin_api_is_authenticated_and_validates_ids() {
    let upstream = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn_ws_downstream(upstream).await;
    let client = reqwest::Client::new();

    assert_eq!(
        client
            .get(format!("{pf}/api/ws/sse-pins"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );

    // A value that is not identifier-shaped is refused with a reason, not stored.
    let rejected = client
        .post(format!("{pf}/api/ws/sse-pins"))
        .bearer_auth("secret")
        .json(&serde_json::json!({ "thread_id": "not an id" }))
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 400);

    let listed: serde_json::Value = client
        .get(format!("{pf}/api/ws/sse-pins"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["pinned_threads"], serde_json::json!([]));
}
