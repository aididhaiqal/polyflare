//! A thread pinned to SSE must be answered `426` at the WebSocket handshake while every other
//! thread still upgrades — the per-thread equivalent of the global WS on/off switch.

mod support;

use support::spawn_ws_downstream;

// The end-to-end "pinned thread gets 426, sibling gets 101" proof lives in
// `ws_downstream_relay.rs` (`a_pinned_thread_is_diverted_and_its_session_siblings_still_upgrade`),
// whose harness is the one that can complete a real `/responses` handshake. This file covers the
// pin API itself; the two must agree on what an id means, which is exactly what that test asserts.

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
