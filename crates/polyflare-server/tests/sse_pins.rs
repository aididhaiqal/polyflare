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

/// The other half of the operator flow: an authenticated POST must ACCEPT a real thread id, store
/// it, and land it in the exact settings row the WebSocket gate reads. Paired with
/// `ws_downstream_relay.rs`'s handshake test, which seeds that same key and asserts the 426 —
/// together they cover pin-then-divert without either half assuming the other's spelling.
#[tokio::test]
async fn the_pin_api_stores_what_the_handshake_gate_reads() {
    let upstream = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn_ws_downstream(upstream).await;
    let client = reqwest::Client::new();

    let thread_id = "019f96f4-d4e8-7751-87c9-beba24bb3330";
    let added: serde_json::Value = client
        .post(format!("{pf}/api/ws/sse-pins"))
        .bearer_auth("secret")
        .json(&serde_json::json!({ "thread_id": thread_id }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(added["pinned_threads"], serde_json::json!([thread_id]));

    // The literal row the gate reads. Spelled out rather than taken from the (crate-private)
    // constant so a rename cannot move the write without the handshake test noticing.
    assert_eq!(
        state
            .store
            .settings()
            .get_all()
            .await
            .unwrap()
            .get("ws_transport_sse_pinned_threads")
            .map(String::as_str),
        Some(thread_id),
        "the API must write the row the WebSocket handshake gate reads"
    );

    let removed: serde_json::Value = client
        .delete(format!("{pf}/api/ws/sse-pins/{thread_id}"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        removed["pinned_threads"],
        serde_json::json!([]),
        "unpinning returns the thread to WebSocket"
    );
}
