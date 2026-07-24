//! **The WS handshake fingerprint-parity GATE** (M5a Task 9) — the same treatment
//! `codex_fingerprint_parity_gate.rs` gives the HTTP `/responses` egress, applied to the NEW
//! egress surface `WsConn::connect` opens (`docs/WS-GROUND-TRUTH-CODEX.md` §1, `docs/
//! DESIGN-DECISIONS.md` E4(a)). Do NOT modify `polyflare_codex::ws::conn` from this file — this
//! gate only CAPTURES and ASSERTS the handshake `WsConn::connect` (T3, already verified) builds.
//!
//! # Capture mechanism
//! `WsConn::connect` is driven, unmodified, at a **raw-TCP local mock** (`spawn_handshake_capture`
//! below) instead of an axum-based one: axum's/`http`'s `HeaderMap` does not preserve wire receipt
//! order (see `polyflare_server::fingerprint_capture`'s "Header-order fidelity" module doc — the
//! same reason the HTTP gate settles for an alphabetically-sorted structural diff instead of an
//! order comparison). Ground truth §1 flags WS handshake header **byte order** as the one thing
//! left unverified; reading the raw bytes off a local TCP socket — before any HTTP-parsing layer
//! reshuffles them — is the only way to observe the true order `tungstenite` puts headers on the
//! wire in per this crate's pinned fork, so this gate captures it directly rather than inheriting
//! the HTTP gate's order-blind limitation.
//!
//! The mock accepts one TCP connection, reads the request up to the blank line terminating the
//! header block, parses the request line + each `Name: value` line **in the exact order and
//! casing received**, then replies with a syntactically complete but deliberately non-101 HTTP
//! response (`400 Bad Request`) so `tungstenite`'s client handshake parser returns promptly
//! instead of hanging — this gate only needs what `WsConn::connect` SENT, never a working
//! connection, so `connect`'s resulting `Err` is expected and ignored.
//!
//! **What the raw capture revealed:** the wire order is deterministic per build but is NOT
//! `build_handshake_request`'s insertion order (see `EXPECTED_WS_HANDSHAKE_HEADER_ORDER`'s doc for
//! the specifics) — `http::HeaderMap`'s internal layout drives it instead. So this gate commits
//! the actually-observed order as a regression guard (a future dependency bump or accidental
//! header change shows up as a diff here), not as proof of insertion-order fidelity or real-codex
//! byte-parity (that remains a live-capture question, per the "Status" section below).
//!
//! # Content safety
//! The mock records header NAMES + VALUES exactly as sent — safe here because every value in this
//! CI-run capture is a synthetic placeholder from this test's own fixture (`"test-token"`, a fake
//! `chatgpt-account-id`, canary turn-metadata ids), never a real credential: this test drives
//! `WsConn::connect` against a local mock, not a live backend, so there is no real secret in play
//! at all. The golden below asserts against these same synthetic values, not against anything
//! captured from `POLYFLARE_CAPTURE_FINGERPRINT` output (no real account was used to derive it —
//! see the module-doc "Status" note below for why).
//!
//! # Status: SPEC-DERIVED, pending live capture-verify
//! Unlike `codex_fingerprint_parity_gate.rs` (CAPTURE-VERIFIED against a real `codex-cli 0.144.4`
//! wire capture), this golden has NOT yet been diffed against a real Codex CLI's live WS
//! handshake — no live capture was performed for this task (see the task report,
//! `.superpowers/sdd/m5a-task-9-report.md`, for why). It is derived from `WS-GROUND-TRUTH-CODEX.md`
//! §1/§7.1's from-source header contract, applied to `WsConn::connect`'s own (already T3-verified)
//! construction logic, and cross-checked against this test's own local capture of what
//! `WsConn::connect` actually sends. A future live capture via `scripts/codex-polyflare` +
//! `POLYFLARE_CAPTURE_FINGERPRINT` should update this doc comment to CAPTURE-VERIFIED once run.

use std::io::{Read, Write};
use std::net::TcpListener as StdTcpListener;
use std::sync::{Arc, Mutex};

use polyflare_codex::codex_headers::{
    codex_user_agent, conversation_key, originator, TurnIdentity, CODEX_CLI_VERSION,
};
use polyflare_codex::ws::WsConn;
use polyflare_core::Account;

/// One header exactly as received on the wire: literal name casing + value, in receipt order.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RawHeader {
    name: String,
    value: String,
}

/// Ground truth §7.1: MUST be absent from the WS handshake — it travels only inside
/// `client_metadata`, never as a header, unlike the HTTP path.
const TURN_STATE_HEADER: &str = "x-codex-turn-state";

/// Ground truth §1 (`client.rs:1092-1095`): `.insert()`'d exactly once, exact value, always.
const OPENAI_BETA_WS: &str = "responses_websockets=2026-02-06";

/// A canary bearer/account-id: distinctive enough that if either leaked anywhere it could only
/// have come from this test's own synthetic fixture — never a real secret (see module doc).
const FAKE_BEARER: &str = "test-token";
const FAKE_ACCOUNT_ID: &str = "acct-canary-should-never-be-a-real-id";

/// Spawn a raw-TCP handshake-capturing mock: accepts exactly one connection, records the request
/// line + every header in receipt order/casing, then answers with a complete non-101 HTTP
/// response so the client's handshake attempt returns (with an `Err`, expected and ignored by the
/// caller) instead of hanging. Returns the `ws://` base URL and a handle to read the capture back.
fn spawn_handshake_capture() -> (String, Arc<Mutex<Option<Vec<RawHeader>>>>) {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind local capture listener");
    listener.set_nonblocking(false).unwrap();
    let addr = listener.local_addr().unwrap();
    let captured: Arc<Mutex<Option<Vec<RawHeader>>>> = Arc::new(Mutex::new(None));
    let captured_writer = Arc::clone(&captured);

    std::thread::spawn(move || {
        // Exactly one connection expected — the single `WsConn::connect` attempt this gate drives.
        let (mut stream, _peer) = match listener.accept() {
            Ok(pair) => pair,
            Err(_) => return,
        };

        // Read until the blank line terminating the header block. A WS handshake request has no
        // body, so reading up to "\r\n\r\n" captures the complete request.
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => break,
            }
        }

        let text = String::from_utf8_lossy(&buf);
        let mut lines = text.split("\r\n");
        let _request_line = lines.next(); // "GET /responses HTTP/1.1" — not asserted here.
        let mut headers = Vec::new();
        for line in lines {
            if line.is_empty() {
                break;
            }
            if let Some((name, value)) = line.split_once(':') {
                headers.push(RawHeader {
                    name: name.to_string(),
                    value: value.trim_start().to_string(),
                });
            }
        }
        *captured_writer.lock().unwrap() = Some(headers);

        // A complete, syntactically valid, deliberately non-101 response: `tungstenite`'s client
        // handshake parser reads this and returns `Err(Error::Http(_))` promptly rather than
        // blocking for more bytes. This gate never needs a working connection.
        let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\n\r\n");
        let _ = stream.flush();
    });

    (format!("ws://{addr}"), captured)
}

/// Poll `captured` for up to ~2s (the local capture thread runs concurrently with `connect`'s
/// await) and return the recorded headers once the capture thread has written them.
async fn wait_for_capture(captured: &Arc<Mutex<Option<Vec<RawHeader>>>>) -> Vec<RawHeader> {
    for _ in 0..200 {
        if let Some(headers) = captured.lock().unwrap().clone() {
            return headers;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("handshake capture mock never recorded a request within 2s");
}

/// Build the `forward_headers` a real ingress would hand `WsConn::connect`: the same
/// codex-identity set the HTTP gate synthesizes (`codex_fingerprint_parity_gate.rs`), PLUS
/// `x-codex-turn-state` — present here specifically to prove `WsConn::connect` strips it (ground
/// truth §7.1), the one WS-specific divergence from the HTTP path.
fn forward_headers_with_turn_state() -> Vec<(String, String)> {
    let body = serde_json::json!({
        "model": "gpt-5.6-sol",
        "input": "hello",
        "prompt_cache_key": "conversation-ws-gate-canary",
    });
    let identity = TurnIdentity::derive(&conversation_key(&body));
    vec![
        (
            "user-agent".to_string(),
            codex_user_agent(CODEX_CLI_VERSION),
        ),
        ("originator".to_string(), originator().to_string()),
        ("accept".to_string(), "text/event-stream".to_string()),
        ("session-id".to_string(), identity.session_id.clone()),
        ("thread-id".to_string(), identity.thread_id.clone()),
        (
            "x-client-request-id".to_string(),
            identity.thread_id.clone(),
        ),
        ("x-codex-window-id".to_string(), identity.window_id.clone()),
        (
            "x-codex-turn-metadata".to_string(),
            identity.turn_metadata_json(),
        ),
        // Must be stripped by `WsConn::connect` — ground truth §7.1. If this ever leaks through,
        // `turn_state_header_is_absent_from_the_ws_handshake` below fails.
        (
            TURN_STATE_HEADER.to_string(),
            "some-server-issued-value".to_string(),
        ),
    ]
}

/// Lowercased header names in receipt order, for name-set/order assertions.
fn lower_names(headers: &[RawHeader]) -> Vec<String> {
    headers
        .iter()
        .map(|h| h.name.to_ascii_lowercase())
        .collect()
}

fn find<'a>(headers: &'a [RawHeader], name: &str) -> Option<&'a RawHeader> {
    headers.iter().find(|h| h.name.eq_ignore_ascii_case(name))
}

async fn capture_ws_handshake() -> Vec<RawHeader> {
    let (base, captured) = spawn_handshake_capture();
    let account = Account {
        id: "ws-fingerprint-gate".into(),
        base_url: base,
        bearer_token: FAKE_BEARER.into(),
        chatgpt_account_id: Some(FAKE_ACCOUNT_ID.into()),
        is_fedramp: false,
    };
    let forward_headers = forward_headers_with_turn_state();

    // The mock never completes a real 101 upgrade (see `spawn_handshake_capture`'s doc) — an
    // `Err` here is expected and carries no information this gate needs; only what was SENT
    // matters.
    let _ = WsConn::connect(&account, &forward_headers).await;

    wait_for_capture(&captured).await
}

/// The codex-identity + WS-protocol header NAMES a real handshake carries, per ground truth §1 —
/// this gate's golden. Unlike the HTTP gate's SUPERSET check (reqwest/hyper add transport auto
/// headers unpredictably), this is an EXACT set: `tungstenite`'s raw handshake writer has no
/// equivalent auto-header surprise, so an exact match is both possible and meaningful here.
const EXPECTED_WS_HANDSHAKE_HEADER_NAMES: &[&str] = &[
    "host",
    "connection",
    "upgrade",
    "sec-websocket-version",
    "sec-websocket-key",
    "sec-websocket-extensions",
    "user-agent",
    "originator",
    "accept",
    "session-id",
    "thread-id",
    "x-client-request-id",
    "x-codex-window-id",
    "x-codex-turn-metadata",
    "authorization",
    "chatgpt-account-id",
    "openai-beta",
];

#[tokio::test]
async fn ws_handshake_header_set_matches_the_from_source_golden() {
    let headers = capture_ws_handshake().await;
    let names = lower_names(&headers);

    let mut sorted_actual = names.clone();
    sorted_actual.sort_unstable();
    sorted_actual.dedup();
    let mut sorted_expected: Vec<String> = EXPECTED_WS_HANDSHAKE_HEADER_NAMES
        .iter()
        .map(|s| s.to_string())
        .collect();
    sorted_expected.sort_unstable();

    assert_eq!(
        sorted_actual, sorted_expected,
        "WS handshake header NAME set diverged from the from-source golden.\nactual (in receipt \
         order): {names:?}"
    );
}

#[tokio::test]
async fn turn_state_header_is_absent_from_the_ws_handshake() {
    // Ground truth §7.1: the single WS-specific divergence from the HTTP path — `x-codex-turn-
    // state` is fed to `WsConn::connect` via `forward_headers` (as a real ingress would forward
    // whatever a prior turn's server-supplied value was) and MUST be stripped before the wire,
    // since on WS it only ever travels inside a frame's `client_metadata`.
    let headers = capture_ws_handshake().await;
    assert!(
        find(&headers, TURN_STATE_HEADER).is_none(),
        "x-codex-turn-state MUST be absent from the WS handshake (ground truth §7.1); found: \
         {headers:?}"
    );
}

#[tokio::test]
async fn openai_beta_is_present_exactly_once_with_the_exact_ws_value() {
    let headers = capture_ws_handshake().await;
    let matches: Vec<&RawHeader> = headers
        .iter()
        .filter(|h| h.name.eq_ignore_ascii_case("openai-beta"))
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "OpenAI-Beta must appear exactly once (ground truth §7.2 — `.insert()`'d, never \
         appended): found {matches:?}"
    );
    assert_eq!(
        matches[0].value, OPENAI_BETA_WS,
        "OpenAI-Beta must carry the exact WS beta value"
    );
}

#[tokio::test]
async fn permessage_deflate_is_offered() {
    // Ground truth §1: "`permessage-deflate` IS offered ... omitting the offer is a detectable
    // handshake difference." The M5a fork adoption makes this real (module doc of
    // `polyflare_codex::ws::conn`); this gate pins it so a future accidental regression (e.g. a
    // config change that drops the extension offer) is caught here rather than in production.
    let headers = capture_ws_handshake().await;
    let ext = find(&headers, "sec-websocket-extensions")
        .unwrap_or_else(|| panic!("Sec-WebSocket-Extensions header missing: {headers:?}"));
    assert!(
        ext.value.contains("permessage-deflate"),
        "Sec-WebSocket-Extensions must offer permessage-deflate: {ext:?}"
    );
}

/// The exact wire-receipt order this gate's raw-TCP capture observes today, for the fixed
/// `forward_headers_with_turn_state()` fixture. **This is NOT `build_handshake_request`'s
/// insertion order** — empirically (see this test's own history: an earlier revision asserted
/// `authorization < chatgpt-account-id < openai-beta` on the theory that insertion order would
/// hold, and that assertion FAILED against this exact capture) `http::HeaderMap`'s iteration/wire
/// order is driven by its internal hash-bucket layout, not append order: `openai-beta` (inserted
/// LAST in code) is written before `authorization` and `chatgpt-account-id` (inserted earlier),
/// and the forwarded-header block itself does not come out in its original sequence either. This
/// matches `polyflare_server::fingerprint_capture`'s own "Header-order fidelity" caveat for the
/// HTTP gate (`HeaderMap`'s order is documented as unspecified) — it turns out to apply here too,
/// just discovered empirically instead of assumed up front. What IS true, confirmed by running
/// this capture repeatedly: the order is **deterministic for a given build** of this crate (same
/// header name set in, same order out, every time) — so committing the exact observed sequence as
/// a golden is still meaningful: it can't verify byte-parity with the real Codex CLI's wire order
/// (a different, still-unverified question — see the module doc's "Status" note), but it DOES
/// catch a regression: a dependency bump or an accidental extra/missing/reordered header changes
/// this list, and CI fails instead of the drift going unnoticed until production.
const EXPECTED_WS_HANDSHAKE_HEADER_ORDER: &[&str] = &[
    "host",
    "connection",
    "upgrade",
    "sec-websocket-version",
    "sec-websocket-key",
    "openai-beta",
    "chatgpt-account-id",
    "authorization",
    "x-codex-turn-metadata",
    "x-codex-window-id",
    "user-agent",
    "originator",
    "accept",
    "session-id",
    "thread-id",
    "x-client-request-id",
    "sec-websocket-extensions",
];

#[tokio::test]
async fn ws_handshake_header_order_matches_the_captured_golden() {
    let headers = capture_ws_handshake().await;
    let names = lower_names(&headers);
    assert_eq!(
        names, EXPECTED_WS_HANDSHAKE_HEADER_ORDER,
        "WS handshake wire order diverged from the committed golden (see \
         EXPECTED_WS_HANDSHAKE_HEADER_ORDER's doc — this is a regression guard, not an \
         insertion-order or real-codex-parity claim)"
    );
}

/// The content-safety guarantee, proven directly (mirrors
/// `fingerprint_capture.rs`'s own canary test): grep the fully captured raw request text for every
/// fake secret/id value used above — none may ever be a REAL value, and this test only ever uses
/// synthetic placeholders, never a real bearer/account-id. This is not a redaction test (this
/// gate's mock legitimately records the synthetic values verbatim, unlike
/// `fingerprint_capture.rs`'s production redaction path) — it is a guardrail that this test file
/// itself never accidentally starts threading a real credential through.
#[tokio::test]
async fn no_value_captured_here_is_anything_but_a_synthetic_placeholder() {
    let headers = capture_ws_handshake().await;
    let auth = find(&headers, "authorization").expect("authorization header present");
    assert_eq!(auth.value, format!("Bearer {FAKE_BEARER}"));
    let account_id = find(&headers, "chatgpt-account-id").expect("chatgpt-account-id present");
    assert_eq!(account_id.value, FAKE_ACCOUNT_ID);
}
