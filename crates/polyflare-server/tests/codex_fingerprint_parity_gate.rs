//! **The Codex egress fingerprint-parity GATE** (U-header-parity milestone).
//!
//! Captures PolyFlare's own outbound `POST /responses` header STRUCTURE (via
//! `CodexExecutor::execute` driven at a `MockUpstream` that records the request it received) and
//! diffs it against [`EXPECTED_CODEX_IDENTITY_HEADER_NAMES`] / [`EXPECTED_TURN_METADATA_KEYS`] —
//! an "expected codex-rs egress fingerprint" golden built entirely from a local `openai/codex`
//! source read (see `polyflare_codex::executor` and `polyflare_codex::codex_headers` module docs
//! for the exact files/functions cited: `login/src/auth/default_client.rs`,
//! `codex-api/src/endpoint/responses.rs`, `codex-api/src/requests/headers.rs`,
//! `core/src/responses_metadata.rs`, `core/src/session/mod.rs`).
//!
//! # Status: CAPTURE-VERIFIED (codex-cli 0.144.4, 2026-07-15)
//! This golden has been diffed against a live wire capture of the real Codex CLI (`codex-cli
//! 0.144.4`), obtained by routing a `scripts/codex-polyflare` run through
//! `POLYFLARE_CAPTURE_FINGERPRINT` (see [`polyflare_server::fingerprint_capture`]). The capture
//! CONFIRMED [`EXPECTED_CODEX_IDENTITY_HEADER_NAMES`], [`EXPECTED_TURN_METADATA_KEYS`], and the UA
//! format. It also surfaced two headers a real codex sends CONDITIONALLY, both deliberately absent
//! from the always-present expected set (and validly absent from PolyFlare's synthesis — see
//! `polyflare_codex::codex_headers` module doc): `x-codex-beta-features` (present only when the
//! session enables experimental features) and `x-openai-internal-codex-responses-lite: true`
//! (present only for responses-lite models). Because this gate asserts the captured egress is a
//! SUPERSET of the always-present set, those optional headers do not belong in it; the native
//! forward path relays them verbatim when a real client sends them.
//!
//! # What this gate checks (and what it deliberately does NOT)
//! - Checks: PolyFlare's captured egress header-NAME set is a SUPERSET of
//!   [`EXPECTED_CODEX_IDENTITY_HEADER_NAMES`] (see that const's doc for why superset, not exact
//!   equality); the `x-codex-turn-metadata` JSON field-KEY set matches
//!   [`EXPECTED_TURN_METADATA_KEYS`] exactly; the `user-agent` header's masked FORMAT shape
//!   matches codex-rs's `{originator}/{version} ({os...}; {arch}) {terminal}` structure.
//! - Does NOT check header ORDER: `capture_request_fingerprint` sorts headers alphabetically
//!   because `axum`'s `HeaderMap` does not preserve wire receipt order (see
//!   `polyflare_server::fingerprint_capture` module docs, "Header-order fidelity").
//! - Does NOT check exact id VALUES (only presence/shape). The codex-rs release version string
//!   (`polyflare_codex::codex_headers::CODEX_CLI_VERSION`) is now capture-verified to `0.144.4`.
//!
//! This test must FAIL before the executor sets these headers and PASS after.

use std::collections::BTreeSet;

use futures_util::StreamExt;
use polyflare_codex::CodexExecutor;
use polyflare_core::{Account, Executor, PreparedRequest};
use polyflare_server::fingerprint_capture::capture_request_fingerprint;
use polyflare_testkit::MockUpstream;

/// The codex-identity header NAMES a real codex-rs turn carries on `POST /responses`, verified
/// from source (see module doc for the exact files). PolyFlare's captured egress header-name set
/// must be a SUPERSET of this, not an exact match: reqwest/hyper add transport-level auto headers
/// (`content-type`, `content-length`, `host`, `accept-encoding`, ...) that a real codex-rs's own
/// reqwest-based transport would *also* add — those say nothing about the codex-identity headers
/// this milestone is about, and asserting exact equality against them would make this gate
/// fragile against reqwest/hyper version changes rather than meaningful.
const EXPECTED_CODEX_IDENTITY_HEADER_NAMES: &[&str] = &[
    "authorization",
    "user-agent",
    "originator",
    "accept",
    "session-id",
    "thread-id",
    "x-client-request-id",
    "x-codex-window-id",
    "x-codex-turn-metadata",
];

/// The `x-codex-turn-metadata` JSON field KEY set this milestone synthesizes, verified from
/// `core/src/responses_metadata.rs`'s `CodexTurnMetadataPayload`. The real struct also carries
/// `forked_from_thread_id`/`parent_thread_id`/`subagent_kind`/`compaction`/`extra` — omitted here
/// as conditional/rare fields (forking, subagents, compaction requests) out of scope for this M1
/// baseline-turn synthesis; see `polyflare_codex::codex_headers` module doc.
const EXPECTED_TURN_METADATA_KEYS: &[&str] = &[
    "installation_id",
    "session_id",
    "thread_id",
    "turn_id",
    "window_id",
    "request_kind",
    "sandbox",
    "thread_source",
    "workspaces",
    "turn_started_at_unix_ms",
];

#[tokio::test]
async fn codex_egress_header_structure_matches_the_from_source_codex_rs_golden() {
    let mock = MockUpstream::new(vec![r#"{"type":"response.completed"}"#.to_string()]);
    let handle = mock.clone();
    let base = mock.spawn().await;

    let executor = CodexExecutor::new().unwrap();
    let account = Account {
        id: "fingerprint-gate".into(),
        base_url: base,
        bearer_token: "test-token".into(),
    };
    // The executor now FORWARDS `forward_headers` rather than synthesizing (the codex-lb
    // forward-native/synthesize-non-native split). The translated ingress path synthesizes via
    // `codex_headers`; mirror that synthesis here and assert the executor forwards it intact — so
    // this gate covers both that the synthesized set matches the from-source golden AND that the
    // executor relays it untouched. A `prompt_cache_key` is present so the stable-id derivation
    // exercises its primary path, not the no-key fallback.
    use polyflare_codex::codex_headers::{
        codex_user_agent, conversation_key, originator, TurnIdentity,
    };
    let body = serde_json::json!({
        "model": "gpt-5.6-sol",
        "input": "hello",
        "prompt_cache_key": "conversation-abc123",
    });
    let identity = TurnIdentity::derive(&conversation_key(&body));
    let forward_headers = vec![
        ("user-agent".to_string(), codex_user_agent()),
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
    ];
    let req = PreparedRequest {
        body,
        model: "gpt-5.6-sol".into(),
        forward_headers,
    };

    let mut stream = executor.execute(req, &account).await.unwrap();
    while stream.next().await.is_some() {}

    let headers = handle
        .last_headers()
        .expect("mock should have recorded the outbound request's headers");
    let fp = capture_request_fingerprint("POST", "/responses", &headers);

    let captured_names: BTreeSet<&str> = fp["headers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["name"].as_str().unwrap())
        .collect();

    let missing: Vec<&str> = EXPECTED_CODEX_IDENTITY_HEADER_NAMES
        .iter()
        .filter(|name| !captured_names.contains(*name))
        .copied()
        .collect();
    assert!(
        missing.is_empty(),
        "egress is missing codex-identity header(s) {missing:?}; captured names: \
         {captured_names:?}"
    );

    let turn_meta = fp["headers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|h| h["name"] == "x-codex-turn-metadata")
        .unwrap_or_else(|| panic!("x-codex-turn-metadata header missing: {fp}"));
    assert_eq!(
        turn_meta["value"]["kind"], "json",
        "x-codex-turn-metadata must be a JSON object: {turn_meta}"
    );
    let captured_keys: BTreeSet<&str> = turn_meta["value"]["keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|k| k.as_str().unwrap())
        .collect();
    let expected_keys: BTreeSet<&str> = EXPECTED_TURN_METADATA_KEYS.iter().copied().collect();
    assert_eq!(
        captured_keys, expected_keys,
        "x-codex-turn-metadata field-key set must match the from-source golden exactly"
    );

    let ua = fp["headers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|h| h["name"] == "user-agent")
        .unwrap_or_else(|| panic!("user-agent header missing: {fp}"));
    let masked = ua["value"]["format"]
        .as_str()
        .unwrap_or_else(|| panic!("user-agent should be format-masked: {ua}"));
    assert_codex_ua_format_shape(masked);
}

/// Structural check on the masked `user-agent` value: `{originator}/<ver> ({os info}; {arch})
/// {terminal}`. Checks shape only (not exact tokens, which vary by build machine/terminal) —
/// codex-rs's UA format per `login/src/auth/default_client.rs::get_codex_user_agent`.
fn assert_codex_ua_format_shape(masked: &str) {
    assert!(
        masked.starts_with("codex_cli_rs/<ver> ("),
        "UA should start with the masked originator/version prefix: {masked}"
    );
    let after_prefix = masked
        .strip_prefix("codex_cli_rs/<ver> (")
        .expect("checked by the assert above");
    let (paren_body, terminal_part) = after_prefix.split_once(") ").unwrap_or_else(|| {
        panic!("UA should have a ') <terminal>' suffix after the (os; arch) group: {masked}")
    });
    assert!(
        paren_body.contains("; "),
        "UA parenthetical should separate os info from arch with '; ': {masked}"
    );
    assert_eq!(
        terminal_part, "<seg>",
        "UA terminal segment should be masked to a single <seg> token: {masked}"
    );
}
