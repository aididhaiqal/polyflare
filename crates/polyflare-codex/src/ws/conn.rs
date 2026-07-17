//! The upstream WS connection + codex-parity handshake (`docs/WS-GROUND-TRUTH-CODEX.md` §1).
//!
//! Same "dumb executor, smart ingress" division of labor as `executor.rs`'s HTTP path: this
//! module never synthesizes codex-identity headers itself. It relays whatever `forward_headers`
//! ingress decided to send, then overrides ONLY auth/account headers — with one WS-specific
//! exception ground truth §7.1 calls out explicitly: `x-codex-turn-state` must be STRIPPED from
//! the handshake even if present in `forward_headers`, because on WS (unlike HTTP) it is not a
//! header at all — it travels solely inside each frame's `client_metadata`, and only once the
//! server has supplied a value (never invented client-side). That's the opposite of the HTTP
//! path, which sends it as a real header (`client.rs:1146`).
//!
//! **`permessage-deflate`: now offered, matching real codex exactly (M5a fork adoption).** Real
//! codex offers it (ground truth §1; `codex-rs/codex-api/src/endpoint/responses_websocket.rs:
//! 546-553`'s `websocket_config()` sets `extensions.permessage_deflate =
//! Some(DeflateConfig::default())`), and this crate now matches: stock crates.io
//! `tungstenite`/`tokio-tungstenite` have no `deflate` Cargo feature at all (verified: `cargo add
//! tungstenite@0.27 --features deflate` → `error: unrecognized feature for crate tungstenite:
//! deflate`), so codex gets deflate support from **OpenAI's own forks**, pinned by git rev in the
//! workspace root `Cargo.toml`'s `[patch.crates-io]` (mirroring `codex-rs/Cargo.toml:557-564`
//! exactly: `openai-oss-forks/tokio-tungstenite` rev `0e5b2d73aa18dd9f0a50ee9ff199d5aef7594186`,
//! `openai-oss-forks/tungstenite-rs` rev `4fffad30fe373adbdcffab9545e9e9bf4f2fc19f`). This crate
//! now pins the same versions (`tokio-tungstenite` 0.28.0, `tungstenite` 0.27.0 with the
//! `deflate` feature) via those forks, so it CAN decode `permessage-deflate` frames, and
//! `WsConn::connect` builds the handshake's `WebSocketConfig` the same way
//! `websocket_config()` does (`extensions.permessage_deflate = Some(DeflateConfig::default())`,
//! via `connect_async_with_config`) rather than by hand-writing a
//! `Sec-WebSocket-Extensions` header.
//!
//! This was live-measured, not theoretical: `crates/polyflare-server/examples/ws_deflate_probe.rs`
//! (`.superpowers/sdd/m5a-deflate-probe-report.md`) first proved the backend confirms the offer
//! but that stock `tokio-tungstenite` 0.26 cannot decode the resulting frames (`WebSocket protocol
//! error: Reserved bits are non-zero`, zero readable frames) — which is exactly why the offer was
//! withheld until the forks above were adopted (see `.superpowers/sdd/m5a-deflate-forks-report.md`
//! for the re-run confirming frames now arrive readable WITH the offer, now backed by real decode
//! support).

use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tungstenite::extensions::ExtensionsConfig;
use tungstenite::extensions::compression::deflate::DeflateConfig;
use tungstenite::protocol::WebSocketConfig;

use polyflare_core::{Account, ExecError};

use crate::executor::ensure_rustls_crypto_provider;

/// Ground truth §7.2: `OpenAI-Beta` is `.insert()`'d exactly once with exactly this value, never
/// appended to.
const OPENAI_BETA_WS: &str = "responses_websockets=2026-02-06";

/// Ground truth §1/§7.1: MUST NOT appear as a WS handshake header — stripped from
/// `forward_headers` even if present, since it belongs only inside frame `client_metadata`.
const TURN_STATE_HEADER: &str = "x-codex-turn-state";

/// An established WS connection to a codex backend account, plus the incremental-continuation
/// state later tasks read/write: `last_response_id` (Task 5, from the most recent
/// `response.completed` on THIS socket), `last_input_count` / `last_input_fingerprint` (Task 6's
/// strict-extension check). This task only connects and stores the fields at their initial
/// (unset) values — nothing here sends or classifies a frame yet.
pub struct WsConn {
    #[allow(dead_code)] // held for Tasks 4-7 (frame send/receive); unused until then.
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    /// `response.id` of the most recent `response.completed` seen on this socket. `None` until a
    /// turn has completed (Task 5).
    pub last_response_id: Option<String>,
    /// `input` item count of the most recently sent turn on this socket (Task 6's strict-extension
    /// check reads this).
    pub last_input_count: Option<u32>,
    /// A fingerprint of the most recently sent turn's non-input fields (model, instructions,
    /// tools, tool_choice, parallel_tool_calls, reasoning, service_tier, text) — Task 6's "do the
    /// non-input fields match" check reads this. Never raw conversation content.
    pub last_input_fingerprint: Option<String>,
}

impl WsConn {
    /// Dial `{account.base_url}/responses` (scheme swapped `http`→`ws` / `https`→`wss`, ground
    /// truth §1) with the codex-parity handshake headers, then hand back the open socket.
    ///
    /// Header construction mirrors `executor.rs:96-126`'s insert-not-append rules: every entry in
    /// `forward_headers` is set first (minus `x-codex-turn-state`, stripped per the module doc),
    /// then `Authorization`/`chatgpt-account-id` are overridden from `account`, then `OpenAI-Beta`
    /// is inserted. `permessage-deflate` IS now offered — via `connect_async_with_config`'s
    /// `WebSocketConfig` (the library's own negotiation mechanism, matching codex's
    /// `websocket_config()`), not a hand-written header; see the module doc for why this is now
    /// safe (the OpenAI-fork adoption backing real decode support). No subprotocol, no `Origin` —
    /// never set anywhere in this function, and `into_client_request()` does not add them itself
    /// (ground truth §1).
    pub async fn connect(
        account: &Account,
        forward_headers: &[(String, String)],
    ) -> Result<WsConn, ExecError> {
        // Must run before the first WS TLS handshake so tokio-tungstenite's rustls backend picks
        // up aws-lc-rs instead of falling back to ring — same reason `CodexExecutor::new` calls
        // this before its first HTTP TLS use.
        ensure_rustls_crypto_provider();

        let url = ws_url_for(&account.base_url)?;
        let mut request = url
            .as_str()
            .into_client_request()
            .map_err(|e| ExecError::Upstream(e.to_string()))?;
        let headers = request.headers_mut();

        for (name, value) in forward_headers {
            if name.eq_ignore_ascii_case(TURN_STATE_HEADER) {
                continue;
            }
            let header_name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|e| ExecError::Upstream(e.to_string()))?;
            let header_value =
                HeaderValue::from_str(value).map_err(|e| ExecError::Upstream(e.to_string()))?;
            headers.insert(header_name, header_value);
        }

        let bearer = HeaderValue::from_str(&format!("Bearer {}", account.bearer_token))
            .map_err(|e| ExecError::Upstream(e.to_string()))?;
        headers.insert(HeaderName::from_static("authorization"), bearer);
        // Pair the SELECTED account's ChatGPT id with its Bearer — same reasoning as
        // `executor.rs:109-119`: `insert` (replace), never leave a forwarded value for a
        // DIFFERENT account sitting next to our overridden Bearer.
        if let Some(account_id) = &account.chatgpt_account_id {
            headers.insert(
                HeaderName::from_static("chatgpt-account-id"),
                HeaderValue::from_str(account_id)
                    .map_err(|e| ExecError::Upstream(e.to_string()))?,
            );
        }

        headers.insert(
            HeaderName::from_static("openai-beta"),
            HeaderValue::from_static(OPENAI_BETA_WS),
        );
        // permessage-deflate is offered via `ws_config()` below, not a hand-written header —
        // `tokio_tungstenite::connect_async_with_config` negotiates the `Sec-WebSocket-Extensions`
        // header itself from `WebSocketConfig::extensions`, the same mechanism codex's own
        // `websocket_config()` uses (see module doc).

        let (socket, _response) =
            tokio_tungstenite::connect_async_with_config(request, Some(ws_config()), false)
                .await
                .map_err(|e| ExecError::Upstream(e.to_string()))?;

        Ok(WsConn {
            socket,
            last_response_id: None,
            last_input_count: None,
            last_input_fingerprint: None,
        })
    }
}

/// Mirrors codex's own `websocket_config()` (`codex-api/src/endpoint/responses_websocket.rs:
/// 546-553`) exactly: enable the `permessage-deflate` extension via the library's own
/// `WebSocketConfig`/`ExtensionsConfig` mechanism (never a hand-written
/// `Sec-WebSocket-Extensions` header), with default deflate parameters — same as codex, which
/// also uses `DeflateConfig::default()`.
fn ws_config() -> WebSocketConfig {
    let mut extensions = ExtensionsConfig::default();
    extensions.permessage_deflate = Some(DeflateConfig::default());

    let mut config = WebSocketConfig::default();
    config.extensions = extensions;
    config
}

/// `{base_url}/responses` with the scheme swapped `http→ws` / `https→wss` (ground truth §1).
/// `Account::base_url` is always `http(s)` in production (the same field the HTTP executor reads
/// verbatim), but an already-`ws`/`wss` base is passed through unchanged so a test harness (e.g.
/// `MockWsUpstream::spawn`, which itself returns a `ws://` base URL) can be pointed at directly
/// without a production-only scheme requirement leaking into test setup. Any other scheme (or
/// none) is a configuration error, not an upstream failure.
fn ws_url_for(base_url: &str) -> Result<String, ExecError> {
    let trimmed = base_url.trim_end_matches('/');
    let swapped = if let Some(rest) = trimmed.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = trimmed.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if trimmed.starts_with("wss://") || trimmed.starts_with("ws://") {
        trimmed.to_string()
    } else {
        return Err(ExecError::Upstream(format!(
            "account base_url has no http(s)/ws(s) scheme to rewrite for WS: {base_url}"
        )));
    };
    Ok(format!("{swapped}/responses"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use polyflare_testkit::{MockWsUpstream, ScriptedTurn};
    use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
    use tokio_tungstenite::tungstenite::http::HeaderMap;

    fn test_account(base_url: String) -> Account {
        Account {
            id: "acct-1".into(),
            base_url,
            bearer_token: "secret-bearer-abc".into(),
            chatgpt_account_id: Some("chatgpt-acct-xyz".into()),
        }
    }

    #[test]
    fn ws_url_swaps_scheme_and_appends_responses_path() {
        assert_eq!(
            ws_url_for("https://chatgpt.com/backend-api/codex").unwrap(),
            "wss://chatgpt.com/backend-api/codex/responses"
        );
        assert_eq!(
            ws_url_for("http://127.0.0.1:9999/").unwrap(),
            "ws://127.0.0.1:9999/responses"
        );
        // Already-ws(s) bases (e.g. `MockWsUpstream::spawn`'s return value) pass through.
        assert_eq!(
            ws_url_for("ws://127.0.0.1:9999").unwrap(),
            "ws://127.0.0.1:9999/responses"
        );
        assert!(ws_url_for("ftp://example.test").is_err());
    }

    #[tokio::test]
    async fn connect_succeeds_against_the_mock_upstream_with_unset_incremental_state() {
        let mock = MockWsUpstream::new(ScriptedTurn::normal(vec![]));
        let base = mock.clone().spawn().await;
        let account = test_account(base);

        let conn = WsConn::connect(&account, &[]).await.expect("connect");

        assert_eq!(mock.handshake_count(), 1);
        assert_eq!(conn.last_response_id, None);
        assert_eq!(conn.last_input_count, None);
        assert_eq!(conn.last_input_fingerprint, None);
    }

    /// Spins up a raw TCP + `accept_hdr_async_with_config` server (NOT `MockWsUpstream`, which has
    /// no header introspection in its public API) purely to capture the client's handshake
    /// request headers for exact assertion. Returns the base URL to connect to and a handle that
    /// yields the captured headers once the handshake completes.
    ///
    /// Must pass `Some(WebSocketConfig::default())`, not `accept_hdr_async`'s implicit `None`:
    /// the forked `tungstenite`'s server handshake (`handshake/server.rs`) treats ANY
    /// `Sec-WebSocket-Extensions` header on the request as a hard `InvalidHeader` protocol error
    /// when `self.config` is `None` (`self.config.ok_or_else(...)?` before it can even decline
    /// the offer) — since `WsConn::connect` now always offers `permessage-deflate`, a bare `None`
    /// config here would fail every handshake this test drives, not just this one property. A
    /// real server (the live codex backend, or `accept_hdr_async_with_config` with a `Some`
    /// config) always supplies a config, so this is a test-double-only wrinkle, not a production
    /// concern; `MockWsUpstream` (axum-based, used by the other test in this module) is a
    /// different WS stack entirely and is unaffected.
    async fn spawn_header_capture_server() -> (String, Arc<Mutex<Option<HeaderMap>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let captured: Arc<Mutex<Option<HeaderMap>>> = Arc::new(Mutex::new(None));
        let captured_task = captured.clone();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let captured_cb = captured_task.clone();
                let callback = move |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
                    *captured_cb.lock().unwrap() = Some(req.headers().clone());
                    Ok(resp)
                };
                let _ = tokio_tungstenite::accept_hdr_async_with_config(
                    stream,
                    callback,
                    Some(WebSocketConfig::default()),
                )
                .await;
            }
        });
        (format!("http://{addr}"), captured)
    }

    #[tokio::test]
    async fn handshake_headers_match_ground_truth() {
        let (base_url, captured) = spawn_header_capture_server().await;
        let account = test_account(base_url);
        let forward_headers = vec![
            ("x-client-request-id".to_string(), "thread-123".to_string()),
            // Deliberately included to prove it gets ACTIVELY stripped, not merely never added —
            // ground truth §7.1: this must be absent from the WS handshake even though the HTTP
            // path (executor.rs) would forward it untouched.
            (
                "x-codex-turn-state".to_string(),
                "should-be-stripped".to_string(),
            ),
        ];

        let _conn = WsConn::connect(&account, &forward_headers)
            .await
            .expect("connect");

        let headers = captured.lock().unwrap().clone().expect("headers captured");

        // §7.2: inserted exactly once, exact value.
        assert_eq!(
            headers.get_all("openai-beta").iter().count(),
            1,
            "OpenAI-Beta must appear exactly once"
        );
        assert_eq!(
            headers.get("openai-beta").unwrap(),
            "responses_websockets=2026-02-06"
        );

        // Auth override rules mirror executor.rs.
        assert_eq!(
            headers.get("authorization").unwrap(),
            "Bearer secret-bearer-abc"
        );
        assert_eq!(headers.get("chatgpt-account-id").unwrap(), "chatgpt-acct-xyz");

        // §7.1: the single easiest thing to get wrong — must be ABSENT, opposite of the HTTP path.
        assert!(
            headers.get("x-codex-turn-state").is_none(),
            "x-codex-turn-state must NOT be a WS handshake header"
        );

        // Dumb-relay: other forwarded headers pass through untouched.
        assert_eq!(headers.get("x-client-request-id").unwrap(), "thread-123");

        // Fingerprint parity restored (M5a fork adoption): real codex offers permessage-deflate
        // (ground truth §1), and this crate now matches, because it can actually decode it —
        // `tokio-tungstenite`/`tungstenite` are now pinned to OpenAI's own forks (workspace root
        // `Cargo.toml`'s `[patch.crates-io]`) which add the `deflate` feature stock crates.io
        // lacks. `.superpowers/sdd/m5a-deflate-probe-report.md` first measured that the backend
        // confirms the offer but stock tokio-tungstenite 0.26 can't decode the result
        // (`WebSocket protocol error: Reserved bits are non-zero`, zero readable frames);
        // `.superpowers/sdd/m5a-deflate-forks-report.md` re-ran the same live probe after this
        // fork adoption and confirmed frames now arrive readable WITH the offer.
        assert!(
            headers.get("sec-websocket-extensions").is_some(),
            "must offer permessage-deflate — matches codex-parity ground truth §1, now backed by \
             real decode support (see m5a-deflate-forks-report.md)"
        );

        // §1: no subprotocol, no Origin.
        assert!(headers.get("origin").is_none(), "must never set Origin");
        assert!(
            headers.get("sec-websocket-protocol").is_none(),
            "must never set a subprotocol"
        );
    }
}
