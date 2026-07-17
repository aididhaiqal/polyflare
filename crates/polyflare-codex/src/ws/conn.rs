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
//! **`permessage-deflate`: deliberately NOT offered — a known fingerprint divergence, not an
//! oversight.** Real codex DOES offer it (ground truth §1; `codex-rs/codex-api/src/endpoint/
//! responses_websocket.rs:546-553`'s `websocket_config()` sets `extensions.permessage_deflate =
//! Some(DeflateConfig::default())`). This crate omits the offer because it cannot decode the
//! extension if the backend accepts it: `deflate` is not a real Cargo feature of stock
//! `tungstenite`/`tokio-tungstenite` (verified: `cargo add tungstenite@0.27 --features deflate` →
//! `error: unrecognized feature for crate tungstenite: deflate`). Codex gets deflate support from
//! **OpenAI's own forks**, pinned by git rev in `codex-rs/Cargo.toml:557-564`
//! (`openai-oss-forks/tokio-tungstenite` rev `0e5b2d73aa18dd9f0a50ee9ff199d5aef7594186`,
//! `openai-oss-forks/tungstenite-rs` rev `4fffad30fe373adbdcffab9545e9e9bf4f2fc19f`, with
//! `tungstenite = { version = "0.27.0", features = ["deflate", "proxy"] }`) — this workspace does
//! not depend on those forks (adopting them is a pending decision, out of scope here).
//!
//! This was live-measured, not theoretical: `crates/polyflare-server/examples/ws_deflate_probe.rs`
//! (`.superpowers/sdd/m5a-deflate-probe-report.md`) connected to the real Codex backend twice.
//! WITH the offer, the 101 response echoed `sec-websocket-extensions: permessage-deflate` (the
//! backend confirmed it), and the very first inbound frame then killed the connection —
//! `WebSocket protocol error: Reserved bits are non-zero` — zero readable frames, connection
//! unusable. WITHOUT the offer (same account, same turn), all 13 frames arrived as plain
//! `Text`/valid-JSON. **Do not re-add this offer for fingerprint parity without first adding real
//! deflate support** (i.e. adopting the forks above) — restoring it as-is breaks every live WS
//! connection this crate makes.

use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

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
    /// is inserted. `permessage-deflate` is NOT offered — see the module doc's caveat: real codex
    /// does offer it, this crate can't decode it, and offering it anyway kills every live
    /// connection (live-measured). No subprotocol, no `Origin` — never set anywhere in this
    /// function, and `into_client_request()` does not add them itself (ground truth §1).
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
        // No `sec-websocket-extensions: permessage-deflate` offer here — see the module doc's
        // caveat. Live-measured: offering it makes the backend confirm deflate and then kill the
        // connection on the first inbound frame, since this crate can't decode it.

        let (socket, _response) = tokio_tungstenite::connect_async(request)
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

    /// Spins up a raw TCP + `accept_hdr_async` server (NOT `MockWsUpstream`, which has no header
    /// introspection in its public API) purely to capture the client's handshake request headers
    /// for exact assertion. Returns the base URL to connect to and a handle that yields the
    /// captured headers once the handshake completes.
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
                let _ = tokio_tungstenite::accept_hdr_async(stream, callback).await;
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

        // Deliberate fingerprint divergence, pinned so it doesn't drift back: real codex offers
        // permessage-deflate (ground truth §1), but this crate must NOT, because it can't decode
        // it — live-measured in `.superpowers/sdd/m5a-deflate-probe-report.md`: offering it made
        // the real backend confirm the extension and then kill the connection on the first
        // inbound frame (`WebSocket protocol error: Reserved bits are non-zero`, zero readable
        // frames). Without the offer, the same live account/turn produced 13 valid JSON frames.
        assert!(
            headers.get("sec-websocket-extensions").is_none(),
            "must NOT offer permessage-deflate — see m5a-deflate-probe-report.md; restoring this \
             without real deflate support breaks every live WS connection"
        );

        // §1: no subprotocol, no Origin.
        assert!(headers.get("origin").is_none(), "must never set Origin");
        assert!(
            headers.get("sec-websocket-protocol").is_none(),
            "must never set a subprotocol"
        );
    }
}
