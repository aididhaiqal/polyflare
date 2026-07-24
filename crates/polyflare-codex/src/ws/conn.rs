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

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tungstenite::extensions::compression::deflate::DeflateConfig;
use tungstenite::extensions::ExtensionsConfig;
use tungstenite::protocol::WebSocketConfig;

use polyflare_core::{Account, ExecError};

use crate::executor::ensure_rustls_crypto_provider;

/// Ground truth §7.2: `OpenAI-Beta` is `.insert()`'d exactly once with exactly this value, never
/// appended to.
const OPENAI_BETA_WS: &str = "responses_websockets=2026-02-06";

/// The canonical `x-codex-turn-state` name — a single source of truth rather than drifting
/// literals, since this crate must keep it byte-identical in three places:
/// 1. **Stripped** from the WS handshake REQUEST headers here (ground truth §1/§7.1: on WS it must
///    NOT be a handshake header — it belongs only inside frame `client_metadata`).
/// 2. **Captured** as the WS UPGRADE-RESPONSE header this module reads in
///    [`connect_detailed_with_timeout`] into [`WsConn::upgrade_turn_state`]
///    (`responses_websocket.rs:529-535`) — the sole turn-state source in PolyFlare's model.
/// 3. **Replayed** as the `client_metadata` key in `ws::executor` (`client.rs:1568-1569`).
///
/// `pub(crate)`: read by `ws::executor` for (3).
pub(crate) const TURN_STATE_HEADER: &str = "x-codex-turn-state";
const UPGRADE_RESPONSE_HEADERS: [&str; 4] = [
    TURN_STATE_HEADER,
    "x-models-etag",
    "x-reasoning-included",
    "openai-model",
];

/// The part of an upstream WebSocket upgrade response that remains observable to Codex for the
/// lifetime of its downstream socket. A transparent PolyFlare redial is safe only when this
/// contract is unchanged; `x-codex-turn-state` is deliberately excluded because it is per-turn
/// routing state learned from response metadata, not a stable connection capability.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WsRelayContract {
    models_etag: Option<String>,
    reasoning_included: bool,
    model: Option<String>,
}

impl WsRelayContract {
    fn from_upgrade_headers(headers: &tokio_tungstenite::tungstenite::http::HeaderMap) -> Self {
        let value = |name: &str| {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
        };
        Self {
            models_etag: value("x-models-etag"),
            // Codex consumes this as a presence capability (`contains_key`), not as a parsed
            // boolean header value.
            reasoning_included: headers.contains_key("x-reasoning-included"),
            model: value("openai-model"),
        }
    }

    /// Replace the catalog identity with the value visible to the downstream client. PolyFlare
    /// pooled routes use a virtual pool ETag rather than one member account's upstream ETag.
    pub fn with_models_etag(mut self, models_etag: Option<String>) -> Self {
        self.models_etag = models_etag;
        self
    }
}

/// Bounded dial/handshake budget — a hung TCP/TLS/WS-upgrade must not stall a turn until the OS TCP
/// timeout. Mirrors CLIProxyAPI's 30s codex WS dial bound. Overridable per-call via
/// [`connect_detailed_with_timeout`] (tests inject a short value); [`connect_detailed`] uses this.
pub(crate) const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Bounded send budget — a hung write (a backpressured/half-open peer that never drains the socket)
/// must not stall a turn forever the way an untimed `self.socket.send(...).await` would. This is the
/// missing third bound alongside the dial ([`WS_CONNECT_TIMEOUT`]) and per-read
/// ([`WS_READ_IDLE_TIMEOUT`]) bounds — codex itself bounds its send with its idle timeout. On elapse
/// the socket is poisoned (`self.closed = true`) exactly like any other send failure, so it is
/// evicted on reuse. Overridable per-call via [`WsConn::send_frame_with_timeout`] (tests inject a
/// short value); [`WsConn::send_frame`] uses this.
pub(crate) const WS_SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// Max silence (per `recv_frame` call) before the socket is treated as stalled and poisoned
/// (`self.closed = true` → [`WsConn::is_closed`] → evicted on next reuse). Deliberately ~10s BELOW
/// `polyflare-server`'s `DEFAULT_STREAM_IDLE_TIMEOUT` (300s, codex's own `stream_idle_timeout`
/// default) so the WS layer poisons a stalled socket JUST BEFORE the ingress idle-watchdog cancels
/// the stream — guaranteeing the dead socket is evicted on the FIRST stall rather than possibly only
/// on a second (which would happen if this fired at or after the watchdog). Still ~codex's own
/// minutes-long tolerance, and the keepalive pings [`WS_PING_INTERVAL`] keep a legit slow-but-alive
/// turn warm, so this fires ONLY on a genuinely dead socket, never on a slow-but-alive generation.
/// Overridable per-call via [`WsConn::recv_frame_with_timeout`] (tests inject a short value);
/// [`WsConn::recv_frame`] uses this.
pub(crate) const WS_READ_IDLE_TIMEOUT: Duration = Duration::from_secs(290);

/// Client keepalive ping cadence while a turn's read is silent. **Used ONLY when
/// `POLYFLARE_WS_CLIENT_PING` is on** — the DEFAULT is codex-rs-faithful (NO client-initiated ping;
/// [`WsConn::client_ping_interval`] is `None`, and real codex-rs never pings — ground truth §7).
/// When the flag is on, this mirrors codex-lb (which leaves the `websockets` lib default ~20s) —
/// keeps the upstream socket alive through NAT/middlebox idle reaping during the minutes a codex
/// turn can spend silently reasoning, and surfaces a dead peer fast (the ping send fails) instead of
/// waiting out the full read-idle budget. Even then we deliberately do NOT enforce a pong response
/// (no pong-watchdog): the absolute read-idle deadline is the sole stall decision — same split
/// codex-lb documents. Injected per-call via [`WsConn::recv_frame_with_timeout`]'s `Option`
/// (`None` = off = default; tests inject a short `Some(...)` to exercise the flag-on path);
/// [`WsConn::recv_frame`] passes [`WsConn::client_ping_interval`], set from the flag.
pub(crate) const WS_PING_INTERVAL: Duration = Duration::from_secs(20);

/// Substring marker on the `ExecError::Stream` a read-idle poison raises, so a consumer could
/// classify "the socket went silent" apart from other stream errors — mirrors `turn.rs`'s
/// `SOCKET_CLOSED_MARKER` convention (a named constant shared by producer and any future consumer,
/// rather than a bare literal that could silently drift).
pub(crate) const WS_READ_IDLE_MARKER: &str = "websocket read idle timeout";

/// True when `err` is the read-idle poison raised by [`WsConn::recv_frame_with_timeout`]'s
/// deadline (marker [`WS_READ_IDLE_MARKER`]) rather than a genuine transport failure. The relay
/// pump uses this between turns to tell "the idle budget elapsed — we deliberately let the socket
/// go" apart from "the peer dropped us" when choosing its honest-close telemetry label.
pub fn is_read_idle_error(err: &ExecError) -> bool {
    matches!(err, ExecError::Stream(msg) if msg.contains(WS_READ_IDLE_MARKER))
}

pub(crate) struct PendingBaseline {
    pub(crate) input_count: u32,
    pub(crate) item_hashes: Vec<super::delta::ItemHash>,
    pub(crate) non_input_fingerprint: String,
}

/// An established WS connection to a codex backend account, plus the incremental-continuation
/// state later tasks read/write: `last_response_id` (Task 5, from the most recent
/// `response.completed` on THIS socket), `last_item_hashes` / `last_non_input_fingerprint` /
/// `last_input_count` (Task 6's `delta::plan_request` strict-extension check). This task only
/// connects and stores the fields at their initial (unset) values — nothing here sends or
/// classifies a frame yet.
pub struct WsConn {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    /// `response.id` of the most recent `response.completed` seen on this socket. `None` until a
    /// turn has completed (Task 5).
    pub last_response_id: Option<String>,
    /// `input` item count of the most recently sent turn on this socket. Not read directly by
    /// `delta::plan_request` (which derives the count from `last_item_hashes.len()` instead); kept
    /// as a cheap pre-check hint (e.g. "new input can't possibly be a strict extension if it's not
    /// even longer than this" without computing any hashes) available to Task 5/7's send path.
    pub last_input_count: Option<u32>,
    /// Content-free per-item hashes (`delta::ItemHash`, via `delta::item_hashes`) of the `input`
    /// array most recently SENT on this socket, in order — `delta::plan_request`'s rule 2
    /// (strict-extension) check reads this. **Whoever sends a turn on this connection (Task 5/7's
    /// turn-send code) MUST set this immediately after sending**, to `Some(delta::item_hashes(&body))`
    /// for the envelope `body` just sent. `None` until the first turn is sent. If a sender forgets
    /// to set this after sending, every subsequent `plan_request` call on this connection silently
    /// and permanently sees `None` here and returns `Full` — no error, just a milestone that
    /// quietly never produces an incremental turn again.
    pub last_item_hashes: Option<Vec<super::delta::ItemHash>>,
    /// A content-free fingerprint (`delta::non_input_fingerprint`) of the most recently SENT
    /// turn's non-input fields (model, instructions, tools, tool_choice, parallel_tool_calls,
    /// reasoning, service_tier, text — despite the field's name, this covers everything EXCEPT
    /// `input`) — `delta::plan_request`'s rule 3 ("do the non-input fields match") check reads
    /// this. Never raw conversation content. Must be set at the same time and by the same sender
    /// as `last_item_hashes` (see that field's doc for the failure mode if it's forgotten).
    pub last_non_input_fingerprint: Option<String>,
    /// Baseline staged by a successful send and promoted only by `response.completed`. A failed,
    /// incomplete, wrapped-error, or pre-terminal-close turn clears it without changing the last
    /// committed baseline.
    pub(crate) pending_baseline: Option<PendingBaseline>,
    /// The server-issued `x-codex-turn-state` sticky-routing token captured from THIS socket's WS
    /// UPGRADE-response header at dial (in [`connect_detailed_with_timeout`], this construction
    /// site), or `None` if the header was absent (never fabricated). **A ONE-SHOT belonging to the
    /// turn that ESTABLISHED this socket, NOT persistent per-turn state** — it is consumed
    /// (`.take()`) by the FIRST turn that uses the socket (`ws::executor::drive_turn`), which replays
    /// it into that turn's outbound frames; every LATER turn on the same reused socket takes `None`
    /// and sends NO `x-codex-turn-state`. That is codex parity: codex scopes turn-state PER TURN (a
    /// fresh `OnceLock` per `ModelClientSession`, empty at send time on a reused socket —
    /// `client.rs:479-484`) even though it reuses the cached socket, and its doc
    /// (`client.rs:268-283`) is explicit the token may be kept "unchanged between turn requests
    /// (retries/incremental appends within the turn) … must NOT [be] sen[t] between different
    /// turns". Replay lives ONLY in frame `client_metadata["x-codex-turn-state"]`
    /// (`ws::executor::plan_and_build_locked`, ground truth §7.1: on WS the token is never a
    /// handshake header — the opposite of the HTTP path). A RECONNECT dials a FRESH `WsConn` that
    /// captures the NEW socket's own upgrade token; it is NOT persisted across reconnects.
    /// Content-free routing token (server-issued, not conversation content) — safe to store/replay,
    /// but its VALUE is NEVER logged. `pub(crate)`: consumed by `ws::executor`.
    pub(crate) upgrade_turn_state: Option<String>,
    /// Codex-consumed metadata from the successful upstream `101`, restricted to the exact
    /// allowlist the client reads. The downstream relay exposes these on its own `101`.
    upgrade_response_headers: Vec<(String, String)>,
    /// Stable, client-visible portion of the successful upstream `101`. Internal upstream
    /// reconnects must match it because the downstream `101` cannot be updated in place.
    relay_contract: WsRelayContract,
    /// Set once this socket is known dead: a `Close` frame / clean stream end observed by
    /// `recv_frame`, or a send/recv error. Ground truth §2: real codex reuse is "gated only on
    /// liveness (`conn.is_closed()`)" — Task 7's connection cache reads this via [`Self::is_closed`]
    /// to decide "reuse the cached handle" vs "this needs a fresh `connect`" BEFORE attempting to
    /// reuse it, rather than reactively discovering the failure on the next attempted send.
    closed: bool,
    /// The client keepalive-ping cadence used by [`Self::recv_frame`] during a silent read, or
    /// `None` for the **codex-rs-faithful default** (no client-initiated ping ever — real codex-rs
    /// never pings; ground truth §7). `None` at construction; the WS executor overrides it to
    /// `Some(WS_PING_INTERVAL)` ONLY when `POLYFLARE_WS_CLIENT_PING` is on (`ws::executor`'s
    /// `connect_and_cache`) — an opt-in fingerprint divergence for aggressive-NAT/middlebox
    /// deployments. `pub(crate)`: set by `ws::executor`, read by [`Self::recv_frame`].
    pub(crate) client_ping_interval: Option<Duration>,
}

impl WsConn {
    /// Whether this socket is known dead (see [`Self::closed`]'s doc). `pub(crate)`: read by
    /// Task 7's connection cache (`ws::executor`), not exposed outside this crate.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }

    /// Allowlisted metadata from the successful upstream upgrade response.
    pub fn upgrade_response_headers(&self) -> &[(String, String)] {
        &self.upgrade_response_headers
    }

    /// Stable upgrade contract used to decide whether an internal redial can remain transparent.
    pub fn relay_contract(&self) -> &WsRelayContract {
        &self.relay_contract
    }

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
        match connect_detailed(account, forward_headers).await {
            ConnectOutcome::Connected(conn) => Ok(*conn),
            // Collapses the one distinguished outcome back to a generic error for callers that
            // don't need to act on it differently (this fn's existing callers/tests) — Task 7's
            // `ws::executor::CodexWsExecutor` calls `connect_detailed` directly instead, precisely
            // so it CAN tell this case apart (ground truth §5: the ONE `FallbackToHttp` trigger).
            ConnectOutcome::UpgradeRequired => Err(ExecError::Upstream(
                "WS handshake rejected: HTTP 426 Upgrade Required".to_string(),
            )),
            ConnectOutcome::Failed(e) => Err(e),
        }
    }
}

/// **The PUBLIC WS-downstream relay entry point (Phase-1 Task 4).** Dial the conversation owner
/// account's upstream WS and hand back an open [`WsConn`] the `polyflare-server` relay pump drives
/// via [`WsConn::send_text`] / [`WsConn::recv_text`].
///
/// This is the ONE public door the relay needs: the entire codex-parity handshake — `permessage-
/// deflate`, `OpenAI-Beta`, the `Authorization`/`chatgpt-account-id` override, and the ground-truth
/// §7.1 `x-codex-turn-state` strip — lives in [`connect_detailed`] and is REUSED verbatim, never
/// reimplemented in the relay. Collapses [`ConnectOutcome`] exactly as [`WsConn::connect`] does:
/// `Connected → Ok(*conn)`, `Failed → Err(ExecError)`, and `UpgradeRequired → UpstreamHttp(426)` so
/// a downstream relay can preserve Codex's sole WS-to-HTTP fallback signal. `forward_headers` are
/// the DOWNSTREAM handshake headers the relay already filtered through
/// `ingress::forward_headers_from_inbound`; this fn forwards them through the same insert-not-append
/// rules the HTTP executor uses (see [`connect_detailed`]). No frame is sent here and nothing is
/// logged (the relay's content-free discipline).
pub async fn dial_upstream(
    account: &Account,
    forward_headers: &[(String, String)],
) -> Result<WsConn, ExecError> {
    match connect_detailed(account, forward_headers).await {
        ConnectOutcome::Connected(conn) => Ok(*conn),
        ConnectOutcome::UpgradeRequired => {
            Err(ExecError::UpstreamHttp(polyflare_core::UpstreamHttpError {
                signal: polyflare_core::FailureSignal {
                    status: 426,
                    retry_after: None,
                    error_code: None,
                },
                headers: Vec::new(),
                body: bytes::Bytes::new(),
            }))
        }
        ConnectOutcome::Failed(e) => Err(e),
    }
}

/// The distinguished outcomes of one handshake attempt (M5a Task 7). Ground truth §5 is explicit
/// that HTTP 426 Upgrade Required is the ONLY `FallbackToHttp` trigger codex itself recognizes —
/// "No other status falls back". `ws::executor::CodexWsExecutor` needs to tell that ONE case apart
/// from every other connect/handshake failure (which stays a plain `ExecError::Upstream`, surfaced
/// unchanged, per `SPEC-M5-WEBSOCKET.md` §4 / the M5a plan's Task 7 table) — hence this three-way
/// split instead of collapsing straight to `Result<WsConn, ExecError>` the way [`WsConn::connect`]
/// (this function's thin public wrapper, kept for existing callers/tests) still does.
pub(crate) enum ConnectOutcome {
    /// Boxed purely to keep this enum small (clippy's `large_enum_variant`): `WsConn` embeds the
    /// full `WebSocketStream`/TLS-stream buffers, dwarfing the other two variants.
    Connected(Box<WsConn>),
    /// The handshake was rejected with HTTP 426 — the ONE `FallbackToHttp` trigger (ground truth
    /// §5). No socket was ever established.
    UpgradeRequired,
    /// Any other handshake/transport failure (DNS, refused, timeout, malformed header value, ...).
    Failed(ExecError),
}

/// Build the WS handshake request (header construction only — no I/O). Split out from
/// `connect_detailed` purely so that function's dial step can `match` on outcomes without
/// threading `?` through header-building AND the dial in the same expression.
fn build_handshake_request(
    account: &Account,
    forward_headers: &[(String, String)],
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request, ExecError> {
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
    headers.remove(HeaderName::from_static("x-openai-fedramp"));
    if account.is_fedramp {
        headers.insert(
            HeaderName::from_static("x-openai-fedramp"),
            HeaderValue::from_static("true"),
        );
    }
    // Pair the SELECTED account's ChatGPT id with its Bearer — same reasoning as
    // `executor.rs:109-119`: `insert` (replace), never leave a forwarded value for a
    // DIFFERENT account sitting next to our overridden Bearer.
    headers.remove(HeaderName::from_static("chatgpt-account-id"));
    if let Some(account_id) = &account.chatgpt_account_id {
        headers.insert(
            HeaderName::from_static("chatgpt-account-id"),
            HeaderValue::from_str(account_id).map_err(|e| ExecError::Upstream(e.to_string()))?,
        );
    }

    headers.insert(
        HeaderName::from_static("openai-beta"),
        HeaderValue::from_static(OPENAI_BETA_WS),
    );
    // permessage-deflate is offered via `ws_config()`, not a hand-written header —
    // `tokio_tungstenite::connect_async_with_config` negotiates the `Sec-WebSocket-Extensions`
    // header itself from `WebSocketConfig::extensions`, the same mechanism codex's own
    // `websocket_config()` uses (see module doc).

    Ok(request)
}

/// Dial the handshake and distinguish the ONE fallback-worthy outcome (HTTP 426) from every other
/// failure. `pub(crate)`: `ws::executor::CodexWsExecutor` (Task 7) is the one caller that needs
/// this distinction; [`WsConn::connect`] stays the public, collapsed-to-`Result` entry point for
/// everyone else (this module's own tests included).
pub(crate) async fn connect_detailed(
    account: &Account,
    forward_headers: &[(String, String)],
) -> ConnectOutcome {
    connect_detailed_with_timeout(account, forward_headers, WS_CONNECT_TIMEOUT).await
}

/// [`connect_detailed`] with the dial budget injected, so a test can pass a SHORT duration and prove
/// a hung handshake self-terminates without sleeping the production 30s. Production calls delegate
/// here via [`connect_detailed`] with [`WS_CONNECT_TIMEOUT`].
pub(crate) async fn connect_detailed_with_timeout(
    account: &Account,
    forward_headers: &[(String, String)],
    connect_timeout: Duration,
) -> ConnectOutcome {
    // Must run before the first WS TLS handshake so tokio-tungstenite's rustls backend picks up
    // aws-lc-rs instead of falling back to ring — same reason `CodexExecutor::new` calls this
    // before its first HTTP TLS use.
    ensure_rustls_crypto_provider();

    let request = match build_handshake_request(account, forward_headers) {
        Ok(r) => r,
        Err(e) => return ConnectOutcome::Failed(e),
    };

    // Bound the whole dial (TCP + TLS + WS upgrade). A hung handshake must not stall a turn until
    // the OS TCP timeout — an elapsed budget maps to the SAME generic `Failed`/`ExecError::Upstream`
    // outcome any other transport failure uses (the variant doc already lists "timeout"); it never
    // becomes `UpgradeRequired`.
    match tokio::time::timeout(
        connect_timeout,
        tokio_tungstenite::connect_async_with_config(request, Some(ws_config()), false),
    )
    .await
    {
        Err(_elapsed) => ConnectOutcome::Failed(ExecError::Upstream(format!(
            "upstream WS handshake timed out after {connect_timeout:?}"
        ))),
        Ok(inner) => match inner {
            Ok((socket, response)) => {
                // codex parity (`responses_websocket.rs:529-535`): the turn-state capture — read the
                // server-issued `x-codex-turn-state` off the WS UPGRADE response header
                // (`.to_str().ok()`, exactly as codex does). `None` when the header is absent (never
                // fabricated). This is a ONE-SHOT for the turn that establishes the socket; it is
                // `.take()`-consumed by the first turn in `ws::executor::drive_turn` (see
                // [`Self::upgrade_turn_state`]). PolyFlare captures turn-state ONLY here, not from
                // mid-response `response.metadata` frames: it sends one `response.create` per turn
                // and a `response.metadata` frame arrives only AFTER that send, while a same-turn
                // retry re-dials (fresh upgrade token) — so the upgrade-header token is the operative
                // source and a frame-captured one would have no effect in PolyFlare's model. The
                // token VALUE is never logged.
                let upgrade_turn_state = response
                    .headers()
                    .get(TURN_STATE_HEADER)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string);
                let upgrade_response_headers = UPGRADE_RESPONSE_HEADERS
                    .iter()
                    .filter_map(|name| {
                        response
                            .headers()
                            .get(*name)
                            .and_then(|value| value.to_str().ok())
                            .map(|value| ((*name).to_string(), value.to_string()))
                    })
                    .collect();
                let relay_contract = WsRelayContract::from_upgrade_headers(response.headers());
                ConnectOutcome::Connected(Box::new(WsConn {
                    socket,
                    closed: false,
                    last_response_id: None,
                    last_input_count: None,
                    last_item_hashes: None,
                    last_non_input_fingerprint: None,
                    pending_baseline: None,
                    upgrade_turn_state,
                    upgrade_response_headers,
                    relay_contract,
                    // `None` = codex-rs default: no client-initiated ping. `ws::executor` overrides
                    // this to `Some(WS_PING_INTERVAL)` right after connect ONLY when
                    // `POLYFLARE_WS_CLIENT_PING` is on.
                    client_ping_interval: None,
                }))
            }
            // Ground truth §5: HTTP 426 Upgrade Required, checked at handshake time, is the ONLY
            // `FallbackToHttp` trigger — "No other status falls back". Everything else (refused,
            // timeout, DNS, any other HTTP status, a protocol-level handshake error, ...) collapses
            // to the generic `Failed` arm below, surfaced as `ExecError::Upstream` unchanged.
            Err(tokio_tungstenite::tungstenite::Error::Http(response))
                if response.status()
                    == tokio_tungstenite::tungstenite::http::StatusCode::UPGRADE_REQUIRED =>
            {
                ConnectOutcome::UpgradeRequired
            }
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                let status = response.status().as_u16();
                let retry_after = response
                    .headers()
                    .get("retry-after")
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.trim().parse::<i64>().ok())
                    .filter(|value| *value >= 0);
                let headers = response
                    .headers()
                    .iter()
                    .filter(|(name, _)| {
                        !matches!(
                            name.as_str(),
                            "connection"
                                | "content-length"
                                | "content-encoding"
                                | "transfer-encoding"
                                | "set-cookie"
                        )
                    })
                    .filter_map(|(name, value)| {
                        value
                            .to_str()
                            .ok()
                            .map(|value| (name.as_str().to_string(), value.to_string()))
                    })
                    .collect();
                let body = response
                    .body()
                    .as_ref()
                    .map(|body| bytes::Bytes::copy_from_slice(body))
                    .unwrap_or_default();
                ConnectOutcome::Failed(ExecError::UpstreamHttp(polyflare_core::UpstreamHttpError {
                    signal: polyflare_core::FailureSignal {
                        status,
                        retry_after,
                        error_code: None,
                    },
                    headers,
                    body,
                }))
            }
            Err(e) => ConnectOutcome::Failed(ExecError::Upstream(e.to_string())),
        },
    }
}

impl WsConn {
    /// Send one outbound frame (a `response.create` envelope Task 4's `codec::build_response_create`
    /// already built, and Task 6's `delta::plan_request` already decided anchor/suffix for) as a
    /// `Text` WS message. First consumer: Task 5's turn stream (`ws::turn`), hence `pub(crate)` —
    /// the socket itself stays private to this module.
    pub(crate) async fn send_frame(&mut self, envelope: &Value) -> Result<(), ExecError> {
        self.send_frame_with_timeout(envelope, WS_SEND_TIMEOUT)
            .await
    }

    /// [`send_frame`] with the send budget injected, so a test can pass a SHORT duration and prove a
    /// hung write self-terminates (poisoning the socket) without sleeping the production 30s.
    /// Production calls delegate here via [`send_frame`] with [`WS_SEND_TIMEOUT`]. The `Ok`/`Err`
    /// arms below are unchanged from the untimed original; only the `tokio::time::timeout` wrapper
    /// and its elapsed arm are new (the third transport bound alongside dial + read).
    pub(crate) async fn send_frame_with_timeout(
        &mut self,
        envelope: &Value,
        timeout: Duration,
    ) -> Result<(), ExecError> {
        let text = serde_json::to_string(envelope).map_err(|e| {
            ExecError::Upstream(format!("failed to serialize response.create: {e}"))
        })?;
        self.send_message_with_timeout(Message::Text(text.into()), timeout)
            .await
    }

    /// The shared write path for every outbound message (frame-from-`Value` and raw-text alike): a
    /// bounded [`WS_SEND_TIMEOUT`]-style send that poisons the socket (`self.closed = true`) on ANY
    /// failure or elapse so it is evicted on reuse. Extracted so [`send_frame_with_timeout`]'s exact
    /// behavior is preserved AND [`Self::send_text`] can reuse the identical bound/poison contract
    /// without a second copy. Never logs the message (it may be conversation content).
    async fn send_message_with_timeout(
        &mut self,
        message: Message,
        timeout: Duration,
    ) -> Result<(), ExecError> {
        let deadline = tokio::time::Instant::now() + timeout;
        self.send_message_until(message, deadline, timeout).await
    }

    /// Send without ever polling the socket after `deadline`. A biased select checks the absolute
    /// deadline first, unlike `tokio::time::timeout`, whose wrapped future may be polled once before
    /// its timer. `timeout_label` is the caller-facing budget included in the content-free error.
    async fn send_message_until(
        &mut self,
        message: Message,
        deadline: tokio::time::Instant,
        timeout_label: Duration,
    ) -> Result<(), ExecError> {
        // `sleep_until(now)` is not guaranteed to be timer-ready until it has been polled once.
        // Reject an already-spent budget synchronously so an immediately writable socket cannot
        // win that first poll. If the task is descheduled after this check, the biased select below
        // polls the now-expired sleep before the send.
        if tokio::time::Instant::now() >= deadline {
            self.closed = true;
            return Err(ExecError::Upstream(format!(
                "upstream WS send timed out after {timeout_label:?}"
            )));
        }
        let result = tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => None,
            result = self.socket.send(message) => Some(result),
        };
        match result {
            Some(Ok(())) => Ok(()),
            Some(Err(e)) => {
                // A send failure means this socket can no longer be trusted for reuse — see
                // `closed`'s doc. Task 7's cache checks this before ever attempting reuse.
                self.closed = true;
                Err(ExecError::Upstream(e.to_string()))
            }
            None => {
                self.closed = true;
                Err(ExecError::Upstream(format!(
                    "upstream WS send timed out after {timeout_label:?}"
                )))
            }
        }
    }

    /// **PUBLIC WS-downstream relay send (Task 4): forward a RAW text frame VERBATIM.** The `text`
    /// bytes go on the wire UNCHANGED — this is the relay's fidelity crux. Unlike [`Self::send_frame`]
    /// (which serializes a `Value`, RESHAPING key order and dropping the client's exact formatting),
    /// this forwards the client's own frame byte-for-byte, so the codex wire fingerprint is preserved
    /// — the relay MUST NOT parse-then-reserialize a relayed frame. Reuses the same
    /// [`WS_SEND_TIMEOUT`] bound and poison-on-fail as [`Self::send_frame`]. **Content-free:** the raw
    /// frame text IS conversation content and is NEVER logged here.
    pub async fn send_text(&mut self, text: String) -> Result<(), ExecError> {
        self.send_message_with_timeout(Message::Text(text.into()), WS_SEND_TIMEOUT)
            .await
    }

    /// Read the next frame's raw text off the socket, for the turn stream (Task 5, `ws::turn`) to
    /// parse and `classify`.
    ///
    /// - **Client keepalive pings — OFF BY DEFAULT, codex-rs-faithful:** by default
    ///   ([`Self::client_ping_interval`] is `None`) this does NOT initiate a `Ping` at all — it just
    ///   waits to the absolute read-idle deadline, exactly matching real codex-rs, which NEVER sends
    ///   a client ping (ground truth §7; verified in `codex-api` + `websocket-client`: no
    ///   `send(Message::Ping)`, no ping timer). Liveness through a long silent reasoning turn relies
    ///   solely on the pinned tungstenite fork auto-ponging inbound *server* pings (fork
    ///   `mod.rs:794`) plus that absolute deadline. Only when `POLYFLARE_WS_CLIENT_PING` is on does
    ///   `ws::executor` set [`Self::client_ping_interval`] to `Some(WS_PING_INTERVAL)`, at which
    ///   point this DOES send codex-lb-style keepalive `Ping`s (empty, content-free payload) every
    ///   [`WS_PING_INTERVAL`] during a silent read — an opt-in, documented fingerprint divergence for
    ///   deployments behind aggressive NAT/middleboxes that reap idle sockets. Even then no pong is
    ///   enforced (no pong-watchdog — the absolute read-idle deadline is the sole stall decision).
    ///   Either way, inbound `Ping`s are still auto-`Pong`ed by the tungstenite fork internally
    ///   (queued on the next write) and inbound `Pong`s ignored — neither is ever surfaced as a turn
    ///   event.
    /// - Ground truth §3 (`responses_websocket.rs:797-799`): an unexpected `Binary` frame is a
    ///   hard error.
    /// - Ground truth §3 (`:800-804`): a `Close` frame (no close-code inspection, per that same
    ///   citation) and a clean stream end (`None`) both mean "the socket closed before any
    ///   terminal frame" — both collapse to `Ok(None)` here; the caller (Task 5's turn stream)
    ///   turns that into the required `ExecError::Stream("websocket closed by server before
    ///   response.completed")`.
    pub(crate) async fn recv_frame(&mut self) -> Result<Option<String>, ExecError> {
        self.recv_frame_with_timeout(WS_READ_IDLE_TIMEOUT, self.client_ping_interval)
            .await
    }

    /// **PUBLIC WS-downstream relay recv (Task 4): read the next RAW text frame VERBATIM** — the
    /// public counterpart to [`Self::send_text`], for the relay pump to forward upstream frames back
    /// to the client unchanged. A thin `pub` wrapper over [`Self::recv_frame`], inheriting its
    /// read-idle timeout and codex-rs-faithful no-client-ping default exactly (behavior unchanged).
    /// `Ok(None)` means the socket closed before a frame. **Content-free:** the returned text IS
    /// conversation content and is NEVER logged here.
    pub async fn recv_text(&mut self) -> Result<Option<String>, ExecError> {
        self.recv_frame().await
    }

    /// **PUBLIC WS-downstream relay BETWEEN-TURNS recv:** wait for the next upstream frame with the
    /// caller's own `idle_budget` and optional keepalive-`Ping` cadence instead of the mid-turn
    /// stall deadline [`WS_READ_IDLE_TIMEOUT`].
    ///
    /// Between turns, upstream silence is HEALTHY — the socket is parked waiting for the client's
    /// next `response.create`, and the 290s deadline (a mid-turn "the model stopped streaming"
    /// stall bound) is the wrong tool: applying it there poisons a perfectly good idle socket,
    /// which kills the connection-scoped `store:false` anchor with it and forces the next anchored
    /// delta into a `previous_response_not_found` round-trip (the 2026-07-24 recurring
    /// "Reconnecting n/5" incident: every failing turn had sat idle > 290s). `idle_budget` elapsing
    /// still poisons the socket — distinguishable via [`is_read_idle_error`] so the relay can close
    /// BOTH legs honestly instead of leaving the client believing its anchor survived.
    pub async fn recv_text_idle(
        &mut self,
        idle_budget: Duration,
        ping: Option<Duration>,
    ) -> Result<Option<String>, ExecError> {
        self.recv_frame_with_timeout(idle_budget, ping).await
    }

    /// [`recv_frame`] with BOTH budgets injected, so a test can pass SHORT durations and prove the
    /// no-ping / keepalive-ping / poison behavior without sleeping the production 290s / 20s.
    /// Production calls delegate here via [`recv_frame`] with [`WS_READ_IDLE_TIMEOUT`] and
    /// [`Self::client_ping_interval`] (`None` = the codex-rs-faithful default; `Some(WS_PING_INTERVAL)`
    /// only under `POLYFLARE_WS_CLIENT_PING`).
    ///
    /// The absolute deadline is captured ONCE, before the loop, from `idle_timeout`.
    /// - **`ping == None` (the default): NO client ping ever.** Each iteration simply waits the full
    ///   remaining time to the absolute deadline; when it elapses the socket is poisoned. This is
    ///   real codex-rs behavior — it never initiates a `Ping`, it just bounds the read.
    /// - **`ping == Some(p)` (opt-in via `POLYFLARE_WS_CLIENT_PING`):** each iteration waits at most
    ///   `p` (capped so it never runs past the deadline); when a wait elapses with no frame, a
    ///   keepalive `Ping` is sent and the loop `continue`s — the deadline is re-checked at the top
    ///   and is NOT reset by the ping. So a socket that is ping-alive but data-silent is still
    ///   poisoned once cumulative silence-since-entry crosses the deadline: keepalive pings keep the
    ///   peer warm and detect a dead one fast, but never postpone the stall decision.
    ///
    /// Inbound keepalive `Ping`/`Pong` (and `Frame`) `continue` without resetting the deadline in
    /// both modes.
    pub(crate) async fn recv_frame_with_timeout(
        &mut self,
        idle_timeout: Duration,
        ping: Option<Duration>,
    ) -> Result<Option<String>, ExecError> {
        let deadline = tokio::time::Instant::now() + idle_timeout;
        loop {
            let now = tokio::time::Instant::now();
            // The absolute deadline is the SOLE stall decision (no pong-watchdog): total
            // silence-since-entry is bounded regardless of how many keepalive pings were sent in
            // between. Checked here (not via a single `timeout_at`) because a flag-on read waits only
            // a ping interval, so a ping-alive-but-data-silent socket must still be poisoned once the
            // cumulative deadline passes.
            if now >= deadline {
                self.closed = true;
                return Err(ExecError::Stream(format!(
                    "{WS_READ_IDLE_MARKER}: no frame within idle timeout"
                )));
            }
            let remaining = deadline - now;
            // Default (`ping == None`): wait the whole remaining budget — no client ping, exactly
            // codex-rs. Flag-on (`Some(p)`): wait at most one ping interval, never past the deadline.
            let wait = ping.map(|p| p.min(remaining)).unwrap_or(remaining);
            match tokio::time::timeout(wait, self.socket.next()).await {
                // The wait elapsed with no frame. When `ping` is None this is only reachable at the
                // absolute deadline (`wait == remaining`), so the top-of-loop check above poisons on
                // the next iteration — never a client ping. When `ping` is Some AND we are not yet at
                // the deadline, send one codex-lb-style keepalive ping (opt-in fingerprint divergence):
                // it keeps intermediaries seeing liveness through a long silent reasoning turn; a DEAD
                // peer makes this send fail, poisoning the socket fast instead of waiting out the whole
                // idle budget. We do NOT enforce a pong (no pong-watchdog): an inbound `Pong` is
                // ignored like any keepalive, and only the absolute deadline decides a true stall. The
                // payload is EMPTY — never content.
                Err(_elapsed) => {
                    if ping.is_some() {
                        // `timeout(wait, ...)` may return at the absolute idle deadline. Never start
                        // one final write after the read budget is spent, and cap the write itself
                        // to both the normal WS send bound and whatever remains of this idle budget.
                        let now = tokio::time::Instant::now();
                        if now >= deadline {
                            self.closed = true;
                            return Err(ExecError::Stream(format!(
                                "{WS_READ_IDLE_MARKER}: no frame within idle timeout"
                            )));
                        }
                        let send_deadline = deadline.min(now + WS_SEND_TIMEOUT);
                        let send_budget = send_deadline.saturating_duration_since(now);
                        self.send_message_until(
                            Message::Ping(Vec::new().into()),
                            send_deadline,
                            send_budget,
                        )
                        .await?;
                    }
                    continue;
                }
                Ok(next) => match next {
                    Some(Ok(Message::Text(text))) => return Ok(Some(text.as_str().to_string())),
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                    Some(Ok(Message::Binary(_))) => {
                        return Err(ExecError::Stream(
                            "unexpected binary WS frame from codex backend".to_string(),
                        ));
                    }
                    Some(Ok(Message::Frame(_))) => continue,
                    Some(Ok(Message::Close(_))) | None => {
                        // Proven dead: no further reuse — see `closed`'s doc.
                        self.closed = true;
                        return Ok(None);
                    }
                    Some(Err(e)) => {
                        self.closed = true;
                        return Err(ExecError::Stream(e.to_string()));
                    }
                },
            }
        }
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
    use std::sync::atomic::{AtomicUsize, Ordering};
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
            is_fedramp: false,
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

    #[test]
    fn relay_contract_compares_every_stable_upgrade_property_but_not_turn_state() {
        fn headers(
            turn_state: &str,
            models_etag: &str,
            reasoning_included: &str,
            model: &str,
        ) -> HeaderMap {
            let mut headers = HeaderMap::new();
            for (name, value) in [
                ("x-codex-turn-state", turn_state),
                ("x-models-etag", models_etag),
                ("x-reasoning-included", reasoning_included),
                ("openai-model", model),
            ] {
                headers.insert(
                    HeaderName::from_static(name),
                    HeaderValue::from_str(value).unwrap(),
                );
            }
            headers
        }

        let baseline =
            WsRelayContract::from_upgrade_headers(&headers("turn-1", "etag-1", "true", "model-a"));
        assert_eq!(
            baseline,
            WsRelayContract::from_upgrade_headers(&headers("turn-2", "etag-1", "false", "model-a")),
            "turn-state and reasoning header text changes must not force a downstream reconnect"
        );
        let mut reasoning_absent = headers("turn-1", "etag-1", "true", "model-a");
        reasoning_absent.remove("x-reasoning-included");
        for replacement in [
            headers("turn-1", "etag-2", "true", "model-a"),
            headers("turn-1", "etag-1", "true", "model-b"),
            reasoning_absent,
        ] {
            assert_ne!(
                baseline,
                WsRelayContract::from_upgrade_headers(&replacement),
                "each stable upgrade property must independently block transparent redial"
            );
        }
    }

    /// A TCP listener bound but NEVER `accept`ing: the kernel completes the TCP handshake into the
    /// backlog, so a client `connect` succeeds at the socket level, but the WS/HTTP-upgrade response
    /// never comes — a "black hole" that hangs the handshake read. Returns the base URL and the
    /// listener (kept alive by the caller so the port stays bound).
    async fn spawn_black_hole() -> (String, tokio::net::TcpListener) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        (format!("http://{addr}"), listener)
    }

    async fn spawn_http_reject(status: u16, retry_after: u64, body: &'static str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = [0u8; 4096];
            let _ = stream.read(&mut request).await;
            let reason = if status == 401 {
                "Unauthorized"
            } else {
                "Error"
            };
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n\
                 Retry-After: {retry_after}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.expect("write");
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn non_426_handshake_http_status_remains_structured_and_actionable() {
        let base = spawn_http_reject(401, 7, "denied").await;
        let outcome = connect_detailed(&test_account(base), &[]).await;
        match outcome {
            ConnectOutcome::Failed(ExecError::UpstreamHttp(response)) => {
                assert_eq!(response.signal.status, 401);
                assert_eq!(response.signal.retry_after, Some(7));
                assert_eq!(response.body.as_ref(), b"denied");
            }
            _ => panic!("expected a structured HTTP handshake failure"),
        }
    }

    /// A WS server that completes the handshake then goes permanently silent — never sends a frame,
    /// never closes. The client connects fine, but any `recv_frame` blocks forever (until poisoned
    /// by the read-idle deadline). Must pass `Some(WebSocketConfig::default())` for the same
    /// permessage-deflate reason `spawn_header_capture_server` documents.
    async fn spawn_silent_ws_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                if let Ok(_ws) = tokio_tungstenite::accept_async_with_config(
                    stream,
                    Some(WebSocketConfig::default()),
                )
                .await
                {
                    // Hold the socket open, forever silent — never send, never close.
                    std::future::pending::<()>().await;
                }
            }
        });
        format!("http://{addr}")
    }

    /// A hung dial (TCP connects into the backlog, but the WS-upgrade response never comes) must
    /// self-terminate within the injected budget as a generic `Failed`, not hang until the OS TCP
    /// timeout and never as `UpgradeRequired`. Uses a 50ms injected timeout — no 30s sleep.
    #[tokio::test]
    async fn connect_detailed_times_out_on_a_black_hole() {
        let (base, _listener) = spawn_black_hole().await;
        let account = test_account(base);

        let start = std::time::Instant::now();
        let outcome = connect_detailed_with_timeout(&account, &[], Duration::from_millis(50)).await;
        let elapsed = start.elapsed();

        assert!(
            matches!(outcome, ConnectOutcome::Failed(_)),
            "a hung dial must map to Failed, never UpgradeRequired or a hang"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "must return within the injected 50ms budget (well under the 30s default), took {elapsed:?}"
        );
    }

    /// A stalled-but-alive socket (handshake completed, backend then permanently silent) must be
    /// poisoned so it is evicted on reuse rather than re-stalling every turn: `recv_frame` returns
    /// the marked idle error AND `is_closed()` becomes true. Uses a 50ms injected deadline — no 300s
    /// sleep.
    #[tokio::test]
    async fn recv_frame_poisons_conn_on_read_idle() {
        let base = spawn_silent_ws_server().await;
        let account = test_account(base);
        let mut conn = WsConn::connect(&account, &[]).await.expect("connect");
        assert!(
            !conn.is_closed(),
            "a freshly connected socket is not closed"
        );

        // Flag-on path (`Some(30ms)`): ping interval (30ms) is SHORTER than idle (120ms), so the
        // client keepalive pings fire repeatedly into the silent-but-open socket (which accepts them
        // into its buffer without ever reading), proving the pings do NOT prevent the absolute
        // read-idle deadline from still poisoning the socket — the no-pong-watchdog property. No 290s
        // sleep.
        let start = std::time::Instant::now();
        let result = conn
            .recv_frame_with_timeout(Duration::from_millis(120), Some(Duration::from_millis(30)))
            .await;
        let elapsed = start.elapsed();

        match result {
            Err(ExecError::Stream(msg)) => assert!(
                msg.contains(WS_READ_IDLE_MARKER),
                "idle error must carry the read-idle marker, got: {msg}"
            ),
            other => panic!("expected a read-idle Stream error, got {other:?}"),
        }
        assert!(
            conn.is_closed(),
            "a read-idle socket must be poisoned (is_closed → evicted on next reuse)"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "must return within the injected 50ms budget (well under the 300s default), took {elapsed:?}"
        );
    }

    /// A send whose peer never drains the socket must not stall a turn forever: a frame far larger
    /// than any socket send buffer + peer receive window, written to a server that never reads,
    /// backpressures (WouldBlock) and never completes. `send_frame_with_timeout` must return an
    /// `Upstream` send timeout AND poison the connection (`is_closed()`) within the SHORT injected
    /// budget — not hang until the OS TCP timeout. A 200ms injected budget drives it with no 30s
    /// production sleep. (`spawn_silent_ws_server` uses `WebSocketConfig::default()` → no
    /// permessage-deflate is negotiated, so the filler bytes hit the wire uncompressed and actually
    /// fill the buffers; without deflate a repetitive payload cannot be compressed away.)
    #[tokio::test]
    async fn send_frame_times_out_on_a_stalled_write() {
        let base = spawn_silent_ws_server().await;
        let account = test_account(base);
        let mut conn = WsConn::connect(&account, &[]).await.expect("connect");
        assert!(
            !conn.is_closed(),
            "a freshly connected socket is not closed"
        );

        // 16 MiB — comfortably beyond any default TCP send buffer + peer receive window, so the very
        // first flush backpressures and the send future parks instead of completing.
        let big = serde_json::json!({ "filler": "x".repeat(16 * 1024 * 1024) });

        let start = std::time::Instant::now();
        let result = conn
            .send_frame_with_timeout(&big, Duration::from_millis(200))
            .await;
        let elapsed = start.elapsed();

        match result {
            Err(ExecError::Upstream(msg)) => assert!(
                msg.contains("send timed out"),
                "a stalled send must surface a send-timeout Upstream error, got: {msg}"
            ),
            other => panic!("expected a send-timeout Upstream error, got {other:?}"),
        }
        assert!(
            conn.is_closed(),
            "a send-timeout socket must be poisoned (is_closed → evicted on next reuse)"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "must return within the injected 200ms budget (well under the 30s default), took {elapsed:?}"
        );
    }

    /// A WS server that completes the handshake, then stays SILENT (sends no frame) while it keeps
    /// reading the socket — counting every inbound `Ping` (and letting tungstenite auto-`Pong` it) —
    /// until it has observed at least one keepalive ping, at which point it sends a single text
    /// frame. This is the mirror image of `spawn_silent_ws_server`: it proves the client emits
    /// keepalive pings during a silent read AND that a real frame still comes through afterward.
    /// Returns the base URL and a shared ping counter. `Some(WebSocketConfig::default())` for the
    /// same permessage-deflate reason the other helper servers document.
    async fn spawn_ping_observing_then_frame_server(frame: String) -> (String, Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let ping_count = Arc::new(AtomicUsize::new(0));
        let counter = ping_count.clone();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                if let Ok(mut ws) = tokio_tungstenite::accept_async_with_config(
                    stream,
                    Some(WebSocketConfig::default()),
                )
                .await
                {
                    // Read (never proactively send) so inbound keepalive pings are surfaced and
                    // auto-ponged. A 2s safety deadline bounds the wait so a missing ping fails the
                    // ping assertion rather than hanging the test.
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
                    loop {
                        match tokio::time::timeout_at(deadline, ws.next()).await {
                            Ok(Some(Ok(Message::Ping(_)))) => {
                                counter.fetch_add(1, Ordering::SeqCst);
                                break; // saw a keepalive ping — now respond
                            }
                            // Any other inbound frame (e.g. a Pong) is ignored; keep reading.
                            Ok(Some(Ok(_))) => continue,
                            // Client went away, or the safety deadline elapsed: stop waiting and
                            // fall through to send the frame anyway (the test's ping assert decides).
                            Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
                        }
                    }
                    let _ = ws.send(Message::Text(frame.into())).await;
                    // Hold the socket open briefly so the client can read the frame before EOF.
                    let _ = tokio::time::timeout(
                        Duration::from_millis(500),
                        std::future::pending::<()>(),
                    )
                    .await;
                }
            }
        });
        (format!("http://{addr}"), ping_count)
    }

    /// Flag-on (`POLYFLARE_WS_CLIENT_PING`) codex-lb-parity path: with `ping = Some(...)`, during a
    /// silent read the client must send a keepalive `Ping` (~every `WS_PING_INTERVAL` in production)
    /// so intermediaries see liveness through a long silent codex reasoning turn — and a real frame
    /// arriving afterward is still returned undisturbed. A short injected interval (`Some(30ms)`)
    /// under a longer `idle_timeout` (1s) drives it without any 20s sleep; the server proves the ping
    /// actually reached the wire by counting it. (The DEFAULT `None` path is the separate
    /// `recv_frame_with_no_client_ping_never_pings_during_silence` fidelity test above.)
    #[tokio::test]
    async fn recv_frame_sends_keepalive_ping_during_silence() {
        let expected =
            r#"{"type":"response.completed","response":{"id":"resp_probe"}}"#.to_string();
        let (base, ping_count) = spawn_ping_observing_then_frame_server(expected.clone()).await;
        let account = test_account(base);
        let mut conn = WsConn::connect(&account, &[]).await.expect("connect");

        let result = conn
            .recv_frame_with_timeout(Duration::from_secs(1), Some(Duration::from_millis(30)))
            .await;

        assert!(
            ping_count.load(Ordering::SeqCst) >= 1,
            "the client must send at least one keepalive ping during the silent read"
        );
        assert_eq!(
            result.expect("read must succeed, not poison"),
            Some(expected),
            "the frame that arrives after the keepalive pings must still be returned verbatim"
        );
        assert!(
            !conn.is_closed(),
            "a successful read (frame returned) must NOT poison the connection"
        );
    }

    /// A ping interval that lands exactly on the absolute idle deadline must not cause one final
    /// keepalive write after the read budget has already expired. The deadline owns the decision:
    /// the connection is poisoned and no ping reaches the peer.
    #[tokio::test]
    async fn recv_frame_does_not_ping_at_the_absolute_deadline() {
        let unused_frame =
            r#"{"type":"response.completed","response":{"id":"resp_never"}}"#.to_string();
        let (base, ping_count) = spawn_ping_observing_then_frame_server(unused_frame).await;
        let account = test_account(base);
        let mut conn = WsConn::connect(&account, &[]).await.expect("connect");

        let result = conn
            .recv_frame_with_timeout(Duration::from_millis(80), Some(Duration::from_millis(80)))
            .await;
        // Give the server task a scheduling turn so a mistakenly emitted ping cannot race the
        // assertion below.
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert_eq!(
            ping_count.load(Ordering::SeqCst),
            0,
            "the absolute idle deadline must win before any keepalive send begins"
        );
        assert!(
            matches!(result, Err(ExecError::Stream(ref msg)) if msg.contains(WS_READ_IDLE_MARKER)),
            "deadline expiry must remain the marked read-idle error, got {result:?}"
        );
        assert!(conn.is_closed(), "deadline expiry must poison the socket");
    }

    /// The absolute send helper must choose an already-ready deadline before it polls an equally
    /// ready socket write. This guards the parked-read path against scheduler delay between
    /// calculating its remaining idle budget and beginning a keepalive send.
    #[tokio::test]
    async fn send_message_until_expired_deadline_never_emits_ping() {
        let unused_frame =
            r#"{"type":"response.completed","response":{"id":"resp_never"}}"#.to_string();
        let (base, ping_count) = spawn_ping_observing_then_frame_server(unused_frame).await;
        let account = test_account(base);
        let mut conn = WsConn::connect(&account, &[]).await.expect("connect");

        let result = conn
            .send_message_until(
                Message::Ping(Vec::new().into()),
                tokio::time::Instant::now(),
                Duration::ZERO,
            )
            .await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert!(
            matches!(result, Err(ExecError::Upstream(ref msg)) if msg.contains("send timed out")),
            "expired absolute send deadline must surface a bounded timeout, got {result:?}"
        );
        assert_eq!(
            ping_count.load(Ordering::SeqCst),
            0,
            "an expired absolute deadline must win before the socket send is polled"
        );
        assert!(
            conn.is_closed(),
            "send deadline expiry must poison the socket"
        );
    }

    /// The codex-rs-faithful DEFAULT (`POLYFLARE_WS_CLIENT_PING` off ⇒ `ping = None`): during a
    /// silent read the client must NEVER initiate a `Ping` — matching real codex-rs, which never
    /// pings (ground truth §7; verified in `codex-api` + `websocket-client`). It still bounds the
    /// read with the absolute idle deadline, so a stalled socket is poisoned exactly as before —
    /// just without any client-initiated keepalive. THE key fidelity test. Reuses the ping-counting
    /// server: with a short idle (150ms) well under that server's 2s frame-fallback, the client
    /// poisons first and the ping counter must stay 0.
    #[tokio::test]
    async fn recv_frame_with_no_client_ping_never_pings_during_silence() {
        let unused_frame =
            r#"{"type":"response.completed","response":{"id":"resp_never"}}"#.to_string();
        let (base, ping_count) = spawn_ping_observing_then_frame_server(unused_frame).await;
        let account = test_account(base);
        let mut conn = WsConn::connect(&account, &[]).await.expect("connect");

        // ping = None ⇒ codex-rs default: no client ping ever, just the absolute idle deadline.
        let start = std::time::Instant::now();
        let result = conn
            .recv_frame_with_timeout(Duration::from_millis(150), None)
            .await;
        let elapsed = start.elapsed();

        assert_eq!(
            ping_count.load(Ordering::SeqCst),
            0,
            "with client-ping OFF (None) the client must NEVER send a keepalive ping — this is the \
             codex-rs-faithful default (ground truth §7)"
        );
        match result {
            Err(ExecError::Stream(msg)) => assert!(
                msg.contains(WS_READ_IDLE_MARKER),
                "a silent socket must still poison at the absolute idle deadline, got: {msg}"
            ),
            other => panic!("expected a read-idle Stream error, got {other:?}"),
        }
        assert!(
            conn.is_closed(),
            "a read-idle socket must be poisoned even with no client ping"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "must poison at the injected 150ms deadline, well before the server's 2s fallback, \
             took {elapsed:?}"
        );
    }

    /// The PUBLIC relay API (Task 4): `dial_upstream` opens a codex-parity WS to the mock;
    /// `send_text` forwards a frame BYTE-IDENTICAL (the verbatim crux — a deliberate key order +
    /// interior whitespace must survive, which a serde reparse would destroy); `recv_text` reads a
    /// scripted frame back. Exercises the exact seam `polyflare-server`'s relay dial calls.
    #[tokio::test]
    async fn dial_upstream_connects_to_the_mock() {
        // Deliberately UNSORTED keys (`z_before_a`, `type` first) + doubled interior whitespace: a
        // parse-then-reserialize would sort keys and collapse the spaces, so an exact-bytes match
        // proves `send_text` never reparsed the frame.
        let raw =
            r#"{"type":"response.create",  "z_before_a":1,  "a_after_z":2,"input":[]}"#.to_string();
        let mock = MockWsUpstream::new(ScriptedTurn::normal(vec![serde_json::json!(
            {"type":"response.output_text.delta","delta":"hi"}
        )
        .to_string()]))
        .capturing_raw_frames();
        let base = mock.clone().spawn().await;
        let account = test_account(base);

        let mut conn = dial_upstream(&account, &[]).await.expect("dial_upstream");
        assert_eq!(
            mock.handshake_count(),
            1,
            "dial_upstream must establish exactly one upstream WS"
        );

        conn.send_text(raw.clone()).await.expect("send_text");

        // The mock records on its server task, so poll briefly for the frame to land.
        for _ in 0..50 {
            if !mock.raw_frames().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            mock.raw_frames(),
            vec![raw],
            "the mock must receive the frame BYTE-IDENTICAL — verbatim, no key reorder / whitespace \
             drift (a serde reparse would fail this)"
        );

        // `recv_text` reads the scripted delta frame back, verbatim.
        let frame = conn
            .recv_text()
            .await
            .expect("recv_text ok")
            .expect("a frame, not a close");
        let v: Value = serde_json::from_str(&frame).expect("valid json frame");
        assert_eq!(v["type"], "response.output_text.delta");
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
        assert!(conn.last_item_hashes.is_none());
        assert_eq!(conn.last_non_input_fingerprint, None);
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
                let callback =
                    move |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
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
            ("x-openai-fedramp".to_string(), "true".to_string()),
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
        assert_eq!(
            headers.get("chatgpt-account-id").unwrap(),
            "chatgpt-acct-xyz"
        );

        // §7.1: the single easiest thing to get wrong — must be ABSENT, opposite of the HTTP path.
        assert!(
            headers.get("x-codex-turn-state").is_none(),
            "x-codex-turn-state must NOT be a WS handshake header"
        );

        // Dumb-relay: other forwarded headers pass through untouched.
        assert_eq!(headers.get("x-client-request-id").unwrap(), "thread-123");
        assert!(
            headers.get("x-openai-fedramp").is_none(),
            "a non-FedRAMP selected account must remove the client's stale routing header"
        );

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

    #[tokio::test]
    async fn handshake_sets_fedramp_from_the_selected_account() {
        let (base_url, captured) = spawn_header_capture_server().await;
        let mut account = test_account(base_url);
        account.is_fedramp = true;

        let _conn = WsConn::connect(
            &account,
            &[("x-openai-fedramp".to_string(), "false".to_string())],
        )
        .await
        .expect("connect");

        let headers = captured.lock().unwrap().clone().expect("headers captured");
        assert_eq!(headers.get("x-openai-fedramp").unwrap(), "true");
        assert_eq!(headers.get_all("x-openai-fedramp").iter().count(), 1);
    }

    #[tokio::test]
    async fn handshake_removes_forwarded_account_id_when_selected_account_has_none() {
        let (base_url, captured) = spawn_header_capture_server().await;
        let mut account = test_account(base_url);
        account.chatgpt_account_id = None;

        let _conn = WsConn::connect(
            &account,
            &[(
                "chatgpt-account-id".to_string(),
                "client-stale-workspace".to_string(),
            )],
        )
        .await
        .expect("connect");

        assert!(captured
            .lock()
            .unwrap()
            .clone()
            .expect("headers captured")
            .get("chatgpt-account-id")
            .is_none());
    }

    /// A WS server that completes the handshake, optionally injecting an `x-codex-turn-state`
    /// header into the UPGRADE RESPONSE (via the `accept_hdr` callback's mutable `Response`) — the
    /// server-side of the primary turn-state capture path (`responses_websocket.rs:529-535`). Holds
    /// the socket open briefly so the client's handshake response is fully read before teardown.
    /// `Some(WebSocketConfig::default())` for the same permessage-deflate reason
    /// `spawn_header_capture_server` documents.
    async fn spawn_turn_state_response_server(turn_state: Option<String>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let ts = turn_state.clone();
                let callback =
                    move |_req: &Request, mut resp: Response| -> Result<Response, ErrorResponse> {
                        if let Some(ts) = &ts {
                            resp.headers_mut().insert(
                                HeaderName::from_static("x-codex-turn-state"),
                                HeaderValue::from_str(ts).expect("valid header value"),
                            );
                        }
                        Ok(resp)
                    };
                if let Ok(_ws) = tokio_tungstenite::accept_hdr_async_with_config(
                    stream,
                    callback,
                    Some(WebSocketConfig::default()),
                )
                .await
                {
                    // Hold the socket open briefly so the client can read the 101 response (with
                    // our header) before EOF — no proactive send, no frame.
                    let _ = tokio::time::timeout(
                        Duration::from_millis(200),
                        std::future::pending::<()>(),
                    )
                    .await;
                }
            }
        });
        format!("http://{addr}")
    }

    /// Capture path (`responses_websocket.rs:529-535`): the server-issued `x-codex-turn-state` on
    /// the WS UPGRADE response header is captured onto the `WsConn` as its one-shot
    /// `upgrade_turn_state`; an absent header yields `None` (never fabricated).
    #[tokio::test]
    async fn connect_captures_upgrade_turn_state_from_the_upgrade_response_header() {
        // Present ⇒ captured verbatim.
        let base = spawn_turn_state_response_server(Some("ts-123".to_string())).await;
        let conn = WsConn::connect(&test_account(base), &[])
            .await
            .expect("connect");
        assert_eq!(
            conn.upgrade_turn_state.as_deref(),
            Some("ts-123"),
            "the server-issued upgrade-response turn-state must be captured onto the WsConn"
        );

        // Absent ⇒ None (never fabricated).
        let base = spawn_turn_state_response_server(None).await;
        let conn = WsConn::connect(&test_account(base), &[])
            .await
            .expect("connect");
        assert_eq!(
            conn.upgrade_turn_state, None,
            "an absent upgrade-response header must leave upgrade_turn_state None — never fabricated"
        );
    }
}
