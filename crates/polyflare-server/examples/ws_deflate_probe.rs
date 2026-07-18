//! ws_deflate_probe — empirical check: with the M5a OpenAI-fork adoption
//! (`.superpowers/sdd/m5a-deflate-forks-report.md`), can PolyFlare actually decode the
//! `permessage-deflate` frames the LIVE Codex backend sends once it confirms the offer?
//!
//! History: `crates/polyflare-codex/src/ws/conn.rs` (`WsConn::connect`) originally sent
//! `Sec-WebSocket-Extensions: permessage-deflate` on every WS handshake for codex-parity
//! fingerprinting, but stock `tokio-tungstenite` 0.26 has NO deflate feature at all — it could
//! offer the extension but not negotiate or decompress it
//! (`.superpowers/sdd/m5a-deflate-probe-report.md` measured this: the backend confirmed the
//! offer, then the first frame killed the connection, "Reserved bits are non-zero"). The offer
//! was withheld (commit `c497a39`) until real decode support existed. It now does: this crate is
//! pinned to the same `openai-oss-forks/tokio-tungstenite` + `openai-oss-forks/tungstenite-rs`
//! revs codex-rs itself uses (workspace root `Cargo.toml`'s `[patch.crates-io]`). This probe
//! re-measures with that support in place.
//!
//! Connects TWICE, same live account, each via `connect_async_with_config` with a real
//! `WebSocketConfig` (the library's own negotiation/decode mechanism — never a hand-written
//! `Sec-WebSocket-Extensions` header; the fork's client handshake rejects a server-confirmed
//! extension the client's own config didn't declare):
//!   1. WITH the offer — `extensions.permessage_deflate = Some(DeflateConfig::default())`,
//!      exactly mirroring `ws::conn::WsConn::connect`'s config (same `OpenAI-Beta` value too).
//!   2. WITHOUT the offer — control, `WebSocketConfig::default()` (no extensions configured),
//!      isolates the effect of the offer alone.
//!
//! For each connect: prints the full 101 response header list (names + values — these are
//! SERVER-originated response headers, never our credentials, but `set-cookie`/`cookie` are
//! redacted defensively anyway) and calls out any `Sec-WebSocket-Extensions` echo specifically.
//! Then sends one minimal `response.create` turn (prompt = "hi") and reports, frame by frame,
//! whether the payload parses as plain UTF-8 JSON or arrives unreadable.
//!
//! SAFETY: never prints a token, cookie, refresh token, or Authorization header value — request
//! headers are built (mirroring `WsConn::connect`) but never printed; only response headers are
//! printed, with `set-cookie`/`cookie` redacted. No conversation content beyond the literal "hi".
//!
//! Run: cargo run -p polyflare-server --example ws_deflate_probe --release
//!
//! This is a manual, live-credentials probe — never wired into CI (CI has no network).

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use polyflare_codex::codex_headers::{
    codex_user_agent, originator, TurnIdentity, CODEX_CLI_VERSION,
};
use polyflare_codex::oauth::{self, OAuthClient};
use polyflare_codex::CodexExecutor;
use polyflare_store::{Store, TokenCipher};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::Message;
use tungstenite::extensions::compression::deflate::DeflateConfig;
use tungstenite::extensions::ExtensionsConfig;
use tungstenite::protocol::WebSocketConfig;

const WS_URL: &str = "wss://chatgpt.com/backend-api/codex/responses";
const AUTH_BASE: &str = "https://auth.openai.com";
/// Ground truth §7.2 / `WsConn::connect` (`conn.rs:34`) — inserted exactly once, exact value.
const OPENAI_BETA_WS: &str = "responses_websockets=2026-02-06";
fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}
fn data_dir() -> PathBuf {
    std::env::var("POLYFLARE_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(std::env::var("HOME").expect("HOME")).join(".polyflare"))
}

const INSTRUCTIONS: &str =
    "You are a terse assistant in a WebSocket extension probe. Reply with exactly one short word.";

fn user_msg(text: &str) -> Value {
    json!({"type":"message","role":"user","content":[{"type":"input_text","text":text}]})
}

/// Same shape as `ws_vs_sse_probe.rs`'s `codex_headers` — the codex identity + auth headers that
/// ride every WS handshake, before the two WS-specific headers (`OpenAI-Beta`,
/// `Sec-WebSocket-Extensions`) that `WsConn::connect` adds on top.
fn codex_headers(
    session_key: &str,
    bearer: &str,
    account_id: Option<&str>,
) -> Vec<(String, String)> {
    let id = TurnIdentity::derive(session_key);
    let mut h = vec![
        ("authorization".to_string(), format!("Bearer {bearer}")),
        (
            "user-agent".to_string(),
            codex_user_agent(CODEX_CLI_VERSION),
        ),
        ("originator".to_string(), originator().to_string()),
        ("session-id".to_string(), id.session_id.clone()),
        ("thread-id".to_string(), id.thread_id.clone()),
        ("x-client-request-id".to_string(), id.thread_id.clone()),
        ("x-codex-window-id".to_string(), id.window_id.clone()),
        ("x-codex-turn-metadata".to_string(), id.turn_metadata_json()),
    ];
    if let Some(a) = account_id {
        h.push(("chatgpt-account-id".to_string(), a.to_string()));
    }
    h
}

fn ws_body(model: &str, nonce: &str, input: Value) -> String {
    let b = json!({
        "type": "response.create",
        "model": model,
        "instructions": INSTRUCTIONS,
        "input": input,
        "tool_choice": "auto",
        "parallel_tool_calls": false,
        "reasoning": {"effort": "low"},
        "store": false,
        "stream": true,
        "include": [],
        "prompt_cache_key": nonce,
    });
    serde_json::to_string(&b).unwrap()
}

/// Header names whose VALUES must never be printed, even though these are response headers (a
/// misbehaving/reflecting server could echo something sensitive back). Defensive, not expected.
fn is_sensitive_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "set-cookie" | "cookie" | "authorization"
    )
}

/// Build the WS upgrade request: `codex_headers` first, then `OpenAI-Beta` (always — mirrors
/// `WsConn::connect`). No `Sec-WebSocket-Extensions` header here — that's negotiated by the
/// library itself from the `WebSocketConfig` passed to `connect_async_with_config` (see
/// `ws_config_for`), the same mechanism `WsConn::connect` uses.
fn build_request(
    hdrs: &[(String, String)],
) -> tokio_tungstenite::tungstenite::handshake::client::Request {
    let mut req = WS_URL.into_client_request().expect("ws request");
    let headers = req.headers_mut();
    for (name, value) in hdrs {
        headers.insert(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    headers.insert(
        HeaderName::from_static("openai-beta"),
        HeaderValue::from_static(OPENAI_BETA_WS),
    );
    req
}

/// Mirrors `ws::conn::WsConn`'s (private) `ws_config()` exactly: `permessage_deflate =
/// Some(DeflateConfig::default())` when offering, `WebSocketConfig::default()` (no extensions)
/// otherwise. Duplicated here (rather than imported) because this probe lives in a different
/// crate (`polyflare-server`) than `WsConn` (`polyflare-codex`), and `ws_config()` is private —
/// kept private there because production code has no other caller for it.
fn ws_config_for(offer_deflate: bool) -> WebSocketConfig {
    let mut config = WebSocketConfig::default();
    if offer_deflate {
        let mut extensions = ExtensionsConfig::default();
        extensions.permessage_deflate = Some(DeflateConfig::default());
        config.extensions = extensions;
    }
    config
}

/// One frame's readability classification.
enum FrameReadability {
    PlainUtf8Json,
    PlainUtf8NotJson,
    InvalidUtf8OrBinary,
}

/// Connect, print the full (secret-free) response header list, send one `response.create` turn,
/// and classify every frame that comes back as readable plain JSON or not.
async fn run_variant(label: &str, hdrs: &[(String, String)], model: &str, offer_deflate: bool) {
    println!("\n■ {label}  (offer permessage-deflate = {offer_deflate})");
    let req = build_request(hdrs);
    let config = ws_config_for(offer_deflate);
    let connect = tokio::time::timeout(
        Duration::from_secs(20),
        tokio_tungstenite::connect_async_with_config(req, Some(config), false),
    )
    .await;
    let (mut ws, resp) = match connect {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => {
            println!("  HANDSHAKE FAILED: {e}");
            return;
        }
        Err(_) => {
            println!("  HANDSHAKE TIMEOUT (>20s)");
            return;
        }
    };
    println!("  status: HTTP {}", resp.status());
    println!("  response headers (verbatim, secret-free):");
    let mut saw_extensions_echo = false;
    for (name, value) in resp.headers() {
        let name_str = name.as_str();
        if is_sensitive_header(name_str) {
            println!("    {name_str}: <redacted>");
            continue;
        }
        let value_str = value.to_str().unwrap_or("<non-utf8 value>");
        println!("    {name_str}: {value_str}");
        if name_str.eq_ignore_ascii_case("sec-websocket-extensions") {
            saw_extensions_echo = true;
        }
    }
    if saw_extensions_echo {
        println!("  >>> Sec-WebSocket-Extensions ECHOED by server (extension confirmed)");
    } else {
        println!("  >>> no Sec-WebSocket-Extensions in the response (offer not confirmed)");
    }

    // Send one minimal turn and classify frame readability.
    let nonce = format!("deflateprobe-{}", now_millis());
    let body = ws_body(model, &nonce, json!([user_msg("hi")]));
    if let Err(e) = ws.send(Message::Text(body.into())).await {
        println!("  SEND FAILED: {e}");
        return;
    }
    let mut frame_no = 0usize;
    let mut any_unreadable = false;
    let read = tokio::time::timeout(Duration::from_secs(60), async {
        while let Some(msg) = ws.next().await {
            match msg {
                Ok(Message::Text(txt)) => {
                    frame_no += 1;
                    let is_json = serde_json::from_str::<Value>(txt.as_str()).is_ok();
                    println!(
                        "  frame {frame_no}: Text, {} bytes, valid-json={is_json}",
                        txt.len()
                    );
                    if txt.contains("response.completed") || txt.contains("\"response.failed\"") {
                        break;
                    }
                }
                Ok(Message::Binary(b)) => {
                    frame_no += 1;
                    any_unreadable = true;
                    let readability = match std::str::from_utf8(&b) {
                        Ok(s) if serde_json::from_str::<Value>(s).is_ok() => {
                            FrameReadability::PlainUtf8Json
                        }
                        Ok(_) => FrameReadability::PlainUtf8NotJson,
                        Err(_) => FrameReadability::InvalidUtf8OrBinary,
                    };
                    let tag = match readability {
                        FrameReadability::PlainUtf8Json => {
                            "Binary-but-valid-utf8-json (unexpected)"
                        }
                        FrameReadability::PlainUtf8NotJson => "Binary, utf8 but not JSON",
                        FrameReadability::InvalidUtf8OrBinary => {
                            "Binary, INVALID UTF-8 (compressed/unreadable)"
                        }
                    };
                    println!("  frame {frame_no}: Binary, {} bytes — {tag}", b.len());
                }
                Ok(Message::Close(frame)) => {
                    println!("  connection closed: {frame:?}");
                    break;
                }
                Ok(_) => {}
                Err(e) => {
                    println!("  RECV ERROR (possible protocol/decompression failure): {e}");
                    any_unreadable = true;
                    break;
                }
            }
        }
    })
    .await;
    if read.is_err() {
        println!("  TIMEOUT (>60s) waiting for completion");
    }
    let _ = ws.send(Message::Close(None)).await;
    println!("  → frames observed: {frame_no}, any binary/unreadable frame: {any_unreadable}");
}

#[tokio::main]
async fn main() {
    // Install the aws-lc-rs rustls provider (constructing CodexExecutor does it) so
    // tokio-tungstenite's rustls connector picks it up for the wss handshake.
    let _exec = CodexExecutor::new().expect("executor / rustls provider");

    let model =
        std::env::var("POLYFLARE_PROBE_MODEL").unwrap_or_else(|_| "gpt-5.6-sol".to_string());

    let dir = data_dir();
    let cipher = TokenCipher::load_or_create(&dir.join("key")).expect("key");
    let store = Store::open(&dir.join("store.db")).await.expect("store");
    let oauth = OAuthClient::new(AUTH_BASE).expect("oauth");

    let all = store.accounts().list().await.expect("list");
    let acct = all
        .into_iter()
        .find(|a| a.provider == "codex" && a.status == "active")
        .expect("an active codex account");
    let (row_a, tokens) = store
        .accounts()
        .get_with_tokens(&acct.id, &cipher)
        .await
        .unwrap()
        .unwrap();
    let mut bearer = tokens.access_token.clone();
    if oauth::should_refresh(oauth::token_exp(&bearer), row_a.last_refresh, now_secs()) {
        if let Ok(r) = oauth.refresh(&tokens.refresh_token).await {
            bearer = r.tokens.access_token;
            eprintln!("(token refreshed)");
        }
    }
    let account_id = row_a.chatgpt_account_id.clone();

    println!("════════════════════════════════════════════════════════════════════════════");
    println!(
        "permessage-deflate acceptance probe — LIVE Codex backend   model={model}  plan=[{}]",
        row_a.plan_type
    );
    println!("════════════════════════════════════════════════════════════════════════════");

    // Two independent connects (independent nonces/session keys so each gets its own identity).
    let nonce_with = format!("deflateprobe-with-{}", now_millis());
    let hdrs_with = codex_headers(&nonce_with, &bearer, account_id.as_deref());
    run_variant("WITH permessage-deflate offer", &hdrs_with, &model, true).await;

    let nonce_without = format!("deflateprobe-without-{}", now_millis());
    let hdrs_without = codex_headers(&nonce_without, &bearer, account_id.as_deref());
    run_variant(
        "WITHOUT permessage-deflate offer (control)",
        &hdrs_without,
        &model,
        false,
    )
    .await;

    println!("\n════════════════════════════════════════════════════════════════════════════");
    println!(
        "Read the two \">>>\" lines above for the Sec-WebSocket-Extensions echo, and the per-frame \
         readability lines, to determine the verdict. See also \
         .superpowers/sdd/m5a-deflate-probe-report.md for the recorded verdict."
    );
    println!("════════════════════════════════════════════════════════════════════════════\n");
}
