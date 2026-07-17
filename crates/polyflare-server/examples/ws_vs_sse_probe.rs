//! ws_vs_sse_probe — POC: does the Codex backend accept a WebSocket transport, and how much does
//! incremental continuation (previous_response_id + ONLY the new items, over a live WS) beat the
//! HTTP-SSE full-resend PolyFlare does today?
//!
//! Uses ONE live account from ~/.polyflare/store.db. Faithful to real codex:
//!   - WS URL = wss://chatgpt.com/backend-api/codex/responses (codex-api provider.rs)
//!   - request frame = Text of {"type":"response.create", ...ResponseCreateWsRequest}
//!     (codex-api common.rs); store:false; previous_response_id + `generate` allowed
//!   - same auth + codex identity headers ride the upgrade
//!
//! It runs:
//!   1. WS connect (prints the 101 handshake + turn-state header)
//!   2. WS turn 1 (full input) → capture response id
//!   3. WS turn 2 INCREMENTAL (previous_response_id + just "reply ACK") → TTFT + upload bytes
//!   4. HTTP-SSE turn 2 FULL RESEND (whole history) → TTFT + upload bytes
//!   5. bonus: WS `generate:false` warmup (prefill-only, no generation)
//!
//! SAFETY: prints only timings + byte counts, never tokens/account-ids/bodies.
//!
//! Run: cargo run -p polyflare-server --example ws_vs_sse_probe --release

use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use polyflare_codex::codex_headers::{
    codex_user_agent, originator, TurnIdentity, CODEX_CLI_VERSION,
};
use polyflare_codex::oauth::{self, OAuthClient};
use polyflare_codex::CodexExecutor;
use polyflare_store::{Store, TokenCipher};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

const HTTP_BASE: &str = "https://chatgpt.com/backend-api/codex";
const WS_URL: &str = "wss://chatgpt.com/backend-api/codex/responses";
const AUTH_BASE: &str = "https://auth.openai.com";

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

fn user_msg(text: &str) -> Value {
    json!({"type":"message","role":"user","content":[{"type":"input_text","text":text}]})
}
fn assistant_msg(text: &str) -> Value {
    json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":text}]})
}

const INSTRUCTIONS: &str =
    "You are a terse assistant in a transport latency probe. Answer in as few \
     tokens as possible. When asked to reply with a word, reply with exactly that word.";

fn filler(nonce: &str, ctx_kb: usize) -> String {
    let target = ctx_kb * 1024;
    let mut s = String::with_capacity(target + 256);
    let mut i = 0usize;
    while s.len() < target {
        s.push_str(&format!(
            "line {i:05} [{nonce}] the quick brown fox jumps over the lazy dog while the proxy \
             measures websocket vs sse continuation latency.\n"
        ));
        i += 1;
    }
    s
}

/// The codex identity + auth headers, as (name, value) pairs, for either transport.
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

fn ws_body(
    model: &str,
    nonce: &str,
    input: Value,
    prev: Option<&str>,
    generate: Option<bool>,
) -> String {
    let mut b = json!({
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
    if let Some(p) = prev {
        b["previous_response_id"] = json!(p);
    }
    if let Some(g) = generate {
        b["generate"] = json!(g);
    }
    serde_json::to_string(&b).unwrap()
}

#[derive(Default)]
struct T {
    created: Option<u128>,
    first_delta: Option<u128>,
    completed: Option<u128>,
    total: u128,
    up_bytes: usize,
    resp_id: Option<String>,
    err: Option<String>,
}
impl T {
    fn ttft(&self) -> Option<u128> {
        self.first_delta.or(self.created)
    }
}

fn find_resp_id(buf: &str) -> Option<String> {
    let i = buf.find("resp_")?;
    let rest = &buf[i..];
    let end = rest
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
        .unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

fn scan(buf: &str, t: &mut T, t0: Instant) {
    if t.created.is_none() && buf.contains("response.created") {
        t.created = Some(t0.elapsed().as_millis());
    }
    if t.first_delta.is_none() && buf.contains(".delta\"") {
        t.first_delta = Some(t0.elapsed().as_millis());
    }
    if t.completed.is_none() && buf.contains("response.completed") {
        t.completed = Some(t0.elapsed().as_millis());
    }
}

fn fmt(o: Option<u128>) -> String {
    o.map(|v| format!("{v:>6}"))
        .unwrap_or_else(|| "     —".into())
}
fn row(label: &str, t: &T) {
    if let Some(e) = &t.err {
        println!("  {label:<28} ERROR: {e}");
        return;
    }
    println!(
        "  {:<28} ttft={} created={} firstΔ={} done={} up={:>8}B",
        label,
        fmt(t.ttft()),
        fmt(t.created),
        fmt(t.first_delta),
        fmt(t.completed),
        t.up_bytes
    );
}

#[tokio::main]
async fn main() {
    // Install the aws-lc-rs rustls provider (constructing CodexExecutor does it) so tokio-tungstenite's
    // rustls connector — same rustls 0.23 — uses it for the wss handshake.
    let _exec = CodexExecutor::new().expect("executor / rustls provider");

    let model =
        std::env::var("POLYFLARE_PROBE_MODEL").unwrap_or_else(|_| "gpt-5.6-sol".to_string());
    let ctx_kb: usize = std::env::var("POLYFLARE_PROBE_CTX_KB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(48);

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

    let nonce = format!("wsposc-{}", now_millis());
    let session_key = nonce.clone();
    let hdrs = codex_headers(&session_key, &bearer, account_id.as_deref());
    let big = filler(&nonce, ctx_kb);
    let question = format!("{big}\n\nIn one word, what animal is mentioned above?");

    println!("\n════════════════════════════════════════════════════════════════════════════");
    println!(
        "WS vs SSE transport POC — LIVE Codex backend   model={model}  ctx≈{ctx_kb}KB  plan=[{}]",
        row_a.plan_type
    );
    println!("════════════════════════════════════════════════════════════════════════════");

    // ── WebSocket path ───────────────────────────────────────────────────────────────────────
    println!("\n■ WebSocket transport");
    let mut req = WS_URL.into_client_request().expect("ws request");
    for (name, value) in &hdrs {
        req.headers_mut().insert(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    let connect = tokio::time::timeout(
        Duration::from_secs(20),
        tokio_tungstenite::connect_async(req),
    )
    .await;
    let (mut ws, resp) = match connect {
        Ok(Ok((ws, resp))) => (ws, resp),
        Ok(Err(e)) => {
            println!("  HANDSHAKE FAILED: {e}");
            print_verdict_ws_unavailable();
            return;
        }
        Err(_) => {
            println!("  HANDSHAKE TIMEOUT (>20s)");
            return;
        }
    };
    println!("  handshake OK: HTTP {}", resp.status());
    if let Some(ts) = resp.headers().get("x-codex-turn-state") {
        println!("  x-codex-turn-state present ({} bytes)", ts.len());
    }

    // WS turn 1 — full input.
    let body1 = ws_body(&model, &nonce, json!([user_msg(&question)]), None, None);
    let t1 = ws_turn(&mut ws, body1).await;
    row("WS turn1 (full)", &t1);
    let anchor = t1.resp_id.clone();

    // WS turn 2 — INCREMENTAL: only the new user message + previous_response_id, same live socket.
    let mut t2 = T {
        err: Some("no anchor from turn1".into()),
        ..Default::default()
    };
    if let Some(anchor) = &anchor {
        let body2 = ws_body(
            &model,
            &nonce,
            json!([user_msg("Reply with exactly the word: ACK")]),
            Some(anchor),
            None,
        );
        t2 = ws_turn(&mut ws, body2).await;
    }
    row("WS turn2 (INCREMENTAL)", &t2);

    // Bonus: a warmup (generate:false) — prefill only, no generation.
    let warm_body = ws_body(
        &model,
        &format!("{nonce}-warm"),
        json!([user_msg(&question)]),
        None,
        Some(false),
    );
    let tw = ws_turn(&mut ws, warm_body).await;
    row("WS warmup (generate:false)", &tw);

    let _ = ws.send(Message::Close(None)).await;

    // ── HTTP-SSE path (what PolyFlare does today): full resend of the whole history ───────────
    println!("\n■ HTTP-SSE transport (full resend)");
    let http = reqwest::Client::builder()
        .use_rustls_tls()
        .connect_timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let o1 = "fox"; // placeholder assistant turn — size is dominated by `big`, not this
    let sse_body = json!({
        "model": model,
        "instructions": INSTRUCTIONS,
        "input": [user_msg(&question), assistant_msg(o1), user_msg("Reply with exactly the word: ACK")],
        "tool_choice": "auto",
        "parallel_tool_calls": false,
        "reasoning": {"effort": "low"},
        "store": false,
        "stream": true,
        "include": [],
        "prompt_cache_key": nonce,
    });
    let sse = sse_turn(&http, &hdrs, sse_body).await;
    row("SSE turn2 (FULL RESEND)", &sse);

    // ── Verdict ──────────────────────────────────────────────────────────────────────────────
    println!("\n════════════════════════════════════════════════════════════════════════════");
    if let (Some(ws_ttft), Some(sse_ttft)) = (t2.ttft(), sse.ttft()) {
        println!(
            "VERDICT  WS-incremental ttft={ws_ttft}ms up={}B   vs   SSE-full-resend ttft={sse_ttft}ms up={}B",
            t2.up_bytes, sse.up_bytes
        );
        println!(
            "         upload {:.0}× smaller on WS; ttft {:+}ms",
            sse.up_bytes as f64 / t2.up_bytes.max(1) as f64,
            sse_ttft as i128 - ws_ttft as i128
        );
    } else {
        println!("VERDICT  incomplete — see errors above");
    }
    println!("════════════════════════════════════════════════════════════════════════════\n");
}

fn print_verdict_ws_unavailable() {
    println!(
        "\n  → The backend rejected the WebSocket upgrade for this request shape. This is itself a \
         result: either the TLS/header fingerprint isn't accepted for WS, or subscription accounts \
         gate WS. HTTP-SSE remains the working transport."
    );
}

/// Send one WS `response.create` frame, time the response stream to completion (bounded 90s).
async fn ws_turn(ws: &mut Ws, body: String) -> T {
    let mut t = T {
        up_bytes: body.len(),
        ..Default::default()
    };
    let t0 = Instant::now();
    if let Err(e) = ws.send(Message::Text(body.into())).await {
        t.err = Some(format!("send: {e}"));
        t.total = t0.elapsed().as_millis();
        return t;
    }
    let mut buf = String::new();
    let read = tokio::time::timeout(Duration::from_secs(90), async {
        while let Some(msg) = ws.next().await {
            match msg {
                Ok(Message::Text(txt)) => {
                    buf.push_str(txt.as_str());
                    scan(&buf, &mut t, t0);
                    if t.resp_id.is_none() {
                        t.resp_id = find_resp_id(&buf);
                    }
                    if buf.contains("response.completed") || buf.contains("\"response.failed\"") {
                        break;
                    }
                }
                Ok(Message::Binary(b)) => {
                    buf.push_str(&String::from_utf8_lossy(&b));
                    scan(&buf, &mut t, t0);
                    if buf.contains("response.completed") {
                        break;
                    }
                }
                Ok(Message::Close(frame)) => {
                    t.err = Some(format!("closed: {frame:?}"));
                    break;
                }
                Ok(_) => {}
                Err(e) => {
                    t.err = Some(format!("recv: {e}"));
                    break;
                }
            }
        }
    })
    .await;
    if read.is_err() {
        t.err = Some("TIMEOUT >90s".into());
    }
    t.total = t0.elapsed().as_millis();
    if t.resp_id.is_none() {
        t.resp_id = find_resp_id(&buf);
    }
    t
}

async fn sse_turn(client: &reqwest::Client, hdrs: &[(String, String)], body: Value) -> T {
    let text = serde_json::to_string(&body).unwrap();
    let mut t = T {
        up_bytes: text.len(),
        ..Default::default()
    };
    let mut rb = client.post(format!("{HTTP_BASE}/responses"));
    for (name, value) in hdrs {
        rb = rb.header(name, value);
    }
    rb = rb.header("accept", "text/event-stream");
    let t0 = Instant::now();
    let resp = match rb.json(&body).send().await {
        Ok(r) => r,
        Err(e) => {
            t.err = Some(format!("send: {e}"));
            return t;
        }
    };
    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let snippet: String = resp
            .text()
            .await
            .unwrap_or_default()
            .chars()
            .take(200)
            .collect();
        t.err = Some(format!("HTTP {status}: {snippet}"));
        return t;
    }
    let mut stream = resp.bytes_stream();
    let mut buf = String::new();
    let read = tokio::time::timeout(Duration::from_secs(90), async {
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(b) => {
                    buf.push_str(&String::from_utf8_lossy(&b));
                    scan(&buf, &mut t, t0);
                    if buf.contains("response.completed") {
                        break;
                    }
                }
                Err(e) => {
                    t.err = Some(format!("stream: {e}"));
                    break;
                }
            }
        }
    })
    .await;
    if read.is_err() {
        t.err = Some("TIMEOUT >90s".into());
    }
    t.total = t0.elapsed().as_millis();
    t
}
