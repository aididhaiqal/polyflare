//! ws_cache_handover_probe — does prompt caching survive over WebSocket, and does it survive
//! MOVING ACCOUNTS on WS? Companion to `cache_billing_probe` (HTTP: same-account ~95% cached,
//! cross-account same-key ALSO ~95%) and `ws_ratelimit_probe` (found WS *incremental* turn 2 bills
//! the full context at 0% cached). This probe isolates the two effects — transport vs delta — over
//! WS, across 1 account then 2 accounts:
//!
//!   1. **WS incremental (A, 1 account)** — seed (full) then a delta continuation
//!      (`previous_response_id` + only the new item). Expected ~0% (the delta re-sends no prefix).
//!   2. **WS full-resend (A, 1 account)** — the SAME conversation re-sent as a FULL turn on a fresh
//!      A socket. Tests whether the WS *transport* caches when you DO re-send the prefix (the HTTP
//!      path gets ~95% here). If this is high, the 0% above is the DELTA's fault, not WS's.
//!   3. **WS full-resend (B, 2 accounts / handover)** — the SAME full turn + SAME prompt_cache_key
//!      sent to a DIFFERENT account. Tests whether the cache follows a cross-account MOVE on WS
//!      (the HTTP path does — ~95%).
//!
//! The `usage.input_tokens_details.cached_tokens` field (same shape on WS and HTTP, per codex-rs
//! `ResponseCompletedUsage`) is the signal. A large deterministic seed prefix (`filler`) clears
//! OpenAI's ~1024-token prompt-cache minimum so caching can engage at all.
//!
//! ## HOW TO RUN IT (when you have headroom — dry-run by default, zero network)
//! ```text
//! cargo run -p polyflare-server --example ws_cache_handover_probe --release -- --live
//! ```
//! Env: POLYFLARE_PROBE_MODEL (default gpt-5.6-luna), POLYFLARE_PROBE_CTX_KB (seed size, default 6),
//! POLYFLARE_DATA_DIR (default ~/.polyflare).
//!
//! Live cost: account A = 3 generations (seed + incremental + full-resend); account B = 1 (the
//! handover full-resend), attempted only if a 2nd active account with headroom exists. So 3-4 real
//! generations, no /wham/usage reads.
//!
//! SAFETY: never prints a token/cookie/refresh/Authorization value — only rank label, plan_type,
//! byte/token COUNTS. Conversation content is the short generic literal prompts + deterministic
//! filler this probe authors. Only touches `active` accounts (the two with the most headroom).

use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use polyflare_codex::codex_headers::{
    codex_user_agent, originator, TurnIdentity, CODEX_CLI_VERSION,
};
use polyflare_codex::oauth::{self, OAuthClient};
use polyflare_codex::CodexExecutor;
use polyflare_core::Account;
use polyflare_store::{Store, TokenCipher};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

const CODEX_BASE: &str = "https://chatgpt.com/backend-api/codex";
const WS_URL: &str = "wss://chatgpt.com/backend-api/codex/responses";
const AUTH_BASE: &str = "https://auth.openai.com";
const INSTRUCTIONS: &str = "You are a terse assistant in a cache measurement probe. Answer in as \
     few tokens as possible. When asked to reply with a specific word, reply with exactly that \
     word and nothing else.";

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

/// ~ctx_kb KB deterministic filler, unique per run (nonce woven in) so a re-run never rides a warm
/// cache. Identical bytes for a given (nonce, ctx_kb) so every turn re-sending it forms the SAME
/// cacheable prefix under one prompt_cache_key.
fn filler(nonce: &str, ctx_kb: usize) -> String {
    let target = ctx_kb * 1024;
    let mut s = String::with_capacity(target + 256);
    let mut i = 0usize;
    while s.len() < target {
        s.push_str(&format!(
            "line {i:05} [{nonce}] the quick brown fox jumps over the lazy dog while the proxy \
             measures prompt-cache survival across transports and accounts.\n"
        ));
        i += 1;
    }
    s
}

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

fn ws_body(model: &str, nonce: &str, input: Value, prev: Option<&str>) -> String {
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
    serde_json::to_string(&b).unwrap()
}

#[derive(Default, Clone, Copy)]
struct Usage {
    input_tokens: i64,
    cached_tokens: i64,
    total_tokens: i64,
}
impl Usage {
    fn cached_pct(&self) -> f64 {
        if self.input_tokens <= 0 {
            0.0
        } else {
            100.0 * self.cached_tokens as f64 / self.input_tokens as f64
        }
    }
}

#[derive(Default)]
struct Turn {
    up_bytes: usize,
    resp_id: Option<String>,
    output_text: String,
    total_ms: u128,
    usage: Option<Usage>,
    err: Option<String>,
}

fn completed_usage(v: &Value) -> Option<Usage> {
    if v.get("type").and_then(Value::as_str) != Some("response.completed") {
        return None;
    }
    let u = v.pointer("/response/usage")?;
    Some(Usage {
        input_tokens: u.get("input_tokens").and_then(Value::as_i64).unwrap_or(0),
        cached_tokens: u
            .pointer("/input_tokens_details/cached_tokens")
            .and_then(Value::as_i64)
            .unwrap_or(0),
        total_tokens: u.get("total_tokens").and_then(Value::as_i64).unwrap_or(0),
    })
}

fn find_resp_id(buf: &str) -> Option<String> {
    let i = buf.find("resp_")?;
    let rest = &buf[i..];
    let end = rest
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
        .unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

fn scrape_output_text(buf: &str) -> String {
    let mut out = String::new();
    for line in buf.lines() {
        let payload = line.strip_prefix("data:").map(str::trim).unwrap_or(line);
        if payload.is_empty() || payload == "[DONE]" {
            continue;
        }
        let Ok(v) = serde_json::from_str::<Value>(payload) else {
            continue;
        };
        if v.get("type").and_then(Value::as_str) == Some("response.output_text.delta") {
            if let Some(d) = v.get("delta").and_then(Value::as_str) {
                out.push_str(d);
            }
        }
    }
    out
}

fn row(label: &str, t: &Turn) {
    if let Some(e) = &t.err {
        println!("    {label:<34} ERROR: {e}");
        return;
    }
    let usage = match &t.usage {
        Some(u) => format!(
            "in={:>6} cached={:>6} ({:>5.1}%) total={:>6}",
            u.input_tokens,
            u.cached_tokens,
            u.cached_pct(),
            u.total_tokens
        ),
        None => "usage=NONE".to_string(),
    };
    println!(
        "    {:<34} up={:>7}B  {:>6}ms  resp_id={}  {}",
        label,
        t.up_bytes,
        t.total_ms,
        if t.resp_id.is_some() { "yes" } else { "NONE" },
        usage,
    );
}

/// Send one `response.create` frame over `ws`, collect to a terminal event (bounded 90s). Captures
/// the isolated `response.completed` frame so its `usage` parses cleanly (WS frames are per-message
/// bare JSON that concatenate in the buffer with no separators).
async fn ws_turn(ws: &mut Ws, body: String) -> Turn {
    let mut t = Turn {
        up_bytes: body.len(),
        ..Default::default()
    };
    let t0 = Instant::now();
    if let Err(e) = ws.send(Message::Text(body.into())).await {
        t.err = Some(format!("send: {e}"));
        t.total_ms = t0.elapsed().as_millis();
        return t;
    }
    let mut buf = String::new();
    let mut completed_frame: Option<String> = None;
    let read = tokio::time::timeout(Duration::from_secs(90), async {
        while let Some(msg) = ws.next().await {
            match msg {
                Ok(Message::Text(txt)) => {
                    let s = txt.as_str();
                    buf.push_str(s);
                    if s.contains("response.completed") {
                        completed_frame = Some(s.to_string());
                    }
                    if buf.contains("response.completed") || buf.contains("\"response.failed\"") {
                        break;
                    }
                }
                Ok(Message::Binary(b)) => {
                    let s = String::from_utf8_lossy(&b);
                    if s.contains("response.completed") {
                        completed_frame = Some(s.to_string());
                    }
                    buf.push_str(&s);
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
    t.total_ms = t0.elapsed().as_millis();
    t.resp_id = find_resp_id(&buf);
    t.output_text = scrape_output_text(&buf);
    t.usage = completed_frame
        .as_deref()
        .and_then(scrape_usage)
        .or_else(|| scrape_usage(&buf));
    t
}

fn scrape_usage(buf: &str) -> Option<Usage> {
    for line in buf.lines() {
        let payload = line.strip_prefix("data:").map(str::trim).unwrap_or(line);
        if payload.is_empty() || payload == "[DONE]" {
            continue;
        }
        if let Ok(v) = serde_json::from_str::<Value>(payload) {
            if let Some(u) = completed_usage(&v) {
                return Some(u);
            }
        }
    }
    for v in serde_json::Deserializer::from_str(buf).into_iter::<Value>() {
        let Ok(v) = v else { break };
        if let Some(u) = completed_usage(&v) {
            return Some(u);
        }
    }
    None
}

/// Open a fresh WS to the codex backend for `account`, keyed by `nonce` (the prompt_cache_key). The
/// handshake is the plain (no permessage-deflate / no OpenAI-Beta) shape `ws_ratelimit_probe` proved
/// the backend accepts (HTTP 101).
async fn connect_ws(account: &Account, nonce: &str) -> Result<Ws, String> {
    let hdrs = codex_headers(
        nonce,
        &account.bearer_token,
        account.chatgpt_account_id.as_deref(),
    );
    let mut req = WS_URL.into_client_request().map_err(|e| e.to_string())?;
    for (name, value) in &hdrs {
        req.headers_mut().insert(
            HeaderName::from_bytes(name.as_bytes()).map_err(|e| e.to_string())?,
            HeaderValue::from_str(value).map_err(|e| e.to_string())?,
        );
    }
    let (ws, _resp) = tokio_tungstenite::connect_async(req)
        .await
        .map_err(|e| e.to_string())?;
    Ok(ws)
}

async fn resolve_account(
    store: &Store,
    cipher: &TokenCipher,
    oauth: &OAuthClient,
    id: &str,
    label: &str,
) -> Account {
    let (row, tokens) = store
        .accounts()
        .get_with_tokens(id, cipher)
        .await
        .expect("db")
        .expect("account row");
    let mut access = tokens.access_token.clone();
    if oauth::should_refresh(oauth::token_exp(&access), row.last_refresh, now_secs()) {
        match oauth.refresh(&tokens.refresh_token).await {
            Ok(r) => access = r.tokens.access_token,
            Err(e) => eprintln!("  [{label}] refresh failed ({e:?}); using stored token"),
        }
    }
    Account {
        id: row.id,
        base_url: CODEX_BASE.to_string(),
        bearer_token: access,
        chatgpt_account_id: row.chatgpt_account_id,
    }
}

async fn headroom_score(store: &Store, account_id: &str) -> f64 {
    let usage = store
        .accounts()
        .latest_usage(account_id)
        .await
        .unwrap_or_default();
    let p = usage
        .primary
        .as_ref()
        .map(|w| w.used_percent)
        .unwrap_or(0.0);
    let s = usage
        .secondary
        .as_ref()
        .map(|w| w.used_percent)
        .unwrap_or(0.0);
    p.max(s)
}

#[tokio::main]
async fn main() {
    let live = std::env::args().any(|a| a == "--live");
    let model =
        std::env::var("POLYFLARE_PROBE_MODEL").unwrap_or_else(|_| "gpt-5.6-luna".to_string());
    let ctx_kb: usize = std::env::var("POLYFLARE_PROBE_CTX_KB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6);

    let dir = data_dir();
    let store = Store::open(&dir.join("store.db"))
        .await
        .expect("open store");
    let cipher = TokenCipher::load_or_create(&dir.join("key")).expect("cipher");

    let all = store.accounts().list().await.expect("list accounts");
    let mut scored: Vec<(f64, polyflare_store::Account)> = Vec::new();
    for a in all.into_iter().filter(|a| a.status == "active") {
        scored.push((headroom_score(&store, &a.id).await, a));
    }
    scored.sort_by(|x, y| x.0.partial_cmp(&y.0).unwrap());
    if scored.is_empty() {
        eprintln!("no active accounts with headroom — nothing to probe");
        return;
    }
    let a_meta = scored[0].1.clone();
    let has_b = scored.len() >= 2;

    println!("════════════════════════════════════════════════════════════════════════════");
    println!("ws_cache_handover_probe — does prompt caching survive over WS, and across an");
    println!("account MOVE on WS? (companion to cache_billing_probe/ws_ratelimit_probe)");
    println!("════════════════════════════════════════════════════════════════════════════");
    println!("  model={model}  seed_ctx≈{ctx_kb}KB");
    println!(
        "  Account A (rank #1)  plan=[{}]  local used_percent≈{:.1}%",
        a_meta.plan_type, scored[0].0
    );
    if has_b {
        println!(
            "  Account B (rank #2)  plan=[{}]  local used_percent≈{:.1}%  → 2-account handover WILL be attempted.",
            scored[1].1.plan_type, scored[1].0
        );
    } else {
        println!("  (only one active account — the 2-account handover leg is SKIPPED.)");
    }
    println!("\n  CONSUMES REAL QUOTA IF RUN WITH --live:");
    println!("    A: 3 generations (seed + incremental cont + full-resend cont)");
    if has_b {
        println!("    B: 1 generation (the SAME full-resend cont — the handover). Total 4.");
    } else {
        println!("    Total 3 (no 2nd account).");
    }
    println!("════════════════════════════════════════════════════════════════════════════");

    if !live {
        println!(
            "\n  DRY RUN — nothing sent. Re-run with --live when you have headroom:\n    cargo run \
             -p polyflare-server --example ws_cache_handover_probe --release -- --live"
        );
        return;
    }

    let _exec = CodexExecutor::new().expect("executor / rustls provider");
    let oauth = OAuthClient::new(AUTH_BASE).expect("oauth client");
    let a = resolve_account(&store, &cipher, &oauth, &a_meta.id, "A").await;

    let nonce = format!("ws-handover-{}", now_millis());
    let seed_question = format!(
        "{}\n\nIn one word, what animal is mentioned above?",
        filler(&nonce, ctx_kb)
    );

    // ── 1 ACCOUNT ──────────────────────────────────────────────────────────────────────────────
    println!("\n■ WS — 1 account (A), prompt_cache_key={nonce}");
    let mut ws_a = match connect_ws(&a, &nonce).await {
        Ok(w) => {
            println!("    handshake A OK");
            w
        }
        Err(e) => {
            eprintln!("    WS connect A failed: {e}");
            return;
        }
    };
    let seed = ws_turn(
        &mut ws_a,
        ws_body(&model, &nonce, json!([user_msg(&seed_question)]), None),
    )
    .await;
    row("A seed (full, cold)", &seed);
    let o1 = if seed.output_text.is_empty() {
        "fox".to_string()
    } else {
        seed.output_text.clone()
    };

    // Incremental continuation (delta + anchor) — WS's native shape.
    let incr = match seed.resp_id.as_deref() {
        Some(anchor) => {
            ws_turn(
                &mut ws_a,
                ws_body(
                    &model,
                    &nonce,
                    json!([user_msg("Reply with exactly the word: ok.")]),
                    Some(anchor),
                ),
            )
            .await
        }
        None => Turn {
            err: Some("seed had no resp_id — skipped".into()),
            ..Default::default()
        },
    };
    row("A cont — INCREMENTAL (delta)", &incr);
    let _ = ws_a.close(None).await;

    // The full conversation, re-sent as one FULL turn (what a real handover must do — the WS anchor
    // can't cross accounts). Same big prefix + same prompt_cache_key.
    let history = json!([
        user_msg(&seed_question),
        assistant_msg(&o1),
        user_msg("Reply with exactly the word: ok."),
    ]);

    let mut ws_a2 = match connect_ws(&a, &nonce).await {
        Ok(w) => w,
        Err(e) => {
            eprintln!("    WS re-connect A failed: {e}");
            return;
        }
    };
    let full_a = ws_turn(&mut ws_a2, ws_body(&model, &nonce, history.clone(), None)).await;
    row("A cont — FULL-RESEND", &full_a);
    let _ = ws_a2.close(None).await;

    // ── 2 ACCOUNTS (handover) ──────────────────────────────────────────────────────────────────
    let full_b = if has_b {
        let b_meta = scored[1].1.clone();
        let b = resolve_account(&store, &cipher, &oauth, &b_meta.id, "B").await;
        println!("\n■ WS — 2 accounts: SAME conversation + SAME key moved to account B");
        match connect_ws(&b, &nonce).await {
            Ok(mut ws_b) => {
                println!("    handshake B OK");
                let t = ws_turn(&mut ws_b, ws_body(&model, &nonce, history, None)).await;
                row("B cont — FULL-RESEND (handover)", &t);
                let _ = ws_b.close(None).await;
                Some(t)
            }
            Err(e) => {
                eprintln!("    WS connect B failed: {e}");
                None
            }
        }
    } else {
        None
    };

    // ── VERDICT ────────────────────────────────────────────────────────────────────────────────
    println!("\n════════════════════════════════════════════════════════════════════════════");
    println!(
        "VERDICT — cached fraction of input tokens (higher = more of the context billed cheap)"
    );
    let pct = |t: &Turn| t.usage.map(|u| u.cached_pct());
    match pct(&incr) {
        Some(p) => {
            println!("  WS incremental (A, 1 account)      {p:>5.1}%  ← WS's native delta shape")
        }
        None => println!("  WS incremental (A, 1 account)      n/a"),
    }
    match pct(&full_a) {
        Some(p) => println!(
            "  WS full-resend (A, 1 account)      {p:>5.1}%  ← same account, re-sent prefix"
        ),
        None => println!("  WS full-resend (A, 1 account)      n/a"),
    }
    match full_b.as_ref().and_then(pct) {
        Some(p) => {
            println!("  WS full-resend (B, 2 accounts)     {p:>5.1}%  ← MOVED account, same key")
        }
        None if has_b => println!("  WS full-resend (B, 2 accounts)     n/a (handover leg failed)"),
        None => println!("  WS full-resend (B, 2 accounts)     SKIPPED (only one account)"),
    }
    println!(
        "  (HTTP baseline for the same scenario, from cache_billing_probe: ~95% both 1- and 2-account.)"
    );
    println!("════════════════════════════════════════════════════════════════════════════");
}
