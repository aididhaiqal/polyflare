//! ws_wedge_demo — reproduce the cross-account WebSocket "wedge" on the LIVE Codex backend.
//!
//! The wedge (SPEC-M3): a `previous_response_id` anchor is a WS-session-scoped ephemeral handle. If a
//! turn carrying that anchor is routed to a DIFFERENT account (or a fresh connection that never
//! produced it), the anchor is dead. codex-lb's failure mode is a SILENT hang; PolyFlare's continuity
//! engine exists to detect + recover it. This demo shows, on real infrastructure, exactly what the
//! backend does with a foreign anchor — and confirms the controls (own anchor works, full resend
//! recovers).
//!
//! Uses TWO live accounts (A, B) from ~/.polyflare/store.db. Sequence:
//!   T1  WS-A turn1 (full)                     → capture anchor_A
//!   T2  WS-B turn1 (full)                     → capture anchor_B, proves B's session works
//!   T3  WS-B incremental w/ anchor_B (own)    → CONTROL: incremental works same-session
//!   T4  fresh WS-B' incremental w/ anchor_A   → THE WEDGE (foreign anchor)
//!   T5  same WS-B' full resend (no anchor)    → RECOVERY (what ResendFull does)
//!
//! Classifies T4 as: completed (accepted?!) / fast-reject (error frame) / SILENT HANG (timeout, no
//! frames) / hang-after-accept.
//!
//! SAFETY: prints only labels + timings + upstream error messages (no tokens/account-ids/bodies).
//! Run: cargo run -p polyflare-server --example ws_wedge_demo --release

use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use polyflare_codex::codex_headers::{
    codex_user_agent, originator, TurnIdentity, CODEX_CLI_VERSION,
};
use polyflare_codex::oauth::{self, OAuthClient};
use polyflare_codex::CodexExecutor;
use polyflare_store::{Account, Store, TokenCipher};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

const WS_URL: &str = "wss://chatgpt.com/backend-api/codex/responses";
const AUTH_BASE: &str = "https://auth.openai.com";
/// The wedge classifier's patience. A dead anchor that is going to hang will hang past this; a
/// backend that rejects does so in well under a second.
const WEDGE_TIMEOUT: Duration = Duration::from_secs(45);

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

const INSTRUCTIONS: &str =
    "You are a terse assistant in a transport probe. Reply with exactly the \
     requested word and nothing else.";

fn filler(nonce: &str, ctx_kb: usize) -> String {
    let target = ctx_kb * 1024;
    let mut s = String::with_capacity(target + 256);
    let mut i = 0usize;
    while s.len() < target {
        s.push_str(&format!(
            "line {i:05} [{nonce}] the quick brown fox jumps over the lazy dog; wedge demo context.\n"
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

fn ws_body(model: &str, cache_key: &str, input: Value, prev: Option<&str>) -> String {
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
        "prompt_cache_key": cache_key,
    });
    if let Some(p) = prev {
        b["previous_response_id"] = json!(p);
    }
    serde_json::to_string(&b).unwrap()
}

#[derive(Default)]
struct T {
    created: Option<u128>,
    first_delta: Option<u128>,
    completed: Option<u128>,
    total: u128,
    resp_id: Option<String>,
    outcome: String,
    note: Option<String>,
}

fn find_resp_id(buf: &str) -> Option<String> {
    let i = buf.find("resp_")?;
    let rest = &buf[i..];
    let end = rest
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
        .unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

async fn connect(headers: &[(String, String)]) -> Result<Ws, String> {
    let mut req = WS_URL.into_client_request().map_err(|e| e.to_string())?;
    for (name, value) in headers {
        req.headers_mut().insert(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    match tokio::time::timeout(
        Duration::from_secs(20),
        tokio_tungstenite::connect_async(req),
    )
    .await
    {
        Ok(Ok((ws, resp))) => {
            if resp.status().as_u16() != 101 {
                return Err(format!("handshake status {}", resp.status()));
            }
            Ok(ws)
        }
        Ok(Err(e)) => Err(format!("handshake: {e}")),
        Err(_) => Err("handshake timeout".into()),
    }
}

/// Send one frame and observe the response, bounded by `patience`. Classifies the outcome.
async fn ws_turn(ws: &mut Ws, body: String, patience: Duration) -> T {
    let mut t = T::default();
    let t0 = Instant::now();
    if let Err(e) = ws.send(Message::Text(body.into())).await {
        t.outcome = format!("SEND-ERR ({e})");
        t.total = t0.elapsed().as_millis();
        return t;
    }
    let mut buf = String::new();
    let mut saw_error = false;
    let read = tokio::time::timeout(patience, async {
        while let Some(msg) = ws.next().await {
            match msg {
                Ok(Message::Text(txt)) => buf.push_str(txt.as_str()),
                Ok(Message::Binary(b)) => buf.push_str(&String::from_utf8_lossy(&b)),
                Ok(Message::Close(frame)) => {
                    t.note = frame.map(|f| format!("close {}: {}", f.code, f.reason));
                    t.outcome = "CLOSED".into();
                    return;
                }
                Ok(Message::Ping(p)) => {
                    let _ = ws.send(Message::Pong(p)).await;
                    continue;
                }
                Ok(_) => continue,
                Err(e) => {
                    t.outcome = format!("RECV-ERR ({e})");
                    return;
                }
            }
            if t.created.is_none() && buf.contains("response.created") {
                t.created = Some(t0.elapsed().as_millis());
            }
            if t.first_delta.is_none() && buf.contains(".delta\"") {
                t.first_delta = Some(t0.elapsed().as_millis());
            }
            if t.resp_id.is_none() {
                t.resp_id = find_resp_id(&buf);
            }
            let marker = buf
                .find("\"type\":\"error\"")
                .or_else(|| buf.find("\"type\": \"error\""))
                .or_else(|| buf.find("response.failed"));
            if let Some(i) = marker {
                saw_error = true;
                // Window from the error frame (skip the codex.rate_limits preamble that precedes it).
                t.note = Some(buf[i..].chars().take(300).collect::<String>());
                t.outcome = "FAST-REJECT".into();
                return;
            }
            if buf.contains("response.completed") {
                t.completed = Some(t0.elapsed().as_millis());
                t.outcome = "COMPLETED".into();
                return;
            }
        }
        // stream ended with no terminal marker
        if !saw_error {
            t.outcome = if buf.is_empty() {
                "EOF-EMPTY".into()
            } else {
                "EOF".into()
            };
        }
    })
    .await;
    if read.is_err() {
        t.outcome = if t.created.is_some() {
            "HANG-AFTER-ACCEPT".into()
        } else {
            "SILENT-HANG".into()
        };
    }
    t.total = t0.elapsed().as_millis();
    t
}

fn report(label: &str, t: &T) {
    let ttft = t.first_delta.or(t.created);
    let ttft_s = ttft.map(|v| format!("{v}ms")).unwrap_or_else(|| "—".into());
    print!(
        "  {:<34} {:<18} ttft={:<7} total={}ms",
        label, t.outcome, ttft_s, t.total
    );
    if let Some(n) = &t.note {
        print!("  «{n}»");
    }
    println!();
}

async fn resolve(
    store: &Store,
    cipher: &TokenCipher,
    oauth: &OAuthClient,
    a: &Account,
) -> (String, Option<String>) {
    let (row, tokens) = store
        .accounts()
        .get_with_tokens(&a.id, cipher)
        .await
        .unwrap()
        .unwrap();
    let mut bearer = tokens.access_token.clone();
    if oauth::should_refresh(oauth::token_exp(&bearer), row.last_refresh, now_secs()) {
        if let Ok(r) = oauth.refresh(&tokens.refresh_token).await {
            bearer = r.tokens.access_token;
        }
    }
    (bearer, row.chatgpt_account_id)
}

#[tokio::main]
async fn main() {
    let _exec = CodexExecutor::new().expect("rustls provider");
    let model =
        std::env::var("POLYFLARE_PROBE_MODEL").unwrap_or_else(|_| "gpt-5.6-sol".to_string());
    let ctx_kb: usize = std::env::var("POLYFLARE_PROBE_CTX_KB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32);

    let dir = data_dir();
    let cipher = TokenCipher::load_or_create(&dir.join("key")).unwrap();
    let store = Store::open(&dir.join("store.db")).await.unwrap();
    let oauth = OAuthClient::new(AUTH_BASE).unwrap();

    let all = store.accounts().list().await.unwrap();
    let codex: Vec<_> = all
        .into_iter()
        .filter(|a| a.provider == "codex" && a.status == "active")
        .collect();
    assert!(codex.len() >= 2, "need 2 active codex accounts");
    let (bearer_a, id_a) = resolve(&store, &cipher, &oauth, &codex[0]).await;
    let (bearer_b, id_b) = resolve(&store, &cipher, &oauth, &codex[1]).await;

    let nonce = format!("wedge-{}", now_millis());
    let big = filler(&nonce, ctx_kb);
    let question = format!("{big}\n\nReply with exactly the word: READY");
    let hdrs_a = codex_headers(&format!("{nonce}-A"), &bearer_a, id_a.as_deref());
    let hdrs_b = codex_headers(&format!("{nonce}-B"), &bearer_b, id_b.as_deref());

    println!("\n════════════════════════════════════════════════════════════════════════════");
    println!(
        "Cross-account WebSocket WEDGE demo — LIVE Codex backend   model={model}  ctx≈{ctx_kb}KB"
    );
    println!("  A=[{}]  B=[{}]", codex[0].plan_type, codex[1].plan_type);
    println!("════════════════════════════════════════════════════════════════════════════\n");

    // T1: WS-A turn1 → anchor_A
    let mut ws_a = match connect(&hdrs_a).await {
        Ok(w) => w,
        Err(e) => {
            println!("  WS-A connect failed: {e}");
            return;
        }
    };
    let t1 = ws_turn(
        &mut ws_a,
        ws_body(&model, &nonce, json!([user_msg(&question)]), None),
        WEDGE_TIMEOUT,
    )
    .await;
    report("T1  WS-A turn1 (full)", &t1);
    let anchor_a = t1.resp_id.clone();

    // T2: WS-B turn1 → anchor_B (proves B works)
    let mut ws_b = match connect(&hdrs_b).await {
        Ok(w) => w,
        Err(e) => {
            println!("  WS-B connect failed: {e}");
            return;
        }
    };
    let t2 = ws_turn(
        &mut ws_b,
        ws_body(&model, &nonce, json!([user_msg(&question)]), None),
        WEDGE_TIMEOUT,
    )
    .await;
    report("T2  WS-B turn1 (full)", &t2);
    let anchor_b = t2.resp_id.clone();

    // T3: CONTROL — WS-B incremental with B's OWN anchor → should work
    if let Some(anchor_b) = &anchor_b {
        let t3 = ws_turn(
            &mut ws_b,
            ws_body(
                &model,
                &nonce,
                json!([user_msg("Reply with exactly the word: ACK")]),
                Some(anchor_b),
            ),
            WEDGE_TIMEOUT,
        )
        .await;
        report("T3  WS-B incr, OWN anchor  (control)", &t3);
    } else {
        println!("  T3  skipped (no anchor_B)");
    }

    println!("\n  ── firing dead anchors on FRESH B sockets ──");

    // W1: SAME-account fresh-reattach — B's OWN anchor, but replayed on a NEW B connection that never
    // produced it (the sol-anchor-wedge-rootcause recurring scenario).
    let mut w1 = T::default();
    if let Some(anchor_b) = &anchor_b {
        if let Ok(mut ws_b_re) = connect(&hdrs_b).await {
            w1 = ws_turn(
                &mut ws_b_re,
                ws_body(
                    &model,
                    &nonce,
                    json!([user_msg("Reply with exactly the word: ACK")]),
                    Some(anchor_b),
                ),
                WEDGE_TIMEOUT,
            )
            .await;
            report("W1  fresh WS-B, OWN anchor (reattach)", &w1);
            let _ = ws_b_re.send(Message::Close(None)).await;
        }
    }

    // W2: CROSS-account — A's foreign anchor on a fresh B connection.
    let mut w2 = T::default();
    let mut t5 = T::default();
    if let Some(anchor_a) = &anchor_a {
        if let Ok(mut ws_b2) = connect(&hdrs_b).await {
            w2 = ws_turn(
                &mut ws_b2,
                ws_body(
                    &model,
                    &nonce,
                    json!([user_msg("Reply with exactly the word: ACK")]),
                    Some(anchor_a),
                ),
                WEDGE_TIMEOUT,
            )
            .await;
            report("W2  fresh WS-B, FOREIGN anchor (x-acct)", &w2);

            // Recovery — full resend on the same socket, no anchor (what ResendFull does).
            t5 = ws_turn(
                &mut ws_b2,
                ws_body(
                    &model,
                    &nonce,
                    json!([
                        user_msg(&question),
                        user_msg("Reply with exactly the word: ACK")
                    ]),
                    None,
                ),
                WEDGE_TIMEOUT,
            )
            .await;
            report("W3  fresh WS-B, full resend (recovery)", &t5);
            let _ = ws_b2.send(Message::Close(None)).await;
        }
    }

    println!("\n════════════════════════════════════════════════════════════════════════════");
    println!(
        "DEAD-ANCHOR OUTCOMES:  same-acct-reattach={}   cross-acct={}   recovery={}",
        w1.outcome, w2.outcome, t5.outcome
    );
    let classify = |o: &str| match o {
        "SILENT-HANG" => "classic wedge (silent hang → silence-watchdog REQUIRED)",
        "HANG-AFTER-ACCEPT" => "accepted-then-stalled (watchdog still catches)",
        "FAST-REJECT" | "CLOSED" => "clean fast reject (error-catch + resend suffices)",
        "COMPLETED" => "served anyway (possible silent context-loss — investigate)",
        _ => "other",
    };
    println!("  same-account reattach → {}", classify(&w1.outcome));
    println!("  cross-account         → {}", classify(&w2.outcome));
    println!("════════════════════════════════════════════════════════════════════════════\n");

    let _ = ws_a.send(Message::Close(None)).await;
    let _ = ws_b.send(Message::Close(None)).await;
}
