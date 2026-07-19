//! ws_ratelimit_probe — measures the M5a milestone's ACTUAL premise (`SPEC-M5-WEBSOCKET.md` §8).
//!
//! `ws_vs_sse_probe.rs` proved WS's incremental continuation uploads 86× fewer BYTES than HTTP's
//! stateless full resend. That is not the same claim as "WS reduces RATE-LIMIT consumption" — the
//! user's actual constraint. If the backend still bills prefilled/cached tokens fully against
//! `/wham/usage` windows, the whole milestone's quota rationale is wrong and should be said so
//! loudly rather than shipped as an unverified assumption.
//!
//! This probe answers that empirically: it runs N identical continuation turns over WS
//! (incremental — `previous_response_id` + only the new item, one live connection) on one account,
//! and N identical continuation turns over HTTP (PolyFlare's transport today — stateless full
//! resend of the whole growing history) on a comparable second account. It reads each account's
//! `/wham/usage` windows BEFORE the run and AFTER the run and compares `used_percent` movement,
//! divided by N, as the per-turn rate-limit cost of each transport.
//!
//! Modeled on:
//!   - `ws_vs_sse_probe.rs`  — WS turn shape (`response.create` frame, incremental anchor,
//!     codex identity headers over the WS upgrade), the live-account harness idiom.
//!   - `handover_probe.rs`   — loading accounts from `~/.polyflare/store.db`, `resolve_account`
//!     (decrypt tokens, refresh if stale, never write back), the HTTP full-resend turn shape.
//!
//! The `/wham/usage` URL rule — it lives at the `/backend-api` root, NOT under `/codex` — is
//! `crates/polyflare-server/src/usage_refresh.rs:48-55`. That module's helpers are private (an
//! internal background-loop implementation detail), so the rule is reimplemented here in
//! `usage_url()`, verbatim in spirit, exactly like the sibling probes reimplement header
//! synthesis instead of reaching into server-private internals.
//!
//! ## HOW TO RUN IT (when you have headroom — NOT now, NOT by default)
//! Running this file with no arguments ONLY PRINTS THE COST PLAN — it makes zero network calls
//! and burns zero quota. That is the default and it is intentional: this probe is scoped to be
//! *built*, not *run*, until whoever holds the accounts has headroom to spend on the measurement.
//!
//! To actually execute it against your two most-headroom ACTIVE codex accounts:
//!
//! ```text
//! cargo run -p polyflare-server --example ws_ratelimit_probe --release -- --live
//! ```
//!
//! Tune the cost with env vars (read the printed plan either way before adding `--live`):
//!   POLYFLARE_PROBE_MODEL   default "gpt-5.6-luna"
//!   POLYFLARE_PROBE_N       continuation turns per transport, default 5 (small on purpose —
//!                           raise only if the measured delta turns out too small to read)
//!   POLYFLARE_PROBE_CTX_KB  seed context size in KB, default 8 (small on purpose — this probe
//!                           measures incremental per-turn billing, not prefill cost, which
//!                           `handover_probe.rs` already covers)
//!   POLYFLARE_DATA_DIR      default $HOME/.polyflare
//!
//! Total live cost at defaults: 2 accounts × (1 seed turn + 5 continuation turns) = 12 real
//! model generations, plus 4 `/wham/usage` reads (cheap GETs, not generations).
//!
//! SAFETY: never prints a token, cookie, refresh token, or Authorization value — only header
//! NAMES and non-secret values (account rank label, plan_type, byte counts, `used_percent`).
//! Never prints conversation content beyond the short generic literal prompts below. Only
//! touches `active` accounts (never `quota_exceeded` / `rate_limited` / anything else), and of
//! those, the two with the MOST headroom (lowest locally-known `used_percent`).

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

const INSTRUCTIONS: &str =
    "You are a terse assistant in a rate-limit measurement probe. Answer in as few tokens as \
     possible. When asked to reply with a specific word, reply with exactly that word and \
     nothing else.";

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

/// A ~ctx_kb KB filler, unique per run (the nonce is woven in) so a re-run never rides a warm
/// cache from a previous invocation.
fn filler(nonce: &str, ctx_kb: usize) -> String {
    let target = ctx_kb * 1024;
    let mut s = String::with_capacity(target + 256);
    let mut i = 0usize;
    while s.len() < target {
        s.push_str(&format!(
            "line {i:05} [{nonce}] the quick brown fox jumps over the lazy dog while the proxy \
             measures rate-limit consumption across transports.\n"
        ));
        i += 1;
    }
    s
}

/// The codex identity + auth headers, as (name, value) pairs — shared shape for WS and HTTP.
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

fn http_body(model: &str, nonce: &str, history: &[Value]) -> Value {
    json!({
        "model": model,
        "instructions": INSTRUCTIONS,
        "input": history,
        "tool_choice": "auto",
        "parallel_tool_calls": false,
        "reasoning": {"effort": "low"},
        "store": false,
        "stream": true,
        "include": [],
        "prompt_cache_key": nonce,
    })
}

/// One turn's outcome: how much we uploaded, what came back, never anything secret.
#[derive(Default)]
struct Turn {
    up_bytes: usize,
    resp_id: Option<String>,
    output_text: String,
    total_ms: u128,
    /// Per-turn billing from the terminal `response.completed` `usage` block — the cache signal.
    /// `None` when the turn errored / no completed usage was seen.
    usage: Option<Usage>,
    err: Option<String>,
}

/// The `usage.{input_tokens, input_tokens_details.{cached_tokens, cache_write_tokens}, total_tokens}`
/// block from a `response.completed` event — identical shape on WS and HTTP (same codex-rs
/// `ResponseCompletedUsage`). This is the cache-billing signal: on a warm continuation the backend
/// reports most of `input_tokens` as `cached_tokens` (billed at the cached rate). Content-free —
/// pure token counts.
#[derive(Default, Clone, Copy)]
struct Usage {
    input_tokens: i64,
    cached_tokens: i64,
    cache_write_tokens: i64,
    total_tokens: i64,
}

impl Usage {
    /// Fraction of `input_tokens` billed at the cached rate, as a percent (0 when input is 0).
    fn cached_pct(&self) -> f64 {
        if self.input_tokens <= 0 {
            0.0
        } else {
            100.0 * self.cached_tokens as f64 / self.input_tokens as f64
        }
    }
}

/// Pull the [`Usage`] out of a single already-parsed event Value iff it is `response.completed`.
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
        cache_write_tokens: u
            .pointer("/input_tokens_details/cache_write_tokens")
            .and_then(Value::as_i64)
            .unwrap_or(0),
        total_tokens: u.get("total_tokens").and_then(Value::as_i64).unwrap_or(0),
    })
}

/// Best-effort scrape of the terminal `response.completed` `usage` block out of a raw WS/SSE text
/// buffer. TWO framings must both work: HTTP-SSE is newline-delimited `data:` lines, but a WS turn
/// arrives as bare JSON event objects that concatenate in the buffer with NO separators
/// (`{..completed..}` glued to its neighbours) — which `buf.lines()` alone cannot split. So try the
/// line path first (HTTP), then fall back to a streaming parse of consecutive top-level JSON values
/// over the whole buffer (WS). `None` if no completed-event usage was found (e.g. `response.failed`).
fn scrape_usage(buf: &str) -> Option<Usage> {
    // HTTP path: one `data:`-prefixed JSON event per line.
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
    // WS path: bare JSON objects, possibly concatenated without separators. `StreamDeserializer`
    // reads consecutive top-level values; stop at the first parse error (a truncated trailing frame).
    for v in serde_json::Deserializer::from_str(buf).into_iter::<Value>() {
        let Ok(v) = v else { break };
        if let Some(u) = completed_usage(&v) {
            return Some(u);
        }
    }
    None
}

fn find_resp_id(buf: &str) -> Option<String> {
    let i = buf.find("resp_")?;
    let rest = &buf[i..];
    let end = rest
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
        .unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

/// Best-effort scrape of the assistant's output text (concatenated `output_text.delta`s) out of a
/// raw SSE/WS text buffer, so the HTTP full-resend track can grow a faithful history.
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
        println!("    {label:<24} ERROR: {e}");
        return;
    }
    let usage = match &t.usage {
        Some(u) => format!(
            "in={:>6} cached={:>6} ({:>5.1}%) cache_write={:>6} total={:>6}",
            u.input_tokens,
            u.cached_tokens,
            u.cached_pct(),
            u.cache_write_tokens,
            u.total_tokens
        ),
        None => "usage=NONE".to_string(),
    };
    println!(
        "    {:<24} up={:>7}B  total={:>6}ms  resp_id={}  {}",
        label,
        t.up_bytes,
        t.total_ms,
        if t.resp_id.is_some() { "yes" } else { "NONE" },
        usage,
    );
}

/// Send one WS `response.create` frame, collect the stream to a terminal event (bounded 90s).
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
    // The single WS frame carrying `response.completed`, kept ISOLATED so it parses cleanly — WS
    // events are per-frame bare JSON that concatenate in `buf` with no separators (which defeats a
    // line/stream scrape over the whole buffer). Its `usage` is the cache signal.
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
    // Prefer the isolated completed frame (parses cleanly); fall back to the whole buffer.
    t.usage = completed_frame
        .as_deref()
        .and_then(scrape_usage)
        .or_else(|| scrape_usage(&buf));
    t
}

/// One HTTP-SSE full-resend turn against `base_url`/responses, pinned to the given headers.
async fn http_turn(
    client: &reqwest::Client,
    base_url: &str,
    headers: &[(String, String)],
    body: Value,
) -> Turn {
    let text = serde_json::to_string(&body).unwrap();
    let mut t = Turn {
        up_bytes: text.len(),
        ..Default::default()
    };
    let mut rb = client.post(format!("{base_url}/responses"));
    for (name, value) in headers {
        rb = rb.header(name, value);
    }
    rb = rb.header("accept", "text/event-stream");
    let t0 = Instant::now();
    let resp = match rb.json(&body).send().await {
        Ok(r) => r,
        Err(e) => {
            t.err = Some(format!("send: {e}"));
            t.total_ms = t0.elapsed().as_millis();
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
        t.total_ms = t0.elapsed().as_millis();
        return t;
    }
    let mut stream = resp.bytes_stream();
    let mut buf = String::new();
    let read = tokio::time::timeout(Duration::from_secs(90), async {
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(b) => {
                    buf.push_str(&String::from_utf8_lossy(&b));
                    if buf.contains("\"response.completed\"") {
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
    t.total_ms = t0.elapsed().as_millis();
    t.resp_id = find_resp_id(&buf);
    t.output_text = scrape_output_text(&buf);
    t.usage = scrape_usage(&buf);
    t
}

/// The `/wham/usage` URL rule (`usage_refresh.rs:48-55`): it lives at the `/backend-api` root, NOT
/// under `/codex` — truncate at the `/backend-api` marker rather than appending under the codex
/// base.
fn usage_url(upstream_base: &str) -> String {
    let base = upstream_base.trim_end_matches('/');
    const MARKER: &str = "/backend-api";
    match base.find(MARKER) {
        Some(idx) => format!("{}/wham/usage", &base[..idx + MARKER.len()]),
        None => format!("{base}{MARKER}/wham/usage"),
    }
}

/// The two `used_percent` readings this probe cares about — never anything else off the payload.
#[derive(Default, Clone, Copy)]
struct UsagePoint {
    primary_used_percent: Option<f64>,
    secondary_used_percent: Option<f64>,
}

impl UsagePoint {
    /// The 5h (primary) window if present — it moves fastest and is the most readable signal at
    /// small N — else the weekly (secondary) window. Mirrors `usage_refresh.rs`'s "missing primary
    /// never gates" stance: we just fall back, we don't treat it as zero.
    fn headline(&self) -> Option<f64> {
        self.primary_used_percent.or(self.secondary_used_percent)
    }
}

async fn fetch_usage(
    client: &reqwest::Client,
    base_url: &str,
    bearer: &str,
    account_id: Option<&str>,
) -> Result<UsagePoint, String> {
    let mut rb = client
        .get(usage_url(base_url))
        .header("authorization", format!("Bearer {bearer}"))
        .header("accept", "application/json");
    if let Some(id) = account_id {
        rb = rb.header("chatgpt-account-id", id);
    }
    let resp = rb.send().await.map_err(|e| format!("send: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status().as_u16()));
    }
    let v: Value = resp.json().await.map_err(|e| format!("parse: {e}"))?;
    Ok(UsagePoint {
        primary_used_percent: v
            .pointer("/rate_limit/primary_window/used_percent")
            .and_then(Value::as_f64),
        secondary_used_percent: v
            .pointer("/rate_limit/secondary_window/used_percent")
            .and_then(Value::as_f64),
    })
}

fn print_usage(label: &str, u: &Result<UsagePoint, String>) {
    match u {
        Ok(p) => println!(
            "    {label:<20} primary(5h)={}  secondary(weekly)={}",
            p.primary_used_percent
                .map(|v| format!("{v:.3}%"))
                .unwrap_or_else(|| "not reported".into()),
            p.secondary_used_percent
                .map(|v| format!("{v:.3}%"))
                .unwrap_or_else(|| "not reported".into()),
        ),
        Err(e) => println!("    {label:<20} ERROR: {e}"),
    }
}

/// Resolve a live `Account` (fresh bearer, refreshed only if stale) for a store row id. No writes
/// back to the store — the refreshed token is used in-memory for this probe run only.
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
            Ok(r) => {
                access = r.tokens.access_token;
                eprintln!("  [{label}] token refreshed");
            }
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

/// Local (DB-only, zero network) headroom score for ranking candidates: the higher of the two
/// windows' last-known `used_percent` — lower score = more headroom. Never touches the network,
/// so this runs even in dry-run mode.
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

fn fallback_text(t: &Turn, default: &str) -> String {
    if t.output_text.is_empty() {
        default.to_string()
    } else {
        t.output_text.clone()
    }
}

#[tokio::main]
async fn main() {
    let live = std::env::args().any(|a| a == "--live");

    let model = std::env::var("POLYFLARE_PROBE_MODEL").unwrap_or_else(|_| "gpt-5.6-luna".into());
    let n: usize = std::env::var("POLYFLARE_PROBE_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let ctx_kb: usize = std::env::var("POLYFLARE_PROBE_CTX_KB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);

    let dir = data_dir();
    let cipher = TokenCipher::load_or_create(&dir.join("key")).expect("load key");
    let store = Store::open(&dir.join("store.db"))
        .await
        .expect("open store");

    // Rank ACTIVE codex accounts by LOCAL (already-known, zero-network) headroom. Never considers
    // quota_exceeded/rate_limited/reauth_required/paused/deactivated — filtered out before ranking,
    // by construction, not by a runtime check we could get wrong.
    let all = store.accounts().list().await.expect("list accounts");
    let candidates: Vec<_> = all
        .into_iter()
        .filter(|a| a.provider == "codex" && a.status == "active")
        .collect();
    if candidates.len() < 2 {
        eprintln!(
            "need >=2 ACTIVE codex accounts to compare transports, found {} \
             (quota_exceeded/rate_limited accounts are intentionally excluded)",
            candidates.len()
        );
        std::process::exit(1);
    }
    let mut scored = Vec::with_capacity(candidates.len());
    for a in &candidates {
        scored.push((headroom_score(&store, &a.id).await, a.clone()));
    }
    scored.sort_by(|x, y| x.0.partial_cmp(&y.0).unwrap());
    let ws_meta = scored[0].1.clone();
    let http_meta = scored[1].1.clone();

    let seed_kb_last_turn = ctx_kb; // WS uploads stay ~constant per turn (delta only)
    let http_kb_last_turn = ctx_kb * (n + 1); // HTTP resends the whole growing history each turn

    println!("\n════════════════════════════════════════════════════════════════════════════");
    println!("ws_ratelimit_probe — measures SPEC-M5-WEBSOCKET.md §8: does WS incremental");
    println!("continuation reduce RATE-LIMIT consumption, not just upload bytes?");
    println!("════════════════════════════════════════════════════════════════════════════");
    println!("  model={model}  N={n} continuation turns per transport  seed_ctx≈{ctx_kb}KB");
    println!(
        "  WS account   (most headroom, rank #1)  plan=[{}]  local used_percent≈{:.1}%",
        ws_meta.plan_type, scored[0].0
    );
    println!(
        "  HTTP account (2nd most headroom, rank #2)  plan=[{}]  local used_percent≈{:.1}%",
        http_meta.plan_type, scored[1].0
    );
    println!("\n  THIS WILL CONSUME REAL QUOTA ON BOTH ACCOUNTS IF RUN WITH --live:");
    println!(
        "    WS   account: 1 seed turn + {n} incremental continuation turns \
         (upload stays ~{seed_kb_last_turn}KB/turn — the whole point being measured)"
    );
    println!(
        "    HTTP account: 1 seed turn + {n} full-resend continuation turns \
         (upload GROWS to ~{http_kb_last_turn}KB by the last turn — full history resent every time)"
    );
    println!(
        "    Total: {} real model generations, + 4 cheap /wham/usage GETs (not generations)",
        2 * (n + 1)
    );
    println!("════════════════════════════════════════════════════════════════════════════");

    if !live {
        println!(
            "\nDRY RUN (default) — zero network calls were made, zero quota consumed. Re-run with \
             `--live` appended only when you have headroom on both accounts above:\n"
        );
        println!(
            "  cargo run -p polyflare-server --example ws_ratelimit_probe --release -- --live\n"
        );
        return;
    }

    // ── Live path from here — every line above already printed before anything was sent. ──────
    let _exec = CodexExecutor::new().expect("executor / rustls provider"); // installs rustls provider
    let oauth = OAuthClient::new(AUTH_BASE).expect("oauth client");
    let ws_acct = resolve_account(&store, &cipher, &oauth, &ws_meta.id, "WS").await;
    let http_acct = resolve_account(&store, &cipher, &oauth, &http_meta.id, "HTTP").await;
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .connect_timeout(Duration::from_secs(10))
        .build()
        .expect("http client");

    println!("\n■ Reading BEFORE usage windows");
    let ws_before = fetch_usage(
        &client,
        &ws_acct.base_url,
        &ws_acct.bearer_token,
        ws_acct.chatgpt_account_id.as_deref(),
    )
    .await;
    let http_before = fetch_usage(
        &client,
        &http_acct.base_url,
        &http_acct.bearer_token,
        http_acct.chatgpt_account_id.as_deref(),
    )
    .await;
    print_usage("WS account", &ws_before);
    print_usage("HTTP account", &http_before);

    // ── WS experiment: 1 seed turn (establishes the anchor) + N incremental continuations ──────
    println!("\n■ WS experiment — {n} incremental continuation turns, one live connection");
    let nonce_ws = format!("ratelimit-ws-{}", now_millis());
    let ws_hdrs = codex_headers(
        &nonce_ws,
        &ws_acct.bearer_token,
        ws_acct.chatgpt_account_id.as_deref(),
    );
    let mut req = WS_URL.into_client_request().expect("ws request");
    for (name, value) in &ws_hdrs {
        req.headers_mut().insert(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    let (mut ws, resp) = tokio_tungstenite::connect_async(req)
        .await
        .expect("ws connect");
    println!("    handshake OK: HTTP {}", resp.status());

    let seed_question = format!(
        "{}\n\nIn one word, what animal is mentioned above?",
        filler(&nonce_ws, ctx_kb)
    );
    let seed = ws_turn(
        &mut ws,
        ws_body(&model, &nonce_ws, json!([user_msg(&seed_question)]), None),
    )
    .await;
    row("WS seed (full)", &seed);
    let mut anchor = seed.resp_id.clone();
    if anchor.is_none() {
        eprintln!("  WS seed produced no response id — continuation turns will be skipped");
    }
    for i in 1..=n {
        if anchor.is_none() {
            break;
        }
        let body = ws_body(
            &model,
            &nonce_ws,
            json!([user_msg(&format!("Reply with exactly the word: ACK{i}"))]),
            anchor.as_deref(),
        );
        let t = ws_turn(&mut ws, body).await;
        row(&format!("WS turn {i}/{n} (incremental)"), &t);
        if t.resp_id.is_some() {
            anchor = t.resp_id.clone();
        }
    }
    let _ = ws.send(Message::Close(None)).await;

    // ── HTTP experiment: 1 seed turn + N full-resend continuations, growing history each time ──
    println!(
        "\n■ HTTP experiment — {n} full-resend continuation turns (PolyFlare's transport today)"
    );
    let nonce_http = format!("ratelimit-http-{}", now_millis());
    let http_hdrs = codex_headers(
        &nonce_http,
        &http_acct.bearer_token,
        http_acct.chatgpt_account_id.as_deref(),
    );
    let seed_question_http = format!(
        "{}\n\nIn one word, what animal is mentioned above?",
        filler(&nonce_http, ctx_kb)
    );
    let mut history: Vec<Value> = vec![user_msg(&seed_question_http)];
    let seed_http = http_turn(
        &client,
        &http_acct.base_url,
        &http_hdrs,
        http_body(&model, &nonce_http, &history),
    )
    .await;
    row("HTTP seed (full)", &seed_http);
    history.push(assistant_msg(&fallback_text(&seed_http, "fox")));
    for i in 1..=n {
        history.push(user_msg(&format!("Reply with exactly the word: ACK{i}")));
        let t = http_turn(
            &client,
            &http_acct.base_url,
            &http_hdrs,
            http_body(&model, &nonce_http, &history),
        )
        .await;
        row(&format!("HTTP turn {i}/{n} (full resend)"), &t);
        history.push(assistant_msg(&fallback_text(&t, &format!("ACK{i}"))));
    }

    println!("\n■ Reading AFTER usage windows");
    let ws_after = fetch_usage(
        &client,
        &ws_acct.base_url,
        &ws_acct.bearer_token,
        ws_acct.chatgpt_account_id.as_deref(),
    )
    .await;
    let http_after = fetch_usage(
        &client,
        &http_acct.base_url,
        &http_acct.bearer_token,
        http_acct.chatgpt_account_id.as_deref(),
    )
    .await;
    print_usage("WS account", &ws_after);
    print_usage("HTTP account", &http_after);

    // ── Verdict ──────────────────────────────────────────────────────────────────────────────
    println!("\n════════════════════════════════════════════════════════════════════════════");
    println!("VERDICT — used_percent movement over {n} continuation turns (÷N = per-turn cost)");
    let ws_delta = match (
        ws_before.as_ref().ok().and_then(UsagePoint::headline),
        ws_after.as_ref().ok().and_then(UsagePoint::headline),
    ) {
        (Some(b), Some(a)) => Some(a - b),
        _ => None,
    };
    let http_delta = match (
        http_before.as_ref().ok().and_then(UsagePoint::headline),
        http_after.as_ref().ok().and_then(UsagePoint::headline),
    ) {
        (Some(b), Some(a)) => Some(a - b),
        _ => None,
    };
    match (ws_delta, http_delta) {
        (Some(wd), Some(hd)) => {
            let ws_per_turn = wd / n as f64;
            let http_per_turn = hd / n as f64;
            println!("  WS   Δused_percent={wd:+.4}  ({ws_per_turn:+.5}/turn)");
            println!("  HTTP Δused_percent={hd:+.4}  ({http_per_turn:+.5}/turn)");
            if wd < 0.0 || hd < 0.0 {
                println!(
                    "  NOTE: a negative delta means a rate-limit window reset mid-probe — re-run \
                     for a clean read."
                );
            }
            if ws_per_turn.abs() < 0.01 && http_per_turn.abs() < 0.01 {
                println!(
                    "  INCONCLUSIVE — movement too small to read at N={n}, ctx={ctx_kb}KB. Raise \
                     POLYFLARE_PROBE_N or POLYFLARE_PROBE_CTX_KB and re-run with headroom."
                );
            } else if ws_per_turn < http_per_turn {
                let reduction =
                    (1.0 - (ws_per_turn.max(0.0) / http_per_turn.max(f64::EPSILON))) * 100.0;
                println!(
                    "  WS reduces rate-limit consumption by {reduction:.0}% per turn vs HTTP full \
                     resend."
                );
            } else {
                println!(
                    "  WS does NOT reduce rate-limit consumption (prefill still billed against \
                     limits)."
                );
            }
        }
        _ => {
            println!("  INCOMPLETE — could not read usage windows before/after (see errors above).")
        }
    }
    println!("════════════════════════════════════════════════════════════════════════════\n");
}
