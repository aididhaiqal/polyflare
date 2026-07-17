//! handover_probe — measure the REAL cost of a cross-account "handover" against the LIVE Codex
//! backend, using two real accounts from the local PolyFlare store.
//!
//! It answers the question behind PolyFlare's wedge fix + B4 failover: when we strip a dead anchor
//! and RESEND the full conversation to a FRESH account, does OpenAI cold-prefill the whole history
//! ("long reprocessing on their side")? And how many ms does the client pay versus a normal
//! owner-continuation that rides the `previous_response_id` anchor?
//!
//! It drives the SAME `CodexExecutor` PolyFlare's ingress uses (same rustls fingerprint, same
//! synthesized codex identity headers, same wire body) — the only thing this bypasses is the
//! selector, so we can PIN each request to a chosen account.
//!
//! Two experiments:
//!   1. Cache probe — identical large context → A (cold) → A (warm) → B (cold/fresh) → B (warm);
//!      isolates whether the prompt-prefill cache is org-scoped (fresh acct = cold).
//!   2. Anchored h/o — real turn on A, then owner-continue via previous_response_id (cheap path)
//!      vs full anchor-stripped resend to B (the wedge-recovery / B4 path).
//!
//! SAFETY: prints only A/B labels, plan types, and timings — never tokens, account ids, emails,
//! or request/response bodies.
//!
//! Run:  cargo run -p polyflare-server --example handover_probe --release
//! Env:  POLYFLARE_PROBE_MODEL   (default gpt-5.6-luna)
//!       POLYFLARE_PROBE_RUNS    (default 3)
//!       POLYFLARE_PROBE_CTX_KB  (approx big-context size in KB, default 48 ≈ ~12k tokens)
//!       POLYFLARE_DATA_DIR      (default $HOME/.polyflare)

use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use polyflare_codex::codex_headers::{
    codex_user_agent, conversation_key, originator, TurnIdentity, CODEX_CLI_VERSION,
};
use polyflare_codex::oauth::{self, OAuthClient};
use polyflare_core::Account;
use polyflare_store::{Store, TokenCipher};
use serde_json::{json, Value};

const CODEX_BASE: &str = "https://chatgpt.com/backend-api/codex";
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

#[derive(Default)]
struct Timings {
    first_byte_ms: Option<u128>,
    created_ms: Option<u128>,
    /// Time to the first `.delta` event — the model cannot emit its first token until it has
    /// prefilled the input context, so this is our proxy for prefill / "reprocessing" cost.
    first_delta_ms: Option<u128>,
    completed_ms: Option<u128>,
    total_ms: u128,
    sse_bytes: usize,
    response_id: Option<String>,
    output_text: String,
    error: Option<String>,
}

impl Timings {
    /// The headline number: time-to-first-token (prefill proxy), else whatever we got.
    fn ttft(&self) -> Option<u128> {
        self.first_delta_ms
            .or(self.created_ms)
            .or(self.first_byte_ms)
    }
}

fn data_dir() -> PathBuf {
    std::env::var("POLYFLARE_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(std::env::var("HOME").expect("HOME")).join(".polyflare"))
}

/// The synthesized codex identity headers for a translated request — a verbatim copy of
/// `ingress::synthesize_codex_forward_headers` (private), so this probe's egress matches PolyFlare's.
fn forward_headers(body: &Value) -> Vec<(String, String)> {
    let id = TurnIdentity::derive(&conversation_key(body));
    vec![
        (
            "user-agent".to_string(),
            codex_user_agent(CODEX_CLI_VERSION),
        ),
        ("originator".to_string(), originator().to_string()),
        ("accept".to_string(), "text/event-stream".to_string()),
        ("session-id".to_string(), id.session_id.clone()),
        ("thread-id".to_string(), id.thread_id.clone()),
        ("x-client-request-id".to_string(), id.thread_id.clone()),
        ("x-codex-window-id".to_string(), id.window_id.clone()),
        ("x-codex-turn-metadata".to_string(), id.turn_metadata_json()),
    ]
}

fn user_msg(text: &str) -> Value {
    json!({"type":"message","role":"user","content":[{"type":"input_text","text":text}]})
}

fn assistant_msg(text: &str) -> Value {
    json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":text}]})
}

const INSTRUCTIONS: &str =
    "You are a terse assistant embedded in a load-balancer latency probe. Answer in as few tokens \
     as possible. When asked to reply with a specific word, reply with exactly that word and nothing else.";

/// Build a ~ctx_kb KB filler that is UNIQUE per run (the nonce is woven in), so a re-run of this
/// probe never hits a warm cache on its first request.
fn filler(nonce: &str, ctx_kb: usize) -> String {
    let target = ctx_kb * 1024;
    let mut s = String::with_capacity(target + 256);
    let mut i = 0usize;
    while s.len() < target {
        s.push_str(&format!(
            "line {i:05} [{nonce}] the quick brown fox jumps over the lazy dog while the load \
             balancer measures prefill latency across accounts and reasoning windows.\n"
        ));
        i += 1;
    }
    s
}

fn base_params(model: &str, cache_key: &str) -> Value {
    json!({
        "model": model,
        "instructions": INSTRUCTIONS,
        "store": false,
        "stream": true,
        "reasoning": {"effort": "low"},
        "prompt_cache_key": cache_key,
    })
}

/// A single measured request against the live Codex backend. Builds a byte-faithful codex
/// `/responses` POST (same identity headers PolyFlare synthesizes, same body), pinned to `account`,
/// and times the SSE stream. On a non-2xx it captures the upstream error body (an OpenAI error
/// message — no secrets) so a 400 is diagnosable. Bounded at 120s so a genuine wedge is caught.
async fn measure(client: &reqwest::Client, account: &Account, body: Value) -> Timings {
    let mut t = Timings::default();
    let url = format!("{}/responses", account.base_url);

    // `content-type: application/json` is set by `.json()` below (avoid a duplicate header).
    let mut rb = client
        .post(&url)
        .header("authorization", format!("Bearer {}", account.bearer_token));
    for (name, value) in forward_headers(&body) {
        rb = rb.header(name, value);
    }
    if let Some(acct_id) = &account.chatgpt_account_id {
        rb = rb.header("chatgpt-account-id", acct_id.clone());
    }

    let t0 = Instant::now();
    let resp = match rb.json(&body).send().await {
        Ok(r) => r,
        Err(e) => {
            t.error = Some(format!("send: {e}"));
            t.total_ms = t0.elapsed().as_millis();
            return t;
        }
    };
    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body_txt = resp.text().await.unwrap_or_default();
        let snippet: String = body_txt.chars().take(300).collect();
        t.error = Some(format!("HTTP {status}: {snippet}"));
        t.total_ms = t0.elapsed().as_millis();
        return t;
    }

    let consumed = tokio::time::timeout(Duration::from_secs(120), async {
        let mut stream = resp.bytes_stream();
        let mut buf = String::new();
        let mut created = false;
        let mut first_delta = false;
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    if t.first_byte_ms.is_none() {
                        t.first_byte_ms = Some(t0.elapsed().as_millis());
                    }
                    buf.push_str(&String::from_utf8_lossy(&bytes));
                    if !created && buf.contains("\"response.created\"") {
                        created = true;
                        t.created_ms = Some(t0.elapsed().as_millis());
                    }
                    if !first_delta && buf.contains(".delta\"") {
                        first_delta = true;
                        t.first_delta_ms = Some(t0.elapsed().as_millis());
                    }
                    if buf.contains("\"response.completed\"") {
                        t.completed_ms = Some(t0.elapsed().as_millis());
                        break;
                    }
                }
                Err(e) => {
                    t.error = Some(format!("stream: {e}"));
                    break;
                }
            }
        }
        buf
    })
    .await;

    match consumed {
        Ok(buf) => {
            t.sse_bytes = buf.len();
            let (id, out) = parse_sse(&buf);
            t.response_id = id;
            t.output_text = out;
        }
        Err(_) => {
            t.error = Some("TIMEOUT >120s (possible wedge / silent upstream)".to_string());
        }
    }
    t.total_ms = t0.elapsed().as_millis();
    t
}

/// Best-effort SSE parse: pull the top-level response id (from `response.created`) and concatenate
/// the assistant output text (from `response.output_text.delta` events).
fn parse_sse(buf: &str) -> (Option<String>, String) {
    let mut response_id = None;
    let mut out = String::new();
    for line in buf.lines() {
        let Some(rest) = line.strip_prefix("data:") else {
            continue;
        };
        let rest = rest.trim();
        if rest.is_empty() || rest == "[DONE]" {
            continue;
        }
        let Ok(v) = serde_json::from_str::<Value>(rest) else {
            continue;
        };
        match v.get("type").and_then(Value::as_str) {
            Some("response.created") | Some("response.completed") => {
                if response_id.is_none() {
                    if let Some(id) = v.pointer("/response/id").and_then(Value::as_str) {
                        response_id = Some(id.to_string());
                    }
                }
            }
            Some("response.output_text.delta") => {
                if let Some(d) = v.get("delta").and_then(Value::as_str) {
                    out.push_str(d);
                }
            }
            _ => {}
        }
    }
    (response_id, out)
}

/// Resolve a live `Account` (fresh bearer) for `id` from the store, refreshing the OAuth token only
/// if it's stale. No writes — the refreshed token is used in-memory for this probe run.
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

fn fmt(o: Option<u128>) -> String {
    o.map(|v| format!("{v:>6}"))
        .unwrap_or_else(|| "     —".to_string())
}

fn row(label: &str, t: &Timings) {
    if let Some(err) = &t.error {
        println!("  {label:<26} ERROR: {err}");
        return;
    }
    println!(
        "  {:<26} ttft={} created={} firstΔ={} done={} total={:>6}  ({} B)",
        label,
        fmt(t.ttft()),
        fmt(t.created_ms),
        fmt(t.first_delta_ms),
        fmt(t.completed_ms),
        t.total_ms,
        t.sse_bytes,
    );
}

#[tokio::main]
async fn main() {
    let model =
        std::env::var("POLYFLARE_PROBE_MODEL").unwrap_or_else(|_| "gpt-5.6-luna".to_string());
    let runs: usize = std::env::var("POLYFLARE_PROBE_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    let ctx_kb: usize = std::env::var("POLYFLARE_PROBE_CTX_KB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(48);

    let dir = data_dir();
    let cipher = TokenCipher::load_or_create(&dir.join("key")).expect("load key");
    let store = Store::open(&dir.join("store.db"))
        .await
        .expect("open store");
    let oauth = OAuthClient::new(AUTH_BASE).expect("oauth client");
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .connect_timeout(Duration::from_secs(10))
        .build()
        .expect("http client");

    // Pick the first two active codex accounts.
    let all = store.accounts().list().await.expect("list");
    let codex: Vec<_> = all
        .into_iter()
        .filter(|a| a.provider == "codex" && a.status == "active")
        .collect();
    if codex.len() < 2 {
        eprintln!("need >=2 active codex accounts, found {}", codex.len());
        std::process::exit(1);
    }
    let (a_plan, b_plan) = (codex[0].plan_type.clone(), codex[1].plan_type.clone());
    let a = resolve_account(&store, &cipher, &oauth, &codex[0].id, "A").await;
    let b = resolve_account(&store, &cipher, &oauth, &codex[1].id, "B").await;

    println!("\n════════════════════════════════════════════════════════════════════════════");
    println!("PolyFlare handover probe — LIVE Codex backend");
    println!("  model={model}  runs={runs}  ctx≈{ctx_kb}KB  A=[{a_plan}]  B=[{b_plan}]");
    println!("  ttft/created/firstΔ/done are ms from request send; firstΔ ≈ prefill cost");
    println!("════════════════════════════════════════════════════════════════════════════");

    // ── Experiment 1: cache probe ────────────────────────────────────────────────────────────
    // Identical large context, sent A(cold) → A(warm) → B(cold) → B(warm). If the prefill cache is
    // org-scoped, A(warm) is fast but B(cold) is slow again — that slowness IS the handover's
    // reprocessing cost.
    println!("\n■ Experiment 1 — prompt-prefill cache scope (is a fresh account cold?)");
    let mut e1: Vec<(u128, u128)> = vec![]; // (a_warm_ttft, b_cold_ttft)
    for run in 0..runs {
        let nonce = format!("probe1-{run}-{}", now_millis());
        let mut body = base_params(&model, &nonce);
        body["input"] = json!([
            user_msg(&format!(
                "{}\n\nAcknowledge you received the log above.",
                filler(&nonce, ctx_kb)
            )),
            assistant_msg("Received."),
            user_msg("Reply with exactly the word: ACK"),
        ]);
        println!("\n run {run} (nonce {nonce}):");
        let a_cold = measure(&client, &a, body.clone()).await;
        row("A cold  (first hit)", &a_cold);
        let a_warm = measure(&client, &a, body.clone()).await;
        row("A warm  (repeat →A)", &a_warm);
        let b_cold = measure(&client, &b, body.clone()).await;
        row("B cold  (HANDOVER →B)", &b_cold);
        let b_warm = measure(&client, &b, body.clone()).await;
        row("B warm  (repeat →B)", &b_warm);
        if let (Some(aw), Some(bc)) = (a_warm.ttft(), b_cold.ttft()) {
            e1.push((aw, bc));
        }
    }

    // ── Experiment 2: does server-side anchoring work? + handover cost ───────────────────────
    // The wedge premise is that `previous_response_id` names a live-then-dead ephemeral anchor. Test
    // whether the backend accepts an anchor AT ALL, under store=false (what real codex sends) and
    // store=true. Where the anchor works, the handover cost = full-resend-to-B firstΔ minus
    // owner-anchored firstΔ (the reprocessing the owner avoids but a fresh account must pay).
    println!("\n■ Experiment 2 — anchor support (store variants) + handover cost");
    let mut e2: Vec<(u128, u128)> = vec![]; // (owner_ttft, handover_ttft) — only when anchor works
    for store in [false, true] {
        println!("\n store={store}:");
        for run in 0..runs {
            let nonce = format!("probe2-s{store}-{run}-{}", now_millis());
            let big = filler(&nonce, ctx_kb);
            let question = format!("{big}\n\nIn one word, what animal is mentioned above?");

            // Turn 1 on A — establishes a would-be anchor.
            let mut t1 = base_params(&model, &nonce);
            t1["store"] = json!(store);
            t1["input"] = json!([user_msg(&question)]);
            let seed = measure(&client, &a, t1).await;
            row("  turn1 →A (seed)", &seed);
            let Some(anchor) = seed.response_id.clone() else {
                println!("   (no response id — skip)");
                continue;
            };
            let o1 = if seed.output_text.is_empty() {
                "fox".to_string()
            } else {
                seed.output_text.clone()
            };

            // Owner-continue on A via the anchor (small new input — the would-be cheap path).
            let mut cont = base_params(&model, &nonce);
            cont["store"] = json!(store);
            cont["previous_response_id"] = json!(anchor);
            cont["input"] = json!([user_msg("Reply with exactly the word: ACK")]);
            let owner = measure(&client, &a, cont).await;
            row("  turn2 →A (anchored)", &owner);

            // Handover: full resend to B, no anchor (what ResendFull does).
            let mut resend = base_params(&model, &nonce);
            resend["store"] = json!(store);
            resend["input"] = json!([
                user_msg(&question),
                assistant_msg(&o1),
                user_msg("Reply with exactly the word: ACK"),
            ]);
            let handover = measure(&client, &b, resend).await;
            row("  turn2 →B (full resend)", &handover);

            if owner.error.is_none() {
                if let (Some(ow), Some(ho)) = (owner.ttft(), handover.ttft()) {
                    e2.push((ow, ho));
                }
            }
        }
    }

    // ── Summary ──────────────────────────────────────────────────────────────────────────────
    println!("\n════════════════════════════════════════════════════════════════════════════");
    println!("SUMMARY (median time-to-first-token, ms)");
    if !e1.is_empty() {
        let aw = median(e1.iter().map(|x| x.0));
        let bc = median(e1.iter().map(|x| x.1));
        println!(
            "  Exp1  A-warm={aw:>6}   B-cold(handover)={bc:>6}   penalty={:>+6}  ({:.1}×)",
            bc as i128 - aw as i128,
            bc as f64 / aw.max(1) as f64
        );
    }
    if !e2.is_empty() {
        let ow = median(e2.iter().map(|x| x.0));
        let ho = median(e2.iter().map(|x| x.1));
        println!(
            "  Exp2  owner-anchor={ow:>6}   full-resend→B={ho:>6}   penalty={:>+6}  ({:.1}×)",
            ho as i128 - ow as i128,
            ho as f64 / ow.max(1) as f64
        );
    }
    println!("════════════════════════════════════════════════════════════════════════════\n");
}

fn median(it: impl Iterator<Item = u128>) -> u128 {
    let mut v: Vec<u128> = it.collect();
    v.sort_unstable();
    if v.is_empty() {
        0
    } else {
        v[v.len() / 2]
    }
}
