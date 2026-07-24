//! cache_billing_probe — measures the field observation behind `docs/TRANSPORT-FINDINGS-2026-07-17.md`
//! decision **D4**: "same-account continuation keeps the prompt cache warm; bouncing accounts is a
//! cold cache every turn." That doc states the lever (`prompt_cache_key` affinity) but carries no
//! measured token-level number for it. This probe measures the actual billing signal: the
//! `usage.input_tokens_details.cached_tokens` field the Codex backend returns on every
//! `response.completed` event.
//!
//! Three questions, in order:
//!   1. Same-account, stable `prompt_cache_key`, two turns (turn 1 cold / establishes the prefix,
//!      turn 2 continues it) — does turn 2 report `cached_tokens > 0`? What fraction of turn 2's
//!      `input_tokens` were billed at the cached rate?
//!   2. The SAME turn-2 continuation, same `prompt_cache_key`, sent to a DIFFERENT account — does
//!      it report `cached_tokens ≈ 0` (the cache is per-account/org-scoped, so a fresh account never
//!      shares it)? Skipped (not failed) if there is no second account with headroom.
//!   3. The verdict: the measured per-turn input-token saving from cache affinity, printed as a
//!      percentage, plus a one-line summary sentence.
//!
//! ## Where the usage field shape came from
//! `openai/codex` `codex-rs/codex-api/src/sse/responses.rs`:
//!   - `struct ResponseCompletedUsage` (`input_tokens`, `input_tokens_details`, `output_tokens`,
//!     `output_tokens_details`, `total_tokens`) parsed off the `response.completed` SSE event's
//!     `response.usage` object.
//!   - `struct ResponseCompletedInputTokensDetails { cached_tokens, cache_write_tokens }` — the
//!     `cached_tokens` field is exactly the "billed at the cached rate" count this probe reads.
//!   - The test fixture at that file's `parses_cache_write_token_usage` test and the
//!     `process_sse_emits_completed_with_usage`-style fixtures confirm the wire shape:
//!     `{"type":"response.completed","response":{"id":"...","usage":{"input_tokens":100,
//!     "input_tokens_details":{"cached_tokens":40,"cache_write_tokens":60},"output_tokens":10,
//!     "output_tokens_details":{"reasoning_tokens":5},"total_tokens":110}}}`.
//!     So this probe scrapes `data.response.usage.{input_tokens,total_tokens}` and
//!     `data.response.usage.input_tokens_details.{cached_tokens,cache_write_tokens}` out of the raw
//!     SSE buffer — the same "don't reach into server-private internals, reimplement the tiny bit
//!     we need" idiom `ws_ratelimit_probe.rs` uses for `/wham/usage`.
//!
//! ## Modeled on
//! `handover_probe.rs` — account loading (`resolve_account`, no writeback), the `/responses` HTTP
//! turn shape, SSE draining. `ws_ratelimit_probe.rs` — the local (zero-network) headroom ranking,
//! dry-run-by-default cost-plan gate, and the printed-verdict style.
//!
//! ## HOW TO RUN IT (when you have headroom — NOT now, NOT by default)
//! Running this file with no arguments ONLY PRINTS THE COST PLAN — zero network calls, zero quota
//! spent. To actually execute it against your most-headroom ACTIVE codex account(s):
//!
//! ```text
//! cargo run -p polyflare-server --example cache_billing_probe --release -- --live
//! ```
//!
//! Env vars (read the printed plan either way before adding `--live`):
//!   POLYFLARE_PROBE_MODEL   default "gpt-5.6-luna"
//!   POLYFLARE_DATA_DIR      default $HOME/.polyflare
//!
//! Total live cost: still one of the cheapest probes here — the cache signal lives in each
//! response's `usage`, not in a moving window, so it needs only a SHORT conversation. Each turn
//! carries a deterministic ~4k-token content-free prefix (see `stable_cache_prefix`) so turn 1
//! clears OpenAI's ~1024-token prompt-cache minimum — WITHOUT it, `cached_tokens` is structurally 0
//! and the probe measures nothing (the reason an earlier tiny-prompt version read 0% cache).
//!   - 1 account (minimum): 2 real model generations (turn 1 seed + turn 2 same-account
//!     continuation), ~4k input tokens each.
//!   - 2 accounts (if a 2nd ACTIVE account with headroom exists): +1 more generation (turn 2's
//!     continuation replayed on the 2nd account) = 3 total.
//!
//! No `/wham/usage` reads at all — this probe never touches rate-limit windows, only per-response
//! `usage` fields already returned by the turns it sends anyway.
//!
//! SAFETY: never prints a token, cookie, refresh token, or Authorization value — only header NAMES
//! and non-secret usage numbers (account rank label, plan_type, token COUNTS). Never prints
//! conversation content beyond the short generic literal prompts + the deterministic content-free
//! padding block this probe itself authors. Only touches `active`
//! accounts (never `quota_exceeded` / `rate_limited` / anything else), and of those, the ones with
//! the MOST local headroom.

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

const INSTRUCTIONS: &str =
    "You are a terse assistant in a cache-billing measurement probe. Answer in as few tokens as \
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

/// A deterministic, content-free ~4k-token block prepended to the seed prompt ONLY to push turn 1's
/// input above OpenAI's ~1024-token prompt-cache minimum prefix length — below that floor the
/// backend never caches, so `cached_tokens` reads 0 regardless of same-account affinity (the reason
/// a tiny-prompt run measures nothing). Identical bytes on every call (line index only, no
/// randomness / no timestamp) so turn 1 and turn 2 share ONE cacheable prefix under a single
/// `prompt_cache_key`. Carries no real conversation content — pure fixed padding.
fn stable_cache_prefix() -> String {
    let mut s = String::with_capacity(200 * 120);
    s.push_str("Deterministic prompt-cache measurement context block (content-free padding):\n");
    for i in 0..200 {
        s.push_str(&format!(
            "Line {i:04}: fixed padding to exceed the prompt-cache minimum prefix length; \
             this line carries no real conversation content whatsoever.\n"
        ));
    }
    s
}

/// The synthesized codex identity headers for a translated request — a verbatim copy of
/// `ingress::synthesize_codex_forward_headers` (private), same as `handover_probe.rs`.
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

/// The token-usage fields this probe cares about, off `response.completed`'s `response.usage`
/// object. Field names/shape confirmed against `codex-rs/codex-api/src/sse/responses.rs`
/// (`ResponseCompletedUsage` / `ResponseCompletedInputTokensDetails`) — see module doc comment.
#[derive(Debug, Default, Clone, Copy)]
struct CacheUsage {
    input_tokens: i64,
    cached_tokens: i64,
    cache_write_tokens: i64,
    output_tokens: i64,
    total_tokens: i64,
}

impl CacheUsage {
    /// Fraction of `input_tokens` billed at the cached rate, as a percent. `None` when
    /// `input_tokens` is zero (nothing to divide by — happens on error/empty responses).
    fn cached_fraction_pct(&self) -> Option<f64> {
        if self.input_tokens <= 0 {
            None
        } else {
            Some(100.0 * self.cached_tokens as f64 / self.input_tokens as f64)
        }
    }
}

/// One turn's outcome: token usage plus enough plumbing to chain the next turn. Never carries
/// conversation content beyond what the caller already authored as a short literal prompt.
#[derive(Default)]
struct Turn {
    response_id: Option<String>,
    output_text: String,
    usage: Option<CacheUsage>,
    total_ms: u128,
    sse_bytes: usize,
    error: Option<String>,
}

/// A single measured request against the live Codex backend, pinned to `account`. Bounded at 90s.
async fn measure(client: &reqwest::Client, account: &Account, body: Value) -> Turn {
    let mut t = Turn::default();
    let url = format!("{}/responses", account.base_url);

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

    let consumed = tokio::time::timeout(Duration::from_secs(90), async {
        let mut stream = resp.bytes_stream();
        let mut buf = String::new();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    buf.push_str(&String::from_utf8_lossy(&bytes));
                    if buf.contains("\"response.completed\"") {
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
            let (id, out, usage) = parse_sse(&buf);
            t.response_id = id;
            t.output_text = out;
            t.usage = usage;
        }
        Err(_) => {
            t.error = Some("TIMEOUT >90s (possible wedge / silent upstream)".to_string());
        }
    }
    t.total_ms = t0.elapsed().as_millis();
    t
}

/// Best-effort SSE parse: pull the response id, concatenated output text, and the `usage` block
/// off the `response.completed` event. Field paths mirror `ResponseCompletedUsage` exactly.
fn parse_sse(buf: &str) -> (Option<String>, String, Option<CacheUsage>) {
    let mut response_id = None;
    let mut out = String::new();
    let mut usage = None;
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
                if let Some(u) = v.pointer("/response/usage") {
                    usage = Some(CacheUsage {
                        input_tokens: u.get("input_tokens").and_then(Value::as_i64).unwrap_or(0),
                        cached_tokens: u
                            .pointer("/input_tokens_details/cached_tokens")
                            .and_then(Value::as_i64)
                            .unwrap_or(0),
                        cache_write_tokens: u
                            .pointer("/input_tokens_details/cache_write_tokens")
                            .and_then(Value::as_i64)
                            .unwrap_or(0),
                        output_tokens: u.get("output_tokens").and_then(Value::as_i64).unwrap_or(0),
                        total_tokens: u.get("total_tokens").and_then(Value::as_i64).unwrap_or(0),
                    });
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
    (response_id, out, usage)
}

/// Resolve a live `Account` (fresh bearer) for `id` from the store, refreshing the OAuth token only
/// if it's stale. No writes — the refreshed token is used in-memory for this probe run only.
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
    let mut id_token = tokens.id_token.clone();
    if oauth::should_refresh(oauth::token_exp(&access), row.last_refresh, now_secs()) {
        match oauth.refresh(&tokens.refresh_token).await {
            Ok(r) => {
                access = r.tokens.access_token;
                id_token = r.tokens.id_token;
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
        is_fedramp: oauth::is_fedramp_account(&id_token),
    }
}

/// Local (DB-only, zero network) headroom score for ranking candidates: the higher of the two
/// windows' last-known `used_percent` — lower score = more headroom. Never touches the network, so
/// this runs even in dry-run mode.
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

fn fmt_ms(ms: u128) -> String {
    format!("{ms:>6}")
}

fn row(label: &str, t: &Turn) {
    if let Some(err) = &t.error {
        println!("    {label:<28} ERROR: {err}");
        return;
    }
    match &t.usage {
        Some(u) => println!(
            "    {:<28} in={:>6} cached={:>6} cache_write={:>6} out={:>5} total={:>6} \
             total_ms={} ({} B) resp_id={}",
            label,
            u.input_tokens,
            u.cached_tokens,
            u.cache_write_tokens,
            u.output_tokens,
            u.total_tokens,
            fmt_ms(t.total_ms),
            t.sse_bytes,
            if t.response_id.is_some() {
                "yes"
            } else {
                "NONE"
            },
        ),
        None => println!(
            "    {:<28} (no usage in response)   total_ms={} ({} B) resp_id={}",
            label,
            fmt_ms(t.total_ms),
            t.sse_bytes,
            if t.response_id.is_some() {
                "yes"
            } else {
                "NONE"
            },
        ),
    }
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
    if candidates.is_empty() {
        eprintln!("need >=1 ACTIVE codex account, found 0 (quota_exceeded/rate_limited excluded)");
        std::process::exit(1);
    }
    let mut scored = Vec::with_capacity(candidates.len());
    for a in &candidates {
        scored.push((headroom_score(&store, &a.id).await, a.clone()));
    }
    scored.sort_by(|x, y| x.0.partial_cmp(&y.0).unwrap());

    let acct_a = scored[0].1.clone();
    let has_second = scored.len() >= 2;

    println!("\n════════════════════════════════════════════════════════════════════════════");
    println!("cache_billing_probe — measures TRANSPORT-FINDINGS D4: does same-account");
    println!("continuation with a stable prompt_cache_key report cached input tokens?");
    println!("════════════════════════════════════════════════════════════════════════════");
    println!("  model={model}");
    println!(
        "  Account A (most headroom, rank #1)  plan=[{}]  local used_percent≈{:.1}%",
        acct_a.plan_type, scored[0].0
    );
    if has_second {
        let acct_b = &scored[1].1;
        println!(
            "  Account B (2nd most headroom, rank #2)  plan=[{}]  local used_percent≈{:.1}%",
            acct_b.plan_type, scored[1].0
        );
        println!("  → cross-account cache-miss contrast WILL be attempted (B has headroom).");
    } else {
        println!(
            "  Only 1 ACTIVE codex account with headroom found — cross-account contrast will be \
             SKIPPED (not failed)."
        );
    }
    println!("\n  THIS WILL CONSUME REAL QUOTA ON THE ACCOUNT(S) ABOVE IF RUN WITH --live:");
    println!("    Account A: 2 short generations (turn 1 seed + turn 2 same-account continuation)");
    if has_second {
        println!("    Account B: 1 short generation (turn 2's SAME continuation, cross-account)");
        println!("    Total: 3 real model generations. Zero /wham/usage reads (not needed).");
    } else {
        println!("    Total: 2 real model generations. Zero /wham/usage reads (not needed).");
    }
    println!(
        "    Each generation carries a deterministic ~4k-token content-free prefix (authored by this\n\
         \x20   probe) so turn 1 clears OpenAI's ~1024-token prompt-cache minimum — below that floor\n\
         \x20   cached_tokens is structurally 0 and the probe would measure nothing. Still cheap:\n\
         \x20   ~4k input tokens/gen, trivial output, no /wham/usage reads."
    );
    println!("════════════════════════════════════════════════════════════════════════════");

    if !live {
        println!(
            "\nDRY RUN (default) — zero network calls were made, zero quota consumed. Re-run with \
             `--live` appended only when you have headroom on the account(s) above:\n"
        );
        println!(
            "  cargo run -p polyflare-server --example cache_billing_probe --release -- --live\n"
        );
        return;
    }

    // ── Live path from here — every line above already printed before anything was sent. ──────
    let oauth = OAuthClient::new(AUTH_BASE).expect("oauth client");
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .connect_timeout(Duration::from_secs(10))
        .build()
        .expect("http client");

    let a = resolve_account(&store, &cipher, &oauth, &acct_a.id, "A").await;

    let nonce = format!("cache-probe-{}", now_millis());
    println!("\n■ Same-account run — account A, stable prompt_cache_key={nonce}");

    // The seed prompt carries a large, deterministic, content-free prefix (see
    // `stable_cache_prefix`) so turn 1's input clears OpenAI's ~1024-token prompt-cache MINIMUM —
    // below that floor `cached_tokens` is structurally 0 no matter how warm the affinity, which is
    // exactly why a tiny-prompt probe measures nothing. Turn 1 and turn 2 share these identical
    // bytes verbatim, so they form one cacheable prefix under this run's `prompt_cache_key`.
    let seed_prompt = format!(
        "{}\nThis is turn one of a cache-affinity measurement. Reply with exactly the word: ok.",
        stable_cache_prefix()
    );

    // Turn 1 (cold): establishes + WRITES the prefix cache under this key (expect cache_write>0).
    let mut t1 = base_params(&model, &nonce);
    t1["input"] = json!([user_msg(&seed_prompt)]);
    let seed = measure(&client, &a, t1).await;
    row("A turn 1 (cold, seed)", &seed);
    if seed.response_id.is_none() {
        eprintln!("\n  Turn 1 produced no response id — continuing anyway (best-effort).");
    }
    let o1 = fallback_text(&seed, "ok");

    // Turn 2 (same account): continue the SAME conversation, same prompt_cache_key, via the
    // history-based continuation HTTP actually uses (no previous_response_id — TRANSPORT-FINDINGS
    // fact 1: HTTP rejects it under store:false). The shared long prefix should now READ the cache
    // (expect cached_tokens ≈ the prefix size).
    let mut t2 = base_params(&model, &nonce);
    t2["input"] = json!([
        user_msg(&seed_prompt),
        assistant_msg(&o1),
        user_msg("Reply with the single word: ok."),
    ]);
    let turn2_same = measure(&client, &a, t2.clone()).await;
    row("A turn 2 (same-account cont.)", &turn2_same);

    // ── Cross-account cache-miss contrast (only if a 2nd headroom account exists) ───────────────
    let mut turn2_cross: Option<Turn> = None;
    if has_second {
        let acct_b = scored[1].1.clone();
        println!(
            "\n■ Cross-account run — SAME turn-2 continuation + SAME prompt_cache_key, sent to \
             account B"
        );
        let b = resolve_account(&store, &cipher, &oauth, &acct_b.id, "B").await;
        let t = measure(&client, &b, t2).await;
        row("B turn 2 (cross-account, same key)", &t);
        turn2_cross = Some(t);
    } else {
        println!(
            "\n■ Cross-account run — SKIPPED (no 2nd ACTIVE codex account with headroom found)"
        );
    }

    // ── Verdict ──────────────────────────────────────────────────────────────────────────────
    println!("\n════════════════════════════════════════════════════════════════════════════");
    println!("VERDICT");
    match &turn2_same.usage {
        Some(u) => {
            let pct = u.cached_fraction_pct();
            match pct {
                Some(p) => {
                    println!(
                        "  Same-account turn 2: {}/{} input tokens cached ({p:.1}% of turn-2 input \
                         billed at the cached rate; cache_write={}).",
                        u.cached_tokens, u.input_tokens, u.cache_write_tokens
                    );
                }
                None => println!(
                    "  Same-account turn 2: usage present but input_tokens=0 — inconclusive."
                ),
            }
        }
        None => println!(
            "  Same-account turn 2: NO usage field in the response — inconclusive (see error/row \
             above)."
        ),
    }
    match &turn2_cross {
        Some(Turn { usage: Some(u), .. }) => {
            println!(
                "  Cross-account turn 2 (same continuation, same key): {}/{} input tokens cached \
                 ({}).",
                u.cached_tokens,
                u.input_tokens,
                if u.cached_tokens == 0 {
                    "confirms per-account cache scope"
                } else {
                    "UNEXPECTED — cache hit across accounts"
                }
            );
        }
        Some(Turn { usage: None, .. }) => {
            println!("  Cross-account turn 2: NO usage field in the response — inconclusive.");
        }
        None => println!("  Cross-account turn 2: SKIPPED (no 2nd headroom account)."),
    }
    if let Some(u) = &turn2_same.usage {
        if let Some(p) = u.cached_fraction_pct() {
            let cross_note = match &turn2_cross {
                Some(Turn {
                    usage: Some(cu), ..
                }) if cu.input_tokens > 0 => format!(
                    "; 0-account-bounce would have re-billed ~{:.1}% of that turn's input at full \
                     rate instead",
                    100.0 - (100.0 * cu.cached_tokens as f64 / cu.input_tokens as f64)
                ),
                _ => String::new(),
            };
            println!(
                "  → cache affinity saves ~{p:.0}% of per-turn input billing on a warm \
                 same-account session{cross_note}."
            );
        }
    }
    println!("════════════════════════════════════════════════════════════════════════════\n");
}
