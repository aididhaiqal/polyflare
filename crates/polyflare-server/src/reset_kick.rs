//! Reset kick: pin an account's weekly window the moment it resets.
//!
//! Measured on the master, 2026-09-19: after an account's weekly reset passed, the usage endpoint
//! reported 0% but its `reset_at` SLID FORWARD on every poll (16:28 → 16:39 → 16:49 …, always
//! "now + 7 days") until the first real generation pinned it — 18:17 for one account, 18:04 for
//! another. An idle account's week therefore does not start at its reset; it starts whenever
//! traffic happens to reach it, and every idle hour pushes the NEXT reset out by an hour. With a
//! fleet that is spent in lockstep, that is exactly the hour the next account is needed.
//!
//! A kick is one minimal generation on the account (`store: false`, low effort, a one-word
//! answer; ~20 tokens) followed by a usage refresh, so the window starts at the reset and the
//! store sees the new numbers immediately. Runs three ways: a scheduler that watches for a passed
//! reset or a sliding window, `POST /api/accounts/{id}/kick`, and `polyflare accounts kick`.
//!
//! Content-free: nothing of the (fixed, synthetic) request or its answer is logged; only outcome
//! words, codes, statuses and token counts.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use polyflare_store::{Store, TokenCipher};
use serde::Serialize;

use crate::app::AppState;
use crate::log_bus::LogEvent;

/// The cheapest model the fleet serves everywhere. Overridable per call.
pub const DEFAULT_KICK_MODEL: &str = "gpt-5.6-luna";
/// How often the scheduler looks.
const SCHEDULER_TICK: Duration = Duration::from_secs(60);
/// Minimum spacing between kicks of one account, so a refusing upstream cannot be hammered.
const KICK_SPACING_SECS: i64 = 20 * 60;
/// A weekly `reset_at` that moved forward by more than this between two polls is sliding, i.e.
/// the window has not been pinned by a generation yet (polls are ~10 min apart).
const SLIDE_THRESHOLD_SECS: i64 = 300;
/// Whole-request bound for the kick generation.
const KICK_TIMEOUT: Duration = Duration::from_secs(60);

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Serialize)]
pub struct KickResponse {
    pub account_id: String,
    pub model: String,
    /// `completed` | `refused` | `http_error` | `transport` | `timeout` | `no_tokens`.
    pub outcome: &'static str,
    pub error_code: Option<String>,
    pub http_status: Option<u16>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    /// Whether the post-kick usage refresh returned trustworthy numbers.
    pub usage_refreshed: bool,
    pub status_after: Option<String>,
    pub reset_at_after: Option<i64>,
    pub kicked_at: i64,
}

/// The generation itself, against the store + cipher only (no `AppState`), so the CLI can run it
/// in another process. Does NOT refresh usage — see [`kick_account`] / [`kick_standalone`].
pub async fn send_kick(
    store: &Store,
    cipher: &TokenCipher,
    upstream_base: &str,
    codex_version: &str,
    account_id: &str,
    model: &str,
) -> KickResponse {
    let kicked_at = unix_now();
    let mut out = KickResponse {
        account_id: account_id.to_string(),
        model: model.to_string(),
        outcome: "transport",
        error_code: None,
        http_status: None,
        input_tokens: None,
        output_tokens: None,
        usage_refreshed: false,
        status_after: None,
        reset_at_after: None,
        kicked_at,
    };
    let (account, tokens) = match store.accounts().get_with_tokens(account_id, cipher).await {
        Ok(Some(pair)) => pair,
        _ => {
            out.outcome = "no_tokens";
            return out;
        }
    };
    let client = match polyflare_codex::build_client() {
        Ok(c) => c,
        Err(_) => return out,
    };
    let mut request = client
        .post(format!("{}/responses", upstream_base.trim_end_matches('/')))
        .timeout(KICK_TIMEOUT)
        .header("Authorization", format!("Bearer {}", tokens.access_token))
        .header("Content-Type", "application/json")
        .header("Accept", "text/event-stream")
        .header("originator", polyflare_codex::codex_headers::originator())
        .header(
            "User-Agent",
            polyflare_codex::codex_headers::codex_user_agent(codex_version),
        );
    if let Some(chatgpt_account_id) = &account.chatgpt_account_id {
        request = request.header("chatgpt-account-id", chatgpt_account_id);
    }
    if polyflare_codex::oauth::is_fedramp_account(&tokens.id_token) {
        request = request.header("x-openai-fedramp", "true");
    }
    let body = serde_json::json!({
        "model": model,
        "store": false,
        "stream": true,
        "reasoning": {"effort": "low"},
        "instructions": "Reply with the single word ok.",
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
    });
    let response = match request.json(&body).send().await {
        Ok(r) => r,
        Err(e) if e.is_timeout() => {
            out.outcome = "timeout";
            return out;
        }
        Err(_) => return out,
    };
    let status = response.status().as_u16();
    out.http_status = Some(status);
    if status != 200 {
        out.outcome = "http_error";
        let text = response.text().await.unwrap_or_default();
        out.error_code = serde_json::from_str::<serde_json::Value>(&text)
            .ok()
            .and_then(|v| {
                v.pointer("/error/code")
                    .and_then(|c| c.as_str())
                    .map(str::to_string)
            });
        return out;
    }
    // Read the SSE stream to its terminal frame (bounded by the client timeout).
    let text = match tokio::time::timeout(KICK_TIMEOUT, response.text()).await {
        Ok(Ok(t)) => t,
        Ok(Err(_)) => return out,
        Err(_) => {
            out.outcome = "timeout";
            return out;
        }
    };
    out.outcome = "transport";
    for line in text.lines() {
        let Some(payload) = line.strip_prefix("data:").map(str::trim) else {
            continue;
        };
        let Some((ty, code, _)) = crate::trace::frame_facts(payload) else {
            continue;
        };
        match ty.as_str() {
            "response.completed" => {
                out.outcome = "completed";
                if let Some(u) = crate::trace::usage_facts(payload) {
                    out.input_tokens = u.input;
                    out.output_tokens = u.output;
                }
                break;
            }
            "response.failed" | "error" | "response.incomplete" => {
                out.outcome = "refused";
                out.error_code = code;
                break;
            }
            _ => {}
        }
    }
    out
}

/// Kick through the running server: the generation, then an immediate usage refresh so the
/// routing gate and the dashboard see the pinned window at once.
pub async fn kick_account(state: &AppState, account_id: &str, model: Option<&str>) -> KickResponse {
    let model = model.unwrap_or(DEFAULT_KICK_MODEL);
    let version = state.codex_version.cached_or_fallback();
    let mut out = send_kick(
        &state.store,
        &state.cipher,
        state.upstream_base_url_for(polyflare_core::Provider::Codex),
        &version,
        account_id,
        model,
    )
    .await;
    out.usage_refreshed = crate::usage_refresh::refresh_account_now(state, account_id)
        .await
        .unwrap_or(false);
    if let Ok(Some(account)) = state.store.accounts().get(account_id).await {
        out.status_after = Some(account.status);
        out.reset_at_after = account.reset_at;
    }
    let _ = state.account_cache.snapshots(&state.store).await;
    state.relay_metrics.record(match out.outcome {
        "completed" => "reset_kick_completed",
        _ => "reset_kick_failed",
    });
    tracing::info!(
        target: "polyflare_server::reset_kick",
        account_id,
        model,
        outcome = out.outcome,
        error_code = out.error_code.as_deref().unwrap_or("-"),
        http_status = out.http_status.unwrap_or(0),
        status_after = out.status_after.as_deref().unwrap_or("-"),
        "reset kick"
    );
    state.log_bus.publish(LogEvent::info(
        "reset_kick",
        format!(
            "kick {}: {} code={} status_after={}",
            &account_id[..account_id.len().min(13)],
            out.outcome,
            out.error_code.as_deref().unwrap_or("-"),
            out.status_after.as_deref().unwrap_or("-")
        ),
    ));
    out
}

/// Whether `account_id`'s weekly window is currently unpinned — its `reset_at` moved forward
/// between the last two polls while usage stayed at zero.
async fn weekly_window_is_sliding(store: &Store, account_id: &str, now: i64) -> bool {
    let Ok(rows) = store
        .accounts()
        .usage_history_full_since(account_id, now - 3 * 3600)
        .await
    else {
        return false;
    };
    let weekly: Vec<_> = rows
        .iter()
        .filter(|(_, w)| w.window_minutes.is_none_or(|m| m > 24 * 60))
        .collect();
    let n = weekly.len();
    if n < 2 {
        return false;
    }
    let (_, last) = weekly[n - 1];
    let (_, prev) = weekly[n - 2];
    match (last.reset_at, prev.reset_at) {
        (Some(a), Some(b)) => a - b >= SLIDE_THRESHOLD_SECS && last.used_percent < 1.0,
        _ => false,
    }
}

/// The scheduler: every minute, kick any Codex account whose reset has passed while it is still
/// gated, or whose fresh window is sliding unpinned. At most one kick per account per
/// [`KICK_SPACING_SECS`]. Disabled live via the `reset_kick_enabled` setting.
pub fn spawn_reset_kick(state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut last_kick: HashMap<String, i64> = HashMap::new();
        loop {
            tokio::time::sleep(SCHEDULER_TICK).await;
            if !state.runtime_settings.reset_kick_enabled() {
                continue;
            }
            let now = unix_now();
            let accounts = state.store.accounts().list().await.unwrap_or_default();
            for account in accounts.iter().filter(|a| a.provider == "codex") {
                if last_kick
                    .get(&account.id)
                    .is_some_and(|at| now - at < KICK_SPACING_SECS)
                {
                    continue;
                }
                let reset_passed =
                    matches!(account.status.as_str(), "quota_exceeded" | "rate_limited")
                        && account.reset_at.is_some_and(|r| r <= now);
                let reason = if reset_passed {
                    "reset_passed"
                } else if account.status == "active"
                    && weekly_window_is_sliding(&state.store, &account.id, now).await
                {
                    "window_sliding"
                } else {
                    continue;
                };
                last_kick.insert(account.id.clone(), now);
                tracing::info!(
                    target: "polyflare_server::reset_kick",
                    account_id = %account.id,
                    reason,
                    "kicking the account to pin its weekly window"
                );
                let _ = kick_account(&state, &account.id, None).await;
            }
        }
    });
}

/// CLI path: kick + usage refresh against the store directly (no running server needed). The
/// server picks the store change up on its next account-cache rebuild.
pub async fn kick_standalone(
    store: &Store,
    cipher: &TokenCipher,
    upstream_base: &str,
    account_id: &str,
    model: Option<&str>,
) -> KickResponse {
    let model = model.unwrap_or(DEFAULT_KICK_MODEL);
    let version = polyflare_codex::CodexVersionCache::new()
        .map(|c| c.cached_or_fallback())
        .unwrap_or_else(|_| "0.0.0".to_string());
    let mut out = send_kick(store, cipher, upstream_base, &version, account_id, model).await;
    out.usage_refreshed =
        crate::usage_refresh::refresh_account_standalone(store, cipher, upstream_base, account_id)
            .await;
    if let Ok(Some(account)) = store.accounts().get(account_id).await {
        out.status_after = Some(account.status);
        out.reset_at_after = account.reset_at;
    }
    out
}

/// `POST /api/accounts/{id}/kick[?model=…]`.
pub async fn kick_handler(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match state.store.accounts().get(&id).await {
        Ok(Some(a)) if a.provider == "codex" => {}
        Ok(Some(_)) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                "kick is only for codex accounts",
            )
                .into_response()
        }
        Ok(None) => return (axum::http::StatusCode::NOT_FOUND, "no such account").into_response(),
        Err(_) => return crate::ingress::internal_error(),
    }
    axum::Json(kick_account(&state, &id, q.get("model").map(String::as_str)).await).into_response()
}
