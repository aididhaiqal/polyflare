//! D17 Task 2: soft session→owner affinity account resolution for CONTROL requests
//! (`thread/goal/*`, `agent-identities/jwks`, `memories/trace_summarize` — the D17 minimal
//! control-endpoint set; see `docs/superpowers/plans/2026-07-18-d17-control-endpoints.md`).
//!
//! Unlike `/responses`'s HARD `previous_response_id` anchor (`crate::watchdog::apply_ownership`,
//! which narrows to the pinned owner and RECOVERS — never falls back to a different account — when
//! that owner turns out ineligible), a control request has no such anchor. Binding it to the
//! conversation's owner here is a SOFT, best-effort optimization: a request that carries no session
//! header, or whose owner happens to be unavailable right now, ALWAYS falls through to normal
//! (any-eligible) selection — the exact same machinery `/responses` uses when unowned. Over-binding
//! this into a hard pin-or-fail is the primary risk the D17 scoping study flagged (Global
//! Constraints: "SOFT affinity — do NOT over-bind (inviolable for correctness)").

use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use reqwest::Method;

use polyflare_core::{Account, AccountId, Provider, SelectionCtx};

use crate::app::AppState;
use crate::ingress::{
    forward_headers_from_inbound, internal_error, no_eligible, resolve_core_account,
    spawn_persist_request_log,
};
use crate::observability::RequestLog;
use crate::session_key::header_session_key;
use crate::snapshot::filter_by_provider_and_pool;

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Resolve which account a request should be forwarded to given an already-derived `session_key`
/// and `pool` scope, then materialize it (decrypt + refresh-if-stale, via
/// `crate::ingress::resolve_core_account` — unchanged from `/responses`).
///
/// D14a Task 1: this is the soft-affinity core extracted out of `resolve_control_account`,
/// generalized so a caller can supply a BODY-derived `session_key` (not just a header-only one)
/// and a `pool` (so selection can be scoped to a single pool instead of "all Codex accounts").
/// `resolve_control_account` below is a thin wrapper that derives its header-only key and passes
/// `pool = None` — byte-identical behavior to the pre-extraction version.
///
/// 1. `session_key` names the affinity signal (or `None` for "no affinity signal" — the caller
///    decides how it was derived; `resolve_control_account` uses
///    `crate::session_key::header_session_key`, the SAME Hard-strength derivation
///    `session_key::parse_inbound` uses for `x-codex-turn-state`/`session_id`/`x-session-id`).
/// 2. If a session key was given AND its continuity session row names an owner
///    (`ContinuityRepo::get_session(..).owning_account_id` — the same read-only primitive
///    `CodexContinuity::prepare` uses) AND that owner is currently ELIGIBLE (appears pickable in
///    the overlaid + provider/pool-filtered snapshot — checked by narrowing candidates to just the
///    owner and running it through the SAME selector, mirroring `watchdog::apply_ownership`'s
///    narrow-then-pick shape) ⇒ use the owner (soft affinity hit).
/// 3. OTHERWISE (no session key, no owner on record, or the owner is currently ineligible —
///    benched/cooled-down/inactive/absent from the pool) ⇒ fall through to the SAME any-eligible
///    selection `/responses` uses when unowned: `account_cache.snapshots()` →
///    `filter_by_provider_and_pool(Codex, pool)` → `runtime.overlay` → `selector.pick`. **This
///    fallback is INVIOLABLE** — a call must never be stranded merely because its owner happens to
///    be unavailable right now (contrast `apply_ownership`'s `RouteDecision::Recover`, which is
///    correct for `/responses`'s hard anchor but would be over-binding here).
/// 4. `resolve_core_account` the chosen id.
///
/// No eligible account at all (neither the owner nor any fallback candidate) ⇒ a clean 503
/// (`crate::ingress::no_eligible`, byte-identical to `/responses`'s empty-pool response).
pub(crate) async fn resolve_owner_affine_account(
    state: &AppState,
    session_key: Option<&polyflare_core::SessionKey>,
    pool: Option<&str>,
) -> Result<(Account, AccountId), Response> {
    let now = unix_now();

    let snapshots = match state.account_cache.snapshots(&state.store).await {
        Ok(s) => s,
        Err(_) => return Err(internal_error()),
    };
    let mut snapshots = filter_by_provider_and_pool(&snapshots, Provider::Codex, pool);
    state.runtime.overlay(&mut snapshots, now);
    let selector = state.selector_for(pool);
    let sel_ctx = SelectionCtx {
        now,
        // C9 Task 3: startup-resolved, never a per-request env read (mirrors the `/responses`/
        // `/v1/messages` sel_ctx sites in `crate::ingress`).
        inflight_penalty_pct: state.inflight_penalty_pct,
        ..Default::default()
    };

    // Step 2: soft owner lookup — a read-only session-row fetch, no write, no watchdog arm, no
    // recovery plan (control has none of those concepts; this is deliberately NOT
    // `Continuity::prepare`, which would also mutate the session row's state).
    let owner: Option<AccountId> = match session_key {
        Some(sk) => match state.store.continuity().get_session(&sk.value).await {
            Ok(Some(row)) => row.owning_account_id.map(AccountId::from),
            Ok(None) | Err(_) => None,
        },
        None => None,
    };

    // Step 2 (eligibility) + Step 3 (inviolable fallback).
    let picked = match owner {
        Some(owner_id) => {
            let narrowed: Vec<_> = snapshots
                .iter()
                .filter(|s| s.id == owner_id)
                .cloned()
                .collect();
            match selector.pick(&narrowed, &sel_ctx) {
                // The owner is present in the eligible pool and the selector accepted it (the
                // narrowed candidate list has exactly one member, so a `Some` here can only ever
                // be that owner) — soft affinity hit.
                Some(id) => id,
                // Owner absent from the pool entirely, or present but currently ineligible
                // (benched/cooled-down/inactive/wrong-provider) ⇒ NEVER stranded: fall through to
                // the same any-eligible selection an unowned request would get.
                None => match selector.pick(&snapshots, &sel_ctx) {
                    Some(id) => id,
                    None => return Err(no_eligible()),
                },
            }
        }
        None => match selector.pick(&snapshots, &sel_ctx) {
            Some(id) => id,
            None => return Err(no_eligible()),
        },
    };

    let (account, _provider) = resolve_core_account(state, &picked, now).await?;
    Ok((account, picked))
}

/// D17's control-endpoint entry point. Control requests have no body ⇒ a header-only session key
/// (no content to derive a soft key from), and are never pool-scoped today (no `/{pool}/…` control
/// route exists) ⇒ `pool = None` — the same "select over ALL accounts" behavior the bare
/// `/responses` path uses when unowned. See [`resolve_owner_affine_account`] for the full
/// soft-affinity algorithm.
pub async fn resolve_control_account(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(Account, AccountId), Response> {
    // Control endpoints have no body ⇒ header-only session key, and are never pool-scoped.
    let session_key = header_session_key(headers, None);
    resolve_owner_affine_account(state, session_key.as_ref(), None).await
}

// -------------------------------------------------------------------------------------------
// D17 Task 3: the route handlers. Each is a thin `pub async fn` extractor wrapper (mirroring
// `crate::ingress::responses_handler`/`pooled_responses_handler`'s two-tier shape) that forwards
// straight into [`control_route`] — the shared glue: `resolve_control_account` (Task 2) →
// `polyflare_codex::control_forward` (Task 1) → build the client-facing `Response` from the
// filtered `ControlResponse` → write ONE content-free `request_log` row.
//
// The body is threaded through OPAQUE — `Option<Bytes>`, never parsed/re-serialized (the plan's
// "treat as generic forwards... do NOT parse the goal payload" instruction). It flows: axum's
// `Bytes` extractor → `control_route`'s `body` parameter → `polyflare_codex::control_forward`'s
// `body` parameter → the outbound `reqwest::RequestBuilder::body()` call — never touched, matched,
// or logged anywhere in between (see `control_forward.rs`'s own content-safety doc for the
// upstream half of this chain; `control_route` below is the downstream half: the ONLY thing it
// derives from the body is its byte length is never even read — `Bytes` is passed by value).
// -------------------------------------------------------------------------------------------

/// The response-header allow-set filtering is Task 1's job (`control_forward`'s
/// `ALLOWED_RESPONSE_HEADERS`) — this fn just re-materializes an axum `Response` from the already
/// -filtered `(status, headers, body)` triple, skipping any header whose name/value fails to
/// parse (defensive; Task 1's filtered set is expected to already be well-formed ASCII).
fn control_response_from(cr: polyflare_codex::ControlResponse) -> Response {
    let status = StatusCode::from_u16(cr.status).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut builder = Response::builder().status(status);
    for (name, value) in &cr.headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            builder = builder.header(name, value);
        }
    }
    builder
        .body(Body::from(cr.body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// Shared control-route glue: resolve the account (Task 2's soft affinity) → forward (Task 1's
/// unary primitive) → relay the response → persist ONE content-free `request_log` row, mirroring
/// `crate::ingress::responses_route`'s wrapper shape exactly (grab the log repo/bus, run the
/// logic, build+emit+persist the row, return the response).
///
/// `log_label` is the content-free `request_log` "kind" discriminator (`"codex_control_<path>"`,
/// per the plan's Global Constraints) written into the row's `path` field — the existing schema
/// has no separate `request_kind` column (see `polyflare_store::RequestLogRecord`), so `path` is
/// the field that already plays that role for `/responses`/`/v1/messages`'s own rows.
/// `forward_path` is the SEPARATE literal handed to `control_forward`/`control_url` (e.g.
/// `"thread/goal/set"`, no `"codex_control_"` prefix) — the actual upstream path segment.
///
/// A `resolve_control_account` failure (503, no eligible account) or a `control_forward` transport
/// failure (mapped to 502 here) both still write a content-free log row — mirroring
/// `responses_route`, which logs every outcome (including its own early-exit 503s) via
/// `responses_handler_impl`'s `RouteOutcome`, never only the success path.
async fn control_route(
    state: Arc<AppState>,
    log_label: &'static str,
    forward_path: &'static str,
    forward_method: Method,
    method_label: &'static str,
    headers: HeaderMap,
    body: Option<Bytes>,
) -> Response {
    let start = Instant::now();
    // Grab the log repo/bus BEFORE `&state` is borrowed further below — cheap `Arc`/pool clones,
    // matching `responses_route`'s "grab before the state borrow" ordering.
    let log_repo = state.store.request_log();
    let log_bus = state.log_bus.clone();

    // "Dumb executor, smart ingress": the SAME hop-by-hop drop-list `/responses` uses for its own
    // native forward-headers path (`crate::ingress::forward_headers_from_inbound`) — control
    // requests get identical treatment, not a second, independently-maintained filter.
    let forward_headers = forward_headers_from_inbound(&headers);

    let (response, account_id) = match resolve_control_account(&state, &headers).await {
        Err(resp) => (resp, None),
        Ok((account, account_id)) => {
            let outcome = polyflare_codex::control_forward(
                &state.control_client,
                &account,
                forward_path,
                forward_method,
                &forward_headers,
                body,
            )
            .await;
            let resp = match outcome {
                Ok(cr) => control_response_from(cr),
                Err(_e) => {
                    (StatusCode::BAD_GATEWAY, "control upstream forward failed").into_response()
                }
            };
            (resp, Some(account_id))
        }
    };

    let log = RequestLog {
        method: method_label,
        path: log_label,
        provider: Provider::Codex,
        aliased: false,
        status: response.status(),
        duration_ms: start.elapsed().as_millis() as u64,
        account_id: account_id.map(|id| id.to_string()),
        model: None,
        reasoning_effort: None,
        service_tier: None,
        transport: Some("http".to_string()),
        ttft_ms: None,
        total_tokens: None,
        cached_tokens: None,
        // Not derived for control endpoints (Task 3's `RequestCtx.subagent` wiring covers only
        // the `/responses`/`/v1/messages` set-sites); left `None` here.
        subagent: None,
    };
    log.emit();
    log_bus.publish(log.to_log_event());
    // C11b Task 2: the content-free `upstream_requests` counter, keyed by the SAME
    // `(account_id, status)` pair the log/log-bus/persisted row already carry — bumped exactly
    // once per client control request, mirroring `responses_route`/`messages_route`'s own bump.
    state
        .upstream_request_metrics
        .record(log.account_id.as_deref(), log.status.as_u16());
    spawn_persist_request_log(log_repo, log.record(unix_now()));

    response
}

/// `POST /thread/goal/set` — forwards the body verbatim; PolyFlare does not parse the goal
/// payload (unlike codex-lb's payload rebuild).
pub async fn thread_goal_set_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    control_route(
        state,
        "codex_control_thread/goal/set",
        "thread/goal/set",
        Method::POST,
        "POST",
        headers,
        Some(body),
    )
    .await
}

/// `POST /thread/goal/clear`.
pub async fn thread_goal_clear_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    control_route(
        state,
        "codex_control_thread/goal/clear",
        "thread/goal/clear",
        Method::POST,
        "POST",
        headers,
        Some(body),
    )
    .await
}

/// `GET /thread/goal/get` — no body.
pub async fn thread_goal_get_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    control_route(
        state,
        "codex_control_thread/goal/get",
        "thread/goal/get",
        Method::GET,
        "GET",
        headers,
        None,
    )
    .await
}

/// `GET /agent-identities/jwks`.
pub async fn jwks_handler(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    control_route(
        state,
        "codex_control_agent-identities/jwks",
        "agent-identities/jwks",
        Method::GET,
        "GET",
        headers,
        None,
    )
    .await
}

/// `GET /wham/agent-identities/jwks` — the `wham/`-prefixed variant, joined WITHOUT a `/codex/`
/// segment (see `polyflare_codex::control_url`).
pub async fn wham_jwks_handler(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    control_route(
        state,
        "codex_control_wham/agent-identities/jwks",
        "wham/agent-identities/jwks",
        Method::GET,
        "GET",
        headers,
        None,
    )
    .await
}

/// `POST /memories/trace_summarize` — forwards the body verbatim.
pub async fn trace_summarize_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    control_route(
        state,
        "codex_control_memories/trace_summarize",
        "memories/trace_summarize",
        Method::POST,
        "POST",
        headers,
        Some(body),
    )
    .await
}

// -------------------------------------------------------------------------------------------
// D14a Task 2 (final): `/responses/compact` — a UNARY passthrough the real Codex CLI emits
// (`codex-rs client.rs:159`) that PolyFlare previously 404'd. Unlike the D17 control endpoints
// above, compact carries a `/responses`-SHAPED body (it has its own `prompt_cache_key`/`model`),
// so its owner-affinity session key + content-free `model` are derived from that body via
// `crate::session_key::parse_inbound` — the SAME parse (and SAME Hard-key derivation)
// `/responses` itself uses — rather than the header-only key `resolve_control_account` derives.
// Still sidesteps the SSE relay / `ObservingStream` / continuity `prepare` entirely: this is a
// plain unary round-trip through `polyflare_codex::control_forward`, exactly like `control_route`.
// -------------------------------------------------------------------------------------------

/// D14a: the `/responses/compact` glue. UNARY, like `control_route`, but compact carries a
/// `/responses`-shaped BODY, so it derives the owner-affinity session key + the (content-free)
/// `model` from that body via `parse_inbound` — then forwards the SAME bytes verbatim to
/// `{base}/responses/compact` (`control_forward` with path `"responses/compact"`). Sidesteps the
/// SSE relay / `ObservingStream` / continuity entirely (unary round-trip).
async fn compact_route(
    state: Arc<AppState>,
    pool: Option<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let start = Instant::now();
    let log_repo = state.store.request_log();
    let log_bus = state.log_bus.clone();

    // Shallow parse: derive the session key (for soft owner affinity) + the content-free model.
    // None ⇒ malformed body ⇒ 400 (mirrors `/responses`'s malformed-body behavior); still log it.
    let facts = crate::session_key::parse_inbound(&headers, &body);
    let (session_key, model) = match &facts {
        Some(f) => (f.ctx.session_key.clone(), Some(f.model.clone())),
        None => (None, None),
    };

    let forward_headers = forward_headers_from_inbound(&headers);

    let (response, account_id) = if facts.is_none() {
        // Malformed compact body — do not forward garbage upstream.
        (
            (StatusCode::BAD_REQUEST, "malformed compact body").into_response(),
            None,
        )
    } else {
        match resolve_owner_affine_account(&state, session_key.as_ref(), pool.as_deref()).await {
            Err(resp) => (resp, None),
            Ok((account, account_id)) => {
                let outcome = polyflare_codex::control_forward(
                    &state.control_client,
                    &account,
                    "responses/compact",
                    Method::POST,
                    &forward_headers,
                    Some(body),
                )
                .await;
                let resp = match outcome {
                    Ok(cr) => control_response_from(cr),
                    Err(_e) => {
                        (StatusCode::BAD_GATEWAY, "compact upstream forward failed").into_response()
                    }
                };
                (resp, Some(account_id))
            }
        }
    };

    let log = RequestLog {
        method: "POST",
        path: "responses_compact",
        provider: Provider::Codex,
        aliased: false,
        status: response.status(),
        duration_ms: start.elapsed().as_millis() as u64,
        account_id: account_id.map(|id| id.to_string()),
        model,
        reasoning_effort: None,
        service_tier: None,
        transport: Some("http".to_string()),
        ttft_ms: None,
        total_tokens: None,
        cached_tokens: None,
        // Not derived for control endpoints — see the sibling `control_route` set-site's note.
        subagent: None,
    };
    log.emit();
    log_bus.publish(log.to_log_event());
    state
        .upstream_request_metrics
        .record(log.account_id.as_deref(), log.status.as_u16());
    spawn_persist_request_log(log_repo, log.record(unix_now()));

    response
}

/// `POST /responses/compact` — unpooled.
pub async fn compact_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    compact_route(state, None, headers, body).await
}

/// `POST /{pool}/responses/compact` — pool-scoped.
pub async fn pooled_compact_handler(
    State(state): State<Arc<AppState>>,
    Path(pool): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    compact_route(state, Some(pool), headers, body).await
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use axum::http::HeaderName;
    use polyflare_codex::oauth::OAuthClient;
    use polyflare_codex::CodexExecutor;
    use polyflare_core::{Continuity, RoundRobin, Selector};
    use polyflare_store::{Account as StoreAccount, PlainTokens, Store, TokenCipher};

    use crate::continuity::CodexContinuity;

    fn now() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    fn hdr(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        h
    }

    fn account(id: &str) -> StoreAccount {
        StoreAccount {
            id: id.to_string(),
            chatgpt_account_id: None,
            chatgpt_user_id: None,
            email: "u@example.test".to_string(),
            alias: None,
            workspace_id: None,
            workspace_label: None,
            seat_type: None,
            plan_type: "pro".to_string(),
            routing_policy: "normal".to_string(),
            last_refresh: now(),
            created_at: now(),
            status: "active".to_string(),
            deactivation_reason: None,
            reset_at: None,
            blocked_at: None,
            security_work_authorized: false,
            provider: "codex".to_string(),
            pool: None,
        }
    }

    async fn seed_account(store: &Store, cipher: &TokenCipher, id: &str, token: &str) {
        store
            .accounts()
            .insert(
                &account(id),
                &PlainTokens {
                    access_token: token.into(),
                    refresh_token: "r".into(),
                    id_token: "i".into(),
                },
                cipher,
            )
            .await
            .unwrap();
    }

    /// D14a Task 1: same as `seed_account`, but assigns the account to `pool` — needed to exercise
    /// `resolve_owner_affine_account`'s new `pool` parameter (`resolve_control_account`'s existing
    /// coverage above never varies `pool` away from `None`).
    async fn seed_pooled_account(
        store: &Store,
        cipher: &TokenCipher,
        id: &str,
        token: &str,
        pool: &str,
    ) {
        let mut acct = account(id);
        acct.pool = Some(pool.to_string());
        store
            .accounts()
            .insert(
                &acct,
                &PlainTokens {
                    access_token: token.into(),
                    refresh_token: "r".into(),
                    id_token: "i".into(),
                },
                cipher,
            )
            .await
            .unwrap();
    }

    /// Builds a full `AppState` for these tests, mirroring `tests/ownership.rs`'s construction
    /// pattern exactly. `Store` is NOT `Clone`, so (matching that existing pattern) callers reach
    /// the store/cipher back out via `state.store`/`state.cipher`, never a separately-held copy.
    async fn build_state(selector: Arc<dyn Selector>) -> Arc<AppState> {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("s.db")).await.unwrap();
        std::mem::forget(dir);
        let cipher = TokenCipher::from_key_bytes(&[9u8; 32]).unwrap();
        let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
            store.continuity(),
            Duration::from_secs(30),
        ));
        Arc::new(AppState {
            enforce_client_keys: false,
            codex_executor: Arc::new(CodexExecutor::new().unwrap()),
            control_client: polyflare_codex::build_client().expect("build control_client"),
            anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
            selector,
            pool_selectors: Default::default(),
            continuity,
            store,
            cipher,
            oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
            upstream_base_url: "http://127.0.0.1:9".to_string(),
            anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
            refresh_locks: Default::default(),
            capture_fingerprint_path: None,
            codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
            account_cache: Arc::new(crate::account_cache::AccountCache::new()),
            token_cache: Default::default(),
            admin_token: None,
            live_logs: false,
            log_bus: crate::log_bus::LogBus::new(1000),
            max_account_attempts: 3,
            failover_metrics: crate::observability::FailoverMetrics::new(),
            health_tier_metrics: crate::observability::HealthTierMetrics::new(),
            starvation_wait_budget: Duration::from_secs(60),
            starvation_heartbeat: Duration::from_secs(10),
            wake_jitter_ms: 0,
            starvation_metrics: crate::observability::StarvationMetrics::new(),
            stream_idle_timeout: Duration::from_secs(300),
            soft_drain_enabled: true,
            request_log_retention_days: 0,
            usage_history_retention_days: 0,
            runtime: Default::default(),
            inflight_penalty_pct: 2.5,
            lease_metrics: crate::observability::LeaseMetrics::new(),
            upstream_request_metrics: crate::observability::UpstreamRequestMetrics::new(),
            rate_limit_metrics: crate::observability::RateLimitMetrics::new(),
            model_catalog: crate::model_catalog::floor_only_model_catalog(),
        })
    }

    /// (a) A control request carrying a session header whose session has a KNOWN, ELIGIBLE owner
    /// resolves to that OWNER — even though the REAL `RoundRobin` selector, unpinned, would prefer
    /// a DIFFERENT account (both accounts are fresh/never-selected, so `RoundRobin`'s tiebreak
    /// falls to account id ascending, i.e. "A" — see `no_session_header_falls_back_to_normal_
    /// selection` below, which proves that fact directly). Owner is deliberately seeded as "B" —
    /// the NON-default pick — so this test cannot pass by coincidence.
    #[tokio::test]
    async fn session_header_with_eligible_owner_resolves_to_owner() {
        let state = build_state(Arc::new(RoundRobin)).await;
        seed_account(&state.store, &state.cipher, "A", "tokA").await;
        seed_account(&state.store, &state.cipher, "B", "tokB").await;
        // Seed the continuity session row (under the SAME key `header_session_key` derives for
        // this exact header) naming "B" as the owner.
        let now = now();
        let headers = hdr(&[("x-codex-turn-state", "ts-owned")]);
        let sk = header_session_key(&headers, None).unwrap();
        state
            .store
            .continuity()
            .ensure_session(&sk.value, "hard", now)
            .await
            .unwrap();
        state
            .store
            .continuity()
            .record_completion(&sk.value, "hard", "B", "resp_owned", "fp", 1, now)
            .await
            .unwrap();

        let (_account, picked) = resolve_control_account(&state, &headers).await.unwrap();
        assert_eq!(
            picked,
            AccountId::from("B"),
            "resolved to the session's owner (B), not RoundRobin's default tiebreak pick (A)"
        );
    }

    /// (b) A control request with NO session header resolves to a selected (any-eligible) account
    /// via normal selection — it must NOT error. Also establishes the baseline fact
    /// `session_header_with_eligible_owner_resolves_to_owner` depends on: `RoundRobin`, unpinned,
    /// over two fresh (never-selected) accounts, deterministically ties to the lexicographically
    /// smaller account id — "A".
    #[tokio::test]
    async fn no_session_header_falls_back_to_normal_selection() {
        let state = build_state(Arc::new(RoundRobin)).await;
        seed_account(&state.store, &state.cipher, "A", "tokA").await;
        seed_account(&state.store, &state.cipher, "B", "tokB").await;

        let headers = hdr(&[]);
        let (_account, picked) = resolve_control_account(&state, &headers)
            .await
            .expect("must not error when no session header is present");
        assert_eq!(
            picked,
            AccountId::from("A"),
            "RoundRobin's any-eligible tiebreak choice, proving normal selection ran"
        );
    }

    /// (c) A session header whose owner is INELIGIBLE (benched via a rate-limit cooldown — the
    /// SAME runtime API the request-failure path uses, not a test-only backdoor) falls back to
    /// ANOTHER eligible account — never stranded, and never the benched owner. This is the central
    /// inviolable: over-binding to an unavailable owner (à la `/responses`'s hard anchor recovery)
    /// would be the bug.
    #[tokio::test]
    async fn ineligible_owner_falls_back_to_another_eligible_account() {
        let state = build_state(Arc::new(RoundRobin)).await;
        seed_account(&state.store, &state.cipher, "A", "tokA").await;
        seed_account(&state.store, &state.cipher, "B", "tokB").await;

        let headers = hdr(&[("x-codex-turn-state", "ts-benched")]);
        let sk = header_session_key(&headers, None).unwrap();
        let now = now();
        state
            .store
            .continuity()
            .ensure_session(&sk.value, "hard", now)
            .await
            .unwrap();
        state
            .store
            .continuity()
            .record_completion(&sk.value, "hard", "B", "resp_benched", "fp", 1, now)
            .await
            .unwrap();
        // Bench the owner "B" (a real cooldown — `RuntimeStates::overlay` applies `cooldown_until`
        // onto the snapshot at selection time, and `select.rs`'s real eligibility gate rejects it
        // regardless of the account's durable `status`).
        state.runtime.record_rate_limit(
            &AccountId::from("B"),
            Some(3600),
            now,
            &state.rate_limit_metrics,
        );

        let (_account, picked) = resolve_control_account(&state, &headers)
            .await
            .expect("an ineligible owner must fall back, never 503 or hang");
        assert_ne!(
            picked,
            AccountId::from("B"),
            "must NOT return the benched owner"
        );
        assert_eq!(
            picked,
            AccountId::from("A"),
            "falls back to the other eligible account"
        );
    }

    /// (d) No eligible account at all (empty pool) ⇒ a clean 503, matching `/responses`'s
    /// `no_eligible()` — never a panic, never a hang.
    #[tokio::test]
    async fn no_eligible_account_at_all_yields_503() {
        let state = build_state(Arc::new(RoundRobin)).await;
        // No accounts seeded at all.
        let headers = hdr(&[]);
        let err = resolve_control_account(&state, &headers)
            .await
            .expect_err("empty pool must error, not panic");
        assert_eq!(err.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Regression: an owner recorded for a DIFFERENT session key must never leak into a request
    /// whose own session key resolves to no owner (a distinct, never-seen key, NOT the same as "no
    /// header at all" — proves the lookup is keyed correctly, not just "any session row exists").
    /// The seeded owner is "B" (the non-default pick) so a leak would be distinguishable from the
    /// genuine fallback result ("A").
    #[tokio::test]
    async fn unrelated_sessions_owner_never_leaks() {
        let state = build_state(Arc::new(RoundRobin)).await;
        seed_account(&state.store, &state.cipher, "A", "tokA").await;
        seed_account(&state.store, &state.cipher, "B", "tokB").await;

        let owned_headers = hdr(&[("x-codex-turn-state", "ts-other")]);
        let owned_key = header_session_key(&owned_headers, None).unwrap();
        let now = now();
        state
            .store
            .continuity()
            .ensure_session(&owned_key.value, "hard", now)
            .await
            .unwrap();
        state
            .store
            .continuity()
            .record_completion(&owned_key.value, "hard", "B", "resp_other", "fp", 1, now)
            .await
            .unwrap();

        // A DIFFERENT session header — its row was never created, so `get_session` returns `None`.
        let fresh_headers = hdr(&[("x-codex-turn-state", "ts-fresh-unseen")]);
        let (_account, picked) = resolve_control_account(&state, &fresh_headers)
            .await
            .unwrap();
        assert_eq!(
            picked,
            AccountId::from("A"),
            "an unrelated/unseen session key must fall back to normal selection, not B's ownership"
        );
    }

    // -----------------------------------------------------------------------------------------
    // D14a Task 1: `resolve_owner_affine_account` unit tests. `resolve_control_account`'s coverage
    // above already proves the soft-affinity algorithm itself (owner hit / ineligible-owner
    // fallback / no-header fallback / cross-session leak / empty-pool 503) end-to-end through the
    // header-only, pool-less wrapper. These tests instead cover the TWO axes the extraction adds
    // that the wrapper never exercises: a session key that did NOT come from
    // `header_session_key` (simulating Task 2's body-derived key), and a non-`None` `pool` that
    // narrows both the owner-eligibility check and the fallback candidate set.
    // -----------------------------------------------------------------------------------------

    use polyflare_core::{KeyStrength, SessionKey};

    /// A body-derived-style session key (never touches `header_session_key`) whose owner sits in
    /// pool "p" resolves to that owner when `pool = Some("p")` — proving the extracted core
    /// honors an arbitrary `session_key` input, not just a header-derived one.
    #[tokio::test]
    async fn owner_affine_core_resolves_pooled_owner_via_non_header_key() {
        let state = build_state(Arc::new(RoundRobin)).await;
        seed_pooled_account(&state.store, &state.cipher, "P", "tokP", "p").await;
        seed_account(&state.store, &state.cipher, "U", "tokU").await;

        let key = SessionKey {
            value: "body-derived-session-key-abc".to_string(),
            strength: KeyStrength::Soft,
        };
        let now = now();
        state
            .store
            .continuity()
            .ensure_session(&key.value, "soft", now)
            .await
            .unwrap();
        state
            .store
            .continuity()
            .record_completion(&key.value, "soft", "P", "resp_p", "fp", 1, now)
            .await
            .unwrap();

        let (_account, picked) = resolve_owner_affine_account(&state, Some(&key), Some("p"))
            .await
            .unwrap();
        assert_eq!(
            picked,
            AccountId::from("P"),
            "a non-header session key must still resolve to its owner when the owner is in-pool"
        );
    }

    /// With no owner on record, the `pool` parameter still scopes the INVIOLABLE fallback: an
    /// account outside the requested pool (the unpooled decoy) must never be picked when `pool =
    /// Some("p")`, even though it would be a perfectly eligible candidate under `pool = None`.
    ///
    /// The seeded ids are deliberately chosen so this test has TEETH against a dropped `pool`
    /// argument: `RoundRobin`'s full-tie tiebreak is pure ID-ascending-alphabetical
    /// (`polyflare_core::select::deterministic_by`). If the pooled account's id sorted BEFORE the
    /// unpooled decoy's, a regression that silently widened the candidate set to `None` (i.e. lost
    /// the `pool` scoping) would still coincidentally pick the pooled account — the assertion would
    /// pass for the wrong reason and the test would prove nothing. Seeding the pooled account as
    /// `"z-pooled"` (sorts AFTER the decoy `"a-unpooled"`) makes the two behaviors diverge:
    /// - correct (pool honored): only `"z-pooled"` is a candidate ⇒ picked == `"z-pooled"`.
    /// - regressed (pool dropped to `None`): both are candidates ⇒ RoundRobin's ascending tiebreak
    ///   picks `"a-unpooled"` first ⇒ the assertion below FAILS.
    #[tokio::test]
    async fn owner_affine_core_fallback_is_scoped_to_the_given_pool() {
        let state = build_state(Arc::new(RoundRobin)).await;
        seed_pooled_account(&state.store, &state.cipher, "z-pooled", "tokP", "p").await;
        seed_account(&state.store, &state.cipher, "a-unpooled", "tokU").await;

        let (_account, picked) = resolve_owner_affine_account(&state, None, Some("p"))
            .await
            .expect("pool-scoped fallback must not error when the pool has an eligible account");
        assert_eq!(
            picked,
            AccountId::from("z-pooled"),
            "fallback must stay within the requested pool, never picking the unpooled decoy \
             (would fail if a dropped `pool` arg let the alphabetically-first decoy win the tiebreak)"
        );
    }

    /// `resolve_owner_affine_account(None, None)` — no affinity signal, no pool scope — behaves
    /// exactly like `resolve_control_account`'s own any-eligible fallback: it must not error, and
    /// must return one of the genuinely eligible (in this case, either) accounts.
    #[tokio::test]
    async fn owner_affine_core_with_no_key_and_no_pool_picks_any_eligible() {
        let state = build_state(Arc::new(RoundRobin)).await;
        seed_pooled_account(&state.store, &state.cipher, "P", "tokP", "p").await;
        seed_account(&state.store, &state.cipher, "U", "tokU").await;

        let (_account, picked) = resolve_owner_affine_account(&state, None, None)
            .await
            .expect("must not error when unowned and unpooled, mirroring the wrapper's fallback");
        assert!(
            picked == AccountId::from("P") || picked == AccountId::from("U"),
            "must pick one of the genuinely eligible accounts, got {picked:?}"
        );
    }
}
