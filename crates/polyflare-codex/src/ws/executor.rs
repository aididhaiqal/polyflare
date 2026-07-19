//! `CodexWsExecutor` — the WS `Executor` impl (M5a Task 7): the per-conversation connection
//! cache, the bounded recovery paths, and the error mapping that make M5a's transport swap real.
//!
//! **This module wires together, but does not re-derive, the semantics of every earlier M5a
//! task**: `ws::conn::{WsConn, connect_detailed}` (Task 3) for the handshake, `ws::codec` (Task 4)
//! for frame classification (consumed indirectly, through `ws::turn`'s already-classified `Err`
//! shapes — see the "Recovery: how classification reaches this module" section below),
//! `ws::delta::plan_request_for_conn` (Task 6) for the incremental-vs-full decision, and
//! `ws::turn::turn_stream` (Task 5, extended by this task — see that module's `turn_stream` doc
//! for the exact point the delta-tracking hashes are now set) for driving one turn.
//!
//! # "Dumb executor, smart ingress" — same division of labor as `executor.rs` and `ws::conn`
//! This executor does not synthesize codex-identity headers itself; it relays whatever
//! `req.forward_headers` ingress already decided (see `executor.rs`'s module doc and
//! `ws::conn::WsConn::connect`'s own doc for the one WS-specific exception,
//! `x-codex-turn-state`). Nothing here changes that.
//!
//! # The connection cache
//! Keyed by `conn_key`, which LEADS with the owner `account.id`, then `ctx.session_key`'s hashed
//! `value` folded with the request's `ws::delta::non_input_fingerprint`, and — when present — the
//! hashed `ctx.conn_discriminator` (the thread-unique `x-codex-window-id`):
//! `"<account_id>:<session>:<non-input-hash>:<window-id-hash>"`. The last three components are
//! content-free sha256 hex digests — `SessionKey::value` is computed by
//! `polyflare-server::session_key`, the fingerprint hashes only non-input request fields (model,
//! instructions, tools, …), and the discriminator is a bounded thread id (`{thread_id}:0`), hashed
//! via `ws::delta::sha256_hex` — so the key never contains the raw request body; the leading
//! `account.id` is a non-secret internal id (the same one stored in `RequestLog.account_id`), never
//! a token. **The `account.id` MUST lead the key because a chatgpt.com WS socket is authenticated
//! per-account at the HANDSHAKE (Bearer + ChatGPT-Account-ID), and reuse is gated on liveness
//! (`is_closed()`) alone — never on the account.** So a DIFFERENT account MUST resolve a DIFFERENT
//! key and get its OWN socket: on a failover the session/conversation/body/window-id are all
//! identical (owner account A -> account B), and without `account.id` in the key the two would be
//! byte-identical, so B's turn would be driven over A's still-live, A-authenticated socket and be
//! served/billed on A — the very account the failover exists to avoid. Leading with `account.id`
//! makes a failover UNABLE to reuse the prior account's connection. Folding in the non-input
//! fingerprint gives each interleaved model-stream on one conversation (codex drives two models per
//! turn) its OWN socket + anchor chain: without it, the two streams shared one socket and clobbered
//! each other's stored fingerprint, forcing every turn to a full (uncached) send. Folding in the
//! window-id additionally isolates each codex THREAD (main / review / compact / spawn) even when
//! its `session_key` AND `non_input_fingerprint` coincide with another thread's — the one gap the
//! session+fingerprint key leaves, since the `x-codex-turn-state` `session_key` branch drops
//! `prompt_cache_key`; each thread then gets its own socket regardless of which `session_key`
//! branch fired. When `ctx.conn_discriminator` is `None` the window-id component is omitted
//! entirely, so `conn_key` is BYTE-IDENTICAL to the `"<account_id>:<session>:<hash>"` form — a
//! deliberate back-compat constraint that preserves the per-model-stream caching above exactly. A
//! stable account keeps a stable key, so same-account reuse (hence the just-merged incremental
//! caching) is untouched; only an account CHANGE yields a new key. This folding is transport-only
//! and ownership-blind: it never touches continuity/wedge/`delta.rs` logic. The 426 WS-disable and all logging stay keyed on the plain `session_key` (a 426 disables
//! WS for the whole session, every model-stream and thread). A request with no session key
//! (`ctx.session_key: None`) gets `conn_key: None` — a fresh, uncached connection every call: no reuse is possible
//! without something to key on, but the request still completes correctly (as a full send with no
//! anchor) — WS just delivers no benefit for it, which is a degraded-but-correct default, not a
//! failure.
//!
//! Eviction: ground truth §2 gates reuse "only on liveness (`conn.is_closed()`)" — never on a
//! generic staleness timer. [`WsConn::is_closed`] (Task 7's own small addition to `conn.rs`, set
//! at the two points a socket is proven dead: a `Close`/end-of-stream observed by `recv_frame`, or
//! a `send_frame` error) is checked BEFORE reuse; a dead cached entry is evicted and replaced by a
//! fresh `connect`, never reactively discovered by attempting to reuse it and handling the failure
//! after the fact. A connection is also evicted (and replaced) as part of the bounded reconnect
//! recovery path below.
//!
//! # Recovery: how classification reaches this module
//! `ws::turn::turn_stream` (Task 5) already classifies every received frame via `ws::codec`, but
//! its `ResponseStream` interface can only report a WS-specific condition as a plain
//! `ExecError::Stream(String)` (the same `Result<Bytes, ExecError>` shape every other executor's
//! stream produces — `ResponseStream` cannot carry a richer type without breaking that contract).
//! So the two turn-stream conditions this module recovers from — `AnchorMiss` and
//! `ConnectionLimitReached` (plus the generic "closed before any terminal frame" condition) — are
//! carried as documented, constant marker substrings inside that message
//! (`ws::turn::{ANCHOR_MISS_MARKER, CONNECTION_LIMIT_MARKER, SOCKET_CLOSED_MARKER}`), produced in
//! exactly one place (`ws::turn`) and consumed in exactly one place ([`classify_recovery`] below) —
//! a deliberate, narrowly-scoped choice over widening `ExecError`/`FailureSignal` with a `code`
//! field for a need that is WS-only and would otherwise ripple into every other `Executor` impl
//! and caller in the workspace.
//!
//! **Recovery only ever inspects the FIRST item of a turn's stream.** Once a turn has yielded one
//! successful byte to the caller, every later item — including a LATER anchor-miss/connection-limit/
//! close condition on the SAME turn — is passed through completely unchanged (`ExecError::Stream`,
//! as `ws::turn` already produces it): SPEC-M5-WEBSOCKET.md §4's last row, "mid-stream failure
//! after first byte", is unconditional and un-retried. This is also why `execute` itself resolves
//! the FIRST item before returning: exactly like `CodexExecutor::execute` awaiting the HTTP
//! response's status line before returning its stream (`executor.rs`), this module awaits the
//! first WS frame back before returning ITS stream — both are "resolve the pre-flight outcome
//! synchronously; stream only the body" the same way.
//!
//! # Fallback scope — a DELIBERATE divergence from codex (document here, not just in the plan)
//! Ground truth §5: real codex-rs flips a single `disable_websockets: AtomicBool` for the whole
//! process, one-way, no reset path. That is wrong for PolyFlare: a long-lived, multi-tenant server
//! serving many accounts and sessions from one process must not let one account's rejected
//! handshake silently and permanently disable WS for every OTHER account/session too. So fallback
//! here is scoped in two independent dimensions instead of codex's one process-wide switch:
//!
//! - **Per session**, triggered ONLY by a 426 (ground truth §5's one `FallbackToHttp` trigger):
//!   [`CodexWsExecutor::disable_session`] marks THIS session's `SessionKey` so every future
//!   `execute` call for it skips WS entirely and goes straight to the injected HTTP-SSE fallback —
//!   permanently for that session's in-memory lifetime (a 426 means the account/session
//!   combination doesn't get WS at all; there is no reason to keep re-trying it).
//! - **Per account, bounded by a cooldown**, triggered by the SAME 426 event:
//!   [`CodexWsExecutor::start_account_cooldown`] additionally blocks WS for every OTHER session on
//!   that same account for a bounded window (`account_cooldown`), so a single bad account doesn't
//!   get hammered with repeated doomed handshake attempts from many concurrent sessions before its
//!   first 426 has even been recorded against it. Unlike the session-level mark, this expires.
//!
//! **A future reader must not "fix" this back to codex's one-way global switch** — that would
//! reintroduce exactly the blast radius (one account's rejection taking down WS for every tenant)
//! this scoping exists to avoid. If codex's upstream behavior ever changes, re-verify against
//! ground truth before touching this.
//!
//! **Generic (non-426) transport/handshake failures do NOT fall back at all** — ground truth §5 is
//! explicit that 426 is the ONLY trigger ("No other status falls back"); a DNS failure, a refused
//! connection, or any other handshake error surfaces as `ExecError::Upstream`, exactly as
//! `CodexExecutor` would surface an equivalent HTTP failure today. This keeps M5a's promise of
//! "changes nothing above the seam" — no new retry/failover machinery, per the plan's Global
//! Constraints.
//!
//! # Content-safety
//! `body` (the materialized request) and `envelope` (the built `response.create`) carry
//! conversation content, same as everywhere else in this crate — never logged, never included in
//! `Debug`. The wedge-visibility logging this module does emit ([`log_wedge_recovery`],
//! [`log_fallback`]) is deliberately reason-code-and-counts only: a session key (already a
//! content-free hash) and an account id, never a body, envelope, or frame. This is precisely the
//! visibility codex-lb lacked — it wedged on ~31% of reattaches with no way to measure it; every
//! bounded-recovery attempt here is a `tracing::warn!` a dashboard can count.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::Value;

use polyflare_core::{Account, ExecError, Executor, PreparedRequest, RequestCtx, ResponseStream};

use super::codec::build_response_create;
use super::conn::{connect_detailed, ConnectOutcome, WsConn};
use super::delta::{plan_request_for_conn, RequestPlan};
use super::turn::{
    shared_conn, turn_stream_with_guard, SharedWsConn, ANCHOR_MISS_MARKER, CONNECTION_LIMIT_MARKER,
    SOCKET_CLOSED_MARKER,
};

/// Bounded attempts for the same-socket anchor-miss recovery (strip anchor, full resend). Chosen
/// small and fixed rather than configurable in production: a SECOND consecutive anchor-miss on the
/// very same socket, moments after stripping the anchor and resending fresh, indicates something
/// more persistently wrong than a one-off dead anchor — better to surface than to keep re-billing
/// full history silently.
const DEFAULT_MAX_ANCHOR_MISS_RETRIES: u32 = 2;
/// Bounded attempts for the reconnect-and-resend recovery (connection-limit / closed-before-first-byte).
const DEFAULT_MAX_RECONNECT_RETRIES: u32 = 2;
/// How long a 426 keeps WS disabled for OTHER sessions on the SAME account (the per-account
/// dimension of the fallback-scope divergence documented in the module doc above).
const DEFAULT_ACCOUNT_COOLDOWN: Duration = Duration::from_secs(30);

/// The WS `Executor` impl. See the module doc for the cache key, eviction rule, recovery bounds,
/// and the fallback-scope divergence from codex.
pub struct CodexWsExecutor {
    /// The per-session connection cache. `std::sync::Mutex` (not `tokio::sync::Mutex`): only ever
    /// held for a plain map operation (get/insert/remove), never across an `.await` point.
    conns: StdMutex<HashMap<String, SharedWsConn>>,
    /// Sessions permanently (for this process's lifetime) routed to `fallback` after a 426 —
    /// see the module doc's "Fallback scope" section.
    session_ws_disabled: StdMutex<HashSet<String>>,
    /// Accounts temporarily routed to `fallback` after a 426, until the paired `Instant` — the
    /// per-account dimension of the same fallback-scope decision.
    account_cooldown_until: StdMutex<HashMap<String, Instant>>,
    account_cooldown: Duration,
    max_anchor_miss_retries: u32,
    max_reconnect_retries: u32,
    /// HTTP-SSE fallback, used ONLY for the 426 case (and its per-session/per-account cooldown
    /// echoes) — never for a generic transport failure, which surfaces unchanged (module doc).
    fallback: Arc<dyn Executor>,
    /// How many times this executor has actually attempted a NEW WS handshake (cache hits don't
    /// count). `pub(crate)`-visible only for this module's own tests, to prove the session/account
    /// fallback scoping actually skips the WS attempt rather than merely failing it again.
    ws_connect_attempts: AtomicUsize,
}

impl CodexWsExecutor {
    /// Construct with production defaults (`DEFAULT_MAX_ANCHOR_MISS_RETRIES`,
    /// `DEFAULT_MAX_RECONNECT_RETRIES`, `DEFAULT_ACCOUNT_COOLDOWN`). `fallback` is the HTTP-SSE
    /// executor to use on a 426 (Task 8 wires in `Arc::new(CodexExecutor::new()?)`; this module
    /// takes any `Arc<dyn Executor>` so its own tests can inject a lightweight stand-in instead of
    /// standing up a real HTTP mock for a scenario this module never actually sends bytes over
    /// HTTP for — only delegates to).
    pub fn new(fallback: Arc<dyn Executor>) -> Self {
        Self::with_config(
            fallback,
            DEFAULT_MAX_ANCHOR_MISS_RETRIES,
            DEFAULT_MAX_RECONNECT_RETRIES,
            DEFAULT_ACCOUNT_COOLDOWN,
        )
    }

    /// Construct with explicit bounds — used directly by this module's own tests (a 30s production
    /// cooldown and a real network round-trip make poor test material) and available to a future
    /// caller that wants non-default bounds.
    pub fn with_config(
        fallback: Arc<dyn Executor>,
        max_anchor_miss_retries: u32,
        max_reconnect_retries: u32,
        account_cooldown: Duration,
    ) -> Self {
        Self {
            conns: StdMutex::new(HashMap::new()),
            session_ws_disabled: StdMutex::new(HashSet::new()),
            account_cooldown_until: StdMutex::new(HashMap::new()),
            account_cooldown,
            max_anchor_miss_retries,
            max_reconnect_retries,
            fallback,
            ws_connect_attempts: AtomicUsize::new(0),
        }
    }

    /// Test-only introspection: how many NEW WS handshakes this executor has attempted (cache hits
    /// excluded). See the field's own doc.
    #[cfg(test)]
    pub(crate) fn ws_connect_attempts(&self) -> usize {
        self.ws_connect_attempts.load(Ordering::SeqCst)
    }

    fn is_session_ws_disabled(&self, session_key: &str) -> bool {
        self.session_ws_disabled
            .lock()
            .unwrap()
            .contains(session_key)
    }

    fn disable_session(&self, session_key: &str) {
        self.session_ws_disabled
            .lock()
            .unwrap()
            .insert(session_key.to_string());
    }

    fn is_account_in_cooldown(&self, account_id: &str) -> bool {
        match self.account_cooldown_until.lock().unwrap().get(account_id) {
            Some(until) => Instant::now() < *until,
            None => false,
        }
    }

    fn start_account_cooldown(&self, account_id: &str) {
        let until = Instant::now() + self.account_cooldown;
        self.account_cooldown_until
            .lock()
            .unwrap()
            .insert(account_id.to_string(), until);
    }

    fn evict(&self, conn_key: Option<&str>) {
        if let Some(key) = conn_key {
            self.conns.lock().unwrap().remove(key);
        }
    }

    /// Get a live cached connection for `conn_key`, or dial a fresh one — never reusing a
    /// connection [`WsConn::is_closed`] reports dead (module doc's eviction rule).
    ///
    /// `conn_key` is purely the connection-cache key (the owner `account.id` leading `session_key`
    /// folded with the request's `non_input_fingerprint` — see where it's computed in this impl's
    /// `execute` for why): this function has no account-vs-session-vs-model semantics of its own, it
    /// just get/insert/removes `self.conns` by whatever key it's handed. Because `account.id` leads
    /// the key, a failover to a different account can never resolve — and so can never reuse — the
    /// prior account's cached socket here.
    async fn connect_and_cache(
        &self,
        account: &Account,
        forward_headers: &[(String, String)],
        conn_key: Option<&str>,
    ) -> ConnAttempt {
        if let Some(key) = conn_key {
            let cached = self.conns.lock().unwrap().get(key).cloned();
            if let Some(shared) = cached {
                let dead = shared.lock().await.is_closed();
                if !dead {
                    return ConnAttempt::Ready(shared);
                }
                self.conns.lock().unwrap().remove(key);
            }
        }

        self.ws_connect_attempts.fetch_add(1, Ordering::SeqCst);
        match connect_detailed(account, forward_headers).await {
            ConnectOutcome::Connected(fresh) => {
                let shared = shared_conn(*fresh);
                if let Some(key) = conn_key {
                    self.conns
                        .lock()
                        .unwrap()
                        .insert(key.to_string(), shared.clone());
                }
                ConnAttempt::Ready(shared)
            }
            ConnectOutcome::UpgradeRequired => ConnAttempt::UpgradeRequired,
            ConnectOutcome::Failed(e) => ConnAttempt::Failed(e),
        }
    }

    /// Plan (Task 6) and build (Task 4) the envelope for the next attempt, reading ALL prior-turn
    /// state off `conn` itself (content-free — see `delta.rs`'s module doc).
    ///
    /// **Concurrency (M5a Task 8): takes `&WsConn`, not `&SharedWsConn` — deliberately.** The
    /// caller ([`Self::drive_turn`]) must hold `conn`'s lock (an already-acquired
    /// [`tokio::sync::OwnedMutexGuard`]) across this call AND the send that follows it, in the
    /// SAME critical section — see `ws::turn::turn_stream_with_guard`'s doc for the exact race
    /// this closes (a second concurrent turn on the same session key planning against state a
    /// first turn is about to advance, then sending a now-stale envelope). This function no
    /// longer locks anything itself; it is plain, synchronous, content-free planning over a
    /// reference the caller already holds exclusively.
    fn plan_and_build_locked(conn: &WsConn, body: &Value) -> Value {
        match plan_request_for_conn(conn, body) {
            RequestPlan::Incremental { anchor, suffix } => {
                build_response_create(body, Some(&anchor), &suffix, None)
            }
            RequestPlan::Full => build_response_create(body, None, &full_input(body), None),
        }
    }

    /// Drive one turn to completion, applying the two bounded recovery paths (module doc) to the
    /// FIRST item only. Returns a `ResponseStream` whose first item is already known-good (or the
    /// turn has already exhausted its recovery budget and this returns `Err` instead) — mirrors
    /// `CodexExecutor::execute` resolving the HTTP status line before returning its stream.
    ///
    /// **Concurrency: plan+send is now atomic per connection (M5a Task 8).** Each loop iteration
    /// acquires `shared`'s lock ONCE (`shared.clone().lock_owned()`) and holds it across BOTH
    /// planning (`plan_and_build_locked`, a synchronous read of the guard) and the send
    /// (`turn_stream_with_guard`, which holds the SAME guard through the whole read loop until the
    /// turn ends). A second concurrent `drive_turn` call on the SAME session key — hence the same
    /// cached `SharedWsConn` — blocks at its own `lock_owned()` until this one's guard is dropped
    /// (at `Done`), so it always plans against this turn's POST-send state, never a stale
    /// snapshot. A turn on a DIFFERENT session key uses a different `SharedWsConn` (a different
    /// `Mutex`) and is never blocked by this one. See `ws::turn::turn_stream_with_guard`'s doc for
    /// the full race this closes.
    async fn drive_turn(
        &self,
        account: &Account,
        forward_headers: &[(String, String)],
        conn_key: Option<&str>,
        session_key: Option<&str>,
        body: &Value,
        mut shared: SharedWsConn,
    ) -> Result<ResponseStream, ExecError> {
        let mut anchor_attempts: u32 = 0;
        let mut reconnect_attempts: u32 = 0;

        loop {
            let guard = shared.clone().lock_owned().await;
            let envelope = Self::plan_and_build_locked(&guard, body);
            let mut stream = turn_stream_with_guard(guard, envelope);

            match stream.next().await {
                None => {
                    // Unreachable in practice: `ws::turn::turn_stream`'s state machine always
                    // yields at least one item (a frame, or the send/first-read error) before ever
                    // reaching `Done` — see that module's `poll_next`. Treated as an error rather
                    // than silently returning an empty stream, so a future change to that
                    // invariant fails loudly here instead of masking a real bug.
                    return Err(ExecError::Stream(
                        "WS turn produced no frames at all (unexpected end of stream before the \
                         first item)"
                            .to_string(),
                    ));
                }
                Some(Ok(first)) => {
                    let rest = stream;
                    let combined =
                        futures_util::stream::once(async move { Ok::<_, ExecError>(first) })
                            .chain(rest);
                    return Ok(Box::pin(combined));
                }
                Some(Err(e)) => match classify_recovery(&e) {
                    RecoveryAction::StripAnchorAndResend
                        if anchor_attempts < self.max_anchor_miss_retries =>
                    {
                        anchor_attempts += 1;
                        log_wedge_recovery(
                            "anchor_miss_full_resend",
                            &account.id,
                            session_key,
                            anchor_attempts,
                        );
                        // Strip the (now-dead) anchor state so `plan_request_for_conn` computes
                        // `Full` on the next loop iteration — never re-derive/guess, just clear
                        // exactly what a fresh connection would also have unset.
                        let mut guard = shared.lock().await;
                        guard.last_response_id = None;
                        guard.last_item_hashes = None;
                        guard.last_non_input_fingerprint = None;
                        drop(guard);
                        continue;
                    }
                    RecoveryAction::Reconnect
                        if reconnect_attempts < self.max_reconnect_retries =>
                    {
                        reconnect_attempts += 1;
                        log_wedge_recovery(
                            "reconnect_full_resend",
                            &account.id,
                            session_key,
                            reconnect_attempts,
                        );
                        self.evict(conn_key);
                        match self
                            .connect_and_cache(account, forward_headers, conn_key)
                            .await
                        {
                            ConnAttempt::Ready(fresh) => {
                                shared = fresh;
                                continue;
                            }
                            // A 426 discovered mid-recovery (not the common initial-connect case,
                            // which `execute` handles with the real fallback dispatch): update the
                            // session/account state for FUTURE calls, but this in-flight turn has
                            // no `PreparedRequest`/`RequestCtx` to hand to `fallback` from this
                            // deep in the loop — surface the error that triggered the reconnect
                            // instead of the 426 itself, matching "handshake/transport failure
                            // surfaces unchanged" for everything that isn't the top-level 426 path.
                            ConnAttempt::UpgradeRequired => {
                                if let Some(key) = session_key {
                                    self.disable_session(key);
                                }
                                self.start_account_cooldown(&account.id);
                                log_fallback(
                                    "handshake_426_during_reconnect",
                                    &account.id,
                                    session_key,
                                );
                                return Err(e);
                            }
                            ConnAttempt::Failed(conn_err) => return Err(conn_err),
                        }
                    }
                    _ => return Err(e),
                },
            }
        }
    }
}

/// Outcome of one connect attempt from [`CodexWsExecutor::connect_and_cache`].
enum ConnAttempt {
    Ready(SharedWsConn),
    UpgradeRequired,
    Failed(ExecError),
}

/// What to do about the FIRST item of a turn's stream being an `Err`. See the module doc's
/// "Recovery: how classification reaches this module" section for why this string-matches
/// `ExecError::Stream` messages rather than reading a richer type.
enum RecoveryAction {
    StripAnchorAndResend,
    Reconnect,
    /// Not recoverable here: surface as-is. Covers `ExecError::UpstreamStatus` (429, a genuine 400,
    /// ...) unconditionally — SPEC-M5 §4's "key off STATUS, not the code string" — and any
    /// `ExecError::Stream`/`Upstream` that doesn't carry one of the known recoverable markers.
    Surface,
}

fn classify_recovery(e: &ExecError) -> RecoveryAction {
    if let ExecError::Stream(msg) = e {
        if msg.contains(ANCHOR_MISS_MARKER) {
            return RecoveryAction::StripAnchorAndResend;
        }
        if msg.contains(CONNECTION_LIMIT_MARKER) || msg.contains(SOCKET_CLOSED_MARKER) {
            return RecoveryAction::Reconnect;
        }
    }
    RecoveryAction::Surface
}

/// `body["input"]` as an owned `Vec`, or empty if absent/not an array — the FULL history to send
/// on a `RequestPlan::Full` decision. Mirrors `delta.rs`'s private `input_items` (not exposed from
/// that module, so reimplemented here rather than widening its visibility for one caller).
fn full_input(body: &Value) -> Vec<Value> {
    body.get("input")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

/// Materialize `req`'s body as a `Value` for WS planning/sending. **This is NOT the session-key
/// re-parse the plan's Global Constraints forbid** — that constraint is specifically about never
/// re-deriving the session key by re-parsing the body (Task 1 threads `ctx.session_key` down
/// exactly to avoid that). This parse is different and unavoidable: the WS path must inspect and
/// partially rebuild `input` (the delta decision, Task 6) and cannot do that from raw bytes. A
/// native pass-through's `raw_body` is parsed here once; a translated/already-materialized request
/// (`req.body: Some(_)`) is simply cloned.
fn materialize_body(req: &PreparedRequest) -> Result<Value, ExecError> {
    if let Some(body) = &req.body {
        return Ok(body.clone());
    }
    match &req.raw_body {
        Some(raw) => serde_json::from_slice(raw).map_err(|e| {
            ExecError::Upstream(format!(
                "WS executor requires a JSON request body to plan/send a delta: {e}"
            ))
        }),
        None => Err(ExecError::Upstream(
            "WS executor: PreparedRequest has neither body nor raw_body".to_string(),
        )),
    }
}

/// Content-free wedge-visibility logging for a bounded recovery attempt: reason code + counts
/// only, per the module doc's content-safety section. Never a body, envelope, or frame.
fn log_wedge_recovery(reason: &str, account_id: &str, session_key: Option<&str>, attempt: u32) {
    tracing::warn!(
        reason,
        account_id,
        session_key = session_key.unwrap_or("<none>"),
        attempt,
        "ws executor recovering a turn (content-free: reason code + counts only — this is the \
         wedge-rate visibility codex-lb lacked, which wedged ~31% of reattaches with no way to \
         measure it)"
    );
}

/// Content-free fallback-dispatch logging (the 426 / session-disabled / account-cooldown paths).
fn log_fallback(reason: &str, account_id: &str, session_key: Option<&str>) {
    tracing::warn!(
        reason,
        account_id,
        session_key = session_key.unwrap_or("<none>"),
        "ws executor falling back to HTTP-SSE (content-free)"
    );
}

#[async_trait]
impl Executor for CodexWsExecutor {
    async fn execute(
        &self,
        req: PreparedRequest,
        account: &Account,
        ctx: &RequestCtx,
    ) -> Result<ResponseStream, ExecError> {
        let session_key = ctx.session_key.as_ref().map(|k| k.value.clone());

        if let Some(key) = &session_key {
            if self.is_session_ws_disabled(key) {
                log_fallback("session_ws_disabled", &account.id, Some(key));
                return self.fallback.execute(req, account, ctx).await;
            }
        }
        if self.is_account_in_cooldown(&account.id) {
            log_fallback("account_cooldown", &account.id, session_key.as_deref());
            return self.fallback.execute(req, account, ctx).await;
        }

        let body = materialize_body(&req)?;

        // Per-account, per-thread, per-model-stream connection key. `account.id` LEADS the key:
        // a chatgpt.com WS socket is authenticated per-account at the handshake (Bearer +
        // ChatGPT-Account-ID) and reuse is gated on liveness alone, so a different account MUST get
        // a different key -> its own socket. On failover (owner A -> B) session/body/window-id are
        // identical; without account.id the key would be byte-identical and B's turn would ride A's
        // still-live A-authenticated socket -> served/billed on A. Then: codex interleaves multiple
        // models (e.g. gpt-5.6-luna + gpt-5.6-sol) on ONE conversation, and drives several THREADS
        // (main / review / compact / spawn) — each with a distinct `x-codex-window-id`. Keying the
        // socket cache on session_key alone made model-streams share a socket and clobber each
        // other's anchor/non-input fingerprint, forcing plan_request to Full every turn (0% cache).
        // Folding the non-input fingerprint in gives each model-stream its OWN socket + clean
        // strict-extension chain -> Incremental -> the backend caches. Folding the hashed
        // `conn_discriminator` (the `x-codex-window-id`) in additionally isolates each THREAD even
        // when session_key AND non_input_fingerprint coincide (the gap the `x-codex-turn-state`
        // session_key branch leaves by dropping `prompt_cache_key`). Back-compat: a stable account
        // keeps a stable key, and when `conn_discriminator` is None the window-id component is
        // omitted, so same-account reuse and the just-merged per-model-stream caching are unchanged;
        // only an account CHANGE yields a new key. Content-free: session/fingerprint/disc are
        // sha256 hex digests (`disc` hashed, never used raw); `account.id` is a non-secret internal
        // id (already in RequestLog.account_id), never a token — and conn_key is never logged.
        let conn_key = session_key.as_ref().map(|sk| {
            let base = format!(
                "{}:{sk}:{}",
                account.id,
                crate::ws::delta::non_input_fingerprint(&body)
            );
            match ctx.conn_discriminator.as_deref() {
                Some(disc) => format!("{base}:{}", crate::ws::delta::sha256_hex(disc.as_bytes())),
                None => base,
            }
        });

        let shared = match self
            .connect_and_cache(account, &req.forward_headers, conn_key.as_deref())
            .await
        {
            ConnAttempt::Ready(shared) => shared,
            ConnAttempt::UpgradeRequired => {
                // Ground truth §5: the ONE `FallbackToHttp` trigger. Scoped per-session
                // (permanent for this session) + per-account (bounded cooldown) — module doc's
                // "Fallback scope" section.
                if let Some(key) = &session_key {
                    self.disable_session(key);
                }
                self.start_account_cooldown(&account.id);
                log_fallback("handshake_426", &account.id, session_key.as_deref());
                return self.fallback.execute(req, account, ctx).await;
            }
            // Generic handshake/transport failure: surfaces unchanged, exactly like today's HTTP
            // path — no fallback, no cooldown (module doc: 426 is the only trigger).
            ConnAttempt::Failed(e) => return Err(e),
        };

        self.drive_turn(
            account,
            &req.forward_headers,
            conn_key.as_deref(),
            session_key.as_deref(),
            &body,
            shared,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyflare_core::{Account, KeyStrength, SessionKey};
    use polyflare_testkit::{MockWsUpstream, ScriptedTurn};
    use serde_json::json;
    use std::sync::atomic::AtomicUsize as StdAtomicUsize;

    fn test_account(base_url: String) -> Account {
        Account {
            id: "acct-1".into(),
            base_url,
            bearer_token: "secret-bearer".into(),
            chatgpt_account_id: None,
        }
    }

    /// Same as [`test_account`] but with an explicit `id` — used by the Task 1 account-isolation
    /// test to construct two accounts pointing at the SAME mock upstream (same `base_url`) that
    /// differ ONLY in `id`, so the shared `handshake_count` proves a different account gets its own
    /// socket.
    fn test_account_with_id(base_url: String, id: &str) -> Account {
        Account {
            id: id.into(),
            ..test_account(base_url)
        }
    }

    fn ctx_with_session(session: &str) -> RequestCtx {
        RequestCtx {
            session_key: Some(SessionKey {
                value: format!("session-hash-{session}"),
                strength: KeyStrength::Hard,
            }),
            ..Default::default()
        }
    }

    /// Same as [`ctx_with_session`] but also sets `conn_discriminator` (the content-free
    /// `x-codex-window-id`, `{thread_id}:0`) — used by the Task 2 thread-isolation test.
    fn ctx_with_session_and_window(session: &str, window_id: &str) -> RequestCtx {
        RequestCtx {
            conn_discriminator: Some(window_id.to_string()),
            ..ctx_with_session(session)
        }
    }

    fn item(n: u32) -> Value {
        json!({"role": "user", "content": format!("item-{n}")})
    }

    fn prepared(body: Value) -> PreparedRequest {
        PreparedRequest {
            body: Some(body),
            model: "gpt-5.6-sol".to_string(),
            forward_headers: vec![],
            raw_body: None,
        }
    }

    /// A fallback stand-in that must NEVER be called — used by every test whose whole point is
    /// that WS handles the turn without ever reaching the fallback path. Panicking (rather than
    /// silently returning something) turns an unnoticed fallback-dispatch bug into a loud test
    /// failure instead of a green test that quietly proves nothing.
    struct NeverCalledExecutor;

    #[async_trait]
    impl Executor for NeverCalledExecutor {
        async fn execute(
            &self,
            _req: PreparedRequest,
            _account: &Account,
            _ctx: &RequestCtx,
        ) -> Result<ResponseStream, ExecError> {
            panic!("fallback executor must not be called for this test");
        }
    }

    fn never_called_fallback() -> Arc<dyn Executor> {
        Arc::new(NeverCalledExecutor)
    }

    /// A fallback stand-in that records how many times it was called and returns a trivially
    /// empty (but successful) stream — for tests whose point IS that fallback gets used.
    #[derive(Clone)]
    struct RecordingExecutor {
        calls: Arc<StdAtomicUsize>,
    }

    impl RecordingExecutor {
        fn new() -> Self {
            Self {
                calls: Arc::new(StdAtomicUsize::new(0)),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Executor for RecordingExecutor {
        async fn execute(
            &self,
            _req: PreparedRequest,
            _account: &Account,
            _ctx: &RequestCtx,
        ) -> Result<ResponseStream, ExecError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(futures_util::stream::empty()))
        }
    }

    async fn drain(mut stream: ResponseStream) -> Vec<Result<bytes::Bytes, String>> {
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item.map_err(|e| e.to_string()));
        }
        out
    }

    // ---- THE central delta proof: two executor.execute() calls, one connection, a real delta --

    #[tokio::test]
    async fn second_execute_call_same_session_reuses_the_connection_and_sends_a_real_delta() {
        // 4 scripted turns: this test now drives FOUR sequential executor.execute() calls, not
        // two. The original 2-turn version of this test is exactly why the "delta reverts to
        // full-resend every OTHER turn" regression shipped silently — it stopped looking right
        // after the one turn that happened to still work. Turns 3 and 4 are the regression guard:
        // per SPEC-M5-WEBSOCKET.md §2, the HTTP client resends the FULL accumulated history every
        // turn, so turn 3's body already has 4 items (2 from turn 1, 1 new in turn 2, 1 new here).
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::normal(vec![]),
        ]);
        let base = mock.clone().spawn().await;
        let executor = CodexWsExecutor::new(never_called_fallback());
        let account = test_account(base);
        let ctx = ctx_with_session("alpha");

        // Turn 1: two items, no anchor possible yet.
        let body1 = json!({"model": "gpt-5.6-sol", "input": [item(0), item(1)]});
        let stream1 = executor
            .execute(prepared(body1), &account, &ctx)
            .await
            .expect("turn 1 must succeed");
        drain(stream1).await;

        // Turn 2: the SAME two items plus exactly one new one — a strict extension.
        let body2 = json!({"model": "gpt-5.6-sol", "input": [item(0), item(1), item(2)]});
        let stream2 = executor
            .execute(prepared(body2), &account, &ctx)
            .await
            .expect("turn 2 must succeed");
        drain(stream2).await;

        assert_eq!(
            mock.handshake_count(),
            1,
            "two executor.execute() calls for the same session must reuse ONE connection"
        );
        assert_eq!(
            mock.last_frame_anchor(),
            Some("resp_1".to_string()),
            "turn 2 must anchor on turn 1's response id, not resend full history"
        );
        assert_eq!(
            mock.last_frame_input_len(),
            Some(1),
            "turn 2 must send ONLY the one new item — this is the delta actually being a delta, \
             not merely 'nothing failed'"
        );

        // Turn 3 — THE REGRESSION GUARD: the client resends the full accumulated history (4
        // items: item0, item1 from turn 1, item2 from turn 2, item3 new here). The socket's
        // `last_item_hashes` must reflect the FULL 3-item history it holds after turn 2 (item0,
        // item1, item2), not merely the 1-item wire suffix turn 2 actually sent — otherwise
        // this 4-item body has no valid recorded prefix to extend and silently falls back to a
        // full resend (the bug this test exists to catch).
        let body3 = json!({
            "model": "gpt-5.6-sol",
            "input": [item(0), item(1), item(2), item(3)],
        });
        let stream3 = executor
            .execute(prepared(body3), &account, &ctx)
            .await
            .expect("turn 3 must succeed");
        drain(stream3).await;

        assert_eq!(
            mock.last_frame_anchor(),
            Some("resp_2".to_string()),
            "turn 3 must anchor on turn 2's completion id (resp_2) — the bug forgets the \
             accumulated history and forces an unanchored Full resend here instead"
        );
        assert_eq!(
            mock.last_frame_input_len(),
            Some(1),
            "turn 3 must send ONLY the single newest item (item3), not the full 4-item history"
        );

        // Turn 4 — proves the chain holds indefinitely, not just for one extra turn past the
        // point the bug used to bite.
        let body4 = json!({
            "model": "gpt-5.6-sol",
            "input": [item(0), item(1), item(2), item(3), item(4)],
        });
        let stream4 = executor
            .execute(prepared(body4), &account, &ctx)
            .await
            .expect("turn 4 must succeed");
        drain(stream4).await;

        assert_eq!(
            mock.last_frame_anchor(),
            Some("resp_3".to_string()),
            "turn 4 must still be a delta, anchored on turn 3's completion id (resp_3)"
        );
        assert_eq!(
            mock.last_frame_input_len(),
            Some(1),
            "turn 4 must still send only the single newest item (item4)"
        );

        assert_eq!(
            mock.handshake_count(),
            1,
            "all four turns must stay on the SAME connection throughout"
        );
        let frames = mock.frames();
        assert_eq!(frames.len(), 4);
        assert_eq!(frames[0].input_len, 2, "turn 1: full 2-item send");
        assert_eq!(frames[1].input_len, 1, "turn 2: 1-item delta");
        assert_eq!(
            frames[2].input_len, 1,
            "turn 3: 1-item delta (the regression guard)"
        );
        assert_eq!(
            frames[3].input_len, 1,
            "turn 4: 1-item delta (the chain holds)"
        );
    }

    // ---- M5a follow-up: interleaved models on ONE session must NOT share a socket --------------

    /// Root cause this guards: codex interleaves TWO models per turn (e.g. gpt-5.6-luna +
    /// gpt-5.6-sol) on the SAME conversation, hence the SAME `session_key`. Before this fix, the
    /// connection cache was keyed on `session_key` alone, so both models shared one socket and
    /// clobbered each other's `last_non_input_fingerprint` — every turn failed delta.rs's Gate 3
    /// and fell back to `RequestPlan::Full` (0% cache). The fix folds
    /// `non_input_fingerprint(body)` into the cache key (`conn_key`), so each model-stream gets
    /// its own socket + its own clean strict-extension chain, while `session_key` alone still
    /// governs the 426 disable (session-scoped, not model-scoped).
    #[tokio::test]
    async fn interleaved_models_same_session_get_distinct_sockets_and_each_still_caches() {
        // 4 scripted turns: model A's first call, model B's first call (must NOT reuse A's
        // socket), model A's second call (must reuse A's socket + send a real delta), model B's
        // second call (must reuse B's socket + send a real delta) — proving BOTH model-streams
        // cache independently, not just that they don't collide once.
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::normal(vec![]),
        ]);
        let base = mock.clone().spawn().await;
        let executor = CodexWsExecutor::new(never_called_fallback());
        let account = test_account(base);
        let ctx = ctx_with_session("interleaved"); // ONE stable session_key for both models

        // Model A, turn 1: two items, no anchor yet.
        let a1 = json!({"model": "gpt-5.6-luna", "input": [item(0), item(1)]});
        let stream = executor
            .execute(prepared(a1), &account, &ctx)
            .await
            .expect("model A turn 1 must succeed");
        drain(stream).await;
        assert_eq!(
            mock.handshake_count(),
            1,
            "model A's first call dials its own fresh socket"
        );

        // Model B, first call on the SAME session_key: body differs ONLY in `model`. This is the
        // regression case — pre-fix, this reused model A's socket (same session_key) and got
        // planned as an Incremental anchored on model A's response, or clobbered A's fingerprint.
        let b1 = json!({"model": "gpt-5.6-sol", "input": [item(0), item(1)]});
        let stream = executor
            .execute(prepared(b1), &account, &ctx)
            .await
            .expect("model B turn 1 must succeed");
        drain(stream).await;
        assert_eq!(
            mock.handshake_count(),
            2,
            "model B must dial its OWN fresh socket, not reuse model A's — a different \
             conn_key (session_key + non_input_fingerprint) for a different model"
        );
        assert_eq!(
            mock.last_frame_anchor(),
            None,
            "model B's first turn must be a genuine Full send — NEVER an Incremental anchored \
             on model A's response id, which is what the shared-socket bug produced"
        );
        assert_eq!(
            mock.last_frame_input_len(),
            Some(2),
            "model B's first turn sends its full 2-item input, not a delta off model A"
        );

        // Model A, turn 2: a strict extension of model A's OWN history. Must reuse model A's
        // socket (still only 2 handshakes total) and send a real 1-item delta anchored on A's
        // turn-1 response — proving model A's own cache entry survived model B's calls untouched.
        let a2 = json!({"model": "gpt-5.6-luna", "input": [item(0), item(1), item(2)]});
        let stream = executor
            .execute(prepared(a2), &account, &ctx)
            .await
            .expect("model A turn 2 must succeed");
        drain(stream).await;
        assert_eq!(
            mock.handshake_count(),
            2,
            "model A's second call must REUSE its own cached socket — no new handshake"
        );
        assert_eq!(
            mock.last_frame_anchor(),
            Some("resp_1".to_string()),
            "model A's second turn must anchor on model A's OWN turn-1 response (resp_1)"
        );
        assert_eq!(
            mock.last_frame_input_len(),
            Some(1),
            "model A's second turn must send only the one new item — a real delta"
        );

        // Model B, turn 2: a strict extension of model B's OWN history. Must reuse model B's
        // socket (still only 2 handshakes total) and send a real 1-item delta anchored on B's
        // turn-1 response (resp_2, since model A's turn 2 above produced resp_3... but B's own
        // chain only ever saw its own turn 1, so B's anchor is whatever ITS OWN prior response
        // was) — the mock assigns response ids in the order turns are actually driven, so this
        // is asserted structurally (an anchor exists and is NOT model A's) rather than by a
        // hardcoded id.
        let b2 = json!({"model": "gpt-5.6-sol", "input": [item(0), item(1), item(2)]});
        let stream = executor
            .execute(prepared(b2), &account, &ctx)
            .await
            .expect("model B turn 2 must succeed");
        drain(stream).await;
        assert_eq!(
            mock.handshake_count(),
            2,
            "model B's second call must REUSE its own cached socket — still no new handshake"
        );
        assert!(
            mock.last_frame_anchor().is_some(),
            "model B's second turn must be anchored (a real delta), not a fresh Full send"
        );
        assert_eq!(
            mock.last_frame_input_len(),
            Some(1),
            "model B's second turn must send only the one new item — a real delta on B's OWN \
             chain, independent of model A's"
        );
    }

    // ---- Task 2 (sub-agent identity): distinct codex threads must NOT share a socket -----------

    /// Root cause this guards: each codex THREAD (main / review / compact / spawn) carries a
    /// distinct `x-codex-window-id` (`{thread_id}:0`), surfaced content-free as
    /// `ctx.conn_discriminator`. Two threads can arrive on the SAME `session_key` AND SAME model
    /// (hence identical `non_input_fingerprint`) — the one gap the just-merged
    /// session+fingerprint key does NOT split (e.g. the `x-codex-turn-state` session_key branch
    /// drops `prompt_cache_key`). Folding the hashed discriminator in as a THIRD component
    /// (`session:fingerprint:window_id`) guarantees each thread gets its OWN socket + anchor chain
    /// regardless of which branch fired. When `conn_discriminator` is `None` the key is
    /// byte-identical to the pre-task key (back-compat — the interleaved-models test above still
    /// passes unchanged), so this closes the gap without regressing that behavior.
    #[tokio::test]
    async fn distinct_window_ids_same_session_and_model_get_distinct_sockets() {
        // 3 turns for 3 execute() calls: thread A dials, thread B (different window-id, SAME
        // session + SAME body) dials its OWN socket, then thread A REPEATED (same window-id) reuses
        // A's socket — proving both that a DIFFERENT window-id splits and that the SAME window-id
        // reuses (the Some==Some path), not merely that Some!=None differs.
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::normal(vec![]),
        ]);
        let base = mock.clone().spawn().await;
        let executor = CodexWsExecutor::new(never_called_fallback());
        let account = test_account(base);

        // ONE session_key, ONE model/body — the ONLY thing that differs is the window-id.
        let ctx_a = ctx_with_session_and_window("threaded", "tid-A:0");
        let ctx_b = ctx_with_session_and_window("threaded", "tid-B:0");
        let body = json!({"model": "gpt-5.6-sol", "input": [item(0), item(1)]});

        // Thread A, first call: dials its own fresh socket.
        let stream = executor
            .execute(prepared(body.clone()), &account, &ctx_a)
            .await
            .expect("thread A must succeed");
        drain(stream).await;
        assert_eq!(
            mock.handshake_count(),
            1,
            "thread A's first call dials its own fresh socket"
        );

        // Thread B: SAME session_key AND SAME body (hence SAME non_input_fingerprint), differing
        // ONLY in x-codex-window-id. Pre-fix this reused thread A's socket (identical
        // session+fingerprint key). It must now dial its OWN socket — the whole point of Task 2.
        let stream = executor
            .execute(prepared(body.clone()), &account, &ctx_b)
            .await
            .expect("thread B must succeed");
        drain(stream).await;
        assert_eq!(
            mock.handshake_count(),
            2,
            "thread B must dial its OWN socket — a different x-codex-window-id yields a different \
             conn_key even with an identical session_key AND non_input_fingerprint"
        );

        // Thread A again, IDENTICAL in all three (session, fingerprint, window-id): must REUSE A's
        // socket — proving same-discriminator reuse, not just that different-discriminators split.
        let stream = executor
            .execute(prepared(body.clone()), &account, &ctx_a)
            .await
            .expect("thread A's repeat must succeed");
        drain(stream).await;
        assert_eq!(
            mock.handshake_count(),
            2,
            "a repeat with the SAME window-id must REUSE the existing socket — no third handshake"
        );
    }

    // ---- WS hardening Task 1 (account identity): a failover must NEVER reuse another account's socket

    /// Root cause this guards: a WS socket to chatgpt.com is authenticated per-account at the
    /// HANDSHAKE (Bearer + ChatGPT-Account-ID). On failover the owner account changes (A -> B) but
    /// the session/conversation/body/window-id are all identical, so a `conn_key` that omits the
    /// account is BYTE-IDENTICAL across the switch — and `connect_and_cache` reused the cached
    /// socket on `is_closed()` alone, never the account. B's turn would then be driven over A's
    /// still-live authenticated socket -> served/billed on A (the very account failover exists to
    /// avoid). Folding `account.id` in as the FIRST `conn_key` component makes a different account
    /// resolve a different key -> its own fresh handshake, while the SAME account keeps a stable key
    /// -> the just-merged incremental caching is untouched.
    #[tokio::test]
    async fn two_accounts_same_session_and_body_get_distinct_sockets() {
        // 3 turns for 3 execute() calls: account A dials, account B (different account.id, SAME
        // session + SAME body + SAME/absent window-id) must dial its OWN socket, then account A
        // REPEATED reuses A's socket — proving both that a DIFFERENT account.id splits AND that the
        // SAME account.id reuses (same-account caching preserved), not merely that they differ once.
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::normal(vec![]),
        ]);
        let base = mock.clone().spawn().await;
        let executor = CodexWsExecutor::new(never_called_fallback());

        // Two accounts pointing at the SAME mock upstream, differing ONLY in `id`.
        let account_a = test_account_with_id(base.clone(), "acct-A");
        let account_b = test_account_with_id(base, "acct-B");

        // ONE session_key, ONE model/body — the ONLY thing that differs is the account.
        let ctx = ctx_with_session("failover");
        let body = json!({"model": "gpt-5.6-sol", "input": [item(0), item(1)]});

        // Account A, first call: dials its own fresh socket.
        let stream = executor
            .execute(prepared(body.clone()), &account_a, &ctx)
            .await
            .expect("account A must succeed");
        drain(stream).await;
        assert_eq!(
            mock.handshake_count(),
            1,
            "account A's first call dials its own fresh socket"
        );

        // Account B: SAME session_key AND SAME body (hence SAME non_input_fingerprint) AND
        // SAME/absent window-id, differing ONLY in account.id. Pre-fix this reused account A's
        // still-live, A-authenticated socket -> B's turn served/billed on A. It must now dial its
        // OWN socket — the whole point of Task 1.
        let stream = executor
            .execute(prepared(body.clone()), &account_b, &ctx)
            .await
            .expect("account B must succeed");
        drain(stream).await;
        assert_eq!(
            mock.handshake_count(),
            2,
            "account B must dial its OWN socket — a different account.id yields a different \
             conn_key even with an identical session_key, body, and window-id: a failover can \
             never reuse the prior account's authenticated connection"
        );

        // Account A again, IDENTICAL in all of (account.id, session, body, window-id): must REUSE
        // A's socket — proving same-account caching still works, not just that accounts split.
        let stream = executor
            .execute(prepared(body.clone()), &account_a, &ctx)
            .await
            .expect("account A's repeat must succeed");
        drain(stream).await;
        assert_eq!(
            mock.handshake_count(),
            2,
            "a repeat on the SAME account must REUSE the existing socket — no third handshake: \
             folding account.id in must not regress same-account incremental caching"
        );
    }

    // ---- Concurrency (M5a Task 8): two execute() calls on the SAME session key must not race ---

    /// The race the task's concurrency review flagged: `plan_and_build`'s read of the cached
    /// connection and the turn's send used to be two SEPARATE lock acquisitions, with a gap in
    /// between. Two concurrent `execute()` calls on the SAME session key (hence the SAME cached
    /// `SharedWsConn`) could both plan against the identical pre-race state before either sent,
    /// then queue behind each other to send — so the SECOND to actually reach the wire would send
    /// a STALE envelope (planned before the FIRST turn advanced the connection's real state).
    ///
    /// This mock never validates `previous_response_id` (only a scripted
    /// `previous_response_not_found` simulates that) — it accepts anything — so the race's actual
    /// signature is NOT a surfaced error; it's two racing turns silently computing the SAME
    /// "genuine 1-item delta anchored on resp_1" plan, i.e. two different turns claiming the exact
    /// same parent. Fixed (this test, post-fix): whichever turn's plan+send critical section
    /// actually runs SECOND (after the connection's lock is acquired, per the fix) sees the
    /// FIRST's already-applied state and correctly recomputes the identical 3-item body as `Full`
    /// (nothing left to extend) instead of a second identical incremental send.
    ///
    /// **Runtime note:** `flavor = "multi_thread"` is required, not cosmetic. On the default
    /// current-thread test runtime, two spawned tasks only ever interleave at a GENUINE
    /// `Poll::Pending` suspension — and (verified empirically while building this test) the
    /// `recv_frame().await` a turn blocks on while awaiting the mock's reply is exactly such a
    /// point, so the connection's `tokio::sync::Mutex` ends up serializing the OLD (unfixed) code
    /// too, purely as a scheduling accident, hiding the race entirely on that runtime. Only true
    /// OS-thread parallelism can land a second task's plan+lock-acquire inside the actual
    /// microseconds-wide gap the fix closes. The 300-iteration loop compensates for that window
    /// being narrow: this test, run against the pre-fix code during development, reliably
    /// reproduced the exact predicted corruption (two frames both `(Some("resp_1"), 1)`) within
    /// the first handful of iterations — see `.superpowers/sdd/m5a-task-8-report.md`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_execute_calls_on_the_same_session_key_do_not_corrupt_the_delta_chain() {
        for _trial in 0..300u32 {
            let mock = MockWsUpstream::scripted(vec![
                ScriptedTurn::normal(vec![]),
                ScriptedTurn::normal(vec![]),
                ScriptedTurn::normal(vec![]),
                ScriptedTurn::normal(vec![]),
            ]);
            let base = mock.clone().spawn().await;
            let executor = Arc::new(CodexWsExecutor::new(never_called_fallback()));
            let account = test_account(base);
            let ctx = ctx_with_session("race");

            // Warm-up (sequential): seeds a real anchor (resp_1) + a 2-item history, and — just as
            // importantly — gets the connection CACHED before the race starts. The initial-connect
            // race (two concurrent calls with NOTHING cached yet, both dialing fresh sockets) is a
            // separate, pre-existing concern in `connect_and_cache`, out of this test's scope; this
            // test isolates the plan-vs-send race specifically.
            let warmup = json!({"model": "m", "input": [item(0), item(1)]});
            let s0 = executor
                .execute(prepared(warmup), &account, &ctx)
                .await
                .expect("warm-up turn must succeed");
            drain(s0).await;
            assert_eq!(mock.handshake_count(), 1);

            // Two concurrent execute() calls, SAME session key, IDENTICAL body: a genuine one-item
            // strict extension of the warm-up's 2-item history (item2).
            let body = json!({"model": "m", "input": [item(0), item(1), item(2)]});
            let spawn_racer =
                |executor: Arc<CodexWsExecutor>, account: Account, ctx: RequestCtx, body: Value| {
                    tokio::spawn(async move {
                        let stream = executor
                            .execute(prepared(body), &account, &ctx)
                            .await
                            .expect("a racing call must succeed");
                        drain(stream).await
                    })
                };
            let task_a = spawn_racer(executor.clone(), account.clone(), ctx.clone(), body.clone());
            let task_b = spawn_racer(executor.clone(), account.clone(), ctx.clone(), body.clone());
            let (a, b) = tokio::join!(task_a, task_b);
            let a = a.expect("racer A must not panic");
            let b = b.expect("racer B must not panic");
            assert!(a.iter().all(|i| i.is_ok()), "racer A: {a:?}");
            assert!(b.iter().all(|i| i.is_ok()), "racer B: {b:?}");

            // No reconnect was ever needed — both racing turns went over the SAME already-cached
            // socket; the race is about planning, not about the connection cache itself.
            assert_eq!(
                mock.handshake_count(),
                1,
                "no reconnect should ever be triggered by this race"
            );

            let frames = mock.frames();
            assert_eq!(
            frames.len(),
            3,
            "warm-up(1) + exactly 2 racing turns(1 each) — no extra resend from an unrecovered or \
             re-recovered corruption"
        );
            assert_eq!(frames[0].previous_response_id, None);
            assert_eq!(frames[0].input_len, 2, "warm-up: full 2-item send");

            // The two racing frames, asserted as a SET (task scheduling order is not deterministic):
            // exactly one must be the genuine extension (anchored on the warm-up's resp_1, 1-item
            // suffix) and the OTHER must be a correctly-recomputed Full send (no anchor, the full
            // 3-item body) — because it planned against the FIRST racer's already-applied state. The
            // corrupted-race shape this guards against is TWO identical anchored 1-item frames (both
            // claiming resp_1 as parent, only one of which is actually true).
            let racing = &frames[1..];
            let debug_racing: Vec<(Option<String>, usize)> = racing
                .iter()
                .map(|f| (f.previous_response_id.clone(), f.input_len))
                .collect();
            let incremental_count = racing
                .iter()
                .filter(|f| f.previous_response_id.as_deref() == Some("resp_1") && f.input_len == 1)
                .count();
            let full_count = racing
                .iter()
                .filter(|f| f.previous_response_id.is_none() && f.input_len == 3)
                .count();
            assert_eq!(
            (incremental_count, full_count),
            (1, 1),
            "exactly one racing turn must be a genuine 1-item delta off resp_1 and the other a \
             correctly-recomputed full resend — NOT two identical stale deltas both claiming \
             resp_1 (that would be the race corrupting the chain): {debug_racing:?}"
        );

            // The chain still holds going forward: a THIRD, sequential (non-racing) turn extending the
            // now-merged 3-item history by one more item must still be planned as a genuine delta, not
            // a permanently-broken Full — proving the race didn't leave the connection's local
            // delta-tracking state corrupted for future turns either.
            let follow_up = json!({
                "model": "m",
                "input": [item(0), item(1), item(2), item(3)],
            });
            let s3 = executor
                .execute(prepared(follow_up), &account, &ctx)
                .await
                .expect("follow-up turn must succeed");
            drain(s3).await;
            assert_eq!(mock.handshake_count(), 1, "still the same one connection");
            let last = mock.frames().last().cloned().unwrap();
            assert_eq!(
                last.input_len, 1,
                "the chain must still be a genuine 1-item delta after the race resolved cleanly"
            );
            assert!(
                last.previous_response_id.is_some(),
                "must still be anchored after the race — the chain was not corrupted"
            );
        }
    }

    // ---- Row: previous_response_not_found -> strip anchor, full resend, SAME socket, bounded ---

    #[tokio::test]
    async fn anchor_miss_recovers_transparently_and_the_client_sees_only_a_clean_stream() {
        // 3-entry script: turn1 completes normally (seeds a real anchor); turn2's first attempt
        // (anchored, per plan_request) gets told the anchor is dead; turn2's retry (now full,
        // anchorless — the recovery) gets a clean completion. Proves recovery AND that the retry
        // actually stripped the anchor (frame 2 anchored, frame 2's retry i.e. frame 3 is not).
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::previous_response_not_found("resp_1"),
            ScriptedTurn::normal(vec![]),
        ]);
        let base = mock.clone().spawn().await;
        let executor = CodexWsExecutor::new(never_called_fallback());
        let account = test_account(base);
        let ctx = ctx_with_session("beta");

        let stream1 = executor
            .execute(
                prepared(json!({"model": "m", "input": [item(0), item(1)]})),
                &account,
                &ctx,
            )
            .await
            .expect("turn 1 must succeed");
        drain(stream1).await;

        let stream2 = executor
            .execute(
                prepared(json!({"model": "m", "input": [item(0), item(1), item(2)]})),
                &account,
                &ctx,
            )
            .await
            .expect("turn 2 must recover from the anchor miss and still succeed");
        let items = drain(stream2).await;

        assert!(
            items.iter().all(|i| i.is_ok()),
            "the client must see a CLEAN stream — the anchor-miss must never surface: {items:?}"
        );
        assert!(
            items.iter().any(
                |i| matches!(i, Ok(b) if String::from_utf8_lossy(b).contains("response.completed"))
            ),
            "must still see a real completion after recovery: {items:?}"
        );

        let frames = mock.frames();
        assert_eq!(
            frames.len(),
            3,
            "turn1(1) + turn2's anchored attempt(1) + turn2's stripped-anchor retry(1) = 3 frames"
        );
        assert_eq!(frames[1].previous_response_id, Some("resp_1".to_string()));
        assert_eq!(
            frames[2].previous_response_id, None,
            "the retry must have stripped the anchor"
        );
        assert_eq!(
            frames[2].input_len, 3,
            "the retry must be a FULL resend (all 3 items), not another delta"
        );
        assert_eq!(
            mock.handshake_count(),
            1,
            "anchor-miss recovery resends on the SAME socket — no reconnect"
        );
    }

    #[tokio::test]
    async fn anchor_miss_recovery_gives_up_after_bounded_retries() {
        // Every attempt gets told the anchor is dead — proves the retry loop actually terminates
        // rather than looping forever, and surfaces the error once the bound is hit.
        let mock = MockWsUpstream::new(ScriptedTurn::previous_response_not_found("resp_dead"));
        let base = mock.clone().spawn().await;
        let executor = CodexWsExecutor::with_config(
            never_called_fallback(),
            2, // max_anchor_miss_retries
            2,
            Duration::from_secs(30),
        );
        let account = test_account(base);
        let ctx = ctx_with_session("gamma");

        let err = executor
            .execute(
                prepared(json!({"model": "m", "input": [item(0)]})),
                &account,
                &ctx,
            )
            .await
            .err()
            .expect("must give up rather than loop forever");

        match err {
            ExecError::Stream(msg) => {
                assert!(msg.contains("previous_response_not_found"), "{msg}");
            }
            other => panic!("expected ExecError::Stream, got {other:?}"),
        }
        // The attempt count, as a real assertable value: 1 initial + 2 bounded retries = 3 frames.
        assert_eq!(
            mock.frames().len(),
            3,
            "must attempt exactly 1 + max_anchor_miss_retries times, then give up"
        );
    }

    // ---- Row: websocket_connection_limit_reached -> reconnect, full resend, bounded -----------

    #[tokio::test]
    async fn connection_limit_reached_triggers_a_reconnect_and_resend() {
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::connection_limit_reached(409),
            ScriptedTurn::normal(vec![]),
        ]);
        let base = mock.clone().spawn().await;
        let executor = CodexWsExecutor::new(never_called_fallback());
        let account = test_account(base);
        let ctx = ctx_with_session("delta-session");

        let stream = executor
            .execute(
                prepared(json!({"model": "m", "input": [item(0)]})),
                &account,
                &ctx,
            )
            .await
            .expect("must recover via reconnect and still succeed");
        let items = drain(stream).await;

        assert!(items.iter().all(|i| i.is_ok()), "{items:?}");
        assert_eq!(
            mock.handshake_count(),
            2,
            "connection-limit recovery must RECONNECT (a brand new handshake), unlike anchor-miss"
        );
    }

    #[tokio::test]
    async fn reconnect_recovery_gives_up_after_bounded_retries() {
        let mock = MockWsUpstream::new(ScriptedTurn::connection_limit_reached(409));
        let base = mock.clone().spawn().await;
        let executor = CodexWsExecutor::with_config(
            never_called_fallback(),
            2,
            2, // max_reconnect_retries
            Duration::from_secs(30),
        );
        let account = test_account(base);
        let ctx = ctx_with_session("epsilon");

        let err = executor
            .execute(
                prepared(json!({"model": "m", "input": [item(0)]})),
                &account,
                &ctx,
            )
            .await
            .err()
            .expect("must give up rather than reconnect forever");

        match err {
            ExecError::Stream(msg) => {
                assert!(msg.contains("websocket_connection_limit_reached"), "{msg}")
            }
            other => panic!("expected ExecError::Stream, got {other:?}"),
        }
        assert_eq!(
            mock.handshake_count(),
            3,
            "1 initial handshake + max_reconnect_retries(2) more = 3, then give up"
        );
    }

    #[tokio::test]
    async fn close_before_any_terminal_frame_as_the_first_item_triggers_reconnect() {
        // Distinguishes THIS case (close arrives before ANY byte reached the caller — recoverable,
        // grouped with connection-limit per SPEC-M5 §4) from the "after the first byte" case
        // below (never retried).
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::close_mid_stream(vec![]), // closes with NO events at all first
            ScriptedTurn::normal(vec![]),
        ]);
        let base = mock.clone().spawn().await;
        let executor = CodexWsExecutor::new(never_called_fallback());
        let account = test_account(base);
        let ctx = ctx_with_session("zeta");

        let stream = executor
            .execute(
                prepared(json!({"model": "m", "input": [item(0)]})),
                &account,
                &ctx,
            )
            .await
            .expect("must recover via reconnect");
        let items = drain(stream).await;

        assert!(items.iter().all(|i| i.is_ok()), "{items:?}");
        assert_eq!(mock.handshake_count(), 2);
    }

    // ---- Row: mid-stream failure after the first byte -> surfaces immediately, never retried --

    #[tokio::test]
    async fn close_mid_stream_after_the_first_byte_surfaces_immediately_without_retry() {
        let mock = MockWsUpstream::new(ScriptedTurn::close_mid_stream(vec![json!({
            "type": "response.output_text.delta",
            "delta": "partial",
        })
        .to_string()]));
        let base = mock.clone().spawn().await;
        let executor = CodexWsExecutor::new(never_called_fallback());
        let account = test_account(base);
        let ctx = ctx_with_session("eta");

        let stream = executor
            .execute(
                prepared(json!({"model": "m", "input": [item(0)]})),
                &account,
                &ctx,
            )
            .await
            .expect("the first byte succeeded, so execute() itself returns Ok");
        let items = drain(stream).await;

        assert_eq!(
            items.len(),
            2,
            "one good delta byte, then the surfaced error: {items:?}"
        );
        assert!(items[0].is_ok());
        match &items[1] {
            Err(msg) => assert!(msg.contains("closed"), "{msg}"),
            Ok(_) => panic!("expected the close to surface as an error after the first byte"),
        }
        assert_eq!(
            mock.handshake_count(),
            1,
            "no reconnect — a post-first-byte failure is never retried"
        );
    }

    // ---- Row: 429 envelope -> ExecError::UpstreamStatus, unchanged, never retried -------------

    #[tokio::test]
    async fn rate_limit_429_surfaces_as_upstream_status_unchanged() {
        let mock = MockWsUpstream::new(ScriptedTurn::rate_limited_429(37));
        let base = mock.clone().spawn().await;
        let executor = CodexWsExecutor::new(never_called_fallback());
        let account = test_account(base);
        let ctx = ctx_with_session("theta");

        let err = executor
            .execute(
                prepared(json!({"model": "m", "input": [item(0)]})),
                &account,
                &ctx,
            )
            .await
            .err()
            .expect("429 must surface, not be retried");

        match err {
            ExecError::UpstreamStatus(sig) => {
                assert_eq!(sig.status, 429);
                assert_eq!(sig.retry_after, Some(37));
            }
            other => panic!("expected ExecError::UpstreamStatus, got {other:?}"),
        }
        assert_eq!(mock.frames().len(), 1, "a 429 must never be retried");
    }

    // ---- Task 3: general error envelope carries error.code into ExecError::UpstreamStatus -----

    #[tokio::test]
    async fn general_error_envelope_carries_the_error_code_content_safely() {
        // A general code — NEITHER previous_response_not_found nor
        // websocket_connection_limit_reached — must surface via the ordinary UpstreamStatus arm,
        // now carrying `error_code`.
        let mock = MockWsUpstream::new(ScriptedTurn::ErrorEnvelope {
            status: 403,
            code: "account_deactivated".to_string(),
            message: "secret ws detail".to_string(),
            error_extra: vec![],
            headers: vec![],
        });
        let base = mock.clone().spawn().await;
        let executor = CodexWsExecutor::new(never_called_fallback());
        let account = test_account(base);
        let ctx = ctx_with_session("mu");

        let err = executor
            .execute(
                prepared(json!({"model": "m", "input": [item(0)]})),
                &account,
                &ctx,
            )
            .await
            .err()
            .expect("a general error envelope must surface, not be retried");

        match &err {
            ExecError::UpstreamStatus(sig) => {
                assert_eq!(sig.status, 403);
                assert_eq!(sig.error_code.as_deref(), Some("account_deactivated"));
            }
            other => panic!("expected ExecError::UpstreamStatus, got {other:?}"),
        }
        assert_eq!(
            mock.frames().len(),
            1,
            "a general error must never be retried"
        );

        // Content-safety: the envelope's `error.message` must never surface via Display or Debug.
        let display = format!("{err}");
        let debug = format!("{err:?}");
        assert!(
            !display.contains("secret ws detail"),
            "Display leaked the envelope message: {display}"
        );
        assert!(
            !debug.contains("secret ws detail"),
            "Debug leaked the envelope message: {debug}"
        );
    }

    // ---- Row: terminal response.failed -> reframe as SSE, pass through, the error as today ----

    #[tokio::test]
    async fn terminal_response_failed_is_reframed_as_sse_and_passed_through() {
        let mock = MockWsUpstream::new(ScriptedTurn::Failed {
            code: "context_window_exceeded".to_string(),
            message: "too long".to_string(),
        });
        let base = mock.clone().spawn().await;
        let executor = CodexWsExecutor::new(never_called_fallback());
        let account = test_account(base);
        let ctx = ctx_with_session("iota");

        let stream = executor
            .execute(
                prepared(json!({"model": "m", "input": [item(0)]})),
                &account,
                &ctx,
            )
            .await
            .expect("execute() itself succeeds; the failure travels IN the SSE stream");
        let items = drain(stream).await;

        assert_eq!(items.len(), 1);
        let text = match &items[0] {
            Ok(bytes) => String::from_utf8_lossy(bytes).to_string(),
            Err(e) => panic!("expected an Ok SSE frame carrying the failure, got Err({e})"),
        };
        assert!(text.starts_with("data: "), "{text}");
        assert!(text.contains("context_window_exceeded"), "{text}");
    }

    // ---- Row: handshake 426 -> HTTP-SSE for this session, never surfaces ----------------------

    #[tokio::test]
    async fn handshake_426_falls_back_and_disables_ws_for_this_session_only() {
        let mock = MockWsUpstream::rejecting_handshake();
        let base = mock.clone().spawn().await;
        let fallback = RecordingExecutor::new();
        let executor = CodexWsExecutor::new(Arc::new(fallback.clone()));
        let account = test_account(base);
        let ctx = ctx_with_session("kappa");

        let stream = executor
            .execute(
                prepared(json!({"model": "m", "input": [item(0)]})),
                &account,
                &ctx,
            )
            .await
            .expect("must fall back to HTTP-SSE, never surface the 426 itself");
        drain(stream).await;

        assert_eq!(fallback.call_count(), 1);
        assert_eq!(executor.ws_connect_attempts(), 1);
        assert_eq!(mock.handshake_count(), 0, "426 never establishes a socket");

        // A SECOND call, same session: must skip the WS attempt entirely (session disabled), not
        // merely fail it again.
        let stream2 = executor
            .execute(
                prepared(json!({"model": "m", "input": [item(0)]})),
                &account,
                &ctx,
            )
            .await
            .expect("must fall back again");
        drain(stream2).await;

        assert_eq!(fallback.call_count(), 2);
        assert_eq!(
            executor.ws_connect_attempts(),
            1,
            "the session-disabled bypass must skip the WS connect attempt entirely on the 2nd call"
        );
    }

    #[tokio::test]
    async fn account_cooldown_after_426_bypasses_ws_for_a_different_session_same_account() {
        let mock = MockWsUpstream::rejecting_handshake();
        let base = mock.clone().spawn().await;
        let fallback = RecordingExecutor::new();
        let executor = CodexWsExecutor::new(Arc::new(fallback.clone()));
        let account = test_account(base);

        let ctx_a = ctx_with_session("session-A");
        let stream = executor
            .execute(
                prepared(json!({"model": "m", "input": [item(0)]})),
                &account,
                &ctx_a,
            )
            .await
            .unwrap();
        drain(stream).await;
        assert_eq!(executor.ws_connect_attempts(), 1);

        // A DIFFERENT session on the SAME account: session-level disablement doesn't apply (new
        // key), but the account-level cooldown must still bypass WS.
        let ctx_b = ctx_with_session("session-B");
        let stream2 = executor
            .execute(
                prepared(json!({"model": "m", "input": [item(0)]})),
                &account,
                &ctx_b,
            )
            .await
            .unwrap();
        drain(stream2).await;

        assert_eq!(fallback.call_count(), 2);
        assert_eq!(
            executor.ws_connect_attempts(),
            1,
            "account cooldown must bypass WS for a DIFFERENT session on the same account too"
        );
    }

    // ---- Row: handshake/transport failure (not 426) -> ExecError::Upstream, unchanged ---------

    #[tokio::test]
    async fn generic_connect_failure_surfaces_as_upstream_error_unchanged() {
        let fallback = RecordingExecutor::new();
        let executor = CodexWsExecutor::new(Arc::new(fallback.clone()));
        // Port 1 is a privileged/unassigned port: connection should be refused promptly.
        let account = test_account("ws://127.0.0.1:1".to_string());
        let ctx = ctx_with_session("lambda");

        let err = executor
            .execute(
                prepared(json!({"model": "m", "input": [item(0)]})),
                &account,
                &ctx,
            )
            .await
            .err()
            .expect("a refused connection must surface as an error");

        assert!(
            matches!(err, ExecError::Upstream(_)),
            "generic transport failure must surface as ExecError::Upstream, unchanged, got {err:?}"
        );
        assert_eq!(
            fallback.call_count(),
            0,
            "a non-426 transport failure must NOT fall back — ground truth §5: 426 is the ONLY trigger"
        );
    }

    // ---- No session key: still correct, just no reuse -----------------------------------------

    #[tokio::test]
    async fn no_session_key_still_completes_a_turn_with_no_connection_reuse() {
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::normal(vec![]),
        ]);
        let base = mock.clone().spawn().await;
        let executor = CodexWsExecutor::new(never_called_fallback());
        let account = test_account(base);
        let ctx = RequestCtx::default(); // no session_key

        let s1 = executor
            .execute(
                prepared(json!({"model": "m", "input": [item(0)]})),
                &account,
                &ctx,
            )
            .await
            .unwrap();
        drain(s1).await;
        let s2 = executor
            .execute(
                prepared(json!({"model": "m", "input": [item(0), item(1)]})),
                &account,
                &ctx,
            )
            .await
            .unwrap();
        drain(s2).await;

        assert_eq!(
            mock.handshake_count(),
            2,
            "with no session key there is nothing to cache on — each call gets a fresh connection"
        );
        assert_eq!(
            mock.last_frame_anchor(),
            None,
            "no reuse ⇒ no anchor ⇒ always a full send"
        );
    }
}
