//! C12 Task 3: the retention-pruning background loop — age-deletes old rows from the two
//! append-only, per-event log tables (`request_log`, `usage_history`) so a long-running proxy's
//! SQLite DB does not grow unbounded. Mirrors `crate::usage_refresh::spawn_usage_refresh`'s
//! `tokio::spawn(async move { loop { ...; sleep(INTERVAL).await } })` shape exactly.
//!
//! # Global Constraints (see `docs/superpowers/plans/2026-07-18-c12-retention-pruning.md`,
//! extended by `docs/superpowers/plans/2026-07-22-continuity-owner-conflict-hardening.md`)
//! - **Prune ONLY `request_log` + `usage_history` + the two continuity tables.** This module is
//!   structurally incapable of touching `accounts`/`api_keys` — it calls exactly four repo
//!   methods, each scoped to one prunable table, and nothing else. The continuity tables joined
//!   under S3(a) directive 3 (the 2026-07-22 codex-lb incident): affinity state without a TTL
//!   becomes false ownership evidence over time.
//! - **The log tables are disabled by default.** Both `request_log_retention_days`/
//!   `usage_history_retention_days` default to `0` (see `crate::config`); `0` ⇒ that table is
//!   skipped entirely (no-op). The continuity TTL is ALWAYS ON by design — a disabled-by-default
//!   knob would recreate codex-lb's no-TTL trap (see [`CONTINUITY_TTL_DAYS`]).
//! - **Content-free.** Only row COUNTS are ever logged (`tracing::info!(deleted = n, table = ..)`)
//!   — never row content, never a usage value, never a request field.
//! - **A failed prune must never crash the task.** Each table's prune is independently
//!   `tracing::warn!`-and-continue: an error pruning `request_log` must not prevent
//!   `usage_history` from being attempted the same tick, and must never propagate out of the
//!   spawned task (which would silently kill the loop for the rest of the process's life).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::app::AppState;

/// How often the retention pruner ticks. Mirrors the plan's Task 3 spec + codex-lb's
/// `RETENTION_INTERVAL_SECONDS=3600` precedent (`job.py`/`scheduler.py`) — the two log tables grow
/// slowly enough that hourly is generous, and each tick is a batched, bounded operation anyway.
const RETENTION_INTERVAL: Duration = Duration::from_secs(3600);

/// Rows deleted per internal batch within a single table's prune (matches the plan's Task 1/2
/// `BATCH_SIZE = 10_000` precedent, and `RequestLogRepo::prune_older_than`/
/// `AccountRepo::prune_usage_history_older_than`'s own batching loop, which internally re-issues
/// this many rows per `DELETE` until a batch affects fewer than `PRUNE_BATCH_SIZE` rows).
const PRUNE_BATCH_SIZE: i64 = 10_000;

/// S3(a) directive 3 (2026-07-22 incident): the fixed, always-on TTL for continuity affinity
/// state — `continuity_sessions` idle past this (by `last_activity_at`, bumped every turn) and
/// `continuity_anchors` older than this (by `created_at`; every completed turn inserts a fresh
/// row, so a live conversation's resolvable anchor is always young). Deliberately a constant,
/// NOT a runtime setting: deletion is proven safe at any moment (see
/// `tests/continuity_owner_conflict.rs`), so the exact value is uncritical — and codex-lb's
/// incident came precisely from affinity rows that could live forever. A >30-day-idle
/// conversation that resumes degrades to an unowned pick + armed-watchdog recovery, never a
/// wedge.
const CONTINUITY_TTL_DAYS: i64 = 30;

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// The testable per-tick body: prunes `request_log` (if `state.request_log_retention_days > 0`),
/// then `usage_history` (if `state.usage_history_retention_days > 0`), then the two continuity
/// tables at the fixed always-on [`CONTINUITY_TTL_DAYS`] — each independently. The two log tables
/// no-op when their retention-days knob is `0` (the disabled default) — this function never reads
/// any OTHER table, and never issues an unbounded delete (all four repo methods batch
/// internally). A prune `Err` is logged via `tracing::warn!` (table name + error only — content-
/// free) and does NOT prevent the other tables' prunes from running, and does NOT panic or
/// propagate: the caller (`spawn_retention_prune`'s loop) can call this every tick forever without
/// the task ever dying from a transient store error.
pub async fn run_retention_pass(state: &AppState) {
    let now = unix_now();

    let request_log_retention_days = state.runtime_settings.request_log_retention_days();
    if request_log_retention_days > 0 {
        let cutoff = now - (request_log_retention_days as i64) * 86400;
        match state
            .store
            .request_log()
            .prune_older_than(cutoff, PRUNE_BATCH_SIZE)
            .await
        {
            Ok(0) => {}
            Ok(deleted) => {
                tracing::info!(deleted, table = "request_log", "retention prune");
            }
            Err(e) => {
                tracing::warn!(error = %e, table = "request_log", "retention prune failed");
            }
        }
    }

    let usage_history_retention_days = state.runtime_settings.usage_history_retention_days();
    if usage_history_retention_days > 0 {
        let cutoff = now - (usage_history_retention_days as i64) * 86400;
        match state
            .store
            .accounts()
            .prune_usage_history_older_than(cutoff, PRUNE_BATCH_SIZE)
            .await
        {
            Ok(0) => {}
            Ok(deleted) => {
                tracing::info!(deleted, table = "usage_history", "retention prune");
            }
            Err(e) => {
                tracing::warn!(error = %e, table = "usage_history", "retention prune failed");
            }
        }
    }

    // S3(a) directive 3: the always-on continuity-affinity TTL. Sessions first — its CASCADE
    // takes each pruned session's anchors with it — then the anchor sweep catches old anchors of
    // still-live sessions (superseded turns). Same independent warn-and-continue isolation.
    let cutoff = now - CONTINUITY_TTL_DAYS * 86400;
    match state
        .store
        .continuity()
        .prune_sessions_older_than(cutoff, PRUNE_BATCH_SIZE)
        .await
    {
        Ok(0) => {}
        Ok(deleted) => {
            tracing::info!(deleted, table = "continuity_sessions", "retention prune");
        }
        Err(e) => {
            tracing::warn!(error = %e, table = "continuity_sessions", "retention prune failed");
        }
    }
    match state
        .store
        .continuity()
        .prune_anchors_older_than(cutoff, PRUNE_BATCH_SIZE)
        .await
    {
        Ok(0) => {}
        Ok(deleted) => {
            tracing::info!(deleted, table = "continuity_anchors", "retention prune");
        }
        Err(e) => {
            tracing::warn!(error = %e, table = "continuity_anchors", "retention prune failed");
        }
    }
}

/// Spawn the background retention-pruning loop: every [`RETENTION_INTERVAL`], run one
/// [`run_retention_pass`]. Always spawns (even when both retention-days knobs are `0` at startup)
/// rather than conditionally skip-spawning — ticking harmlessly is simplest and cheapest (an hourly
/// no-op pass costs nothing), and it means a later live config change (a future dashboard lever,
/// per the plan's follow-ups) would not need a process restart to start taking effect, unlike a
/// skip-spawned task which would need re-spawning.
pub fn spawn_retention_prune(state: Arc<AppState>) {
    tokio::spawn(async move {
        loop {
            run_retention_pass(&state).await;
            tokio::time::sleep(RETENTION_INTERVAL).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration as StdDuration;

    use polyflare_codex::oauth::OAuthClient;
    use polyflare_codex::CodexExecutor;
    use polyflare_core::{Continuity, RoutingStrategy, Selector};
    use polyflare_store::{Account, PlainTokens, Store, TokenCipher};

    use crate::continuity::CodexContinuity;
    use crate::runtime_settings::{RuntimeSettings, RuntimeSettingsFields};

    fn account(id: &str) -> Account {
        Account {
            id: id.to_string(),
            chatgpt_account_id: None,
            chatgpt_user_id: None,
            email: format!("{id}@example.test"),
            alias: None,
            workspace_id: None,
            workspace_label: None,
            seat_type: None,
            plan_type: "pro".to_string(),
            routing_policy: "normal".to_string(),
            last_refresh: 0,
            created_at: 0,
            status: "active".to_string(),
            deactivation_reason: None,
            reset_at: None,
            blocked_at: None,
            security_work_authorized: false,
            provider: "codex".to_string(),
            pool: None,
        }
    }

    async fn seed_account(store: &Store, cipher: &TokenCipher, id: &str) {
        store
            .accounts()
            .insert(
                &account(id),
                &PlainTokens {
                    access_token: "a".into(),
                    refresh_token: "b".into(),
                    id_token: "c".into(),
                },
                cipher,
            )
            .await
            .unwrap();
    }

    /// Builds a full `AppState` for these tests, mirroring `crate::control`'s test `build_state`
    /// helper exactly (same field set, same construction pattern) — the only difference is the two
    /// retention-days fields, which each test sets explicitly.
    async fn build_state(
        request_log_retention_days: u32,
        usage_history_retention_days: u32,
    ) -> Arc<AppState> {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("s.db")).await.unwrap();
        std::mem::forget(dir);
        let cipher = TokenCipher::from_key_bytes(&[9u8; 32]).unwrap();
        let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
            store.continuity(),
            StdDuration::from_secs(30),
        ));
        let selector: Arc<dyn Selector> = RoutingStrategy::default().selector();
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
            runtime_settings: Arc::new(RuntimeSettings::new_from_fields(RuntimeSettingsFields {
                max_account_attempts: 3,
                starvation_wait_budget: StdDuration::from_secs(60),
                starvation_heartbeat: StdDuration::from_secs(10),
                wake_jitter_ms: 0,
                stream_idle_timeout: StdDuration::from_secs(300),
                inflight_penalty_pct: 2.5,
                soft_drain_enabled: true,
                request_log_retention_days,
                usage_history_retention_days,
                live_logs: false,
            })),
            ws_downstream: false,
            ws_relay_idle: crate::ws_relay::WsRelayIdlePolicy::default(),
            log_bus: crate::log_bus::LogBus::new(1000),
            failover_metrics: crate::observability::FailoverMetrics::new(),
            health_tier_metrics: crate::observability::HealthTierMetrics::new(),
            starvation_metrics: crate::observability::StarvationMetrics::new(),
            runtime: Default::default(),
            lease_metrics: crate::observability::LeaseMetrics::new(),
            upstream_request_metrics: crate::observability::UpstreamRequestMetrics::new(),
            rate_limit_metrics: crate::observability::RateLimitMetrics::new(),
            relay_metrics: crate::observability::RelayMetrics::new(),
            model_catalog: crate::model_catalog::floor_only_model_catalog(),
        })
    }

    async fn request_log_count(store: &Store) -> i64 {
        store.request_log().count().await.unwrap()
    }

    async fn usage_history_recorded_ats(store: &Store, account_id: &str) -> Vec<i64> {
        let rows: Vec<(i64,)> = sqlx::query_as(
            "SELECT recorded_at FROM usage_history WHERE account_id = ? ORDER BY recorded_at ASC",
        )
        .bind(account_id)
        .fetch_all(store.pool())
        .await
        .unwrap();
        rows.into_iter().map(|(ts,)| ts).collect()
    }

    fn rec(requested_at: i64) -> polyflare_store::RequestLogRecord {
        polyflare_store::RequestLogRecord {
            requested_at,
            provider: "codex".into(),
            method: "POST".into(),
            path: "/responses".into(),
            aliased: false,
            status: 200,
            duration_ms: 100,
            account_id: Some("acct-1".into()),
            target_kind: Some("account".into()),
            provider_credential_id: None,
            model: Some("gpt-5.6-sol".into()),
            upstream_model: None,
            upstream_transport: Some("http_sse".into()),
            reasoning_effort: None,
            service_tier: None,
            transport: Some("http".into()),
            ttft_ms: None,
            total_tokens: None,
            cached_tokens: None,
            subagent: None,
            request_id: None,
            session_key: None,
            input_tokens: None,
            output_tokens: None,
            cached_input_tokens: None,
            reasoning_tokens: None,
            orchestration_input_tokens: None,
            orchestration_output_tokens: None,
            orchestration_cached_input_tokens: None,
            cost_usd: None,
            latency_first_token_ms: None,
            protocol_outcome: None,
        }
    }

    /// (b) THE direct test of `run_retention_pass`: with both retention-days knobs > 0, seeded old
    /// + new rows in BOTH tables (usage_history's old rows include a protected-latest one), one
    /// pass deletes exactly the old `request_log` rows and the old (non-latest) `usage_history`
    /// rows, while new rows AND the protected latest usage row per window all survive.
    #[tokio::test]
    async fn run_retention_pass_prunes_old_rows_and_protects_latest_usage_row() {
        let now = unix_now();
        let cutoff_request_log = now - 30 * 86400; // 30-day retention
        let cutoff_usage_history = now - 45 * 86400; // 45-day retention
        let state = build_state(30, 45).await;
        seed_account(&state.store, &state.cipher, "acct-1").await;

        // request_log: one clearly-old row, one clearly-new row.
        let old_ts = cutoff_request_log - 1000;
        let new_ts = now - 10;
        state
            .store
            .request_log()
            .insert(&rec(old_ts))
            .await
            .unwrap();
        state
            .store
            .request_log()
            .insert(&rec(new_ts))
            .await
            .unwrap();

        // usage_history: an idle account whose only "primary" rows are ALL older than the
        // usage_history cutoff — the newest of them must be PROTECTED (never pruned), plus one
        // unrelated "secondary"-window row well within the retention window (survives on age
        // alone, not because of the guard).
        let old1 = cutoff_usage_history - 300;
        let old2 = cutoff_usage_history - 200;
        let protected_latest = cutoff_usage_history - 100; // still < cutoff, but the group's max
        let fresh_secondary = now - 10;
        state
            .store
            .accounts()
            .insert_usage_window("acct-1", "primary", 10.0, None, None, old1)
            .await
            .unwrap();
        state
            .store
            .accounts()
            .insert_usage_window("acct-1", "primary", 20.0, None, None, old2)
            .await
            .unwrap();
        state
            .store
            .accounts()
            .insert_usage_window("acct-1", "primary", 30.0, None, None, protected_latest)
            .await
            .unwrap();
        state
            .store
            .accounts()
            .insert_usage_window("acct-1", "secondary", 40.0, None, None, fresh_secondary)
            .await
            .unwrap();

        run_retention_pass(&state).await;

        assert_eq!(
            request_log_count(&state.store).await,
            1,
            "the old request_log row is pruned, the new one survives"
        );

        let remaining_primary = usage_history_recorded_ats(&state.store, "acct-1").await;
        assert!(
            remaining_primary.contains(&protected_latest),
            "the latest primary-window row survives even though it's older than cutoff (guard)"
        );
        assert!(
            remaining_primary.contains(&fresh_secondary),
            "the fresh secondary-window row survives on age alone"
        );
        assert!(
            !remaining_primary.contains(&old1) && !remaining_primary.contains(&old2),
            "the two older, non-latest primary rows are pruned"
        );
        assert_eq!(
            remaining_primary.len(),
            2,
            "exactly the protected-latest primary row + the fresh secondary row survive"
        );
    }

    /// (c) With BOTH retention-days knobs at `0` (the disabled default), `run_retention_pass`
    /// deletes NOTHING from either table — proving the disable lever is a true no-op, not merely
    /// "prunes very little."
    #[tokio::test]
    async fn run_retention_pass_with_both_knobs_zero_deletes_nothing() {
        let now = unix_now();
        let state = build_state(0, 0).await;
        seed_account(&state.store, &state.cipher, "acct-1").await;

        // Seed rows far older than any sane cutoff would allow — if the disable lever were
        // leaky, these would be the first to go.
        let ancient = now - 3650 * 86400 - 1;
        state
            .store
            .request_log()
            .insert(&rec(ancient))
            .await
            .unwrap();
        state
            .store
            .accounts()
            .insert_usage_window("acct-1", "primary", 10.0, None, None, ancient)
            .await
            .unwrap();

        run_retention_pass(&state).await;

        assert_eq!(
            request_log_count(&state.store).await,
            1,
            "disabled (0) ⇒ request_log untouched"
        );
        assert_eq!(
            usage_history_recorded_ats(&state.store, "acct-1").await,
            vec![ancient],
            "disabled (0) ⇒ usage_history untouched"
        );
    }

    /// Only `request_log_retention_days` enabled: `usage_history` is left completely untouched
    /// even though it has old, non-latest rows that WOULD be pruned if its own knob were on —
    /// proving the two tables' pruning is independently gated, not an all-or-nothing switch.
    #[tokio::test]
    async fn run_retention_pass_request_log_only_leaves_usage_history_untouched() {
        let now = unix_now();
        let cutoff = now - 30 * 86400;
        let state = build_state(30, 0).await;
        seed_account(&state.store, &state.cipher, "acct-1").await;

        state
            .store
            .request_log()
            .insert(&rec(cutoff - 1000))
            .await
            .unwrap();
        state
            .store
            .accounts()
            .insert_usage_window("acct-1", "primary", 10.0, None, None, cutoff - 1000)
            .await
            .unwrap();
        state
            .store
            .accounts()
            .insert_usage_window("acct-1", "primary", 20.0, None, None, cutoff - 500)
            .await
            .unwrap();

        run_retention_pass(&state).await;

        assert_eq!(request_log_count(&state.store).await, 0);
        assert_eq!(
            usage_history_recorded_ats(&state.store, "acct-1")
                .await
                .len(),
            2,
            "usage_history's own knob is 0 ⇒ both rows survive, even the non-latest one"
        );
    }

    /// S3(a) directive 3: the continuity-affinity TTL is ALWAYS ON — even with both log-table
    /// knobs at `0`, one pass prunes a `> CONTINUITY_TTL_DAYS`-idle session (its anchor CASCADEs
    /// away) while an active session and its anchor survive untouched.
    #[tokio::test]
    async fn run_retention_pass_always_prunes_idle_continuity_state() {
        let now = unix_now();
        let state = build_state(0, 0).await;
        seed_account(&state.store, &state.cipher, "acct-1").await;
        let repo = state.store.continuity();

        let idle_ts = now - (CONTINUITY_TTL_DAYS + 1) * 86400;
        repo.ensure_session("idle", "soft", idle_ts).await.unwrap();
        repo.record_completion("idle", "soft", "acct-1", "resp_idle", "fp", 1, idle_ts)
            .await
            .unwrap();
        repo.ensure_session("active", "soft", now - 10)
            .await
            .unwrap();
        repo.record_completion("active", "soft", "acct-1", "resp_active", "fp", 1, now - 10)
            .await
            .unwrap();

        run_retention_pass(&state).await;

        assert!(
            repo.get_session("idle").await.unwrap().is_none(),
            "a TTL-expired affinity row is pruned even with both log knobs disabled"
        );
        assert!(
            repo.get_anchor_owner("resp_idle").await.unwrap().is_none(),
            "the idle session's anchor cascaded away"
        );
        assert!(repo.get_session("active").await.unwrap().is_some());
        assert_eq!(
            repo.get_anchor_owner("resp_active")
                .await
                .unwrap()
                .as_deref(),
            Some("acct-1"),
            "the active conversation's ownership evidence is untouched"
        );
    }
}
