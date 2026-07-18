//! C11: a pure, content-free Prometheus text-exposition renderer (SPEC/`PORTING-CODEXLB.md:196-222`).
//!
//! [`render_prometheus_text`] takes plain, already-content-free values — never `AppState`, never a
//! store row — so it is unit-testable without a server and structurally cannot leak email/token/
//! session content: the types it accepts ([`MetricsSnapshot`]/[`AccountMetric`]) simply don't carry
//! those fields. The HTTP wiring (`GET /metrics`, admin-gated) that builds a [`MetricsSnapshot`] from
//! live `AppState` is a separate task/module; this file is the render step only.
//!
//! Output is valid Prometheus 0.0.4 text exposition format: `# HELP <name> <help>` and
//! `# TYPE <name> counter|gauge` lines precede a metric family's samples, emitted exactly ONCE per
//! family (not once per sample) — repeating HELP/TYPE per-account line would be invalid exposition
//! text. Label values are escaped per the format's rules (`\` → `\\`, `"` → `\"`, newline → `\n`).

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;

use crate::app::AppState;

/// A plain-data snapshot of everything `/metrics` renders — the 5 process-wide counters (4 read
/// directly off `AppState`'s metric structs, `polyflare_lease_inflight` derived) plus one
/// [`AccountMetric`] per pool account, from the existing overlaid `AccountSnapshot` read path. Pure
/// data: no `AppState`, no store handle, no async — the render step ([`render_prometheus_text`]) is
/// fully synchronous and unit-testable.
pub struct MetricsSnapshot {
    pub failover_total: u64,
    pub starvation_total: u64,
    pub health_tier_transitions_total: u64,
    pub lease_acquired_total: u64,
    pub lease_released_total: u64,
    pub accounts: Vec<AccountMetric>,
    /// C11b: one `(account_id, status, count)` triple per distinct pair recorded by
    /// `AppState::upstream_request_metrics` (`crate::observability::UpstreamRequestMetrics`) — a
    /// content-free, monotonic, in-process counter bumped once per completed client request at
    /// each of the 3 request-completion wrapper sites. `account_id == ""` is the documented
    /// `None` (no-eligible-account) render convention, mirroring `AccountMetric::pool`'s
    /// `None -> ""` treatment.
    pub upstream_requests: Vec<(String, u16, u64)>,
    /// C11b: one `(type, count)` pair per distinct rate-limit kind recorded by
    /// `AppState::rate_limit_metrics` (`crate::observability::RateLimitMetrics`) — bumped once per
    /// 429 writeback inside `RuntimeStates::record_rate_limit`, the single chokepoint. `type` is
    /// always one of the fixed strings `"upstream"` (upstream supplied `Retry-After`) or
    /// `"backoff"` (computed backoff) — never free-form upstream text.
    pub rate_limit_hits: Vec<(String, u64)>,
}

/// One account's content-free gauge inputs. `account_id` is the OPAQUE store-row id (the same class
/// `RequestLog::account_id`/`FailoverSignal`'s ids already treat as loggable, see
/// `crate::observability`) — NEVER an email address, token, or session identifier; this type simply
/// has no field to carry those, so a caller cannot accidentally leak them through here.
pub struct AccountMetric {
    pub account_id: String,
    pub status: String,
    pub provider: String,
    pub pool: Option<String>,
    pub in_flight: u32,
    pub error_count: u32,
    pub health_tier: u8,
    pub cooldown_active: bool,
}

/// Escapes a Prometheus label value per the text-exposition format's rules: backslash and double
/// quote are backslash-escaped, and a literal newline is rendered as the two-character `\n` escape.
/// Applied to every label value this module emits — `account_id` is opaque today, but the escape is
/// defensive (never assume an upstream id is free of these characters).
fn escape_label_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

/// Writes one process-wide counter/gauge family: a `# HELP`, a `# TYPE ... <kind>`, then exactly one
/// sample line `name value`. Used for the 5 process metrics (4 real counters + the derived
/// `polyflare_lease_inflight` gauge), each of which has exactly one sample and no labels.
fn write_scalar_metric(out: &mut String, name: &str, help: &str, kind: &str, value: u64) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
    let _ = writeln!(out, "{name} {value}");
}

/// Renders a [`MetricsSnapshot`] as Prometheus 0.0.4 text exposition format. Pure function: same
/// input always produces the same output string, no I/O, no clock read. See the module doc for the
/// content-safety argument and the exposition-format contract (HELP/TYPE once per family).
pub fn render_prometheus_text(snapshot: &MetricsSnapshot) -> String {
    let mut out = String::new();

    write_scalar_metric(
        &mut out,
        "polyflare_failover_total",
        "Total cross-account failover events actually taken.",
        "counter",
        snapshot.failover_total,
    );
    write_scalar_metric(
        &mut out,
        "polyflare_starvation_total",
        "Total Layer 2 keepalive-wait terminal outcomes.",
        "counter",
        snapshot.starvation_total,
    );
    write_scalar_metric(
        &mut out,
        "polyflare_health_tier_transitions_total",
        "Total health-tier soft-drain transitions actually applied.",
        "counter",
        snapshot.health_tier_transitions_total,
    );
    write_scalar_metric(
        &mut out,
        "polyflare_lease_acquired_total",
        "Total in-flight lease acquisitions.",
        "counter",
        snapshot.lease_acquired_total,
    );
    write_scalar_metric(
        &mut out,
        "polyflare_lease_released_total",
        "Total in-flight lease releases.",
        "counter",
        snapshot.lease_released_total,
    );
    write_scalar_metric(
        &mut out,
        "polyflare_lease_inflight",
        "Derived instantaneous in-flight lease count (acquired minus released, saturating at 0).",
        "gauge",
        snapshot
            .lease_acquired_total
            .saturating_sub(snapshot.lease_released_total),
    );

    write_account_gauge(
        &mut out,
        &snapshot.accounts,
        "polyflare_account_inflight",
        "Current in-flight request count per account.",
        |a| a.in_flight as u64,
    );
    write_account_gauge(
        &mut out,
        &snapshot.accounts,
        "polyflare_account_error_count",
        "Current consecutive error count per account.",
        |a| a.error_count as u64,
    );
    write_account_gauge(
        &mut out,
        &snapshot.accounts,
        "polyflare_account_health_tier",
        "Current health tier per account (0=HEALTHY, 1=DRAINING, 2=PROBING).",
        |a| a.health_tier as u64,
    );
    write_account_gauge(
        &mut out,
        &snapshot.accounts,
        "polyflare_account_cooldown_active",
        "Whether the account is currently in cooldown (1) or not (0).",
        |a| u64::from(a.cooldown_active),
    );

    write_accounts_total(&mut out, &snapshot.accounts);

    write_upstream_requests_total(&mut out, &snapshot.upstream_requests);
    write_rate_limit_hits_total(&mut out, &snapshot.rate_limit_hits);

    out
}

/// Writes one per-account gauge family: a single `# HELP`/`# TYPE` pair, then one sample line per
/// account with the standard 4-label set (`account_id`, `status`, `provider`, `pool`). `pool: None`
/// renders as the empty label value `pool=""` (standard Prometheus practice for an absent label).
fn write_account_gauge(
    out: &mut String,
    accounts: &[AccountMetric],
    name: &str,
    help: &str,
    value_of: impl Fn(&AccountMetric) -> u64,
) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    for account in accounts {
        let pool = account.pool.as_deref().unwrap_or("");
        let _ = writeln!(
            out,
            "{name}{{account_id=\"{}\",status=\"{}\",provider=\"{}\",pool=\"{}\"}} {}",
            escape_label_value(&account.account_id),
            escape_label_value(&account.status),
            escape_label_value(&account.provider),
            escape_label_value(pool),
            value_of(account),
        );
    }
}

/// Writes `polyflare_accounts_total{status="..."}` — one sample per distinct status value, counting
/// how many accounts currently report that status. Order is insertion order of first appearance
/// (deterministic given the input `accounts` slice's order), which keeps rendered output stable and
/// test-friendly without needing a sorted-map dependency.
fn write_accounts_total(out: &mut String, accounts: &[AccountMetric]) {
    let name = "polyflare_accounts_total";
    let _ = writeln!(out, "# HELP {name} Current account count per status.");
    let _ = writeln!(out, "# TYPE {name} gauge");

    let mut counts: Vec<(String, u64)> = Vec::new();
    for account in accounts {
        match counts
            .iter_mut()
            .find(|(status, _)| status == &account.status)
        {
            Some((_, count)) => *count += 1,
            None => counts.push((account.status.clone(), 1)),
        }
    }

    for (status, count) in counts {
        let _ = writeln!(
            out,
            "{name}{{status=\"{}\"}} {count}",
            escape_label_value(&status)
        );
    }
}

/// Writes `polyflare_upstream_requests_total{account_id="...",status="..."}` — one sample per
/// `(account_id, status, count)` triple in `entries` (C11b). A single `# HELP`/`# TYPE ... counter`
/// pair precedes all samples, emitted once for the whole family (never once per sample). `status`
/// is rendered as its decimal `u16` value (a numeric HTTP status code, not free text); `account_id`
/// goes through [`escape_label_value`] like every other label this module emits — the empty-string
/// `None` convention renders as the valid, empty label `account_id=""`.
fn write_upstream_requests_total(out: &mut String, entries: &[(String, u16, u64)]) {
    let name = "polyflare_upstream_requests_total";
    let _ = writeln!(
        out,
        "# HELP {name} Total completed client requests per (account_id, status)."
    );
    let _ = writeln!(out, "# TYPE {name} counter");
    for (account_id, status, count) in entries {
        let _ = writeln!(
            out,
            "{name}{{account_id=\"{}\",status=\"{status}\"}} {count}",
            escape_label_value(account_id),
        );
    }
}

/// Writes `polyflare_rate_limit_hits_total{type="..."}` — one sample per `(type, count)` pair in
/// `entries` (C11b). A single `# HELP`/`# TYPE ... counter` pair precedes all samples, emitted once
/// for the whole family. `type` is always one of the fixed `&'static str` values `RateLimitMetrics`
/// records (`"upstream"`/`"backoff"`) but is still escaped via [`escape_label_value`] for
/// consistency with every other label this module emits.
fn write_rate_limit_hits_total(out: &mut String, entries: &[(String, u64)]) {
    let name = "polyflare_rate_limit_hits_total";
    let _ = writeln!(out, "# HELP {name} Total 429 writebacks per type.");
    let _ = writeln!(out, "# TYPE {name} counter");
    for (kind, count) in entries {
        let _ = writeln!(
            out,
            "{name}{{type=\"{}\"}} {count}",
            escape_label_value(kind),
        );
    }
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// `GET /metrics` — admin-gated (see `crate::app::build_app`'s route registration and
/// `crate::auth::require_admin`, whose `route_layer` is what actually enforces this — this
/// handler itself performs no auth check of its own). Reads the 4 process-wide counters directly
/// off `AppState`'s existing metric structs, and builds one [`AccountMetric`] per pool account
/// from the SAME overlaid-snapshot read path the dashboard's `/api/pools`/`/api/overview` read
/// (`state.account_cache.snapshots(&state.store)` + `state.runtime.overlay`) — never the store's
/// account row, so the `AccountId`-only [`AccountMetric`] this maps into structurally cannot carry
/// an email or token (see this module's top doc). A store-read error collapses to a generic `500`,
/// mirroring `crate::read_api`'s `Response::error()` idiom — the store error's own text is never
/// surfaced.
pub async fn metrics_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let now = unix_now();

    let snapshots = match state.account_cache.snapshots(&state.store).await {
        Ok(s) => s,
        Err(_) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
        }
    };
    // The cache's `Arc<Vec<..>>` is shared; the runtime overlay mutates in place, so clone the
    // slice into an owned `Vec` first — the same pattern `read_api`'s handlers use before
    // `state.runtime.overlay`.
    let mut snapshots = (*snapshots).clone();
    state.runtime.overlay(&mut snapshots, now);

    let accounts = snapshots
        .iter()
        .map(|snap| AccountMetric {
            // OPAQUE store-row id only — `AccountSnapshot` has no email/token field to leak.
            account_id: snap.id.as_str().to_string(),
            status: snap.status.clone(),
            provider: snap.provider.to_string(),
            pool: snap.pool.clone(),
            in_flight: snap.in_flight,
            error_count: snap.error_count,
            health_tier: snap.health_tier,
            cooldown_active: snap.cooldown_until.is_some_and(|c| now < c),
        })
        .collect();

    let snapshot = MetricsSnapshot {
        failover_total: state.failover_metrics.total(),
        starvation_total: state.starvation_metrics.total(),
        health_tier_transitions_total: state.health_tier_metrics.total(),
        lease_acquired_total: state.lease_metrics.acquired(),
        lease_released_total: state.lease_metrics.released(),
        accounts,
        upstream_requests: state.upstream_request_metrics.snapshot(),
        rate_limit_hits: state.rate_limit_metrics.snapshot(),
    };

    let body = render_prometheus_text(&snapshot);
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account(id: &str, status: &str) -> AccountMetric {
        AccountMetric {
            account_id: id.to_string(),
            status: status.to_string(),
            provider: "codex".to_string(),
            pool: None,
            in_flight: 0,
            error_count: 0,
            health_tier: 0,
            cooldown_active: false,
        }
    }

    /// (a) Given a snapshot with known process counters and 2 accounts, the output contains the
    /// exact `# TYPE ... counter` lines, the counter value lines, and per-account gauge lines with
    /// the right labels and values.
    #[test]
    fn renders_process_counters_and_per_account_gauges() {
        let snapshot = MetricsSnapshot {
            failover_total: 3,
            starvation_total: 5,
            health_tier_transitions_total: 7,
            lease_acquired_total: 10,
            lease_released_total: 4,
            accounts: vec![
                AccountMetric {
                    account_id: "acct-a".to_string(),
                    status: "active".to_string(),
                    provider: "codex".to_string(),
                    pool: Some("fast".to_string()),
                    in_flight: 2,
                    error_count: 1,
                    health_tier: 0,
                    cooldown_active: false,
                },
                AccountMetric {
                    account_id: "acct-b".to_string(),
                    status: "cooldown".to_string(),
                    provider: "claude".to_string(),
                    pool: None,
                    in_flight: 0,
                    error_count: 4,
                    health_tier: 1,
                    cooldown_active: true,
                },
            ],
            upstream_requests: vec![],
            rate_limit_hits: vec![],
        };

        let body = render_prometheus_text(&snapshot);

        assert!(body.contains("# TYPE polyflare_failover_total counter"));
        assert!(body.contains("polyflare_failover_total 3"));
        assert!(body.contains("# TYPE polyflare_starvation_total counter"));
        assert!(body.contains("polyflare_starvation_total 5"));
        assert!(body.contains("# TYPE polyflare_health_tier_transitions_total counter"));
        assert!(body.contains("polyflare_health_tier_transitions_total 7"));
        assert!(body.contains("# TYPE polyflare_lease_acquired_total counter"));
        assert!(body.contains("polyflare_lease_acquired_total 10"));
        assert!(body.contains("# TYPE polyflare_lease_released_total counter"));
        assert!(body.contains("polyflare_lease_released_total 4"));

        assert!(body.contains("# TYPE polyflare_account_inflight gauge"));
        assert!(body.contains(
            "polyflare_account_inflight{account_id=\"acct-a\",status=\"active\",provider=\"codex\",pool=\"fast\"} 2"
        ));
        assert!(body.contains(
            "polyflare_account_inflight{account_id=\"acct-b\",status=\"cooldown\",provider=\"claude\",pool=\"\"} 0"
        ));

        assert!(body.contains("# TYPE polyflare_account_error_count gauge"));
        assert!(body.contains(
            "polyflare_account_error_count{account_id=\"acct-a\",status=\"active\",provider=\"codex\",pool=\"fast\"} 1"
        ));
        assert!(body.contains(
            "polyflare_account_error_count{account_id=\"acct-b\",status=\"cooldown\",provider=\"claude\",pool=\"\"} 4"
        ));

        assert!(body.contains("# TYPE polyflare_account_health_tier gauge"));
        assert!(body.contains(
            "polyflare_account_health_tier{account_id=\"acct-a\",status=\"active\",provider=\"codex\",pool=\"fast\"} 0"
        ));
        assert!(body.contains(
            "polyflare_account_health_tier{account_id=\"acct-b\",status=\"cooldown\",provider=\"claude\",pool=\"\"} 1"
        ));

        assert!(body.contains("# TYPE polyflare_account_cooldown_active gauge"));
        assert!(body.contains(
            "polyflare_account_cooldown_active{account_id=\"acct-a\",status=\"active\",provider=\"codex\",pool=\"fast\"} 0"
        ));
        assert!(body.contains(
            "polyflare_account_cooldown_active{account_id=\"acct-b\",status=\"cooldown\",provider=\"claude\",pool=\"\"} 1"
        ));

        // Each metric family's HELP/TYPE appears exactly once, never repeated per account.
        assert_eq!(
            body.matches("# TYPE polyflare_account_inflight gauge")
                .count(),
            1,
            "HELP/TYPE must be emitted once per family, not once per account"
        );
    }

    /// (b) `polyflare_lease_inflight` == acquired - released, saturating at 0.
    #[test]
    fn lease_inflight_is_derived_from_acquired_minus_released() {
        let snapshot = MetricsSnapshot {
            failover_total: 0,
            starvation_total: 0,
            health_tier_transitions_total: 0,
            lease_acquired_total: 10,
            lease_released_total: 4,
            accounts: vec![],
            upstream_requests: vec![],
            rate_limit_hits: vec![],
        };
        let body = render_prometheus_text(&snapshot);
        assert!(body.contains("# TYPE polyflare_lease_inflight gauge"));
        assert!(body.contains("polyflare_lease_inflight 6"));
    }

    #[test]
    fn lease_inflight_saturates_at_zero_when_released_exceeds_acquired() {
        let snapshot = MetricsSnapshot {
            failover_total: 0,
            starvation_total: 0,
            health_tier_transitions_total: 0,
            lease_acquired_total: 2,
            lease_released_total: 9,
            accounts: vec![],
            upstream_requests: vec![],
            rate_limit_hits: vec![],
        };
        let body = render_prometheus_text(&snapshot);
        assert!(body.contains("polyflare_lease_inflight 0"));
    }

    /// (c) `polyflare_accounts_total{status="..."}` aggregates the count of accounts per distinct
    /// status correctly.
    #[test]
    fn accounts_total_aggregates_per_status() {
        let snapshot = MetricsSnapshot {
            failover_total: 0,
            starvation_total: 0,
            health_tier_transitions_total: 0,
            lease_acquired_total: 0,
            lease_released_total: 0,
            accounts: vec![
                account("acct-a", "active"),
                account("acct-b", "active"),
                account("acct-c", "cooldown"),
            ],
            upstream_requests: vec![],
            rate_limit_hits: vec![],
        };
        let body = render_prometheus_text(&snapshot);
        assert!(body.contains("# TYPE polyflare_accounts_total gauge"));
        assert!(body.contains("polyflare_accounts_total{status=\"active\"} 2"));
        assert!(body.contains("polyflare_accounts_total{status=\"cooldown\"} 1"));
    }

    /// (d) Content-safety: a snapshot whose account_id is a plain id renders no `@`/email/SECRET
    /// substring. The renderer only ever sees `MetricsSnapshot`/`AccountMetric` fields — which by
    /// design carry no email/token field — so this is a structural guarantee this test documents,
    /// not a filter the renderer applies.
    #[test]
    fn rendered_body_never_contains_email_or_secret_substrings() {
        let snapshot = MetricsSnapshot {
            failover_total: 1,
            starvation_total: 1,
            health_tier_transitions_total: 1,
            lease_acquired_total: 1,
            lease_released_total: 1,
            accounts: vec![account("acct-opaque-row-id-123", "active")],
            upstream_requests: vec![("acct-opaque-row-id-123".to_string(), 200, 1)],
            rate_limit_hits: vec![("upstream".to_string(), 1)],
        };
        let body = render_prometheus_text(&snapshot);
        assert!(!body.contains('@'), "body must never contain an @: {body}");
        assert!(
            !body.to_uppercase().contains("SECRET"),
            "body must never contain SECRET: {body}"
        );
        assert!(
            !body.to_lowercase().contains("email"),
            "body must never contain 'email': {body}"
        );
    }

    /// (e) Label escaping: an account_id containing `"` or `\` is escaped per Prometheus rules.
    #[test]
    fn label_values_are_escaped() {
        let snapshot = MetricsSnapshot {
            failover_total: 0,
            starvation_total: 0,
            health_tier_transitions_total: 0,
            lease_acquired_total: 0,
            lease_released_total: 0,
            accounts: vec![account("acct-\"quote\"-\\backslash\\", "active")],
            upstream_requests: vec![],
            rate_limit_hits: vec![],
        };
        let body = render_prometheus_text(&snapshot);
        assert!(
            body.contains("account_id=\"acct-\\\"quote\\\"-\\\\backslash\\\\\""),
            "expected escaped label value in: {body}"
        );
        // The raw unescaped form must never appear standalone inside a label value.
        assert!(!body.contains("account_id=\"acct-\"quote\""));
    }

    #[test]
    fn newline_in_label_value_is_escaped() {
        assert_eq!(escape_label_value("a\nb"), "a\\nb");
        assert_eq!(escape_label_value("a\"b\\c"), "a\\\"b\\\\c");
    }

    /// (f) Empty account list ⇒ valid output: just the process counters, no account lines, no
    /// panic.
    #[test]
    fn empty_account_list_renders_process_counters_only_without_panicking() {
        let snapshot = MetricsSnapshot {
            failover_total: 1,
            starvation_total: 2,
            health_tier_transitions_total: 3,
            lease_acquired_total: 4,
            lease_released_total: 1,
            accounts: vec![],
            upstream_requests: vec![],
            rate_limit_hits: vec![],
        };
        let body = render_prometheus_text(&snapshot);

        assert!(body.contains("polyflare_failover_total 1"));
        assert!(body.contains("polyflare_starvation_total 2"));
        assert!(body.contains("polyflare_health_tier_transitions_total 3"));
        assert!(body.contains("polyflare_lease_acquired_total 4"));
        assert!(body.contains("polyflare_lease_released_total 1"));
        assert!(body.contains("polyflare_lease_inflight 3"));

        // The per-account gauge families still emit their HELP/TYPE header (a valid, empty metric
        // family is legal Prometheus text), but no sample lines and no account_id label anywhere.
        assert!(!body.contains("account_id="));

        // accounts_total has no per-status samples when there are no accounts.
        assert!(body.contains("# TYPE polyflare_accounts_total gauge"));
        assert!(!body.contains("polyflare_accounts_total{status="));

        // The two C11b counter families still emit their HELP/TYPE header even when empty (a
        // valid, zero-sample metric family is legal Prometheus text), but no sample lines.
        assert!(body.contains("# TYPE polyflare_upstream_requests_total counter"));
        assert!(!body.contains("polyflare_upstream_requests_total{"));
        assert!(body.contains("# TYPE polyflare_rate_limit_hits_total counter"));
        assert!(!body.contains("polyflare_rate_limit_hits_total{"));
    }

    /// (a) C11b: `render_prometheus_text` with seeded `upstream_requests`/`rate_limit_hits` vecs
    /// emits the exact `# TYPE polyflare_upstream_requests_total counter` line ONCE, one labeled
    /// sample line per `(account_id, status, count)` entry, and the mirrored
    /// `polyflare_rate_limit_hits_total{type="..."}` family — HELP/TYPE appear once per family, not
    /// once per sample, exactly like every other family this renderer emits.
    #[test]
    fn renders_upstream_requests_and_rate_limit_hits_counter_families() {
        let snapshot = MetricsSnapshot {
            failover_total: 0,
            starvation_total: 0,
            health_tier_transitions_total: 0,
            lease_acquired_total: 0,
            lease_released_total: 0,
            accounts: vec![],
            upstream_requests: vec![
                ("acct-a".to_string(), 200, 5),
                ("acct-b".to_string(), 429, 2),
                // The `None` (no-eligible-account) render convention: empty account_id.
                (String::new(), 503, 1),
            ],
            rate_limit_hits: vec![("upstream".to_string(), 3), ("backoff".to_string(), 1)],
        };

        let body = render_prometheus_text(&snapshot);

        assert_eq!(
            body.matches("# TYPE polyflare_upstream_requests_total counter")
                .count(),
            1,
            "HELP/TYPE must be emitted once per family, not once per sample: {body}"
        );
        assert_eq!(
            body.matches("# HELP polyflare_upstream_requests_total")
                .count(),
            1,
            "HELP must be emitted once per family: {body}"
        );
        assert!(body
            .contains("polyflare_upstream_requests_total{account_id=\"acct-a\",status=\"200\"} 5"));
        assert!(body
            .contains("polyflare_upstream_requests_total{account_id=\"acct-b\",status=\"429\"} 2"));
        assert!(
            body.contains("polyflare_upstream_requests_total{account_id=\"\",status=\"503\"} 1"),
            "the None account_id convention must render as the valid empty label: {body}"
        );

        assert_eq!(
            body.matches("# TYPE polyflare_rate_limit_hits_total counter")
                .count(),
            1,
            "HELP/TYPE must be emitted once per family, not once per sample: {body}"
        );
        assert_eq!(
            body.matches("# HELP polyflare_rate_limit_hits_total")
                .count(),
            1,
            "HELP must be emitted once per family: {body}"
        );
        assert!(body.contains("polyflare_rate_limit_hits_total{type=\"upstream\"} 3"));
        assert!(body.contains("polyflare_rate_limit_hits_total{type=\"backoff\"} 1"));
    }

    /// (b) Content-safety: a snapshot whose `upstream_requests`/`rate_limit_hits` entries carry only
    /// plain, opaque values renders no `@`/email/SECRET substring — same structural guarantee as
    /// `rendered_body_never_contains_email_or_secret_substrings` (`(d)` above), which was itself
    /// extended to seed a populated `upstream_requests`/`rate_limit_hits` and assert nothing new
    /// leaks through them.
    #[test]
    fn upstream_and_rate_limit_families_never_contain_email_or_secret_substrings() {
        let snapshot = MetricsSnapshot {
            failover_total: 0,
            starvation_total: 0,
            health_tier_transitions_total: 0,
            lease_acquired_total: 0,
            lease_released_total: 0,
            accounts: vec![],
            upstream_requests: vec![("acct-opaque-row-id-123".to_string(), 200, 1)],
            rate_limit_hits: vec![("upstream".to_string(), 1)],
        };
        let body = render_prometheus_text(&snapshot);
        assert!(!body.contains('@'), "body must never contain an @: {body}");
        assert!(
            !body.to_uppercase().contains("SECRET"),
            "body must never contain SECRET: {body}"
        );
        assert!(
            !body.to_lowercase().contains("email"),
            "body must never contain 'email': {body}"
        );
    }
}
