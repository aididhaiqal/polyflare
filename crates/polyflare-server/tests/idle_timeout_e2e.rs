//! Stream-idle-timeout plan (`docs/superpowers/plans/2026-07-18-stream-idle-timeout.md`) Task 2:
//! e2e proof that the CONFIGURED `AppState.stream_idle_timeout` value — not just Task 1's bare
//! mechanism — actually reaches `ObservingStream` on a real HTTP round-trip through `build_app`.
//! Task 1's own suite (`watchdog::tests`) proves the mechanism works given a `Duration` parameter;
//! these tests prove the WIRING: two different `AppState.stream_idle_timeout` values, against the
//! identical byte-then-stall upstream, produce two different observed client behaviors purely as a
//! function of that one field — the only way that's possible is if the value genuinely flows
//! AppState → ingress call site → `wrap_stream` → `ObservingStream`'s `IdleDeadline`.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use polyflare_testkit::MockUpstream;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn store_account(id: &str) -> Account {
    Account {
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

/// Spawns a real PolyFlare server (via `build_app`) against `upstream`, seeded with one active
/// account, with `AppState.stream_idle_timeout` set to `idle` — the exact field
/// `crate::config::stream_idle_timeout_secs_from_env`/`ServeConfig::from_env` resolves at startup
/// and `main.rs` threads into `AppState`. This test constructs it directly (rather than going
/// through env vars) per the plan's own suggested e2e shape — the point under test is that THIS
/// FIELD reaches `ObservingStream`, not that env-var parsing works (that's `config.rs`'s own unit
/// tests, covered separately).
async fn spawn_polyflare(upstream: String, idle: Duration) -> String {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[11u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &store_account("idle-e2e"),
            &PlainTokens {
                access_token: "tok".to_string(),
                refresh_token: "r".to_string(),
                id_token: "i".to_string(),
            },
            &cipher,
        )
        .await
        .unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    std::mem::forget(dir);

    let state = Arc::new(AppState {
        enforce_client_keys: false,
        codex_executor: Arc::new(CodexExecutor::new().unwrap()),
        control_client: polyflare_codex::build_client().expect("build control_client"),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: upstream,
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        runtime: Default::default(),
        admin_token: None,
        live_logs: false,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        max_account_attempts: 3,
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        starvation_wait_budget: Duration::from_secs(60),
        starvation_heartbeat: Duration::from_secs(10),
        wake_jitter_ms: 0,
        inflight_penalty_pct: 2.5,
        lease_metrics: polyflare_server::observability::LeaseMetrics::new(),

        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        // The field under test: everything else here is boilerplate identical to every other
        // e2e harness in this crate (see `tests/e2e_passthrough.rs`).
        stream_idle_timeout: idle,
        soft_drain_enabled: true,
    });
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// A SHORT configured idle (200ms) against a byte-then-stall upstream: the client must receive its
/// byte, then a BOUNDED terminal — never a hang — within roughly the configured window. Bounded by
/// an outer `tokio::time::timeout(5s, ...)` so a regression (the field silently not reaching
/// `ObservingStream`, i.e. still using the old 300s `DEFAULT_STREAM_IDLE_TIMEOUT` placeholder)
/// fails fast as "Elapsed", never as an actual CI hang.
#[tokio::test]
async fn configured_idle_timeout_terminates_a_stalled_upstream_within_the_bound() {
    let idle = Duration::from_millis(200);
    let mock = MockUpstream::stall_after_first(
        r#"{"type":"response.output_text.delta","delta":"first-byte"}"#,
    );
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(upstream, idle).await;

    let client = reqwest::Client::new();
    let start = tokio::time::Instant::now();

    let body = tokio::time::timeout(Duration::from_secs(5), async {
        let resp = client
            .post(format!("{pf}/responses"))
            .json(&serde_json::json!({"model": "m", "input": "hi"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "the first byte already committed a 200");

        let mut body = String::new();
        let mut stream = resp.bytes_stream();
        // Defensive: an idle-terminated body surfaces to the HTTP client as either a stream-read
        // error (axum turns the `Err` item into a truncated/aborted body) or a clean end, depending
        // on the exact hyper/h2 behavior — either way the loop below terminates (never hangs,
        // bounded by the outer timeout), and we only assert on what a real client can observe:
        // the FIRST byte definitely arrived, and the read loop definitely ended.
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => body.push_str(&String::from_utf8_lossy(&bytes)),
                Err(_) => break, // idle-terminated: the connection ended abnormally — expected
            }
        }
        body
    })
    .await
    .expect("bounded: a stalled upstream under a SHORT configured idle must not hang the client");

    let elapsed = start.elapsed();
    assert!(
        body.contains("first-byte"),
        "the client received the byte relayed before the stall: {body:?}"
    );
    assert!(
        elapsed >= idle,
        "must not terminate before the configured idle window: {elapsed:?} < {idle:?}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "must terminate close to the configured idle window, not merely 'eventually': {elapsed:?}"
    );
}

/// `stream_idle_timeout = Duration::ZERO` (the documented disable lever) against the SAME
/// byte-then-stall upstream: no idle-based termination should occur. Guarded so the test itself
/// stays bounded without ever depending on the connection actually finishing — the first byte is
/// read normally, then the NEXT read is raced against a short external `tokio::time::timeout`
/// and asserted to ELAPSE (the stream itself never resolves within that window), proving the
/// disabled config produces no idle bound. Mirrors `watchdog::tests::
/// disabled_idle_timeout_never_terminates_a_stalled_stream`'s exact bounding idiom, at the e2e
/// layer.
#[tokio::test]
async fn disabled_stream_idle_timeout_has_no_idle_bound_e2e() {
    let mock = MockUpstream::stall_after_first(
        r#"{"type":"response.output_text.delta","delta":"first-byte"}"#,
    );
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(upstream, Duration::ZERO).await;

    let client = reqwest::Client::new();
    let resp = tokio::time::timeout(Duration::from_secs(5), async {
        client
            .post(format!("{pf}/responses"))
            .json(&serde_json::json!({"model": "m", "input": "hi"}))
            .send()
            .await
            .unwrap()
    })
    .await
    .expect("bounded: sending the request itself must not hang");
    assert_eq!(resp.status(), 200);

    let mut stream = resp.bytes_stream();
    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("bounded: the first byte must still relay promptly")
        .expect("a chunk")
        .expect("the first byte is Ok");
    assert!(
        String::from_utf8_lossy(&first).contains("first-byte"),
        "the first byte still relays with the idle timeout disabled"
    );

    // The disabled path must NOT resolve within this short window — if the config value were
    // silently NOT reaching `ObservingStream` (e.g. some code path re-hardcoded a nonzero
    // default), a spurious idle termination would make this next read resolve quickly and the
    // outer `timeout` below would return `Ok(_)` instead of `Err` (elapsed). Bounded either way.
    let raced = tokio::time::timeout(Duration::from_millis(400), stream.next()).await;
    assert!(
        raced.is_err(),
        "disabled (stream_idle_timeout = ZERO) must never idle-terminate a genuine stall: {raced:?}"
    );
}
