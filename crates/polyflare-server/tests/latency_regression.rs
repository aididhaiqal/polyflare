//! Latency-regression gate (MVP CI gate): guards against the single most likely latency
//! regression — PolyFlare accidentally buffering the streaming upstream response instead of
//! relaying it chunk-by-chunk. Check 1 is the primary, machine-independent assertion (relative
//! timing against an injected inter-chunk gap); Check 2 is a secondary, deliberately generous
//! absolute-overhead budget that only catches gross (e.g. 10x) regressions, not an SLA.
//!
//! Timing uses `tokio::time::Instant` (real wall-clock time — this test never pauses the tokio
//! test clock, since we are measuring actual relay behavior across a real TCP loopback hop).

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
use tokio::time::Instant;

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

/// Spawn a store-backed polyflare server whose single (Codex) account relays to `upstream`.
async fn spawn_polyflare(upstream: String) -> String {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    store
        .accounts()
        .insert(
            &store_account("latency-1"),
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
    // Keep the temp DB alive for the (short-lived) server task.
    std::mem::forget(dir);

    let state = Arc::new(AppState {
        codex_executor: Arc::new(CodexExecutor::new().unwrap()),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap()),
        selector: Arc::new(CapacityWeighted),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(), // never called (fresh token)
        upstream_base_url: upstream,
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: std::sync::Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: std::sync::Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        admin_token: None,
        live_logs: false,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        max_account_attempts: 3,
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        starvation_wait_budget: std::time::Duration::from_secs(60),
        starvation_heartbeat: std::time::Duration::from_secs(10),
        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),

        runtime: Default::default(),
    });
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// Check 1 (PRIMARY, machine-independent): PolyFlare must forward the first upstream chunk as
/// soon as it arrives, not after buffering the whole stream. The mock upstream emits one chunk
/// immediately, waits `GAP`, then emits the rest. The assertion is relative to `GAP` (not to any
/// absolute wall-clock budget), so it holds regardless of how fast or slow the CI machine is:
///   - `t_first_byte` (time to the first relayed body byte) must be well under `GAP` — if
///     PolyFlare buffered the response, it would instead be close to `t_full`.
///   - `t_full` (time to the complete stream) must be at least `GAP` — a sanity check that the
///     injected gap actually happened (i.e. this isn't trivially passing because nothing was
///     ever delayed).
#[tokio::test]
async fn non_buffering_first_byte_arrives_well_before_full_stream() {
    const GAP: Duration = Duration::from_millis(300);

    let mock = MockUpstream::chunked_with_gap(
        r#"{"type":"response.output_text.delta","delta":"a"}"#,
        vec![
            r#"{"type":"response.output_text.delta","delta":"b"}"#.to_string(),
            r#"{"type":"response.completed"}"#.to_string(),
        ],
        GAP,
    );
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(upstream).await;

    let client = reqwest::Client::new();
    let start = Instant::now();
    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let mut stream = resp.bytes_stream();
    let first_chunk = stream.next().await.unwrap().unwrap();
    let t_first_byte = start.elapsed();
    assert!(
        String::from_utf8_lossy(&first_chunk).contains("delta\":\"a"),
        "first relayed chunk should carry the first upstream event"
    );

    let mut body = String::from_utf8_lossy(&first_chunk).into_owned();
    while let Some(chunk) = stream.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    let t_full = start.elapsed();
    assert!(body.contains("delta\":\"b") && body.contains("response.completed"));

    // Primary assertion: relative to GAP, independent of machine speed. A buffering relay would
    // make t_first_byte ≈ t_full (both ≈ GAP); a non-buffering relay makes t_first_byte ≈ 0.
    // GAP=300ms leaves a wide margin even on a slow/noisy CI box.
    assert!(
        t_first_byte < GAP / 2,
        "t_first_byte={t_first_byte:?} should be well under half of GAP={GAP:?} \
         (PolyFlare appears to be buffering the stream instead of relaying it)"
    );
    // Sanity: the injected gap really happened.
    assert!(
        t_full >= GAP,
        "t_full={t_full:?} should be at least GAP={GAP:?} (the mock's gap didn't take effect?)"
    );
}

/// Check 2 (SECONDARY): catches gross (e.g. 10x) per-request overhead regressions. This is
/// deliberately NOT a latency SLA — CI machines are slow and noisy (shared runners, thermal
/// throttling, disk-backed sqlite I/O for the store), so a tight budget here would be flaky for
/// no benefit. The budget only needs to be loose enough to never false-fail on a healthy CI box
/// while still catching an actual regression (e.g. an accidental extra round trip, a blocking
/// call on the hot path, etc.), which would show up as multiples of this budget, not a few ms.
#[tokio::test]
async fn overhead_budget_median_end_to_end_time() {
    // Deliberately generous — see module + fn doc comment above.
    const BUDGET: Duration = Duration::from_millis(50);
    const TOTAL_REQUESTS: usize = 20;
    const WARMUP: usize = 3;

    let mock = MockUpstream::new(vec![r#"{"type":"response.completed"}"#.to_string()]);
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(upstream).await;
    let client = reqwest::Client::new();

    let mut samples = Vec::with_capacity(TOTAL_REQUESTS);
    for _ in 0..TOTAL_REQUESTS {
        let start = Instant::now();
        let resp = client
            .post(format!("{pf}/responses"))
            .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            chunk.unwrap();
        }
        samples.push(start.elapsed());
    }

    let measured = &mut samples[WARMUP..];
    measured.sort();
    let median = measured[measured.len() / 2];

    assert!(
        median < BUDGET,
        "median end-to-end latency {median:?} over {} requests exceeds the generous \
         {BUDGET:?} regression-catcher budget (this is not an SLA — see comment above; if this \
         is flaky on a real CI box, the budget should be raised, not removed)",
        measured.len(),
    );
}

/// Nearest-rank percentile (`p` in 0..=100) over an already-sorted slice.
fn percentile(sorted: &[Duration], p: usize) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((p * sorted.len()) / 100).min(sorted.len() - 1);
    sorted[idx]
}

fn mean(samples: &[Duration]) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    samples.iter().sum::<Duration>() / samples.len() as u32
}

/// Drive `n` full request→last-byte round-trips against `url` (plus `warmup` discarded ones),
/// returning the per-request durations.
async fn measure_round_trips(
    client: &reqwest::Client,
    url: &str,
    n: usize,
    warmup: usize,
) -> Vec<Duration> {
    let mut samples = Vec::with_capacity(n);
    for i in 0..(n + warmup) {
        let start = Instant::now();
        let resp = client
            .post(url)
            .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            chunk.unwrap();
        }
        if i >= warmup {
            samples.push(start.elapsed());
        }
    }
    samples
}

/// Reports PolyFlare's PROXY OVERHEAD — the latency it adds on top of the upstream — measured
/// against an INSTANT mock upstream (so the upstream's own time is ~loopback-only and doesn't
/// dominate). This is the metric comparable to better-ccflare's "<10ms" claim: proxy processing
/// (selection + account resolve/decrypt + prepare + relay), NOT total end-to-end latency, which is
/// dominated by the real LLM round-trip + token generation (hundreds of ms) and says nothing about
/// the proxy. Overhead = median(through-PolyFlare) − median(direct-to-mock), so the fixed loopback
/// + client cost is subtracted out. The numbers print with `cargo test -- --nocapture`; the
/// assertion is a generous gross-regression guard, not the SLA.
#[tokio::test]
async fn report_proxy_overhead_against_instant_upstream() {
    const N: usize = 100;
    const WARMUP: usize = 10;

    let mock = MockUpstream::new(vec![r#"{"type":"response.completed"}"#.to_string()]);
    let upstream = mock.spawn().await;
    let client = reqwest::Client::new();

    // Baseline: client -> mock directly (1 hop). The fixed cost that is NOT PolyFlare's doing.
    let baseline = measure_round_trips(&client, &format!("{upstream}/responses"), N, WARMUP).await;

    // Through PolyFlare: client -> polyflare -> the SAME mock (2 hops + PolyFlare's processing).
    let pf = spawn_polyflare(upstream).await;
    let proxy = measure_round_trips(&client, &format!("{pf}/responses"), N, WARMUP).await;

    let mut b = baseline.clone();
    b.sort();
    let mut p = proxy.clone();
    p.sort();
    let overhead = percentile(&p, 50).saturating_sub(percentile(&b, 50));

    println!("--- PolyFlare proxy overhead vs instant mock (n={N}) ---");
    println!(
        "  baseline direct-to-mock : p50={:?}  p99={:?}  mean={:?}",
        percentile(&b, 50),
        percentile(&b, 99),
        mean(&baseline)
    );
    println!(
        "  through PolyFlare       : p50={:?}  p99={:?}  mean={:?}",
        percentile(&p, 50),
        percentile(&p, 99),
        mean(&proxy)
    );
    println!("  PROXY OVERHEAD (p50 diff): {overhead:?}   [better-ccflare claims <10ms]");

    // Catastrophe-only guard — NOT an SLA and NOT the reportable figure. On a real machine this
    // overhead is sub-millisecond (~0.33ms release / ~0.6ms debug locally), which is the number to
    // quote against ccflare's <10ms; but a shared CI runner's disk-backed sqlite lookup+decrypt per
    // request (debug, thermal-throttled) inflates it into the tens of ms, so the bound here is
    // deliberately huge and only trips on a truly gross regression (an added round trip, a blocking
    // call, a per-request hang). Read the printed p50 from a local `--release --nocapture` run for
    // the meaningful figure.
    assert!(
        overhead < Duration::from_millis(200),
        "proxy overhead p50={overhead:?} exceeds the catastrophe guard — investigate a \
         hot-path regression (an extra round trip, a blocking call, etc.)"
    );
}
