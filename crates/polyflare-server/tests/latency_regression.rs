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
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(), // never called (fresh token)
        upstream_base_url: upstream,
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
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
