//! Request-log repository round-trip: insert -> list (newest-first, paginated) -> count, and a
//! full-field round-trip.

use polyflare_store::{RequestLogRecord, Store};

fn rec(
    requested_at: i64,
    provider: &str,
    path: &str,
    status: u16,
    duration_ms: i64,
) -> RequestLogRecord {
    RequestLogRecord {
        requested_at,
        provider: provider.to_string(),
        method: "POST".to_string(),
        path: path.to_string(),
        aliased: false,
        status,
        duration_ms,
        account_id: None,
        target_kind: None,
        provider_credential_id: None,
        model: None,
        upstream_model: None,
        upstream_transport: None,
        reasoning_effort: None,
        service_tier: None,
        transport: None,
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

#[tokio::test]
async fn insert_list_count_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let repo = store.request_log();

    assert_eq!(repo.count().await.unwrap(), 0);

    repo.insert(&rec(100, "codex", "/responses", 200, 12))
        .await
        .unwrap();
    repo.insert(&rec(200, "anthropic", "/v1/messages", 503, 5))
        .await
        .unwrap();
    repo.insert(&rec(150, "codex", "/responses", 502, 9))
        .await
        .unwrap();

    assert_eq!(repo.count().await.unwrap(), 3);

    // Newest-first by requested_at.
    let rows = repo.list(10, 0).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].requested_at, 200);
    assert_eq!(rows[0].provider, "anthropic");
    assert_eq!(rows[0].status, 503);
    assert_eq!(rows[1].requested_at, 150);
    assert_eq!(rows[2].requested_at, 100);

    // Offset pagination keeps the newest-first order.
    let page = repo.list(2, 1).await.unwrap();
    assert_eq!(page.len(), 2);
    assert_eq!(page[0].requested_at, 150);
    assert_eq!(page[1].requested_at, 100);
}

#[tokio::test]
async fn round_trips_every_field() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let repo = store.request_log();

    let mut r = rec(42, "sakana", "/responses", 200, 77);
    r.aliased = true;
    r.target_kind = Some("credential".into());
    r.provider_credential_id = Some("sakana-primary".into());
    r.model = Some("fugu-ultra".into());
    r.upstream_model = Some("fugu-ultra-v1.1".into());
    r.upstream_transport = Some("http_sse".into());
    r.orchestration_input_tokens = Some(100);
    r.orchestration_output_tokens = Some(25);
    r.orchestration_cached_input_tokens = Some(40);
    repo.insert(&r).await.unwrap();

    let rows = repo.list(1, 0).await.unwrap();
    let row = &rows[0];
    assert!(row.id > 0);
    assert_eq!(row.requested_at, 42);
    assert_eq!(row.provider, "sakana");
    assert_eq!(row.method, "POST");
    assert_eq!(row.path, "/responses");
    assert!(row.aliased);
    assert_eq!(row.status, 200);
    assert_eq!(row.duration_ms, 77);
    assert_eq!(row.target_kind.as_deref(), Some("credential"));
    assert_eq!(
        row.provider_credential_id.as_deref(),
        Some("sakana-primary")
    );
    assert_eq!(row.model.as_deref(), Some("fugu-ultra"));
    assert_eq!(row.upstream_model.as_deref(), Some("fugu-ultra-v1.1"));
    assert_eq!(row.upstream_transport.as_deref(), Some("http_sse"));
    assert_eq!(row.orchestration_input_tokens, Some(100));
    assert_eq!(row.orchestration_output_tokens, Some(25));
    assert_eq!(row.orchestration_cached_input_tokens, Some(40));

    let aggregate = repo.aggregate_since(0).await.unwrap();
    assert_eq!(aggregate.orchestration_tokens, 125);
    assert_eq!(aggregate.orchestration_cached_tokens, 40);

    let reports = repo.reports_totals(0, Some("sakana")).await.unwrap();
    assert_eq!(reports.orchestration_tokens, 125);
    assert_eq!(reports.orchestration_cached_tokens, 40);
}
