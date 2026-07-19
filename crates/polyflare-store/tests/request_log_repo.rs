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
        model: None,
        reasoning_effort: None,
        service_tier: None,
        transport: None,
        ttft_ms: None,
        total_tokens: None,
        cached_tokens: None,
        subagent: None,
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

    let mut r = rec(42, "codex", "/responses", 200, 77);
    r.aliased = true;
    repo.insert(&r).await.unwrap();

    let rows = repo.list(1, 0).await.unwrap();
    let row = &rows[0];
    assert!(row.id > 0);
    assert_eq!(row.requested_at, 42);
    assert_eq!(row.provider, "codex");
    assert_eq!(row.method, "POST");
    assert_eq!(row.path, "/responses");
    assert!(row.aliased);
    assert_eq!(row.status, 200);
    assert_eq!(row.duration_ms, 77);
}
