//! Round-trip: open a temp-file DB, run migrations, assert the schema exists.

use polyflare_store::Store;

type UsageClassificationRow = (
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
);

#[tokio::test]
async fn open_creates_schema() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("store.db");

    let store = Store::open(&db_path).await.unwrap();

    let names: Vec<String> =
        sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
            .fetch_all(store.pool())
            .await
            .unwrap();

    assert!(names.iter().any(|n| n == "accounts"), "tables: {names:?}");
    assert!(
        names.iter().any(|n| n == "usage_history"),
        "tables: {names:?}"
    );
    assert!(db_path.exists(), "the DB file must be created on disk");
}

#[tokio::test]
async fn open_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("store.db");
    // Opening twice must not error: migrations already applied are skipped.
    let _first = Store::open(&db_path).await.unwrap();
    let _second = Store::open(&db_path).await.unwrap();
}

#[tokio::test]
async fn migration_0016_erases_preexisting_raw_session_ids() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("store.db");
    let store = Store::open(&db_path).await.unwrap();

    sqlx::query(
        "INSERT INTO request_log \
         (requested_at, provider, method, path, aliased, status, duration_ms, session_id) \
         VALUES (1, 'codex', 'POST', '/responses', 0, 200, 1, 'raw-session-secret')",
    )
    .execute(store.pool())
    .await
    .unwrap();

    // Recreate the exact pre-0016 schema/version boundary, then let Store::open perform the real
    // embedded upgrade. SQLite supports DROP COLUMN in the version bundled by sqlx.
    sqlx::query("DROP INDEX idx_request_log_session_key_requested_at")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("ALTER TABLE request_log DROP COLUMN session_key")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 16")
        .execute(store.pool())
        .await
        .unwrap();
    store.pool().close().await;
    drop(store);

    let upgraded = Store::open(&db_path).await.unwrap();
    let raw_session: Option<String> =
        sqlx::query_scalar("SELECT session_id FROM request_log LIMIT 1")
            .fetch_one(upgraded.pool())
            .await
            .unwrap();
    assert_eq!(
        raw_session, None,
        "the upgrade must erase legacy raw session identifiers"
    );
    let session_key_column: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('request_log') WHERE name = 'session_key'",
    )
    .fetch_one(upgraded.pool())
    .await
    .unwrap();
    assert_eq!(session_key_column, 1, "the hashed session column was added");
}

#[tokio::test]
async fn migration_0021_classifies_legacy_usage_without_inventing_new_token_facts() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("store.db");
    let store = Store::open(&db_path).await.unwrap();

    sqlx::query(
        "INSERT INTO request_log \
         (requested_at, provider, method, path, aliased, status, duration_ms, input_tokens, \
          output_tokens, cached_input_tokens, reasoning_tokens, import_source_id) \
         VALUES (1, 'codex', 'POST', '/responses', 0, 0, 1, 100, 25, 80, 5, 42)",
    )
    .execute(store.pool())
    .await
    .unwrap();

    for column in [
        "usage_status",
        "usage_source",
        "usage_schema",
        "reported_total_tokens",
        "cache_write_input_tokens",
    ] {
        sqlx::query(&format!("ALTER TABLE request_log DROP COLUMN {column}"))
            .execute(store.pool())
            .await
            .unwrap();
    }
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 21")
        .execute(store.pool())
        .await
        .unwrap();
    store.pool().close().await;
    drop(store);

    let upgraded = Store::open(&db_path).await.unwrap();
    let row: UsageClassificationRow = sqlx::query_as(
        "SELECT cache_write_input_tokens, reported_total_tokens, usage_schema, usage_source, \
             usage_status FROM request_log WHERE import_source_id = 42",
    )
    .fetch_one(upgraded.pool())
    .await
    .unwrap();
    assert_eq!(
        row,
        (
            None,
            None,
            Some("legacy_unknown".into()),
            Some("codex_lb_import".into()),
            Some("legacy".into()),
        ),
        "migration must classify provenance while leaving unobserved token facts unknown"
    );
}

#[tokio::test]
async fn ensure_session_reattaching_matches_ensure_then_set_state() {
    // The folded UPSERT must be behavior-equivalent to `ensure_session` + `set_state("reattaching")`:
    // a new key is created directly in `reattaching`; a re-call keeps it reattaching with created_at
    // preserved; and it must equal the two-call sequence run on a separate key.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let repo = store.continuity();

    // New key via the folded call.
    repo.ensure_session_reattaching("k1", "hard", 1000)
        .await
        .unwrap();
    let a = repo.get_session("k1").await.unwrap().unwrap();
    assert_eq!(a.state, "reattaching");
    assert_eq!(a.key_strength, "hard");
    assert_eq!(a.created_at, 1000);

    // Re-call at a later time: still reattaching, created_at preserved, timestamps bumped.
    repo.ensure_session_reattaching("k1", "hard", 2000)
        .await
        .unwrap();
    let a2 = repo.get_session("k1").await.unwrap().unwrap();
    assert_eq!(a2.state, "reattaching");
    assert_eq!(a2.created_at, 1000, "created_at preserved on conflict");
    assert_eq!(a2.updated_at, 2000, "updated_at bumped on conflict");

    // The two-call sequence on a separate key must land in the same state.
    repo.ensure_session("k2", "hard", 1000).await.unwrap();
    repo.set_state("k2", "reattaching", 1000).await.unwrap();
    let b = repo.get_session("k2").await.unwrap().unwrap();
    assert_eq!(b.state, a.state);
    assert_eq!(b.created_at, a.created_at);
    assert_eq!(b.key_strength, a.key_strength);
}
