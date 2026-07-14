//! Round-trip: open a temp-file DB, run migrations, assert the schema exists.

use polyflare_store::Store;

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
