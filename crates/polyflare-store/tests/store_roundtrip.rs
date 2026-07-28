//! Round-trip: open a temp-file DB, run migrations, assert the schema exists.

use polyflare_store::Store;
use sqlx::Row;

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
    assert!(
        names.iter().any(|n| n == "translation_routes"),
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
async fn migration_0022_defaults_existing_models_to_noop_profiles() {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlx::raw_sql(
        "CREATE TABLE provider_models (id TEXT PRIMARY KEY); \
         INSERT INTO provider_models (id) VALUES ('legacy-model'); \
         CREATE TABLE request_log (id INTEGER PRIMARY KEY);",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::raw_sql(include_str!("../migrations/0022_custom_model_profiles.sql"))
        .execute(&pool)
        .await
        .unwrap();

    let row = sqlx::query(
        "SELECT instruction_mode, instruction_text, request_overrides_json \
         FROM provider_models WHERE id = 'legacy-model'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("instruction_mode"), "none");
    assert_eq!(row.get::<String, _>("instruction_text"), "");
    assert_eq!(row.get::<String, _>("request_overrides_json"), "{}");

    let columns = sqlx::query("PRAGMA table_info(request_log)")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(columns
        .iter()
        .any(|column| column.get::<String, _>("name") == "profile_revision"));
}

#[tokio::test]
async fn provider_model_multi_target_migration_preserves_rows_and_scopes_uniqueness() {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlx::query(
        "CREATE TABLE request_log (id INTEGER PRIMARY KEY, requested_at INTEGER NOT NULL DEFAULT 0)",
    )
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!(
        "../migrations/0019_custom_model_providers.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../migrations/0020_provider_model_catalog_visibility.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::raw_sql(include_str!("../migrations/0022_custom_model_profiles.sql"))
        .execute(&pool)
        .await
        .unwrap();

    sqlx::query(
        "INSERT INTO custom_providers \
         (id, slug, display_name, base_url, created_at, updated_at) \
         VALUES ('provider-a', 'a', 'A', 'https://a.example/v1', 1, 1), \
                ('provider-b', 'b', 'B', 'https://b.example/v1', 1, 1)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO provider_models \
         (id, provider_id, public_model, upstream_model, display_name, created_at, updated_at) \
         VALUES ('model-a', 'provider-a', 'shared-model', 'upstream-a', 'Shared A', 1, 1)",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::raw_sql(include_str!(
        "../migrations/20260727210344_provider_model_multi_target.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();

    let preserved: (String, String, String, String, bool, bool, String) = sqlx::query_as(
        "SELECT provider_id, public_model, upstream_model, display_name, \
         visible_in_codex, visible_in_openai, instruction_mode \
         FROM provider_models WHERE id = 'model-a'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        preserved,
        (
            "provider-a".into(),
            "shared-model".into(),
            "upstream-a".into(),
            "Shared A".into(),
            true,
            true,
            "none".into()
        )
    );

    sqlx::query(
        "INSERT INTO provider_models \
         (id, provider_id, public_model, upstream_model, display_name, created_at, updated_at) \
         VALUES ('model-b', 'provider-b', 'shared-model', 'upstream-b', 'Shared B', 2, 2)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let duplicate = sqlx::query(
        "INSERT INTO provider_models \
         (id, provider_id, public_model, upstream_model, display_name, created_at, updated_at) \
         VALUES ('model-a-duplicate', 'provider-a', 'shared-model', 'upstream-c', 'Shared C', 3, 3)",
    )
    .execute(&pool)
    .await
    .unwrap_err();
    assert!(
        duplicate.to_string().contains("UNIQUE constraint failed"),
        "unexpected error: {duplicate}"
    );
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
async fn migration_0028_preserves_legacy_native_requests_with_unknown_scope() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("store.db");
    let store = Store::open(&db_path).await.unwrap();

    sqlx::query(
        "INSERT INTO reset_credit_native_requests \
         (redeem_request_id, account_id, requested_credit_id, created_at, pool_scope) \
         VALUES ('legacy-native', 'deleted-account', 'credit-a', 100, 'alpha')",
    )
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query("ALTER TABLE reset_credit_native_requests DROP COLUMN pool_scope")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 28")
        .execute(store.pool())
        .await
        .unwrap();
    store.pool().close().await;
    drop(store);

    let upgraded = Store::open(&db_path).await.unwrap();
    let row: (String, Option<String>) = sqlx::query_as(
        "SELECT redeem_request_id, pool_scope FROM reset_credit_native_requests \
         WHERE redeem_request_id = 'legacy-native'",
    )
    .fetch_one(upgraded.pool())
    .await
    .unwrap();
    assert_eq!(row, ("legacy-native".into(), None));
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

/// Reverse migration 0025 on an already-migrated database so the next `Store::open` re-runs it
/// against whatever rows the test seeded. Mirrors the 0021/0016 pattern.
async fn revert_migration_0025(pool: &sqlx::SqlitePool) {
    sqlx::query("DROP INDEX accounts_provider_upstream_identity_idx")
        .execute(pool)
        .await
        .unwrap();
    for column in [
        "upstream_identity",
        "auth_mode",
        "access_token_expires_at",
        "oauth_contract_version",
        "granted_scopes",
    ] {
        sqlx::query(&format!("ALTER TABLE accounts DROP COLUMN {column}"))
            .execute(pool)
            .await
            .unwrap();
    }
    // Restore the pre-0025 onboarding-flow table, CHECK constraint and all.
    sqlx::query("DROP TABLE account_onboarding_flows")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE account_onboarding_flows (
            id TEXT PRIMARY KEY,
            provider TEXT NOT NULL CHECK (provider = 'codex'),
            oauth_state TEXT NOT NULL UNIQUE,
            verifier_enc BLOB NOT NULL,
            initial_pool TEXT,
            status TEXT NOT NULL CHECK (status IN ('pending','exchanging','completed','failed')),
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            finished_at INTEGER,
            account_id TEXT,
            error_code TEXT,
            FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE SET NULL
        )",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 25")
        .execute(pool)
        .await
        .unwrap();
}

/// Insert a pre-0025 account row directly (the repository would write the new columns).
async fn seed_pre_0025_account(
    pool: &sqlx::SqlitePool,
    id: &str,
    provider: &str,
    chatgpt_account_id: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO accounts (id, chatgpt_account_id, email, plan_type, routing_policy, \
         access_token_enc, refresh_token_enc, id_token_enc, last_refresh, created_at, status, \
         security_work_authorized, provider) \
         VALUES (?, ?, ?, 'pro', 'normal', X'01', X'02', X'03', 0, 0, 'active', 0, ?)",
    )
    .bind(id)
    .bind(chatgpt_account_id)
    .bind(format!("{id}@example.test"))
    .bind(provider)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn migration_0025_backfills_identity_and_auth_mode_without_wedging_on_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("store.db");
    let store = Store::open(&db_path).await.unwrap();
    revert_migration_0025(store.pool()).await;

    // A unique Codex identity, two Codex rows SHARING one identity (historically possible — no
    // unique constraint ever enforced it), and a legacy Anthropic row with no ChatGPT identity.
    seed_pre_0025_account(store.pool(), "codex-unique", "codex", Some("acct-unique")).await;
    seed_pre_0025_account(store.pool(), "codex-dupe-a", "codex", Some("acct-shared")).await;
    seed_pre_0025_account(store.pool(), "codex-dupe-b", "codex", Some("acct-shared")).await;
    seed_pre_0025_account(store.pool(), "anthropic-legacy", "anthropic", None).await;
    // A pending Codex onboarding flow must survive the table rebuild.
    sqlx::query(
        "INSERT INTO account_onboarding_flows (id, provider, oauth_state, verifier_enc, \
         initial_pool, status, created_at, expires_at) \
         VALUES ('flow-legacy', 'codex', 'state-legacy', X'AA', 'team-a', 'pending', 5, 900)",
    )
    .execute(store.pool())
    .await
    .unwrap();

    store.pool().close().await;
    drop(store);

    // Re-running 0025 must succeed rather than fail on the duplicate identity.
    let upgraded = Store::open(&db_path).await.unwrap();

    let rows: Vec<(String, Option<String>, String)> =
        sqlx::query_as("SELECT id, upstream_identity, auth_mode FROM accounts ORDER BY id")
            .fetch_all(upgraded.pool())
            .await
            .unwrap();
    assert_eq!(
        rows,
        vec![
            // The legacy Anthropic row keeps static-bearer behavior — it predates subscription
            // OAuth, so calling it refreshable would be a lie.
            (
                "anthropic-legacy".to_string(),
                None,
                "static_bearer".to_string()
            ),
            // Both halves of the duplicate stay unset for operator resolution; neither is merged
            // or deleted, and both remain fully usable accounts.
            ("codex-dupe-a".to_string(), None, "codex_oauth".to_string()),
            ("codex-dupe-b".to_string(), None, "codex_oauth".to_string()),
            (
                "codex-unique".to_string(),
                Some("acct-unique".to_string()),
                "codex_oauth".to_string()
            ),
        ]
    );

    // `chatgpt_account_id` is copied, never repurposed: the Codex companion header still resolves.
    let chatgpt_id: Option<String> =
        sqlx::query_scalar("SELECT chatgpt_account_id FROM accounts WHERE id = 'codex-dupe-a'")
            .fetch_one(upgraded.pool())
            .await
            .unwrap();
    assert_eq!(chatgpt_id.as_deref(), Some("acct-shared"));

    // The rebuilt flow table carries its row forward and now admits Anthropic.
    let flow: (String, String, Option<String>) = sqlx::query_as(
        "SELECT flow_provider, status, redirect_uri FROM account_onboarding_flows \
         WHERE id = 'flow-legacy'",
    )
    .fetch_one(upgraded.pool())
    .await
    .unwrap();
    assert_eq!(flow, ("codex".to_string(), "pending".to_string(), None));
    sqlx::query(
        "INSERT INTO account_onboarding_flows (id, flow_provider, oauth_state, verifier_enc, \
         status, created_at, expires_at, redirect_uri) \
         VALUES ('flow-anthropic', 'anthropic', 'state-a', X'BB', 'pending', 5, 900, \
                 'http://127.0.0.1:54321/callback')",
    )
    .execute(upgraded.pool())
    .await
    .unwrap();
}

#[tokio::test]
async fn migration_0025_unique_index_rejects_a_second_row_for_one_upstream_identity() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();

    seed_pre_0025_account(store.pool(), "anthropic-a", "anthropic", None).await;
    seed_pre_0025_account(store.pool(), "anthropic-b", "anthropic", None).await;
    seed_pre_0025_account(store.pool(), "codex-a", "codex", None).await;
    let set_identity = |id: &'static str, provider: &'static str| {
        let pool = store.pool().clone();
        async move {
            sqlx::query(
                "UPDATE accounts SET upstream_identity = 'seat-1', provider = ? WHERE id = ?",
            )
            .bind(provider)
            .bind(id)
            .execute(&pool)
            .await
        }
    };

    set_identity("anthropic-a", "anthropic").await.unwrap();
    // Same identity under a DIFFERENT provider is a different seat — allowed.
    set_identity("codex-a", "codex").await.unwrap();
    // Same identity under the SAME provider is the same seat — rejected, so re-login updates the
    // existing row instead of silently creating a second account holding the same grant.
    let conflict = set_identity("anthropic-b", "anthropic").await;
    assert!(
        conflict.is_err(),
        "a duplicate (provider, upstream_identity) must be rejected"
    );

    // The rejected row keeps its NULL identity and is otherwise untouched — a conflict must not
    // partially apply. NULL is exempt from the partial index, so many such rows coexist.
    let unset: Vec<String> =
        sqlx::query_scalar("SELECT id FROM accounts WHERE upstream_identity IS NULL ORDER BY id")
            .fetch_all(store.pool())
            .await
            .unwrap();
    assert_eq!(unset, vec!["anthropic-b".to_string()]);
}
