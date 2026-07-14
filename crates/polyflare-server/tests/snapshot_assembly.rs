//! Snapshot assembly: seed an account + usage rows, assemble, assert the snapshot fields
//! (latest-per-window usage, durable metadata, deferred runtime defaults).

use polyflare_server::snapshot::assemble_snapshots;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};

fn account(id: &str) -> Account {
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
        last_refresh: 1_700_000_000,
        created_at: 1_699_000_000,
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: Some(1_700_100_000),
        blocked_at: None,
        security_work_authorized: true,
        provider: "codex".to_string(),
    }
}

fn tokens() -> PlainTokens {
    PlainTokens {
        access_token: "a".to_string(),
        refresh_token: "r".to_string(),
        id_token: "i".to_string(),
    }
}

async fn insert_usage(
    store: &Store,
    account_id: &str,
    window: &str,
    used_percent: f64,
    recorded_at: i64,
    reset_at: i64,
) {
    sqlx::query(
        "INSERT INTO usage_history (account_id, recorded_at, \"window\", used_percent, reset_at) \
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(account_id)
    .bind(recorded_at)
    .bind(window)
    .bind(used_percent)
    .bind(reset_at)
    .execute(store.pool())
    .await
    .unwrap();
}

#[tokio::test]
async fn assembles_snapshot_with_latest_usage_per_window() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[7u8; 32]).unwrap();
    store
        .accounts()
        .insert(&account("acct-1"), &tokens(), &cipher)
        .await
        .unwrap();

    insert_usage(&store, "acct-1", "primary", 10.0, 1000, 2000).await;
    // Older secondary row has a HIGHER used_percent than the newer one: this proves the query
    // selects by recency (ORDER BY recorded_at DESC LIMIT 1), NOT by magnitude (a MAX bug would
    // pick 80.0 here). The newer, lower value (55.0) must win.
    insert_usage(&store, "acct-1", "secondary", 80.0, 1000, 3000).await; // older, higher
    insert_usage(&store, "acct-1", "secondary", 55.0, 2000, 3500).await; // newer, lower — wins

    let snaps = assemble_snapshots(&store).await.unwrap();
    assert_eq!(snaps.len(), 1);
    let s = &snaps[0];
    assert_eq!(s.id.as_str(), "acct-1");
    assert_eq!(s.status, "active");
    assert_eq!(s.used_percent, 10.0);
    assert_eq!(s.secondary_used_percent, 55.0); // newest secondary row (recency, not max)
    assert_eq!(s.reset_at, Some(1_700_100_000)); // durable account column
    assert_eq!(s.plan_type, "pro");
    assert!(s.security_work_authorized);
    // Deferred runtime defaults.
    assert_eq!(s.health_tier, 0);
    assert_eq!(s.in_flight, 0);
    assert!(s.last_error_at.is_none());
}

#[tokio::test]
async fn account_without_usage_gets_zeroed_windows() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[7u8; 32]).unwrap();
    store
        .accounts()
        .insert(&account("acct-2"), &tokens(), &cipher)
        .await
        .unwrap();

    let snaps = assemble_snapshots(&store).await.unwrap();
    assert_eq!(snaps.len(), 1);
    assert_eq!(snaps[0].used_percent, 0.0);
    assert_eq!(snaps[0].secondary_used_percent, 0.0);
}

#[tokio::test]
async fn assembles_candidates_in_stable_id_order() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[7u8; 32]).unwrap();
    // Insert OUT of alphabetical order; the assembler must still return them id-sorted. The
    // selector's seed-reproducibility ("same input order + same seed ⇒ same pick") depends on a
    // deterministic candidate order, so this guards against a future regression in `list()` /
    // the assembler that would reorder or shuffle the returned candidates.
    for id in ["c", "a", "b"] {
        store
            .accounts()
            .insert(&account(id), &tokens(), &cipher)
            .await
            .unwrap();
    }

    let snaps = assemble_snapshots(&store).await.unwrap();
    let ids: Vec<&str> = snaps.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(ids, ["a", "b", "c"]);
}

#[tokio::test]
async fn assemble_snapshots_populates_provider_and_filter_narrows_by_it() {
    use polyflare_core::Provider;
    use polyflare_server::snapshot::filter_by_provider;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[9u8; 32]).unwrap();

    let mut anthro = account("anthropic-1");
    anthro.provider = "anthropic".to_string();
    store
        .accounts()
        .insert(&anthro, &tokens(), &cipher)
        .await
        .unwrap();
    store
        .accounts()
        .insert(&account("codex-1"), &tokens(), &cipher)
        .await
        .unwrap();

    let snaps = assemble_snapshots(&store).await.unwrap();
    assert_eq!(snaps.len(), 2);

    let codex_only = filter_by_provider(&snaps, Provider::Codex);
    assert_eq!(codex_only.len(), 1);
    assert_eq!(codex_only[0].id.as_str(), "codex-1");

    let anthropic_only = filter_by_provider(&snaps, Provider::Anthropic);
    assert_eq!(anthropic_only.len(), 1);
    assert_eq!(anthropic_only[0].id.as_str(), "anthropic-1");
}

#[tokio::test]
async fn assemble_snapshots_excludes_accounts_with_an_unknown_provider() {
    // A provider value outside the known vocabulary can only exist via data written outside the app
    // (`AccountRepo` always writes a `Provider::Display` string). Its backend is unknown, so it must
    // be excluded from selection entirely — not surfaced as a routable (Codex) candidate that would
    // only hard-fail at `resolve_core_account`. Guards the fail-closed reconciliation.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[9u8; 32]).unwrap();

    store
        .accounts()
        .insert(&account("codex-1"), &tokens(), &cipher)
        .await
        .unwrap();
    let mut bogus = account("mystery-1");
    bogus.provider = "gemini".to_string(); // unknown to Provider::from_str
    store
        .accounts()
        .insert(&bogus, &tokens(), &cipher)
        .await
        .unwrap();

    let snaps = assemble_snapshots(&store).await.unwrap();
    let ids: Vec<&str> = snaps.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(
        ids,
        ["codex-1"],
        "the unknown-provider account must be excluded from selection"
    );
}
