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
    insert_usage(&store, "acct-1", "secondary", 30.0, 1000, 3000).await;
    insert_usage(&store, "acct-1", "secondary", 55.0, 2000, 3500).await; // newer wins

    let snaps = assemble_snapshots(&store).await.unwrap();
    assert_eq!(snaps.len(), 1);
    let s = &snaps[0];
    assert_eq!(s.id.as_str(), "acct-1");
    assert_eq!(s.status, "active");
    assert_eq!(s.used_percent, 10.0);
    assert_eq!(s.secondary_used_percent, 55.0); // newest secondary row
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
