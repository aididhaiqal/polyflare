use polyflare_core::{CapacityWeighted, SelectionCtx, Selector};
use polyflare_server::snapshot::assemble_snapshots;
use polyflare_store::{Account, PlainTokens, Store, TokenCipher};

fn account() -> Account {
    Account {
        id: "acct-a".to_string(),
        chatgpt_account_id: None,
        chatgpt_user_id: None,
        email: "a@example.test".to_string(),
        alias: None,
        workspace_id: None,
        workspace_label: None,
        seat_type: None,
        plan_type: "pro".to_string(),
        routing_policy: "normal".to_string(),
        last_refresh: 1_000,
        created_at: 1_000,
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
        provider: "codex".to_string(),
        pool: None,
    }
}

#[tokio::test]
async fn durable_rate_limit_gate_survives_store_reopen_and_blocks_early_admission() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let cipher = TokenCipher::from_key_bytes(&[31u8; 32]).unwrap();
    let store = Store::open(&path).await.unwrap();
    store
        .accounts()
        .insert(
            &account(),
            &PlainTokens {
                access_token: "access".to_string(),
                refresh_token: "refresh".to_string(),
                id_token: "id".to_string(),
            },
            &cipher,
        )
        .await
        .unwrap();
    store
        .accounts()
        .record_routing_cooldown("acct-a", 1_060, "rate_limit", 1_000)
        .await
        .unwrap();
    drop(store);

    let reopened = Store::open(&path).await.unwrap();
    let snapshots = assemble_snapshots(&reopened).await.unwrap();
    assert_eq!(snapshots[0].cooldown_until, Some(1_060));

    let selector = CapacityWeighted;
    let blocked = SelectionCtx {
        now: 1_010,
        ..SelectionCtx::default()
    };
    assert!(
        selector.pick(&snapshots, &blocked).is_none(),
        "restart must not erase the known rate-limit gate"
    );

    let elapsed = SelectionCtx {
        now: 1_061,
        ..SelectionCtx::default()
    };
    assert_eq!(
        selector.pick(&snapshots, &elapsed).unwrap().as_str(),
        "acct-a",
        "the durable gate expires normally"
    );
}
