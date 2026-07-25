use polyflare_store::{Account, PlainTokens, ResetCredit, Store, TokenCipher};

fn account() -> Account {
    Account {
        id: "account-a".to_string(),
        chatgpt_account_id: Some("workspace-a".to_string()),
        chatgpt_user_id: None,
        email: "a@example.test".to_string(),
        alias: None,
        workspace_id: None,
        workspace_label: None,
        seat_type: None,
        plan_type: "plus".to_string(),
        routing_policy: "normal".to_string(),
        last_refresh: 10,
        created_at: 10,
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: false,
        usage_cap_percent: None,
        usage_cap_override: false,
        provider: "codex".to_string(),
        pool: None,
    }
}

async fn store() -> Store {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);
    let cipher = TokenCipher::from_key_bytes(&[8; 32]).unwrap();
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
}

fn credit(id: &str, expiry: Option<i64>) -> ResetCredit {
    ResetCredit {
        account_id: "account-a".to_string(),
        credit_id: id.to_string(),
        reset_type: Some("rate_limit_reset".to_string()),
        status: Some("available".to_string()),
        granted_at: Some(100),
        expires_at: expiry,
        title: None,
        description: None,
        redeem_started_at: None,
        redeemed_at: None,
    }
}

#[tokio::test]
async fn snapshot_replacement_is_atomic_and_orders_expiring_credits_first() {
    let store = store().await;
    let repo = store.reset_credits();
    repo.replace_snapshot(
        "account-a",
        2,
        500,
        &[credit("persistent", None), credit("soon", Some(600))],
    )
    .await
    .unwrap();

    let snapshot = repo.get_snapshot("account-a").await.unwrap().unwrap();
    assert_eq!(snapshot.available_count, 2);
    assert_eq!(snapshot.credits[0].credit_id, "soon");

    repo.replace_snapshot("account-a", 0, 700, &[])
        .await
        .unwrap();
    let replaced = repo.get_snapshot("account-a").await.unwrap().unwrap();
    assert!(replaced.credits.is_empty());
    assert_eq!(replaced.available_count, 0);
}

#[tokio::test]
async fn claim_excludes_peers_and_expired_lease_can_be_taken_over() {
    let store = store().await;
    let repo = store.reset_credits();
    assert!(repo
        .try_acquire_claim("account-a", "holder-a", 100, 130)
        .await
        .unwrap());
    assert!(!repo
        .try_acquire_claim("account-a", "holder-b", 110, 140)
        .await
        .unwrap());
    assert!(repo
        .try_acquire_claim("account-a", "holder-b", 131, 161)
        .await
        .unwrap());
    repo.release_claim("account-a", "holder-a").await.unwrap();
    assert!(!repo
        .try_acquire_claim("account-a", "holder-c", 132, 162)
        .await
        .unwrap());
}

#[tokio::test]
async fn request_id_is_permanently_pinned_to_the_first_credit_within_ttl() {
    let store = store().await;
    let repo = store.reset_credits();
    assert_eq!(
        repo.pin_request("account-a", "request-1", "credit-a", 100, 86_400)
            .await
            .unwrap(),
        "credit-a"
    );
    assert_eq!(
        repo.pin_request("account-a", "request-1", "credit-b", 101, 86_400)
            .await
            .unwrap(),
        "credit-a"
    );
    repo.complete_request("account-a", "request-1", "reset", 2, Some(105), 106)
        .await
        .unwrap();
    let completed = repo
        .get_request("account-a", "request-1", 107, 86_400)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.credit_id, "credit-a");
    assert_eq!(completed.result_code.as_deref(), Some("reset"));
    assert_eq!(completed.windows_reset, Some(2));
}

#[tokio::test]
async fn native_request_id_is_permanently_pinned_to_one_account_and_requested_credit() {
    let store = store().await;
    let repo = store.reset_credits();
    let first = repo
        .pin_native_request(
            "native-request-1",
            "account-a",
            Some("credit-a"),
            "alpha",
            100,
            86_400,
        )
        .await
        .unwrap();
    assert_eq!(first.account_id.as_deref(), Some("account-a"));
    assert_eq!(first.requested_credit_id.as_deref(), Some("credit-a"));
    assert_eq!(first.pool_scope.as_deref(), Some("alpha"));

    let replay = repo
        .pin_native_request(
            "native-request-1",
            "account-b",
            Some("credit-b"),
            "beta",
            101,
            86_400,
        )
        .await
        .unwrap();
    assert_eq!(replay.account_id.as_deref(), Some("account-a"));
    assert_eq!(replay.requested_credit_id.as_deref(), Some("credit-a"));
    assert_eq!(replay.pool_scope.as_deref(), Some("alpha"));
}

#[tokio::test]
async fn native_request_pin_survives_account_deletion() {
    let store = store().await;
    let repo = store.reset_credits();
    repo.pin_native_request(
        "native-request-after-delete",
        "account-a",
        Some("credit-a"),
        "alpha",
        100,
        86_400,
    )
    .await
    .unwrap();

    store.accounts().delete("account-a", true).await.unwrap();

    let retained = repo
        .get_native_request("native-request-after-delete", 101, 86_400)
        .await
        .unwrap()
        .expect("ambiguous request pin must outlive its account");
    assert_eq!(retained.account_id.as_deref(), Some("account-a"));
    assert_eq!(retained.requested_credit_id.as_deref(), Some("credit-a"));
    assert_eq!(retained.pool_scope.as_deref(), Some("alpha"));
}

#[tokio::test]
async fn native_no_credit_is_a_durable_terminal_result() {
    let store = store().await;
    let repo = store.reset_credits();
    let terminal = repo
        .complete_native_no_credit("native-empty", "", 100, 86_400)
        .await
        .unwrap();
    assert!(terminal.account_id.is_none());
    assert_eq!(terminal.result_code.as_deref(), Some("no_credit"));
    assert_eq!(terminal.windows_reset, Some(0));
    assert_eq!(terminal.pool_scope.as_deref(), Some(""));

    let later_pin = repo
        .pin_native_request(
            "native-empty",
            "account-a",
            Some("new-credit"),
            "alpha",
            101,
            86_400,
        )
        .await
        .unwrap();
    assert!(later_pin.account_id.is_none());
    assert_eq!(later_pin.result_code.as_deref(), Some("no_credit"));
    assert_eq!(later_pin.pool_scope.as_deref(), Some(""));
}

#[tokio::test]
async fn native_account_pin_can_be_completed_as_local_no_credit() {
    let store = store().await;
    let repo = store.reset_credits();
    repo.pin_native_request(
        "native-account-empty",
        "account-a",
        Some("credit-a"),
        "",
        100,
        86_400,
    )
    .await
    .unwrap();
    assert!(repo
        .complete_native_account_no_credit("native-account-empty", "account-a", 101)
        .await
        .unwrap());

    let terminal = repo
        .get_native_request("native-account-empty", 102, 86_400)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.account_id.as_deref(), Some("account-a"));
    assert_eq!(terminal.requested_credit_id.as_deref(), Some("credit-a"));
    assert_eq!(terminal.result_code.as_deref(), Some("no_credit"));
}

#[tokio::test]
async fn fleet_request_id_is_pinned_to_the_original_account_sequence() {
    let store = store().await;
    let repo = store.reset_credits();
    assert_eq!(
        repo.pin_fleet_request("fleet-request-1", r#"["account-a"]"#, 100, 86_400)
            .await
            .unwrap(),
        r#"["account-a"]"#
    );
    assert_eq!(
        repo.pin_fleet_request(
            "fleet-request-1",
            r#"["account-a","account-b"]"#,
            101,
            86_400,
        )
        .await
        .unwrap(),
        r#"["account-a"]"#
    );
}
