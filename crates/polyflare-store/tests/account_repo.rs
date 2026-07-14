//! Account repository integration test against a temp-file DB.

use polyflare_store::{Account, PlainTokens, Store, TokenCipher};

fn sample_account(id: &str) -> Account {
    Account {
        id: id.to_string(),
        chatgpt_account_id: Some("ws-acct".to_string()),
        chatgpt_user_id: Some("user-1".to_string()),
        email: "user@example.test".to_string(),
        alias: Some("main".to_string()),
        workspace_id: Some("ws-1".to_string()),
        workspace_label: Some("Workspace One".to_string()),
        seat_type: Some("standard".to_string()),
        plan_type: "pro".to_string(),
        routing_policy: "normal".to_string(),
        last_refresh: 1_700_000_000,
        created_at: 1_699_000_000,
        status: "active".to_string(),
        deactivation_reason: None,
        reset_at: None,
        blocked_at: None,
        security_work_authorized: true,
    }
}

fn sample_tokens() -> PlainTokens {
    PlainTokens {
        access_token: "access-abc".to_string(),
        refresh_token: "refresh-def".to_string(),
        id_token: "id-ghi".to_string(),
    }
}

#[tokio::test]
async fn insert_get_list_decrypt_and_update() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[9u8; 32]).unwrap();
    let repo = store.accounts();

    // insert
    repo.insert(&sample_account("acct-1"), &sample_tokens(), &cipher)
        .await
        .unwrap();

    // get (present + absent)
    let got = repo.get("acct-1").await.unwrap().unwrap();
    assert_eq!(got.email, "user@example.test");
    assert_eq!(got.plan_type, "pro");
    assert!(got.security_work_authorized);
    assert!(repo.get("missing").await.unwrap().is_none());

    // list (ordered by id)
    repo.insert(&sample_account("acct-2"), &sample_tokens(), &cipher)
        .await
        .unwrap();
    let all = repo.list().await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].id, "acct-1");
    assert_eq!(all[1].id, "acct-2");

    // decrypt_tokens == originals
    let toks = repo
        .decrypt_tokens("acct-1", &cipher)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(toks.access_token, "access-abc");
    assert_eq!(toks.refresh_token, "refresh-def");
    assert_eq!(toks.id_token, "id-ghi");

    // update_status
    repo.update_status("acct-1", "rate_limited").await.unwrap();
    assert_eq!(
        repo.get("acct-1").await.unwrap().unwrap().status,
        "rate_limited"
    );

    // update_tokens (re-encrypts + stamps last_refresh)
    let new_tokens = PlainTokens {
        access_token: "access-new".to_string(),
        refresh_token: "refresh-new".to_string(),
        id_token: "id-new".to_string(),
    };
    repo.update_tokens("acct-1", &new_tokens, &cipher, 1_700_500_000)
        .await
        .unwrap();
    let toks2 = repo
        .decrypt_tokens("acct-1", &cipher)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(toks2.access_token, "access-new");
    assert_eq!(
        repo.get("acct-1").await.unwrap().unwrap().last_refresh,
        1_700_500_000
    );
}
