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
        provider: "codex".to_string(),
        pool: None,
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

#[tokio::test]
async fn provider_round_trips_and_legacy_rows_default_to_codex() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[11u8; 32]).unwrap();
    let repo = store.accounts();

    // A fresh Anthropic account round-trips its provider through insert/get.
    let mut anthropic = sample_account("anthropic-1");
    anthropic.provider = "anthropic".to_string();
    repo.insert(&anthropic, &sample_tokens(), &cipher)
        .await
        .unwrap();
    assert_eq!(
        repo.get("anthropic-1").await.unwrap().unwrap().provider,
        "anthropic"
    );

    // A legacy row written the way pre-M4a code would (no `provider` column mentioned at all)
    // must default to 'codex' via the migration's column default — the real regression this
    // migration protects against.
    sqlx::query(
        "INSERT INTO accounts (id, email, plan_type, routing_policy, access_token_enc, \
         refresh_token_enc, id_token_enc, last_refresh, created_at, status, \
         security_work_authorized) VALUES ('legacy-1', 'legacy@example.test', 'pro', 'normal', \
         x'00', x'00', x'00', 0, 0, 'active', 0)",
    )
    .execute(store.pool())
    .await
    .unwrap();
    assert_eq!(
        repo.get("legacy-1").await.unwrap().unwrap().provider,
        "codex"
    );
}

#[tokio::test]
async fn find_by_chatgpt_account_id_powers_onboard_vs_reauth() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[7u8; 32]).unwrap();
    let repo = store.accounts();
    repo.insert(&sample_account("acct-1"), &sample_tokens(), &cipher)
        .await
        .unwrap();

    // Matches the seat by its ChatGPT id (sample_account uses "ws-acct") → re-auth path.
    let found = repo.find_by_chatgpt_account_id("ws-acct").await.unwrap();
    assert_eq!(found.map(|a| a.id), Some("acct-1".to_string()));
    // A new ChatGPT id → None → onboard (insert) path.
    assert!(repo
        .find_by_chatgpt_account_id("some-other-account")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn usage_refresh_persists_windows_and_gate() {
    // Backs the runtime usage-refresh loop: `insert_usage_window` rows must surface through
    // `latest_usage` (latest-per-window wins), and `update_status_and_reset` must move both the
    // routing gate and the reset time together.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[5u8; 32]).unwrap();
    let repo = store.accounts();
    repo.insert(&sample_account("acct-1"), &sample_tokens(), &cipher)
        .await
        .unwrap();

    // Only the weekly (secondary) window is written — mirrors current upstream, where the 5h
    // (primary) window is absent. An earlier row for the same window must be superseded.
    repo.insert_usage_window(
        "acct-1",
        "secondary",
        40.0,
        Some(1_783_000_000),
        Some(10080),
        100,
    )
    .await
    .unwrap();
    repo.insert_usage_window(
        "acct-1",
        "secondary",
        73.5,
        Some(1_783_900_000),
        Some(10080),
        200,
    )
    .await
    .unwrap();

    let usage = repo.latest_usage("acct-1").await.unwrap();
    assert!(
        usage.primary.is_none(),
        "no 5h window written → primary absent"
    );
    let sec = usage.secondary.expect("weekly window present");
    assert_eq!(sec.used_percent, 73.5, "latest recorded_at wins");
    assert_eq!(sec.reset_at, Some(1_783_900_000));

    // Gate + reset move together; a cleared gate carries no reset.
    repo.update_status_and_reset("acct-1", "quota_exceeded", Some(1_783_900_000))
        .await
        .unwrap();
    let acct = repo.get("acct-1").await.unwrap().unwrap();
    assert_eq!(acct.status, "quota_exceeded");
    assert_eq!(acct.reset_at, Some(1_783_900_000));

    repo.update_status_and_reset("acct-1", "active", None)
        .await
        .unwrap();
    let acct = repo.get("acct-1").await.unwrap().unwrap();
    assert_eq!(acct.status, "active");
    assert_eq!(acct.reset_at, None);
}

#[tokio::test]
async fn account_and_token_generations_decouple_by_write_kind() {
    // #4: usage/status/pool/routing writes bump ONLY the account (snapshot) generation; token writes
    // bump ONLY the token generation; an insert bumps BOTH. This is what keeps the token cache warm
    // across the usage-refresh loop's periodic writes while the snapshot cache still invalidates.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[17u8; 32]).unwrap();
    let repo = store.accounts();

    let (a0, t0) = (store.account_generation(), store.token_generation());
    // insert → BOTH bump.
    repo.insert(&sample_account("acct-1"), &sample_tokens(), &cipher)
        .await
        .unwrap();
    assert!(store.account_generation() > a0, "insert bumps account gen");
    assert!(store.token_generation() > t0, "insert bumps token gen");

    // A usage write → account gen bumps, token gen UNCHANGED (the key decoupling).
    let (a1, t1) = (store.account_generation(), store.token_generation());
    repo.insert_usage_window(
        "acct-1",
        "secondary",
        50.0,
        Some(1_800_000_000),
        Some(10080),
        100,
    )
    .await
    .unwrap();
    assert!(
        store.account_generation() > a1,
        "usage write bumps account gen"
    );
    assert_eq!(
        store.token_generation(),
        t1,
        "usage write must NOT evict the token cache"
    );

    // Status + pool + routing writes → account gen only.
    let t_before_meta = store.token_generation();
    repo.update_status("acct-1", "rate_limited").await.unwrap();
    repo.update_pool("acct-1", Some("team-a")).await.unwrap();
    repo.update_routing_policy("acct-1", "burn_first")
        .await
        .unwrap();
    assert_eq!(
        store.token_generation(),
        t_before_meta,
        "metadata writes must NOT evict tokens"
    );

    // A token write → token gen bumps, account gen UNCHANGED (tokens aren't snapshot data).
    let (a2, t2) = (store.account_generation(), store.token_generation());
    repo.update_tokens("acct-1", &sample_tokens(), &cipher, 1_700_600_000)
        .await
        .unwrap();
    assert_eq!(
        store.account_generation(),
        a2,
        "token refresh must NOT rebuild the snapshot cache"
    );
    assert!(
        store.token_generation() > t2,
        "token refresh bumps token gen"
    );
}

#[tokio::test]
async fn get_with_tokens_returns_account_and_decrypted_tokens_in_one_call() {
    // The request hot path's single-read replacement for `get` + `decrypt_tokens`: it must return
    // the SAME account row and the SAME decrypted tokens as the two-call sequence.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    let repo = store.accounts();
    repo.insert(&sample_account("acct-1"), &sample_tokens(), &cipher)
        .await
        .unwrap();

    let (account, tokens) = repo
        .get_with_tokens("acct-1", &cipher)
        .await
        .unwrap()
        .expect("account present");
    // Account row matches `get`.
    let via_get = repo.get("acct-1").await.unwrap().unwrap();
    assert_eq!(account.id, via_get.id);
    assert_eq!(account.email, via_get.email);
    assert_eq!(account.provider, via_get.provider);
    assert_eq!(account.chatgpt_account_id, via_get.chatgpt_account_id);
    // Tokens match `decrypt_tokens` (== the originals).
    assert_eq!(tokens.access_token, "access-abc");
    assert_eq!(tokens.refresh_token, "refresh-def");
    assert_eq!(tokens.id_token, "id-ghi");

    assert!(
        repo.get_with_tokens("missing", &cipher)
            .await
            .unwrap()
            .is_none(),
        "absent id → None"
    );
}
