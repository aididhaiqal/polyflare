//! The replica half: pull credentials from the master over HTTP and re-encrypt them locally.
//!
//! # Why this lives in the binary and not in a shell script
//!
//! Stored tokens are XChaCha-encrypted with the node's own key. The master serves them decrypted,
//! so the replica must ENCRYPT before writing — which needs the store's cipher. That has a real
//! benefit over the `.dump`-over-SSH path it replaces: each node keeps its OWN key file. The old
//! path copied ciphertext, so both nodes had to share a key; a replica holding a different key
//! silently stored tokens it could not decrypt.
//!
//! # The one invariant this must never break
//!
//! Only the master refreshes OAuth tokens. Refresh tokens rotate, so a second refresher burns the
//! grant (`refresh_token_reused` -> the account is forced back through a browser login). This is
//! the copy half of that contract; the refusal half is `reactive_auth::is_replica`.

use std::collections::HashSet;
use std::time::Duration;

use polyflare_store::{Account, PlainTokens, Store, TokenCipher};
use serde::Deserialize;

/// The contract version this build can consume. A master serving anything else is REFUSED rather
/// than parsed leniently — a half-understood credential payload is worse than a stale one.
const SUPPORTED_CONTRACT: u32 = 1;

#[derive(Deserialize)]
struct PulledAccount {
    id: String,
    provider: String,
    email: String,
    alias: Option<String>,
    plan_type: String,
    routing_policy: String,
    status: String,
    pool: Option<String>,
    chatgpt_account_id: Option<String>,
    auth_mode: String,
    access_token: String,
    refresh_token: String,
    id_token: String,
    access_token_expires_at: Option<i64>,
    granted_scopes: Option<String>,
    oauth_contract_version: Option<String>,
    last_refresh: i64,
    security_work_authorized: bool,
}

#[derive(Deserialize)]
struct PulledAccounts {
    contract: u32,
    accounts: Vec<PulledAccount>,
}

pub struct PullReport {
    pub updated: usize,
    pub inserted: usize,
    pub unchanged: usize,
}

type Fallible<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Fetch every account from `master_base` and reconcile it into the local store.
///
/// Accounts present locally but absent upstream are LEFT ALONE. A master that briefly serves a
/// short list (mid-migration, partial decrypt failure) must not be able to delete a replica's
/// working credentials; an operator removing an account for real can do it explicitly.
pub async fn pull(
    store: &Store,
    cipher: &TokenCipher,
    master_base: &str,
    admin_token: &str,
) -> Fallible<PullReport> {
    let url = format!("{}/api/replica/accounts", master_base.trim_end_matches('/'));
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?
        .get(&url)
        .header("authorization", format!("Bearer {admin_token}"))
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        // The body is not echoed: on this route it can carry credentials.
        return Err(format!("master returned HTTP {status}").into());
    }

    let payload: PulledAccounts = response.json().await?;
    if payload.contract != SUPPORTED_CONTRACT {
        return Err(format!(
            "master speaks replica contract v{}, this build understands v{SUPPORTED_CONTRACT}; \
             upgrade both sides before syncing",
            payload.contract
        )
        .into());
    }
    if payload.accounts.is_empty() {
        // Never let an empty read look like success: it would leave stale tokens in place until
        // they expire, and a replica cannot mint new ones.
        return Err("master returned no accounts".into());
    }

    let repo = store.accounts();
    let existing: HashSet<String> = repo.list().await?.into_iter().map(|a| a.id).collect();

    let mut report = PullReport {
        updated: 0,
        inserted: 0,
        unchanged: 0,
    };

    for pulled in payload.accounts {
        let tokens = PlainTokens {
            access_token: pulled.access_token,
            refresh_token: pulled.refresh_token,
            id_token: pulled.id_token,
        };

        if existing.contains(&pulled.id) {
            // Skip a write when nothing rotated. Every write bumps the token generation, which
            // invalidates the serving token cache — on a 5-minute timer that would evict a warm
            // cache all day for no reason.
            let current = repo.decrypt_tokens(&pulled.id, cipher).await.ok().flatten();
            let same = current.is_some_and(|c| {
                c.access_token == tokens.access_token && c.refresh_token == tokens.refresh_token
            });
            if same {
                report.unchanged += 1;
                continue;
            }
            repo.update_oauth_tokens(
                &pulled.id,
                &tokens,
                cipher,
                pulled.last_refresh,
                pulled.access_token_expires_at,
            )
            .await?;
            report.updated += 1;
        } else {
            let account = Account {
                id: pulled.id.clone(),
                chatgpt_account_id: pulled.chatgpt_account_id,
                chatgpt_user_id: None,
                email: pulled.email,
                alias: pulled.alias,
                workspace_id: None,
                workspace_label: None,
                seat_type: None,
                plan_type: pulled.plan_type,
                routing_policy: pulled.routing_policy,
                last_refresh: pulled.last_refresh,
                created_at: pulled.last_refresh,
                status: pulled.status,
                deactivation_reason: None,
                reset_at: None,
                blocked_at: None,
                security_work_authorized: pulled.security_work_authorized,
                usage_cap_percent: None,
                usage_cap_override: false,
                provider: pulled.provider,
                pool: pulled.pool,
            };
            repo.insert(&account, &tokens, cipher).await?;
            // `insert` does not carry these, and `auth_mode` DEFAULTS to 'codex_oauth' — an
            // Anthropic account left at the default would be refreshed down the Codex path and
            // serve a Bearer token to the wrong upstream. Set them explicitly.
            sqlx::query(
                "UPDATE accounts SET auth_mode = ?, access_token_expires_at = ?, \
                 granted_scopes = ?, oauth_contract_version = ? WHERE id = ?",
            )
            .bind(&pulled.auth_mode)
            .bind(pulled.access_token_expires_at)
            .bind(pulled.granted_scopes.as_deref())
            .bind(pulled.oauth_contract_version.as_deref())
            .bind(&pulled.id)
            .execute(store.pool())
            .await?;
            report.inserted += 1;
        }
    }

    Ok(report)
}
