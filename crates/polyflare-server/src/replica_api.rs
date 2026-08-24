//! The master half of master/replica: a stable contract a replica pulls its credentials from.
//!
//! # Why this exists rather than copying the table
//!
//! The first version shipped `sqlite3 .dump accounts` over SSH. That works, but `.dump` emits
//! POSITIONAL inserts, so the wire format IS the schema: any migration either breaks the sync or —
//! with a same-count column reorder — writes values into the wrong columns of a table holding
//! encrypted credentials. This endpoint decouples the two. The master serialises named fields; the
//! schema underneath is free to change.
//!
//! # What a replica gets, and what it deliberately does not
//!
//! Only what is needed to SERVE: identity, routing metadata, status, and the current credentials.
//! Notably absent:
//!
//! - **`continuity_*`** — anchors are owned by one account on one node. Replicating them across
//!   nodes recreates exactly the cross-node anchor confusion they exist to prevent.
//! - **request history** — that flows the other way (replica → master), and never down.
//! - **quota/usage** — both nodes poll upstream directly, which is authoritative there. Shipping a
//!   stale copy down would be worse than no copy.
//!
//! # Secrets
//!
//! This returns live OAuth access and refresh tokens. It sits inside the `require_admin` router, so
//! it carries the same gate as every other `/api/*` route and is never reachable unauthenticated.
//! Tokens are returned DECRYPTED because the replica must present them upstream; the transport is
//! the operator's own tailnet. Nothing here is logged.

use std::sync::Arc;

use axum::{extract::State, http::header, http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;

use crate::app::AppState;

/// One account as a replica needs it. Field names are the CONTRACT — a replica parses these, so
/// renaming one is a breaking change even if the column behind it is renamed freely.
#[derive(Serialize)]
pub struct ReplicaAccountView {
    pub id: String,
    pub provider: String,
    pub email: String,
    pub alias: Option<String>,
    pub plan_type: String,
    pub routing_policy: String,
    pub status: String,
    pub pool: Option<String>,
    pub chatgpt_account_id: Option<String>,
    pub auth_mode: String,
    pub access_token: String,
    pub refresh_token: String,
    pub id_token: String,
    pub access_token_expires_at: Option<i64>,
    pub granted_scopes: Option<String>,
    pub oauth_contract_version: Option<String>,
    pub last_refresh: i64,
    pub security_work_authorized: bool,
}

#[derive(Serialize)]
pub struct ReplicaAccountsView {
    /// Contract version. A replica refuses a payload it does not understand rather than guessing,
    /// so an incompatible master can never half-apply credentials.
    pub contract: u32,
    /// When the master built this snapshot (unix seconds) — lets a replica report its own staleness.
    pub generated_at: i64,
    pub accounts: Vec<ReplicaAccountView>,
}

/// The version this build serves. Bump only on a BREAKING change to the field set.
pub const REPLICA_CONTRACT_VERSION: u32 = 1;

fn unix_now() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// `GET /api/replica/accounts` — every account with its current credentials.
///
/// A decrypt failure drops that ONE account rather than failing the whole pull: a replica serving
/// seven of eight accounts is strictly better than one serving none, and the master's own logs
/// already surface a broken cipher.
pub async fn replica_accounts_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let repo = state.store.accounts();
    let accounts = match repo.list().await {
        Ok(a) => a,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    };

    let mut out = Vec::with_capacity(accounts.len());
    for account in accounts {
        let Ok(Some((_, tokens, auth))) = repo
            .get_with_tokens_and_auth(&account.id, &state.cipher)
            .await
        else {
            continue;
        };
        out.push(ReplicaAccountView {
            id: account.id,
            provider: account.provider,
            email: account.email,
            alias: account.alias,
            plan_type: account.plan_type,
            routing_policy: account.routing_policy,
            status: account.status,
            pool: account.pool,
            chatgpt_account_id: account.chatgpt_account_id,
            auth_mode: auth.mode().as_str().to_string(),
            // Cloned, not moved: `PlainTokens` zeroizes on drop, and moving the fields out would
            // defeat that. The clones live only as long as this response body.
            access_token: tokens.access_token.clone(),
            refresh_token: tokens.refresh_token.clone(),
            id_token: tokens.id_token.clone(),
            access_token_expires_at: auth.access_token_expires_at,
            granted_scopes: auth.granted_scopes.clone(),
            oauth_contract_version: auth.oauth_contract_version.clone(),
            last_refresh: account.last_refresh,
            security_work_authorized: account.security_work_authorized,
        });
    }

    (
        // Credentials must not be cacheable anywhere between here and the replica.
        [(header::CACHE_CONTROL, "no-store")],
        Json(ReplicaAccountsView {
            contract: REPLICA_CONTRACT_VERSION,
            generated_at: unix_now(),
            accounts: out,
        }),
    )
        .into_response()
}
