//! Repository over `dashboard_passkeys` + `dashboard_sessions` — passkey (WebAuthn) sign-in for
//! the dashboard.
//!
//! Two deliberate asymmetries with the rest of this crate:
//!
//! - **Passkey material is NOT encrypted at rest.** `credential_json` holds only the credential id,
//!   the COSE *public* key, and a signature counter. The private key never leaves the
//!   authenticator, so there is nothing here an attacker could use to forge an assertion — unlike
//!   the account tokens, which are ciphertext precisely because they ARE the secret.
//! - **Session tokens are stored only as SHA-256 hashes**, the same rule `api_keys` follows: a
//!   reader of this database must not be able to replay a live dashboard session.

use sqlx::sqlite::SqlitePool;

use crate::StoreError;

/// One registered passkey. Carries no private material — see the module docs.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PasskeyRow {
    pub id: String,
    pub credential_id: String,
    /// webauthn-rs's serialized `Passkey` (public key + signature counter).
    pub credential_json: String,
    pub label: String,
    pub created_at: i64,
    pub last_used_at: Option<i64>,
}

/// Cheap to construct (clones the pool handle).
#[derive(Clone)]
pub struct PasskeyRepo {
    pool: SqlitePool,
}

impl PasskeyRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Whether ANY passkey is registered. This is the switch that closes the tokenless local
    /// bypass: once an operator can sign in, "anything on loopback is trusted" stops being the
    /// posture (see `polyflare_server::auth`). Called per dashboard request, so it stays a single
    /// indexed count rather than a row fetch.
    pub async fn any_registered(&self) -> Result<bool, StoreError> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM dashboard_passkeys")
                .fetch_one(&self.pool)
                .await?
                > 0,
        )
    }

    pub async fn list(&self) -> Result<Vec<PasskeyRow>, StoreError> {
        Ok(sqlx::query_as::<_, PasskeyRow>(
            "SELECT id, credential_id, credential_json, label, created_at, last_used_at \
             FROM dashboard_passkeys ORDER BY created_at",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn insert(
        &self,
        id: &str,
        credential_id: &str,
        credential_json: &str,
        label: &str,
        now: i64,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO dashboard_passkeys (id, credential_id, credential_json, label, created_at) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(credential_id)
        .bind(credential_json)
        .bind(label)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Persist the post-assertion credential state. WebAuthn's signature counter is a
    /// cloned-authenticator signal, so a successful assertion MUST write back the counter the
    /// authenticator reported or the check is inert on the next sign-in.
    pub async fn update_after_use(
        &self,
        id: &str,
        credential_json: &str,
        now: i64,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE dashboard_passkeys SET credential_json = ?, last_used_at = ? WHERE id = ?",
        )
        .bind(credential_json)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Returns whether a row was removed. Deleting a passkey cascades to its sessions, so revoking
    /// a lost device also ends whatever it was already signed into.
    pub async fn delete(&self, id: &str) -> Result<bool, StoreError> {
        Ok(sqlx::query("DELETE FROM dashboard_passkeys WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected()
            > 0)
    }

    /// Record a session for a validated assertion. `token_hash` is the SHA-256 of the token handed
    /// to the browser; the raw token is never stored.
    pub async fn create_session(
        &self,
        token_hash: &str,
        passkey_id: &str,
        now: i64,
        expires_at: i64,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO dashboard_sessions (token_hash, passkey_id, created_at, expires_at) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(token_hash)
        .bind(passkey_id)
        .bind(now)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Whether `token_hash` names a live (unexpired) session. The expiry is enforced in the query
    /// itself so a stale row can never authenticate even if the sweeper has not run.
    pub async fn session_is_valid(&self, token_hash: &str, now: i64) -> Result<bool, StoreError> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM dashboard_sessions WHERE token_hash = ? AND expires_at > ?",
        )
        .bind(token_hash)
        .bind(now)
        .fetch_one(&self.pool)
        .await?
            > 0)
    }

    pub async fn delete_session(&self, token_hash: &str) -> Result<(), StoreError> {
        sqlx::query("DELETE FROM dashboard_sessions WHERE token_hash = ?")
            .bind(token_hash)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Drop expired sessions. Opportunistic, called from the sign-in path.
    pub async fn prune_sessions(&self, now: i64) -> Result<(), StoreError> {
        sqlx::query("DELETE FROM dashboard_sessions WHERE expires_at <= ?")
            .bind(now)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::Store;

    #[tokio::test]
    async fn passkeys_gate_the_local_bypass_and_sessions_expire() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.passkeys();

        assert!(!repo.any_registered().await.unwrap(), "empty store is open");
        repo.insert("pk-1", "cred-1", "{}", "Touch ID", 100)
            .await
            .unwrap();
        assert!(repo.any_registered().await.unwrap(), "a passkey closes it");

        repo.create_session("hash-1", "pk-1", 100, 200)
            .await
            .unwrap();
        assert!(repo.session_is_valid("hash-1", 150).await.unwrap());
        // Expiry is enforced by the lookup, not only by the sweeper.
        assert!(!repo.session_is_valid("hash-1", 200).await.unwrap());
        assert!(!repo.session_is_valid("nope", 150).await.unwrap());

        // Revoking a passkey ends the sessions it minted.
        assert!(repo.delete("pk-1").await.unwrap());
        assert!(!repo.session_is_valid("hash-1", 150).await.unwrap());
        assert!(!repo.any_registered().await.unwrap());
    }
}
