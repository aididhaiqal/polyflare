//! Persistence for operator-configured model providers.
//!
//! Providers, credentials, and models are separate records on purpose: endpoint/protocol policy
//! belongs to a provider, an encrypted credential is an independently selectable routing target,
//! and a model is a catalog/routing declaration. None of the public row types carries plaintext
//! secret material.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use sqlx::sqlite::SqlitePool;

use crate::{StoreError, TokenCipher};

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct CustomProvider {
    pub id: String,
    pub slug: String,
    pub display_name: String,
    pub base_url: String,
    pub wire_api: String,
    pub enabled: bool,
    pub stateless_responses: bool,
    pub allow_private_hosts: bool,
    pub connect_timeout_ms: i64,
    pub stream_idle_timeout_ms: i64,
    pub request_max_retries: i64,
    pub max_concurrency: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewCustomProvider {
    pub id: String,
    pub slug: String,
    pub display_name: String,
    pub base_url: String,
    pub wire_api: String,
    pub enabled: bool,
    pub stateless_responses: bool,
    pub allow_private_hosts: bool,
    pub connect_timeout_ms: i64,
    pub stream_idle_timeout_ms: i64,
    pub request_max_retries: i64,
    pub max_concurrency: Option<i64>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct ProviderCredential {
    pub id: String,
    pub provider_id: String,
    pub label: String,
    pub enabled: bool,
    pub health_status: String,
    pub routing_weight: f64,
    pub max_concurrency: Option<i64>,
    pub cooldown_until: Option<i64>,
    pub last_error_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, zeroize::Zeroize, zeroize::ZeroizeOnDrop)]
pub struct ProviderCredentialSecret(pub String);

impl std::fmt::Debug for ProviderCredentialSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ProviderCredentialSecret(***)")
    }
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct ProviderModel {
    pub id: String,
    pub provider_id: String,
    pub public_model: String,
    pub upstream_model: String,
    pub display_name: String,
    pub context_window: Option<i64>,
    pub max_output_tokens: Option<i64>,
    pub supports_tools: bool,
    pub supports_vision: bool,
    pub supports_parallel_tool_calls: bool,
    pub supports_web_search: bool,
    pub supports_reasoning_summaries: bool,
    pub reasoning_levels_json: String,
    pub model_info_json: Option<String>,
    pub input_per_million: Option<f64>,
    pub cached_input_per_million: Option<f64>,
    pub output_per_million: Option<f64>,
    pub visible_in_codex: bool,
    pub visible_in_openai: bool,
    pub enabled: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewProviderModel {
    pub id: String,
    pub provider_id: String,
    pub public_model: String,
    pub upstream_model: String,
    pub display_name: String,
    pub context_window: Option<i64>,
    pub max_output_tokens: Option<i64>,
    pub supports_tools: bool,
    pub supports_vision: bool,
    pub supports_parallel_tool_calls: bool,
    pub supports_web_search: bool,
    pub supports_reasoning_summaries: bool,
    pub reasoning_levels_json: String,
    pub model_info_json: Option<String>,
    pub input_per_million: Option<f64>,
    pub cached_input_per_million: Option<f64>,
    pub output_per_million: Option<f64>,
    pub visible_in_codex: bool,
    pub visible_in_openai: bool,
    pub enabled: bool,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct ProviderRepo {
    pool: SqlitePool,
    generation: Arc<AtomicU64>,
}

impl ProviderRepo {
    pub(crate) fn new(pool: SqlitePool, generation: Arc<AtomicU64>) -> Self {
        Self { pool, generation }
    }

    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    pub async fn create_provider(&self, provider: &NewCustomProvider) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO custom_providers \
             (id, slug, display_name, base_url, wire_api, enabled, stateless_responses, \
              allow_private_hosts, connect_timeout_ms, stream_idle_timeout_ms, \
              request_max_retries, max_concurrency, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&provider.id)
        .bind(&provider.slug)
        .bind(&provider.display_name)
        .bind(&provider.base_url)
        .bind(&provider.wire_api)
        .bind(provider.enabled)
        .bind(provider.stateless_responses)
        .bind(provider.allow_private_hosts)
        .bind(provider.connect_timeout_ms)
        .bind(provider.stream_idle_timeout_ms)
        .bind(provider.request_max_retries)
        .bind(provider.max_concurrency)
        .bind(provider.created_at)
        .bind(provider.created_at)
        .execute(&self.pool)
        .await?;
        self.bump_generation();
        Ok(())
    }

    pub async fn list_providers(&self) -> Result<Vec<CustomProvider>, StoreError> {
        Ok(sqlx::query_as::<_, CustomProvider>(
            "SELECT id, slug, display_name, base_url, wire_api, enabled, stateless_responses, \
             allow_private_hosts, connect_timeout_ms, stream_idle_timeout_ms, \
             request_max_retries, max_concurrency, created_at, updated_at \
             FROM custom_providers ORDER BY display_name, id",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn get_provider(&self, id: &str) -> Result<Option<CustomProvider>, StoreError> {
        Ok(sqlx::query_as::<_, CustomProvider>(
            "SELECT id, slug, display_name, base_url, wire_api, enabled, stateless_responses, \
             allow_private_hosts, connect_timeout_ms, stream_idle_timeout_ms, \
             request_max_retries, max_concurrency, created_at, updated_at \
             FROM custom_providers WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn set_provider_enabled(
        &self,
        id: &str,
        enabled: bool,
        now: i64,
    ) -> Result<bool, StoreError> {
        let changed =
            sqlx::query("UPDATE custom_providers SET enabled = ?, updated_at = ? WHERE id = ?")
                .bind(enabled)
                .bind(now)
                .bind(id)
                .execute(&self.pool)
                .await?
                .rows_affected()
                == 1;
        if changed {
            self.bump_generation();
        }
        Ok(changed)
    }

    pub async fn delete_provider(&self, id: &str) -> Result<bool, StoreError> {
        let changed = sqlx::query("DELETE FROM custom_providers WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected()
            == 1;
        if changed {
            self.bump_generation();
        }
        Ok(changed)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_credential(
        &self,
        id: &str,
        provider_id: &str,
        label: &str,
        api_key: &str,
        routing_weight: f64,
        max_concurrency: Option<i64>,
        now: i64,
        cipher: &TokenCipher,
    ) -> Result<(), StoreError> {
        let encrypted = cipher.encrypt(api_key)?;
        sqlx::query(
            "INSERT INTO provider_credentials \
             (id, provider_id, label, api_key_enc, enabled, health_status, routing_weight, \
              max_concurrency, created_at, updated_at) \
             VALUES (?, ?, ?, ?, 1, 'healthy', ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(provider_id)
        .bind(label)
        .bind(encrypted)
        .bind(routing_weight)
        .bind(max_concurrency)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        self.bump_generation();
        Ok(())
    }

    pub async fn list_credentials(
        &self,
        provider_id: &str,
    ) -> Result<Vec<ProviderCredential>, StoreError> {
        Ok(sqlx::query_as::<_, ProviderCredential>(
            "SELECT id, provider_id, label, enabled, health_status, routing_weight, \
             max_concurrency, cooldown_until, last_error_at, created_at, updated_at \
             FROM provider_credentials WHERE provider_id = ? ORDER BY label, id",
        )
        .bind(provider_id)
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn set_credential_enabled(
        &self,
        id: &str,
        enabled: bool,
        now: i64,
    ) -> Result<bool, StoreError> {
        let health = if enabled { "healthy" } else { "disabled" };
        let changed = sqlx::query(
            "UPDATE provider_credentials SET enabled = ?, health_status = ?, updated_at = ? \
             WHERE id = ?",
        )
        .bind(enabled)
        .bind(health)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected()
            == 1;
        if changed {
            self.bump_generation();
        }
        Ok(changed)
    }

    pub async fn set_credential_health(
        &self,
        id: &str,
        health_status: &str,
        cooldown_until: Option<i64>,
        now: i64,
    ) -> Result<bool, StoreError> {
        if !matches!(
            health_status,
            "healthy" | "cooldown" | "reauth_required" | "disabled"
        ) {
            return Err(StoreError::InvalidState(
                "invalid provider credential health status".into(),
            ));
        }
        let changed = sqlx::query(
            "UPDATE provider_credentials SET health_status = ?, cooldown_until = ?, \
             last_error_at = CASE WHEN ? = 'healthy' THEN last_error_at ELSE ? END, \
             updated_at = ? WHERE id = ?",
        )
        .bind(health_status)
        .bind(cooldown_until)
        .bind(health_status)
        .bind(now)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected()
            == 1;
        if changed {
            self.bump_generation();
        }
        Ok(changed)
    }

    pub async fn delete_credential(&self, id: &str) -> Result<bool, StoreError> {
        let changed = sqlx::query("DELETE FROM provider_credentials WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected()
            == 1;
        if changed {
            self.bump_generation();
        }
        Ok(changed)
    }

    pub async fn decrypt_credential(
        &self,
        id: &str,
        cipher: &TokenCipher,
    ) -> Result<Option<(ProviderCredential, ProviderCredentialSecret)>, StoreError> {
        #[derive(sqlx::FromRow)]
        struct WithSecret {
            id: String,
            provider_id: String,
            label: String,
            enabled: bool,
            health_status: String,
            routing_weight: f64,
            max_concurrency: Option<i64>,
            cooldown_until: Option<i64>,
            last_error_at: Option<i64>,
            created_at: i64,
            updated_at: i64,
            api_key_enc: Vec<u8>,
        }

        let Some(row) = sqlx::query_as::<_, WithSecret>(
            "SELECT id, provider_id, label, enabled, health_status, routing_weight, \
             max_concurrency, cooldown_until, last_error_at, created_at, updated_at, api_key_enc \
             FROM provider_credentials WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };
        let secret = ProviderCredentialSecret(cipher.decrypt(&row.api_key_enc)?);
        let credential = ProviderCredential {
            id: row.id,
            provider_id: row.provider_id,
            label: row.label,
            enabled: row.enabled,
            health_status: row.health_status,
            routing_weight: row.routing_weight,
            max_concurrency: row.max_concurrency,
            cooldown_until: row.cooldown_until,
            last_error_at: row.last_error_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        };
        Ok(Some((credential, secret)))
    }

    pub async fn create_model(&self, model: &NewProviderModel) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO provider_models \
             (id, provider_id, public_model, upstream_model, display_name, context_window, \
              max_output_tokens, supports_tools, supports_vision, supports_parallel_tool_calls, \
              supports_web_search, supports_reasoning_summaries, reasoning_levels_json, \
              model_info_json, input_per_million, cached_input_per_million, output_per_million, \
              visible_in_codex, visible_in_openai, enabled, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&model.id)
        .bind(&model.provider_id)
        .bind(&model.public_model)
        .bind(&model.upstream_model)
        .bind(&model.display_name)
        .bind(model.context_window)
        .bind(model.max_output_tokens)
        .bind(model.supports_tools)
        .bind(model.supports_vision)
        .bind(model.supports_parallel_tool_calls)
        .bind(model.supports_web_search)
        .bind(model.supports_reasoning_summaries)
        .bind(&model.reasoning_levels_json)
        .bind(&model.model_info_json)
        .bind(model.input_per_million)
        .bind(model.cached_input_per_million)
        .bind(model.output_per_million)
        .bind(model.visible_in_codex)
        .bind(model.visible_in_openai)
        .bind(model.enabled)
        .bind(model.created_at)
        .bind(model.created_at)
        .execute(&self.pool)
        .await?;
        self.bump_generation();
        Ok(())
    }

    pub async fn list_models(&self, provider_id: &str) -> Result<Vec<ProviderModel>, StoreError> {
        Ok(sqlx::query_as::<_, ProviderModel>(
            "SELECT id, provider_id, public_model, upstream_model, display_name, context_window, \
             max_output_tokens, supports_tools, supports_vision, supports_parallel_tool_calls, \
             supports_web_search, supports_reasoning_summaries, reasoning_levels_json, \
             model_info_json, input_per_million, cached_input_per_million, output_per_million, \
             visible_in_codex, visible_in_openai, enabled, created_at, updated_at FROM provider_models \
             WHERE provider_id = ? ORDER BY display_name, id",
        )
        .bind(provider_id)
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn set_model_enabled(
        &self,
        id: &str,
        enabled: bool,
        now: i64,
    ) -> Result<bool, StoreError> {
        let changed =
            sqlx::query("UPDATE provider_models SET enabled = ?, updated_at = ? WHERE id = ?")
                .bind(enabled)
                .bind(now)
                .bind(id)
                .execute(&self.pool)
                .await?
                .rows_affected()
                == 1;
        if changed {
            self.bump_generation();
        }
        Ok(changed)
    }

    pub async fn update_model_policy(
        &self,
        id: &str,
        enabled: Option<bool>,
        visible_in_codex: Option<bool>,
        visible_in_openai: Option<bool>,
        now: i64,
    ) -> Result<bool, StoreError> {
        let changed = sqlx::query(
            "UPDATE provider_models SET \
             enabled = COALESCE(?, enabled), \
             visible_in_codex = COALESCE(?, visible_in_codex), \
             visible_in_openai = COALESCE(?, visible_in_openai), \
             updated_at = ? WHERE id = ?",
        )
        .bind(enabled)
        .bind(visible_in_codex)
        .bind(visible_in_openai)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected()
            == 1;
        if changed {
            self.bump_generation();
        }
        Ok(changed)
    }

    pub async fn delete_model(&self, id: &str) -> Result<bool, StoreError> {
        let changed = sqlx::query("DELETE FROM provider_models WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected()
            == 1;
        if changed {
            self.bump_generation();
        }
        Ok(changed)
    }

    pub async fn resolve_model(
        &self,
        public_model: &str,
    ) -> Result<Option<(CustomProvider, ProviderModel)>, StoreError> {
        let Some(provider_id) = sqlx::query_scalar::<_, String>(
            "SELECT provider_id FROM provider_models \
             WHERE public_model = ? AND enabled = 1",
        )
        .bind(public_model)
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };
        let Some(provider) = self.get_provider(&provider_id).await? else {
            return Ok(None);
        };
        if !provider.enabled {
            return Ok(None);
        }
        let model = sqlx::query_as::<_, ProviderModel>(
            "SELECT id, provider_id, public_model, upstream_model, display_name, context_window, \
             max_output_tokens, supports_tools, supports_vision, supports_parallel_tool_calls, \
             supports_web_search, supports_reasoning_summaries, reasoning_levels_json, \
             model_info_json, input_per_million, cached_input_per_million, output_per_million, \
             visible_in_codex, visible_in_openai, enabled, created_at, updated_at \
             FROM provider_models WHERE public_model = ?",
        )
        .bind(public_model)
        .fetch_one(&self.pool)
        .await?;
        Ok(Some((provider, model)))
    }

    pub async fn list_enabled_models(
        &self,
    ) -> Result<Vec<(CustomProvider, ProviderModel)>, StoreError> {
        let providers = self.list_providers().await?;
        let mut out = Vec::new();
        for provider in providers.into_iter().filter(|provider| provider.enabled) {
            for model in self
                .list_models(&provider.id)
                .await?
                .into_iter()
                .filter(|model| model.enabled)
            {
                out.push((provider.clone(), model));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;

    fn provider(now: i64) -> NewCustomProvider {
        NewCustomProvider {
            id: "provider-sakana".into(),
            slug: "sakana".into(),
            display_name: "Sakana".into(),
            base_url: "https://api.sakana.ai/v1".into(),
            wire_api: "responses".into(),
            enabled: true,
            stateless_responses: true,
            allow_private_hosts: false,
            connect_timeout_ms: 10_000,
            stream_idle_timeout_ms: 7_200_000,
            request_max_retries: 4,
            max_concurrency: Some(8),
            created_at: now,
        }
    }

    fn model(now: i64) -> NewProviderModel {
        NewProviderModel {
            id: "model-fugu-ultra".into(),
            provider_id: "provider-sakana".into(),
            public_model: "fugu-ultra".into(),
            upstream_model: "fugu-ultra-v1.1".into(),
            display_name: "Fugu Ultra".into(),
            context_window: Some(1_000_000),
            max_output_tokens: None,
            supports_tools: true,
            supports_vision: true,
            supports_parallel_tool_calls: true,
            supports_web_search: true,
            supports_reasoning_summaries: true,
            reasoning_levels_json: r#"["high","xhigh","max"]"#.into(),
            model_info_json: None,
            input_per_million: Some(1.0),
            cached_input_per_million: Some(0.5),
            output_per_million: Some(4.0),
            visible_in_codex: true,
            visible_in_openai: true,
            enabled: true,
            created_at: now,
        }
    }

    #[tokio::test]
    async fn provider_credential_and_model_round_trip_without_exposing_secret() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let cipher = TokenCipher::load_or_create(&dir.path().join("key")).unwrap();
        let repo = store.providers();
        repo.create_provider(&provider(10)).await.unwrap();
        repo.create_credential(
            "cred-1",
            "provider-sakana",
            "primary",
            "fish_secret",
            1.0,
            Some(4),
            10,
            &cipher,
        )
        .await
        .unwrap();
        repo.create_model(&model(10)).await.unwrap();

        let public = repo.list_credentials("provider-sakana").await.unwrap();
        assert_eq!(public[0].label, "primary");
        assert!(!format!("{public:?}").contains("fish_secret"));
        let (_, secret) = repo
            .decrypt_credential("cred-1", &cipher)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(secret.0, "fish_secret");
        assert!(!format!("{secret:?}").contains("fish_secret"));

        let (resolved_provider, resolved_model) =
            repo.resolve_model("fugu-ultra").await.unwrap().unwrap();
        assert_eq!(resolved_provider.slug, "sakana");
        assert_eq!(resolved_model.upstream_model, "fugu-ultra-v1.1");
        assert!(resolved_model.visible_in_codex);
        assert!(resolved_model.visible_in_openai);

        assert!(repo
            .update_model_policy("model-fugu-ultra", None, Some(false), Some(false), 11,)
            .await
            .unwrap());
        let routed = repo.resolve_model("fugu-ultra").await.unwrap().unwrap().1;
        assert!(
            routed.enabled && !routed.visible_in_codex && !routed.visible_in_openai,
            "catalog visibility must not disable an explicitly addressed route"
        );
    }

    #[tokio::test]
    async fn disabled_provider_is_not_resolved_but_history_rows_remain_independent() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.providers();
        repo.create_provider(&provider(10)).await.unwrap();
        repo.create_model(&model(10)).await.unwrap();
        assert!(repo.resolve_model("fugu-ultra").await.unwrap().is_some());
        assert!(repo
            .set_provider_enabled("provider-sakana", false, 11)
            .await
            .unwrap());
        assert!(repo.resolve_model("fugu-ultra").await.unwrap().is_none());
    }
}
