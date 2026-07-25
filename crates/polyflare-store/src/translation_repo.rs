//! Persisted cross-protocol translation routes.
//!
//! Matching is deliberately performed by SQLite in one bounded query so ingress sees edits on
//! the next request without a process restart or a cache-invalidation race.

use sqlx::sqlite::SqlitePool;

use crate::StoreError;

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct TranslationRoute {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub source_protocol: String,
    pub match_kind: String,
    pub model_pattern: String,
    pub target_kind: String,
    pub target_provider_id: String,
    pub target_model: String,
    pub reasoning_effort: Option<String>,
    pub priority: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTranslationRoute {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub source_protocol: String,
    pub match_kind: String,
    pub model_pattern: String,
    pub target_kind: String,
    pub target_provider_id: String,
    pub target_model: String,
    pub reasoning_effort: Option<String>,
    pub priority: i64,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranslationRouteUpdate {
    pub name: String,
    pub enabled: bool,
    pub source_protocol: String,
    pub match_kind: String,
    pub model_pattern: String,
    pub target_kind: String,
    pub target_provider_id: String,
    pub target_model: String,
    pub reasoning_effort: Option<String>,
    pub priority: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone)]
pub struct TranslationRepo {
    pool: SqlitePool,
}

impl TranslationRepo {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn list(&self) -> Result<Vec<TranslationRoute>, StoreError> {
        Ok(sqlx::query_as::<_, TranslationRoute>(
            "SELECT id, name, enabled, source_protocol, match_kind, model_pattern, \
             target_kind, target_provider_id, target_model, reasoning_effort, priority, created_at, updated_at \
             FROM translation_routes ORDER BY priority, id",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn get(&self, id: &str) -> Result<Option<TranslationRoute>, StoreError> {
        Ok(sqlx::query_as::<_, TranslationRoute>(
            "SELECT id, name, enabled, source_protocol, match_kind, model_pattern, \
             target_kind, target_provider_id, target_model, reasoning_effort, priority, created_at, updated_at \
             FROM translation_routes WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn create(&self, route: &NewTranslationRoute) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO translation_routes \
             (id, name, enabled, legacy_source_protocol, source_protocol, match_kind, model_pattern, \
              legacy_target_provider, target_kind, target_provider_id, target_model, \
              reasoning_effort, priority, created_at, updated_at) \
             VALUES (?, ?, ?, 'anthropic_messages', ?, ?, ?, 'codex', ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&route.id)
        .bind(&route.name)
        .bind(route.enabled)
        .bind(&route.source_protocol)
        .bind(&route.match_kind)
        .bind(&route.model_pattern)
        .bind(&route.target_kind)
        .bind(&route.target_provider_id)
        .bind(&route.target_model)
        .bind(&route.reasoning_effort)
        .bind(route.priority)
        .bind(route.created_at)
        .bind(route.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update(
        &self,
        id: &str,
        route: &TranslationRouteUpdate,
    ) -> Result<bool, StoreError> {
        let result = sqlx::query(
            "UPDATE translation_routes SET name = ?, enabled = ?, source_protocol = ?, \
             match_kind = ?, model_pattern = ?, target_kind = ?, target_provider_id = ?, target_model = ?, \
             reasoning_effort = ?, priority = ?, updated_at = ? WHERE id = ?",
        )
        .bind(&route.name)
        .bind(route.enabled)
        .bind(&route.source_protocol)
        .bind(&route.match_kind)
        .bind(&route.model_pattern)
        .bind(&route.target_kind)
        .bind(&route.target_provider_id)
        .bind(&route.target_model)
        .bind(&route.reasoning_effort)
        .bind(route.priority)
        .bind(route.updated_at)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn delete(&self, id: &str) -> Result<bool, StoreError> {
        let result = sqlx::query("DELETE FROM translation_routes WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Resolve the first enabled route. Priority is ascending and the stable id breaks ties.
    pub async fn resolve(
        &self,
        source_protocol: &str,
        model: &str,
    ) -> Result<Option<TranslationRoute>, StoreError> {
        Ok(sqlx::query_as::<_, TranslationRoute>(
            "SELECT id, name, enabled, source_protocol, match_kind, model_pattern, \
             target_kind, target_provider_id, target_model, reasoning_effort, priority, created_at, updated_at \
             FROM translation_routes \
             WHERE enabled = 1 AND source_protocol = ? AND ( \
                 (match_kind = 'exact' AND lower(?) = lower(model_pattern)) OR \
                 (match_kind = 'prefix' AND instr(lower(?), lower(model_pattern)) = 1) OR \
                 (match_kind = 'contains' AND instr(lower(?), lower(model_pattern)) > 0) \
             ) ORDER BY priority, id LIMIT 1",
        )
        .bind(source_protocol)
        .bind(model)
        .bind(model)
        .bind(model)
        .fetch_optional(&self.pool)
        .await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;

    #[tokio::test]
    async fn migration_seeds_compatible_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let routes = store.translations().list().await.unwrap();

        assert_eq!(routes.len(), 3);
        assert_eq!(routes[0].model_pattern, "opus");
        assert_eq!(routes[0].target_model, "gpt-5.6-sol");
        assert_eq!(routes[0].reasoning_effort.as_deref(), Some("high"));
    }

    #[tokio::test]
    async fn resolution_is_case_insensitive_and_honors_kind_priority_and_enabled() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.translations();
        let now = 1_000;

        repo.create(&NewTranslationRoute {
            id: "exact".into(),
            name: "Exact".into(),
            enabled: true,
            source_protocol: "anthropic_messages".into(),
            match_kind: "exact".into(),
            model_pattern: "CLAUDE-OPUS-SPECIAL".into(),
            target_kind: "builtin_provider".into(),
            target_provider_id: "codex".into(),
            target_model: "exact-target".into(),
            reasoning_effort: None,
            priority: 10,
            created_at: now,
        })
        .await
        .unwrap();
        repo.create(&NewTranslationRoute {
            id: "prefix".into(),
            name: "Prefix".into(),
            enabled: true,
            source_protocol: "anthropic_messages".into(),
            match_kind: "prefix".into(),
            model_pattern: "claude-opus".into(),
            target_kind: "builtin_provider".into(),
            target_provider_id: "codex".into(),
            target_model: "prefix-target".into(),
            reasoning_effort: None,
            priority: 20,
            created_at: now,
        })
        .await
        .unwrap();

        let exact = repo
            .resolve("anthropic_messages", "claude-opus-special")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(exact.id, "exact");

        let prefix = repo
            .resolve("anthropic_messages", "CLAUDE-OPUS-OTHER")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(prefix.id, "prefix");

        repo.update(
            "prefix",
            &TranslationRouteUpdate {
                name: "Prefix".into(),
                enabled: false,
                source_protocol: "anthropic_messages".into(),
                match_kind: "prefix".into(),
                model_pattern: "claude-opus".into(),
                target_kind: "builtin_provider".into(),
                target_provider_id: "codex".into(),
                target_model: "prefix-target".into(),
                reasoning_effort: None,
                priority: 20,
                updated_at: now + 1,
            },
        )
        .await
        .unwrap();

        let seeded = repo
            .resolve("anthropic_messages", "claude-opus-other")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(seeded.id, "default-anthropic-opus");
    }

    #[tokio::test]
    async fn crud_round_trips_and_delete_removes_the_route() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let repo = store.translations();
        repo.create(&NewTranslationRoute {
            id: "custom".into(),
            name: "Custom".into(),
            enabled: true,
            source_protocol: "anthropic_messages".into(),
            match_kind: "contains".into(),
            model_pattern: "custom".into(),
            target_kind: "builtin_provider".into(),
            target_provider_id: "codex".into(),
            target_model: "gpt-custom".into(),
            reasoning_effort: Some("medium".into()),
            priority: 50,
            created_at: 10,
        })
        .await
        .unwrap();
        assert_eq!(repo.get("custom").await.unwrap().unwrap().name, "Custom");

        assert!(repo
            .update(
                "custom",
                &TranslationRouteUpdate {
                    name: "Updated".into(),
                    enabled: false,
                    source_protocol: "anthropic_messages".into(),
                    match_kind: "exact".into(),
                    model_pattern: "custom-v2".into(),
                    target_kind: "builtin_provider".into(),
                    target_provider_id: "codex".into(),
                    target_model: "gpt-custom-v2".into(),
                    reasoning_effort: None,
                    priority: 5,
                    updated_at: 11,
                },
            )
            .await
            .unwrap());
        let updated = repo.get("custom").await.unwrap().unwrap();
        assert_eq!(updated.name, "Updated");
        assert!(!updated.enabled);
        assert_eq!(updated.priority, 5);

        assert!(repo.delete("custom").await.unwrap());
        assert!(repo.get("custom").await.unwrap().is_none());
    }
}
