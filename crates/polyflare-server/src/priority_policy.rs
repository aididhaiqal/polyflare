//! Content-free priority service-tier policy.

use std::collections::HashMap;
use std::sync::RwLock;

use axum::body::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use polyflare_store::Store;

pub const GLOBAL_SETTING_KEY: &str = "priority_policy";
pub const SESSION_SETTING_PREFIX: &str = "priority_session:";
pub const MAX_PRESENCE_MINUTES: u16 = 240;
const MAX_TRACKED_PRESENCE_SESSIONS: usize = 100_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid priority policy")]
pub struct InvalidPriorityPolicy;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverallMode {
    Passthrough,
    ForcePriority,
    ForceStandard,
    Schedule,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionMode {
    Priority,
    Standard,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriorityPolicyConfig {
    pub mode: OverallMode,
    pub active_start_minute: u16,
    pub active_end_minute: u16,
    pub utc_offset_minutes: i16,
    pub presence_minutes: u16,
}

impl Default for PriorityPolicyConfig {
    fn default() -> Self {
        Self {
            mode: OverallMode::Passthrough,
            active_start_minute: 9 * 60,
            active_end_minute: 18 * 60,
            utc_offset_minutes: 0,
            presence_minutes: 30,
        }
    }
}

impl PriorityPolicyConfig {
    pub fn validate(&self) -> bool {
        self.active_start_minute < 24 * 60
            && self.active_end_minute < 24 * 60
            && (-14 * 60..=14 * 60).contains(&self.utc_offset_minutes)
            && (1..=MAX_PRESENCE_MINUTES).contains(&self.presence_minutes)
    }

    fn active_at(&self, now: i64) -> bool {
        let local_minute =
            (now.div_euclid(60) + i64::from(self.utc_offset_minutes)).rem_euclid(24 * 60) as u16;
        match self.active_start_minute.cmp(&self.active_end_minute) {
            std::cmp::Ordering::Equal => true,
            std::cmp::Ordering::Less => {
                local_minute >= self.active_start_minute && local_minute < self.active_end_minute
            }
            std::cmp::Ordering::Greater => {
                local_minute >= self.active_start_minute || local_minute < self.active_end_minute
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriorityDecision {
    Passthrough,
    Priority,
    Standard,
}

pub struct PriorityPolicyRuntime {
    config: RwLock<PriorityPolicyConfig>,
    overrides: RwLock<HashMap<String, SessionMode>>,
    /// Only sessions that were actually granted a presence window are retained. Entries remain
    /// after expiry so continued traffic can never renew a window within this process. The hard
    /// cap keeps adversarial/high-cardinality session traffic bounded; once full, an otherwise-new
    /// session fails closed to standard instead of receiving an untracked renewable window.
    presence_until: Mutex<HashMap<String, i64>>,
}

impl Default for PriorityPolicyRuntime {
    fn default() -> Self {
        Self {
            config: RwLock::new(PriorityPolicyConfig::default()),
            overrides: RwLock::new(HashMap::new()),
            presence_until: Mutex::new(HashMap::new()),
        }
    }
}

impl PriorityPolicyRuntime {
    pub fn config(&self) -> PriorityPolicyConfig {
        self.config
            .read()
            .expect("priority policy lock poisoned")
            .clone()
    }

    pub fn set_config(&self, config: PriorityPolicyConfig) -> Result<(), InvalidPriorityPolicy> {
        if !config.validate() {
            return Err(InvalidPriorityPolicy);
        }
        *self.config.write().expect("priority policy lock poisoned") = config;
        Ok(())
    }

    pub fn session_override(&self, session_key: &str) -> Option<SessionMode> {
        self.overrides
            .read()
            .expect("priority override lock poisoned")
            .get(session_key)
            .copied()
    }

    pub fn set_session_override(&self, session_key: String, mode: Option<SessionMode>) {
        let mut overrides = self
            .overrides
            .write()
            .expect("priority override lock poisoned");
        match mode {
            Some(mode) => {
                overrides.insert(session_key, mode);
            }
            None => {
                overrides.remove(&session_key);
            }
        }
    }

    pub fn load_persisted(&self, settings: &HashMap<String, String>) {
        if let Some(raw) = settings.get(GLOBAL_SETTING_KEY) {
            if let Ok(config) = serde_json::from_str::<PriorityPolicyConfig>(raw) {
                let _ = self.set_config(config);
            }
        }
        for (key, raw) in settings {
            let Some(session_key) = key.strip_prefix(SESSION_SETTING_PREFIX) else {
                continue;
            };
            if valid_session_key(session_key) {
                if let Ok(mode) = serde_json::from_str::<SessionMode>(raw) {
                    self.set_session_override(session_key.to_string(), Some(mode));
                }
            }
        }
    }

    pub async fn decide(
        &self,
        store: &Store,
        session_key: Option<&str>,
        is_subagent: bool,
        now: i64,
    ) -> PriorityDecision {
        if let Some(mode) = session_key.and_then(|key| self.session_override(key)) {
            return match mode {
                SessionMode::Priority => PriorityDecision::Priority,
                SessionMode::Standard => PriorityDecision::Standard,
            };
        }

        let config = self.config();
        match config.mode {
            OverallMode::Passthrough => PriorityDecision::Passthrough,
            OverallMode::ForcePriority => PriorityDecision::Priority,
            OverallMode::ForceStandard => PriorityDecision::Standard,
            OverallMode::Schedule if config.active_at(now) => PriorityDecision::Priority,
            OverallMode::Schedule if is_subagent => PriorityDecision::Standard,
            OverallMode::Schedule => match session_key {
                Some(session_key)
                    if self
                        .observe_main_session(store, session_key, now)
                        .await
                        .is_some_and(|interactive_until| interactive_until > now) =>
                {
                    PriorityDecision::Priority
                }
                _ => PriorityDecision::Standard,
            },
        }
    }

    async fn observe_main_session(
        &self,
        store: &Store,
        session_key: &str,
        now: i64,
    ) -> Option<i64> {
        if let Some(interactive_until) = self.presence_until.lock().await.get(session_key).copied()
        {
            return Some(interactive_until);
        }

        let existed = store
            .request_log()
            .has_main_session_request(session_key)
            .await
            .unwrap_or(true);
        if existed {
            return None;
        }

        let interactive_until =
            now.saturating_add(i64::from(self.config().presence_minutes).saturating_mul(60));
        let mut presence = self.presence_until.lock().await;
        if let Some(existing) = presence.get(session_key) {
            return Some(*existing);
        }
        if presence.len() >= MAX_TRACKED_PRESENCE_SESSIONS {
            return None;
        }
        presence.insert(session_key.to_string(), interactive_until);
        Some(interactive_until)
    }
}

pub fn valid_session_key(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub fn apply_decision(raw: &Bytes, decision: PriorityDecision) -> Option<Bytes> {
    if decision == PriorityDecision::Passthrough {
        return Some(raw.clone());
    }
    let mut value = serde_json::from_slice::<serde_json::Value>(raw).ok()?;
    let object = value.as_object_mut()?;
    match decision {
        PriorityDecision::Priority => {
            object.insert(
                "service_tier".to_string(),
                serde_json::Value::String("priority".to_string()),
            );
        }
        PriorityDecision::Standard => {
            object.remove("service_tier");
        }
        PriorityDecision::Passthrough => unreachable!(),
    }
    serde_json::to_vec(&value).ok().map(Bytes::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyflare_store::{RequestLogRecord, Store};

    async fn store() -> Store {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("priority.db")).await.unwrap();
        std::mem::forget(dir);
        store
    }

    fn inactive_schedule(presence_minutes: u16) -> PriorityPolicyConfig {
        PriorityPolicyConfig {
            mode: OverallMode::Schedule,
            active_start_minute: 60,
            active_end_minute: 120,
            utc_offset_minutes: 0,
            presence_minutes,
        }
    }

    fn request_record(session_key: &str, subagent: Option<&str>) -> RequestLogRecord {
        RequestLogRecord {
            requested_at: 1,
            provider: "codex".into(),
            method: "POST".into(),
            path: "/responses".into(),
            aliased: false,
            status: 200,
            duration_ms: 1,
            account_id: None,
            target_kind: Some("account".into()),
            provider_credential_id: None,
            model: Some("gpt-test".into()),
            upstream_model: None,
            upstream_transport: Some("http_sse".into()),
            profile_revision: None,
            reasoning_effort: None,
            service_tier: None,
            transport: Some("sse".into()),
            ttft_ms: None,
            total_tokens: None,
            cached_tokens: None,
            subagent: subagent.map(str::to_string),
            request_id: None,
            session_key: Some(session_key.to_string()),
            input_tokens: None,
            output_tokens: None,
            cached_input_tokens: None,
            reasoning_tokens: None,
            orchestration_input_tokens: None,
            orchestration_output_tokens: None,
            orchestration_cached_input_tokens: None,
            cost_usd: None,
            latency_first_token_ms: None,
            protocol_outcome: None,
            error_code: None,
        }
    }

    #[test]
    fn schedule_handles_midnight_and_rewrite_preserves_passthrough_bytes() {
        let cfg = PriorityPolicyConfig {
            mode: OverallMode::Schedule,
            active_start_minute: 22 * 60,
            active_end_minute: 6 * 60,
            utc_offset_minutes: 0,
            presence_minutes: 30,
        };
        assert!(cfg.active_at(23 * 60 * 60));
        assert!(cfg.active_at(2 * 60 * 60));
        assert!(!cfg.active_at(12 * 60 * 60));

        let raw = Bytes::from_static(br#"{"model":"m","service_tier":"priority","input":[]}"#);
        let standard = apply_decision(&raw, PriorityDecision::Standard).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&standard).unwrap();
        assert_eq!(parsed["model"], "m");
        assert_eq!(parsed["input"], serde_json::json!([]));
        assert!(parsed.get("service_tier").is_none());
        assert_eq!(
            apply_decision(&raw, PriorityDecision::Passthrough).unwrap(),
            raw
        );
    }

    #[test]
    fn malformed_policy_fails_back_to_passthrough() {
        let runtime = PriorityPolicyRuntime::default();
        runtime.load_persisted(&HashMap::from([(
            GLOBAL_SETTING_KEY.to_string(),
            r#"{"mode":"schedule","active_start_minute":9999}"#.to_string(),
        )]));
        assert_eq!(runtime.config(), PriorityPolicyConfig::default());
    }

    #[tokio::test]
    async fn new_main_session_gets_one_bounded_presence_window_without_renewal() {
        let store = store().await;
        let runtime = PriorityPolicyRuntime::default();
        runtime.set_config(inactive_schedule(30)).unwrap();
        let session = "a".repeat(64);
        let noon = 12 * 60 * 60;

        assert_eq!(
            runtime.decide(&store, Some(&session), false, noon).await,
            PriorityDecision::Priority
        );
        assert_eq!(
            runtime
                .decide(&store, Some(&session), false, noon + 29 * 60)
                .await,
            PriorityDecision::Priority
        );
        assert_eq!(
            runtime
                .decide(&store, Some(&session), false, noon + 31 * 60)
                .await,
            PriorityDecision::Standard
        );
    }

    #[tokio::test]
    async fn subagent_never_opens_presence_and_session_override_has_highest_precedence() {
        let store = store().await;
        let runtime = PriorityPolicyRuntime::default();
        runtime.set_config(inactive_schedule(30)).unwrap();
        let session = "b".repeat(64);
        let noon = 12 * 60 * 60;

        assert_eq!(
            runtime.decide(&store, Some(&session), true, noon).await,
            PriorityDecision::Standard
        );
        runtime
            .set_config(PriorityPolicyConfig {
                mode: OverallMode::ForcePriority,
                ..inactive_schedule(30)
            })
            .unwrap();
        runtime.set_session_override(session.clone(), Some(SessionMode::Standard));
        assert_eq!(
            runtime.decide(&store, Some(&session), false, noon).await,
            PriorityDecision::Standard
        );
    }

    #[tokio::test]
    async fn persisted_main_history_prevents_restart_from_treating_session_as_new() {
        let store = store().await;
        let session = "c".repeat(64);
        store
            .request_log()
            .insert(&request_record(&session, None))
            .await
            .unwrap();
        assert!(store
            .request_log()
            .has_main_session_request(&session)
            .await
            .unwrap());

        let runtime = PriorityPolicyRuntime::default();
        runtime.set_config(inactive_schedule(30)).unwrap();
        assert_eq!(
            runtime
                .decide(&store, Some(&session), false, 12 * 60 * 60)
                .await,
            PriorityDecision::Standard
        );
    }

    #[tokio::test]
    async fn presence_tracking_is_bounded_and_fails_closed_when_full() {
        let store = store().await;
        let runtime = PriorityPolicyRuntime::default();
        runtime.set_config(inactive_schedule(30)).unwrap();
        {
            let mut presence = runtime.presence_until.lock().await;
            for index in 0..MAX_TRACKED_PRESENCE_SESSIONS {
                presence.insert(format!("{index:064x}"), i64::MAX);
            }
        }

        let overflow_session = "f".repeat(64);
        assert_eq!(
            runtime
                .decide(&store, Some(&overflow_session), false, 12 * 60 * 60)
                .await,
            PriorityDecision::Standard
        );
        assert_eq!(
            runtime.presence_until.lock().await.len(),
            MAX_TRACKED_PRESENCE_SESSIONS
        );
    }
}
