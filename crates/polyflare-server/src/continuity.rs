//! The Codex continuity state machine: a store-backed `Continuity` impl. Holds a `ContinuityRepo`;
//! persists NO conversation content — only session state + a response_id -> owner map.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use polyflare_core::{
    AccountId, Continuity, ContinuityDirective, ContinuityError, KeyStrength, Prepared,
    PreparedRequest, RecoveryPlan, RequestCtx, TurnOutcome, WatchdogArm,
};
use polyflare_store::{ContinuityRepo, StoreError};

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn box_store_err(e: StoreError) -> ContinuityError {
    ContinuityError::Store(Box::new(e))
}

fn strength_str(s: KeyStrength) -> &'static str {
    match s {
        KeyStrength::Hard => "hard",
        KeyStrength::Soft => "soft",
    }
}

/// Codex continuity backed by a `ContinuityRepo`. `watchdog_timeout` (N) is stamped into the
/// directive on every anchor-bearing request.
pub struct CodexContinuity {
    repo: ContinuityRepo,
    watchdog_timeout: Duration,
}

impl CodexContinuity {
    pub fn new(repo: ContinuityRepo, watchdog_timeout: Duration) -> Self {
        Self {
            repo,
            watchdog_timeout,
        }
    }
}

#[async_trait]
impl Continuity for CodexContinuity {
    async fn prepare(
        &self,
        req: PreparedRequest,
        ctx: &RequestCtx,
    ) -> Result<Prepared, ContinuityError> {
        let now = now_secs();
        let session_key = ctx.session_key.clone();
        let anchor = ctx.client_previous_response_id.clone();

        // Resolve the owner: the client-supplied anchor map is authoritative; else the session row.
        let mut owner: Option<AccountId> = None;
        if let Some(rid) = anchor.as_deref() {
            if let Some(acc) = self
                .repo
                .get_anchor_owner(rid)
                .await
                .map_err(box_store_err)?
            {
                owner = Some(AccountId::from(acc));
            }
        }
        if owner.is_none() {
            if let Some(sk) = session_key.as_ref() {
                if let Some(row) = self
                    .repo
                    .get_session(&sk.value)
                    .await
                    .map_err(box_store_err)?
                {
                    owner = row.owning_account_id.map(AccountId::from);
                }
            }
        }

        // Ensure a session row exists (Fresh on miss); mark reattaching when an anchor is in flight.
        if let Some(sk) = session_key.as_ref() {
            self.repo
                .ensure_session(&sk.value, strength_str(sk.strength), now)
                .await
                .map_err(box_store_err)?;
            if anchor.is_some() {
                self.repo
                    .set_state(&sk.value, "reattaching", now)
                    .await
                    .map_err(box_store_err)?;
            }
        }

        // Arm the watchdog ONLY on anchor-bearing requests; pick the recovery strategy.
        let (watchdog, recovery) = if anchor.is_some() {
            let arm = WatchdogArm::Armed {
                timeout: self.watchdog_timeout,
            };
            if ctx.is_full_resend {
                let mut stripped = req.body.clone();
                if let Some(obj) = stripped.as_object_mut() {
                    obj.remove("previous_response_id");
                }
                let anchorless_req = PreparedRequest {
                    body: stripped,
                    model: req.model.clone(),
                };
                (arm, RecoveryPlan::ResendFull { anchorless_req })
            } else {
                (arm, RecoveryPlan::SignalClient)
            }
        } else {
            (WatchdogArm::Disarmed, RecoveryPlan::None)
        };

        Ok(Prepared {
            req,
            directive: ContinuityDirective {
                pin_account: owner,
                watchdog,
                recovery,
                session_key,
            },
        })
    }

    async fn observe(
        &self,
        _outcome: TurnOutcome,
        _ctx: &RequestCtx,
    ) -> Result<(), ContinuityError> {
        // Implemented in C6.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polyflare_store::Store;

    async fn make() -> (Store, CodexContinuity) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("s.db")).await.unwrap();
        std::mem::forget(dir);
        let cont = CodexContinuity::new(store.continuity(), Duration::from_millis(150));
        (store, cont)
    }

    fn req(body: serde_json::Value) -> PreparedRequest {
        PreparedRequest {
            body,
            model: "gpt-5.6-sol".to_string(),
        }
    }

    #[tokio::test]
    async fn no_anchor_disarms_and_does_not_pin() {
        let (_s, cont) = make().await;
        let ctx = RequestCtx {
            session_key: Some(polyflare_core::SessionKey {
                value: "sk".into(),
                strength: KeyStrength::Soft,
            }),
            ..Default::default()
        };
        let p = cont
            .prepare(req(serde_json::json!({"input": "hi"})), &ctx)
            .await
            .unwrap();
        assert!(p.directive.pin_account.is_none());
        assert!(matches!(p.directive.watchdog, WatchdogArm::Disarmed));
        assert!(matches!(p.directive.recovery, RecoveryPlan::None));
    }

    #[tokio::test]
    async fn anchor_full_resend_arms_with_resendfull_stripped() {
        let (_s, cont) = make().await;
        let ctx = RequestCtx {
            session_key: Some(polyflare_core::SessionKey {
                value: "sk".into(),
                strength: KeyStrength::Soft,
            }),
            client_previous_response_id: Some("resp_dead".into()),
            is_full_resend: true,
            ..Default::default()
        };
        let body =
            serde_json::json!({"previous_response_id": "resp_dead", "input": [{"a":1},{"b":2}]});
        let p = cont.prepare(req(body), &ctx).await.unwrap();
        assert!(matches!(p.directive.watchdog, WatchdogArm::Armed { .. }));
        match p.directive.recovery {
            RecoveryPlan::ResendFull { anchorless_req } => {
                assert!(
                    anchorless_req.body.get("previous_response_id").is_none(),
                    "anchor stripped"
                );
                assert!(
                    anchorless_req.body.get("input").is_some(),
                    "full input preserved"
                );
            }
            other => panic!("expected ResendFull, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn anchor_bare_tail_arms_with_signalclient() {
        let (_s, cont) = make().await;
        let ctx = RequestCtx {
            session_key: Some(polyflare_core::SessionKey {
                value: "sk".into(),
                strength: KeyStrength::Soft,
            }),
            client_previous_response_id: Some("resp_x".into()),
            is_full_resend: false,
            ..Default::default()
        };
        let body = serde_json::json!({"previous_response_id": "resp_x", "input": "tail"});
        let p = cont.prepare(req(body), &ctx).await.unwrap();
        assert!(matches!(p.directive.watchdog, WatchdogArm::Armed { .. }));
        assert!(matches!(p.directive.recovery, RecoveryPlan::SignalClient));
    }

    #[tokio::test]
    async fn anchor_map_resolves_owner_for_pin() {
        let (store, cont) = make().await;
        // Seed an account + a completed turn so the anchor map knows resp_1 -> A.
        sqlx::query(
            "INSERT INTO accounts (id, email, access_token_enc, refresh_token_enc, id_token_enc, created_at) \
             VALUES ('A', 'e@x', X'00', X'00', X'00', 0)",
        )
        .execute(store.pool())
        .await
        .unwrap();
        store
            .continuity()
            .ensure_session("skA", "soft", 1)
            .await
            .unwrap();
        store
            .continuity()
            .record_completion("skA", "soft", "A", "resp_1", "fp", 2, 1)
            .await
            .unwrap();

        let ctx = RequestCtx {
            session_key: Some(polyflare_core::SessionKey {
                value: "skZ".into(),
                strength: KeyStrength::Soft,
            }),
            client_previous_response_id: Some("resp_1".into()),
            is_full_resend: true,
            ..Default::default()
        };
        let body =
            serde_json::json!({"previous_response_id": "resp_1", "input": [{"a":1},{"b":2}]});
        let p = cont.prepare(req(body), &ctx).await.unwrap();
        assert_eq!(
            p.directive.pin_account,
            Some(AccountId::from("A")),
            "anchor map pins to owner"
        );
    }
}
