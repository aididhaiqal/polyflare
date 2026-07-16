//! Continuity implementations that live in the neutral core. `NoopContinuity` keeps a non-Codex
//! backend's ingress path uniform: it never pins, never arms the watchdog, and observes nothing.

use async_trait::async_trait;

use crate::traits::Continuity;
use crate::types::{
    ContinuityDirective, ContinuityError, Prepared, PreparedRequest, RecoveryPlan, RequestCtx,
    TurnOutcome, WatchdogArm,
};

/// A `Continuity` that does nothing — for backends without continuity (e.g. Anthropic in M3).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopContinuity;

#[async_trait]
impl Continuity for NoopContinuity {
    async fn prepare(
        &self,
        req: PreparedRequest,
        ctx: &RequestCtx,
    ) -> Result<Prepared, ContinuityError> {
        Ok(Prepared {
            req,
            directive: ContinuityDirective {
                pin_account: None,
                watchdog: WatchdogArm::Disarmed,
                recovery: RecoveryPlan::None,
                session_key: ctx.session_key.clone(),
            },
        })
    }

    async fn observe(
        &self,
        _outcome: TurnOutcome,
        _ctx: &RequestCtx,
    ) -> Result<(), ContinuityError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_prepare_disarms_and_never_pins() {
        let noop = NoopContinuity;
        let req = PreparedRequest {
            body: Some(serde_json::json!({})),
            model: "m".to_string(),
            forward_headers: vec![],
            raw_body: None,
        };
        let prepared = noop.prepare(req, &RequestCtx::default()).await.unwrap();
        assert!(prepared.directive.pin_account.is_none());
        assert!(matches!(prepared.directive.watchdog, WatchdogArm::Disarmed));
        assert!(matches!(prepared.directive.recovery, RecoveryPlan::None));
    }
}
