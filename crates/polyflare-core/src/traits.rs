//! The five trait seams. M1 implemented only `Executor`; M2b implements `Selector`
//! (reshaped here per M2-GATE1 + the `CapacityWeighted` impl in `select.rs`); M3 reshapes
//! `Continuity` (see `prepare`/`observe` below + `CodexContinuity` in the server crate).
//! `Coordinator` stays PROVISIONAL — reshaped at its own milestone.

use async_trait::async_trait;

use crate::types::{
    Account, AccountId, AccountSnapshot, ContinuityError, ExecError, Prepared, PreparedRequest,
    RequestCtx, ResponseStream, SelectionCtx, SessionKey, TurnOutcome,
};

/// Executes a prepared request against an upstream using an account, returning a byte stream.
///
/// `ctx` is the same [`RequestCtx`] ingress already computed for `Continuity::prepare` — threaded
/// down rather than re-derived so a future WS transport can key its per-conversation connection
/// cache off `ctx.session_key` without re-parsing the body (see `session_key.rs::parse_inbound`,
/// whose whole point is to derive this once). Today's HTTP-SSE impls don't need it and ignore it.
#[async_trait]
pub trait Executor: Send + Sync {
    async fn execute(
        &self,
        req: PreparedRequest,
        account: &Account,
        ctx: &RequestCtx,
    ) -> Result<ResponseStream, ExecError>;
}

/// Picks an account from a pool of per-account snapshots for a request. Sync + pure: scoring is
/// deterministic given the snapshots + ctx (async DB snapshot-assembly lives in the caller).
/// Returns an owned `AccountId` (M2-GATE1).
pub trait Selector: Send + Sync {
    fn pick(&self, candidates: &[AccountSnapshot], ctx: &SelectionCtx) -> Option<AccountId>;

    /// The selector's canonical snake_case strategy name (dashboard-facing, e.g. via
    /// `/api/pools`'s `strategy` field). Config-selectable strategies match
    /// `RoutingStrategy::name()`'s strings; ad hoc/test selectors just need a stable identifier.
    fn name(&self) -> &'static str;
}

/// The continuity state machine seam (M3). `prepare` resolves session + ownership and decides
/// routing + watchdog; `observe` advances the machine from how the turn resolved. Both read/write
/// persisted session state and may fail.
#[async_trait]
pub trait Continuity: Send + Sync {
    async fn prepare(
        &self,
        req: PreparedRequest,
        ctx: &RequestCtx,
    ) -> Result<Prepared, ContinuityError>;

    async fn observe(&self, outcome: TurnOutcome, ctx: &RequestCtx) -> Result<(), ContinuityError>;

    /// TA6(b) Task 3: stamp `capability` as a sticky requirement on `session_key`'s session, so a
    /// LATER `prepare` on that session pre-filters (see `ContinuityDirective::require_security_work_authorized`).
    /// Called once, right when a cyber-rejected turn is successfully rerouted onto a
    /// capability-holding account (`ingress.rs::reroute_cyber_rejection`) — NOT on every ordinary
    /// silence-recovery, only on a genuine capability move. Content-free: `capability` is a fixed
    /// label (e.g. `"security_work"`), never conversation content.
    async fn mark_required_capability(
        &self,
        session_key: &SessionKey,
        capability: &'static str,
    ) -> Result<(), ContinuityError>;
}

/// Coordinates session ownership + admission. (In-process pass in M1.)
pub trait Coordinator: Send + Sync {
    fn admit(&self, ctx: &RequestCtx) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{AccountId, AccountSnapshot, SelectionCtx};

    // A trivial Selector proves the reshaped trait is object-safe and returns an owned id.
    struct FirstCandidate;
    impl Selector for FirstCandidate {
        fn pick(&self, candidates: &[AccountSnapshot], _ctx: &SelectionCtx) -> Option<AccountId> {
            candidates.first().map(|s| s.id.clone())
        }

        fn name(&self) -> &'static str {
            "first_candidate"
        }
    }

    #[test]
    fn selector_returns_owned_account_id() {
        let pool = vec![AccountSnapshot::new("a"), AccountSnapshot::new("b")];
        let sel: Box<dyn Selector> = Box::new(FirstCandidate);
        let picked = sel.pick(&pool, &SelectionCtx::default()).unwrap();
        assert_eq!(picked.as_str(), "a");
    }
}
