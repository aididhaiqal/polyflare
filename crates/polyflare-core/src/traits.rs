//! The five trait seams. M1 implemented only `Executor`; M2b implements `Selector`
//! (reshaped here per M2-GATE1 + the `CapacityWeighted` impl in `select.rs`); M3 reshapes
//! `Continuity` (see `prepare`/`observe` below + `CodexContinuity` in the server crate).
//! `Coordinator` stays PROVISIONAL — reshaped at its own milestone.

use async_trait::async_trait;

use crate::types::{
    Account, AccountId, AccountSnapshot, ContinuityError, ExecError, Prepared, PreparedRequest,
    RequestCtx, ResponseStream, SelectionCtx, TurnOutcome,
};

/// Executes a prepared request against an upstream using an account, returning a byte stream.
#[async_trait]
pub trait Executor: Send + Sync {
    async fn execute(
        &self,
        req: PreparedRequest,
        account: &Account,
    ) -> Result<ResponseStream, ExecError>;
}

/// Picks an account from a pool of per-account snapshots for a request. Sync + pure: scoring is
/// deterministic given the snapshots + ctx (async DB snapshot-assembly lives in the caller).
/// Returns an owned `AccountId` (M2-GATE1).
pub trait Selector: Send + Sync {
    fn pick(&self, candidates: &[AccountSnapshot], ctx: &SelectionCtx) -> Option<AccountId>;
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
    }

    #[test]
    fn selector_returns_owned_account_id() {
        let pool = vec![AccountSnapshot::new("a"), AccountSnapshot::new("b")];
        let sel: Box<dyn Selector> = Box::new(FirstCandidate);
        let picked = sel.pick(&pool, &SelectionCtx::default()).unwrap();
        assert_eq!(picked.as_str(), "a");
    }
}
