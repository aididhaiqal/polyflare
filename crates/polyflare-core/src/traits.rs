//! The five trait seams. M1 implements only `Executor` (in `polyflare-codex`);
//! `Selector`/`Continuity`/`Coordinator` are defined here and fleshed out in M2/M3.

use async_trait::async_trait;

use crate::types::{Account, ExecError, PreparedRequest, RequestCtx, ResponseStream};

/// Executes a prepared request against an upstream using an account, returning a byte stream.
#[async_trait]
pub trait Executor: Send + Sync {
    async fn execute(
        &self,
        req: PreparedRequest,
        account: &Account,
    ) -> Result<ResponseStream, ExecError>;
}

/// Picks an account from a pool for a request. (Skeleton in M1; real scoring in M2.)
pub trait Selector: Send + Sync {
    fn pick<'a>(&self, pool: &'a [Account], ctx: &RequestCtx) -> Option<&'a Account>;
}

/// Applies continuity (anchor/trim/resend) before execution. (No-op in M1; state machine in M3.)
pub trait Continuity: Send + Sync {
    fn prepare(&self, req: PreparedRequest, ctx: &RequestCtx) -> PreparedRequest;
}

/// Coordinates session ownership + admission. (In-process pass in M1.)
pub trait Coordinator: Send + Sync {
    fn admit(&self, ctx: &RequestCtx) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Account, RequestCtx};

    // A trivial Selector proves the trait is object-safe and usable.
    struct FirstAccount;
    impl Selector for FirstAccount {
        fn pick<'a>(&self, pool: &'a [Account], _ctx: &RequestCtx) -> Option<&'a Account> {
            pool.first()
        }
    }

    #[test]
    fn selector_picks_first_account() {
        let pool = vec![
            Account { id: "a".into(), base_url: "http://x".into(), bearer_token: "t".into() },
            Account { id: "b".into(), base_url: "http://y".into(), bearer_token: "u".into() },
        ];
        let sel: Box<dyn Selector> = Box::new(FirstAccount);
        let picked = sel.pick(&pool, &RequestCtx::default()).unwrap();
        assert_eq!(picked.id, "a");
    }
}
