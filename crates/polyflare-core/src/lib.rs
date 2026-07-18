//! PolyFlare neutral core: formats, translator registry, core types, and the trait spine.

pub mod continuity;
pub mod depletion;
pub mod format;
pub mod provider;
pub mod select;
pub mod traits;
pub mod translate;
pub mod types;

pub use continuity::NoopContinuity;
pub use format::Format;
pub use provider::Provider;
pub use select::{
    BackoffCensus, BackoffKind, CacheAffinityTier, CapacityWeighted, FillFirst, Recovery,
    RoundRobin, RoutingStrategy, SequentialDrain, UsageWeighted,
};
pub use traits::{Continuity, Coordinator, Executor, Selector};
pub use translate::{IdentityTranslator, Translator, TranslatorRegistry};
pub use types::{
    Account, AccountId, AccountSnapshot, ContinuityDirective, ContinuityError, ExecError,
    FailureSignal, KeyStrength, Prepared, PreparedRequest, ReasoningItems, RecoveryPlan,
    RequestCtx, ResponseStream, SelectionCtx, SessionKey, Tier, TurnOutcome, WatchdogArm,
};
