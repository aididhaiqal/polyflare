//! Per-upstream-origin recovery for failures that happen before an HTTP response or WebSocket
//! handshake exists.
//!
//! The registry is process-local and content-free. Keys retain only a normalized
//! `scheme://host:port` origin; snapshots expose only a short hash of that origin. Paths, query
//! strings, credentials, request content, and raw transport errors never enter this module.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use polyflare_core::{Account, ExecError, Executor, PreparedRequest, RequestCtx, ResponseStream};
use sha2::{Digest, Sha256};
use tokio::sync::Notify;
use tokio::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OriginKey(String);

impl OriginKey {
    pub fn parse(value: &str) -> Result<Self, OriginParseError> {
        let url = reqwest::Url::parse(value).map_err(|_| OriginParseError)?;
        let host = url.host_str().ok_or(OriginParseError)?.to_ascii_lowercase();
        let port = url.port_or_known_default().ok_or(OriginParseError)?;
        Ok(Self(format!("{}://{host}:{port}", url.scheme())))
    }

    fn telemetry_id(&self) -> String {
        let digest = Sha256::digest(self.0.as_bytes());
        hex::encode(&digest[..8])
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid upstream origin")]
pub struct OriginParseError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitStatus {
    Online,
    Offline,
    Probing,
}

impl CircuitStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Online => "online",
            Self::Offline => "offline",
            Self::Probing => "probing",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CircuitSnapshot {
    pub origin_id: String,
    pub status: CircuitStatus,
    pub failures: u32,
    pub transport_failures: u64,
    pub recoveries: u64,
}

#[derive(Debug, Clone, Copy)]
struct RecoveryPolicy {
    delays: [Duration; 5],
    max_jitter: Duration,
}

impl Default for RecoveryPolicy {
    fn default() -> Self {
        Self {
            delays: [
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(5),
                Duration::from_secs(10),
                Duration::from_secs(30),
            ],
            max_jitter: Duration::from_millis(250),
        }
    }
}

impl RecoveryPolicy {
    fn delay(self, origin: &OriginKey, failures: u32) -> Duration {
        let index = failures
            .saturating_sub(1)
            .min(self.delays.len().saturating_sub(1) as u32) as usize;
        let base = self.delays[index];
        if self.max_jitter.is_zero() {
            return base;
        }
        let mut hasher = Sha256::new();
        hasher.update(origin.0.as_bytes());
        hasher.update(failures.to_le_bytes());
        let digest = hasher.finalize();
        let sample = u64::from_le_bytes(digest[..8].try_into().expect("fixed digest slice"));
        let ceiling = self.max_jitter.as_millis().min(u128::from(u64::MAX)) as u64;
        base.saturating_add(Duration::from_millis(sample % (ceiling + 1)))
    }
}

#[derive(Debug)]
enum CircuitPhase {
    Closed,
    Open { retry_at: Instant, failures: u32 },
    HalfOpen { failures: u32 },
}

#[derive(Debug)]
struct Circuit {
    phase: CircuitPhase,
    notify: Arc<Notify>,
    transport_failures: u64,
    recoveries: u64,
}

impl Circuit {
    fn new() -> Self {
        Self {
            phase: CircuitPhase::Closed,
            notify: Arc::new(Notify::new()),
            transport_failures: 0,
            recoveries: 0,
        }
    }
}

#[derive(Debug)]
struct RegistryInner {
    circuits: Mutex<HashMap<OriginKey, Circuit>>,
    policy: RecoveryPolicy,
}

#[derive(Debug, Clone)]
pub struct NetworkRecoveryRegistry {
    inner: Arc<RegistryInner>,
}

impl Default for NetworkRecoveryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl NetworkRecoveryRegistry {
    pub fn new() -> Self {
        Self::with_policy(RecoveryPolicy::default())
    }

    fn with_policy(policy: RecoveryPolicy) -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                circuits: Mutex::new(HashMap::new()),
                policy,
            }),
        }
    }

    pub async fn acquire(
        &self,
        origin: &OriginKey,
        deadline: Instant,
    ) -> Result<AttemptPermit, RecoveryBudgetExceeded> {
        loop {
            let wait = {
                let mut circuits = self.inner.circuits.lock().expect("circuit mutex poisoned");
                let circuit = circuits.entry(origin.clone()).or_insert_with(Circuit::new);
                match circuit.phase {
                    CircuitPhase::Closed => {
                        return Ok(AttemptPermit::normal(self.clone(), origin.clone()));
                    }
                    CircuitPhase::Open { retry_at, failures } if Instant::now() >= retry_at => {
                        circuit.phase = CircuitPhase::HalfOpen { failures };
                        return Ok(AttemptPermit::probe(self.clone(), origin.clone(), failures));
                    }
                    CircuitPhase::Open { retry_at, .. } => {
                        (circuit.notify.clone().notified_owned(), Some(retry_at))
                    }
                    CircuitPhase::HalfOpen { .. } => {
                        (circuit.notify.clone().notified_owned(), None)
                    }
                }
            };

            if Instant::now() >= deadline {
                return Err(RecoveryBudgetExceeded);
            }
            let wake_at = wait.1.map_or(deadline, |retry_at| retry_at.min(deadline));
            if tokio::time::timeout_at(wake_at, wait.0).await.is_err() && Instant::now() >= deadline
            {
                return Err(RecoveryBudgetExceeded);
            }
        }
    }

    pub fn snapshots(&self) -> Vec<CircuitSnapshot> {
        let circuits = self.inner.circuits.lock().expect("circuit mutex poisoned");
        let mut snapshots = circuits
            .iter()
            .map(|(origin, circuit)| {
                let (status, failures) = match circuit.phase {
                    CircuitPhase::Closed => (CircuitStatus::Online, 0),
                    CircuitPhase::Open { failures, .. } => (CircuitStatus::Offline, failures),
                    CircuitPhase::HalfOpen { failures } => (CircuitStatus::Probing, failures),
                };
                CircuitSnapshot {
                    origin_id: origin.telemetry_id(),
                    status,
                    failures,
                    transport_failures: circuit.transport_failures,
                    recoveries: circuit.recoveries,
                }
            })
            .collect::<Vec<_>>();
        snapshots.sort_by(|a, b| a.origin_id.cmp(&b.origin_id));
        snapshots
    }

    fn success(&self, origin: &OriginKey) {
        let mut circuits = self.inner.circuits.lock().expect("circuit mutex poisoned");
        let circuit = circuits.entry(origin.clone()).or_insert_with(Circuit::new);
        let recovered = !matches!(circuit.phase, CircuitPhase::Closed);
        circuit.phase = CircuitPhase::Closed;
        if recovered {
            circuit.recoveries = circuit.recoveries.saturating_add(1);
        }
        circuit.notify.notify_waiters();
    }

    fn failure(&self, origin: &OriginKey, permit_kind: PermitKind) {
        let mut circuits = self.inner.circuits.lock().expect("circuit mutex poisoned");
        let circuit = circuits.entry(origin.clone()).or_insert_with(Circuit::new);
        circuit.transport_failures = circuit.transport_failures.saturating_add(1);
        let next_failures = match (permit_kind, &circuit.phase) {
            (PermitKind::Probe { failures }, CircuitPhase::HalfOpen { .. }) => {
                failures.saturating_add(1)
            }
            (PermitKind::Normal, CircuitPhase::Closed) => 1,
            (_, CircuitPhase::Open { failures, .. }) | (_, CircuitPhase::HalfOpen { failures }) => {
                *failures
            }
            (PermitKind::Probe { failures }, CircuitPhase::Closed) => failures.saturating_add(1),
        };
        let retry_at = Instant::now() + self.inner.policy.delay(origin, next_failures);
        circuit.phase = CircuitPhase::Open {
            retry_at,
            failures: next_failures,
        };
        circuit.notify.notify_waiters();
    }
}

#[derive(Debug, Clone, Copy)]
enum PermitKind {
    Normal,
    Probe { failures: u32 },
}

#[derive(Debug)]
pub struct AttemptPermit {
    registry: NetworkRecoveryRegistry,
    origin: OriginKey,
    kind: PermitKind,
    completed: bool,
}

impl AttemptPermit {
    fn normal(registry: NetworkRecoveryRegistry, origin: OriginKey) -> Self {
        Self {
            registry,
            origin,
            kind: PermitKind::Normal,
            completed: false,
        }
    }

    fn probe(registry: NetworkRecoveryRegistry, origin: OriginKey, failures: u32) -> Self {
        Self {
            registry,
            origin,
            kind: PermitKind::Probe { failures },
            completed: false,
        }
    }

    pub fn is_probe(&self) -> bool {
        matches!(self.kind, PermitKind::Probe { .. })
    }

    pub fn success(mut self) {
        self.completed = true;
        self.registry.success(&self.origin);
    }

    pub fn failure(mut self) {
        self.completed = true;
        self.registry.failure(&self.origin, self.kind);
    }
}

impl Drop for AttemptPermit {
    fn drop(&mut self) {
        if !self.completed && matches!(self.kind, PermitKind::Probe { .. }) {
            self.registry.failure(&self.origin, self.kind);
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("upstream network recovery budget exceeded")]
pub struct RecoveryBudgetExceeded;

static GLOBAL_REGISTRY: OnceLock<NetworkRecoveryRegistry> = OnceLock::new();

pub fn global_registry() -> &'static NetworkRecoveryRegistry {
    GLOBAL_REGISTRY.get_or_init(NetworkRecoveryRegistry::new)
}

/// Executor decorator that retries only pre-response transport failures against the same account.
/// An `Ok(ResponseStream)` or status-bearing `ExecError` proves the origin is reachable and closes
/// its circuit. Stream failures after this method returns are deliberately outside the retry loop.
pub struct RecoveringExecutor {
    inner: Arc<dyn Executor>,
    registry: NetworkRecoveryRegistry,
    budget: RecoveryBudget,
}

enum RecoveryBudget {
    Live(Arc<crate::runtime_settings::RuntimeSettings>),
    #[cfg(test)]
    Fixed(Duration),
}

impl RecoveryBudget {
    fn get(&self) -> Duration {
        match self {
            Self::Live(settings) => settings.starvation_wait_budget(),
            #[cfg(test)]
            Self::Fixed(duration) => *duration,
        }
    }
}

impl RecoveringExecutor {
    pub fn new(
        inner: Arc<dyn Executor>,
        wait_budget: Arc<crate::runtime_settings::RuntimeSettings>,
    ) -> Self {
        Self {
            inner,
            registry: global_registry().clone(),
            budget: RecoveryBudget::Live(wait_budget),
        }
    }
}

#[async_trait]
impl Executor for RecoveringExecutor {
    async fn execute(
        &self,
        req: PreparedRequest,
        account: &Account,
        ctx: &RequestCtx,
    ) -> Result<ResponseStream, ExecError> {
        let Ok(origin) = OriginKey::parse(&account.base_url) else {
            return self.inner.execute(req, account, ctx).await;
        };
        let deadline = Instant::now() + self.budget.get();
        loop {
            let permit = self
                .registry
                .acquire(&origin, deadline)
                .await
                .map_err(|_| ExecError::Upstream("network recovery budget exceeded".into()))?;
            match self.inner.execute(req.clone(), account, ctx).await {
                Ok(stream) => {
                    permit.success();
                    return Ok(stream);
                }
                Err(error @ (ExecError::UpstreamStatus(_) | ExecError::UpstreamHttp(_))) => {
                    permit.success();
                    return Err(error);
                }
                Err(ExecError::Upstream(_)) => {
                    permit.failure();
                    if Instant::now() >= deadline {
                        return Err(ExecError::Upstream(
                            "network recovery budget exceeded".into(),
                        ));
                    }
                }
                Err(error @ ExecError::Stream(_)) => {
                    permit.success();
                    return Err(error);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn origin(value: &str) -> OriginKey {
        OriginKey::parse(value).expect("valid origin")
    }

    fn fast_registry() -> NetworkRecoveryRegistry {
        NetworkRecoveryRegistry::with_policy(RecoveryPolicy {
            delays: [
                Duration::from_millis(10),
                Duration::from_millis(20),
                Duration::from_millis(50),
                Duration::from_millis(100),
                Duration::from_millis(300),
            ],
            max_jitter: Duration::ZERO,
        })
    }

    #[derive(Debug, Clone, Copy)]
    enum MockResult {
        Transport,
        Status,
        StreamError,
        Success,
    }

    struct SequenceExecutor {
        results: Mutex<VecDeque<MockResult>>,
        calls: AtomicUsize,
    }

    impl SequenceExecutor {
        fn new(results: impl IntoIterator<Item = MockResult>) -> Self {
            Self {
                results: Mutex::new(results.into_iter().collect()),
                calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl Executor for SequenceExecutor {
        async fn execute(
            &self,
            _req: PreparedRequest,
            _account: &Account,
            _ctx: &RequestCtx,
        ) -> Result<ResponseStream, ExecError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            match self.results.lock().unwrap().pop_front().unwrap() {
                MockResult::Transport => Err(ExecError::Upstream("redacted".into())),
                MockResult::Status => {
                    Err(ExecError::UpstreamStatus(polyflare_core::FailureSignal {
                        status: 503,
                        retry_after: None,
                        error_code: None,
                    }))
                }
                MockResult::StreamError => Ok(ResponseStream::new(stream::iter([Err(
                    ExecError::Stream("redacted".into()),
                )]))),
                MockResult::Success => Ok(ResponseStream::new(stream::empty())),
            }
        }
    }

    fn recovering(
        inner: Arc<dyn Executor>,
        registry: NetworkRecoveryRegistry,
        budget: Duration,
    ) -> RecoveringExecutor {
        RecoveringExecutor {
            inner,
            registry,
            budget: RecoveryBudget::Fixed(budget),
        }
    }

    fn request() -> PreparedRequest {
        PreparedRequest {
            body: Some(serde_json::json!({"model": "test"})),
            model: "test".into(),
            forward_headers: Vec::new(),
            raw_body: None,
        }
    }

    fn account() -> Account {
        Account {
            id: "account-a".into(),
            base_url: "https://executor.example/v1".into(),
            bearer_token: "secret".into(),
            chatgpt_account_id: None,
            is_fedramp: false,
        }
    }

    #[test]
    fn origin_normalization_drops_paths_queries_and_case() {
        assert_eq!(
            origin("HTTPS://Example.COM/v1/responses?secret=no").0,
            "https://example.com:443"
        );
    }

    #[tokio::test]
    async fn transport_failure_opens_only_its_origin() {
        let registry = fast_registry();
        let broken = origin("https://broken.example/v1");
        let healthy = origin("https://healthy.example/v1");
        registry
            .acquire(&broken, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .failure();

        assert!(registry.acquire(&broken, Instant::now()).await.is_err());
        assert!(registry.acquire(&healthy, Instant::now()).await.is_ok());
    }

    #[tokio::test]
    async fn only_one_half_open_probe_is_admitted() {
        let registry = fast_registry();
        let key = origin("https://probe.example");
        registry
            .acquire(&key, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .failure();
        tokio::time::sleep(Duration::from_millis(12)).await;

        let probe = registry
            .acquire(&key, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(probe.is_probe());
        assert!(registry
            .acquire(&key, Instant::now() + Duration::from_millis(5))
            .await
            .is_err());
        probe.success();
        assert!(registry
            .acquire(&key, Instant::now() + Duration::from_millis(5))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn failed_probe_increases_bounded_backoff() {
        let registry = fast_registry();
        let key = origin("https://backoff.example");
        registry
            .acquire(&key, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .failure();
        tokio::time::sleep(Duration::from_millis(12)).await;
        registry
            .acquire(&key, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .failure();

        let snapshot = registry.snapshots().pop().unwrap();
        assert_eq!(snapshot.failures, 2);
        assert_eq!(snapshot.transport_failures, 2);
        assert_eq!(snapshot.status, CircuitStatus::Offline);
    }

    #[tokio::test]
    async fn success_closes_circuit_and_wakes_waiters() {
        let registry = fast_registry();
        let key = origin("https://wake.example");
        registry
            .acquire(&key, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .failure();
        tokio::time::sleep(Duration::from_millis(12)).await;
        let probe = registry
            .acquire(&key, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();

        let waiting_registry = registry.clone();
        let waiting_key = key.clone();
        let waiter = tokio::spawn(async move {
            waiting_registry
                .acquire(&waiting_key, Instant::now() + Duration::from_secs(1))
                .await
        });
        tokio::task::yield_now().await;
        probe.success();
        assert!(waiter.await.unwrap().is_ok());
        assert_eq!(registry.snapshots()[0].recoveries, 1);
    }

    #[tokio::test]
    async fn wait_stops_at_budget() {
        let registry = fast_registry();
        let key = origin("https://budget.example");
        registry
            .acquire(&key, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .failure();

        let start = Instant::now();
        assert!(registry
            .acquire(&key, start + Duration::from_millis(4))
            .await
            .is_err());
        assert!(start.elapsed() < Duration::from_millis(30));
    }

    #[tokio::test]
    async fn executor_retries_transport_setup_on_the_same_account() {
        let inner = Arc::new(SequenceExecutor::new([
            MockResult::Transport,
            MockResult::Success,
        ]));
        let executor = recovering(inner.clone(), fast_registry(), Duration::from_millis(100));

        assert!(executor
            .execute(request(), &account(), &RequestCtx::default())
            .await
            .is_ok());
        assert_eq!(inner.calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn received_http_status_closes_the_connectivity_circuit() {
        let registry = fast_registry();
        let inner = Arc::new(SequenceExecutor::new([
            MockResult::Transport,
            MockResult::Status,
        ]));
        let executor = recovering(inner, registry.clone(), Duration::from_millis(100));

        assert!(matches!(
            executor
                .execute(request(), &account(), &RequestCtx::default())
                .await,
            Err(ExecError::UpstreamStatus(_))
        ));
        let snapshot = registry.snapshots().pop().unwrap();
        assert_eq!(snapshot.status, CircuitStatus::Online);
        assert_eq!(snapshot.recoveries, 1);
    }

    #[tokio::test]
    async fn midstream_failure_is_returned_without_replay() {
        let registry = fast_registry();
        let inner = Arc::new(SequenceExecutor::new([MockResult::StreamError]));
        let executor = recovering(inner.clone(), registry.clone(), Duration::from_millis(100));

        let mut response = executor
            .execute(request(), &account(), &RequestCtx::default())
            .await
            .unwrap();
        assert!(matches!(
            futures_util::StreamExt::next(&mut response).await,
            Some(Err(ExecError::Stream(_)))
        ));
        assert_eq!(inner.calls.load(Ordering::Relaxed), 1);
        assert_eq!(registry.snapshots()[0].status, CircuitStatus::Online);
    }

    #[test]
    fn telemetry_is_hashed_and_content_free() {
        let registry = fast_registry();
        let key = origin("https://user:secret@example.com/private?prompt=hello");
        registry.success(&key);
        let snapshot = registry.snapshots().pop().unwrap();
        assert_eq!(snapshot.origin_id.len(), 16);
        assert!(!snapshot.origin_id.contains("example"));
        assert!(!snapshot.origin_id.contains("secret"));
        assert_eq!(snapshot.status, CircuitStatus::Online);
    }

    #[test]
    fn backoff_caps_at_thirty_seconds_plus_bounded_jitter() {
        let policy = RecoveryPolicy::default();
        let delay = policy.delay(&origin("https://cap.example"), u32::MAX);
        assert!(delay >= Duration::from_secs(30));
        assert!(delay <= Duration::from_millis(30_250));
    }

    #[tokio::test]
    async fn normal_failures_racing_after_open_do_not_advance_backoff() {
        let registry = fast_registry();
        let key = origin("https://race.example");
        let first = registry
            .acquire(&key, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        let second = registry
            .acquire(&key, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        first.failure();
        second.failure();
        assert_eq!(registry.snapshots()[0].failures, 1);
    }
}
