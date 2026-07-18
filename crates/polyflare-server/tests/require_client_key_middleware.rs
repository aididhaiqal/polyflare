//! D18 Task 3: `require_client_key` — hash-lookup, enabled-check, never-log middleware.
//!
//! Tested standalone against a TINY one-route test router (NOT `build_app`'s real proxy routes —
//! wiring the layer onto the proxy sub-router with a bind-aware posture is Task 4's job; this
//! suite only proves "given a key IS required, is the presented one valid"). The router is built
//! against a real `AppState`/`Store` so Task 1's `ApiKeyRepo` and Task 2's `create_key`/
//! `sha256_hex` are exercised for real, not mocked.

use std::sync::Arc;
use std::time::Duration;

use axum::routing::get;
use axum::Router;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity, Executor};
use polyflare_server::app::AppState;
use polyflare_server::auth::require_client_key;
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::keys::{create_key, sha256_hex};
use polyflare_store::{Store, TokenCipher};

async fn build_state() -> Arc<AppState> {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);
    let cipher = TokenCipher::from_key_bytes(&[21u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    Arc::new(AppState {
        enforce_client_keys: false,
        codex_executor: Arc::new(CodexExecutor::new().unwrap()) as Arc<dyn Executor>,
        control_client: polyflare_codex::build_client().expect("build control_client"),
        anthropic_executor: Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap())
            as Arc<dyn Executor>,
        selector: Arc::new(CapacityWeighted),
        pool_selectors: Default::default(),
        continuity,
        store,
        cipher,
        oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
        upstream_base_url: "http://127.0.0.1:9".to_string(),
        anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
        refresh_locks: Default::default(),
        capture_fingerprint_path: None,
        codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        admin_token: None,
        live_logs: false,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        max_account_attempts: 3,
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        starvation_wait_budget: Duration::from_secs(60),
        starvation_heartbeat: Duration::from_secs(10),
        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        stream_idle_timeout: std::time::Duration::from_secs(300),
        runtime: Default::default(),
    })
}

async fn dummy_handler() -> &'static str {
    "inner-handler-ran"
}

/// A one-route test router: `GET /probe` behind `require_client_key` only — nothing else from
/// `build_app` (no `/dashboard`, no `/api/*`, no proxy paths). Proves the middleware in isolation.
fn test_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/probe", get(dummy_handler))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_client_key,
        ))
        .with_state(state)
}

async fn spawn(state: Arc<AppState>) -> String {
    let app = test_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn valid_enabled_key_passes_through_and_touches_last_used() {
    let state = build_state().await;
    let created = create_key(&state.store, Some("laptop"), 1_000).await.unwrap();
    let base = spawn(state.clone()).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/probe"))
        .header("authorization", format!("Bearer {}", created.raw))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "a valid enabled key must pass through");
    assert_eq!(
        resp.text().await.unwrap(),
        "inner-handler-ran",
        "the inner handler must have actually run"
    );

    // `touch_last_used` is fire-and-forget (see auth.rs's doc comment) — poll briefly instead of
    // asserting immediately after the response, which only proves `next.run` completed, not that
    // the spawned audit write landed yet.
    let hash = sha256_hex(&created.raw);
    let mut last_used = None;
    for _ in 0..50 {
        let row = state.store.api_keys().get_by_hash(&hash).await.unwrap().unwrap();
        if row.last_used_at.is_some() {
            last_used = row.last_used_at;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        last_used.is_some(),
        "last_used_at must be stamped after a successful validation"
    );
}

#[tokio::test]
async fn unknown_key_is_401() {
    let state = build_state().await;
    let base = spawn(state).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/probe"))
        .header("authorization", "Bearer sk-pf-this-key-was-never-created")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn revoked_key_is_401() {
    let state = build_state().await;
    let created = create_key(&state.store, None, 1).await.unwrap();
    state
        .store
        .api_keys()
        .set_enabled(&created.id, false)
        .await
        .unwrap();
    let base = spawn(state).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/probe"))
        .header("authorization", format!("Bearer {}", created.raw))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "a revoked (enabled=false) key must be rejected");
}

#[tokio::test]
async fn missing_authorization_is_401() {
    let state = build_state().await;
    let base = spawn(state).await;

    let client = reqwest::Client::new();
    let resp = client.get(format!("{base}/probe")).send().await.unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn non_bearer_scheme_is_401() {
    let state = build_state().await;
    let created = create_key(&state.store, None, 1).await.unwrap();
    let base = spawn(state).await;

    let client = reqwest::Client::new();
    // A valid key's raw value, but presented with the wrong scheme — must still be rejected; the
    // middleware only ever strips a literal "Bearer " prefix.
    let resp = client
        .get(format!("{base}/probe"))
        .header("authorization", format!("Basic {}", created.raw))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn empty_bearer_is_401() {
    let state = build_state().await;
    let base = spawn(state).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/probe"))
        .header("authorization", "Bearer ")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

/// Content-safety (the key one): drive a request with an UNKNOWN key containing a sentinel value
/// through the middleware, capture everything a `tracing` subscriber would see across the call,
/// and assert the sentinel appears in NEITHER the capture NOR the 401 response body. Mirrors
/// `keys.rs::never_logs_the_raw_key`'s capture pattern and the cyber-suite's
/// message-must-never-leak assertion style (`sticky_cyber_prefilter.rs`'s
/// `"classified — must never leak"` frame, asserted absent from anything client/log-visible).
#[tokio::test]
async fn sentinel_key_never_leaks_on_failed_auth() {
    use std::sync::Mutex;

    #[derive(Clone, Default)]
    struct BufWriter(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for BufWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
        type Writer = BufWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    const SENTINEL: &str = "SENTINELVALUE";
    let raw = format!("sk-pf-{SENTINEL}");

    let buf = BufWriter::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(buf.clone())
        .with_max_level(tracing::Level::TRACE)
        .finish();

    let state = build_state().await;
    let base = spawn(state).await;
    let client = reqwest::Client::new();

    // `#[tokio::test]` defaults to a current-thread runtime: the spawned server task and this
    // request never migrate to another OS thread, so the thread-local subscriber set here stays
    // current across the whole request (same rationale as `keys.rs::never_logs_the_raw_key`).
    let guard = tracing::subscriber::set_default(subscriber);
    let resp = client
        .get(format!("{base}/probe"))
        .header("authorization", format!("Bearer {raw}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "an unknown/attacker-controlled key must be rejected");
    let body = resp.text().await.unwrap();
    drop(guard);

    assert!(
        !body.contains(SENTINEL) && !body.contains(&raw),
        "the 401 body must not echo any part of the presented key, got: {body:?}"
    );

    let captured = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    assert!(
        !captured.contains(SENTINEL) && !captured.contains(&raw),
        "no tracing sink may ever see the raw key, even on the failure path — got: {captured:?}"
    );
    // Also assert the derived hash never leaks — the plan is unambiguous that key material means
    // the raw value; the hash is safe to log per Task 1, but proving it's ALSO absent here shows
    // this middleware doesn't even attempt a hash-based log line on failure.
    assert!(
        !captured.contains(&sha256_hex(&raw)),
        "the key hash must not appear in tracing output on the failure path either"
    );
}
