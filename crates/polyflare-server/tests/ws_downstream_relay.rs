//! WS-downstream relay Task 2: `POLYFLARE_WS_DOWNSTREAM` (threaded as `AppState::ws_downstream`)
//! routes the codex CLI's WS-handshake `GET /responses` to the new `ws_relay` accept handler when
//! ON, and keeps today's `426 Upgrade Required` fallback (`ingress::websocket_fallback_handler`)
//! when OFF (the default). See `docs/superpowers/specs/2026-07-20-ws-downstream-relay-design.md` §8.
//!
//! WHY a REAL server + REAL WS client (not `oneshot`): axum's `WebSocketUpgrade` extractor pulls the
//! `hyper::upgrade::OnUpgrade` value out of the request extensions, which is only present on a live,
//! upgradable connection. A tower `oneshot` call has no such connection, so the extractor rejects
//! with `ConnectionNotUpgradable` → `426` — indistinguishable from the flag-OFF fallback. Driving a
//! real `tokio_tungstenite` client against a real `axum::serve` listener (the same harness the WS
//! examples use) is the only way to observe the `101` accept distinctly from the `426` fallback.

use std::sync::Arc;
use std::time::Duration;

use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::CodexExecutor;
use polyflare_core::{CapacityWeighted, Continuity, Executor};
use polyflare_server::app::{build_app, AppState};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{Store, TokenCipher};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Error as WsError;

/// Spawn a PolyFlare instance with `ws_downstream` set as given, returning its `ws://addr` base.
/// The store is empty — both tests answer (accept or 426) at the WS-handshake before any account
/// selection, so no seeded account is needed.
async fn spawn(ws_downstream: bool) -> String {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    std::mem::forget(dir);

    let cipher = TokenCipher::from_key_bytes(&[13u8; 32]).unwrap();
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        Duration::from_secs(30),
    ));
    let codex_executor: Arc<dyn Executor> = Arc::new(CodexExecutor::new().unwrap());
    let anthropic_executor: Arc<dyn Executor> =
        Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap());

    let state = Arc::new(AppState {
        enforce_client_keys: false,
        codex_executor,
        control_client: polyflare_codex::build_client().expect("build control_client"),
        anthropic_executor,
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
        codex_version: std::sync::Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
        account_cache: std::sync::Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Default::default(),
        admin_token: None,
        live_logs: false,
        ws_downstream,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        max_account_attempts: 3,
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        starvation_wait_budget: std::time::Duration::from_secs(60),
        starvation_heartbeat: std::time::Duration::from_secs(10),
        wake_jitter_ms: 0,
        inflight_penalty_pct: 2.5,
        lease_metrics: polyflare_server::observability::LeaseMetrics::new(),
        upstream_request_metrics: polyflare_server::observability::UpstreamRequestMetrics::new(),
        rate_limit_metrics: polyflare_server::observability::RateLimitMetrics::new(),
        relay_metrics: polyflare_server::observability::RelayMetrics::new(),
        model_catalog: polyflare_server::model_catalog::floor_only_model_catalog(),
        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        stream_idle_timeout: std::time::Duration::from_secs(300),
        soft_drain_enabled: true,
        request_log_retention_days: 0,
        usage_history_retention_days: 0,
        runtime: Default::default(),
    });
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("ws://{addr}")
}

/// Attempt a WS handshake at `<base>/responses` and return the resulting HTTP status code: the
/// handshake's `101` on accept, or the non-101 status (e.g. `426`) the server answered with.
async fn ws_handshake_status(base: &str) -> u16 {
    match connect_async(format!("{base}/responses")).await {
        // Accept: `WebSocketUpgrade` completed the handshake with `101 Switching Protocols`. The
        // stub relay then immediately drops the socket, but the handshake status is already 101.
        Ok((_ws, resp)) => resp.status().as_u16(),
        // Non-101: tungstenite surfaces the server's HTTP response verbatim (this is how a `426`
        // fallback arrives at the client — the sole trigger codex-rs recognizes for WS→HTTP).
        Err(WsError::Http(resp)) => resp.status().as_u16(),
        Err(other) => panic!("unexpected WS handshake error (not an HTTP status): {other}"),
    }
}

/// Default OFF: the WS-handshake `GET /responses` still answers exactly `426`, byte-identical to
/// before this flag existed — codex-rs's sole WS→HTTP-SSE fallback trigger.
#[tokio::test]
async fn ws_get_responses_returns_426_when_downstream_flag_off() {
    let base = spawn(false).await;
    let status = ws_handshake_status(&base).await;
    assert_eq!(
        status, 426,
        "with POLYFLARE_WS_DOWNSTREAM off, a WS handshake on /responses must still get 426 \
         (the unchanged fallback), never an accepted upgrade"
    );
}

/// Flag ON: the same WS handshake is ACCEPTED — it routes to `ws_relay::responses_ws_handler`, which
/// completes the upgrade (`101 Switching Protocols`), NOT the `426` fallback. (The stub relay closes
/// immediately for now; Tasks 3-6 add the real pump.)
#[tokio::test]
async fn ws_get_responses_accepts_upgrade_when_flag_on() {
    let base = spawn(true).await;
    let status = ws_handshake_status(&base).await;
    assert_ne!(
        status, 426,
        "with POLYFLARE_WS_DOWNSTREAM on, the WS handshake must be routed to the relay accept \
         handler, not the 426 fallback"
    );
    assert_eq!(
        status, 101,
        "the relay handler must ACCEPT the WS upgrade with 101 Switching Protocols"
    );
}

/// Task 6: the real bidirectional pump + content-free ownership sniff, exercised end to end through
/// a REAL downstream WS client and a REAL (mocked) upstream WS — no invented downstream-trait
/// abstraction, exactly as the task brief calls for.
mod relay_through {
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use axum::http::{HeaderName, HeaderValue};
    use futures_util::{SinkExt, StreamExt};
    use polyflare_codex::oauth::OAuthClient;
    use polyflare_codex::CodexExecutor;
    use polyflare_core::{Continuity, RoundRobin};
    use polyflare_server::app::{build_app, AppState};
    use polyflare_server::continuity::CodexContinuity;
    use polyflare_store::{Account as StoreAccount, PlainTokens, Store, TokenCipher};
    use polyflare_testkit::{MockWsUpstream, ScriptedTurn};
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message as TMessage;

    fn now() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    /// Spawn a real PolyFlare instance with `ws_downstream` on, exactly ONE Codex account seeded
    /// (`id`), and `upstream_base_url` pointed at the mock upstream — so `resolve_owner`'s
    /// unpinned-selection path (Task 3) trivially picks this one account (`RoundRobin` over a
    /// single candidate), and `dial_owner_upstream` (Task 4) connects it to `mock_base`. Returns the
    /// downstream `ws://` base AND the `Arc<AppState>` (kept alive so the test can read
    /// `state.store.continuity()` afterward — `Store` isn't `Clone`, so the state handle is the only
    /// way back to it once `build_app` has consumed a clone of the `Arc`).
    async fn spawn_with_pinned_account(id: &str, mock_base: &str) -> (String, Arc<AppState>) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        std::mem::forget(dir);

        let cipher = TokenCipher::from_key_bytes(&[7u8; 32]).unwrap();
        store
            .accounts()
            .insert(
                &StoreAccount {
                    id: id.to_string(),
                    chatgpt_account_id: None,
                    chatgpt_user_id: None,
                    email: "u@example.test".to_string(),
                    alias: None,
                    workspace_id: None,
                    workspace_label: None,
                    seat_type: None,
                    plan_type: "pro".to_string(),
                    routing_policy: "normal".to_string(),
                    last_refresh: now(), // fresh: never triggers a live OAuth refresh in this test
                    created_at: now(),
                    status: "active".to_string(),
                    deactivation_reason: None,
                    reset_at: None,
                    blocked_at: None,
                    security_work_authorized: false,
                    provider: "codex".to_string(),
                    pool: None,
                },
                &PlainTokens {
                    access_token: "tok".into(),
                    refresh_token: "r".into(),
                    id_token: "i".into(),
                },
                &cipher,
            )
            .await
            .unwrap();

        let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
            store.continuity(),
            Duration::from_secs(30),
        ));
        let codex_executor: Arc<dyn polyflare_core::Executor> =
            Arc::new(CodexExecutor::new().unwrap());
        let anthropic_executor: Arc<dyn polyflare_core::Executor> =
            Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap());

        let state = Arc::new(AppState {
            enforce_client_keys: false,
            codex_executor,
            control_client: polyflare_codex::build_client().expect("build control_client"),
            anthropic_executor,
            // A single seeded account: RoundRobin's tiebreak is moot, it's the only candidate.
            selector: Arc::new(RoundRobin),
            pool_selectors: Default::default(),
            continuity,
            store,
            cipher,
            oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
            // THE crux: dial_owner_upstream builds the upstream WS URL from this field for the
            // Codex provider — pointing it at the mock is what makes the relay dial the mock.
            upstream_base_url: mock_base.to_string(),
            anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
            refresh_locks: Default::default(),
            capture_fingerprint_path: None,
            codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
            account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
            token_cache: Default::default(),
            admin_token: None,
            live_logs: false,
            ws_downstream: true,
            log_bus: polyflare_server::log_bus::LogBus::new(1000),
            max_account_attempts: 3,
            failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
            health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
            starvation_wait_budget: Duration::from_secs(60),
            starvation_heartbeat: Duration::from_secs(10),
            wake_jitter_ms: 0,
            starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
            stream_idle_timeout: Duration::from_secs(300),
            soft_drain_enabled: true,
            request_log_retention_days: 0,
            usage_history_retention_days: 0,
            runtime: Default::default(),
            inflight_penalty_pct: 2.5,
            lease_metrics: polyflare_server::observability::LeaseMetrics::new(),
            upstream_request_metrics: polyflare_server::observability::UpstreamRequestMetrics::new(
            ),
            rate_limit_metrics: polyflare_server::observability::RateLimitMetrics::new(),
            relay_metrics: polyflare_server::observability::RelayMetrics::new(),
            model_catalog: polyflare_server::model_catalog::floor_only_model_catalog(),
        });

        let app = build_app(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("ws://{addr}"), state)
    }

    /// Phase 3, Task 4 variant of [`spawn_with_pinned_account`]: seeds TWO active Codex accounts
    /// (`id_a`, `id_b`), both pointing at the SAME mock upstream (so a re-dial to either succeeds
    /// identically) and deliberately does NOT pre-pin either as an owner — the first turn's
    /// selection runs `resolve_owner`'s unpinned path (`RoundRobin` over the two, tying to the
    /// lexicographically smaller id), so the exhaustion-move test can observe a REAL re-select
    /// rather than asserting on a hardcoded pin.
    async fn spawn_with_two_accounts(
        id_a: &str,
        id_b: &str,
        mock_base: &str,
    ) -> (String, Arc<AppState>) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        std::mem::forget(dir);

        let cipher = TokenCipher::from_key_bytes(&[11u8; 32]).unwrap();
        for id in [id_a, id_b] {
            store
                .accounts()
                .insert(
                    &StoreAccount {
                        id: id.to_string(),
                        chatgpt_account_id: None,
                        chatgpt_user_id: None,
                        email: "u@example.test".to_string(),
                        alias: None,
                        workspace_id: None,
                        workspace_label: None,
                        seat_type: None,
                        plan_type: "pro".to_string(),
                        routing_policy: "normal".to_string(),
                        last_refresh: now(), // fresh: never triggers a live OAuth refresh
                        created_at: now(),
                        status: "active".to_string(),
                        deactivation_reason: None,
                        reset_at: None,
                        blocked_at: None,
                        security_work_authorized: false,
                        provider: "codex".to_string(),
                        pool: None,
                    },
                    &PlainTokens {
                        access_token: format!("tok-{id}"),
                        refresh_token: "r".into(),
                        id_token: "i".into(),
                    },
                    &cipher,
                )
                .await
                .unwrap();
        }

        let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
            store.continuity(),
            Duration::from_secs(30),
        ));
        let codex_executor: Arc<dyn polyflare_core::Executor> =
            Arc::new(CodexExecutor::new().unwrap());
        let anthropic_executor: Arc<dyn polyflare_core::Executor> =
            Arc::new(polyflare_anthropic::AnthropicExecutor::new().unwrap());

        let state = Arc::new(AppState {
            enforce_client_keys: false,
            codex_executor,
            control_client: polyflare_codex::build_client().expect("build control_client"),
            anthropic_executor,
            // Two candidates: RoundRobin ties to the lexicographically smaller id — the test relies
            // on this to know which account gets picked (and thus benched) first.
            selector: Arc::new(RoundRobin),
            pool_selectors: Default::default(),
            continuity,
            store,
            cipher,
            oauth: OAuthClient::new("http://127.0.0.1:9").unwrap(),
            // Both accounts' upstream dial lands on the SAME mock — the move is proven by WHICH
            // account gets benched + who owns the completed turn, not by which URL was dialed.
            upstream_base_url: mock_base.to_string(),
            anthropic_upstream_base_url: "http://127.0.0.1:9".to_string(),
            refresh_locks: Default::default(),
            capture_fingerprint_path: None,
            codex_version: Arc::new(polyflare_codex::CodexVersionCache::new().unwrap()),
            account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
            token_cache: Default::default(),
            admin_token: None,
            live_logs: false,
            ws_downstream: true,
            log_bus: polyflare_server::log_bus::LogBus::new(1000),
            max_account_attempts: 3,
            failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
            health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
            starvation_wait_budget: Duration::from_secs(60),
            starvation_heartbeat: Duration::from_secs(10),
            wake_jitter_ms: 0,
            starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
            stream_idle_timeout: Duration::from_secs(300),
            soft_drain_enabled: true,
            request_log_retention_days: 0,
            usage_history_retention_days: 0,
            runtime: Default::default(),
            inflight_penalty_pct: 2.5,
            lease_metrics: polyflare_server::observability::LeaseMetrics::new(),
            upstream_request_metrics: polyflare_server::observability::UpstreamRequestMetrics::new(
            ),
            rate_limit_metrics: polyflare_server::observability::RateLimitMetrics::new(),
            relay_metrics: polyflare_server::observability::RelayMetrics::new(),
            model_catalog: polyflare_server::model_catalog::floor_only_model_catalog(),
        });

        let app = build_app(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("ws://{addr}"), state)
    }

    /// **The real proof.** A REAL `tokio-tungstenite` client connects to the downstream `/responses`
    /// WS; a `response.create` text frame reaches the `MockWsUpstream` BYTE-VERBATIM (unsorted keys +
    /// doubled interior whitespace survive — a serde reparse would destroy both); the mock's
    /// scripted `response.completed` reply reaches the client verbatim; and the sniffed completed id
    /// is recorded as owned by the account the relay actually dialed
    /// (`store.continuity().get_anchor_owner`) — proving the pump's `on_completed_id` callback is
    /// wired all the way to `Continuity::observe`, not just invoked in isolation.
    #[tokio::test]
    async fn forwards_verbatim_and_records_ownership_on_completion() {
        // The scripted turn streams ONE deliberately non-canonical event frame (keys out of
        // alphabetical order + doubled interior whitespace) before its auto `response.completed`.
        // A parse-then-reserialize on the backend->client leg would sort the keys and collapse the
        // whitespace, so a byte-exact match at the CLIENT proves that leg forwards verbatim too
        // (the client->backend leg is proven separately below via `mock.raw_frames()`).
        let weird_event =
            r#"{"type":"response.output_text.delta",  "z_field":1,  "a_field":2}"#.to_string();
        let mock = MockWsUpstream::new(ScriptedTurn::normal(vec![weird_event.clone()]))
            .capturing_raw_frames();
        let mock_base = mock.clone().spawn().await;

        let (base, state) = spawn_with_pinned_account("acct-relay", &mock_base).await;

        // A realistic downstream handshake: the three content-free identity headers the relay's
        // `session_key` derivation reads (Task 5) — not load-bearing for THIS test's assertions
        // (an unpinned conversation selects the sole account regardless), but present for realism.
        let mut request = format!("{base}/responses").into_client_request().unwrap();
        for (k, v) in [
            ("session-id", "s-relay"),
            ("thread-id", "t-relay"),
            ("x-codex-window-id", "w-relay:0"),
        ] {
            request.headers_mut().insert(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        let (mut ws, _resp) = tokio_tungstenite::connect_async(request)
            .await
            .expect("downstream WS handshake must succeed");

        // Deliberately unsorted keys + doubled interior whitespace: a parse-then-reserialize forward
        // would sort keys and collapse the spaces, so an exact-bytes match at the mock proves the
        // relay's client->backend leg never reparsed the frame.
        let raw =
            r#"{"type":"response.create",  "z_before_a":1,  "a_after_z":2,"input":[]}"#.to_string();
        ws.send(TMessage::Text(raw.clone().into())).await.unwrap();

        // The mock records on its own server task; poll briefly for the frame to land.
        for _ in 0..50 {
            if !mock.raw_frames().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            mock.raw_frames(),
            vec![raw],
            "the relay must forward the client's frame to the upstream mock BYTE-VERBATIM"
        );

        // First frame back is the scripted non-canonical event, which the relay must forward to the
        // client BYTE-VERBATIM (a reparse would sort `a_field` before `z_field` and collapse the
        // doubled whitespace, changing these exact bytes) — the backend->client verbatim proof.
        let TMessage::Text(event) = ws.next().await.expect("an event").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        assert_eq!(
            event.as_str(),
            weird_event,
            "the relay must forward the backend's frame to the client BYTE-VERBATIM"
        );

        // Then `ScriptedTurn::normal` auto-replies `response.completed` (id `resp_1`); the client
        // must receive it verbatim, and its id is what the ownership sniff records below.
        let TMessage::Text(reply) = ws.next().await.expect("a reply").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        let v: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(v["type"], "response.completed");
        assert_eq!(v["response"]["id"], "resp_1");

        // Ownership: the sniffed id must be anchored to the account the relay actually dialed. The
        // write happens async (`on_completed_id` awaited AFTER the client-facing send), so poll
        // briefly rather than asserting immediately.
        let mut owner = None;
        for _ in 0..50 {
            owner = state
                .store
                .continuity()
                .get_anchor_owner("resp_1")
                .await
                .unwrap();
            if owner.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            owner.as_deref(),
            Some("acct-relay"),
            "the completed response's ownership must be pinned to the account the relay dialed, \
             proving the pump's sniff -> Continuity::observe wiring actually ran"
        );
    }

    /// Phase 2, Task 3: a `websocket_connection_limit_reached` reply (the 60-minute server cap)
    /// must be INTERCEPTED by the pump — never forwarded to the client — and the pump must eagerly
    /// re-dial the SAME account so the client's downstream socket never closes and its next frame
    /// is simply answered on the fresh connection.
    #[tokio::test]
    async fn reconnect_on_connection_limit_stays_same_account() {
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::connection_limit_reached(409),
            ScriptedTurn::normal(vec![]),
        ]);
        let mock_base = mock.clone().spawn().await;

        let (base, state) = spawn_with_pinned_account("acct-cap", &mock_base).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake must succeed");

        // Turn 1: the mock replies with the wrapped cap-error envelope.
        let frame1 = r#"{"type":"response.create","input":[]}"#.to_string();
        ws.send(TMessage::Text(frame1.into())).await.unwrap();

        // The client must receive NOTHING — the cap frame is intercepted, not forwarded, and the
        // eager re-dial itself produces no client-visible frame.
        let saw = tokio::time::timeout(Duration::from_millis(300), ws.next()).await;
        assert!(
            saw.is_err(),
            "the client must never receive the intercepted cap frame (it was a timeout, got: {saw:?})"
        );

        // The pump must have eagerly re-dialed the SAME account: a second handshake at the mock.
        let mut handshakes = mock.handshake_count();
        for _ in 0..50 {
            handshakes = mock.handshake_count();
            if handshakes >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            handshakes >= 2,
            "the pump must eagerly re-dial after an intercepted cap frame (handshakes: {handshakes})"
        );

        // Turn 2, on the re-dialed connection: answered `response.completed`; downstream still open.
        let frame2 = r#"{"type":"response.create","input":[],"previous_response_id":"whatever"}"#
            .to_string();
        ws.send(TMessage::Text(frame2.into())).await.unwrap();

        let TMessage::Text(reply) = ws.next().await.expect("a reply").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        let v: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(v["type"], "response.completed");
        let resp_id = v["response"]["id"].as_str().unwrap().to_string();

        // Ownership stays pinned to the SAME account across the reconnect.
        let mut owner = None;
        for _ in 0..50 {
            owner = state
                .store
                .continuity()
                .get_anchor_owner(&resp_id)
                .await
                .unwrap();
            if owner.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            owner.as_deref(),
            Some("acct-cap"),
            "the reconnect must stay on the SAME pinned account"
        );

        // Task 5: the eager cap re-dial must have bumped the content-free
        // `reconnect_same_account` counter (and never `move_cross_account`, since no move
        // happened here).
        let snapshot = state.relay_metrics.snapshot();
        let reconnects = snapshot
            .iter()
            .find(|(k, _)| k == "reconnect_same_account")
            .map(|(_, v)| *v)
            .unwrap_or(0);
        assert!(
            reconnects >= 1,
            "expected reconnect_same_account >= 1 after the cap re-dial, got snapshot: {snapshot:?}"
        );
        assert!(
            !snapshot.iter().any(|(k, _)| k == "move_cross_account"),
            "a same-account cap reconnect must never bump move_cross_account, got: {snapshot:?}"
        );
    }

    /// Phase 2, Task 3: an upstream drop mid-stream (network blip / idle close / anything short of
    /// the client closing) must NOT tear down the client's downstream socket. The next client frame
    /// transparently re-dials the same account and is answered normally.
    #[tokio::test]
    async fn reconnect_on_upstream_drop_keeps_downstream_open() {
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::close_mid_stream(vec![]),
            ScriptedTurn::normal(vec![]),
        ]);
        let mock_base = mock.clone().spawn().await;

        let (base, state) = spawn_with_pinned_account("acct-drop", &mock_base).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake must succeed");

        // Turn 1: the mock accepts the frame, sends nothing, then closes its own socket.
        let frame1 = r#"{"type":"response.create","input":[]}"#.to_string();
        ws.send(TMessage::Text(frame1.into())).await.unwrap();

        // The downstream client must stay OPEN and see nothing (no forwarded close, no error) —
        // the pump marks the upstream dead and parks on the client alone.
        let saw = tokio::time::timeout(Duration::from_millis(300), ws.next()).await;
        assert!(
            saw.is_err(),
            "the downstream must stay open and silent after an upstream drop, got: {saw:?}"
        );

        // Turn 2: the client's next frame transparently re-dials the SAME account.
        let frame2 = r#"{"type":"response.create","input":[],"previous_response_id":"whatever"}"#
            .to_string();
        ws.send(TMessage::Text(frame2.into())).await.unwrap();

        let TMessage::Text(reply) = ws.next().await.expect("a reply").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        let v: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(v["type"], "response.completed");
        let resp_id = v["response"]["id"].as_str().unwrap().to_string();

        assert!(
            mock.handshake_count() >= 2,
            "the relay must have re-dialed a second connection after the upstream drop"
        );

        let mut owner = None;
        for _ in 0..50 {
            owner = state
                .store
                .continuity()
                .get_anchor_owner(&resp_id)
                .await
                .unwrap();
            if owner.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            owner.as_deref(),
            Some("acct-drop"),
            "the reconnect after an upstream drop must stay on the SAME pinned account"
        );
    }

    /// Phase 3, Task 4: a durable upstream error (anything other than the connection-limit / anchor-
    /// missing special cases — here a wrapped 429) must MOVE the conversation to a DIFFERENT account:
    /// bench the account that just errored, re-select (falling through to the other eligible
    /// account), and re-dial it — rather than retrying the same exhausted account forever.
    ///
    /// Two accounts are seeded, deliberately UNPINNED — the selector (`RoundRobin`, ties to the
    /// lexicographically smaller id) picks one for turn 1. Both accounts' upstream dial lands on the
    /// SAME mock, scripted `[rate_limited_429(300), normal(vec![])]`: turn 1 gets the 429 (which must
    /// reach the client verbatim — Design Note 3, the error is forwarded first, THEN the move
    /// happens), turn 2 (on the re-dialed connection) gets a normal completion.
    #[tokio::test]
    async fn durable_error_moves_to_a_second_account() {
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::rate_limited_429(300),
            ScriptedTurn::normal(vec![]),
        ]);
        let mock_base = mock.clone().spawn().await;

        let (base, state) = spawn_with_two_accounts("acct-move-a", "acct-move-b", &mock_base).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake must succeed");

        // Turn 1: the selector picks one account (RoundRobin ties to "acct-move-a"); the mock
        // replies with the wrapped 429 error envelope.
        let frame1 = r#"{"type":"response.create","input":[]}"#.to_string();
        ws.send(TMessage::Text(frame1.into())).await.unwrap();

        // The client MUST receive the 429 error frame verbatim — forwarded, never swallowed.
        let TMessage::Text(err_frame) = ws
            .next()
            .await
            .expect("an error frame")
            .expect("no ws error")
        else {
            panic!("expected a text frame back from the relay");
        };
        let ev: serde_json::Value = serde_json::from_str(&err_frame).unwrap();
        assert_eq!(ev["type"], "error");
        assert_eq!(ev["error"]["code"], "rate_limit_exceeded");

        // The relay must have benched the errored account and re-dialed — a second handshake at the
        // shared mock (whether the SAME or the OTHER account re-dials, the mock sees a new socket).
        let mut handshakes = mock.handshake_count();
        for _ in 0..50 {
            handshakes = mock.handshake_count();
            if handshakes >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            handshakes >= 2,
            "the relay must re-dial after a durable error (handshakes: {handshakes})"
        );

        // Exactly ONE of {A, B} now carries a live cooldown in the runtime overlay — the benched one.
        let mut snaps = vec![
            polyflare_core::AccountSnapshot::new("acct-move-a"),
            polyflare_core::AccountSnapshot::new("acct-move-b"),
        ];
        state.runtime.overlay(&mut snaps, now());
        let benched: Vec<String> = snaps
            .iter()
            .filter(|s| s.cooldown_until.is_some())
            .map(|s| s.id.as_str().to_string())
            .collect();
        assert_eq!(
            benched.len(),
            1,
            "exactly one account must carry a live cooldown after the move, got: {benched:?}"
        );

        // Turn 2, on the re-dialed (moved-to) connection: answered `response.completed`.
        let frame2 = r#"{"type":"response.create","input":[],"previous_response_id":"whatever"}"#
            .to_string();
        ws.send(TMessage::Text(frame2.into())).await.unwrap();

        let TMessage::Text(reply) = ws.next().await.expect("a reply").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        let v: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(v["type"], "response.completed");
        let resp_id = v["response"]["id"].as_str().unwrap().to_string();

        // Ownership: the completed turn's owner must be the account NOT in the benched set — the
        // moved-to account — proving the pump records the CURRENT account, not the original one.
        let mut owner = None;
        for _ in 0..50 {
            owner = state
                .store
                .continuity()
                .get_anchor_owner(&resp_id)
                .await
                .unwrap();
            if owner.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let owner = owner.expect("the completed response must be owned by some account");
        assert_ne!(
            owner, benched[0],
            "the completed turn's owner must NOT be the benched (errored) account"
        );
        assert!(
            owner == "acct-move-a" || owner == "acct-move-b",
            "owner must be one of the two seeded accounts, got {owner}"
        );

        // Task 5: the move must have bumped the content-free `move_cross_account` counter (and
        // never `same_account_anchor_miss`, which is a different signal entirely).
        let snapshot = state.relay_metrics.snapshot();
        let moves = snapshot
            .iter()
            .find(|(k, _)| k == "move_cross_account")
            .map(|(_, v)| *v)
            .unwrap_or(0);
        assert!(
            moves >= 1,
            "expected move_cross_account >= 1 after the durable-error move, got snapshot: {snapshot:?}"
        );
        assert!(
            !snapshot
                .iter()
                .any(|(k, _)| k == "same_account_anchor_miss"),
            "a durable-error move must never bump same_account_anchor_miss, got: {snapshot:?}"
        );
    }

    /// Phase 2/3 Task 5: a `previous_response_not_found` (anchor-miss) arriving while the pinned
    /// account has NOT changed since the last completed turn is the ~31% residual non-resumption
    /// this counter exists to measure. ONE account, no move — the mock scripts
    /// `[normal(vec![]), previous_response_not_found("resp_1")]`: turn 1 completes normally (no
    /// move, `account_changed_since_completed` stays `false`); turn 2's anchor-miss must reach the
    /// client VERBATIM (unchanged from Task 3) AND bump `same_account_anchor_miss` — never
    /// `move_cross_account` (no move ever happened).
    #[tokio::test]
    async fn same_account_anchor_miss_is_counted() {
        let mock = MockWsUpstream::scripted(vec![
            ScriptedTurn::normal(vec![]),
            ScriptedTurn::previous_response_not_found("resp_1"),
        ]);
        let mock_base = mock.clone().spawn().await;

        let (base, state) = spawn_with_pinned_account("acct-anchor-miss", &mock_base).await;

        let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/responses"))
            .await
            .expect("downstream WS handshake must succeed");

        // Turn 1: a normal completion — establishes `account_changed_since_completed == false`.
        let frame1 = r#"{"type":"response.create","input":[]}"#.to_string();
        ws.send(TMessage::Text(frame1.into())).await.unwrap();

        let TMessage::Text(reply1) = ws.next().await.expect("a reply").expect("no ws error") else {
            panic!("expected a text frame back from the relay");
        };
        let v1: serde_json::Value = serde_json::from_str(&reply1).unwrap();
        assert_eq!(v1["type"], "response.completed");

        // Turn 2: the mock replies with the wrapped `previous_response_not_found` envelope — a
        // same-account anchor-miss (no move ever happened on this connection).
        let frame2 =
            r#"{"type":"response.create","input":[],"previous_response_id":"resp_1"}"#.to_string();
        ws.send(TMessage::Text(frame2.into())).await.unwrap();

        let TMessage::Text(reply2) = ws
            .next()
            .await
            .expect("an anchor-miss frame")
            .expect("no ws error")
        else {
            panic!("expected a text frame back from the relay");
        };
        let v2: serde_json::Value = serde_json::from_str(&reply2).unwrap();
        assert_eq!(v2["type"], "error");
        assert_eq!(
            v2["error"]["code"], "previous_response_not_found",
            "the anchor-miss frame must reach the client VERBATIM, unchanged from Task 3"
        );

        // The residual counter must be bumped; the move counter must NOT be (no move happened).
        let mut snapshot = state.relay_metrics.snapshot();
        for _ in 0..50 {
            snapshot = state.relay_metrics.snapshot();
            if snapshot
                .iter()
                .any(|(k, v)| k == "same_account_anchor_miss" && *v >= 1)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let residual = snapshot
            .iter()
            .find(|(k, _)| k == "same_account_anchor_miss")
            .map(|(_, v)| *v)
            .unwrap_or(0);
        assert!(
            residual >= 1,
            "expected same_account_anchor_miss >= 1, got snapshot: {snapshot:?}"
        );
        assert!(
            !snapshot.iter().any(|(k, _)| k == "move_cross_account"),
            "a same-account anchor-miss must never bump move_cross_account, got: {snapshot:?}"
        );
    }
}
