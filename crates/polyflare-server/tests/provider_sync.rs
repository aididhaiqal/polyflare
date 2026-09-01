//! `/api/replica/catalog` — the bidirectional custom-provider merge.
//!
//! The payload carries provider API keys, so the tests that matter most are the auth gate and
//! that a refusal leaks nothing; after that, the last-writer-wins rule, which is what makes the
//! merge safe to run from either node.

mod support;

use serde_json::{json, Map, Value};

fn as_map(v: Value) -> Map<String, Value> {
    match v {
        Value::Object(m) => m,
        _ => panic!("fixture must be an object"),
    }
}

fn provider_row(id: &str, slug: &str, display_name: &str, updated_at: i64) -> Map<String, Value> {
    as_map(json!({
        "id": id,
        "slug": slug,
        "display_name": display_name,
        "base_url": "https://api.example.test/v1",
        "legacy_wire_api": "responses",
        "enabled": 1,
        "stateless_responses": 1,
        "allow_private_hosts": 0,
        "connect_timeout_ms": 10000,
        "stream_idle_timeout_ms": 300000,
        "request_max_retries": 1,
        "max_concurrency": Value::Null,
        "created_at": 1_700_000_000,
        "updated_at": updated_at,
        "wire_api": "responses",
    }))
}

fn model_row(
    id: &str,
    provider_id: &str,
    public_model: &str,
    updated_at: i64,
) -> Map<String, Value> {
    as_map(json!({
        "id": id,
        "provider_id": provider_id,
        "public_model": public_model,
        "upstream_model": public_model,
        "display_name": public_model,
        "context_window": Value::Null,
        "max_output_tokens": Value::Null,
        "supports_tools": 1,
        "supports_vision": 0,
        "supports_parallel_tool_calls": 1,
        "supports_web_search": 0,
        "supports_reasoning_summaries": 0,
        "reasoning_levels_json": "[\"high\"]",
        "model_info_json": Value::Null,
        "input_per_million": Value::Null,
        "cached_input_per_million": Value::Null,
        "output_per_million": Value::Null,
        "enabled": 1,
        "created_at": 1_700_000_000,
        "updated_at": updated_at,
        "visible_in_codex": 1,
        "visible_in_openai": 1,
        "instruction_mode": "none",
        "instruction_text": "",
        "request_overrides_json": "{}",
        "priority_input_per_million": Value::Null,
        "priority_cached_input_per_million": Value::Null,
        "priority_output_per_million": Value::Null,
    }))
}

fn credential_row(
    id: &str,
    provider_id: &str,
    label: &str,
    api_key: &str,
    updated_at: i64,
) -> Map<String, Value> {
    as_map(json!({
        "id": id,
        "provider_id": provider_id,
        "label": label,
        "enabled": 1,
        "health_status": "healthy",
        "routing_weight": 1.0,
        "max_concurrency": Value::Null,
        "cooldown_until": Value::Null,
        "last_error_at": Value::Null,
        "created_at": 1_700_000_000,
        "updated_at": updated_at,
        "api_key": api_key,
    }))
}

fn payload(
    providers: Vec<Map<String, Value>>,
    models: Vec<Map<String, Value>>,
    credentials: Vec<Map<String, Value>>,
) -> Value {
    json!({
        "contract": 1,
        "generated_at": 1_700_000_000,
        "providers": providers,
        "models": models,
        "credentials": credentials,
        "model_support": [
            {"account_id": "acct-1", "model": "hidden-model", "supported": 1,
             "source": "probe", "updated_at": 1_700_000_000}
        ],
    })
}

#[tokio::test]
async fn catalog_requires_admin_and_a_refusal_leaks_no_key() {
    let (pf, app_state) = support::spawn("http://127.0.0.1:9".to_string()).await;
    // Seed a real credential so there IS a secret that could leak.
    let enc = app_state.cipher.encrypt("sk-super-secret-key").unwrap();
    sqlx::query(
        "INSERT INTO custom_providers (id, slug, display_name, base_url, enabled, created_at, updated_at) \
         VALUES ('prov-1','testprov','Test','https://x.test',1,1,1)",
    )
    .execute(app_state.store.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO provider_credentials (id, provider_id, label, api_key_enc, enabled, created_at, updated_at) \
         VALUES ('cred-1','prov-1','main',?,1,1,1)",
    )
    .bind(enc)
    .execute(app_state.store.pool())
    .await
    .unwrap();

    let client = reqwest::Client::new();
    let get = client
        .get(format!("{pf}/api/replica/catalog"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 401);
    let body = get.text().await.unwrap();
    assert!(
        !body.contains("sk-super-secret"),
        "401 body must not leak: {body}"
    );

    let put = client
        .put(format!("{pf}/api/replica/catalog"))
        .json(&payload(vec![], vec![], vec![]))
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 401);
}

#[tokio::test]
async fn catalog_round_trips_and_reencrypts_the_key_under_the_local_cipher() {
    let (pf, app_state) = support::spawn("http://127.0.0.1:9".to_string()).await;
    let client = reqwest::Client::new();

    let raw = client
        .put(format!("{pf}/api/replica/catalog"))
        .header("authorization", "Bearer secret")
        .json(&payload(
            vec![provider_row("prov-a", "peer", "Peer Provider", 100)],
            vec![model_row("model-a", "prov-a", "peer/shiny-model", 100)],
            vec![credential_row(
                "cred-a",
                "prov-a",
                "main",
                "sk-from-peer",
                100,
            )],
        ))
        .send()
        .await
        .unwrap();
    let status = raw.status();
    let text = raw.text().await.unwrap();
    assert!(status.is_success(), "PUT failed: HTTP {status}: {text}");
    let put: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(put["providers_applied"], 1);
    assert_eq!(put["models_applied"], 1);
    assert_eq!(put["credentials_applied"], 1);
    assert_eq!(put["model_support_applied"], 1);

    // The stored secret must decrypt under THIS node's cipher — the wire carried plaintext and
    // the receiver re-encrypted, because ciphertext cannot move between nodes with different keys.
    let (enc,): (Vec<u8>,) =
        sqlx::query_as("SELECT api_key_enc FROM provider_credentials WHERE id='cred-a'")
            .fetch_one(app_state.store.pool())
            .await
            .unwrap();
    assert_eq!(app_state.cipher.decrypt(&enc).unwrap(), "sk-from-peer");

    // And the GET side serves it back decrypted, named, and uncacheable.
    let get = client
        .get(format!("{pf}/api/replica/catalog"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap();
    let cache = get
        .headers()
        .get("cache-control")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(cache.contains("no-store"), "cache-control: {cache}");
    let body: serde_json::Value = get.json().await.unwrap();
    assert_eq!(body["contract"], 1);
    let model = body["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["id"] == "model-a")
        .expect("merged model served back");
    assert_eq!(model["public_model"], "peer/shiny-model");
    let cred = body["credentials"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["id"] == "cred-a")
        .expect("merged credential served back");
    assert_eq!(cred["api_key"], "sk-from-peer");
}

/// The rule that makes the merge safe to run from either node: only a STRICTLY newer row
/// overwrites, so an out-of-date peer can never roll back an edit, and identical rows never
/// ping-pong between nodes on alternating syncs.
#[tokio::test]
async fn merge_is_last_writer_wins() {
    let (pf, _) = support::spawn("http://127.0.0.1:9".to_string()).await;
    let client = reqwest::Client::new();
    let put = |prov: Map<String, Value>| {
        let client = client.clone();
        let url = format!("{pf}/api/replica/catalog");
        async move {
            client
                .put(url)
                .header("authorization", "Bearer secret")
                .json(&payload(vec![prov], vec![], vec![]))
                .send()
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap()
        }
    };
    let name_now = || async {
        let body: serde_json::Value = client
            .get(format!("{pf}/api/replica/catalog"))
            .header("authorization", "Bearer secret")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        body["providers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["id"] == "prov-lww")
            .map(|p| p["display_name"].as_str().unwrap().to_string())
    };

    let first = put(provider_row("prov-lww", "lww", "Current", 100)).await;
    assert_eq!(first["providers_applied"], 1);

    // An OLDER copy from the peer must not roll the row back.
    let stale = put(provider_row("prov-lww", "lww", "Stale", 50)).await;
    assert_eq!(
        stale["providers_applied"], 0,
        "an older row must be skipped"
    );
    assert_eq!(name_now().await.as_deref(), Some("Current"));

    // A TIE keeps local: this is what stops identical rows ping-ponging between nodes.
    let tie = put(provider_row("prov-lww", "lww", "Tie", 100)).await;
    assert_eq!(tie["providers_applied"], 0, "a tie must keep the local row");
    assert_eq!(name_now().await.as_deref(), Some("Current"));

    // A newer edit wins.
    let newer = put(provider_row("prov-lww", "lww", "Newer", 200)).await;
    assert_eq!(newer["providers_applied"], 1);
    assert_eq!(name_now().await.as_deref(), Some("Newer"));
}

#[tokio::test]
async fn a_mismatched_contract_is_refused() {
    let (pf, _) = support::spawn("http://127.0.0.1:9".to_string()).await;
    let mut body = payload(vec![], vec![], vec![]);
    body["contract"] = json!(99);
    let response = reqwest::Client::new()
        .put(format!("{pf}/api/replica/catalog"))
        .header("authorization", "Bearer secret")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 409);
}
