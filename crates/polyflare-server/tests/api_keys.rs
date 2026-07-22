//! Dashboard API-keys subsystem Outcome 1: `GET`/`POST /api/keys` + `PATCH /api/keys/{id}`.
//! Asserts the reveal-once contract (POST returns a raw key whose hash matches the persisted row,
//! and that raw value never resurfaces from GET), the admin gate (401 keyless on all three), and
//! PATCH's enable/disable + unknown-id 404.

mod support;
use support::spawn;

use sha2::{Digest, Sha256};

fn sha256_hex(raw: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    hex::encode(hasher.finalize())
}

#[tokio::test]
async fn post_creates_a_key_and_returns_the_raw_value_once() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await;
    let c = reqwest::Client::new();

    let resp = c
        .post(format!("{pf}/api/keys"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "label": "ci" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();

    let id = body["id"].as_str().unwrap().to_string();
    let key_prefix = body["key_prefix"].as_str().unwrap().to_string();
    let raw = body["key"].as_str().unwrap().to_string();
    assert!(raw.starts_with("sk-pf-"));
    assert!(raw.starts_with(&key_prefix));

    // The row is persisted, keyed by the hash of the raw value (never the raw value itself).
    let hash = sha256_hex(&raw);
    let row = state
        .store
        .api_keys()
        .get_by_hash(&hash)
        .await
        .unwrap()
        .expect("a row must exist under the hash of the revealed raw key");
    assert_eq!(row.id, id);
    assert_eq!(row.key_prefix, key_prefix);
    assert_eq!(row.label.as_deref(), Some("ci"));
    assert!(row.enabled);
    // The hash itself differs from the raw value (sanity: this is really sha256, not an echo).
    assert_ne!(hash, raw);
}

#[tokio::test]
async fn post_without_label_defaults_to_none() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await;
    let c = reqwest::Client::new();

    let resp = c
        .post(format!("{pf}/api/keys"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    let raw = body["key"].as_str().unwrap().to_string();

    let hash = sha256_hex(&raw);
    let row = state
        .store
        .api_keys()
        .get_by_hash(&hash)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.label, None);
}

#[tokio::test]
async fn get_lists_keys_redacted_with_no_hash_or_raw_key_field() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn(up).await;
    let c = reqwest::Client::new();

    let created: serde_json::Value = c
        .post(format!("{pf}/api/keys"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "label": "laptop" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let raw = created["key"].as_str().unwrap().to_string();
    let id = created["id"].as_str().unwrap().to_string();

    let list: serde_json::Value = c
        .get(format!("{pf}/api/keys"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let keys = list["keys"].as_array().unwrap();
    let row = keys.iter().find(|k| k["id"] == id).unwrap();

    assert_eq!(row["key_prefix"], created["key_prefix"]);
    assert_eq!(row["label"], "laptop");
    assert_eq!(row["enabled"], true);
    assert!(row["created_at"].is_number());

    // Never the hash, never the raw key, never any field named after either.
    assert!(row.get("key_hash").is_none(), "no key_hash field at all");
    assert!(row.get("key").is_none(), "no raw key field at all");
    let serialized = serde_json::to_string(&list).unwrap();
    assert!(
        !serialized.contains(&raw),
        "GET /api/keys must never contain the raw key value anywhere in its body"
    );
}

#[tokio::test]
async fn patch_disables_the_key() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await;
    let c = reqwest::Client::new();

    let created: serde_json::Value = c
        .post(format!("{pf}/api/keys"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = created["id"].as_str().unwrap().to_string();
    let raw = created["key"].as_str().unwrap().to_string();

    let resp = c
        .patch(format!("{pf}/api/keys/{id}"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "enabled": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let hash = sha256_hex(&raw);
    let row = state
        .store
        .api_keys()
        .get_by_hash(&hash)
        .await
        .unwrap()
        .unwrap();
    assert!(!row.enabled, "the row must be disabled after the PATCH");

    // GET reflects it too.
    let list: serde_json::Value = c
        .get(format!("{pf}/api/keys"))
        .header("authorization", "Bearer secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let view = list["keys"]
        .as_array()
        .unwrap()
        .iter()
        .find(|k| k["id"] == id)
        .unwrap();
    assert_eq!(view["enabled"], false);
}

#[tokio::test]
async fn patch_re_enables_a_disabled_key() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, state) = spawn(up).await;
    let c = reqwest::Client::new();

    let created: serde_json::Value = c
        .post(format!("{pf}/api/keys"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = created["id"].as_str().unwrap().to_string();
    let raw = created["key"].as_str().unwrap().to_string();

    c.patch(format!("{pf}/api/keys/{id}"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "enabled": false }))
        .send()
        .await
        .unwrap();
    let resp = c
        .patch(format!("{pf}/api/keys/{id}"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "enabled": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let hash = sha256_hex(&raw);
    let row = state
        .store
        .api_keys()
        .get_by_hash(&hash)
        .await
        .unwrap()
        .unwrap();
    assert!(row.enabled);
}

#[tokio::test]
async fn patch_unknown_id_is_404() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn(up).await;
    let c = reqwest::Client::new();

    let resp = c
        .patch(format!("{pf}/api/keys/nope"))
        .header("authorization", "Bearer secret")
        .json(&serde_json::json!({ "enabled": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn get_without_admin_token_is_401() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn(up).await;
    let c = reqwest::Client::new();

    let resp = c.get(format!("{pf}/api/keys")).send().await.unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn post_without_admin_token_is_401() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn(up).await;
    let c = reqwest::Client::new();

    let resp = c
        .post(format!("{pf}/api/keys"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn patch_without_admin_token_is_401() {
    let up = polyflare_testkit::MockUpstream::new(vec![]).spawn().await;
    let (pf, _state) = spawn(up).await;
    let c = reqwest::Client::new();

    let resp = c
        .patch(format!("{pf}/api/keys/whatever"))
        .json(&serde_json::json!({ "enabled": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}
