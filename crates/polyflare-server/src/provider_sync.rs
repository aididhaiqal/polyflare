//! Bidirectional sync of the custom-provider catalog between nodes: providers, their models,
//! their credentials, and per-account model-support facts.
//!
//! # Why this is a MERGE, unlike the accounts sync
//!
//! Account credentials have a single writer by necessity (only the master refreshes OAuth tokens,
//! because refresh tokens rotate), so that sync is a one-way copy. The catalog has no such
//! constraint: it is operator config, edited on whichever node's dashboard is closest — and the
//! observed drift proves it (2026-09-01: `gpt-5.6-sol-pro` existed only on the replica,
//! `ox-alpha` only on the master, one row missing in EACH direction). A one-way copy can never
//! converge that; last-writer-wins by `updated_at` can.
//!
//! # Merge rule
//!
//! Per row, keyed by primary key: insert when absent; overwrite only when the incoming
//! `updated_at` is STRICTLY newer (ties keep local, so a row identical on both sides never
//! ping-pongs). Nothing is ever deleted — a node that briefly serves a short list (mid-migration)
//! must not be able to erase the other's config. Deleting a provider everywhere remains a manual
//! act on each node, documented here deliberately.
//!
//! # Secrets
//!
//! `provider_credentials.api_key_enc` is encrypted with the NODE's own cipher, so ciphertext can
//! not be copied across nodes (each keeps its own key file — same rule as account tokens). The
//! wire carries the decrypted key under `api_key`; the receiving side re-encrypts with its own
//! cipher. Rows that fail to decrypt are skipped, not fatal. The endpoints sit behind
//! `require_admin` and set `no-store`, exactly like `/api/replica/accounts`.
//!
//! # Wire format
//!
//! Rows travel as JSON objects keyed by COLUMN NAME (built by SQLite's `json_object`, applied by
//! name), so the wire format is the named-field contract, not the table's column order. The
//! free-text columns (e.g. `instruction_text`) are carried verbatim as data — they can hold
//! operator-authored prompt text, which is content to replicate, never something to interpret.

use std::sync::Arc;

use axum::{extract::State, http::header, http::StatusCode, response::IntoResponse, Json};
use polyflare_store::{Store, TokenCipher};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::app::AppState;

/// Bump only on a BREAKING change to the payload shape.
pub const CATALOG_CONTRACT_VERSION: u32 = 1;

type JsonRow = Map<String, Value>;

/// One table's shape in the contract. `cols` drives BOTH the reading `json_object(...)` and the
/// applying upsert, so the two sides cannot drift from each other.
struct TableSpec {
    table: &'static str,
    cols: &'static [&'static str],
    pk: &'static [&'static str],
}

const PROVIDERS: TableSpec = TableSpec {
    table: "custom_providers",
    cols: &[
        "id",
        "slug",
        "display_name",
        "base_url",
        "legacy_wire_api",
        "enabled",
        "stateless_responses",
        "allow_private_hosts",
        "connect_timeout_ms",
        "stream_idle_timeout_ms",
        "request_max_retries",
        "max_concurrency",
        "created_at",
        "updated_at",
        "wire_api",
    ],
    pk: &["id"],
};

const MODELS: TableSpec = TableSpec {
    table: "provider_models",
    cols: &[
        "id",
        "provider_id",
        "public_model",
        "upstream_model",
        "display_name",
        "context_window",
        "max_output_tokens",
        "supports_tools",
        "supports_vision",
        "supports_parallel_tool_calls",
        "supports_web_search",
        "supports_reasoning_summaries",
        "reasoning_levels_json",
        "model_info_json",
        "input_per_million",
        "cached_input_per_million",
        "output_per_million",
        "enabled",
        "created_at",
        "updated_at",
        "visible_in_codex",
        "visible_in_openai",
        "instruction_mode",
        "instruction_text",
        "request_overrides_json",
        "priority_input_per_million",
        "priority_cached_input_per_million",
        "priority_output_per_million",
    ],
    pk: &["id"],
};

/// `api_key_enc` is deliberately absent: the secret travels decrypted as `api_key` and is
/// re-encrypted by the receiver (see module doc).
const CREDENTIALS: TableSpec = TableSpec {
    table: "provider_credentials",
    cols: &[
        "id",
        "provider_id",
        "label",
        "enabled",
        "health_status",
        "routing_weight",
        "max_concurrency",
        "cooldown_until",
        "last_error_at",
        "created_at",
        "updated_at",
    ],
    pk: &["id"],
};

const MODEL_SUPPORT: TableSpec = TableSpec {
    table: "account_model_support",
    cols: &["account_id", "model", "supported", "source", "updated_at"],
    pk: &["account_id", "model"],
};

/// The full catalog as one payload. `generated_at` is display-only staleness info.
#[derive(Serialize, Deserialize)]
pub struct CatalogPayload {
    pub contract: u32,
    pub generated_at: i64,
    pub providers: Vec<JsonRow>,
    pub models: Vec<JsonRow>,
    pub credentials: Vec<JsonRow>,
    pub model_support: Vec<JsonRow>,
}

/// Rows applied vs skipped, per table, in apply order.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ApplyReport {
    pub providers_applied: u64,
    pub models_applied: u64,
    pub credentials_applied: u64,
    pub model_support_applied: u64,
    pub skipped: u64,
}

impl ApplyReport {
    pub fn total_applied(&self) -> u64 {
        self.providers_applied
            + self.models_applied
            + self.credentials_applied
            + self.model_support_applied
    }
}

fn unix_now() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn json_object_select(spec: &TableSpec) -> String {
    let pairs = spec
        .cols
        .iter()
        .map(|c| format!("'{c}', {c}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT json_object({pairs}) FROM {} ORDER BY {}",
        spec.table,
        spec.pk.join(", ")
    )
}

async fn fetch_rows(store: &Store, spec: &TableSpec) -> Result<Vec<JsonRow>, sqlx::Error> {
    let raw: Vec<(String,)> = sqlx::query_as(&json_object_select(spec))
        .fetch_all(store.pool())
        .await?;
    Ok(raw
        .into_iter()
        .filter_map(|(j,)| serde_json::from_str::<JsonRow>(&j).ok())
        .collect())
}

/// Read this node's full catalog, decrypting credential secrets for transport.
pub async fn fetch_catalog(
    store: &Store,
    cipher: &TokenCipher,
) -> Result<CatalogPayload, sqlx::Error> {
    let providers = fetch_rows(store, &PROVIDERS).await?;
    let models = fetch_rows(store, &MODELS).await?;
    let model_support = fetch_rows(store, &MODEL_SUPPORT).await?;

    // Credentials: named columns as JSON plus the encrypted secret alongside (BLOBs cannot ride
    // inside json_object), decrypted into the row. A row whose secret fails to decrypt is dropped
    // rather than failing the pull — mirroring the accounts endpoint.
    let pairs = CREDENTIALS
        .cols
        .iter()
        .map(|c| format!("'{c}', {c}"))
        .collect::<Vec<_>>()
        .join(", ");
    let raw: Vec<(String, Vec<u8>)> = sqlx::query_as(&format!(
        "SELECT json_object({pairs}), api_key_enc FROM provider_credentials ORDER BY id"
    ))
    .fetch_all(store.pool())
    .await?;
    let mut credentials = Vec::with_capacity(raw.len());
    for (j, enc) in raw {
        let Ok(mut row) = serde_json::from_str::<JsonRow>(&j) else {
            continue;
        };
        let Ok(api_key) = cipher.decrypt(&enc) else {
            continue;
        };
        row.insert("api_key".to_string(), Value::String(api_key));
        credentials.push(row);
    }

    Ok(CatalogPayload {
        contract: CATALOG_CONTRACT_VERSION,
        generated_at: unix_now(),
        providers,
        models,
        credentials,
        model_support,
    })
}

fn bind_json<'q>(
    q: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    v: &Value,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    match v {
        Value::Null => q.bind(None::<String>),
        Value::Bool(b) => q.bind(*b as i64),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                q.bind(i)
            } else {
                q.bind(n.as_f64().unwrap_or(0.0))
            }
        }
        Value::String(s) => q.bind(s.clone()),
        // Column values are scalars by construction; anything else is carried as its JSON text.
        other => q.bind(other.to_string()),
    }
}

/// Upsert one row with the last-writer-wins guard. Returns true when the row was
/// inserted/updated, false when the local row was same-or-newer (skipped).
async fn apply_row(
    store: &Store,
    spec: &TableSpec,
    row: &JsonRow,
    secret_override: Option<(&str, Vec<u8>)>,
) -> Result<bool, sqlx::Error> {
    let mut cols: Vec<&str> = spec.cols.to_vec();
    if let Some((secret_col, _)) = secret_override {
        cols.push(secret_col);
    }
    let placeholders = (1..=cols.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let set = cols
        .iter()
        .filter(|c| !spec.pk.contains(*c))
        .map(|c| format!("{c}=excluded.{c}"))
        .collect::<Vec<_>>()
        .join(", ");
    // Strictly newer wins; a tie keeps the local row, so identical rows never ping-pong between
    // nodes on alternating syncs.
    let sql = format!(
        "INSERT INTO {t} ({c}) VALUES ({placeholders}) \
         ON CONFLICT({pk}) DO UPDATE SET {set} \
         WHERE excluded.updated_at > {t}.updated_at",
        t = spec.table,
        c = cols.join(", "),
        pk = spec.pk.join(", "),
    );

    let mut q = sqlx::query(&sql);
    for c in spec.cols {
        q = bind_json(q, row.get(*c).unwrap_or(&Value::Null));
    }
    if let Some((_, bytes)) = secret_override {
        q = q.bind(bytes);
    }
    Ok(q.execute(store.pool()).await?.rows_affected() > 0)
}

/// Apply a catalog payload with the LWW rule. Providers land before the models and credentials
/// that reference them; nothing is deleted.
pub async fn apply_catalog(
    store: &Store,
    cipher: &TokenCipher,
    payload: &CatalogPayload,
) -> Result<ApplyReport, sqlx::Error> {
    let mut report = ApplyReport::default();

    for row in &payload.providers {
        if apply_row(store, &PROVIDERS, row, None).await? {
            report.providers_applied += 1;
        } else {
            report.skipped += 1;
        }
    }
    for row in &payload.models {
        if apply_row(store, &MODELS, row, None).await? {
            report.models_applied += 1;
        } else {
            report.skipped += 1;
        }
    }
    for row in &payload.credentials {
        let Some(Value::String(api_key)) = row.get("api_key") else {
            report.skipped += 1;
            continue;
        };
        let Ok(enc) = cipher.encrypt(api_key) else {
            report.skipped += 1;
            continue;
        };
        if apply_row(store, &CREDENTIALS, row, Some(("api_key_enc", enc))).await? {
            report.credentials_applied += 1;
        } else {
            report.skipped += 1;
        }
    }
    for row in &payload.model_support {
        if apply_row(store, &MODEL_SUPPORT, row, None).await? {
            report.model_support_applied += 1;
        } else {
            report.skipped += 1;
        }
    }
    Ok(report)
}

/// Rows in `local` that `remote` lacks, or holds an older copy of — the upload half of a merge.
pub fn rows_newer_than(local: &[JsonRow], remote: &[JsonRow], pk: &[&str]) -> Vec<JsonRow> {
    let key_of = |row: &JsonRow| {
        pk.iter()
            .map(|k| row.get(*k).map(Value::to_string).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\u{0}")
    };
    let updated_of = |row: &JsonRow| row.get("updated_at").and_then(Value::as_i64).unwrap_or(0);
    let remote_index: std::collections::HashMap<String, i64> =
        remote.iter().map(|r| (key_of(r), updated_of(r))).collect();
    local
        .iter()
        .filter(|r| match remote_index.get(&key_of(r)) {
            None => true,
            Some(remote_updated) => updated_of(r) > *remote_updated,
        })
        .cloned()
        .collect()
}

/// `GET /api/replica/catalog` — this node's catalog, credential secrets decrypted for transport.
/// Inside the `require_admin` router; `no-store` because the payload carries API keys.
pub async fn catalog_get_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match fetch_catalog(&state.store, &state.cipher).await {
        Ok(payload) => ([(header::CACHE_CONTROL, "no-store")], Json(payload)).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}

/// `PUT /api/replica/catalog` — merge the sender's rows into this node under the LWW rule.
pub async fn catalog_put_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CatalogPayload>,
) -> impl IntoResponse {
    if payload.contract != CATALOG_CONTRACT_VERSION {
        return (
            StatusCode::CONFLICT,
            format!(
                "peer speaks catalog contract v{}, this build understands v{CATALOG_CONTRACT_VERSION}",
                payload.contract
            ),
        )
            .into_response();
    }
    match apply_catalog(&state.store, &state.cipher, &payload).await {
        Ok(report) => Json(report).into_response(),
        Err(error) => {
            // The sqlx error class (constraint, type) is content-free and is the only clue an
            // operator gets when two builds' schemas disagree mid-merge.
            tracing::warn!(%error, "catalog apply failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pairs: &[(&str, Value)]) -> JsonRow {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    /// The upload half of the merge: local rows the peer lacks, or holds an older copy of. Equal
    /// timestamps are NOT uploaded — combined with the strictly-newer apply guard, that is what
    /// makes alternating syncs of identical catalogs a pair of no-ops.
    #[test]
    fn rows_newer_than_selects_missing_and_strictly_newer_only() {
        let local = vec![
            row(&[
                ("id", Value::from("only-local")),
                ("updated_at", Value::from(10)),
            ]),
            row(&[
                ("id", Value::from("newer-here")),
                ("updated_at", Value::from(20)),
            ]),
            row(&[
                ("id", Value::from("equal")),
                ("updated_at", Value::from(30)),
            ]),
            row(&[
                ("id", Value::from("older-here")),
                ("updated_at", Value::from(5)),
            ]),
        ];
        let remote = vec![
            row(&[
                ("id", Value::from("newer-here")),
                ("updated_at", Value::from(10)),
            ]),
            row(&[
                ("id", Value::from("equal")),
                ("updated_at", Value::from(30)),
            ]),
            row(&[
                ("id", Value::from("older-here")),
                ("updated_at", Value::from(50)),
            ]),
        ];
        let upload = rows_newer_than(&local, &remote, &["id"]);
        let ids: Vec<&str> = upload
            .iter()
            .filter_map(|r| r.get("id")?.as_str())
            .collect();
        assert_eq!(ids, vec!["only-local", "newer-here"]);
    }

    /// `account_model_support` keys on (account_id, model): two rows sharing an account must not
    /// collide, and the composite key must match regardless of unrelated fields.
    #[test]
    fn rows_newer_than_respects_a_composite_key() {
        let local = vec![
            row(&[
                ("account_id", Value::from("acct")),
                ("model", Value::from("model-a")),
                ("updated_at", Value::from(10)),
            ]),
            row(&[
                ("account_id", Value::from("acct")),
                ("model", Value::from("model-b")),
                ("updated_at", Value::from(10)),
            ]),
        ];
        let remote = vec![row(&[
            ("account_id", Value::from("acct")),
            ("model", Value::from("model-a")),
            ("updated_at", Value::from(10)),
        ])];
        let upload = rows_newer_than(&local, &remote, &["account_id", "model"]);
        assert_eq!(upload.len(), 1);
        assert_eq!(upload[0].get("model").unwrap(), "model-b");
    }
}
