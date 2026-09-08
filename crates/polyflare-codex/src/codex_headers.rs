//! Synthesizes the HTTP header STRUCTURE a real `codex-rs` sends on `POST /responses` — the
//! egress-parity half of the fingerprint-parity gate (see `executor.rs` and
//! `polyflare-server/tests/codex_fingerprint_parity_gate.rs`).
//!
//! # Status: CAPTURE-VERIFIED (codex-cli 0.144.4, 2026-07-15); SOURCE-VERIFIED through 0.153.4 (2026-09-08)
//! Originally built from a local `openai/codex` source read, this synthesis has since been
//! diffed against a live wire capture of the real Codex CLI (`codex-cli 0.144.4`, obtained by
//! routing a `scripts/codex-polyflare` run through `POLYFLARE_CAPTURE_FINGERPRINT`). The capture
//! CONFIRMED the base identity-header set, the `x-codex-turn-metadata` field-key set, and the UA
//! format below. The [`CODEX_CLI_VERSION`] floor was later bumped 0.144.4 → 0.145.0 after a
//! SOURCE-level diff of openai/codex (0f44bca → 37eef7bac) showed no change to this synthesized
//! structure — only the embedded UA version moved (a byte-level re-capture at 0.145.0 is still
//! recommended; see the const doc). It also revealed two headers this module deliberately does NOT synthesize, both
//! of which represent OPTIONAL codex states (so omitting them is itself a valid codex fingerprint):
//! - `x-codex-beta-features` — a comma-separated list of the session's enabled experimental
//!   feature keys (`core/src/session/mod.rs::build_model_client_beta_features_header`). Absent
//!   entirely when no such feature is enabled (`if beta_features_header.is_empty() { None }`).
//!   A translated non-codex client has no enabled beta features, so its absence is correct.
//! - `x-openai-internal-codex-responses-lite: true` — added only for models with
//!   `use_responses_lite` (`core/src/client.rs::add_responses_lite_header`); absent for non-lite
//!   models. Omitting it matches a non-lite-model codex.
//!
//! Both are relayed untouched on the NATIVE forward path (a real Codex client that sends them has
//! them forwarded verbatim by the executor) — synthesis only governs the TRANSLATED path.
//!
//! Note on the capture's own UA: it read `codex_exec/...` because the capture used `codex exec`;
//! the interactive `codex` CLI (what a translated client impersonates) uses `codex_cli_rs`, the
//! `originator` this module synthesizes. Both share the UA FORMAT the capture confirmed.
//!
//! # Source verification (local `openai/codex` checkout)
//! - UA format (`{originator}/{version} ({os_type} {os_version}; {arch}) {terminal}`), the
//!   `codex_cli_rs` default `originator`, and the base `originator`/`user-agent` headers:
//!   `codex-rs/login/src/auth/default_client.rs` (`get_codex_user_agent`, `default_headers`,
//!   `DEFAULT_ORIGINATOR`).
//! - `accept: text/event-stream` (hardcoded on every `/responses` stream), `session-id` /
//!   `thread-id` (hyphenated, not underscored), `x-client-request-id` (= the thread id, verbatim):
//!   `codex-rs/codex-api/src/endpoint/responses.rs` (`stream_request`/`stream_encoded`) and
//!   `codex-rs/codex-api/src/requests/headers.rs` (`build_session_headers`).
//! - `x-codex-window-id` (always present; format `<thread_id>:<n>`) and the
//!   `x-codex-turn-metadata` JSON field set: `codex-rs/core/src/responses_metadata.rs`
//!   (`compatibility_headers`, `CodexTurnMetadataPayload`) and
//!   `codex-rs/core/src/session/mod.rs::current_window_id`.
//!
//! # Deviation from the task's initial file-path summary
//! The current (heavily-refactored) `codex-rs` splits this logic across more crates than a
//! `core/src/default_client.rs` + `core/src/client.rs` pairing alone: `get_codex_user_agent` lives
//! in `login/src/auth/default_client.rs` (there is no `core/src/default_client.rs`), and the
//! per-request header assembly for the `/responses` POST lives in the newer `codex-api` crate
//! (`codex-api/src/endpoint/responses.rs`, `codex-api/src/requests/headers.rs`), not directly in
//! `core/src/client.rs` (which does still define the `X_CODEX_*` header name constants and
//! `add_originator_header`/`compatibility_headers` glue). The header NAMES, UA FORMAT, and
//! turn-metadata JSON field set are otherwise as summarized in the task.
//!
//! # What is NOT synthesized here (out of scope for this M1 baseline turn)
//! The real `CodexTurnMetadataPayload` also carries `forked_from_thread_id`, `parent_thread_id`,
//! `subagent_kind`, `compaction`, and a flattened `extra` map — all conditional on
//! forking/subagent/compaction flows this executor doesn't model yet. Only the always-relevant
//! baseline-turn field set is synthesized (see [`TurnIdentity::turn_metadata_json`]).
//!
//! # Content safety
//! The ids synthesized here are deterministic, non-secret, synthetic structural placeholders —
//! never a real account/session identifier. They must still never be logged (mirrors
//! `PreparedRequest`/`Account`'s own redacted `Debug` impls in `polyflare-core`).

use sha2::{Digest, Sha256};

/// The `codex-rs` CLI release version embedded in its User-Agent. Byte-capture-verified against
/// live `codex-cli` runs through 0.144.4 (2026-07-15); update in lockstep with the codex-rs release
/// PolyFlare mirrors on egress (a stale version here is a fingerprint tell against a newer real
/// codex). Re-capturing across the 0.144.x line (0.144.1 → 0.144.4) showed the egress fingerprint is
/// patch-stable — the header set, turn-metadata key set, and UA FORMAT are identical; only this
/// version string moves. The 0.144.4 → 0.145.0 bump was verified at the SOURCE level (openai/codex
/// 0f44bca → 37eef7bac): no request-header, UA-format, or turn-metadata-key change — only the
/// embedded version and three models' `context_window` (372k → 272k, carried live via the model
/// catalog, never hardcoded here). A byte-level golden re-capture (`POLYFLARE_CAPTURE_FINGERPRINT`)
/// against a real 0.145.0 client is still recommended to promote 0.145.0 from source-verified to
/// capture-verified.
///
/// **2026-09-08:** raised from 0.145.0 to 0.153.4 as a floor. The upstream `/models` catalog is
/// gated on `client_version` (below 0.153.0 it omits gpt-6-astra), so a cold or source-less
/// version cache must not fall back to a version the backend treats as obsolete. This is the
/// version the fleet has been running live from the version cache for weeks. Fingerprint
/// verification is tracked SEPARATELY in [`FINGERPRINT_VERIFIED_THROUGH`] so raising the floor
/// never silently claims a byte-capture that was not done.
pub const CODEX_CLI_VERSION: &str = "0.153.4";

/// The newest codex-rs release whose egress fingerprint (header set, UA format, turn-metadata
/// keys) PolyFlare has verified — 0.145.0 at the source level (see above). Drives only the
/// drift warning in `codex_version`; a re-capture (`POLYFLARE_CAPTURE_FINGERPRINT`) against a
/// real client is what promotes it.
///
/// **2026-09-08: 0.153.4, source-verified** against the released tag `rust-v0.153.4` (openai/codex,
/// 2026-09-04): `login/src/auth/default_client.rs` (originator, UA format, `default_headers`),
/// `terminal-detection/src/lib.rs` (`unknown` token), `codex-api/src/requests/headers.rs` and
/// `codex-api/src/endpoint/responses.rs` (session/thread/x-client-request-id/accept),
/// `core/src/client.rs` (`x-codex-routing-hint`, turn-state, beta-features, responses-lite),
/// `core/src/responses_metadata.rs` + `core/src/turn_metadata.rs` + `core/src/sandbox_tags.rs`
/// and `sandboxing/src/manager.rs` (turn-metadata key set and values), `protocol/src/protocol.rs`
/// (`ThreadSource` strings). Drift found and fixed in that pass: the always-present
/// `x-codex-routing-hint` header, and the turn-metadata payload (`agent_name`, `sandbox_mode`,
/// `auto_review_enabled`, `node_repl_*`, `thread_source: "user"`, platform `sandbox` tag, no
/// empty `workspaces`). A byte-level re-capture against a real 0.153.4 client is what would
/// promote this to capture-verified.
pub const FINGERPRINT_VERIFIED_THROUGH: &str = "0.153.4";

/// codex-rs's default `originator` (`login/src/auth/default_client.rs::DEFAULT_ORIGINATOR`).
const ORIGINATOR: &str = "codex_cli_rs";

/// The `originator` value PolyFlare's Codex egress identifies as.
pub fn originator() -> &'static str {
    ORIGINATOR
}

/// The codex-rs User-Agent FORMAT: `{originator}/{version} ({os_type} {os_version}; {arch})
/// {terminal}` — verified from `login/src/auth/default_client.rs::get_codex_user_agent`.
///
/// `version` is the live codex release resolved by [`crate::codex_version::CodexVersionCache`]
/// (which itself falls back to [`CODEX_CLI_VERSION`] when upstream sources are down) — passed in so
/// the synthesized User-Agent tracks the real fleet's current version instead of a stale constant.
/// `os_type`/`os_version`/`arch` come from the `os_info` crate exactly as codex-rs itself calls it
/// (`os_info::get().{os_type,version,architecture}`); when `os_info` can't determine the
/// architecture this falls back to `std::env::consts::ARCH` instead of codex-rs's own literal
/// `"unknown"` fallback — a deliberate improvement, flagged here as a deviation. `terminal` is the
/// fixed [`TERMINAL_TOKEN`].
pub fn codex_user_agent(version: &str) -> String {
    let info = os_info::get();
    let arch = info
        .architecture()
        .map(str::to_string)
        .unwrap_or_else(|| std::env::consts::ARCH.to_string());
    format!(
        "{ORIGINATOR}/{version} ({} {}; {arch}) {TERMINAL_TOKEN}",
        info.os_type(),
        info.version(),
    )
}

/// The terminal-identity token codex-rs appends to its User-Agent. codex-rs derives this from the
/// live terminal (`TERM_PROGRAM`/`TERM` → `codex_terminal_detection`), producing e.g.
/// `iTerm.app/3.5` — but for a TRANSLATED (non-codex) client PolyFlare has no client terminal, and
/// reading PolyFlare's OWN `TERM_PROGRAM` would leak the *server's* deployment environment into the
/// synthesized fingerprint (headless prod → one value, a dev shell in iTerm → another) — an
/// unstable tell. Instead we pin codex-rs's own unknown-terminal literal (`"unknown"`, from
/// `codex_terminal_detection`'s `TerminalName::Unknown => "unknown"`), which is exactly what a real
/// codex emits when run headless / with no `TERM_PROGRAM` — a valid, stable codex fingerprint that
/// matches the non-interactive nature of a translated API request. Capture-verified: the live
/// `codex exec` capture confirmed the terminal token occupies this UA position.
const TERMINAL_TOKEN: &str = "unknown";

/// Derives a stable per-conversation key from a prepared request body.
///
/// Prefers `prompt_cache_key` — codex-rs's own per-conversation cache key, stable turn-to-turn by
/// design (`docs/reference/codex-lb-continuity-reference.md` / `session_key.rs` already treat it
/// as a soft session-affinity signal for this same reason). Deliberately does NOT use
/// `previous_response_id`: that value changes every turn (it names the *previous* turn's
/// response), so hashing it would make the derived ids themselves change every request — exactly
/// the fingerprint tell this derivation exists to avoid.
///
/// Falls back to `model` when `prompt_cache_key` is absent, so the derived ids stay deterministic
/// rather than random — but this fallback is NOT per-conversation (every request for the same
/// model collapses to the same key). **Limitation** (flagged per the task): `PreparedRequest`
/// carries no dedicated per-conversation key field, and `Executor::execute` receives no
/// `RequestCtx` (see `polyflare_core::traits::Executor`) — so when a client omits
/// `prompt_cache_key`, no per-conversation identity reaches the executor at all.
pub fn conversation_key(body: &serde_json::Value) -> String {
    body.get("prompt_cache_key")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or_else(|| {
            body.get("model")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| "polyflare-no-conversation-key".to_string())
}

/// The synthesized per-turn identity set — everything derived deterministically from a stable
/// per-conversation key, never from randomness. A value that changes every request is itself a
/// fingerprint tell, so the same conversation key must always yield the same ids.
///
/// capture-pending: exact id formats/derivation confirmed by golden. Real codex-rs ids are
/// server/client-generated UUIDv4s (`core/src/installation_id.rs`, `core/src/thread_manager.rs`);
/// these are deterministic, UUID-SHAPED (`8-4-4-4-12` hex) synthetic stand-ins, not a
/// byte-for-byte match.
pub struct TurnIdentity {
    pub installation_id: String,
    pub session_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub window_id: String,
}

impl TurnIdentity {
    /// Derives the full identity set from one stable per-conversation key.
    ///
    /// `window_id` follows codex-rs's own `<thread_id>:<n>` format
    /// (`core/src/session/mod.rs::current_window_id`); `n` is fixed at `1` since PolyFlare
    /// doesn't yet track a per-conversation window-advance counter (capture-pending).
    ///
    /// `turn_id` is also derived from the conversation key rather than varying per turn as real
    /// codex-rs's does (`core/src/turn_metadata.rs`'s `TurnMetadataState::turn_id` is set fresh
    /// per turn) — PolyFlare's executor has no turn sequence counter yet (capture-pending).
    pub fn derive(conversation_key: &str) -> Self {
        let thread_id = deterministic_uuid_shaped("thread", conversation_key);
        Self {
            installation_id: deterministic_uuid_shaped("installation", conversation_key),
            session_id: deterministic_uuid_shaped("session", conversation_key),
            turn_id: deterministic_uuid_shaped("turn", conversation_key),
            window_id: format!("{thread_id}:1"),
            thread_id,
        }
    }

    /// The `x-codex-turn-metadata` JSON payload: a compact-separator JSON object (matching
    /// codex-rs's own compact, non-pretty serialization — see
    /// `core/src/responses_metadata.rs::turn_metadata_json` / `to_ascii_json_string`; this
    /// synthesis skips codex-rs's additional non-ASCII-escaping formatter since every synthesized
    /// field here is already plain ASCII, so the two are byte-identical in practice for this
    /// payload).
    ///
    /// Field set verified from source (`CodexTurnMetadataPayload` in
    /// `core/src/responses_metadata.rs`): `installation_id`, `session_id`, `thread_id`,
    /// `turn_id`, `window_id`, `request_kind`, `sandbox`, `thread_source`, `workspaces`,
    /// `turn_started_at_unix_ms`. See the module doc for the additional real fields
    /// (`forked_from_thread_id`/`parent_thread_id`/`subagent_kind`/`compaction`/`extra`)
    /// deliberately omitted as out of scope for this baseline-turn synthesis.
    /// The baseline interactive-turn payload with the per-model flags at their most common
    /// values (both `node_repl_*` false — every catalog model except gpt-6-astra as of
    /// 2026-09-08). Prefer [`Self::turn_metadata_json_for`] when the model's catalog entry is
    /// at hand.
    pub fn turn_metadata_json(&self) -> String {
        self.turn_metadata_json_for(&ModelTurnFlags::default())
    }

    /// The `x-codex-turn-metadata` payload codex-rs 0.153.4 sends on an ordinary interactive
    /// turn (`core/src/turn_metadata.rs::to_responses_metadata` + `responses_metadata_template`,
    /// `core/src/sandbox_tags.rs::record_metadata`, `core/src/tasks/mod.rs::start_task`):
    /// - `agent_name` is the root agent path (`AgentPath::root()` → `"/root"`), always.
    /// - `thread_source` is `ThreadSource::User` → `"user"` (NOT the `SessionSource` `"cli"`).
    /// - `sandbox` is the platform sandbox tag (`seatbelt` / `seccomp` / `none`) and the policy
    ///   moved to `sandbox_mode` (`workspace-write`, the CLI default).
    /// - `auto_review_enabled` is `routes_approval_policy_to_guardian(policy, reviewer)`, which is
    ///   `false` whenever the reviewer is the default `User`.
    /// - `node_repl_auto_review_required` / `node_repl_disabled` come from the model's catalog
    ///   entry (`model_info`), so they are per model.
    /// - `workspaces` is emitted only when git enrichment found one (`non_empty_workspaces`);
    ///   an empty `{}` never occurs live, so it is omitted.
    /// - `turn_started_at_unix_ms` is set unconditionally at task start.
    /// - `turn_trigger`, `window_number`, `context_window_id` are absent on an ordinary turn.
    pub fn turn_metadata_json_for(&self, flags: &ModelTurnFlags) -> String {
        let turn_started_at_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        serde_json::json!({
            "installation_id": self.installation_id,
            "session_id": self.session_id,
            "thread_id": self.thread_id,
            "agent_name": ROOT_AGENT_PATH,
            "turn_id": self.turn_id,
            "window_id": self.window_id,
            "request_kind": "turn",
            "thread_source": "user",
            "sandbox": platform_sandbox_tag(),
            "sandbox_mode": "workspace-write",
            "auto_review_enabled": false,
            "node_repl_auto_review_required": flags.node_repl_auto_review_required,
            "node_repl_disabled": flags.node_repl_disabled,
            "turn_started_at_unix_ms": turn_started_at_unix_ms,
        })
        .to_string()
    }
}

/// codex-rs `AgentPath::root()` as it serializes into `agent_name` for the main (non-subagent)
/// thread.
const ROOT_AGENT_PATH: &str = "/root";

/// The per-model booleans codex-rs copies from the model's catalog `model_info` into the
/// turn metadata (`core/src/turn_metadata.rs::TurnMetadataState::new`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelTurnFlags {
    pub node_repl_auto_review_required: bool,
    pub node_repl_disabled: bool,
    /// `model_info.use_responses_lite`. True for every current flagship (gpt-5.6-sol,
    /// gpt-6-astra, ...). Drives the `x-openai-internal-codex-responses-lite: true` header,
    /// `parallel_tool_calls: false`, and `reasoning.context: "all_turns"`.
    pub use_responses_lite: bool,
    /// `model_info.supports_parallel_tool_calls`.
    pub supports_parallel_tool_calls: bool,
    /// `model_info.support_verbosity` — when false codex sends no `text` block at all.
    pub support_verbosity: bool,
    /// `model_info.default_verbosity` (`"low"` on the current flagships).
    pub default_verbosity: Option<String>,
    /// `model_info.default_reasoning_level`, used when the client named no effort.
    pub default_reasoning_level: Option<String>,
    /// `model_info.default_reasoning_summary`; `"none"` (the current default) means the
    /// `reasoning.summary` key is omitted.
    pub default_reasoning_summary: Option<String>,
}

/// Shape the translated `/responses` body the way codex-rs 0.153.4 builds
/// `ResponsesApiRequest` (`codex-api/src/common.rs`, `core/src/client.rs::build_responses_request`)
/// so the body agrees with the identity headers sent alongside it:
/// - `prompt_cache_key` IS the session id (`ModelClient::prompt_cache_key` returns
///   `session_id` absent an override), so it must equal the `session-id` header and the
///   `session_id` in both metadata blocks.
/// - `include: ["reasoning.encrypted_content"]`, `tool_choice: "auto"`, `store: false` and
///   `stream: true` are unconditional.
/// - `parallel_tool_calls = supports && !use_responses_lite`.
/// - `text.verbosity` is the model default when the model supports verbosity, else no `text`.
/// - `reasoning.effort` defaults to the model's `default_reasoning_level`;
///   `reasoning.context: "all_turns"` under responses-lite; `summary` only when the model's
///   default summary is not `"none"`.
/// - `client_metadata` carries the same identity as the headers (`x-codex-installation-id`,
///   `session_id`, `thread_id`, `x-codex-window-id`, `turn_id`, `x-codex-turn-metadata`), per
///   `core/src/responses_metadata.rs::client_metadata`.
///
/// Fields the client already set (its own effort, tool_choice, tools) are kept; only absent
/// ones are filled. Never logs the body.
pub fn apply_codex_body_defaults(
    body: &mut serde_json::Value,
    identity: &TurnIdentity,
    flags: &ModelTurnFlags,
    turn_metadata_json: &str,
) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    obj.insert(
        "prompt_cache_key".to_string(),
        serde_json::Value::String(identity.session_id.clone()),
    );
    obj.insert("store".to_string(), serde_json::Value::Bool(false));
    obj.insert("stream".to_string(), serde_json::Value::Bool(true));
    obj.insert(
        "include".to_string(),
        serde_json::json!(["reasoning.encrypted_content"]),
    );
    obj.entry("tool_choice")
        .or_insert_with(|| serde_json::Value::String("auto".to_string()));
    obj.insert(
        "parallel_tool_calls".to_string(),
        serde_json::Value::Bool(flags.supports_parallel_tool_calls && !flags.use_responses_lite),
    );
    if flags.support_verbosity {
        if let Some(verbosity) = &flags.default_verbosity {
            obj.entry("text")
                .or_insert_with(|| serde_json::json!({ "verbosity": verbosity }));
        }
    }
    let reasoning = obj
        .entry("reasoning")
        .or_insert_with(|| serde_json::json!({}));
    if let Some(reasoning) = reasoning.as_object_mut() {
        if !reasoning.contains_key("effort") {
            if let Some(level) = &flags.default_reasoning_level {
                reasoning.insert(
                    "effort".to_string(),
                    serde_json::Value::String(level.clone()),
                );
            }
        }
        if let Some(summary) = &flags.default_reasoning_summary {
            if summary != "none" && !reasoning.contains_key("summary") {
                reasoning.insert(
                    "summary".to_string(),
                    serde_json::Value::String(summary.clone()),
                );
            }
        }
        if flags.use_responses_lite {
            reasoning.insert(
                "context".to_string(),
                serde_json::Value::String("all_turns".to_string()),
            );
        }
    }
    obj.insert(
        "client_metadata".to_string(),
        serde_json::json!({
            "x-codex-installation-id": identity.installation_id,
            "session_id": identity.session_id,
            "thread_id": identity.thread_id,
            "x-codex-window-id": identity.window_id,
            "turn_id": identity.turn_id,
            "x-codex-turn-metadata": turn_metadata_json,
        }),
    );
}

/// The header codex-rs adds on every request for a responses-lite model
/// (`core/src/client.rs::add_responses_lite_header`).
pub const RESPONSES_LITE_HEADER: &str = "x-openai-internal-codex-responses-lite";

/// The `sandbox` tag codex-rs records for this platform
/// (`sandboxing/src/manager.rs::get_platform_sandbox` → `SandboxType::as_metric_tag`).
pub fn platform_sandbox_tag() -> &'static str {
    if cfg!(target_os = "macos") {
        "seatbelt"
    } else if cfg!(target_os = "linux") {
        "seccomp"
    } else {
        "none"
    }
}

/// The `x-codex-routing-hint` value codex-rs 0.153.4 sends on every HTTP `/responses` turn under
/// ChatGPT login (`core/src/client.rs::build_routing_hint_header`): `model=<slug>` or
/// `model=<slug>;tier=<tier>` when the request names a service tier.
pub fn routing_hint(model: &str, service_tier: Option<&str>) -> String {
    match service_tier {
        Some(tier) if !tier.is_empty() => format!("model={model};tier={tier}"),
        _ => format!("model={model}"),
    }
}

/// A deterministic, UUID-SHAPED (`8-4-4-4-12` hex) synthetic id derived from `(namespace, key)` —
/// never random, so the same conversation always yields the same id. Not an RFC 4122-compliant
/// UUID (no version/variant bit fixup) — just structurally shaped like one, which is all the
/// content-safe fingerprint capture's `describe_id_format` (`polyflare-server/src/
/// fingerprint_capture.rs`) checks for.
fn deterministic_uuid_shaped(namespace: &str, key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(namespace.as_bytes());
    hasher.update(b":");
    hasher.update(key.as_bytes());
    let d = hasher.finalize();
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-\
         {:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        d[0],
        d[1],
        d[2],
        d[3],
        d[4],
        d[5],
        d[6],
        d[7],
        d[8],
        d[9],
        d[10],
        d[11],
        d[12],
        d[13],
        d[14],
        d[15]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversation_key_prefers_prompt_cache_key() {
        let body = serde_json::json!({"model": "m", "prompt_cache_key": "conv-1"});
        assert_eq!(conversation_key(&body), "conv-1");
    }

    #[test]
    fn conversation_key_falls_back_to_model_when_no_prompt_cache_key() {
        let body = serde_json::json!({"model": "gpt-5.6-sol"});
        assert_eq!(conversation_key(&body), "gpt-5.6-sol");
    }

    #[test]
    fn conversation_key_never_uses_previous_response_id() {
        // previous_response_id changes every turn; using it would make the derived ids
        // themselves a fingerprint tell (see module doc). Two bodies differing ONLY in
        // previous_response_id but sharing the same prompt_cache_key must yield the SAME key.
        let turn1 = serde_json::json!({
            "model": "m", "prompt_cache_key": "conv-1", "previous_response_id": "resp_1"
        });
        let turn2 = serde_json::json!({
            "model": "m", "prompt_cache_key": "conv-1", "previous_response_id": "resp_2"
        });
        assert_eq!(conversation_key(&turn1), conversation_key(&turn2));
    }

    #[test]
    fn turn_identity_is_deterministic_for_the_same_conversation_key() {
        let a = TurnIdentity::derive("conv-1");
        let b = TurnIdentity::derive("conv-1");
        assert_eq!(a.installation_id, b.installation_id);
        assert_eq!(a.session_id, b.session_id);
        assert_eq!(a.thread_id, b.thread_id);
        assert_eq!(a.turn_id, b.turn_id);
        assert_eq!(a.window_id, b.window_id);
    }

    #[test]
    fn turn_identity_differs_across_conversation_keys() {
        let a = TurnIdentity::derive("conv-1");
        let b = TurnIdentity::derive("conv-2");
        assert_ne!(a.session_id, b.session_id);
        assert_ne!(a.thread_id, b.thread_id);
    }

    #[test]
    fn window_id_follows_thread_id_colon_n_format() {
        let identity = TurnIdentity::derive("conv-1");
        assert_eq!(identity.window_id, format!("{}:1", identity.thread_id));
    }

    #[test]
    fn ids_are_uuid_shaped() {
        let identity = TurnIdentity::derive("conv-1");
        for id in [
            &identity.installation_id,
            &identity.session_id,
            &identity.thread_id,
            &identity.turn_id,
        ] {
            assert_eq!(id.len(), 36, "not uuid-shaped: {id}");
            let bytes = id.as_bytes();
            for (i, &b) in bytes.iter().enumerate() {
                if matches!(i, 8 | 13 | 18 | 23) {
                    assert_eq!(b, b'-', "expected hyphen at {i} in {id}");
                } else {
                    assert!(b.is_ascii_hexdigit(), "expected hex at {i} in {id}");
                }
            }
        }
    }

    #[test]
    fn turn_metadata_json_has_the_expected_field_set() {
        let identity = TurnIdentity::derive("conv-1");
        let value: serde_json::Value =
            serde_json::from_str(&identity.turn_metadata_json()).unwrap();
        let obj = value.as_object().unwrap();
        for key in [
            "installation_id",
            "session_id",
            "thread_id",
            "agent_name",
            "turn_id",
            "window_id",
            "request_kind",
            "thread_source",
            "sandbox",
            "sandbox_mode",
            "auto_review_enabled",
            "node_repl_auto_review_required",
            "node_repl_disabled",
            "turn_started_at_unix_ms",
        ] {
            assert!(obj.contains_key(key), "missing turn-metadata key `{key}`");
        }
        for absent in [
            "workspaces",
            "turn_trigger",
            "window_number",
            "context_window_id",
        ] {
            assert!(
                !obj.contains_key(absent),
                "`{absent}` must not appear on an ordinary interactive turn"
            );
        }
    }

    /// Values pinned to codex-rs rust-v0.153.4 (see the const docs).
    #[test]
    fn turn_metadata_values_match_codex_rs_0_153_4() {
        let identity = TurnIdentity::derive("conv-1");
        let value: serde_json::Value =
            serde_json::from_str(&identity.turn_metadata_json()).unwrap();
        assert_eq!(value["agent_name"], "/root");
        assert_eq!(
            value["thread_source"], "user",
            "ThreadSource::User, not SessionSource cli"
        );
        assert_eq!(value["request_kind"], "turn");
        assert_eq!(value["sandbox_mode"], "workspace-write");
        assert_eq!(value["sandbox"], platform_sandbox_tag());
        assert!(matches!(
            value["sandbox"].as_str(),
            Some("seatbelt" | "seccomp" | "none")
        ));
        assert_eq!(value["auto_review_enabled"], false);
        assert_eq!(value["node_repl_auto_review_required"], false);
        assert_eq!(value["node_repl_disabled"], false);
        let astra = identity.turn_metadata_json_for(&ModelTurnFlags {
            node_repl_auto_review_required: true,
            ..ModelTurnFlags::default()
        });
        let astra: serde_json::Value = serde_json::from_str(&astra).unwrap();
        assert_eq!(astra["node_repl_auto_review_required"], true);
    }

    #[test]
    fn body_defaults_match_codex_rs_0_153_4_for_a_lite_flagship() {
        let identity = TurnIdentity::derive("conv-1");
        let flags = ModelTurnFlags {
            use_responses_lite: true,
            supports_parallel_tool_calls: true,
            support_verbosity: true,
            default_verbosity: Some("low".into()),
            default_reasoning_level: Some("low".into()),
            default_reasoning_summary: Some("none".into()),
            ..ModelTurnFlags::default()
        };
        let meta = identity.turn_metadata_json_for(&flags);
        let mut body = serde_json::json!({
            "model": "gpt-5.6-sol", "input": [], "stream": true, "store": false,
            "prompt_cache_key": "client-supplied-key"
        });
        apply_codex_body_defaults(&mut body, &identity, &flags, &meta);
        assert_eq!(
            body["prompt_cache_key"], identity.session_id,
            "cache key IS the session id"
        );
        assert_eq!(
            body["include"],
            serde_json::json!(["reasoning.encrypted_content"])
        );
        assert_eq!(body["tool_choice"], "auto");
        assert_eq!(
            body["parallel_tool_calls"], false,
            "lite models never parallelize"
        );
        assert_eq!(body["text"]["verbosity"], "low");
        assert_eq!(body["reasoning"]["effort"], "low");
        assert_eq!(body["reasoning"]["context"], "all_turns");
        assert!(
            body["reasoning"].get("summary").is_none(),
            "summary omitted when default is none"
        );
        let cm = &body["client_metadata"];
        assert_eq!(cm["session_id"], identity.session_id);
        assert_eq!(cm["thread_id"], identity.thread_id);
        assert_eq!(cm["x-codex-window-id"], identity.window_id);
        assert_eq!(cm["x-codex-installation-id"], identity.installation_id);
        assert_eq!(
            cm["x-codex-turn-metadata"], meta,
            "body metadata equals the header"
        );
        assert!(body.get("max_output_tokens").is_none());
    }

    #[test]
    fn body_defaults_keep_what_the_client_set_and_respect_non_lite_models() {
        let identity = TurnIdentity::derive("conv-2");
        let flags = ModelTurnFlags {
            use_responses_lite: false,
            supports_parallel_tool_calls: true,
            support_verbosity: false,
            default_reasoning_level: Some("medium".into()),
            ..ModelTurnFlags::default()
        };
        let meta = identity.turn_metadata_json_for(&flags);
        let mut body = serde_json::json!({
            "model": "m", "input": [], "tool_choice": "none",
            "reasoning": {"effort": "high"}
        });
        apply_codex_body_defaults(&mut body, &identity, &flags, &meta);
        assert_eq!(body["tool_choice"], "none", "client choice preserved");
        assert_eq!(
            body["reasoning"]["effort"], "high",
            "client effort preserved"
        );
        assert!(
            body["reasoning"].get("context").is_none(),
            "no context off lite"
        );
        assert_eq!(body["parallel_tool_calls"], true);
        assert!(
            body.get("text").is_none(),
            "no text block when verbosity unsupported"
        );
    }

    #[test]
    fn routing_hint_matches_build_routing_hint_header() {
        assert_eq!(routing_hint("gpt-5.6-sol", None), "model=gpt-5.6-sol");
        assert_eq!(routing_hint("gpt-5.6-sol", Some("")), "model=gpt-5.6-sol");
        assert_eq!(
            routing_hint("gpt-6-astra", Some("priority")),
            "model=gpt-6-astra;tier=priority"
        );
    }

    #[test]
    fn turn_metadata_json_is_compact_not_pretty() {
        let identity = TurnIdentity::derive("conv-1");
        let json = identity.turn_metadata_json();
        assert!(
            !json.contains('\n'),
            "turn-metadata JSON must be compact: {json}"
        );
        assert!(!json.contains(": {") || json.contains("\"workspaces\":{}"));
    }

    #[test]
    fn codex_user_agent_matches_captured_codex_rs_shape() {
        let ua = codex_user_agent(CODEX_CLI_VERSION);
        // Capture-verified prefix shape: `codex_cli_rs/<ver> (` (byte-captured at 0.144.4; floor now 0.145.0).
        assert!(
            ua.starts_with(&format!("{ORIGINATOR}/{CODEX_CLI_VERSION} (")),
            "unexpected UA prefix: {ua}"
        );
        // The UA ends with the fixed terminal token (headless-codex `unknown`), never a leaked
        // server `TERM_PROGRAM`.
        assert!(
            ua.ends_with(&format!(" {TERMINAL_TOKEN}")),
            "UA should end with the pinned terminal token: {ua}"
        );
    }
}
