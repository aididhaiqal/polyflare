//! Synthesizes the HTTP header STRUCTURE a real `codex-rs` sends on `POST /responses` — the
//! egress-parity half of the fingerprint-parity gate (see `executor.rs` and
//! `polyflare-server/tests/codex_fingerprint_parity_gate.rs`).
//!
//! # Status: CAPTURE-VERIFIED (codex-cli 0.144.4, 2026-07-15); floor now 0.145.0 (source-verified)
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
pub const CODEX_CLI_VERSION: &str = "0.145.0";

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
    pub fn turn_metadata_json(&self) -> String {
        let turn_started_at_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        serde_json::json!({
            "installation_id": self.installation_id,
            "session_id": self.session_id,
            "thread_id": self.thread_id,
            "turn_id": self.turn_id,
            "window_id": self.window_id,
            "request_kind": "turn",
            "sandbox": "workspace-write",
            "thread_source": "cli",
            "workspaces": {},
            "turn_started_at_unix_ms": turn_started_at_unix_ms,
        })
        .to_string()
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
            "turn_id",
            "window_id",
            "request_kind",
            "sandbox",
            "thread_source",
            "workspaces",
            "turn_started_at_unix_ms",
        ] {
            assert!(obj.contains_key(key), "missing turn-metadata key `{key}`");
        }
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
