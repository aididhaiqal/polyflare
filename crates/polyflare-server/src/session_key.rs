//! Ingress-time derivation of the continuity RequestCtx from headers + body: the session key + its
//! strength, whether the input is a full-resend, and any client-supplied previous_response_id.
//!
//! VERIFY-at-implementation (SPEC-M3 risk 4): the exact Codex CLI header names
//! (`x-codex-turn-state`, session / `prompt_cache_key`) must be re-verified against the live CLI —
//! a wrong key silently weakens ownership. The rules below mirror codex-lb `helpers.py:988-1064`
//! (session key) and `helpers.py:849-861` (full-resend heuristic).

use axum::http::HeaderMap;
use polyflare_core::{KeyStrength, RequestCtx, SessionKey};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Lowercase hex sha256 of `bytes`. Used for stable, content-free session keys + input fingerprints.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

fn header_str(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// Multi-item input array, or a string ≥ 4096 code points, or a single item serializing to ≥ 4096
/// code points. Mirrors codex-lb `helpers.py:849-861`, whose Python `len(string)` measures Unicode
/// code points — so we count `chars().count()`, NOT UTF-8 bytes (`str::len`), for boundary fidelity.
///
/// Fidelity note: codex-lb's single-item branch measures `json.dumps(..., ensure_ascii=True)`, which
/// escapes non-ASCII to `\uXXXX` (~6 chars/char). PolyFlare serializes with raw (non-escaped) UTF-8
/// and counts code points, so it intentionally UNDER-counts heavily-non-ASCII single items — an
/// accepted approximation for this heuristic on the ASCII-dominant path.
fn is_full_resend(input: Option<&Value>) -> bool {
    match input {
        Some(Value::String(s)) => s.chars().count() >= 4096,
        Some(Value::Array(items)) => {
            if items.len() >= 2 {
                true
            } else if items.len() == 1 {
                serde_json::to_string(&items[0])
                    .map(|s| s.chars().count() >= 4096)
                    .unwrap_or(false)
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Derive the session key: `x-codex-turn-state` ⇒ Hard; else a session header (+ `prompt_cache_key`
/// isolating threads) ⇒ Hard; else a soft key from `x-request-id` / `prompt_cache_key` / content
/// hash. Values are hashed so no raw header/content is stored.
fn derive_session_key(headers: &HeaderMap, body: &Value) -> SessionKey {
    if let Some(ts) = header_str(headers, "x-codex-turn-state") {
        return SessionKey {
            value: sha256_hex(format!("turn:{ts}").as_bytes()),
            strength: KeyStrength::Hard,
        };
    }
    if let Some(sess) =
        header_str(headers, "session_id").or_else(|| header_str(headers, "x-session-id"))
    {
        let mut raw = sess;
        if let Some(pck) = body.get("prompt_cache_key").and_then(|v| v.as_str()) {
            raw = format!("{raw}:{pck}");
        }
        return SessionKey {
            value: sha256_hex(format!("session:{raw}").as_bytes()),
            strength: KeyStrength::Hard,
        };
    }
    let soft = header_str(headers, "x-request-id")
        .or_else(|| {
            body.get("prompt_cache_key")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| body.get("input").map(|i| i.to_string()).unwrap_or_default());
    SessionKey {
        value: sha256_hex(format!("soft:{soft}").as_bytes()),
        strength: KeyStrength::Soft,
    }
}

/// Build the continuity `RequestCtx` from headers + body BEFORE `prepare`.
pub fn derive_request_ctx(headers: &HeaderMap, body: &Value) -> RequestCtx {
    let session_key = derive_session_key(headers, body);
    let client_previous_response_id = body
        .get("previous_response_id")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let is_full_resend = is_full_resend(body.get("input"));
    let session_id =
        header_str(headers, "session_id").or_else(|| header_str(headers, "x-session-id"));
    RequestCtx {
        session_id,
        session_key: Some(session_key),
        client_previous_response_id,
        is_full_resend,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hdr(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        h
    }

    #[test]
    fn turn_state_header_yields_hard_key() {
        let ctx = derive_request_ctx(
            &hdr(&[("x-codex-turn-state", "ts-abc")]),
            &serde_json::json!({}),
        );
        let sk = ctx.session_key.unwrap();
        assert_eq!(sk.strength, KeyStrength::Hard);
    }

    #[test]
    fn session_header_yields_hard_key() {
        let ctx = derive_request_ctx(&hdr(&[("session_id", "sess-1")]), &serde_json::json!({}));
        assert_eq!(ctx.session_key.unwrap().strength, KeyStrength::Hard);
    }

    #[test]
    fn no_session_headers_yields_soft_key() {
        let ctx = derive_request_ctx(&hdr(&[]), &serde_json::json!({"input": "hi"}));
        assert_eq!(ctx.session_key.unwrap().strength, KeyStrength::Soft);
    }

    #[test]
    fn multi_item_input_is_full_resend() {
        let ctx = derive_request_ctx(
            &hdr(&[]),
            &serde_json::json!({"input": [{"a": 1}, {"b": 2}]}),
        );
        assert!(ctx.is_full_resend);
    }

    #[test]
    fn single_small_item_is_not_full_resend() {
        let ctx = derive_request_ctx(&hdr(&[]), &serde_json::json!({"input": [{"role": "user"}]}));
        assert!(!ctx.is_full_resend);
    }

    #[test]
    fn long_string_input_is_full_resend() {
        let big = "x".repeat(4096);
        let ctx = derive_request_ctx(&hdr(&[]), &serde_json::json!({"input": big}));
        assert!(ctx.is_full_resend);
    }

    #[test]
    fn multibyte_string_uses_code_point_count_not_bytes() {
        // 4095 two-byte chars: 4095 code points (< 4096 ⇒ NOT a full resend) but 8190 UTF-8 bytes
        // (≥ 4096 ⇒ would be a full resend if we counted bytes). Asserting NOT full-resend proves
        // the check counts code points, matching codex-lb's `len(string)`.
        let s = "é".repeat(4095);
        assert_eq!(s.chars().count(), 4095);
        assert!(s.len() >= 4096);
        let ctx = derive_request_ctx(&hdr(&[]), &serde_json::json!({ "input": s }));
        assert!(!ctx.is_full_resend);
    }

    #[test]
    fn previous_response_id_is_extracted() {
        let ctx = derive_request_ctx(
            &hdr(&[]),
            &serde_json::json!({"previous_response_id": "resp_9", "input": "hi"}),
        );
        assert_eq!(ctx.client_previous_response_id.as_deref(), Some("resp_9"));
    }

    #[test]
    fn absent_previous_response_id_yields_none() {
        let ctx = derive_request_ctx(&hdr(&[]), &serde_json::json!({"input": "hi"}));
        assert_eq!(ctx.client_previous_response_id, None);
    }
}
