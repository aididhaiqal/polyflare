//! Content-free conversation identity for the WS-downstream handshake.
//!
//! Phase-0 capture (2026-07-20) confirmed the codex CLI's WS-handshake `GET /responses` carries the
//! conversation identity ENTIRELY in the handshake headers — `session-id`, `thread-id`, and
//! `x-codex-window-id` are all present (`x-codex-turn-state` is server-issued and absent at
//! handshake). Those three headers UNIQUELY and STABLY identify a codex conversation, so the owner
//! lookup key is derivable from the handshake alone — no frame read (and thus no content) required.

use axum::http::HeaderMap;
use polyflare_core::{KeyStrength, SessionKey};

use crate::session_key::sha256_hex;

/// Read a header value as a borrowed `&str`, or `""` when the header is absent or non-UTF-8.
/// `HeaderMap::get` is case-insensitive over `HeaderName`, so the lowercase literal matches the
/// CLI's header regardless of the wire casing. Degrades to empty rather than panicking.
fn header_or_empty<'a>(headers: &'a HeaderMap, name: &str) -> &'a str {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
}

/// Derive the content-free, stable owner-lookup `SessionKey` for a WS-downstream conversation from
/// its handshake headers.
///
/// The basis combines the three Phase-0 identity headers — `session-id`, `thread-id`,
/// `x-codex-window-id` — into a single string that is then sha256-hashed. Properties:
/// - **Content-free:** the returned [`SessionKey::value`] is a 64-char sha256 hex; the raw ids are
///   never stored, returned, or logged (PolyFlare's inviolable no-content limit).
/// - **Stable per conversation:** the SAME `(session-id, thread-id, window-id)` triple always hashes
///   to the SAME key across turns and reconnects, so every turn of one conversation resolves the
///   same owner. Different conversations (any differing id) yield a different key.
/// - **Hard strength:** these headers are a real, server-durable session identity — a hard pin, not a
///   soft best-effort fallback.
///
/// A missing header degrades to an empty component (never panics); the key stays stable for whatever
/// identity the handshake did present.
pub(crate) fn ws_session_key(headers: &HeaderMap) -> SessionKey {
    let session = header_or_empty(headers, "session-id");
    let thread = header_or_empty(headers, "thread-id");
    let window = header_or_empty(headers, "x-codex-window-id");
    let basis = format!("wsds:{session}:{thread}:{window}");
    SessionKey {
        value: sha256_hex(basis.as_bytes()),
        strength: KeyStrength::Hard,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderName, HeaderValue};

    fn hdr(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    #[test]
    fn ws_session_key_is_stable_and_content_free() {
        let h = hdr(&[
            ("session-id", "s1"),
            ("thread-id", "t1"),
            ("x-codex-window-id", "w1:0"),
        ]);
        let k1 = ws_session_key(&h);
        let k2 = ws_session_key(&h);
        // Stable: the same handshake headers always yield the same key.
        assert_eq!(k1.value, k2.value, "same conversation must be stable");
        // Content-free: value is a 64-char lowercase sha256 hex, carrying no raw id substring.
        assert_eq!(k1.value.len(), 64, "sha256 hex is 64 chars");
        assert!(
            k1.value.chars().all(|c| c.is_ascii_hexdigit()),
            "value must be pure hex"
        );
        for raw in ["s1", "t1", "w1:0"] {
            assert!(
                !k1.value.contains(raw),
                "hashed key must not leak the raw id {raw}"
            );
        }
        // Hard: a real session identity, not a soft fallback.
        assert_eq!(k1.strength, KeyStrength::Hard);
    }

    #[test]
    fn ws_session_key_differs_per_conversation() {
        let a = hdr(&[
            ("session-id", "s1"),
            ("thread-id", "t1"),
            ("x-codex-window-id", "w1:0"),
        ]);
        let b = hdr(&[
            ("session-id", "s1"),
            ("thread-id", "t2"), // different thread => different conversation
            ("x-codex-window-id", "w1:0"),
        ]);
        assert_ne!(
            ws_session_key(&a).value,
            ws_session_key(&b).value,
            "a different thread-id must produce a different key"
        );
    }

    #[test]
    fn ws_session_key_degrades_on_missing_header() {
        // One of the three identity headers is absent: must NOT panic and must stay stable.
        let h = hdr(&[("session-id", "s1"), ("thread-id", "t1")]);
        let k1 = ws_session_key(&h);
        let k2 = ws_session_key(&h);
        assert_eq!(k1.value.len(), 64);
        assert_eq!(k1.value, k2.value, "degraded key is still stable");
        assert_eq!(k1.strength, KeyStrength::Hard);
    }
}
