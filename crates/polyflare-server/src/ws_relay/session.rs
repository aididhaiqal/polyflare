//! Content-free conversation identity for the WS-downstream handshake.
//!
//! Current Codex carries `session-id`, `thread-id`, and `x-codex-window-id` on the handshake. Owner
//! identity is the same session + thread + pool tuple used by HTTP and compact. Window remains a
//! socket discriminator only because Codex advances it after compaction.

use axum::http::HeaderMap;
use polyflare_core::{KeyStrength, SessionKey};

use crate::session_key::header_session_key_scoped;

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
pub(crate) fn ws_session_key(headers: &HeaderMap, pool: Option<&str>) -> SessionKey {
    header_session_key_scoped(headers, None, pool).unwrap_or_else(|| {
        // A WS without Codex identity must never share one global hard owner row with every other
        // anonymous socket. Generate an isolated soft key for this connection instead.
        SessionKey {
            value: crate::session_key::sha256_hex(
                format!("anonymous-ws:{:032x}", rand::random::<u128>()).as_bytes(),
            ),
            strength: KeyStrength::Soft,
        }
    })
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
        let k1 = ws_session_key(&h, None);
        let k2 = ws_session_key(&h, None);
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
            ws_session_key(&a, None).value,
            ws_session_key(&b, None).value,
            "a different thread-id must produce a different key"
        );
    }

    #[test]
    fn ws_session_key_stays_stable_when_window_header_is_missing() {
        // Window is not part of durable owner identity.
        let h = hdr(&[("session-id", "s1"), ("thread-id", "t1")]);
        let k1 = ws_session_key(&h, None);
        let k2 = ws_session_key(&h, None);
        assert_eq!(k1.value.len(), 64);
        assert_eq!(k1.value, k2.value, "conversation key is still stable");
        assert_eq!(k1.strength, KeyStrength::Hard);
    }

    #[test]
    fn ws_and_http_use_the_same_pool_scoped_conversation_key() {
        let h = hdr(&[
            ("session-id", "s1"),
            ("thread-id", "t1"),
            ("x-codex-window-id", "w1:0"),
        ]);
        let ws = ws_session_key(&h, Some("premium"));
        let http = crate::session_key::header_session_key_scoped(&h, None, Some("premium"))
            .expect("Codex identity");
        assert_eq!(ws, http);
        assert_ne!(ws, ws_session_key(&h, Some("standard")));
    }

    #[test]
    fn anonymous_ws_connections_do_not_share_a_global_owner_key() {
        let h = HeaderMap::new();
        let a = ws_session_key(&h, None);
        let b = ws_session_key(&h, None);
        assert_eq!(a.strength, KeyStrength::Soft);
        assert_eq!(b.strength, KeyStrength::Soft);
        assert_ne!(a, b);
    }
}
