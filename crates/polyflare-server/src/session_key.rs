//! Ingress-time parse of a native Codex `/responses` request into the owned facts the request path
//! needs — the model, the tier hint, and the continuity `RequestCtx` (session key + strength,
//! full-resend flag, client `previous_response_id`, input count).
//!
//! The deep `input` array is NOT materialized: the body is read as a top-level map of borrowed
//! `&RawValue` fields, and we derive the `input` shape (count + full-resend) by parsing only the
//! array SPINE into raw elements. This map view also makes the parse a faithful PASS-THROUGH — it
//! tolerates duplicate keys (last-wins), `null` fields, and type-drifted / unknown advisory fields
//! exactly as the old `serde_json::Value` parse did, deferring schema validation to upstream. Only a
//! malformed body (invalid JSON, or a non-object root — which is never a real request) 400s. The
//! borrows live entirely inside [`parse_inbound`], which returns owned data, so nothing crosses an
//! await and the caller keeps forwarding the original wire bytes verbatim.
//!
//! VERIFY-at-implementation (SPEC-M3 risk 4): the exact Codex CLI header names
//! (`x-codex-turn-state`, session / `prompt_cache_key`) must be re-verified against the live CLI —
//! a wrong key silently weakens ownership. The rules below mirror codex-lb `helpers.py:988-1064`
//! (session key) and `helpers.py:849-861` (full-resend heuristic).

use std::collections::HashMap;

use axum::http::HeaderMap;
use polyflare_core::{KeyStrength, RequestCtx, SessionKey};
use serde_json::value::RawValue;
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

/// Decode a raw field as a string ONLY if it is a JSON string; any other type (or absent) yields
/// `None` — the lenient equivalent of the old `Value::get(..).and_then(Value::as_str)`.
fn raw_as_str(rv: Option<&RawValue>) -> Option<String> {
    rv.and_then(|r| serde_json::from_str::<String>(r.get()).ok())
}

/// Best-effort `reasoning.effort` string, tolerating any `reasoning` shape (the old graceful path:
/// a non-object / renamed / mistyped `reasoning` simply yields no tier hint rather than an error).
/// `reasoning` is a tiny object, so parsing just it to `Value` is negligible next to the `input`
/// tree we avoid.
fn effort_from_reasoning(rv: Option<&RawValue>) -> Option<String> {
    let v: Value = serde_json::from_str(rv?.get()).ok()?;
    v.get("effort").and_then(|e| e.as_str()).map(str::to_string)
}

/// The owned facts ingress needs from a native `/responses` body. `input` is fully consumed here;
/// nothing borrowed escapes.
pub struct InboundFacts {
    pub model: String,
    /// `reasoning.effort` (for the routing tier); the caller maps it to a `Tier`.
    pub effort: Option<String>,
    pub ctx: RequestCtx,
}

/// Derive `(input_count, is_full_resend)` from the raw `input` value WITHOUT materializing its deep
/// tree. Fidelity with the previous `serde_json::Value` implementation for every real input shape:
/// - array: count = element count (parsed spine only, elements stay raw); full-resend iff ≥ 2 items
///   OR a single item that CANONICALIZES to ≥ 4096 code points;
/// - string: count = 1; full-resend iff the DECODED string is ≥ 4096 code points (matches codex-lb's
///   `len(string)`, which measures Unicode code points, not UTF-8 bytes — so we decode and count
///   `chars()`, never the quoted/escaped raw text);
/// - any other present value (object/number/bool/`null`): count = 1, never a full-resend (a `null`
///   field is captured as raw `"null"` by the map view, so it counts 1 like the old `Value::Null`);
/// - absent: count = 0, never a full-resend.
///
/// Fidelity note (unchanged from the prior impl): the single-item branch canonicalizes via `Value`
/// and counts code points; codex-lb's `json.dumps(ensure_ascii=True)` escapes non-ASCII to `\uXXXX`,
/// so PolyFlare intentionally UNDER-counts heavily-non-ASCII single items — an accepted
/// approximation on the ASCII-dominant path.
fn input_shape(input: Option<&RawValue>) -> (u32, bool) {
    let Some(rv) = input else {
        return (0, false);
    };
    let txt = rv.get();
    match txt.trim_start().as_bytes().first() {
        Some(b'[') => match serde_json::from_str::<Vec<&RawValue>>(txt) {
            Ok(items) => {
                let count = items.len() as u32;
                let full = if items.len() >= 2 {
                    true
                } else if items.len() == 1 {
                    // Canonicalize the single element exactly as the old `to_string(&Value)` path did.
                    serde_json::from_str::<Value>(items[0].get())
                        .ok()
                        .and_then(|v| serde_json::to_string(&v).ok())
                        .map(|s| s.chars().count() >= 4096)
                        .unwrap_or(false)
                } else {
                    false
                };
                (count, full)
            }
            // The body already parsed as a valid JSON object, so a captured array value re-parses
            // fine; treat any unexpected spine-parse failure as a present, non-resend single item.
            Err(_) => (1, false),
        },
        Some(b'"') => {
            let full = serde_json::from_str::<String>(txt)
                .map(|s| s.chars().count() >= 4096)
                .unwrap_or(false);
            (1, full)
        }
        _ => (1, false),
    }
}

/// The HARD-strength half of session-key derivation: hashes `x-codex-turn-state` (with an optional
/// `prompt_cache_key` suffix isolating threads), else a session header (`session_id` /
/// `x-session-id`, same suffix rule). Returns `None` when NEITHER header is present — deliberately
/// does NOT fall through to the soft (`x-request-id` / content-hash) derivation, because that
/// fallback exists to give every native `/responses` turn *some* stable key even absent a real
/// session header; control requests have no such requirement (D17 plan, Global Constraints: "SOFT
/// affinity ... No session header ⇒ select ANY eligible account" — the ABSENCE of a session header
/// is exactly the fallback trigger, so a manufactured soft key here would spuriously report
/// "session present" for a request that carries none).
///
/// Byte-identical hashing to [`derive_session_key`]'s two Hard branches — [`derive_session_key`] is
/// implemented in terms of this fn plus its own soft fallback, so a `/responses` turn and a control
/// request presenting the SAME `x-codex-turn-state`/`session_id` header always hash to the same
/// [`SessionKey::value`] and therefore resolve the same continuity-session row.
pub fn header_session_key(
    headers: &HeaderMap,
    prompt_cache_key: Option<&str>,
) -> Option<SessionKey> {
    if let Some(ts) = header_str(headers, "x-codex-turn-state") {
        return Some(SessionKey {
            value: sha256_hex(format!("turn:{ts}").as_bytes()),
            strength: KeyStrength::Hard,
        });
    }
    if let Some(sess) =
        header_str(headers, "session_id").or_else(|| header_str(headers, "x-session-id"))
    {
        let mut raw = sess;
        if let Some(pck) = prompt_cache_key {
            raw = format!("{raw}:{pck}");
        }
        return Some(SessionKey {
            value: sha256_hex(format!("session:{raw}").as_bytes()),
            strength: KeyStrength::Hard,
        });
    }
    None
}

/// Derive the session key: `x-codex-turn-state` ⇒ Hard; else a session header (+ `prompt_cache_key`
/// isolating threads) ⇒ Hard; else a soft key from `x-request-id` / `prompt_cache_key` / a content
/// hash of the raw `input`. Values are hashed so no raw header/content is stored.
fn derive_session_key(
    headers: &HeaderMap,
    prompt_cache_key: Option<&str>,
    input: Option<&RawValue>,
) -> SessionKey {
    if let Some(hard) = header_session_key(headers, prompt_cache_key) {
        return hard;
    }
    // Soft fallback: `x-request-id`, else `prompt_cache_key`, else a hash of the raw `input` text.
    // (The last-ditch content hash uses the raw JSON slice rather than a canonicalized re-serialize
    // — this path only fires when the request carries NO session identity at all, so the basis just
    // needs to be stable per identical request, which the raw bytes are.)
    let soft = header_str(headers, "x-request-id")
        .or_else(|| prompt_cache_key.map(str::to_string))
        .unwrap_or_else(|| input.map(|i| i.get().to_string()).unwrap_or_default());
    SessionKey {
        value: sha256_hex(format!("soft:{soft}").as_bytes()),
        strength: KeyStrength::Soft,
    }
}

/// Parse a native `/responses` body (raw bytes) into the owned facts ingress needs. Returns `None`
/// only when the body is malformed — invalid JSON or a non-object root (the caller 400s). The
/// `input` tree is never materialized.
pub fn parse_inbound(headers: &HeaderMap, raw: &[u8]) -> Option<InboundFacts> {
    // Read the top-level object as a map of raw fields — ONE shallow scan (values stay raw, so the
    // deep `input` tree is never built and the body is never re-captured whole). A `HashMap` gives
    // last-wins on duplicate keys and captures a `null` value as raw `"null"`, both matching the old
    // `Value` map exactly (a derived struct would instead reject duplicate keys and collapse a `null`
    // field to absent). Type-drifted / unknown / out-of-range fields are all tolerated; only a
    // malformed body — invalid JSON, or a non-object root (never a real request) — fails and 400s.
    //
    // Keys are OWNED `String` (values stay borrowed `&RawValue` — that is where the deep-input win
    // lives). Do NOT narrow the key to `&str`: serde can't borrow an object name that needs
    // unescaping, so a `&str` key would spuriously 400 any body with an escaped top-level key that
    // the old `Value` (owned-`String` keys) accepted. Owning ~10 tiny ASCII keys costs nothing.
    let fields: HashMap<String, &RawValue> = serde_json::from_slice(raw).ok()?;
    let field = |k: &str| fields.get(k).copied();

    let (input_count, is_full_resend) = input_shape(field("input"));
    let prompt_cache_key = raw_as_str(field("prompt_cache_key"));
    let session_key = derive_session_key(headers, prompt_cache_key.as_deref(), field("input"));
    let session_id =
        header_str(headers, "session_id").or_else(|| header_str(headers, "x-session-id"));
    let ctx = RequestCtx {
        session_id,
        session_key: Some(session_key),
        client_previous_response_id: raw_as_str(field("previous_response_id")),
        is_full_resend,
        input_count,
    };
    Some(InboundFacts {
        model: raw_as_str(field("model")).unwrap_or_default(),
        effort: effort_from_reasoning(field("reasoning")),
        ctx,
    })
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

    /// Drive the REAL ingress parse: serialize a JSON body to bytes and run `parse_inbound`, so the
    /// tests exercise the raw-`input` path that production uses (not a Value shortcut).
    fn ctx_of(headers: &HeaderMap, body: serde_json::Value) -> RequestCtx {
        parse_inbound(headers, &serde_json::to_vec(&body).unwrap())
            .expect("valid body")
            .ctx
    }

    #[test]
    fn turn_state_header_yields_hard_key() {
        let ctx = ctx_of(
            &hdr(&[("x-codex-turn-state", "ts-abc")]),
            serde_json::json!({}),
        );
        assert_eq!(ctx.session_key.unwrap().strength, KeyStrength::Hard);
    }

    #[test]
    fn session_header_yields_hard_key() {
        let ctx = ctx_of(&hdr(&[("session_id", "sess-1")]), serde_json::json!({}));
        assert_eq!(ctx.session_key.unwrap().strength, KeyStrength::Hard);
    }

    #[test]
    fn no_session_headers_yields_soft_key() {
        let ctx = ctx_of(&hdr(&[]), serde_json::json!({"input": "hi"}));
        assert_eq!(ctx.session_key.unwrap().strength, KeyStrength::Soft);
    }

    #[test]
    fn multi_item_input_is_full_resend() {
        let ctx = ctx_of(
            &hdr(&[]),
            serde_json::json!({"input": [{"a": 1}, {"b": 2}]}),
        );
        assert!(ctx.is_full_resend);
        assert_eq!(ctx.input_count, 2, "array length threads to input_count");
    }

    #[test]
    fn single_small_item_is_not_full_resend() {
        let ctx = ctx_of(&hdr(&[]), serde_json::json!({"input": [{"role": "user"}]}));
        assert!(!ctx.is_full_resend);
        assert_eq!(ctx.input_count, 1);
    }

    #[test]
    fn single_huge_item_is_full_resend() {
        // A one-item array whose sole element canonicalizes to ≥ 4096 code points IS a full resend —
        // exercises the single-item canonicalization branch of `input_shape`.
        let big = "x".repeat(5000);
        let ctx = ctx_of(&hdr(&[]), serde_json::json!({"input": [{"text": big}]}));
        assert!(ctx.is_full_resend);
        assert_eq!(ctx.input_count, 1);
    }

    #[test]
    fn long_string_input_is_full_resend() {
        let big = "x".repeat(4096);
        let ctx = ctx_of(&hdr(&[]), serde_json::json!({"input": big}));
        assert!(ctx.is_full_resend);
        assert_eq!(ctx.input_count, 1, "a non-array present input counts as 1");
    }

    #[test]
    fn multibyte_string_uses_code_point_count_not_bytes() {
        // 4095 two-byte chars: 4095 code points (< 4096 ⇒ NOT a full resend) but 8190 UTF-8 bytes
        // (≥ 4096 ⇒ would be a full resend if we counted bytes). Asserting NOT full-resend proves
        // the check DECODES the JSON string and counts code points, matching codex-lb's `len(string)`
        // — and that we did NOT regress to counting the quoted/escaped raw text.
        let s = "é".repeat(4095);
        assert_eq!(s.chars().count(), 4095);
        assert!(s.len() >= 4096);
        let ctx = ctx_of(&hdr(&[]), serde_json::json!({ "input": s }));
        assert!(!ctx.is_full_resend);
    }

    #[test]
    fn absent_input_counts_zero() {
        let ctx = ctx_of(&hdr(&[]), serde_json::json!({"model": "gpt-5.6-sol"}));
        assert_eq!(ctx.input_count, 0);
        assert!(!ctx.is_full_resend);
    }

    #[test]
    fn previous_response_id_is_extracted() {
        let ctx = ctx_of(
            &hdr(&[]),
            serde_json::json!({"previous_response_id": "resp_9", "input": "hi"}),
        );
        assert_eq!(ctx.client_previous_response_id.as_deref(), Some("resp_9"));
    }

    #[test]
    fn absent_previous_response_id_yields_none() {
        let ctx = ctx_of(&hdr(&[]), serde_json::json!({"input": "hi"}));
        assert_eq!(ctx.client_previous_response_id, None);
    }

    #[test]
    fn model_and_effort_are_extracted() {
        let facts = parse_inbound(
            &hdr(&[]),
            &serde_json::to_vec(
                &serde_json::json!({"model": "gpt-5.6-sol", "reasoning": {"effort": "high"}}),
            )
            .unwrap(),
        )
        .expect("valid body");
        assert_eq!(facts.model, "gpt-5.6-sol");
        assert_eq!(facts.effort.as_deref(), Some("high"));
    }

    #[test]
    fn invalid_json_returns_none() {
        assert!(parse_inbound(&hdr(&[]), b"{not json").is_none());
    }

    #[test]
    fn type_drifted_known_fields_are_tolerated_not_rejected() {
        // The pass-through contract: a KNOWN field carrying an unexpected JSON type must NOT 400 the
        // request (the old `Value` + `.as_str()` parse degraded such fields to a default and
        // forwarded the bytes; upstream is the schema authority). A future `reasoning` shorthand
        // string, a numeric `model`, and a numeric id all parse OK and degrade gracefully.
        let facts = parse_inbound(
            &hdr(&[]),
            &serde_json::to_vec(&serde_json::json!({
                "model": 123,
                "reasoning": "high",
                "prompt_cache_key": 7,
                "previous_response_id": 42,
                "input": "hi"
            }))
            .unwrap(),
        )
        .expect("type-drifted-but-valid JSON must still parse");
        assert_eq!(
            facts.model, "",
            "non-string model degrades to empty, not a 400"
        );
        assert_eq!(
            facts.effort, None,
            "non-object reasoning yields no tier hint"
        );
        assert_eq!(
            facts.ctx.client_previous_response_id, None,
            "non-string previous_response_id degrades to None"
        );
    }

    #[test]
    fn out_of_range_number_is_forwarded_not_locally_rejected() {
        // A number too large for f64 is valid JSON per RFC 8259. The pass-through parse accepts it
        // (upstream decides), rather than the old full-`Value` parse's incidental "number out of
        // range" 400. Documents the intended, more-correct behavior.
        let facts = parse_inbound(
            &hdr(&[]),
            br#"{"model":"gpt-5.6-sol","input":"hi","junk":1e999}"#,
        );
        assert!(
            facts.is_some(),
            "out-of-range numeric literal must not 400 at the proxy"
        );
    }

    #[test]
    fn duplicate_top_level_key_is_accepted_last_wins() {
        // A duplicate object name is syntactically valid JSON; the old `Value` parse accepted it
        // (last-wins). The map view must too — never 400 a request over it. Last value ("x") wins,
        // so this is a single non-array input => count 1, not a full resend.
        let facts = parse_inbound(&hdr(&[]), br#"{"input":[1,2],"input":"x"}"#)
            .expect("duplicate key must not 400");
        assert_eq!(
            facts.ctx.input_count, 1,
            "last-wins: input is the string \"x\""
        );
        assert!(!facts.ctx.is_full_resend);
    }

    #[test]
    fn escaped_top_level_key_is_accepted() {
        // A top-level object name containing a JSON escape is valid JSON; the old `Value` (owned
        // String keys) accepted it, and the map view must too. This guards against narrowing the
        // map key back to a borrowed `&str`, which cannot represent an unescaped key and would 400.
        // The escaped key here is a stray field ingress never reads — the parse must still succeed.
        let facts = parse_inbound(
            &hdr(&[]),
            br#"{"model":"m","input":[{"a":1},{"b":2}],"x\ty":1}"#,
        )
        .expect("escaped top-level key must not 400");
        assert_eq!(facts.model, "m");
        assert_eq!(facts.ctx.input_count, 2);
        assert!(facts.ctx.is_full_resend);
    }

    #[test]
    fn non_object_root_is_rejected() {
        // A valid-JSON but non-object body is not a real `/responses` request; reject it locally
        // (a clean 400) rather than selecting an account and shipping garbage upstream.
        assert!(parse_inbound(&hdr(&[]), b"[1,2,3]").is_none());
        assert!(parse_inbound(&hdr(&[]), b"\"just a string\"").is_none());
    }

    #[test]
    fn present_null_input_counts_one_like_old_value_null() {
        // A present JSON `null` input is captured as raw "null" by the map view, so it counts 1 —
        // matching the old `Some(Value::Null) => 1`, not collapsing to absent/0.
        let ctx = ctx_of(&hdr(&[]), serde_json::json!({"model": "m", "input": null}));
        assert_eq!(ctx.input_count, 1);
        assert!(!ctx.is_full_resend);
    }

    #[test]
    fn header_session_key_is_none_with_no_session_header() {
        // No `x-codex-turn-state`/`session_id`/`x-session-id` ⇒ None, NOT a manufactured soft key
        // (control's affinity resolution reads this `None` as "no session header present").
        assert!(header_session_key(&hdr(&[]), None).is_none());
        assert!(header_session_key(&hdr(&[("x-request-id", "req-1")]), None).is_none());
    }

    #[test]
    fn header_session_key_matches_derive_session_key_for_hard_headers() {
        // A control request and a `/responses` turn presenting the SAME session header must hash
        // to the identical `SessionKey.value` so they resolve the same continuity-session row.
        let h = hdr(&[("x-codex-turn-state", "ts-abc")]);
        let via_header_fn = header_session_key(&h, None).unwrap();
        let via_full_derive = derive_session_key(&h, None, None);
        assert_eq!(via_header_fn.value, via_full_derive.value);
        assert_eq!(via_header_fn.strength, KeyStrength::Hard);

        let h2 = hdr(&[("session_id", "sess-1")]);
        let via_header_fn2 = header_session_key(&h2, Some("thread-1")).unwrap();
        let via_full_derive2 = derive_session_key(&h2, Some("thread-1"), None);
        assert_eq!(via_header_fn2.value, via_full_derive2.value);
    }

    #[test]
    fn prompt_cache_key_isolates_session_thread() {
        // Same session header, different prompt_cache_key ⇒ different (thread-isolated) hard keys.
        let a = ctx_of(
            &hdr(&[("session_id", "s")]),
            serde_json::json!({"prompt_cache_key": "thread-1", "input": "hi"}),
        );
        let b = ctx_of(
            &hdr(&[("session_id", "s")]),
            serde_json::json!({"prompt_cache_key": "thread-2", "input": "hi"}),
        );
        assert_ne!(a.session_key.unwrap().value, b.session_key.unwrap().value);
    }
}
