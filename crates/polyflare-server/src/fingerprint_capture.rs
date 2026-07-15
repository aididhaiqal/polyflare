//! Content-safe structural HTTP fingerprint capture (M5 — the capture-fixture mechanism the
//! fingerprint-parity gate needs, per `docs/POLYFLARE-DESIGN.md`'s "capture-fixture + CI
//! parity-diff gate" and `docs/DESIGN-DECISIONS.md` E4(a)).
//!
//! Gated behind `POLYFLARE_CAPTURE_FINGERPRINT=<path>` (read into `ServeConfig`/`AppState` —
//! mirrors how `POLYFLARE_ANTHROPIC_UPSTREAM_URL` is read/threaded). When unset, `AppState`'s
//! `capture_fingerprint_path` is `None` and the ingress never calls into this module at all — an
//! `Option` check on the hot path, not a feature flag, so disabled capture costs nothing beyond
//! that check.
//!
//! # Content safety (the whole point)
//! [`capture_request_fingerprint`] records STRUCTURE ONLY: the HTTP method, the request path, and
//! — for every header — the header NAME plus a redacted, structural value descriptor. It must
//! NEVER emit a bearer/token, a real session/thread/turn/window/installation/account id, request
//! or response content, or any raw header value that could carry those. The redaction rules,
//! applied in order per header:
//!
//! 1. `authorization` → the fixed marker `"<bearer redacted>"` — never the token, never even its
//!    length/shape (a credential's shape is itself sensitive enough to withhold).
//! 2. A known id-carrying header name (session/thread/turn/window/installation/account/request id
//!    — see [`ID_HEADER_NAMES`]) → a FORMAT descriptor only (`"uuid"`, `"uuid:int"`, or a coarse
//!    `"<len N class>"` shape tag), never the value.
//! 3. A header whose value parses as a JSON *object* (regardless of header name — this covers
//!    `x-codex-turn-metadata`-shaped headers generically, without hard-coding that exact header
//!    name) → the sorted set of its top-level *and* nested key paths, plus each key's JSON value
//!    TYPE (`string`/`number`/`bool`/`object`/`array`/`null`) — never any leaf VALUE.
//! 4. `user-agent` → [`mask_user_agent`]: the format is preserved (product name, punctuation,
//!    spacing, parens) but every version/host-identifying token is replaced with a shape
//!    placeholder.
//! 5. A `(name, value)` pair that exactly matches a known-static protocol constant (see
//!    [`STATIC_CONSTANTS`] — e.g. `accept: text/event-stream`) → recorded verbatim; these are
//!    fixed wire-protocol values, never user/account-identifying.
//! 6. Anything else → [`classify_generic`]: header name + value length + a coarse class
//!    (`ascii-token`/`number`/`enum`/`opaque`) — never the raw value.
//!
//! When unsure which bucket a header falls into, it falls through to the generic (6), which never
//! echoes raw content — "when in doubt, mask" holds structurally, not just by convention.
//!
//! # Header-order fidelity
//! `axum`'s `HeaderMap` (the `http` crate's) documents its own iteration order as "arbitrary...
//! though consistent across platforms for a given crate version" — this is NOT the client's wire
//! order (the crate only documents insertion order for repeated *same-name* values via `GetAll`,
//! not the overall map). There is no supported way to recover true receipt order from this type
//! once axum has parsed the request into it. So this capture does **not** achieve byte-for-byte
//! header-ORDER fidelity. Instead, [`capture_request_fingerprint`] emits headers **sorted
//! alphabetically by name**, which is deterministic and diffable (a set/shape comparison), just not
//! an order comparison. True wire-order capture would need a lower-level intercept (e.g. a raw
//! hyper `Service`/connection hook reading header bytes before `http`'s map discards order) — out
//! of scope for this capture-fixture mechanism.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use axum::http::{HeaderMap, HeaderValue};
use serde_json::{json, Value};

/// Header names (lowercase) whose value is a bearer/opaque authorization credential.
const AUTH_HEADER_NAMES: &[&str] = &["authorization"];

/// Header names (lowercase) known to carry a real end-to-end identifier. Recorded as a FORMAT
/// descriptor only (rule 2 above), never the raw value. Covers both hyphenated and underscored
/// spellings actually seen in this codebase's own header handling (`session_key.rs` checks
/// `session_id` / `x-session-id`; `docs/reference/codex-lb-continuity-reference.md` / SPEC-M3 §3
/// note `x-codex-turn-state`, session, thread/turn/window/installation ids as the real Codex CLI's
/// identity headers).
const ID_HEADER_NAMES: &[&str] = &[
    "session-id",
    "session_id",
    "x-session-id",
    "thread-id",
    "x-codex-thread-id",
    "turn-id",
    "x-codex-turn-id",
    "window-id",
    "x-codex-window-id",
    "installation-id",
    "x-codex-installation-id",
    "account-id",
    "x-codex-account-id",
    "x-client-request-id",
    "x-request-id",
    "request-id",
];

/// `(header name, exact value)` pairs that are fixed wire-protocol constants — never
/// user/account-identifying — and so may be recorded verbatim (rule 5) instead of redacted.
const STATIC_CONSTANTS: &[(&str, &str)] = &[
    ("accept", "text/event-stream"),
    ("accept", "application/json"),
    ("content-type", "application/json"),
    ("content-type", "text/event-stream"),
    ("connection", "keep-alive"),
    ("accept-encoding", "gzip"),
    ("te", "trailers"),
];

/// Build the content-safe structural fingerprint of one inbound request: HTTP method, request
/// path, and every header's name + a redacted structural value descriptor (see module docs for the
/// per-header redaction rules and the header-order-fidelity limitation).
pub fn capture_request_fingerprint(method: &str, path: &str, headers: &HeaderMap) -> Value {
    let mut entries: Vec<(String, Value)> = headers
        .iter()
        .map(|(name, value)| {
            let lower = name.as_str().to_ascii_lowercase();
            let descriptor = describe_header(&lower, value);
            (lower, descriptor)
        })
        .collect();
    // See "Header-order fidelity" in the module docs: `HeaderMap`'s own order is not wire order,
    // so a stable alphabetical sort stands in as the deterministic, diffable substitute.
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let headers_json: Vec<Value> = entries
        .into_iter()
        .map(|(name, value)| json!({ "name": name, "value": value }))
        .collect();

    json!({
        "method": method,
        "path": path,
        "headers": headers_json,
    })
}

/// Dispatch one header (name already lowercased) through the redaction rules in module-doc order.
fn describe_header(lower_name: &str, value: &HeaderValue) -> Value {
    let text = match value.to_str() {
        Ok(s) => s,
        // Not UTF-8: never surface the raw bytes, only that this happened and their length.
        Err(_) => return json!({ "class": "non-utf8", "len": value.as_bytes().len() }),
    };

    if AUTH_HEADER_NAMES.contains(&lower_name) {
        return json!("<bearer redacted>");
    }

    if ID_HEADER_NAMES.contains(&lower_name) {
        return json!({ "format": describe_id_format(text) });
    }

    if let Some(json_shape) = describe_json_header(text) {
        return json_shape;
    }

    if lower_name == "user-agent" {
        return json!({ "format": mask_user_agent(text) });
    }

    if STATIC_CONSTANTS.contains(&(lower_name, text)) {
        return json!(text);
    }

    classify_generic(text)
}

/// Whether `value` is a canonical `8-4-4-4-12` hyphenated hex UUID (36 chars, hyphens at the
/// RFC 4122 positions, hex elsewhere) — checked structurally, without a regex dependency.
fn is_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    bytes.iter().enumerate().all(|(i, &b)| {
        if matches!(i, 8 | 13 | 18 | 23) {
            b == b'-'
        } else {
            b.is_ascii_hexdigit()
        }
    })
}

/// A FORMAT descriptor for an id-shaped header value: `"uuid"`, `"uuid:int"` (a uuid plus a
/// `:<digits>` suffix, e.g. a window id shaped like `<uuid>:<n>`), or a coarse
/// `"<len N class>"` shape tag — never the value itself.
fn describe_id_format(value: &str) -> String {
    if is_uuid(value) {
        return "uuid".to_string();
    }
    if let Some((maybe_uuid, suffix)) = value.split_once(':') {
        if is_uuid(maybe_uuid) && !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) {
            return "uuid:int".to_string();
        }
    }
    let class = if !value.is_empty() && value.bytes().all(|b| b.is_ascii_hexdigit()) {
        "hex"
    } else if value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        "ascii-token"
    } else {
        "opaque"
    };
    format!("<len {} {class}>", value.chars().count())
}

/// If `value` parses as a JSON *object*, return its structural shape: the sorted set of top-level
/// and nested key paths (dot-joined, e.g. `"sandbox.mode"`) plus each key's JSON value type — never
/// any leaf value. Returns `None` for anything that isn't an object (including JSON arrays/scalars
/// — the header falls through to generic classification instead, which still never echoes raw
/// content).
fn describe_json_header(value: &str) -> Option<Value> {
    let parsed: Value = serde_json::from_str(value).ok()?;
    if !matches!(parsed, Value::Object(_)) {
        return None;
    }
    let mut keys: BTreeSet<String> = BTreeSet::new();
    let mut types: BTreeMap<String, String> = BTreeMap::new();
    walk_json_keys(&parsed, "", &mut keys, &mut types);
    Some(json!({
        "kind": "json",
        "keys": keys.into_iter().collect::<Vec<_>>(),
        "types": types,
    }))
}

/// Recursively collect every key path under a JSON object into `keys`, and every key's own JSON
/// type (including `"object"` for a nested object — its children are ALSO recursed into
/// separately, so nested keys/types are captured too) into `types`. Never records a leaf VALUE.
fn walk_json_keys(
    value: &Value,
    prefix: &str,
    keys: &mut BTreeSet<String>,
    types: &mut BTreeMap<String, String>,
) {
    if let Value::Object(map) = value {
        for (k, v) in map {
            let path = if prefix.is_empty() {
                k.clone()
            } else {
                format!("{prefix}.{k}")
            };
            keys.insert(path.clone());
            types.insert(path.clone(), json_type_name(v).to_string());
            walk_json_keys(v, &path, keys, types);
        }
    }
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Object(_) => "object",
        Value::Array(_) => "array",
        Value::String(_) => "string",
        Value::Number(_) => "number",
        Value::Bool(_) => "bool",
        Value::Null => "null",
    }
}

/// Whether an alnum/`.`/`_`/`-` token looks version-shaped: starts with a digit and contains a
/// `.` (e.g. `0.21.4`, `14.5.0`). Used by [`mask_user_agent`] to decide the placeholder.
fn looks_like_version(token: &str) -> bool {
    token.chars().next().is_some_and(|c| c.is_ascii_digit()) && token.contains('.')
}

fn is_ua_token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-'
}

/// Mask a `user-agent` value's version/host-identifying segments while preserving its FORMAT
/// (punctuation, spacing, parens): the leading product-name token is kept verbatim (e.g.
/// `codex_cli_rs` — a stable, non-identifying constant; `docs/PLAN-M1-skeleton.md` confirms this
/// literal), then every later alnum/`.`/`_`/`-` run is replaced by `<ver>` (version-shaped — see
/// [`looks_like_version`]) or `<seg>` (anything else — an OS name, an OS version, an arch, a
/// terminal name, ...). Every separator byte (`/`, whitespace, `(`, `)`, `;`, `,`) is copied
/// verbatim to preserve the wire shape.
///
/// This is intentionally GENERIC rather than hard-coded to one exact field layout: the live Codex
/// CLI's precise UA field-by-field shape was not independently re-verified for this capture
/// mechanism (the same open verification gap SPEC-M3 risk 4 flags for header names generally) — so
/// per the module's "when unsure, mask" rule, every segment past the product name is masked.
fn mask_user_agent(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut seen_first_token = false;
    let bytes = value.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if is_ua_token_byte(bytes[i]) {
            let start = i;
            while i < bytes.len() && is_ua_token_byte(bytes[i]) {
                i += 1;
            }
            let token = &value[start..i];
            if !seen_first_token {
                out.push_str(token);
                seen_first_token = true;
            } else if looks_like_version(token) {
                out.push_str("<ver>");
            } else {
                out.push_str("<seg>");
            }
        } else {
            // A separator (or non-ASCII) byte: `is_ua_token_byte` only ever matches ASCII, so this
            // position is always a valid `char` boundary.
            let ch = value[i..]
                .chars()
                .next()
                .expect("valid utf-8 at char boundary");
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    out
}

/// The fallback descriptor for a header that isn't a bearer, a known id, JSON, a user-agent, or a
/// known-static constant: its length plus a coarse class — never the raw value.
fn classify_generic(value: &str) -> Value {
    let class = if value.parse::<f64>().is_ok() {
        "number"
    } else if !value.is_empty()
        && value.len() <= 32
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b',' | b'='))
    {
        "enum"
    } else if value.bytes().all(|b| (0x20..=0x7e).contains(&b)) {
        "ascii-token"
    } else {
        "opaque"
    };
    json!({ "len": value.chars().count(), "class": class })
}

/// Append one capture `record` as a single JSON line to `path` (JSON Lines / NDJSON).
///
/// Chosen over "overwrite with the latest": a capture session commonly wants BOTH `/responses` and
/// `/v1/messages` requests (or several of the same kind) collected into one reviewable file without
/// any later request clobbering an earlier one. Each call opens the file in append mode and issues
/// one `write` of a complete `"{json}\n"` line; for the small payloads this produces, that single
/// `write` is effectively atomic under O_APPEND on a local filesystem, so concurrent captures — not
/// expected for this manual, opt-in flow, but not precluded either — interleave at line
/// granularity, never mid-line.
pub fn append_fingerprint_capture(path: &Path, record: &Value) -> std::io::Result<()> {
    let line = serde_json::to_string(record)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{line}")
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

    // Canary values: distinctive enough that if any ever showed up in the serialized fingerprint,
    // it could only have come from here — the content-safety guarantee this whole module exists
    // for. None of these may ever appear in `capture_request_fingerprint`'s output.
    const FAKE_BEARER: &str = "sk-fake-canary-bearer-should-never-leak-abc123XYZ";
    const FAKE_SESSION_ID: &str = "550e8400-e29b-41d4-a716-446655440000";
    const FAKE_THREAD_ID: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
    const FAKE_INSTALLATION_ID: &str = "installation-canary-should-never-leak-42";
    const FAKE_TURN_ID: &str = "turn-canary-should-never-leak-99";
    const FAKE_WINDOW_ID: &str = "window-canary-should-never-leak-7";
    const FAKE_SANDBOX_MODE: &str = "sandbox-mode-canary-should-never-leak";
    const FAKE_UA_VERSION: &str = "0.21.4";
    const FAKE_UA_OSVER: &str = "14.5.0";

    fn fake_turn_metadata() -> String {
        serde_json::json!({
            "installation_id": FAKE_INSTALLATION_ID,
            "session_id": FAKE_SESSION_ID,
            "thread_id": FAKE_THREAD_ID,
            "turn_id": FAKE_TURN_ID,
            "window_id": FAKE_WINDOW_ID,
            "sandbox": { "mode": FAKE_SANDBOX_MODE, "network_disabled": true },
            "retry_count": 2,
        })
        .to_string()
    }

    fn realistic_codex_headers() -> HeaderMap {
        hdr(&[
            ("authorization", &format!("Bearer {FAKE_BEARER}")),
            ("content-type", "application/json"),
            ("accept", "text/event-stream"),
            ("session_id", FAKE_SESSION_ID),
            ("x-codex-thread-id", FAKE_THREAD_ID),
            ("x-codex-turn-metadata", &fake_turn_metadata()),
            (
                "user-agent",
                &format!("codex_cli_rs/{FAKE_UA_VERSION} (Mac OS {FAKE_UA_OSVER}; arm64) unknown_terminal"),
            ),
        ])
    }

    fn find<'a>(fp: &'a Value, name: &str) -> &'a Value {
        fp["headers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|h| h["name"] == name)
            .unwrap_or_else(|| panic!("header `{name}` missing from fingerprint: {fp}"))
    }

    #[test]
    fn records_header_names_and_turn_metadata_key_set_and_types() {
        let headers = realistic_codex_headers();
        let fp = capture_request_fingerprint("POST", "/responses", &headers);

        let names: Vec<&str> = fp["headers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h["name"].as_str().unwrap())
            .collect();
        for expected in [
            "authorization",
            "content-type",
            "accept",
            "session_id",
            "x-codex-thread-id",
            "x-codex-turn-metadata",
            "user-agent",
        ] {
            assert!(
                names.contains(&expected),
                "missing header name `{expected}` in {names:?}"
            );
        }

        let turn_meta = find(&fp, "x-codex-turn-metadata");
        assert_eq!(turn_meta["value"]["kind"], "json");
        let key_strs: Vec<&str> = turn_meta["value"]["keys"]
            .as_array()
            .unwrap()
            .iter()
            .map(|k| k.as_str().unwrap())
            .collect();
        for expected_key in [
            "installation_id",
            "session_id",
            "thread_id",
            "turn_id",
            "window_id",
            "sandbox",
            "sandbox.mode",
            "sandbox.network_disabled",
            "retry_count",
        ] {
            assert!(
                key_strs.contains(&expected_key),
                "missing key `{expected_key}` in {key_strs:?}"
            );
        }
        let types = &turn_meta["value"]["types"];
        assert_eq!(types["installation_id"], "string");
        assert_eq!(types["session_id"], "string");
        assert_eq!(types["sandbox"], "object");
        assert_eq!(types["sandbox.mode"], "string");
        assert_eq!(types["sandbox.network_disabled"], "bool");
        assert_eq!(types["retry_count"], "number");
    }

    #[test]
    fn authorization_is_redacted_to_a_fixed_marker() {
        let headers = realistic_codex_headers();
        let fp = capture_request_fingerprint("POST", "/responses", &headers);
        assert_eq!(find(&fp, "authorization")["value"], "<bearer redacted>");
    }

    #[test]
    fn id_shaped_headers_become_format_descriptors_not_raw_values() {
        let headers = realistic_codex_headers();
        let fp = capture_request_fingerprint("POST", "/responses", &headers);
        assert_eq!(find(&fp, "session_id")["value"]["format"], "uuid");
        assert_eq!(find(&fp, "x-codex-thread-id")["value"]["format"], "uuid");
    }

    #[test]
    fn uuid_int_compound_ids_are_recognized() {
        let headers = hdr(&[(
            "x-codex-window-id",
            "550e8400-e29b-41d4-a716-446655440000:7",
        )]);
        let fp = capture_request_fingerprint("POST", "/responses", &headers);
        assert_eq!(
            find(&fp, "x-codex-window-id")["value"]["format"],
            "uuid:int"
        );
    }

    #[test]
    fn user_agent_is_format_masked() {
        let headers = realistic_codex_headers();
        let fp = capture_request_fingerprint("POST", "/responses", &headers);
        let masked = find(&fp, "user-agent")["value"]["format"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            masked, "codex_cli_rs/<ver> (<seg> <seg> <ver>; <seg>) <seg>",
            "masked UA: {masked}"
        );
        assert!(!masked.contains(FAKE_UA_VERSION));
        assert!(!masked.contains(FAKE_UA_OSVER));
    }

    #[test]
    fn known_static_protocol_constants_are_recorded_verbatim() {
        let headers = realistic_codex_headers();
        let fp = capture_request_fingerprint("POST", "/responses", &headers);
        assert_eq!(find(&fp, "accept")["value"], "text/event-stream");
        assert_eq!(find(&fp, "content-type")["value"], "application/json");
    }

    #[test]
    fn headers_are_sorted_alphabetically_by_name() {
        let headers = realistic_codex_headers();
        let fp = capture_request_fingerprint("POST", "/responses", &headers);
        let names: Vec<&str> = fp["headers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h["name"].as_str().unwrap())
            .collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        assert_eq!(names, sorted);
    }

    #[test]
    fn generic_unknown_header_is_classified_not_echoed_verbatim() {
        let headers = hdr(&[(
            "x-custom-opaque",
            "randomOpaqueValueShouldNotAppearVerbatim123",
        )]);
        let fp = capture_request_fingerprint("GET", "/x", &headers);
        let value = &find(&fp, "x-custom-opaque")["value"];
        let serialized = serde_json::to_string(value).unwrap();
        assert!(!serialized.contains("randomOpaqueValueShouldNotAppearVerbatim123"));
        assert!(value["len"].is_number());
        assert!(value["class"].is_string());
    }

    /// The content-safety guarantee, proven directly: grep the FULLY SERIALIZED fingerprint for
    /// every fake secret/id/version value used above — none may appear anywhere in it.
    #[test]
    fn no_canary_secret_or_id_value_appears_anywhere_in_the_serialized_output() {
        let headers = realistic_codex_headers();
        let fp = capture_request_fingerprint("POST", "/responses", &headers);
        let serialized = serde_json::to_string(&fp).unwrap();

        for canary in [
            FAKE_BEARER,
            FAKE_SESSION_ID,
            FAKE_THREAD_ID,
            FAKE_INSTALLATION_ID,
            FAKE_TURN_ID,
            FAKE_WINDOW_ID,
            FAKE_SANDBOX_MODE,
            FAKE_UA_VERSION,
            FAKE_UA_OSVER,
            "Bearer",
        ] {
            assert!(
                !serialized.contains(canary),
                "canary `{canary}` leaked into fingerprint: {serialized}"
            );
        }
    }

    #[test]
    fn non_utf8_header_value_never_surfaces_raw_bytes() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-weird"),
            HeaderValue::from_bytes(&[0xff, 0xfe, 0xfd, 0xfc]).unwrap(),
        );
        let fp = capture_request_fingerprint("GET", "/x", &headers);
        let value = &find(&fp, "x-weird")["value"];
        assert_eq!(value["class"], "non-utf8");
        assert_eq!(value["len"], 4);
    }

    #[test]
    fn append_fingerprint_capture_writes_one_json_line_per_call() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("golden.jsonl");
        let fp1 = capture_request_fingerprint(
            "POST",
            "/responses",
            &hdr(&[("accept", "text/event-stream")]),
        );
        let fp2 = capture_request_fingerprint(
            "POST",
            "/v1/messages",
            &hdr(&[("accept", "application/json")]),
        );
        append_fingerprint_capture(&path, &fp1).unwrap();
        append_fingerprint_capture(&path, &fp2).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2, "expected one JSON line per call: {content}");
        let parsed0: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed0["path"], "/responses");
        let parsed1: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(parsed1["path"], "/v1/messages");
    }
}
