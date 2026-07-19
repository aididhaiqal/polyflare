//! The WS incremental-vs-full request planning (M5a Task 6, `docs/superpowers/plans/
//! 2026-07-17-polyflare-m5a-upstream-websocket.md` "Task 6: The delta decision").
//!
//! This is why the milestone exists: a continuation turn that qualifies uploads a delta instead
//! of the full conversation history (86x less on the measured probe), with history prefilled once
//! instead of re-billed every turn. Get this "too eager" and we send an incremental request the
//! server can't reconcile (a corrupted turn); get it "too shy" and we silently full-resend every
//! turn and the milestone delivers nothing while appearing to work. **Mirror codex's own rule here
//! — do not improve on it.**
//!
//! **Wire authority:** `docs/WS-GROUND-TRUTH-CODEX.md` §3, citing real `codex-rs`:
//! `prepare_websocket_request` (`client.rs:1222-1253`) — incremental only if `get_last_response()`
//! (non-blocking `try_recv()`) yields a `LastResponse` for **this connection** AND the new request
//! is a strict extension with matching non-input fields, checked by
//! `responses_request_properties_match` (`client.rs:306-359`). `LastResponse.response_id` comes
//! from the `response.id` of the most recent `response.completed` on that same connection
//! (`client.rs:1998-2018`). Incremental **only if ALL** of:
//! 1. the socket has a `last_response_id` from a prior `response.completed` on **this same
//!    connection**;
//! 2. the new `input` is a **strict extension** of what this socket last sent (identical prefix,
//!    then new items — not merely "not shorter": an equal-or-shorter length, or a same-length
//!    array with a changed earlier item, is never an extension);
//! 3. every non-input field matches: `model`, `instructions`, `tools`, `tool_choice`,
//!    `parallel_tool_calls`, `reasoning`, `service_tier`, `text` (ground truth §3's
//!    `ResponseCreateWsRequest` field list, `common.rs:265-293`, minus the fields that
//!    legitimately vary or are decided elsewhere: `input`/`previous_response_id` are what this
//!    function computes, and `store`/`stream`/`stream_options`/`include`/`prompt_cache_key`/
//!    `generate`/`client_metadata` are transport/session plumbing the plan's Task 6 wording does
//!    not list as part of the match).
//!
//! Any mismatch anywhere in 1-3 => [`RequestPlan::Full`]. When in doubt => `Full`.
//!
//! ## Revision: content-free delta state (supersedes the original `last_body` design)
//!
//! The first cut of this module took the prior turn's full envelope as an explicit
//! `last_body: Option<&Value>` parameter, reasoning that `WsConn` shouldn't be widened with a
//! field pinning raw conversation content to a long-lived, connection-scoped struct. That
//! reasoning about `WsConn` was correct — but it only moved the content-retention problem to
//! whichever caller held `last_body` per connection, which is retention in RAM all the same.
//! `docs/SPEC-M5-WEBSOCKET.md` §6 requires M5a to retain **zero** conversation content (unlike
//! M5b's deliberate, fenced RAM accumulation) — a `last_body` sitting anywhere, even off `WsConn`,
//! violates that.
//!
//! The fix: rule 2's "is the new input a strict extension of what we last sent" check does not
//! need the prior items themselves — it only needs to know whether they're *unchanged*, which a
//! content-free per-item hash answers exactly as well (modulo hash collision). [`plan_request`]
//! now takes `last_item_hashes: Option<&[ItemHash]>` (the prior turn's per-item hashes, in order)
//! instead of `last_body`, plus `last_non_input_fingerprint: Option<&str>` (a hash of rule 3's
//! fields, replacing direct `Value` comparison against a retained `last_body`). Both are exactly
//! what [`WsConn`] now stores (`last_item_hashes`, `last_non_input_fingerprint`), so
//! [`plan_request_for_conn`] no longer needs a `last_body` argument at all — ALL prior-turn state
//! the planner needs now lives on the connection, content-free, which was the whole point of
//! declining to widen `WsConn` with raw content in the first place.
//!
//! **Why hash-prefix comparison is exactly equivalent to item-by-item comparison:** a strict
//! extension means "the first `last_item_hashes.len()` items of the new input are identical to
//! what was sent last time, and there is at least one more item after that." Two items are
//! identical iff their canonical JSON encodings are identical (this crate enables no
//! `preserve_order` feature anywhere in the workspace, so `Value::Object`'s `BTreeMap` backing
//! serializes any two semantically-equal objects to byte-identical strings regardless of
//! insertion order — array order, which is what actually matters for conversation history, is
//! preserved either way). Hashing that canonical encoding and comparing hashes therefore agrees
//! with comparing the items themselves for every input except an actual sha256 collision — the
//! same standard the durable `input_fingerprint` (`polyflare-core/src/types.rs:217`,
//! `polyflare-server/src/session_key.rs`'s `sha256_hex`) already relies on elsewhere in this
//! codebase. This module follows that same hashing convention (`sha2`, lowercase hex) rather than
//! introducing a new one.
//!
//! **Who must set the hashes, and when:** [`WsConn::last_item_hashes`] /
//! [`WsConn::last_non_input_fingerprint`] start `None` and stay `None` until whoever SENDS a turn
//! on this connection (Task 5/7's turn-send code) sets them, immediately after sending, via
//! [`item_hashes`] / [`non_input_fingerprint`] applied to the envelope that was just sent. This
//! module only computes and compares; it never sends a frame and therefore never sets these
//! fields itself. If the sender forgets, `plan_request` silently and permanently sees
//! `last_item_hashes: None`, which gate 1 alone turns into `Full` forever — no error, just a
//! milestone that quietly never produces an incremental turn.
//!
//! **Content-safety:** `body` and a produced [`RequestPlan::Incremental`]'s `suffix` still carry
//! conversation content (as before — `body` is a short-lived, per-call argument, never retained).
//! [`ItemHash`] and the fingerprint `String`s are sha256 digests: not reversible to content, safe
//! to hold indefinitely (that's precisely why [`WsConn`] can now carry them). Nothing here
//! implements or derives `Debug` that would print an item; [`RequestPlan`]'s hand-written `Debug`
//! redacts `suffix` the same way `PreparedRequest` redacts `body`
//! (`polyflare-core/src/types.rs:42-50`). This module also never logs a frame or a body — it is
//! pure comparison logic with no I/O.

use serde_json::Value;
use sha2::{Digest, Sha256};

use super::WsConn;

/// The non-input `ResponseCreateWsRequest` fields (ground truth §3) that must match for an
/// incremental request to be valid. `input` and `previous_response_id` are deliberately excluded
/// — they are what this function computes, not what it compares for equality.
const NON_INPUT_FIELDS: &[&str] = &[
    "model",
    "instructions",
    "tools",
    "tool_choice",
    "parallel_tool_calls",
    "reasoning",
    "service_tier",
    "text",
];

/// Lowercase-hex sha256 of `bytes` — the same hashing convention `polyflare-server`'s
/// `session_key::sha256_hex` already uses for content-free session keys / input fingerprints.
/// Reimplemented locally (rather than depending on `polyflare-server`, which depends on this
/// crate, not the other way around) using `sha2` alone — no `hex` crate dependency needed.
///
/// `pub(crate)` so `ws::executor` can hash the content-free `conn_discriminator`
/// (`x-codex-window-id`) into `conn_key` with the SAME hashing convention as the other key halves,
/// without reimplementing hashing inline or adding a dependency.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A content-free sha256 hash of a single `input` item's canonical JSON encoding. Never
/// reversible to the item it was derived from — safe to hold indefinitely on the long-lived
/// [`WsConn`], unlike the item itself. Deriving `Debug` is safe here: the wrapped string IS the
/// hash, not conversation content (same reasoning [`RequestPlan`]'s hand-written `Debug` uses to
/// print `anchor` as-is — an opaque id is not content either).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ItemHash(String);

impl ItemHash {
    /// Hash one `input` item. See the module doc's "Why hash-prefix comparison is exactly
    /// equivalent" section for why equal items always hash equal and unequal items essentially
    /// never collide.
    fn of(item: &Value) -> Self {
        ItemHash(sha256_hex(item.to_string().as_bytes()))
    }
}

/// Per-item content-free hashes of `body`'s `"input"` array, in order. **This is what a turn's
/// SENDER must call** (on the exact envelope just sent) to populate
/// [`WsConn::last_item_hashes`] — see the module doc's "Who must set the hashes" section.
pub fn item_hashes(body: &Value) -> Vec<ItemHash> {
    input_items(body).iter().map(ItemHash::of).collect()
}

/// A content-free fingerprint of `body`'s [`NON_INPUT_FIELDS`] — hashes only the presence/value of
/// those 8 fields (never `input`). Two envelopes fingerprint equal iff every one of those 8 fields
/// is `==` between them (via `Value`'s own equality, same as the original direct comparison this
/// replaces) — a field's absence is encoded as a MISSING map key, never as `null`, so "field
/// present as `null`" and "field absent" still fingerprint differently (codex omits fields it
/// doesn't set rather than nulling them, ground truth §3). **This is what a turn's SENDER must
/// call** to populate [`WsConn::last_non_input_fingerprint`] — see the module doc.
pub fn non_input_fingerprint(body: &Value) -> String {
    let mut present = serde_json::Map::new();
    for field in NON_INPUT_FIELDS {
        if let Some(v) = body.get(*field) {
            present.insert((*field).to_string(), v.clone());
        }
    }
    sha256_hex(Value::Object(present).to_string().as_bytes())
}

/// The outcome of [`plan_request`]: send only the new suffix anchored on a prior `response.id`,
/// or full-resend the entire history with no anchor.
#[derive(Clone, PartialEq, Eq)]
pub enum RequestPlan {
    /// Anchor on `anchor` (a prior `response.completed`'s `response.id` on this same connection,
    /// ground truth §3) and send only `suffix` as `input` — the new items past what this socket
    /// last sent. Never the full history.
    Incremental { anchor: String, suffix: Vec<Value> },
    /// Send the full history with no `previous_response_id`. The safe default whenever any of the
    /// three conditions in the module doc does not hold.
    Full,
}

// `suffix` carries conversation content (new `input` items) — same content-safety reasoning as
// `PreparedRequest`'s `Debug` (`polyflare-core/src/types.rs:42-50`): never print it via `{:?}`.
// `anchor` is an opaque backend-assigned response id, not conversation content, so it prints as-is
// (mirrors how `PreparedRequest::model` is not redacted either).
impl std::fmt::Debug for RequestPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestPlan::Incremental { anchor, suffix } => f
                .debug_struct("RequestPlan::Incremental")
                .field("anchor", anchor)
                .field("suffix_len", &suffix.len())
                .field("suffix", &"<redacted>")
                .finish(),
            RequestPlan::Full => f.write_str("RequestPlan::Full"),
        }
    }
}

/// Decide whether the new turn (`body`, a full `response.create`-shaped envelope: non-input
/// fields plus an `"input"` array) can go out as an incremental continuation of the turn most
/// recently sent on this connection, anchored on `last_response_id` (that prior turn's
/// `response.id`, from a `response.completed` on **this same connection** — never a value
/// borrowed from a different socket or a different account).
///
/// Returns `Full` unless ALL of:
/// 1. `last_response_id` is `Some` AND `last_item_hashes` is `Some` (a turn has both completed and
///    been sent on this connection — see the module doc for who populates `last_item_hashes` and
///    when);
/// 2. `body`'s `"input"` array is a strict extension of `last_item_hashes` — strictly longer, with
///    the new input's per-item hashes matching `last_item_hashes` exactly over that prefix
///    (missing `"input"` is treated as an empty array, same as codex would see no items);
/// 3. `last_non_input_fingerprint` equals `non_input_fingerprint(body)` — i.e. every one of
///    [`NON_INPUT_FIELDS`] matches (a field's absence counts as `None`, so "present as `null`" and
///    "absent" are NOT treated as equal — codex omits fields it doesn't set rather than nulling
///    them, ground truth §3).
///
/// When 2 holds, the produced `suffix` is exactly `body`'s `"input"` items past
/// `last_item_hashes`'s length — never the whole array, never a re-derived guess.
pub fn plan_request(
    last_response_id: Option<&str>,
    last_item_hashes: Option<&[ItemHash]>,
    last_non_input_fingerprint: Option<&str>,
    body: &Value,
) -> RequestPlan {
    let (Some(last_response_id), Some(last_item_hashes)) = (last_response_id, last_item_hashes)
    else {
        return RequestPlan::Full;
    };

    let new_input = input_items(body);
    let new_item_hashes = item_hashes(body);

    // Rule 2: strict extension, checked ENTIRELY through hashes — never through the items
    // themselves (this function never sees the prior items, only their hashes). `<=` (not just
    // "shorter") also correctly rejects the dangerous same-length case: if nothing grew, there is
    // no suffix to send, so it can never be a valid extension regardless of what the hash
    // comparison below would say.
    if new_item_hashes.len() <= last_item_hashes.len() {
        return RequestPlan::Full;
    }
    if new_item_hashes[..last_item_hashes.len()] != *last_item_hashes {
        return RequestPlan::Full;
    }

    // Rule 3: compare fingerprints (content-free), not fields directly — equivalent to the
    // original field-by-field comparison per `non_input_fingerprint`'s doc.
    if last_non_input_fingerprint != Some(non_input_fingerprint(body).as_str()) {
        return RequestPlan::Full;
    }

    RequestPlan::Incremental {
        anchor: last_response_id.to_string(),
        suffix: new_input[last_item_hashes.len()..].to_vec(),
    }
}

/// Convenience wrapper reading ALL prior-turn state straight off `conn` — now possible because
/// `WsConn` carries only content-free state (`last_response_id`, `last_item_hashes`,
/// `last_non_input_fingerprint`), unlike the original design this superseded (see the module
/// doc's "Revision" section), which still needed a `last_body` argument from the caller.
pub fn plan_request_for_conn(conn: &WsConn, body: &Value) -> RequestPlan {
    plan_request(
        conn.last_response_id.as_deref(),
        conn.last_item_hashes.as_deref(),
        conn.last_non_input_fingerprint.as_deref(),
        body,
    )
}

/// `body["input"]` as a slice, or an empty slice if absent/not an array. Never re-parses a raw
/// wire body — `body` is an already-materialized `Value` the caller (Task 7) holds for other
/// reasons; this just borrows the one field it needs.
fn input_items(body: &Value) -> &[Value] {
    body.get("input")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn item(n: u32) -> Value {
        json!({"role": "user", "content": format!("item-{n}")})
    }

    /// A baseline envelope with every `NON_INPUT_FIELDS` entry populated with a distinct,
    /// non-default value, plus a 2-item `input`. Every field-mismatch test mutates exactly one
    /// field off of this baseline.
    fn baseline(input: Vec<Value>) -> Value {
        json!({
            "model": "gpt-5.6-sol",
            "instructions": "be helpful",
            "tools": [{"type": "function", "name": "shell"}],
            "tool_choice": "auto",
            "parallel_tool_calls": true,
            "reasoning": {"effort": "medium"},
            "service_tier": "default",
            "text": {"format": {"type": "text"}},
            "input": input,
        })
    }

    /// The content-free prior-turn state (`last_item_hashes`, `last_non_input_fingerprint`) that
    /// `plan_request` now takes in place of a retained `last_body`. Mirrors exactly what a real
    /// sender (Task 5/7) would compute and store on `WsConn` right after sending `baseline(input)`.
    fn last_state(input: Vec<Value>) -> (Vec<ItemHash>, String) {
        let envelope = baseline(input);
        (item_hashes(&envelope), non_input_fingerprint(&envelope))
    }

    const ANCHOR: &str = "resp_prior_turn_123";

    // ---- strict extension --------------------------------------------------------------

    #[test]
    fn strict_extension_is_incremental_with_only_the_new_items() {
        let (last_hashes, last_fp) = last_state(vec![item(0), item(1)]);
        let new_items = [item(0), item(1), item(2), item(3)];
        let body = baseline(new_items.to_vec());

        let plan = plan_request(Some(ANCHOR), Some(&last_hashes), Some(&last_fp), &body);

        assert_eq!(
            plan,
            RequestPlan::Incremental {
                anchor: ANCHOR.to_string(),
                suffix: vec![item(2), item(3)],
            },
            "suffix must be EXACTLY the two new items, not the whole array and not just the \
             right length"
        );
    }

    // ---- non-input field mismatches (table-driven, one field at a time) ------------------

    #[test]
    fn each_non_input_field_changed_alone_forces_full() {
        let (last_hashes, last_fp) = last_state(vec![item(0), item(1)]);
        let new_items = vec![item(0), item(1), item(2)];

        let mutations: &[(&str, Value)] = &[
            ("model", json!("gpt-5.6-sol-DIFFERENT")),
            ("instructions", json!("be unhelpful instead")),
            (
                "tools",
                json!([{"type": "function", "name": "apply_patch"}]),
            ),
            ("tool_choice", json!("required")),
            ("parallel_tool_calls", json!(false)),
            ("reasoning", json!({"effort": "high"})),
            ("service_tier", json!("priority")),
            ("text", json!({"format": {"type": "json_schema"}})),
        ];

        for (field, new_value) in mutations {
            let mut body = baseline(new_items.clone());
            body[field] = new_value.clone();

            let plan = plan_request(Some(ANCHOR), Some(&last_hashes), Some(&last_fp), &body);

            assert_eq!(
                plan,
                RequestPlan::Full,
                "changing `{field}` alone (leaving a valid strict-extension input untouched) \
                 must force Full, got {plan:?}"
            );
        }
    }

    #[test]
    fn changed_model_is_full() {
        // Single explicit case per the plan's Step 1 list, in addition to the table above.
        let (last_hashes, last_fp) = last_state(vec![item(0), item(1)]);
        let mut body = baseline(vec![item(0), item(1), item(2)]);
        body["model"] = json!("a-completely-different-model");

        assert_eq!(
            plan_request(Some(ANCHOR), Some(&last_hashes), Some(&last_fp), &body),
            RequestPlan::Full
        );
    }

    // ---- the dangerous case: an earlier item changed, not a genuine extension -------------

    #[test]
    fn changed_earlier_item_same_length_is_full() {
        // Same length as last time (no new items at all) but item 0 was edited. Not an
        // extension by definition — must never be reported Incremental with an empty suffix.
        let (last_hashes, last_fp) = last_state(vec![item(0), item(1)]);
        let body = baseline(vec![item(99), item(1)]);

        assert_eq!(
            plan_request(Some(ANCHOR), Some(&last_hashes), Some(&last_fp), &body),
            RequestPlan::Full
        );
    }

    #[test]
    fn changed_earlier_item_despite_growth_is_full() {
        // The genuinely dangerous shape: input DID grow (a naive "did it get longer" check would
        // pass), but an earlier (non-appended) item silently differs from what this socket
        // actually sent last time. Anchoring on `last_response_id` here would resume a history
        // that has diverged from what the server actually has — must be Full, not Incremental.
        let (last_hashes, last_fp) = last_state(vec![item(0), item(1)]);
        let body = baseline(vec![
            item(0),
            item(99), /* changed */
            item(2),  /* new */
        ]);

        assert_eq!(
            plan_request(Some(ANCHOR), Some(&last_hashes), Some(&last_fp), &body),
            RequestPlan::Full
        );
    }

    #[test]
    fn changed_earlier_item_is_caught_through_a_hash_mismatch_not_value_comparison() {
        // Same dangerous shape as `changed_earlier_item_despite_growth_is_full`, but this test
        // proves the MECHANISM: `plan_request` never sees the prior turn's actual items again —
        // only their hashes were ever retained — so the divergence must be caught purely because
        // a hash at the same position differs, not because anything compared the original values.
        let last_hashes = item_hashes(&baseline(vec![item(0), item(1)]));
        let last_fp = non_input_fingerprint(&baseline(vec![item(0), item(1)]));

        let body = baseline(vec![
            item(99), /* changed */
            item(1),
            item(2), /* new */
        ]);
        let new_hashes = item_hashes(&body);

        assert_ne!(
            new_hashes[0], last_hashes[0],
            "item(99) must hash differently from item(0) for the prefix check to catch this"
        );
        assert_eq!(
            new_hashes[1], last_hashes[1],
            "the untouched item(1) must hash identically, isolating the change to position 0"
        );

        assert_eq!(
            plan_request(Some(ANCHOR), Some(&last_hashes), Some(&last_fp), &body),
            RequestPlan::Full,
            "a changed earlier item, caught via the hash prefix mismatch, must force Full"
        );
    }

    #[test]
    fn item_hashes_are_deterministic_and_content_sensitive() {
        let a = item_hashes(&baseline(vec![item(0), item(1)]));
        let b = item_hashes(&baseline(vec![item(0), item(1)]));
        assert_eq!(
            a, b,
            "hashing the same items twice must produce identical hashes"
        );

        let c = item_hashes(&baseline(vec![item(0), item(2)]));
        assert_ne!(
            a, c,
            "a different item at the same position must hash differently"
        );
    }

    #[test]
    fn non_input_fingerprint_distinguishes_absent_from_null() {
        let mut with_null = baseline(vec![item(0)]);
        with_null["service_tier"] = Value::Null;

        let mut absent = baseline(vec![item(0)]);
        absent.as_object_mut().unwrap().remove("service_tier");

        assert_ne!(
            non_input_fingerprint(&with_null),
            non_input_fingerprint(&absent),
            "a field present as `null` must fingerprint differently from the field being absent \
             entirely — codex omits unset fields rather than nulling them"
        );
    }

    #[test]
    fn item_hash_debug_never_contains_raw_item_content() {
        let sensitive = json!({"role": "user", "content": "SENSITIVE_MARKER_qwer9876"});
        let hashes = item_hashes(&baseline(vec![sensitive]));
        let rendered = format!("{hashes:?}");
        assert!(
            !rendered.contains("SENSITIVE_MARKER_qwer9876"),
            "ItemHash Debug must never leak the item it was derived from: {rendered}"
        );
    }

    // ---- no last_response_id / no last_item_hashes -----------------------------------------

    #[test]
    fn no_last_response_id_is_full() {
        let (last_hashes, last_fp) = last_state(vec![item(0), item(1)]);
        let body = baseline(vec![item(0), item(1), item(2)]);

        assert_eq!(
            plan_request(None, Some(&last_hashes), Some(&last_fp), &body),
            RequestPlan::Full
        );
    }

    #[test]
    fn no_last_item_hashes_is_full() {
        // First turn on a fresh connection: nothing sent yet to extend.
        let body = baseline(vec![item(0)]);

        assert_eq!(
            plan_request(Some(ANCHOR), None, None, &body),
            RequestPlan::Full
        );
    }

    // ---- shorter input ----------------------------------------------------------------

    #[test]
    fn shorter_input_is_full() {
        let (last_hashes, last_fp) = last_state(vec![item(0), item(1), item(2)]);
        let body = baseline(vec![item(0), item(1)]);

        assert_eq!(
            plan_request(Some(ANCHOR), Some(&last_hashes), Some(&last_fp), &body),
            RequestPlan::Full
        );
    }

    // ---- Debug redaction ------------------------------------------------------------------

    #[test]
    fn incremental_debug_never_prints_suffix_content() {
        let plan = RequestPlan::Incremental {
            anchor: ANCHOR.to_string(),
            suffix: vec![json!({"role": "user", "content": "SENSITIVE_MARKER_asdf1234"})],
        };

        let rendered = format!("{plan:?}");

        assert!(
            !rendered.contains("SENSITIVE_MARKER_asdf1234"),
            "Debug output must never contain suffix content: {rendered}"
        );
        assert!(
            rendered.contains(ANCHOR),
            "anchor is not content and may print: {rendered}"
        );
    }

    // ---- plan_request_for_conn reads only conn's content-free state ------------------------

    #[tokio::test]
    async fn plan_request_for_conn_uses_conn_last_response_id() {
        use polyflare_core::Account;
        use polyflare_testkit::{MockWsUpstream, ScriptedTurn};

        let mock = MockWsUpstream::new(ScriptedTurn::normal(vec![]));
        let base = mock.clone().spawn().await;
        let account = Account {
            id: "acct".to_string(),
            base_url: base,
            bearer_token: "token".to_string(),
            chatgpt_account_id: None,
        };
        let conn = WsConn::connect(&account, &[]).await.expect("connect");
        assert!(conn.last_response_id.is_none());
        assert!(conn.last_item_hashes.is_none());

        let body = baseline(vec![item(0), item(1)]);

        // No last_response_id yet on a fresh connection => Full, regardless of body.
        assert_eq!(plan_request_for_conn(&conn, &body), RequestPlan::Full);
    }
}
