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
//! ## Signature deviation from the plan's sketch, and why
//!
//! The plan sketches `plan_request(conn: &WsConn, body: &Value)`. `WsConn` (`ws/conn.rs`) does
//! carry `last_response_id: Option<String>`, but its other two fields —
//! `last_input_count: Option<u32>` and `last_input_fingerprint: Option<String>` — are documented
//! there as covering ONLY the count of the last-sent `input` array and a fingerprint of the
//! **non-input** fields; explicitly "Never raw conversation content." Neither field can support
//! rule 2 above: verifying that the new `input`'s prefix is *identical* to what was actually sent
//! last time (the "changed earlier item" dangerous case) requires comparing actual item values,
//! not a length or a non-input-only summary. A count alone cannot distinguish "the earlier items
//! are unchanged, N new ones were appended" from "an earlier item was silently edited and N new
//! ones were appended" — exactly the corruption this module exists to prevent.
//!
//! Rather than widening `WsConn` with a field that would pin raw conversation content to a
//! long-lived, connection-scoped struct (a content-retention footprint of its own, and an edit to
//! `conn.rs`, which is off-limits while another task adopts the deflate forks there concurrently),
//! [`plan_request`] takes the previous turn's full envelope explicitly as `last_body: Option<&Value>`
//! — supplied by whichever short-lived caller already holds it (the turn/executor state, Task 7's
//! concern, not this connection's). [`plan_request_for_conn`] is a thin convenience wrapper that
//! reads `conn.last_response_id` for the anchor-presence gate (rule 1) without reading or
//! restructuring anything else on `WsConn`.
//!
//! **Content-safety:** `last_body`, `body`, and a produced [`RequestPlan::Incremental`]'s `suffix`
//! all carry conversation content. Nothing here implements or derives `Debug` that would print an
//! item; [`RequestPlan`]'s hand-written `Debug` redacts `suffix` the same way `PreparedRequest`
//! redacts `body` (`polyflare-core/src/types.rs:42-50`). This module also never logs a frame or a
//! body — it is pure comparison logic with no I/O.

use serde_json::Value;

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
/// fields plus an `"input"` array) can go out as an incremental continuation of `last_body` (the
/// full envelope this socket most recently sent), anchored on `last_response_id` (that prior
/// turn's `response.id`, from a `response.completed` on **this same connection** — never a value
/// borrowed from a different socket or a different account).
///
/// Returns `Full` unless ALL of:
/// 1. `last_response_id` is `Some` (a turn has actually completed on this connection);
/// 2. `last_body` is `Some`, and `body`'s `"input"` array is a strict extension of `last_body`'s —
///    strictly longer, with an identical prefix (missing `"input"` on either side is treated as
///    an empty array, same as codex would see no items);
/// 3. every field in [`NON_INPUT_FIELDS`] is `==` (via `Value`'s own equality; a field's absence
///    counts as `None`, so "present as `null`" and "absent" are NOT treated as equal — codex omits
///    fields it doesn't set rather than nulling them, ground truth §3).
///
/// When 2 holds, the produced `suffix` is exactly `body`'s `"input"` items past `last_body`'s
/// count — never the whole array, never a re-derived guess.
pub fn plan_request(
    last_response_id: Option<&str>,
    last_body: Option<&Value>,
    body: &Value,
) -> RequestPlan {
    let (Some(last_response_id), Some(last_body)) = (last_response_id, last_body) else {
        return RequestPlan::Full;
    };

    let last_input = input_items(last_body);
    let new_input = input_items(body);

    // Rule 2: strict extension only. `<=` (not just "shorter") also correctly rejects the
    // dangerous same-length case: if nothing grew, there is no suffix to send, so it can never be
    // a valid extension regardless of what the content comparison below would say.
    if new_input.len() <= last_input.len() {
        return RequestPlan::Full;
    }
    if new_input[..last_input.len()] != *last_input {
        return RequestPlan::Full;
    }

    // Rule 3.
    for field in NON_INPUT_FIELDS {
        if last_body.get(*field) != body.get(*field) {
            return RequestPlan::Full;
        }
    }

    RequestPlan::Incremental {
        anchor: last_response_id.to_string(),
        suffix: new_input[last_input.len()..].to_vec(),
    }
}

/// Convenience wrapper reading `conn.last_response_id` for rule 1 (the anchor-presence gate),
/// without reading or restructuring any other `WsConn` field — see the module doc's "Signature
/// deviation" section for why `last_body` still comes in as an explicit parameter rather than
/// from `conn`.
pub fn plan_request_for_conn(
    conn: &WsConn,
    last_body: Option<&Value>,
    body: &Value,
) -> RequestPlan {
    plan_request(conn.last_response_id.as_deref(), last_body, body)
}

/// `body["input"]` as a slice, or an empty slice if absent/not an array. Never re-parses a raw
/// wire body — `body`/`last_body` are already-materialized `Value`s the caller (Task 7) holds for
/// other reasons; this just borrows the one field it needs.
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

    const ANCHOR: &str = "resp_prior_turn_123";

    // ---- strict extension --------------------------------------------------------------

    #[test]
    fn strict_extension_is_incremental_with_only_the_new_items() {
        let last = baseline(vec![item(0), item(1)]);
        let new_items = [item(0), item(1), item(2), item(3)];
        let body = baseline(new_items.to_vec());

        let plan = plan_request(Some(ANCHOR), Some(&last), &body);

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
        let last = baseline(vec![item(0), item(1)]);
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

            let plan = plan_request(Some(ANCHOR), Some(&last), &body);

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
        let last = baseline(vec![item(0), item(1)]);
        let mut body = baseline(vec![item(0), item(1), item(2)]);
        body["model"] = json!("a-completely-different-model");

        assert_eq!(
            plan_request(Some(ANCHOR), Some(&last), &body),
            RequestPlan::Full
        );
    }

    // ---- the dangerous case: an earlier item changed, not a genuine extension -------------

    #[test]
    fn changed_earlier_item_same_length_is_full() {
        // Same length as last time (no new items at all) but item 0 was edited. Not an
        // extension by definition — must never be reported Incremental with an empty suffix.
        let last = baseline(vec![item(0), item(1)]);
        let body = baseline(vec![item(99), item(1)]);

        assert_eq!(
            plan_request(Some(ANCHOR), Some(&last), &body),
            RequestPlan::Full
        );
    }

    #[test]
    fn changed_earlier_item_despite_growth_is_full() {
        // The genuinely dangerous shape: input DID grow (a naive "did it get longer" check would
        // pass), but an earlier (non-appended) item silently differs from what this socket
        // actually sent last time. Anchoring on `last_response_id` here would resume a history
        // that has diverged from what the server actually has — must be Full, not Incremental.
        let last = baseline(vec![item(0), item(1)]);
        let body = baseline(vec![
            item(0),
            item(99), /* changed */
            item(2),  /* new */
        ]);

        assert_eq!(
            plan_request(Some(ANCHOR), Some(&last), &body),
            RequestPlan::Full
        );
    }

    // ---- no last_response_id -------------------------------------------------------------

    #[test]
    fn no_last_response_id_is_full() {
        let last = baseline(vec![item(0), item(1)]);
        let body = baseline(vec![item(0), item(1), item(2)]);

        assert_eq!(plan_request(None, Some(&last), &body), RequestPlan::Full);
    }

    #[test]
    fn no_last_body_is_full() {
        // First turn on a fresh connection: nothing sent yet to extend.
        let body = baseline(vec![item(0)]);

        assert_eq!(plan_request(Some(ANCHOR), None, &body), RequestPlan::Full);
    }

    // ---- shorter input ----------------------------------------------------------------

    #[test]
    fn shorter_input_is_full() {
        let last = baseline(vec![item(0), item(1), item(2)]);
        let body = baseline(vec![item(0), item(1)]);

        assert_eq!(
            plan_request(Some(ANCHOR), Some(&last), &body),
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

    // ---- plan_request_for_conn reads only conn.last_response_id --------------------------

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

        let last = baseline(vec![item(0)]);
        let body = baseline(vec![item(0), item(1)]);

        // No last_response_id yet on a fresh connection => Full, regardless of last_body/body.
        assert_eq!(
            plan_request_for_conn(&conn, Some(&last), &body),
            RequestPlan::Full
        );
    }
}
