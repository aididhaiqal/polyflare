//! Per-thread transport pins: force ONE conversation onto HTTP-SSE while everything else keeps
//! WebSocket.
//!
//! `GET /responses` either upgrades to a WebSocket or answers `426 Upgrade Required`, and a 426 is
//! codex-rs's SOLE WS→HTTP fallback trigger (see `app.rs`'s router comment). That was a global
//! on/off flag: either every client gets WebSocket or none does. A pin makes the same decision per
//! thread, so a single conversation can be moved to SSE without downgrading the whole proxy.
//!
//! The motivating case (2026-07-26): the ChatGPT app generates a `cch` attestation header inside
//! `websocket_connection`, and that generation times out with `timeout_seconds=0` — an app-internal
//! IPC deadline nothing here can reach. It only bites threads large enough to force a compaction on
//! every turn, because compaction is what reaches that code path. Those threads become unusable on
//! WebSocket while every other thread is fine. Answering 426 for just that thread routes it around
//! the broken step; the operator keeps WebSocket everywhere else.
//!
//! Pins are stored as one settings row rather than a table: the expected population is a handful of
//! thread ids an operator is actively unsticking, and a migration would be more machinery than the
//! problem warrants.
//!
//! **Content-free.** A thread id is an opaque client identifier, never conversation text. Ids are
//! length-capped and character-restricted on the way in so a malformed value cannot bloat the
//! settings row or smuggle a delimiter.

use std::collections::BTreeSet;

/// Settings key holding the pinned thread ids, newline-separated.
pub(crate) const SSE_PINS_KEY: &str = "ws_transport_sse_pinned_threads";

/// Upper bound on a single id. Session ids are UUID-shaped; this is generous enough for a prefixed
/// variant while refusing anything that looks like a payload rather than an identifier.
const MAX_ID_LEN: usize = 128;

/// Upper bound on how many threads may be pinned at once. A pin is a temporary unsticking measure,
/// not a routing policy, and an unbounded list would grow a settings row without limit.
const MAX_PINS: usize = 64;

/// Whether `id` is shaped like a thread identifier this will store.
///
/// Deliberately strict: identifiers only, so the stored row cannot be used as free-form storage and
/// no value can contain the newline that separates entries.
pub(crate) fn is_valid_thread_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_ID_LEN
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Parse the stored value into a set. Unparseable or oversized entries are dropped rather than
/// failing the read: a corrupt row must not be able to take the WebSocket path down with it.
pub(crate) fn parse(raw: &str) -> BTreeSet<String> {
    raw.lines()
        .map(str::trim)
        .filter(|line| is_valid_thread_id(line))
        .take(MAX_PINS)
        .map(str::to_string)
        .collect()
}

/// Serialize a set back to the stored form.
pub(crate) fn serialize(pins: &BTreeSet<String>) -> String {
    pins.iter()
        .take(MAX_PINS)
        .cloned()
        .collect::<Vec<_>>()
        .join("\n")
}

/// Whether this thread should be answered `426` instead of upgraded.
pub(crate) fn is_pinned(pins: &BTreeSet<String>, session_id: Option<&str>) -> bool {
    session_id.is_some_and(|id| pins.contains(id))
}

/// Add an id, reporting whether the set changed. Refuses beyond [`MAX_PINS`].
pub(crate) fn insert(pins: &mut BTreeSet<String>, id: &str) -> Result<bool, &'static str> {
    if !is_valid_thread_id(id) {
        return Err("thread id must be 1-128 chars of [A-Za-z0-9_-]");
    }
    if !pins.contains(id) && pins.len() >= MAX_PINS {
        return Err("too many pinned threads");
    }
    Ok(pins.insert(id.to_string()))
}

/// Read the current pins. A storage failure yields an empty set: a settings read that fails must
/// not strand every thread on HTTP, and the WebSocket path is the safe default.
pub(crate) async fn pinned_threads(state: &crate::app::AppState) -> BTreeSet<String> {
    match state.store.settings().get_all().await {
        Ok(map) => map
            .get(SSE_PINS_KEY)
            .map(|raw| parse(raw))
            .unwrap_or_default(),
        Err(_) => BTreeSet::new(),
    }
}

/// Persist a pin set.
async fn store_pins(
    state: &crate::app::AppState,
    pins: &BTreeSet<String>,
) -> Result<(), polyflare_store::StoreError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    state
        .store
        .settings()
        .set(SSE_PINS_KEY, &serialize(pins), now)
        .await
}

#[derive(serde::Serialize)]
struct PinsView {
    /// Thread ids currently answered `426` at the WebSocket handshake.
    pinned_threads: Vec<String>,
}

#[derive(serde::Deserialize)]
pub struct PinBody {
    thread_id: String,
}

/// `GET /api/ws/sse-pins`
pub async fn list(
    axum::extract::State(state): axum::extract::State<std::sync::Arc<crate::app::AppState>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    axum::Json(PinsView {
        pinned_threads: pinned_threads(&state).await.into_iter().collect(),
    })
    .into_response()
}

/// `POST /api/ws/sse-pins` — pin one thread to HTTP-SSE.
pub async fn add(
    axum::extract::State(state): axum::extract::State<std::sync::Arc<crate::app::AppState>>,
    axum::Json(body): axum::Json<PinBody>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let id = body.thread_id.trim().to_string();
    let mut pins = pinned_threads(&state).await;
    if let Err(message) = insert(&mut pins, &id) {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({ "error": message })),
        )
            .into_response();
    }
    if store_pins(&state, &pins).await.is_err() {
        return axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    axum::Json(PinsView {
        pinned_threads: pins.into_iter().collect(),
    })
    .into_response()
}

/// `DELETE /api/ws/sse-pins/{thread_id}` — return the thread to WebSocket.
pub async fn remove(
    axum::extract::State(state): axum::extract::State<std::sync::Arc<crate::app::AppState>>,
    axum::extract::Path(thread_id): axum::extract::Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let mut pins = pinned_threads(&state).await;
    pins.remove(thread_id.trim());
    if store_pins(&state, &pins).await.is_err() {
        return axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    axum::Json(PinsView {
        pinned_threads: pins.into_iter().collect(),
    })
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_identifier_shaped_values_are_accepted() {
        assert!(is_valid_thread_id("019f96f4-d4e8-7751-87c9-beba24bb3330"));
        assert!(is_valid_thread_id("thread_1"));
        assert!(!is_valid_thread_id(""));
        // A newline would corrupt the separator; a space or quote suggests a payload, not an id.
        assert!(!is_valid_thread_id("a\nb"));
        assert!(!is_valid_thread_id("has space"));
        assert!(!is_valid_thread_id("../etc/passwd"));
        assert!(!is_valid_thread_id(&"x".repeat(MAX_ID_LEN + 1)));
    }

    #[test]
    fn a_corrupt_row_degrades_to_the_entries_it_can_read() {
        // A bad line must not fail the whole read: the WebSocket path would go down with it.
        let pins = parse("good-id\n\nhas space\nanother_id\n");
        assert_eq!(
            pins.iter().cloned().collect::<Vec<_>>(),
            vec!["another_id".to_string(), "good-id".to_string()]
        );
    }

    #[test]
    fn round_trips_through_the_stored_form() {
        let mut pins = BTreeSet::new();
        assert_eq!(insert(&mut pins, "thread-a"), Ok(true));
        assert_eq!(insert(&mut pins, "thread-a"), Ok(false), "idempotent");
        assert_eq!(insert(&mut pins, "thread-b"), Ok(true));
        assert_eq!(parse(&serialize(&pins)), pins);
    }

    #[test]
    fn only_a_pinned_thread_is_diverted() {
        let pins = parse("thread-a");
        assert!(is_pinned(&pins, Some("thread-a")));
        assert!(!is_pinned(&pins, Some("thread-b")));
        // No identifiable thread means no pin can apply: the default stays WebSocket, so a client
        // that sends no session header is never silently downgraded.
        assert!(!is_pinned(&pins, None));
    }

    #[test]
    fn the_pin_list_is_bounded() {
        let mut pins = BTreeSet::new();
        for i in 0..MAX_PINS {
            assert!(insert(&mut pins, &format!("thread-{i}")).is_ok());
        }
        assert_eq!(
            insert(&mut pins, "one-too-many"),
            Err("too many pinned threads")
        );
        // An already-present id is still accepted at the cap — it adds nothing.
        assert_eq!(insert(&mut pins, "thread-0"), Ok(false));
    }

    #[test]
    fn a_rejected_id_is_named_rather_than_silently_dropped() {
        let mut pins = BTreeSet::new();
        assert!(insert(&mut pins, "bad id").is_err());
        assert!(pins.is_empty());
    }
}
