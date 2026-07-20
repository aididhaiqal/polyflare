//! Upstream WebSocket transport (M5a). `conn` is the connection + codex-parity handshake
//! (Task 3); `codec` is the frame codec (Task 4: build `response.create`, re-serialize frames to
//! SSE, classify a received frame); `delta` is the incremental-vs-full request planning
//! (Task 6); `turn` is the per-turn stream that ends while the socket stays open (Task 5);
//! `executor` is `CodexWsExecutor` — the connection cache, bounded recovery, and error mapping
//! that make the transport swap real (Task 7).

pub mod codec;
pub mod conn;
pub mod delta;
pub mod executor;
pub mod turn;

pub use codec::{build_response_create, classify, frame_to_sse, FrameClass};
pub use conn::{dial_upstream, WsConn};
pub use delta::{
    item_hashes, non_input_fingerprint, plan_request, plan_request_for_conn, ItemHash, RequestPlan,
};
pub use executor::CodexWsExecutor;
pub use turn::{shared_conn, turn_stream, SharedWsConn};
