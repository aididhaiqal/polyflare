//! Upstream WebSocket transport (M5a). `conn` is the connection + codex-parity handshake
//! (Task 3); `codec` is the frame codec (Task 4: build `response.create`, re-serialize frames to
//! SSE, classify a received frame); `delta` is the incremental-vs-full request planning (Task 6).
//! The per-turn stream and `CodexWsExecutor` land in later tasks alongside these as `turn`/
//! `executor` submodules.

pub mod codec;
pub mod conn;
pub mod delta;

pub use codec::{build_response_create, classify, frame_to_sse, FrameClass};
pub use conn::WsConn;
pub use delta::{plan_request, plan_request_for_conn, RequestPlan};
