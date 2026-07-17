//! Upstream WebSocket transport (M5a). `conn` is the connection + codex-parity handshake
//! (Task 3); `codec` is the frame codec (Task 4: build `response.create`, re-serialize frames to
//! SSE, classify a received frame). The per-turn stream, delta planning, and `CodexWsExecutor`
//! land in later tasks alongside these as `turn`/`delta`/`executor` submodules.

pub mod codec;
pub mod conn;

pub use codec::{build_response_create, classify, frame_to_sse, FrameClass};
pub use conn::WsConn;
