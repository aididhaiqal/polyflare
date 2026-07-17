//! Upstream WebSocket transport (M5a). `conn` is the connection + codex-parity handshake
//! (Task 3, this task); the frame codec, per-turn stream, delta planning, and `CodexWsExecutor`
//! land in later tasks alongside it as `codec`/`turn`/`delta`/`executor` submodules.

pub mod conn;

pub use conn::WsConn;
