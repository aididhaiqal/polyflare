# PolyFlare M1 — Skeleton + Codex Identity Pass-Through — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the PolyFlare cargo workspace and ship a running proxy that accepts an OpenAI-Responses request, routes it through the neutral core (identity translation), and streams the upstream SSE response back verbatim — proven end-to-end against a scriptable mock upstream.

**Architecture:** A single-binary tokio/axum server. Ingress decodes the client request → a `PreparedRequest` → a Codex `Executor` POSTs it to the upstream and returns the response body as a non-buffering byte `Stream` → axum relays that stream to the client as `text/event-stream`. The five core traits (`Translator`, `Executor`, `Selector`, `Continuity`, `Coordinator`) are defined now so later milestones plug into stable seams; M1 only *implements* `Executor` (Codex) and `Translator` (identity).

**Tech Stack:** Rust 2021, tokio, axum 0.8, reqwest 0.12 (`json` + `stream` features), serde/serde_json, futures-util, bytes, async-trait, thiserror.

**Design references:** `POLYFLARE-DESIGN.md` §3 (architecture), §4.2 (translator registry), §4.4 (executors/transport), §4.5 (server edge), §6 (testing). `DESIGN-DECISIONS.md` T2/E4/X1.

## Global Constraints

- **Language / runtime:** Rust edition 2021, stable toolchain, `tokio` async runtime, single binary named `polyflare`.
- **Workspace crates:** `polyflare-core`, `polyflare-codex`, `polyflare-anthropic`, `polyflare-store`, `polyflare-server`, plus `polyflare-testkit` (test-support). In M1, `polyflare-anthropic` and `polyflare-store` are compile-only stubs.
- **Five traits** live in `polyflare-core`: `Translator`, `Executor`, `Selector`, `Continuity`, `Coordinator`. Exact signatures are fixed in Task 3 and MUST NOT drift.
- **Streaming is non-negotiable:** the response body is relayed as a `Stream` of byte chunks. NEVER collect the whole upstream body into memory before returning (except inside tests, where collecting for assertions is fine).
- **Secrets:** never log or print bearer tokens, OAuth values, or cookies. Config reads them from env; they appear only in the outbound `Authorization` header.
- **M1 scope discipline (YAGNI):** identity Codex pass-through only. NO account pool, NO continuity logic, NO fingerprint byte-parity, NO WebSocket transport — those are M2–M5. M1's `Selector`/`Continuity`/`Coordinator` are trait *definitions* only (no real impls wired into the request path); the server uses a single account from config.
- **Every task ends with a green `cargo test` (or `cargo build` for the scaffold task) and a commit.**

---

## File structure (created across M1)

```
polyflare/
├── Cargo.toml                              # workspace
├── .gitignore
├── crates/
│   ├── polyflare-core/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                       # pub mod + re-exports
│   │       ├── format.rs                    # Format enum
│   │       ├── types.rs                     # PreparedRequest, Account, RequestCtx, ExecError, ResponseStream
│   │       ├── translate.rs                 # Translator trait, IdentityTranslator, TranslatorRegistry
│   │       └── traits.rs                    # Executor, Selector, Continuity, Coordinator
│   ├── polyflare-codex/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       └── executor.rs                  # CodexExecutor (impl Executor)
│   ├── polyflare-anthropic/
│   │   ├── Cargo.toml
│   │   └── src/lib.rs                       # stub
│   ├── polyflare-store/
│   │   ├── Cargo.toml
│   │   └── src/lib.rs                       # stub
│   ├── polyflare-testkit/
│   │   ├── Cargo.toml
│   │   └── src/lib.rs                       # MockUpstream (scriptable SSE test server)
│   └── polyflare-server/
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs                       # pub mod app, ingress, config
│           ├── app.rs                       # AppState, build_app()
│           ├── ingress.rs                   # responses_handler
│           ├── config.rs                    # Config::from_env()
│           ├── main.rs                      # binary entrypoint
│           └── ../tests/e2e_passthrough.rs  # e2e integration test
```

---

## Task 1: Scaffold the cargo workspace + git init

**Files:**
- Create: `polyflare/Cargo.toml`, `polyflare/.gitignore`
- Create: `polyflare/crates/polyflare-core/Cargo.toml` + `src/lib.rs`
- Create: `polyflare/crates/polyflare-codex/Cargo.toml` + `src/lib.rs`
- Create: `polyflare/crates/polyflare-anthropic/Cargo.toml` + `src/lib.rs`
- Create: `polyflare/crates/polyflare-store/Cargo.toml` + `src/lib.rs`
- Create: `polyflare/crates/polyflare-testkit/Cargo.toml` + `src/lib.rs`
- Create: `polyflare/crates/polyflare-server/Cargo.toml` + `src/lib.rs` + `src/main.rs`

**Interfaces:**
- Produces: a compiling workspace under git. No public API yet beyond empty stubs.

- [ ] **Step 1: Create the workspace manifest**

`polyflare/Cargo.toml`:
```toml
[workspace]
resolver = "2"
members = [
    "crates/polyflare-core",
    "crates/polyflare-codex",
    "crates/polyflare-anthropic",
    "crates/polyflare-store",
    "crates/polyflare-testkit",
    "crates/polyflare-server",
]

[workspace.package]
edition = "2021"
version = "0.1.0"
license = "MIT"

[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
axum = "0.8"
reqwest = { version = "0.12", features = ["json", "stream"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
futures-core = "0.3"
futures-util = "0.3"
bytes = "1"
async-trait = "0.1"
thiserror = "2"
```

- [ ] **Step 2: Create `.gitignore`**

`polyflare/.gitignore`:
```gitignore
/target
**/*.rs.bk
Cargo.lock
.DS_Store
```
(Keep `Cargo.lock` ignored for now — this is a library-heavy workspace in flux; a later milestone will decide whether to commit it once a binary release is cut.)

- [ ] **Step 3: Create the two stub crates (`polyflare-anthropic`, `polyflare-store`)**

`polyflare/crates/polyflare-anthropic/Cargo.toml`:
```toml
[package]
name = "polyflare-anthropic"
version.workspace = true
edition.workspace = true
license.workspace = true
```
`polyflare/crates/polyflare-anthropic/src/lib.rs`:
```rust
//! Anthropic backend executor + rate-limit semantics. Stub in M1 (built out in M4).
```
`polyflare/crates/polyflare-store/Cargo.toml`:
```toml
[package]
name = "polyflare-store"
version.workspace = true
edition.workspace = true
license.workspace = true
```
`polyflare/crates/polyflare-store/src/lib.rs`:
```rust
//! SQLite persistence + at-rest crypto. Stub in M1 (built out in M2).
```

- [ ] **Step 4: Create the remaining crate manifests + empty lib/main stubs**

`polyflare/crates/polyflare-core/Cargo.toml`:
```toml
[package]
name = "polyflare-core"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
serde = { workspace = true }
serde_json = { workspace = true }
futures-core = { workspace = true }
bytes = { workspace = true }
async-trait = { workspace = true }
thiserror = { workspace = true }
```
`polyflare/crates/polyflare-core/src/lib.rs`:
```rust
//! PolyFlare neutral core: formats, translator registry, and the trait spine.
```
`polyflare/crates/polyflare-codex/Cargo.toml`:
```toml
[package]
name = "polyflare-codex"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
polyflare-core = { path = "../polyflare-core" }
reqwest = { workspace = true }
futures-util = { workspace = true }
bytes = { workspace = true }
async-trait = { workspace = true }

[dev-dependencies]
tokio = { workspace = true }
serde_json = { workspace = true }
polyflare-testkit = { path = "../polyflare-testkit" }
```
`polyflare/crates/polyflare-codex/src/lib.rs`:
```rust
//! Codex backend: WS/SSE transport, fingerprint laundering, continuity. M1 = SSE identity pass-through.
```
`polyflare/crates/polyflare-testkit/Cargo.toml`:
```toml
[package]
name = "polyflare-testkit"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
axum = { workspace = true }
tokio = { workspace = true }
serde_json = { workspace = true }
futures-util = { workspace = true }
```
`polyflare/crates/polyflare-testkit/src/lib.rs`:
```rust
//! Test support: scriptable mock upstreams for e2e tests.
```
`polyflare/crates/polyflare-server/Cargo.toml`:
```toml
[package]
name = "polyflare-server"
version.workspace = true
edition.workspace = true
license.workspace = true

[lib]
name = "polyflare_server"
path = "src/lib.rs"

[[bin]]
name = "polyflare"
path = "src/main.rs"

[dependencies]
polyflare-core = { path = "../polyflare-core" }
polyflare-codex = { path = "../polyflare-codex" }
axum = { workspace = true }
tokio = { workspace = true }
serde_json = { workspace = true }

[dev-dependencies]
polyflare-testkit = { path = "../polyflare-testkit" }
reqwest = { workspace = true }
futures-util = { workspace = true }
```
`polyflare/crates/polyflare-server/src/lib.rs`:
```rust
//! PolyFlare server edge: ingress, config, wiring. (Modules added in Task 6.)
```
`polyflare/crates/polyflare-server/src/main.rs`:
```rust
fn main() {
    // Real entrypoint wired in Task 6.
    println!("polyflare: not yet wired");
}
```

- [ ] **Step 5: Build the workspace to verify it compiles**

Run: `cd polyflare && cargo build`
Expected: `Finished` with no errors (dependencies download on first run).

- [ ] **Step 6: git init + first commit**

```bash
cd polyflare
git init
git add -A
git commit -m "chore: scaffold polyflare cargo workspace"
```

---

## Task 2: `Format` enum + identity translator registry

**Files:**
- Create: `polyflare/crates/polyflare-core/src/format.rs`
- Create: `polyflare/crates/polyflare-core/src/translate.rs`
- Modify: `polyflare/crates/polyflare-core/src/lib.rs`

**Interfaces:**
- Produces:
  - `enum Format { OpenAIResponses, AnthropicMessages }` (Copy, Eq, Hash)
  - `trait Translator: Send + Sync { fn translate_request(&self, body: serde_json::Value) -> serde_json::Value; fn translate_response_event(&self, event: serde_json::Value) -> serde_json::Value; }`
  - `struct IdentityTranslator` implementing `Translator` as pass-through
  - `struct TranslatorRegistry` with `with_defaults() -> Self`, `register(from, to, Box<dyn Translator>)`, `get(from, to) -> Option<&dyn Translator>`

- [ ] **Step 1: Write the failing test**

Append to `polyflare/crates/polyflare-core/src/translate.rs` (create the file with this test block at the bottom; the top of the file is written in Step 3):
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::Format;
    use serde_json::json;

    #[test]
    fn identity_registered_for_same_format_pairs() {
        let reg = TranslatorRegistry::with_defaults();
        assert!(reg.get(Format::OpenAIResponses, Format::OpenAIResponses).is_some());
        assert!(reg.get(Format::AnthropicMessages, Format::AnthropicMessages).is_some());
    }

    #[test]
    fn cross_format_pair_absent_in_m1() {
        let reg = TranslatorRegistry::with_defaults();
        assert!(reg.get(Format::AnthropicMessages, Format::OpenAIResponses).is_none());
    }

    #[test]
    fn identity_translator_passes_through() {
        let t = IdentityTranslator;
        let body = json!({"model": "gpt-5.6-sol", "input": "hi"});
        assert_eq!(t.translate_request(body.clone()), body);
        let ev = json!({"type": "response.completed"});
        assert_eq!(t.translate_response_event(ev.clone()), ev);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd polyflare && cargo test -p polyflare-core translate`
Expected: FAIL — compile errors (`Format`, `TranslatorRegistry`, `IdentityTranslator` not found).

- [ ] **Step 3: Implement `Format`**

`polyflare/crates/polyflare-core/src/format.rs`:
```rust
//! Wire formats PolyFlare can speak on ingress and to backends.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Format {
    /// OpenAI Responses API (the Codex CLI's native wire format).
    OpenAIResponses,
    /// Anthropic Messages API (Claude / Claude Code).
    AnthropicMessages,
}
```

- [ ] **Step 4: Implement the translator registry**

Prepend to `polyflare/crates/polyflare-core/src/translate.rs` (above the `#[cfg(test)]` block from Step 1):
```rust
//! Translator registry: the multi-provider spine. Same-format pairs are identity (zero cost).

use std::collections::HashMap;

use serde_json::Value;

use crate::format::Format;

/// Translates request and streaming-response JSON between two wire formats.
pub trait Translator: Send + Sync {
    fn translate_request(&self, body: Value) -> Value;
    fn translate_response_event(&self, event: Value) -> Value;
}

/// Pass-through translator used for same-format `(F, F)` pairs.
pub struct IdentityTranslator;

impl Translator for IdentityTranslator {
    fn translate_request(&self, body: Value) -> Value {
        body
    }
    fn translate_response_event(&self, event: Value) -> Value {
        event
    }
}

/// Registry keyed by `(from, to)` format. M1 registers only identity pairs.
pub struct TranslatorRegistry {
    map: HashMap<(Format, Format), Box<dyn Translator>>,
}

impl TranslatorRegistry {
    pub fn new() -> Self {
        Self { map: HashMap::new() }
    }

    /// Registry with the M1 defaults: identity for the two native same-format pairs.
    pub fn with_defaults() -> Self {
        let mut reg = Self::new();
        reg.register(
            Format::OpenAIResponses,
            Format::OpenAIResponses,
            Box::new(IdentityTranslator),
        );
        reg.register(
            Format::AnthropicMessages,
            Format::AnthropicMessages,
            Box::new(IdentityTranslator),
        );
        reg
    }

    pub fn register(&mut self, from: Format, to: Format, translator: Box<dyn Translator>) {
        self.map.insert((from, to), translator);
    }

    pub fn get(&self, from: Format, to: Format) -> Option<&dyn Translator> {
        self.map.get(&(from, to)).map(|b| b.as_ref())
    }
}

impl Default for TranslatorRegistry {
    fn default() -> Self {
        Self::with_defaults()
    }
}
```

- [ ] **Step 5: Wire the modules into `lib.rs`**

`polyflare/crates/polyflare-core/src/lib.rs`:
```rust
//! PolyFlare neutral core: formats, translator registry, and the trait spine.

pub mod format;
pub mod translate;

pub use format::Format;
pub use translate::{IdentityTranslator, Translator, TranslatorRegistry};
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd polyflare && cargo test -p polyflare-core translate`
Expected: PASS (3 tests).

- [ ] **Step 7: Commit**

```bash
git add crates/polyflare-core
git commit -m "feat(core): Format enum + identity translator registry"
```

---

## Task 3: Core types + the five trait definitions

**Files:**
- Create: `polyflare/crates/polyflare-core/src/types.rs`
- Create: `polyflare/crates/polyflare-core/src/traits.rs`
- Modify: `polyflare/crates/polyflare-core/src/lib.rs`

**Interfaces:**
- Consumes: nothing (foundational).
- Produces (these signatures are FIXED for all later milestones):
  - `struct PreparedRequest { pub body: serde_json::Value, pub model: String }`
  - `enum ExecError` (`Upstream(String)`, `Stream(String)`), `impl std::error::Error`
  - `type ResponseStream = Pin<Box<dyn Stream<Item = Result<Bytes, ExecError>> + Send>>`
  - `struct Account { pub id: String, pub base_url: String, pub bearer_token: String }`
  - `struct RequestCtx { pub session_id: Option<String> }` (Default)
  - `#[async_trait] trait Executor: Send + Sync { async fn execute(&self, req: PreparedRequest, account: &Account) -> Result<ResponseStream, ExecError>; }`
  - `trait Selector: Send + Sync { fn pick<'a>(&self, pool: &'a [Account], ctx: &RequestCtx) -> Option<&'a Account>; }`
  - `trait Continuity: Send + Sync { fn prepare(&self, req: PreparedRequest, ctx: &RequestCtx) -> PreparedRequest; }`
  - `trait Coordinator: Send + Sync { fn admit(&self, ctx: &RequestCtx) -> bool; }`

- [ ] **Step 1: Write the failing test**

Create `polyflare/crates/polyflare-core/src/traits.rs` with ONLY this test block for now (the trait defs come in Step 3):
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Account, RequestCtx};

    // A trivial Selector proves the trait is object-safe and usable.
    struct FirstAccount;
    impl Selector for FirstAccount {
        fn pick<'a>(&self, pool: &'a [Account], _ctx: &RequestCtx) -> Option<&'a Account> {
            pool.first()
        }
    }

    #[test]
    fn selector_picks_first_account() {
        let pool = vec![
            Account { id: "a".into(), base_url: "http://x".into(), bearer_token: "t".into() },
            Account { id: "b".into(), base_url: "http://y".into(), bearer_token: "u".into() },
        ];
        let sel: Box<dyn Selector> = Box::new(FirstAccount);
        let picked = sel.pick(&pool, &RequestCtx::default()).unwrap();
        assert_eq!(picked.id, "a");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd polyflare && cargo test -p polyflare-core traits`
Expected: FAIL — compile errors (`Selector`, `Account`, `RequestCtx` not found).

- [ ] **Step 3: Implement the core types**

`polyflare/crates/polyflare-core/src/types.rs`:
```rust
//! Core value types threaded through the request path.

use std::pin::Pin;

use bytes::Bytes;
use futures_core::Stream;

/// A request prepared for a specific backend. In M1 this is a thin wrapper over the
/// raw request JSON plus the target model; continuity/translation enrich it later.
#[derive(Debug, Clone)]
pub struct PreparedRequest {
    pub body: serde_json::Value,
    pub model: String,
}

/// Errors an executor can surface.
#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("upstream request failed: {0}")]
    Upstream(String),
    #[error("stream error: {0}")]
    Stream(String),
}

/// A non-buffering streaming response body: pinned, boxed, `Send` stream of byte chunks.
pub type ResponseStream = Pin<Box<dyn Stream<Item = Result<Bytes, ExecError>> + Send>>;

/// A credential/endpoint an executor uses to reach an upstream. M1 = single account from config.
#[derive(Debug, Clone)]
pub struct Account {
    pub id: String,
    pub base_url: String,
    pub bearer_token: String,
}

/// Per-request context threaded through selection/continuity. Minimal in M1.
#[derive(Debug, Clone, Default)]
pub struct RequestCtx {
    pub session_id: Option<String>,
}
```

- [ ] **Step 4: Implement the trait definitions**

Prepend to `polyflare/crates/polyflare-core/src/traits.rs` (above the `#[cfg(test)]` block):
```rust
//! The five trait seams. M1 implements only `Executor` (in `polyflare-codex`);
//! `Selector`/`Continuity`/`Coordinator` are defined here and fleshed out in M2/M3.

use async_trait::async_trait;

use crate::types::{Account, ExecError, PreparedRequest, RequestCtx, ResponseStream};

/// Executes a prepared request against an upstream using an account, returning a byte stream.
#[async_trait]
pub trait Executor: Send + Sync {
    async fn execute(
        &self,
        req: PreparedRequest,
        account: &Account,
    ) -> Result<ResponseStream, ExecError>;
}

/// Picks an account from a pool for a request. (Skeleton in M1; real scoring in M2.)
pub trait Selector: Send + Sync {
    fn pick<'a>(&self, pool: &'a [Account], ctx: &RequestCtx) -> Option<&'a Account>;
}

/// Applies continuity (anchor/trim/resend) before execution. (No-op in M1; state machine in M3.)
pub trait Continuity: Send + Sync {
    fn prepare(&self, req: PreparedRequest, ctx: &RequestCtx) -> PreparedRequest;
}

/// Coordinates session ownership + admission. (In-process pass in M1.)
pub trait Coordinator: Send + Sync {
    fn admit(&self, ctx: &RequestCtx) -> bool;
}
```

- [ ] **Step 5: Wire the modules into `lib.rs`**

Replace `polyflare/crates/polyflare-core/src/lib.rs` with:
```rust
//! PolyFlare neutral core: formats, translator registry, core types, and the trait spine.

pub mod format;
pub mod traits;
pub mod translate;
pub mod types;

pub use format::Format;
pub use traits::{Continuity, Coordinator, Executor, Selector};
pub use translate::{IdentityTranslator, Translator, TranslatorRegistry};
pub use types::{Account, ExecError, PreparedRequest, RequestCtx, ResponseStream};
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd polyflare && cargo test -p polyflare-core`
Expected: PASS (Task 2's 3 tests + Task 3's 1 test).

- [ ] **Step 7: Commit**

```bash
git add crates/polyflare-core
git commit -m "feat(core): core types + the five trait definitions"
```

---

## Task 4: Mock upstream test harness (`polyflare-testkit`)

**Files:**
- Modify: `polyflare/crates/polyflare-testkit/src/lib.rs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `struct MockUpstream` (Clone) with:
    - `fn new(events: Vec<String>) -> Self` — `events` are SSE `data:` payloads emitted in order
    - `async fn spawn(self) -> String` — binds an ephemeral port, serves `POST /responses`, returns the base URL (e.g. `http://127.0.0.1:54xxx`)
    - `fn last_body(&self) -> Option<serde_json::Value>` — the JSON body of the last received request

- [ ] **Step 1: Write the failing test**

Append to `polyflare/crates/polyflare-testkit/src/lib.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_emits_events_and_records_body() {
        let mock = MockUpstream::new(vec!["one".to_string(), "two".to_string()]);
        let handle = mock.clone();
        let base = mock.spawn().await;

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{base}/responses"))
            .json(&serde_json::json!({"model": "gpt-5.6-sol"}))
            .send()
            .await
            .unwrap();
        let text = resp.text().await.unwrap();

        assert!(text.contains("data: one"));
        assert!(text.contains("data: two"));
        assert_eq!(handle.last_body().unwrap()["model"], "gpt-5.6-sol");
    }
}
```

Add a dev-dependency on reqwest for this test. Append to `polyflare/crates/polyflare-testkit/Cargo.toml`:
```toml
[dev-dependencies]
reqwest = { workspace = true }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd polyflare && cargo test -p polyflare-testkit`
Expected: FAIL — compile errors (`MockUpstream` not found).

- [ ] **Step 3: Implement `MockUpstream`**

Prepend to `polyflare/crates/polyflare-testkit/src/lib.rs` (above the `#[cfg(test)]` block, replacing the stub doc line):
```rust
//! Test support: scriptable mock upstreams for e2e tests.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::extract::{Json, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::post;
use axum::Router;
use futures_util::stream::{self, Stream};
use tokio::net::TcpListener;

/// A scriptable mock upstream: serves `POST /responses`, records the request body,
/// and streams back a fixed list of SSE `data:` payloads.
#[derive(Clone)]
pub struct MockUpstream {
    events: Arc<Vec<String>>,
    last_body: Arc<Mutex<Option<serde_json::Value>>>,
}

impl MockUpstream {
    pub fn new(events: Vec<String>) -> Self {
        Self {
            events: Arc::new(events),
            last_body: Arc::new(Mutex::new(None)),
        }
    }

    /// The JSON body of the most recent request, if any.
    pub fn last_body(&self) -> Option<serde_json::Value> {
        self.last_body.lock().unwrap().clone()
    }

    /// Bind an ephemeral port, serve in a background task, and return the base URL.
    pub async fn spawn(self) -> String {
        let app = Router::new()
            .route("/responses", post(handler))
            .with_state(self);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }
}

async fn handler(
    State(mock): State<MockUpstream>,
    Json(body): Json<serde_json::Value>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    *mock.last_body.lock().unwrap() = Some(body);
    let events = (*mock.events).clone();
    let stream = stream::iter(events.into_iter().map(|d| Ok(Event::default().data(d))));
    Sse::new(stream).keep_alive(KeepAlive::default())
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd polyflare && cargo test -p polyflare-testkit`
Expected: PASS (1 test).

- [ ] **Step 5: Commit**

```bash
git add crates/polyflare-testkit
git commit -m "test(testkit): scriptable mock upstream SSE server"
```

---

## Task 5: Codex executor (`polyflare-codex`)

**Files:**
- Create: `polyflare/crates/polyflare-codex/src/executor.rs`
- Modify: `polyflare/crates/polyflare-codex/src/lib.rs`
- Create: `polyflare/crates/polyflare-codex/tests/executor_stream.rs`

**Interfaces:**
- Consumes: `polyflare_core::{Executor, PreparedRequest, Account, ExecError, ResponseStream}`; `polyflare_testkit::MockUpstream` (dev).
- Produces:
  - `struct CodexExecutor` with `fn new() -> Result<Self, ExecError>`
  - `impl Executor for CodexExecutor` — POSTs `req.body` to `{account.base_url}/responses` with bearer auth and returns `response.bytes_stream()` mapped to `ResponseStream` (no buffering).

- [ ] **Step 1: Write the failing test**

`polyflare/crates/polyflare-codex/tests/executor_stream.rs`:
```rust
use futures_util::StreamExt;
use polyflare_codex::executor::CodexExecutor;
use polyflare_core::{Account, Executor, PreparedRequest};
use polyflare_testkit::MockUpstream;

#[tokio::test]
async fn executor_streams_upstream_events_and_forwards_body() {
    let mock = MockUpstream::new(vec![
        r#"{"type":"response.output_text.delta","delta":"hi"}"#.to_string(),
        r#"{"type":"response.completed"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let base = mock.spawn().await;

    let executor = CodexExecutor::new().unwrap();
    let account = Account {
        id: "test".into(),
        base_url: base,
        bearer_token: "test-token".into(),
    };
    let req = PreparedRequest {
        body: serde_json::json!({"model": "gpt-5.6-sol", "input": "hello"}),
        model: "gpt-5.6-sol".into(),
    };

    let mut stream = executor.execute(req, &account).await.unwrap();
    let mut collected = String::new();
    while let Some(chunk) = stream.next().await {
        collected.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }

    assert!(collected.contains("response.output_text.delta"));
    assert!(collected.contains("response.completed"));
    assert_eq!(handle.last_body().unwrap()["model"], "gpt-5.6-sol");
}

#[tokio::test]
async fn executor_surfaces_upstream_error_status() {
    // No route for this path on the mock → 404 → ExecError::Upstream.
    let base = MockUpstream::new(vec![]).spawn().await;
    let executor = CodexExecutor::new().unwrap();
    let account = Account {
        id: "test".into(),
        base_url: format!("{base}/nonexistent-base"),
        bearer_token: "t".into(),
    };
    let req = PreparedRequest {
        body: serde_json::json!({"model": "m"}),
        model: "m".into(),
    };
    // `.err().unwrap()` (not `.unwrap_err()`): the Ok side `ResponseStream`
    // is `Pin<Box<dyn Stream>>` and has no `Debug` impl, which `unwrap_err` requires.
    let err = executor.execute(req, &account).await.err().unwrap();
    assert!(matches!(err, polyflare_core::ExecError::Upstream(_)));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd polyflare && cargo test -p polyflare-codex`
Expected: FAIL — compile errors (`CodexExecutor` not found).

- [ ] **Step 3: Implement `CodexExecutor`**

`polyflare/crates/polyflare-codex/src/executor.rs`:
```rust
//! Codex backend executor. M1: HTTP-SSE identity pass-through (WS transport + byte-parity
//! fingerprint come in later milestones).

use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;

use polyflare_core::{Account, ExecError, Executor, PreparedRequest, ResponseStream};

pub struct CodexExecutor {
    client: reqwest::Client,
}

impl CodexExecutor {
    pub fn new() -> Result<Self, ExecError> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| ExecError::Upstream(e.to_string()))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl Executor for CodexExecutor {
    async fn execute(
        &self,
        req: PreparedRequest,
        account: &Account,
    ) -> Result<ResponseStream, ExecError> {
        let url = format!("{}/responses", account.base_url.trim_end_matches('/'));
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&account.bearer_token)
            // Minimal M1 laundering; full byte-parity fingerprint is M5.
            .header("user-agent", "codex_cli_rs")
            .json(&req.body)
            .send()
            .await
            .map_err(|e| ExecError::Upstream(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(ExecError::Upstream(format!("status {}", resp.status())));
        }

        let stream = resp
            .bytes_stream()
            .map(|chunk| chunk.map_err(|e| ExecError::Stream(e.to_string())));

        Ok(Box::pin(stream))
    }
}
```

- [ ] **Step 4: Export the module**

`polyflare/crates/polyflare-codex/src/lib.rs`:
```rust
//! Codex backend: WS/SSE transport, fingerprint laundering, continuity. M1 = SSE identity pass-through.

pub mod executor;

pub use executor::CodexExecutor;
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd polyflare && cargo test -p polyflare-codex`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add crates/polyflare-codex
git commit -m "feat(codex): SSE pass-through executor over reqwest bytes_stream"
```

---

## Task 6: axum ingress + app wiring + config (`polyflare-server`)

**Files:**
- Create: `polyflare/crates/polyflare-server/src/config.rs`
- Create: `polyflare/crates/polyflare-server/src/app.rs`
- Create: `polyflare/crates/polyflare-server/src/ingress.rs`
- Modify: `polyflare/crates/polyflare-server/src/lib.rs`
- Modify: `polyflare/crates/polyflare-server/src/main.rs`

**Interfaces:**
- Consumes: `polyflare_core::{Executor, Account, PreparedRequest}`; `polyflare_codex::CodexExecutor`.
- Produces:
  - `struct AppState { pub executor: Arc<dyn Executor>, pub account: Account }`
  - `fn build_app(state: Arc<AppState>) -> axum::Router`
  - `async fn responses_handler(State<Arc<AppState>>, Json<Value>) -> Response` — relays the executor stream as `text/event-stream`
  - `struct Config { pub bind_addr: String, pub account: Account }` with `fn from_env() -> Result<Self, String>`

- [ ] **Step 1: Write the failing test (handler-level, via the app router)**

`polyflare/crates/polyflare-server/tests/ingress_relays.rs`:
```rust
use std::sync::Arc;

use futures_util::StreamExt;
use polyflare_codex::CodexExecutor;
use polyflare_core::Account;
use polyflare_server::app::{build_app, AppState};
use polyflare_testkit::MockUpstream;

#[tokio::test]
async fn server_relays_upstream_stream_to_client() {
    let mock = MockUpstream::new(vec![
        r#"{"type":"response.output_text.delta","delta":"yo"}"#.to_string(),
        r#"{"type":"response.completed"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;

    let state = Arc::new(AppState {
        executor: Arc::new(CodexExecutor::new().unwrap()),
        account: Account { id: "a".into(), base_url: upstream, bearer_token: "tok".into() },
    });
    let app = build_app(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    let mut body = String::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }
    assert!(body.contains("response.output_text.delta"));
    assert!(body.contains("response.completed"));
    assert_eq!(handle.last_body().unwrap()["model"], "gpt-5.6-sol");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd polyflare && cargo test -p polyflare-server --test ingress_relays`
Expected: FAIL — compile errors (`build_app`, `AppState` not found).

- [ ] **Step 3: Implement config**

`polyflare/crates/polyflare-server/src/config.rs`:
```rust
//! Process configuration, read from environment. Secrets never logged.

use polyflare_core::Account;

pub struct Config {
    pub bind_addr: String,
    pub account: Account,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let bind_addr =
            std::env::var("POLYFLARE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
        let base_url = std::env::var("POLYFLARE_UPSTREAM_URL")
            .map_err(|_| "POLYFLARE_UPSTREAM_URL not set".to_string())?;
        let bearer_token = std::env::var("POLYFLARE_UPSTREAM_TOKEN")
            .map_err(|_| "POLYFLARE_UPSTREAM_TOKEN not set".to_string())?;
        Ok(Config {
            bind_addr,
            account: Account { id: "default".into(), base_url, bearer_token },
        })
    }
}
```

- [ ] **Step 4: Implement the ingress handler**

`polyflare/crates/polyflare-server/src/ingress.rs`:
```rust
//! Ingress: decode an OpenAI-Responses request and relay the executor's stream to the client.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Json, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

use polyflare_core::PreparedRequest;

use crate::app::AppState;

pub async fn responses_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let model = body
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_string();
    let req = PreparedRequest { body, model };

    // M1: single account from config; pool selection arrives in M2.
    match state.executor.execute(req, &state.account).await {
        Ok(stream) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from_stream(stream))
            .expect("valid response"),
        Err(e) => (StatusCode::BAD_GATEWAY, format!("upstream error: {e}")).into_response(),
    }
}
```

- [ ] **Step 5: Implement app state + router**

`polyflare/crates/polyflare-server/src/app.rs`:
```rust
//! Application state and router construction.

use std::sync::Arc;

use axum::routing::post;
use axum::Router;

use polyflare_core::{Account, Executor};

use crate::ingress::responses_handler;

pub struct AppState {
    pub executor: Arc<dyn Executor>,
    pub account: Account,
}

pub fn build_app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/responses", post(responses_handler))
        .with_state(state)
}
```

- [ ] **Step 6: Wire `lib.rs`**

`polyflare/crates/polyflare-server/src/lib.rs`:
```rust
//! PolyFlare server edge: ingress, config, wiring.

pub mod app;
pub mod config;
pub mod ingress;
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `cd polyflare && cargo test -p polyflare-server --test ingress_relays`
Expected: PASS (1 test).

- [ ] **Step 8: Commit**

```bash
git add crates/polyflare-server
git commit -m "feat(server): axum ingress relaying executor stream as text/event-stream"
```

---

## Task 7: Binary entrypoint + full e2e + manual smoke

**Files:**
- Modify: `polyflare/crates/polyflare-server/src/main.rs`
- Create: `polyflare/crates/polyflare-server/tests/e2e_passthrough.rs`
- Create: `polyflare/README.md`

**Interfaces:**
- Consumes: everything above.
- Produces: a runnable `polyflare` binary; a full-stack e2e test; run instructions.

- [ ] **Step 1: Write the failing e2e test (full stack, exercises `Config`-shaped wiring)**

`polyflare/crates/polyflare-server/tests/e2e_passthrough.rs`:
```rust
//! End-to-end: client → polyflare server → executor → mock upstream, streaming the whole way.

use std::sync::Arc;

use futures_util::StreamExt;
use polyflare_codex::CodexExecutor;
use polyflare_core::Account;
use polyflare_server::app::{build_app, AppState};
use polyflare_testkit::MockUpstream;

async fn spawn_polyflare(upstream: String) -> String {
    let state = Arc::new(AppState {
        executor: Arc::new(CodexExecutor::new().unwrap()),
        account: Account { id: "e2e".into(), base_url: upstream, bearer_token: "tok".into() },
    });
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
    format!("http://{addr}")
}

#[tokio::test]
async fn end_to_end_streaming_passthrough() {
    let mock = MockUpstream::new(vec![
        r#"{"type":"response.output_text.delta","delta":"a"}"#.to_string(),
        r#"{"type":"response.output_text.delta","delta":"b"}"#.to_string(),
        r#"{"type":"response.completed"}"#.to_string(),
    ]);
    let handle = mock.clone();
    let upstream = mock.spawn().await;
    let pf = spawn_polyflare(upstream).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{pf}/responses"))
        .json(&serde_json::json!({"model": "gpt-5.6-sol", "input": "hi"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let mut body = String::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        body.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
    }

    // All three upstream events relayed, in order, with the model forwarded upstream.
    let first = body.find("delta\":\"a").unwrap();
    let second = body.find("delta\":\"b").unwrap();
    let done = body.find("response.completed").unwrap();
    assert!(first < second && second < done);
    assert_eq!(handle.last_body().unwrap()["model"], "gpt-5.6-sol");
}
```

- [ ] **Step 2: Run the test to verify it passes** (it should compile — all deps exist from Task 6; this test asserts ordering)

Run: `cd polyflare && cargo test -p polyflare-server --test e2e_passthrough`
Expected: PASS (1 test). If it FAILS to compile, stop and reconcile against Task 6's `AppState`/`build_app` signatures.

- [ ] **Step 3: Implement the real binary entrypoint**

`polyflare/crates/polyflare-server/src/main.rs`:
```rust
//! PolyFlare binary entrypoint.

use std::sync::Arc;

use polyflare_codex::CodexExecutor;
use polyflare_server::app::{build_app, AppState};
use polyflare_server::config::Config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env()?;
    let executor = Arc::new(CodexExecutor::new()?);
    let state = Arc::new(AppState { executor, account: config.account });
    let app = build_app(state);

    let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;
    println!("polyflare listening on {}", config.bind_addr);
    axum::serve(listener, app).await?;
    Ok(())
}
```

- [ ] **Step 4: Verify the binary builds**

Run: `cd polyflare && cargo build --bin polyflare`
Expected: `Finished`.

- [ ] **Step 5: Write the README with run + smoke instructions**

`polyflare/README.md`:
```markdown
# PolyFlare

Multi-provider LLM-CLI load balancer (Rust). See `../rust-lb-design/POLYFLARE-DESIGN.md`.

## M1 — Codex identity pass-through

Run the whole test suite:

    cargo test

Run the proxy against a real Codex-compatible upstream:

    POLYFLARE_UPSTREAM_URL="https://<upstream-base>" \
    POLYFLARE_UPSTREAM_TOKEN="<oauth-bearer>" \
    POLYFLARE_BIND="127.0.0.1:8080" \
    cargo run --bin polyflare

Then POST an OpenAI-Responses request to `http://127.0.0.1:8080/responses`.
The response is relayed verbatim as `text/event-stream`.

> Secrets are read from env only and never logged.
```

- [ ] **Step 6: Run the full workspace test suite**

Run: `cd polyflare && cargo test`
Expected: PASS — all tests across `polyflare-core`, `polyflare-testkit`, `polyflare-codex`, `polyflare-server`.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(server): binary entrypoint + full e2e passthrough + README"
```

---

## Self-review (completed against the spec)

**Spec coverage (M1 scope only):**
- Neutral core + translator registry (§3, §4.2) → Tasks 2–3 ✅
- Five trait seams (§3) → Task 3 ✅
- Codex executor + SSE transport (§4.4) → Task 5 ✅ (WS transport + byte-parity fingerprint explicitly deferred to M5 per Global Constraints)
- Server ingress relaying a non-buffering stream (§4.5) → Tasks 6–7 ✅
- Mock-upstream e2e harness as a first-class deliverable (§6) → Tasks 4, 7 ✅
- Store/OAuth/crypto (§4.5, §7), pool `Selector` (§4.3), continuity (§4.1), `Anthropic→Codex` translator (§4.2), latency/fingerprint CI gates (§6) → **out of M1 scope by design** — M2–M5.

**Placeholder scan:** No TBD/TODO/"add error handling" — every code step contains complete code. ✅

**Type consistency:** `AppState { executor: Arc<dyn Executor>, account: Account }`, `build_app(Arc<AppState>) -> Router`, `Executor::execute(&self, PreparedRequest, &Account) -> Result<ResponseStream, ExecError>`, `MockUpstream::{new, spawn, last_body}`, `CodexExecutor::new()` — names/signatures match across Tasks 3→5→6→7. ✅

**Known API caveats to watch during execution (not blockers):**
- `axum::body::Body::from_stream` requires the stream's error (`ExecError`) to be `Into<Box<dyn Error + Send + Sync>>` — satisfied because `ExecError` derives `thiserror::Error` (→ `std::error::Error + Send + Sync + 'static`).
- `reqwest::Response::bytes_stream()` requires the `stream` feature (set in the workspace manifest, Task 1).
- If the toolchain rejects `#[async_trait]` object safety, confirm `async-trait` is imported in `polyflare-codex` (it is, Task 1 manifest).

---

## Execution handoff

Milestone 1 delivers a working, tested Codex identity pass-through proxy under git. Milestones 2–5 (store/selector, continuity, Anthropic translator, fingerprint/latency gates) each get their own spec→plan cycle, building on these seams.
