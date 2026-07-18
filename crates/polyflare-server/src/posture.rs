//! D18 Task 4: the bind-address-aware default posture for client-key enforcement on the proxy
//! surface (`/responses`, `/v1/messages`, `/{pool}/…`) — THE security-critical crux of D18. See
//! the "Global Constraints" section of
//! `docs/superpowers/plans/2026-07-18-d18-client-auth.md` for the authoritative decision table;
//! this module implements it as a single pure function, resolved ONCE at startup (in `serve()`,
//! `main.rs`) BEFORE `build_app` runs, so:
//! - the decision is never re-evaluated per request (no hot-path cost, no TOCTOU window where a
//!   key created mid-request-storm could flip enforcement mid-flight in a way that matters — the
//!   posture is a startup-time property of the process, not a live one),
//! - the posture logic itself is unit-testable WITHOUT a real server, a real `Store`, or a real
//!   socket bind — [`resolve_proxy_enforcement`] takes only already-resolved primitives
//!   (`has_keys: bool`, `bind_addr: &str`, `allow_override: bool`).
//!
//! **The three branches (verbatim from the plan's Global Constraints):**
//! 1. Any key exists in the store ⇒ `enforce = true`, unconditionally, on ANY bind (loopback or
//!    not). Once an operator has opted into key-based auth by creating a key, the proxy always
//!    demands one — there is no "loopback keys are optional" carve-out.
//! 2. No keys + LOOPBACK bind ⇒ `enforce = false` (today's zero-config local trust, preserved —
//!    this is the path every existing local dev / single-operator deployment takes unchanged).
//! 3. No keys + NON-LOOPBACK bind ⇒ `POLYFLARE_ALLOW_UNAUTHENTICATED_REMOTE=1` ⇒
//!    `enforce = false` + a LOUD startup `tracing::warn!`; otherwise the process REFUSES TO START
//!    (a clear `StartupError`, not a per-request 401 — an operator who bound non-locally without
//!    thinking about auth should find out at `polyflare serve` time, not after quota is already
//!    draining). This is the "0.0.0.0 + no keys = anonymous" hole the plan calls out explicitly:
//!    do NOT port only codex-lb's permissive half.
//!
//! **Fronting-proxy caveat (documented, not solved here):** a reverse proxy in front of PolyFlare
//! makes the socket peer loopback (`127.0.0.1`) even when the REAL callers are remote — this
//! posture has no way to see through that. An operator running behind a fronting proxy MUST opt
//! into key enforcement explicitly (`polyflare keys create`) rather than relying on the
//! bind-address default; the bind-address heuristic is a floor for the naive "I bound `0.0.0.0`
//! directly" case, not a complete network-topology-aware auth decision.

use std::fmt;
use std::net::SocketAddr;

/// `POLYFLARE_ALLOW_UNAUTHENTICATED_REMOTE` — the ONLY way to boot with client-key enforcement
/// off on a non-loopback bind with no keys configured. Read by `ServeConfig::from_env`
/// (`crate::config`) and threaded in as `allow_override`, never read directly here (this module
/// stays a pure function of its arguments — see the module doc's testability rationale). Any
/// value other than exactly `"1"` is treated as unset, mirroring `ServeConfig`'s other boolean env
/// vars' fail-safe-default convention (`POLYFLARE_LIVE_LOGS`, `POLYFLARE_WS_UPSTREAM`) — a typo
/// here must never accidentally grant the dangerous path.
pub const ALLOW_UNAUTHENTICATED_REMOTE_ENV: &str = "POLYFLARE_ALLOW_UNAUTHENTICATED_REMOTE";

/// The process refuses to start rather than serve the proxy surface anonymously on a non-loopback
/// bind. Carries a human-readable, operator-facing message (never any key material — there is
/// none to leak at this point in startup).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupError(pub String);

impl fmt::Display for StartupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for StartupError {}

/// Resolve whether the proxy surface requires a valid client API key
/// (`crate::auth::require_client_key`), from already-known startup inputs. See the module doc for
/// the full three-branch decision table.
///
/// - `has_keys`: does at least one row exist in `api_keys`, regardless of `enabled`? (See
///   `polyflare_store::ApiKeyRepo::count`.)
/// - `bind_addr`: the configured `POLYFLARE_BIND` socket address, e.g. `"127.0.0.1:8080"`,
///   `"0.0.0.0:8080"`, `"[::1]:8080"`.
/// - `allow_override`: `POLYFLARE_ALLOW_UNAUTHENTICATED_REMOTE=1`.
///
/// **Loopback detection — parse to a `SocketAddr`, then `IpAddr::is_loopback()`; never string
/// matching.** `bind_addr.parse::<SocketAddr>()` uniformly handles `127.0.0.1:P` (and the whole
/// `127.0.0.0/8` block — NOT just the single literal `127.0.0.1`), `[::1]:P`, and the unspecified
/// "all interfaces" forms `0.0.0.0:P` / `[::]:P`. `Ipv4Addr::is_loopback` / `Ipv6Addr::is_loopback`
/// already return `false` for the unspecified addresses without any special-casing needed here —
/// `0.0.0.0`/`::` is deliberately NOT loopback: it is the MOST dangerous bind of all (it includes
/// every remote-reachable interface too, in addition to loopback), so it correctly falls through
/// to the non-loopback branch. A `bind_addr` that fails to parse as a `SocketAddr` at all (e.g. an
/// unresolved hostname like `"localhost:8080"` — this function does no DNS resolution) is ALSO
/// treated as non-loopback: fail toward the safe/refusing branch, never toward silently-open. In
/// practice `TcpListener::bind` would reject a genuinely unparseable address before the process
/// ever gets this far in a way that matters, so this is a defensive default, not a load-bearing
/// parse path.
pub fn resolve_proxy_enforcement(
    has_keys: bool,
    bind_addr: &str,
    allow_override: bool,
) -> Result<bool, StartupError> {
    if has_keys {
        return Ok(true);
    }
    if bind_is_loopback(bind_addr) {
        return Ok(false);
    }
    if allow_override {
        tracing::warn!(
            bind = %bind_addr,
            "PROXY IS UNAUTHENTICATED AND BOUND NON-LOCALLY — anyone who can reach {bind_addr} \
             can spend your account quota. POLYFLARE_ALLOW_UNAUTHENTICATED_REMOTE=1 was set, so \
             this process is starting anyway. Run `polyflare keys create --label <s>` and unset \
             this override as soon as you can."
        );
        Ok(false)
    } else {
        Err(StartupError(format!(
            "bind {bind_addr} is non-local but no client API key exists; run `polyflare keys \
             create --label <s>` or set {ALLOW_UNAUTHENTICATED_REMOTE_ENV}=1"
        )))
    }
}

/// `true` iff `bind_addr` parses as a `SocketAddr` whose IP is a loopback address (the full
/// `127.0.0.0/8` block, or `::1`). Unparseable input and the unspecified "all interfaces"
/// addresses (`0.0.0.0`, `::`) both resolve to `false` — see [`resolve_proxy_enforcement`]'s doc
/// for why both of those must be treated as non-loopback (the safe/dangerous-respectively
/// defaults).
fn bind_is_loopback(bind_addr: &str) -> bool {
    bind_addr
        .parse::<SocketAddr>()
        .map(|s| s.ip().is_loopback())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_exist_enforces_on_any_bind() {
        assert_eq!(
            resolve_proxy_enforcement(true, "127.0.0.1:8080", false),
            Ok(true),
            "keys exist + loopback ⇒ still enforce"
        );
        assert_eq!(
            resolve_proxy_enforcement(true, "0.0.0.0:8080", false),
            Ok(true),
            "keys exist + non-loopback + no override ⇒ enforce (no refusal — keys ARE present)"
        );
        assert_eq!(
            resolve_proxy_enforcement(true, "0.0.0.0:8080", true),
            Ok(true),
            "keys exist + override set ⇒ still enforce; the override only matters when there are \
             no keys at all"
        );
    }

    #[test]
    fn no_keys_loopback_bind_is_open() {
        assert_eq!(
            resolve_proxy_enforcement(false, "127.0.0.1:8080", false),
            Ok(false)
        );
        assert_eq!(
            resolve_proxy_enforcement(false, "127.5.5.5:8080", false),
            Ok(false),
            "the whole 127.0.0.0/8 block is loopback, not just 127.0.0.1"
        );
        assert_eq!(
            resolve_proxy_enforcement(false, "[::1]:8080", false),
            Ok(false)
        );
    }

    #[test]
    fn no_keys_unspecified_bind_without_override_refuses() {
        let err = resolve_proxy_enforcement(false, "0.0.0.0:8080", false)
            .expect_err("0.0.0.0 with no keys and no override must refuse to start");
        assert!(
            err.0.contains("0.0.0.0:8080") && err.0.contains(ALLOW_UNAUTHENTICATED_REMOTE_ENV),
            "error must name the offending bind and the override env var, got: {err}"
        );
    }

    #[test]
    fn no_keys_ipv6_unspecified_bind_without_override_refuses() {
        assert!(
            resolve_proxy_enforcement(false, "[::]:8080", false).is_err(),
            "IPv6 unspecified (::) must be treated as non-loopback too"
        );
    }

    #[test]
    fn no_keys_non_loopback_bind_with_override_is_open() {
        assert_eq!(
            resolve_proxy_enforcement(false, "0.0.0.0:8080", true),
            Ok(false),
            "the override explicitly opts into an open, non-locally-bound proxy"
        );
    }

    #[test]
    fn no_keys_unparseable_bind_without_override_refuses() {
        // No DNS resolution is performed here — an unresolved hostname is treated as non-loopback
        // (fail toward the safe/refusing branch), never silently open.
        assert!(
            resolve_proxy_enforcement(false, "localhost:8080", false).is_err(),
            "an unparseable bind_addr must NOT be treated as loopback"
        );
    }

    /// The override branch must emit the loud startup warning — captured via a real `tracing`
    /// subscriber, mirroring `require_client_key_middleware.rs`'s sentinel-capture pattern.
    #[test]
    fn override_branch_emits_a_loud_warning() {
        use std::sync::{Arc, Mutex};

        #[derive(Clone, Default)]
        struct BufWriter(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for BufWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
            type Writer = BufWriter;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let buf = BufWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::WARN)
            .finish();

        let guard = tracing::subscriber::set_default(subscriber);
        let result = resolve_proxy_enforcement(false, "0.0.0.0:9999", true);
        drop(guard);

        assert_eq!(result, Ok(false));
        let captured = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(
            captured.contains("UNAUTHENTICATED") && captured.contains("0.0.0.0:9999"),
            "the override branch must emit a loud warning naming the bind, got: {captured:?}"
        );
    }

    /// The refuse branch must NOT warn (it errors instead — no server starts to be warned about).
    #[test]
    fn refuse_branch_does_not_warn() {
        use std::sync::{Arc, Mutex};

        #[derive(Clone, Default)]
        struct BufWriter(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for BufWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
            type Writer = BufWriter;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let buf = BufWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::WARN)
            .finish();

        let guard = tracing::subscriber::set_default(subscriber);
        let _ = resolve_proxy_enforcement(false, "0.0.0.0:9999", false);
        drop(guard);

        let captured = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(
            captured.is_empty(),
            "the refuse branch must not log anything, got: {captured:?}"
        );
    }
}
