//! M5 (T-rustls) TLS fingerprint-parity tests.
//!
//! `codex_client_hello_offers_x25519_mlkem768_hybrid_key_share` is the STRONG test from the M5
//! brief: a raw-TCP capture of the actual ClientHello bytes a Codex-executor-shaped `reqwest`
//! client puts on the wire, parsed just far enough to prove the post-quantum hybrid group
//! X25519MLKEM768 (IANA codepoint 0x11EC) is offered — the distinctive marker of rustls's
//! `prefer-post-quantum` feature under the aws-lc-rs provider, and thus of codex-rs's own
//! transport (see `executor.rs` module docs). This is chosen over the lighter
//! "assert the installed CryptoProvider is aws-lc-rs" fallback because a raw capture is fully
//! feasible here (no exotic dependency needed, ~100 lines of hand-rolled record/extension
//! walking) and is strictly stronger evidence: it proves the group is actually offered on the
//! wire, not just present in the provider's in-memory config.
//!
//! NOTE: this proves *structural* parity (rustls version + provider + PQ hybrid offered). Full
//! byte-for-byte parity against a REAL codex-rs capture (extension ordering, GREASE values,
//! cipher-suite list, etc.) is the fingerprint-parity GATE and is deferred — it needs a live
//! codex-rs capture to diff against, which isn't available in this environment.

use std::time::Duration;

use polyflare_codex::CodexExecutor;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

/// TLS extension type `supported_groups` (RFC 8446 §4.2.7).
const EXT_SUPPORTED_GROUPS: u16 = 0x000A;
/// TLS extension type `key_share` (RFC 8446 §4.2.8).
const EXT_KEY_SHARE: u16 = 0x0033;
/// The hybrid PQ/classical named group codepoint rustls's `prefer-post-quantum` feature offers
/// under the aws-lc-rs provider (IANA TLS Supported Groups registry).
const X25519_MLKEM768: u16 = 0x11EC;

#[tokio::test]
async fn codex_client_hello_offers_x25519_mlkem768_hybrid_key_share() {
    // Mirrors production init order: constructing a `CodexExecutor` installs aws-lc-rs as the
    // process-wide default rustls `CryptoProvider` (idempotently) before any TLS use. That
    // installation is process-global, so the plain client built below for the raw-bytes capture
    // picks up the same provider as long as it also forces the rustls backend.
    let _executor = CodexExecutor::new().expect("executor (and provider install) should succeed");

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().unwrap();

    // Built the same way as `CodexExecutor::new()`'s client: rustls backend forced explicitly.
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .connect_timeout(Duration::from_secs(5))
        .build()
        .expect("rustls-backed client should build");

    // Fire the handshake in the background. There's no real TLS server on the other end of this
    // listener, so the request always errors out once the handshake stalls — we only need the
    // ClientHello bytes the client already wrote before that happens.
    let url = format!("https://{addr}/");
    tokio::spawn(async move {
        let _ = client.get(&url).send().await;
    });

    let (mut socket, _peer) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("client should connect within 5s")
        .expect("accept should succeed");

    let record = read_full_tls_record(&mut socket).await;
    assert!(
        client_hello_offers_group(&record, X25519_MLKEM768),
        "ClientHello should offer X25519MLKEM768 (0x11EC) in supported_groups/key_share"
    );
}

/// Constructing a second `CodexExecutor` must not panic on the already-installed crypto
/// provider — `install_default()`'s second-call `Err` must be swallowed, not propagated. This is
/// also the "at minimum" test the M5 brief calls for: the executor builds successfully under the
/// new rustls config.
#[test]
fn codex_executor_new_is_idempotent_under_repeated_provider_install() {
    CodexExecutor::new().expect("first construction should succeed");
    CodexExecutor::new().expect("second construction must not panic on re-install");
}

/// Reads one TLS record (expected: the ClientHello handshake record) off `socket`. The 5-byte
/// record header carries the payload length, so this loops until the whole payload has arrived —
/// loopback delivery can split a single record across multiple `read` calls.
async fn read_full_tls_record(socket: &mut TcpStream) -> Vec<u8> {
    let mut header = [0u8; 5];
    read_exact_with_timeout(socket, &mut header).await;
    assert_eq!(
        header[0], 0x16,
        "expected a TLS handshake record (ClientHello), got record type {}",
        header[0]
    );
    let payload_len = u16::from_be_bytes([header[3], header[4]]) as usize;

    let mut record = header.to_vec();
    let mut payload = vec![0u8; payload_len];
    read_exact_with_timeout(socket, &mut payload).await;
    record.extend_from_slice(&payload);
    record
}

async fn read_exact_with_timeout(socket: &mut TcpStream, buf: &mut [u8]) {
    let mut filled = 0;
    while filled < buf.len() {
        let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut buf[filled..]))
            .await
            .expect("read should not time out")
            .expect("read should succeed");
        assert!(n > 0, "socket closed before the full record was read");
        filled += n;
    }
}

/// Hand-rolled walk of a ClientHello's extension list looking for `group` in either the
/// `supported_groups` list or a `key_share` entry. Not a general TLS parser — just enough
/// structure (record header, handshake header, the fixed ClientHello prefix, then extensions) to
/// reach those two extension types.
fn client_hello_offers_group(record: &[u8], group: u16) -> bool {
    // record[0..5]: TLS record header (type, legacy version, length) — already validated by the
    // caller. record[5..]: the handshake message.
    let handshake = &record[5..];
    assert_eq!(
        handshake[0], 0x01,
        "expected a ClientHello handshake message, got type {}",
        handshake[0]
    );
    let hs_len = u32::from_be_bytes([0, handshake[1], handshake[2], handshake[3]]) as usize;
    let ch = &handshake[4..4 + hs_len];

    let mut i = 0usize;
    i += 2 + 32; // legacy_version(2) + random(32)
    let session_id_len = ch[i] as usize;
    i += 1 + session_id_len;
    let cipher_suites_len = u16::from_be_bytes([ch[i], ch[i + 1]]) as usize;
    i += 2 + cipher_suites_len;
    let compression_len = ch[i] as usize;
    i += 1 + compression_len;
    let extensions_len = u16::from_be_bytes([ch[i], ch[i + 1]]) as usize;
    i += 2;
    let extensions_end = i + extensions_len;

    while i < extensions_end {
        let ext_type = u16::from_be_bytes([ch[i], ch[i + 1]]);
        let ext_len = u16::from_be_bytes([ch[i + 2], ch[i + 3]]) as usize;
        let ext_data = &ch[i + 4..i + 4 + ext_len];

        match ext_type {
            EXT_SUPPORTED_GROUPS => {
                let list = &ext_data[2..]; // skip the 2-byte list-length prefix
                if list
                    .chunks_exact(2)
                    .any(|g| u16::from_be_bytes([g[0], g[1]]) == group)
                {
                    return true;
                }
            }
            EXT_KEY_SHARE => {
                let shares_len = u16::from_be_bytes([ext_data[0], ext_data[1]]) as usize;
                let mut j = 2usize;
                let shares_end = 2 + shares_len;
                while j < shares_end {
                    let g = u16::from_be_bytes([ext_data[j], ext_data[j + 1]]);
                    let kx_len = u16::from_be_bytes([ext_data[j + 2], ext_data[j + 3]]) as usize;
                    if g == group {
                        return true;
                    }
                    j += 4 + kx_len;
                }
            }
            _ => {}
        }
        i += 4 + ext_len;
    }
    false
}
