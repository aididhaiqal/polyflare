//! Content-safety lint (SPEC-M5 §3.4; flagged by the M3 review for whenever logging arrives).
//!
//! `PreparedRequest`/`Prepared`/`RecoveryPlan` (in `polyflare-core::types`), `Account` (both the
//! core routing type and `polyflare-store`'s), `PlainTokens`, and `RefreshedTokens` all already
//! carry hand-written REDACTING `Debug` impls (they print `<redacted>`/`***`/counts instead of
//! the real body or token). Those impls make `{:?}` on the WRAPPER type safe everywhere. The
//! actual risk this lint guards against is a call site bypassing that wrapper and `Debug`- or
//! `Display`-dumping the RAW field directly — e.g. `format!("{:?}", tokens.access_token)` or
//! `tracing::info!(?body)` — which would print the real secret/content in clear.
//!
//! This scans only NON-TEST code: by this repo's convention every `#[cfg(test)] mod tests { .. }`
//! block sits at the very end of its file, and those blocks legitimately call `{:?}` on
//! `Account`/`PreparedRequest`/etc. to PROVE the redaction works (see e.g.
//! `polyflare-core::types::tests::prepared_request_debug_redacts_body`) — that is the opposite of
//! a leak, so it must not be flagged. Everything before the first `#[cfg(test)]` in each file is
//! scanned for the forbidden patterns below.

use std::path::{Path, PathBuf};

/// Identifiers that, if directly `Debug`-captured (bypassing a redacting wrapper type), would
/// print raw secret or request/response content.
const FORBIDDEN_IDENTS: &[&str] = &[
    "body",
    "req",
    "prepared",
    "tokens",
    "access_token",
    "refresh_token",
    "bearer_token",
    "id_token",
];

/// An explicit escape hatch for a genuine future exception: a line containing this marker is
/// never flagged. Unused today — kept so an exception gets documented instead of the lint being
/// worked around by obfuscating the pattern.
const ALLOW_MARKER: &str = "content-safety-allow";

#[derive(Debug)]
struct Violation {
    file: PathBuf,
    line_no: usize,
    line: String,
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Whether `haystack` contains `needle` as a whole identifier (bounded by non-identifier bytes,
/// or start/end of string) anywhere in the line. Pure-byte boundary check (never casts a
/// (possibly multi-byte-UTF8-continuation) byte to `char`), so it's safe on non-ASCII lines —
/// this repo's doc comments use em dashes and curly quotes freely.
fn contains_word(haystack: &str, needle: &str) -> bool {
    let bytes = haystack.as_bytes();
    let mut start = 0;
    while let Some(pos) = haystack[start..].find(needle) {
        let idx = start + pos;
        let before_ok = idx == 0 || !is_ident_byte(bytes[idx - 1]);
        let after_idx = idx + needle.len();
        let after_ok = after_idx >= bytes.len() || !is_ident_byte(bytes[after_idx]);
        if before_ok && after_ok {
            return true;
        }
        start = idx + 1;
        if start >= haystack.len() {
            break;
        }
    }
    false
}

/// Whether `line` contains the tracing/log `Debug`-shorthand capture `?ident` (e.g. `?body`): a
/// `?` immediately followed by `ident` with a non-identifier byte (or end of line) right after.
/// No leading-boundary check is needed: valid Rust syntax never places an identifier character
/// directly after a `?` used as the try-operator — that exact position is only reachable via this
/// macro shorthand (`tracing`/`log`'s `field = ?value` sugar written as bare `?field`).
fn contains_debug_shorthand(line: &str, ident: &str) -> bool {
    let marker = format!("?{ident}");
    let bytes = line.as_bytes();
    let mut start = 0;
    while let Some(pos) = line[start..].find(&marker) {
        let idx = start + pos;
        let after_idx = idx + marker.len();
        let after_ok = after_idx >= bytes.len() || !is_ident_byte(bytes[after_idx]);
        if after_ok {
            return true;
        }
        start = idx + 1;
        if start >= line.len() {
            break;
        }
    }
    false
}

/// Scan the contents of one `.rs` file for forbidden direct-`Debug`/`Display` patterns. `file` is
/// used only to label violations.
fn scan(file: &Path, content: &str) -> Vec<Violation> {
    let prod_code = match content.find("#[cfg(test)]") {
        Some(idx) => &content[..idx],
        None => content,
    };

    let mut violations = Vec::new();
    for (i, raw_line) in prod_code.lines().enumerate() {
        let line = raw_line.trim();
        if line.starts_with("//") || line.contains(ALLOW_MARKER) {
            continue;
        }

        for &ident in FORBIDDEN_IDENTS {
            let brace_shorthand = format!("{{{ident}:?}}");
            let has_brace_shorthand = line.contains(&brace_shorthand);
            let has_debug_shorthand = contains_debug_shorthand(line, ident);
            // A bare `{:?}` placeholder in a format string, with the sensitive identifier used
            // anywhere else on the same line (its positional/named argument in a real log call).
            let has_positional = line.contains("{:?}") && contains_word(line, ident);

            if has_brace_shorthand || has_debug_shorthand || has_positional {
                violations.push(Violation {
                    file: file.to_path_buf(),
                    line_no: i + 1,
                    line: raw_line.to_string(),
                });
                break; // one violation per line is enough signal
            }
        }
    }
    violations
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("polyflare-server's manifest dir is <root>/crates/polyflare-server")
        .to_path_buf()
}

/// All `.rs` files under every `crates/*/src` directory, recursively.
fn all_crate_src_files(workspace_root: &Path) -> Vec<PathBuf> {
    let crates_dir = workspace_root.join("crates");
    let mut files = Vec::new();
    for entry in std::fs::read_dir(&crates_dir)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", crates_dir.display()))
    {
        let crate_dir = entry.unwrap().path();
        let src_dir = crate_dir.join("src");
        if src_dir.is_dir() {
            walk_rs_files(&src_dir, &mut files);
        }
    }
    files
}

fn walk_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in
        std::fs::read_dir(dir).unwrap_or_else(|e| panic!("failed to read {}: {e}", dir.display()))
    {
        let path = entry.unwrap().path();
        if path.is_dir() {
            walk_rs_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

#[test]
fn no_direct_debug_dump_of_body_or_secret_carrying_values_in_the_real_tree() {
    let root = workspace_root();
    let files = all_crate_src_files(&root);
    assert!(
        files.len() > 10,
        "sanity check: expected to find the workspace's crates/*/src files under {}, found {}",
        root.display(),
        files.len()
    );

    let mut all_violations = Vec::new();
    for file in &files {
        let content = std::fs::read_to_string(file)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", file.display()));
        all_violations.extend(scan(file, &content));
    }

    assert!(
        all_violations.is_empty(),
        "content-safety lint found direct Debug-dumps of body/secret-carrying values:\n{}",
        all_violations
            .iter()
            .map(|v| format!("  {}:{}: {}", v.file.display(), v.line_no, v.line.trim()))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn lint_catches_an_injected_tracing_shorthand_body_leak() {
    let injected = r#"
        pub async fn handler(body: serde_json::Value) {
            tracing::info!(?body, "oops");
        }
    "#;
    let violations = scan(Path::new("<injected>"), injected);
    assert!(
        !violations.is_empty(),
        "lint must catch a `?body` tracing shorthand leak"
    );
}

#[test]
fn lint_catches_an_injected_brace_shorthand_prepared_leak() {
    let injected = r#"
        fn log(prepared: &Prepared) {
            println!("{prepared:?}");
        }
    "#;
    let violations = scan(Path::new("<injected>"), injected);
    assert!(
        !violations.is_empty(),
        "lint must catch a `{{prepared:?}}` brace-shorthand leak"
    );
}

#[test]
fn lint_catches_an_injected_positional_access_token_leak() {
    let injected = r#"
        fn log(tokens: &PlainTokens) {
            println!("token={:?}", tokens.access_token);
        }
    "#;
    let violations = scan(Path::new("<injected>"), injected);
    assert!(
        !violations.is_empty(),
        "lint must catch a positional `{{:?}}` access_token leak"
    );
}

#[test]
fn lint_does_not_flag_the_redacting_debug_impls_themselves() {
    // Mirrors `Account`'s real redacting `Debug` impl: it prints a placeholder literal, never
    // `{:?}` on the raw field, so this must NOT be flagged.
    let safe = r#"
        impl std::fmt::Debug for Account {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("Account")
                    .field("id", &self.id)
                    .field("bearer_token", &"***")
                    .finish()
            }
        }
    "#;
    let violations = scan(Path::new("<safe>"), safe);
    assert!(
        violations.is_empty(),
        "must not flag a redacting Debug impl: {violations:?}"
    );
}

#[test]
fn lint_does_not_flag_test_code_exercising_the_redaction() {
    // Mirrors the real `prepared_request_debug_redacts_body` test in
    // `polyflare-core::types::tests` — deliberately calls `{:?}` on `req` to prove the redaction
    // works. Must not be flagged: it comes after `#[cfg(test)]`.
    let with_tests = r#"
        fn production_code() {}

        #[cfg(test)]
        mod tests {
            #[test]
            fn redacts() {
                let req = PreparedRequest { body: serde_json::json!({}), model: "m".into() };
                let s = format!("{req:?}");
                assert!(s.contains("<redacted>"));
            }
        }
    "#;
    let violations = scan(Path::new("<with_tests>"), with_tests);
    assert!(
        violations.is_empty(),
        "must not flag test code that exercises (and proves) redaction: {violations:?}"
    );
}

#[test]
fn lint_does_not_flag_doc_comments_that_merely_mention_the_syntax() {
    // Mirrors the real doc comments in `polyflare-core::types` (e.g. "must never be printed in
    // clear via `{:?}`") — these mention the syntax in prose but never actually invoke it.
    let commented = r#"
        // `body` carries the full user request/conversation content and must never be printed
        // in clear via `{:?}` (mirrors `Account`'s `bearer_token` redaction below).
        fn production_code() {}
    "#;
    let violations = scan(Path::new("<commented>"), commented);
    assert!(
        violations.is_empty(),
        "must not flag a comment merely mentioning `{{:?}}` syntax: {violations:?}"
    );
}
