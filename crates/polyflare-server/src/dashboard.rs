//! The dashboard UI: the built Vite app (`dashboard/dist`) baked into the binary via `rust-embed`
//! and served under `/dashboard`. Static assets are content-typed from their own extension; any
//! path that isn't a real asset falls back to `index.html` (single-page app). If `dashboard/dist`
//! is empty at build time the routes still compile — they just 404 with a "not built" hint.

use axum::extract::Path;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use rust_embed::RustEmbed;

/// The built dashboard bundle. `dashboard/dist` is produced by `bun run build` in that directory
/// and committed, so a Rust-only CI (no node toolchain) can still embed it.
#[derive(RustEmbed)]
#[folder = "dashboard/dist"]
struct DashboardAssets;

/// Serve one embedded asset by its bundle-relative path, or fall back to `index.html`.
fn serve(path: &str) -> Response {
    let path = path.trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    if let Some(file) = DashboardAssets::get(path) {
        // `mimetype()` (rust-embed's `mime-guess` feature) types each asset from its extension —
        // `text/css`, `text/javascript`, `text/html`, etc. Own it before moving `file.data`.
        let mime = file.metadata.mimetype().to_string();
        return ([(header::CONTENT_TYPE, mime)], file.data.into_owned()).into_response();
    }

    // Single-page-app fallback: an unknown path is a client route, not a real file → index.html.
    match DashboardAssets::get("index.html") {
        Some(index) => (
            [(header::CONTENT_TYPE, "text/html; charset=utf-8".to_string())],
            index.data.into_owned(),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            "dashboard not built (run `bun run build` in crates/polyflare-server/dashboard)",
        )
            .into_response(),
    }
}

/// `GET /dashboard` (and `/dashboard/`) — the SPA entrypoint (`index.html`).
pub async fn dashboard_index() -> Response {
    serve("index.html")
}

/// `GET /dashboard/{*path}` — a bundle asset (`assets/index-*.js`, `assets/index-*.css`, …), with
/// SPA fallback to `index.html` for anything unmatched.
pub async fn dashboard_asset(Path(path): Path<String>) -> Response {
    serve(&path)
}
