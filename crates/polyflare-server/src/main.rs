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
