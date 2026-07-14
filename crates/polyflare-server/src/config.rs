//! Process configuration for `polyflare serve`, read from environment. Secrets are NOT here —
//! per-account bearer tokens live in the store; only shared base URLs + data paths are config.

use std::path::{Path, PathBuf};
use std::time::Duration;

/// `serve` configuration. The upstream base URL is shared across accounts; per-account bearer
/// tokens are decrypted from the store per request.
pub struct ServeConfig {
    pub bind_addr: String,
    pub upstream_base_url: String,
    pub auth_base_url: String,
    pub db_path: PathBuf,
    pub key_path: PathBuf,
    pub continuity_watchdog: Duration,
}

impl ServeConfig {
    pub fn from_env() -> Result<Self, String> {
        let bind_addr =
            std::env::var("POLYFLARE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
        let upstream_base_url = std::env::var("POLYFLARE_UPSTREAM_URL")
            .map_err(|_| "POLYFLARE_UPSTREAM_URL not set".to_string())?;
        let auth_base_url = std::env::var("POLYFLARE_AUTH_URL")
            .unwrap_or_else(|_| "https://auth.openai.com".to_string());
        let data_dir = data_dir_from_env();
        let continuity_watchdog = std::env::var("POLYFLARE_WATCHDOG_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(30));
        Ok(ServeConfig {
            bind_addr,
            upstream_base_url,
            auth_base_url,
            db_path: db_path(&data_dir),
            key_path: key_path(&data_dir),
            continuity_watchdog,
        })
    }
}

/// The PolyFlare data directory: `$POLYFLARE_DATA_DIR`, else `$HOME/.polyflare`.
pub fn data_dir_from_env() -> PathBuf {
    if let Ok(dir) = std::env::var("POLYFLARE_DATA_DIR") {
        return PathBuf::from(dir);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".polyflare")
}

/// The store DB path within a data directory.
pub fn db_path(data_dir: &Path) -> PathBuf {
    data_dir.join("store.db")
}

/// The at-rest key file path within a data directory (raw 32 bytes).
pub fn key_path(data_dir: &Path) -> PathBuf {
    data_dir.join("key")
}
