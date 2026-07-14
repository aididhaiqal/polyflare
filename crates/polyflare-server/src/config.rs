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
