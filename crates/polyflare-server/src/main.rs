//! PolyFlare binary entrypoint. Subcommands: `serve` (the M1 gateway) and `accounts import`
//! (the zero-re-auth codex-lb importer). Secrets are read from env / files and never logged.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Parser, Subcommand};

use polyflare_anthropic::AnthropicExecutor;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::{run_login, CodexVersionCache};
use polyflare_core::{Continuity, Executor, Selector};
use polyflare_server::app::{build_app, build_codex_executor, AppState};
use polyflare_server::config::{self, ServeConfig};
use polyflare_server::continuity::CodexContinuity;
use polyflare_store::{import_from_codex_lb, Account, PlainTokens, Store, TokenCipher};

#[derive(Parser)]
#[command(
    name = "polyflare",
    version,
    about = "Multi-provider LLM-CLI load balancer"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the gateway server (the M1 serve path).
    Serve,
    /// Account management.
    Accounts {
        #[command(subcommand)]
        command: AccountsCommands,
    },
}

#[derive(Subcommand)]
enum AccountsCommands {
    /// Import accounts + usage from a codex-lb store.db (zero re-auth).
    Import {
        /// Path to the codex-lb store.db (opened read-only). Import against a STOPPED codex-lb
        /// (or a copy of store.db): a live database in WAL mode opened read-only may fail to open
        /// or read stale data.
        #[arg(long = "from", value_name = "PATH")]
        from: PathBuf,
        /// Path to the codex-lb Fernet key file.
        #[arg(long = "fernet-key", value_name = "PATH")]
        fernet_key: PathBuf,
        /// Preview only: validate + report what would be imported, then roll back — writes nothing.
        #[arg(long = "dry-run")]
        dry_run: bool,
    },
    /// Onboard a Codex account via native OAuth login (authorization_code + PKCE). Prints a URL to
    /// open; catches the loopback callback locally, or paste the redirected URL when headless.
    Login {
        /// Also try to auto-open the authorize URL in a browser (default: print only, for headless).
        #[arg(long = "open")]
        open: bool,
        /// Assign the onboarded account to a named pool (URL slug). Omit for an unpooled account
        /// (reachable only via the bare `/responses` and `/v1/messages` paths).
        #[arg(long = "pool", value_name = "SLUG")]
        pool: Option<String>,
    },
    /// Assign (or clear) an existing account's pool. Pass `--pool <slug>` to tag it, or omit
    /// `--pool` to clear it back to unpooled.
    SetPool {
        /// The account id to re-pool (as shown by `accounts` listing / the dashboard).
        #[arg(long = "id", value_name = "ACCOUNT_ID")]
        id: String,
        /// The pool slug to assign; omit to clear the account's pool (make it unpooled).
        #[arg(long = "pool", value_name = "SLUG")]
        pool: Option<String>,
    },
    /// Mark (or unmark) an existing account as authorized for cyber/security work (TA6). This is
    /// the operator write path for `security_work_authorized` — otherwise only `insert` and the
    /// codex-lb importer can set it.
    SetCapability {
        /// The account id to flip (as shown by `accounts` listing / the dashboard).
        #[arg(long = "id", value_name = "ACCOUNT_ID")]
        id: String,
        /// `true` to authorize the account for cyber/security work, `false` to revoke it.
        #[arg(long = "security-work", value_name = "BOOL", action = clap::ArgAction::Set)]
        security_work: bool,
    },
}

/// Content-safe request logging (SPEC-M5 §3.4): env-filtered (`RUST_LOG`, default `info`) plain
/// `fmt` output. Never initialize a subscriber that echoes request/response bodies — the fields
/// logged are chosen entirely by `polyflare_server::observability::RequestLog`, not by anything
/// configured here.
fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let cli = Cli::parse();
    match cli.command {
        Commands::Serve => serve().await,
        Commands::Accounts { command } => match command {
            AccountsCommands::Import {
                from,
                fernet_key,
                dry_run,
            } => accounts_import(&from, &fernet_key, dry_run).await,
            AccountsCommands::Login { open, pool } => accounts_login(open, pool).await,
            AccountsCommands::SetPool { id, pool } => accounts_set_pool(&id, pool).await,
            AccountsCommands::SetCapability { id, security_work } => {
                accounts_set_capability(&id, security_work).await
            }
        },
    }
}

/// The M2b server: store-backed multi-account pool selection.
async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let config = ServeConfig::from_env()?;
    let store = Store::open(&config.db_path).await?;
    let cipher = TokenCipher::load_or_create(&config.key_path)?;
    // M5a: `POLYFLARE_WS_UPSTREAM` (default OFF) selects `CodexWsExecutor` over the WS transport
    // instead of today's HTTP-SSE `CodexExecutor` — see `build_codex_executor`'s doc.
    let codex_executor: Arc<dyn Executor> = build_codex_executor(config.ws_upstream)?;
    let anthropic_executor: Arc<dyn Executor> = Arc::new(AnthropicExecutor::new()?);
    // Routing: the global default strategy + any per-pool overrides, both from config.
    let selector: Arc<dyn Selector> = config.routing_strategy.selector();
    let pool_selectors: std::collections::HashMap<String, Arc<dyn Selector>> = config
        .pool_strategies
        .iter()
        .map(|(slug, strat)| (slug.clone(), strat.selector()))
        .collect();
    let oauth = OAuthClient::new(config.auth_base_url)?;
    let continuity: Arc<dyn Continuity> = Arc::new(CodexContinuity::new(
        store.continuity(),
        config.continuity_watchdog,
    ));

    // Resolves the live codex-rs release version for the synthesized egress User-Agent. Warmed
    // out-of-band on its TTL cadence so the request hot path (`cached_or_fallback`) never blocks on
    // the GitHub/npm fetch; until the first warm completes it serves the hardcoded version floor.
    let codex_version = Arc::new(CodexVersionCache::new()?);
    {
        let cache = codex_version.clone();
        tokio::spawn(async move {
            loop {
                cache.get_version().await;
                tokio::time::sleep(cache.refresh_interval()).await;
            }
        });
    }

    let state = Arc::new(AppState {
        codex_executor,
        anthropic_executor,
        selector,
        pool_selectors,
        continuity,
        store,
        cipher,
        oauth,
        upstream_base_url: config.upstream_base_url,
        anthropic_upstream_base_url: config.anthropic_upstream_base_url,
        refresh_locks: Default::default(),
        capture_fingerprint_path: config.capture_fingerprint_path,
        codex_version,
        account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Arc::new(polyflare_server::token_cache::TokenCache::new()),
        runtime: Default::default(),
        admin_token: config.admin_token,
        live_logs: config.live_logs,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        max_account_attempts: config.max_account_attempts,
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        starvation_wait_budget: config.starvation_wait_budget,
        starvation_heartbeat: config.starvation_heartbeat,
        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
    });
    // Runtime usage-refresh loop: keeps each Codex account's rate-limit windows (5h + weekly) and
    // routing gate live, instead of the frozen numbers the importer left.
    polyflare_server::usage_refresh::spawn_usage_refresh(state.clone());

    let app = build_app(state);

    let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;
    println!("polyflare listening on {}", config.bind_addr);
    axum::serve(listener, app).await?;
    Ok(())
}

/// Run the importer against the configured store + at-rest key, printing only counts. A `dry_run`
/// validates + reports what would be imported, then rolls back — writing nothing.
async fn accounts_import(
    from: &Path,
    fernet_key: &Path,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = config::data_dir_from_env();
    let db_path = config::db_path(&data_dir);
    let key_path = config::key_path(&data_dir);

    let store = Store::open(&db_path).await?;
    let cipher = TokenCipher::load_or_create(&key_path)?;
    let summary = import_from_codex_lb(&store, from, fernet_key, &cipher, dry_run).await?;
    let verb = if dry_run { "would import" } else { "imported" };
    println!(
        "{verb} {} account(s), {} usage row(s), and {} chat-log row(s){}",
        summary.accounts_imported,
        summary.usage_rows_imported,
        summary.request_logs_imported,
        if dry_run {
            " (dry run — nothing written)"
        } else {
            ""
        }
    );
    Ok(())
}

/// Onboard (or re-auth) a Codex account via native OAuth login, persisting its tokens to the store.
/// Prints only identity — never a token (the token types redact their `Debug`).
async fn accounts_login(
    open: bool,
    pool: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = config::data_dir_from_env();
    let store = Store::open(&config::db_path(&data_dir)).await?;
    let cipher = TokenCipher::load_or_create(&config::key_path(&data_dir))?;
    let auth_base = std::env::var("POLYFLARE_AUTH_URL")
        .unwrap_or_else(|_| "https://auth.openai.com".to_string());
    let oauth = OAuthClient::new(auth_base)?;

    let refreshed = run_login(&oauth, open).await?;
    let claims = refreshed
        .claims
        .ok_or("login succeeded but the id_token carried no decodable identity claims")?;
    let tokens = PlainTokens {
        access_token: refreshed.tokens.access_token,
        refresh_token: refreshed.tokens.refresh_token,
        id_token: refreshed.tokens.id_token,
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let repo = store.accounts();

    // Onboard vs re-auth: if this ChatGPT account already exists, refresh its tokens in place;
    // otherwise insert a new row. `update_tokens`/`insert` both bump the store generation, so the
    // running server's account cache picks the account up without a restart.
    let existing = match &claims.chatgpt_account_id {
        Some(cid) => repo.find_by_chatgpt_account_id(cid).await?,
        None => None,
    };
    let (account_id, verb) = if let Some(existing) = existing {
        repo.update_tokens(&existing.id, &tokens, &cipher, now)
            .await?;
        // Re-auth leaves the pool untouched UNLESS `--pool` was given: an explicit `--pool <slug>`
        // on a re-auth re-tags the account, while omitting it must never clear an existing pool.
        if pool.is_some() {
            repo.update_pool(&existing.id, pool.as_deref()).await?;
        }
        (existing.id, "re-authenticated")
    } else {
        let id = format!(
            "codex_{}",
            claims
                .chatgpt_account_id
                .clone()
                .or_else(|| claims.sub.clone())
                .unwrap_or_else(|| now.to_string())
        );
        let account = Account {
            id: id.clone(),
            chatgpt_account_id: claims.chatgpt_account_id.clone(),
            chatgpt_user_id: claims.chatgpt_user_id.clone(),
            email: claims.email.clone().unwrap_or_default(),
            alias: None,
            workspace_id: claims.workspace_id.clone(),
            workspace_label: claims.workspace_label.clone(),
            seat_type: claims.seat_type.clone(),
            plan_type: claims
                .chatgpt_plan_type
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
            routing_policy: "normal".to_string(),
            last_refresh: now,
            created_at: now,
            status: "active".to_string(),
            deactivation_reason: None,
            reset_at: None,
            blocked_at: None,
            security_work_authorized: false,
            provider: "codex".to_string(),
            pool: pool.clone(),
        };
        repo.insert(&account, &tokens, &cipher).await?;
        (id, "onboarded")
    };
    println!(
        "{verb} {} (account {})",
        claims.email.as_deref().unwrap_or("<no email>"),
        account_id
    );
    Ok(())
}

/// Assign (or clear) an existing account's pool. `update_pool` bumps the store generation, so a
/// running server's account cache picks the re-pooling up without a restart. Prints only the id +
/// the resulting pool — never any secret.
async fn accounts_set_pool(
    id: &str,
    pool: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = config::data_dir_from_env();
    let store = Store::open(&config::db_path(&data_dir)).await?;
    let repo = store.accounts();
    if repo.get(id).await?.is_none() {
        return Err(format!("no account with id {id}").into());
    }
    repo.update_pool(id, pool.as_deref()).await?;
    match pool.as_deref() {
        Some(slug) => println!("account {id} assigned to pool {slug}"),
        None => println!("account {id} pool cleared (unpooled)"),
    }
    Ok(())
}

async fn accounts_set_capability(
    id: &str,
    security_work: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = config::data_dir_from_env();
    let store = Store::open(&config::db_path(&data_dir)).await?;
    let repo = store.accounts();
    if repo.get(id).await?.is_none() {
        return Err(format!("no account with id {id}").into());
    }
    repo.update_security_work_authorized(id, security_work)
        .await?;
    if security_work {
        println!("account {id} authorized for cyber/security work");
    } else {
        println!("account {id} cyber/security-work authorization revoked");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn parses_serve() {
        let cli = Cli::try_parse_from(["polyflare", "serve"]).unwrap();
        assert!(matches!(cli.command, Commands::Serve));
    }

    #[test]
    fn parses_accounts_import() {
        let cli = Cli::try_parse_from([
            "polyflare",
            "accounts",
            "import",
            "--from",
            "/tmp/store.db",
            "--fernet-key",
            "/tmp/encryption.key",
        ])
        .unwrap();
        match cli.command {
            Commands::Accounts {
                command:
                    AccountsCommands::Import {
                        from,
                        fernet_key,
                        dry_run,
                    },
            } => {
                assert_eq!(from, std::path::PathBuf::from("/tmp/store.db"));
                assert_eq!(fernet_key, std::path::PathBuf::from("/tmp/encryption.key"));
                assert!(!dry_run, "dry_run defaults to false without the flag");
            }
            _ => panic!("expected `accounts import`"),
        }
    }

    #[test]
    fn accounts_import_dry_run_flag_parses() {
        let cli = Cli::try_parse_from([
            "polyflare",
            "accounts",
            "import",
            "--from",
            "/tmp/store.db",
            "--fernet-key",
            "/tmp/encryption.key",
            "--dry-run",
        ])
        .unwrap();
        match cli.command {
            Commands::Accounts {
                command: AccountsCommands::Import { dry_run, .. },
            } => assert!(dry_run, "--dry-run must set dry_run"),
            _ => panic!("expected `accounts import`"),
        }
    }

    #[test]
    fn missing_subcommand_is_an_error() {
        assert!(Cli::try_parse_from(["polyflare"]).is_err());
    }

    #[test]
    fn login_pool_flag_parses_and_defaults_to_none() {
        let with =
            Cli::try_parse_from(["polyflare", "accounts", "login", "--pool", "team-a"]).unwrap();
        match with.command {
            Commands::Accounts {
                command: AccountsCommands::Login { pool, .. },
            } => assert_eq!(pool.as_deref(), Some("team-a")),
            _ => panic!("expected `accounts login`"),
        }
        let without = Cli::try_parse_from(["polyflare", "accounts", "login"]).unwrap();
        match without.command {
            Commands::Accounts {
                command: AccountsCommands::Login { pool, .. },
            } => assert!(pool.is_none(), "no --pool ⇒ unpooled"),
            _ => panic!("expected `accounts login`"),
        }
    }

    #[test]
    fn set_pool_parses_assign_and_clear() {
        // Assign.
        let assign = Cli::try_parse_from([
            "polyflare",
            "accounts",
            "set-pool",
            "--id",
            "codex_1",
            "--pool",
            "p1",
        ])
        .unwrap();
        match assign.command {
            Commands::Accounts {
                command: AccountsCommands::SetPool { id, pool },
            } => {
                assert_eq!(id, "codex_1");
                assert_eq!(pool.as_deref(), Some("p1"));
            }
            _ => panic!("expected `accounts set-pool`"),
        }
        // Clear (no --pool ⇒ None ⇒ unpooled).
        let clear =
            Cli::try_parse_from(["polyflare", "accounts", "set-pool", "--id", "codex_1"]).unwrap();
        match clear.command {
            Commands::Accounts {
                command: AccountsCommands::SetPool { pool, .. },
            } => assert!(pool.is_none(), "omitting --pool clears the pool"),
            _ => panic!("expected `accounts set-pool`"),
        }
    }

    #[test]
    fn set_capability_parses_true_and_false() {
        let on = Cli::try_parse_from([
            "polyflare",
            "accounts",
            "set-capability",
            "--id",
            "codex_1",
            "--security-work",
            "true",
        ])
        .unwrap();
        match on.command {
            Commands::Accounts {
                command: AccountsCommands::SetCapability { id, security_work },
            } => {
                assert_eq!(id, "codex_1");
                assert!(security_work);
            }
            _ => panic!("expected `accounts set-capability`"),
        }

        let off = Cli::try_parse_from([
            "polyflare",
            "accounts",
            "set-capability",
            "--id",
            "codex_1",
            "--security-work",
            "false",
        ])
        .unwrap();
        match off.command {
            Commands::Accounts {
                command: AccountsCommands::SetCapability { security_work, .. },
            } => assert!(!security_work),
            _ => panic!("expected `accounts set-capability`"),
        }
    }
}
