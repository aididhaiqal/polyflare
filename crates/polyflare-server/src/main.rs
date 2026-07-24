//! PolyFlare binary entrypoint. Subcommands: `serve` (the M1 gateway) and `accounts import`
//! (the zero-re-auth codex-lb importer). Secrets are read from env / files and never logged.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use std::{
    future::{Future, IntoFuture},
    io,
};

use clap::{Parser, Subcommand};

use polyflare_anthropic::AnthropicExecutor;
use polyflare_codex::oauth::OAuthClient;
use polyflare_codex::{build_client, run_login, CodexVersionCache};
use polyflare_core::{Continuity, Executor, Selector};
use polyflare_server::app::{build_app_for_bind, build_codex_executor_with_client, AppState};
use polyflare_server::config::{self, ServeConfig};
use polyflare_server::continuity::CodexContinuity;
use polyflare_server::model_catalog::{
    floor_only_cache, HttpModelSource, ModelCatalogCache, ModelSource,
};
use polyflare_server::runtime_settings::{overlay_persisted_settings, RuntimeSettings};
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
    /// Client API-key management (D18): keys authenticate the CALLER on the proxy surface,
    /// distinct from the upstream account tokens `accounts` manages.
    Keys {
        #[command(subcommand)]
        command: KeysCommands,
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
        /// Also REFRESH already-present accounts: upsert their token columns (+ reset a stale
        /// `reauth_required`/deactivated status to active) from codex-lb — the zero-re-auth way to
        /// revive an account whose local token went stale. Pool/alias/routing are left untouched.
        /// Without this flag, existing accounts are skipped (insert-new-only, the default).
        #[arg(long = "refresh-existing")]
        refresh_existing: bool,
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

#[derive(Subcommand)]
enum KeysCommands {
    /// Generate + store a new client API key. Prints the RAW key to stdout EXACTLY ONCE — save it
    /// now, it is never shown again (only its hash is persisted).
    Create {
        /// Optional human-readable label (e.g. which caller/deployment this key is for).
        #[arg(long = "label", value_name = "LABEL")]
        label: Option<String>,
    },
    /// List all client API keys (id / prefix / label / enabled / created / last used). Never
    /// prints a raw key — only what's stored (the hash never leaves the store, either).
    List,
    /// Revoke (disable) a client API key by id. The row is kept for audit history; a revoked key
    /// is rejected by `require_client_key` (Task 3) but never deleted here.
    Revoke {
        /// The key id to revoke (as shown by `keys list`).
        #[arg(long = "id", value_name = "KEY_ID")]
        id: String,
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
                refresh_existing,
            } => accounts_import(&from, &fernet_key, dry_run, refresh_existing).await,
            AccountsCommands::Login { open, pool } => accounts_login(open, pool).await,
            AccountsCommands::SetPool { id, pool } => accounts_set_pool(&id, pool).await,
            AccountsCommands::SetCapability { id, security_work } => {
                accounts_set_capability(&id, security_work).await
            }
        },
        Commands::Keys { command } => match command {
            KeysCommands::Create { label } => keys_create(label).await,
            KeysCommands::List => keys_list().await,
            KeysCommands::Revoke { id } => keys_revoke(&id).await,
        },
    }
}

/// The M2b server: store-backed multi-account pool selection.
async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = ServeConfig::from_env()?;
    let store = Store::open(&config.db_path).await?;
    let settings_overlay = store.settings().get_all().await?;
    config::overlay_persisted_websocket_settings(&mut config, &settings_overlay);
    // Live-editable Settings subsystem Task 4: seed the atomic holder from the already-clamped
    // `ServeConfig` — BEFORE any field of `config` below is partially moved out of it
    // (`RuntimeSettings::new` needs a whole-struct borrow, which the borrow checker forbids once a
    // single field has been moved), then overlay any persisted `settings` table rows on top (DB
    // overrides beat the env/file defaults `ServeConfig::from_env` already resolved). A persisted
    // row that fails to parse (unknown key, or a value of the wrong shape for its field) is
    // skipped — see `overlay_persisted_settings`'s doc.
    let runtime_settings = Arc::new(RuntimeSettings::new(&config));
    overlay_persisted_settings(&runtime_settings, &settings_overlay);
    // D18 Task 4: the bind-address-aware posture — resolved ONCE here, BEFORE `AppState`/`build_app`,
    // using only already-available inputs (does any key exist yet; the configured bind; the
    // explicit override env var). See `polyflare_server::posture` for the full decision table and
    // rationale. A `StartupError` here (non-loopback bind, no keys, no override) propagates via `?`
    // and the process exits without ever opening a listener — never a silent/anonymous non-local
    // proxy.
    let has_keys = store.api_keys().count().await? > 0;
    let enforce_client_keys = polyflare_server::posture::resolve_proxy_enforcement(
        has_keys,
        &config.bind_addr,
        config.allow_unauthenticated_remote,
    )?;
    let cipher = TokenCipher::load_or_create(&config.key_path)?;
    let control_client = build_client()?;
    let codex_executor: Arc<dyn Executor> = build_codex_executor_with_client(
        control_client.clone(),
        config.http_requests_use_upstream_websocket,
        config.http_upstream_websocket_ping,
    )?;
    let anthropic_executor: Arc<dyn Executor> = Arc::new(AnthropicExecutor::new()?);
    // Routing: the global default strategy + any per-pool overrides, both from config.
    let selector: Arc<dyn Selector> = config.routing_strategy.selector();
    let pool_selectors: std::collections::HashMap<String, Arc<dyn Selector>> = config
        .pool_strategies
        .iter()
        .map(|(slug, strat)| (slug.clone(), strat.selector()))
        .collect();
    let oauth = OAuthClient::new(config.auth_base_url)?;
    let refresh_locks = polyflare_server::refresh_locks::RefreshLocks::default();
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

    // D15 Task 3: the live upstream model-catalog cache. Built AFTER `store`/`cipher`/
    // `codex_version` above (plus shared OAuth/refresh-lock handles), BEFORE `AppState` — see
    // `crate::model_catalog::HttpModelSource`'s doc for why (it needs its own owned handles, never
    // a circular `Arc<AppState>` back-reference). The
    // floor (`codex_bootstrap_floor()`) is the SAME never-empty static bootstrap `catalog.rs`
    // served before this feature existed, so every fallback rung — disabled, no accounts, fetch
    // failure — degrades to exactly that pre-D15 `/models` behavior.
    let model_catalog_enabled = config.model_catalog_enabled;
    let admission_limits = config.admission_limits;
    let model_catalog_floor = polyflare_server::catalog::codex_bootstrap_floor();
    let model_catalog = Arc::new(if model_catalog_enabled {
        let source: Box<dyn ModelSource> = Box::new(HttpModelSource::new(
            store.clone(),
            cipher.clone(),
            config.upstream_base_url.clone(),
            codex_version.clone(),
            oauth.clone(),
            refresh_locks.clone(),
        )?);
        ModelCatalogCache::new(
            source,
            Duration::from_secs(config.model_catalog_ttl_secs),
            model_catalog_floor,
        )
    } else {
        floor_only_cache(model_catalog_floor)
    });
    let state = Arc::new(AppState {
        codex_executor,
        control_client,
        anthropic_executor,
        selector,
        pool_selectors,
        continuity,
        store,
        cipher,
        oauth,
        upstream_base_url: config.upstream_base_url,
        anthropic_upstream_base_url: config.anthropic_upstream_base_url,
        refresh_locks,
        capture_fingerprint_path: config.capture_fingerprint_path,
        codex_version,
        account_cache: Arc::new(polyflare_server::account_cache::AccountCache::new()),
        token_cache: Arc::new(polyflare_server::token_cache::TokenCache::new()),
        runtime: Arc::new(
            polyflare_server::runtime_state::RuntimeStates::with_admission_limits(admission_limits),
        ),
        admin_token: config.admin_token,
        runtime_settings,
        ws_downstream: config.client_websocket_enabled,
        ws_relay_idle: config.websocket_idle_policy,
        log_bus: polyflare_server::log_bus::LogBus::new(1000),
        failover_metrics: polyflare_server::observability::FailoverMetrics::new(),
        health_tier_metrics: polyflare_server::observability::HealthTierMetrics::new(),
        starvation_metrics: polyflare_server::observability::StarvationMetrics::new(),
        enforce_client_keys,
        lease_metrics: polyflare_server::observability::LeaseMetrics::new(),
        upstream_request_metrics: polyflare_server::observability::UpstreamRequestMetrics::new(),
        rate_limit_metrics: polyflare_server::observability::RateLimitMetrics::new(),
        relay_metrics: polyflare_server::observability::RelayMetrics::new(),
        model_catalog,
    });
    if model_catalog_enabled {
        let readiness_state = state.clone();
        match await_startup_work_with_budget(
            async move {
                polyflare_server::catalog::warm_active_model_scopes(&readiness_state).await
            },
            MODEL_CATALOG_STARTUP_WARM_BUDGET,
        )
        .await
        {
            StartupWork::Completed(warmed) => tracing::info!(
                attempted_scopes = warmed.attempted_scopes,
                authoritative_scopes = warmed.authoritative_scopes,
                "model catalog readiness warmup completed"
            ),
            StartupWork::Continuing => tracing::warn!(
                budget_ms = MODEL_CATALOG_STARTUP_WARM_BUDGET.as_millis(),
                "model catalog readiness budget elapsed; warmup is continuing in background"
            ),
            StartupWork::Failed => tracing::warn!(
                "model catalog readiness warmup task failed; serving with stale/floor fallback"
            ),
        }
        let warm_state = state.clone();
        let refresh_interval = state.model_catalog.refresh_interval();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(refresh_interval).await;
                let warmed = polyflare_server::catalog::warm_active_model_scopes(&warm_state).await;
                tracing::debug!(
                    attempted_scopes = warmed.attempted_scopes,
                    authoritative_scopes = warmed.authoritative_scopes,
                    "model catalog background scope warmup completed"
                );
            }
        });
    }
    // Runtime usage-refresh loop: keeps each Codex account's rate-limit windows (5h + weekly) and
    // routing gate live, instead of the frozen numbers the importer left.
    polyflare_server::usage_refresh::spawn_usage_refresh(state.clone());
    // C12: hourly age-retention pruning over `request_log` + `usage_history` (disabled by default
    // via POLYFLARE_*_RETENTION_DAYS=0; see `polyflare_server::retention`).
    polyflare_server::retention::spawn_retention_prune(state.clone());

    let app = build_app_for_bind(state.clone(), &config.bind_addr);

    let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;
    println!("polyflare listening on {}", config.bind_addr);
    let drain_outcome =
        serve_with_bounded_shutdown(listener, app, shutdown_signal(), SHUTDOWN_DRAIN_TIMEOUT)
            .await?;
    if drain_outcome == DrainOutcome::TimedOut {
        tracing::warn!(
            timeout_secs = SHUTDOWN_DRAIN_TIMEOUT.as_secs(),
            "shutdown drain timed out; closing remaining connections"
        );
    }
    polyflare_server::usage_refresh::flush_cooldown_persistence(&state).await?;
    let flush_result = state.store.flush_background_writes().await;
    flush_result?;
    Ok(())
}

const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
const MODEL_CATALOG_STARTUP_WARM_BUDGET: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DrainOutcome {
    Drained,
    TimedOut,
}

#[derive(Debug, Eq, PartialEq)]
enum StartupWork<T> {
    Completed(T),
    Continuing,
    Failed,
}

/// Give startup work a bounded readiness budget without cancelling it. Dropping a Tokio
/// `JoinHandle` detaches the task, so a slow catalog warmup continues filling the cache after the
/// listener starts instead of either delaying service for the full upstream timeout or wasting
/// successful member fetches already in flight.
async fn await_startup_work_with_budget<F, T>(work: F, budget: Duration) -> StartupWork<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let mut task = tokio::spawn(work);
    match tokio::time::timeout(budget, &mut task).await {
        Ok(Ok(output)) => StartupWork::Completed(output),
        Ok(Err(_)) => StartupWork::Failed,
        Err(_) => StartupWork::Continuing,
    }
}

/// Stop accepting new connections when `shutdown` resolves, but never let long-lived streams keep
/// the process alive forever. The timeout starts at the shutdown signal—not at server startup.
async fn serve_with_bounded_shutdown<F>(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    shutdown: F,
    drain_timeout: Duration,
) -> io::Result<DrainOutcome>
where
    F: Future<Output = ()> + Send + 'static,
{
    let (drain_started_tx, drain_started_rx) = tokio::sync::oneshot::channel::<()>();
    let graceful_shutdown = async move {
        shutdown.await;
        let _ = drain_started_tx.send(());
    };
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(graceful_shutdown)
        .into_future();
    tokio::pin!(server);

    let drain_deadline = async move {
        if drain_started_rx.await.is_ok() {
            tokio::time::sleep(drain_timeout).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    tokio::pin!(drain_deadline);

    tokio::select! {
        result = &mut server => {
            result?;
            Ok(DrainOutcome::Drained)
        }
        _ = &mut drain_deadline => Ok(DrainOutcome::TimedOut),
    }
}

/// Stop accepting new connections on Ctrl-C (and SIGTERM on Unix), then let axum drain active
/// responses up to [`SHUTDOWN_DRAIN_TIMEOUT`] before `serve` closes the remainder and flushes the
/// Store's bounded background-writer queue.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to install Ctrl-C shutdown handler");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                tracing::error!(%error, "failed to install SIGTERM shutdown handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    tracing::info!("shutdown signal received; draining active requests");
}

/// Run the importer against the configured store + at-rest key, printing only counts. A `dry_run`
/// validates + reports what would be imported, then rolls back — writing nothing.
async fn accounts_import(
    from: &Path,
    fernet_key: &Path,
    dry_run: bool,
    refresh_existing: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = config::data_dir_from_env();
    let db_path = config::db_path(&data_dir);
    let key_path = config::key_path(&data_dir);

    let store = Store::open(&db_path).await?;
    let cipher = TokenCipher::load_or_create(&key_path)?;
    let summary =
        import_from_codex_lb(&store, from, fernet_key, &cipher, dry_run, refresh_existing).await?;
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
    if pool
        .as_deref()
        .is_some_and(|slug| !polyflare_server::write_api::valid_pool_slug(slug))
    {
        return Err("pool must be a 1..=48 character lowercase slug using a-z, 0-9, _ or -".into());
    }
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
    if pool
        .as_deref()
        .is_some_and(|slug| !polyflare_server::write_api::valid_pool_slug(slug))
    {
        return Err("pool must be a 1..=48 character lowercase slug using a-z, 0-9, _ or -".into());
    }
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

/// Generate + store a new client API key, then reveal the RAW key to the operator EXACTLY ONCE
/// (D18 Global Constraint — reveal-once). `println!` to stdout is the ONLY place the raw key is
/// ever printed anywhere in this binary — never `tracing::`, never `eprintln!`. After this
/// function returns, the raw key exists nowhere but whatever the operator copied from their
/// terminal; only its hash + display prefix persist in the store.
async fn keys_create(label: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = config::data_dir_from_env();
    let store = Store::open(&config::db_path(&data_dir)).await?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let created = polyflare_server::keys::create_key(&store, label.as_deref(), now).await?;
    println!("client API key created (id {}).", created.id);
    println!();
    println!("  {}", created.raw);
    println!();
    println!("SAVE THIS NOW — it will not be shown again. Only its hash is stored.");
    Ok(())
}

/// List all client API keys. Never prints a raw key — `render_key_row` formats an `ApiKeyRow`,
/// which has no raw-key/hash field to begin with (Task 1).
async fn keys_list() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = config::data_dir_from_env();
    let store = Store::open(&config::db_path(&data_dir)).await?;
    let rows = store.api_keys().list().await?;
    if rows.is_empty() {
        println!("no client API keys.");
        return Ok(());
    }
    for row in &rows {
        println!("{}", polyflare_server::keys::render_key_row(row));
    }
    Ok(())
}

/// Revoke (disable) a client API key by id. The row is kept (for audit history / `keys list`);
/// only `enabled` flips — `require_client_key` (Task 3) is what actually rejects a revoked key.
async fn keys_revoke(id: &str) -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = config::data_dir_from_env();
    let store = Store::open(&config::db_path(&data_dir)).await?;
    store.api_keys().set_enabled(id, false).await?;
    println!("client API key {id} revoked (disabled).");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[tokio::test]
    async fn startup_budget_detaches_slow_work_without_cancelling_it() {
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let outcome = await_startup_work_with_budget(
            async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let _ = finished_tx.send(());
            },
            Duration::from_millis(5),
        )
        .await;

        assert_eq!(outcome, StartupWork::Continuing);
        tokio::time::timeout(Duration::from_secs(1), finished_rx)
            .await
            .expect("detached warmup must continue after the readiness budget")
            .expect("warmup completion signal must be sent");
    }

    #[tokio::test]
    async fn shutdown_drain_is_bounded_for_a_request_that_never_finishes() {
        let entered = Arc::new(tokio::sync::Notify::new());
        let handler_entered = entered.clone();
        let app = axum::Router::new().route(
            "/hold",
            axum::routing::get(move || {
                let handler_entered = handler_entered.clone();
                async move {
                    handler_entered.notify_one();
                    std::future::pending::<&'static str>().await
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(serve_with_bounded_shutdown(
            listener,
            app,
            async move {
                let _ = shutdown_rx.await;
            },
            Duration::from_millis(50),
        ));

        let request = tokio::spawn(reqwest::get(format!("http://{addr}/hold")));
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("the hanging request must enter its handler");
        shutdown_tx.send(()).unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("bounded shutdown must return")
            .expect("server task must not panic")
            .expect("server must not fail");
        assert_eq!(outcome, DrainOutcome::TimedOut);
        request.abort();
    }

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
                        refresh_existing: _,
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

    #[test]
    fn keys_create_parses_label_and_defaults_to_none() {
        let with =
            Cli::try_parse_from(["polyflare", "keys", "create", "--label", "laptop"]).unwrap();
        match with.command {
            Commands::Keys {
                command: KeysCommands::Create { label },
            } => assert_eq!(label.as_deref(), Some("laptop")),
            _ => panic!("expected `keys create`"),
        }

        let without = Cli::try_parse_from(["polyflare", "keys", "create"]).unwrap();
        match without.command {
            Commands::Keys {
                command: KeysCommands::Create { label },
            } => assert!(label.is_none(), "--label is optional"),
            _ => panic!("expected `keys create`"),
        }
    }

    #[test]
    fn keys_list_parses() {
        let cli = Cli::try_parse_from(["polyflare", "keys", "list"]).unwrap();
        match cli.command {
            Commands::Keys {
                command: KeysCommands::List,
            } => {}
            _ => panic!("expected `keys list`"),
        }
    }

    #[test]
    fn keys_revoke_parses_id() {
        let cli = Cli::try_parse_from(["polyflare", "keys", "revoke", "--id", "key_abc"]).unwrap();
        match cli.command {
            Commands::Keys {
                command: KeysCommands::Revoke { id },
            } => assert_eq!(id, "key_abc"),
            _ => panic!("expected `keys revoke`"),
        }
    }
}
