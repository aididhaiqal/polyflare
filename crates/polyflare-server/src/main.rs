//! PolyFlare binary entrypoint. Subcommands: `serve` (the M1 gateway) and `accounts import`
//! (the zero-re-auth codex-lb importer). Secrets are read from env / files and never logged.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Parser, Subcommand};

use polyflare_codex::CodexExecutor;
use polyflare_server::app::{build_app, AppState};
use polyflare_server::config::{self, Config};
use polyflare_store::{import_from_codex_lb, Store, TokenCipher};

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
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Serve => serve().await,
        Commands::Accounts { command } => match command {
            AccountsCommands::Import { from, fernet_key } => {
                accounts_import(&from, &fernet_key).await
            }
        },
    }
}

/// The M1 server: identical wiring and behavior to the pre-M2a `main`.
async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env()?;
    let executor = Arc::new(CodexExecutor::new()?);
    let state = Arc::new(AppState {
        executor,
        account: config.account,
    });
    let app = build_app(state);

    let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;
    println!("polyflare listening on {}", config.bind_addr);
    axum::serve(listener, app).await?;
    Ok(())
}

/// Run the importer against the configured store + at-rest key, printing only counts.
async fn accounts_import(from: &Path, fernet_key: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = config::data_dir_from_env();
    let db_path = config::db_path(&data_dir);
    let key_path = config::key_path(&data_dir);

    let store = Store::open(&db_path).await?;
    let cipher = TokenCipher::load_or_create(&key_path)?;
    let summary = import_from_codex_lb(&store, from, fernet_key, &cipher).await?;
    println!(
        "imported {} account(s) and {} usage row(s)",
        summary.accounts_imported, summary.usage_rows_imported
    );
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
                command: AccountsCommands::Import { from, fernet_key },
            } => {
                assert_eq!(from, std::path::PathBuf::from("/tmp/store.db"));
                assert_eq!(fernet_key, std::path::PathBuf::from("/tmp/encryption.key"));
            }
            _ => panic!("expected `accounts import`"),
        }
    }

    #[test]
    fn missing_subcommand_is_an_error() {
        assert!(Cli::try_parse_from(["polyflare"]).is_err());
    }
}
