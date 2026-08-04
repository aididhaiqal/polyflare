use clap::Parser;
use polyflare_loopback::Config;
use reqwest::Url;
use std::{net::SocketAddr, process::ExitCode};
use tracing_subscriber::EnvFilter;

#[cfg(windows)]
mod windows_service;

#[derive(Debug, Parser)]
#[command(
    name = "polyflare-loopback",
    about = "Loopback companion for a remotely hosted PolyFlare instance",
    after_help = "This companion is unnecessary when PolyFlare already runs on this machine."
)]
struct Args {
    /// Remote PolyFlare HTTPS origin, without /backend-api or any other path.
    #[arg(long, env = "POLYFLARE_LOOPBACK_UPSTREAM_ORIGIN")]
    upstream_origin: Url,

    /// Local listener. Non-loopback addresses are always rejected.
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,

    /// Validate arguments and exit without binding a listener.
    #[arg(long)]
    check_config: bool,

    /// Run under the Windows Service Control Manager.
    #[cfg(windows)]
    #[arg(long, hide = true)]
    windows_service: bool,
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .without_time()
        .with_target(false)
        .init();

    let args = Args::parse();
    let config = match Config::try_new(args.listen, args.upstream_origin) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("configuration rejected: {error}");
            return ExitCode::from(2);
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            eprintln!("could not initialize the async runtime");
            return ExitCode::FAILURE;
        }
    };
    if args.check_config {
        return match runtime.block_on(polyflare_loopback::check_upstream(&config)) {
            Ok(()) => {
                println!("configuration valid");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("configuration rejected: {error}");
                ExitCode::from(2)
            }
        };
    }

    #[cfg(windows)]
    if args.windows_service {
        return match windows_service::run(config.clone()) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("Windows service failed: {error}");
                ExitCode::FAILURE
            }
        };
    }

    match runtime.block_on(polyflare_loopback::run(config)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("companion stopped: {error}");
            ExitCode::FAILURE
        }
    }
}
