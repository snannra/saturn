use clap::{Parser, Subcommand};

use crate::app_state::AppState;

mod api;
mod app_state;
mod config;
mod fault_tolerance;
mod jobs;
mod metrics;
mod node;
mod scheduler;
mod users;
mod worker;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Api,
    Scheduler,
    Worker,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Cli::parse();

    match args.command {
        Command::Api => api::run_api().await,
        Command::Scheduler => scheduler::run_scheduler().await,
        Command::Worker => worker::run_worker().await,
    }
}
