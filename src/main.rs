use clap::{Parser, Subcommand};
use saturn::{
    app::api, db::migrations, nodes::scheduler, nodes::worker, tolerance::fault_tolerance,
};

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
    Migration,
    FaultTolerance,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Cli::parse();

    match args.command {
        Command::Api => api::run_api().await,
        Command::Scheduler => scheduler::run_scheduler().await,
        Command::Worker => worker::run_worker().await,
        Command::Migration => migrations::run_migrations().await,
        Command::FaultTolerance => fault_tolerance::run_fault_tolerance().await,
    }
}
